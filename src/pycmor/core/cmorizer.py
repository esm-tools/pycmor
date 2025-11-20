import copy
import getpass
import os
from importlib.resources import files
from pathlib import Path

import dask  # noqa: F401
import pandas as pd
import questionary
import xarray as xr  # noqa: F401
import yaml
from dask.distributed import Client
from everett.manager import generate_uppercase_key, get_runtime_config
from prefect import flow, get_run_logger, task
from prefect.futures import wait
from rich.progress import track

from ..data_request.collection import DataRequest
from ..data_request.table import DataRequestTable
from ..data_request.variable import DataRequestVariable

# Import CMIP7 interface if available
try:
    from ..data_request.cmip7_interface import CMIP7_API_AVAILABLE, CMIP7Interface
except ImportError:
    CMIP7Interface = None
    CMIP7_API_AVAILABLE = False
from ..std_lib.global_attributes import GlobalAttributes
from ..std_lib.timeaverage import _frequency_from_approx_interval
from .aux_files import attach_files_to_rule
from .cluster import CLUSTER_ADAPT_SUPPORT, CLUSTER_MAPPINGS, CLUSTER_SCALE_SUPPORT, DaskContext, set_dashboard_link
from .config import PycmorConfig, PycmorConfigManager
from .controlled_vocabularies import ControlledVocabularies
from .factory import create_factory
from .filecache import fc
from .logging import logger
from .pipeline import Pipeline
from .resource_loader import CMIP7MetadataLoader
from .rule import Rule
from .utils import wait_for_workers
from .validate import GENERAL_VALIDATOR, PIPELINES_VALIDATOR, RULES_VALIDATOR

DIMENSIONLESS_MAPPING_TABLE = files("pycmor.data").joinpath("dimensionless_mappings.yaml")
"""Path: The dimenionless unit mapping table, used to recreate meaningful units from
dimensionless fractional values (e.g. 0.001 --> g/kg)"""


class CMORizer:
    _SUPPORTED_CMOR_VERSIONS = ("CMIP6", "CMIP7")
    """tuple : Supported CMOR versions."""

    def __init__(
        self,
        pymor_cfg=None,
        pycmor_cfg=None,  # New parameter name
        general_cfg=None,
        pipelines_cfg=None,
        rules_cfg=None,
        dask_cfg=None,
        inherit_cfg=None,
        **kwargs,
    ):
        ################################################################################
        self._general_cfg = general_cfg or {}
        # Use pycmor_cfg if provided, otherwise fall back to pymor_cfg for backward compatibility
        pycmor_cfg = pycmor_cfg or pymor_cfg or {}
        self._pycmor_cfg = PycmorConfigManager.from_pycmor_cfg(pycmor_cfg)
        self._pymor_cfg = self._pycmor_cfg  # For backward compatibility
        self._dask_cfg = dask_cfg or {}
        self._inherit_cfg = inherit_cfg or {}
        self.rules = rules_cfg or []
        self.pipelines = pipelines_cfg or []
        self._cluster = None  # ask Cluster, might be set up later
        ################################################################################
        # CMOR Version Settings:

        if self._general_cfg.get("cmor_version") is None:
            raise ValueError("cmor_version must be set in the general configuration.")
        self.cmor_version = self._general_cfg["cmor_version"]
        if self.cmor_version not in self._SUPPORTED_CMOR_VERSIONS:
            logger.error(f"CMOR version {self.cmor_version} is not supported.")
            logger.error(f"Supported versions are {self._SUPPORTED_CMOR_VERSION}")
            raise ValueError(f"Unsupported CMOR version: {self.cmor_version}")

        ################################################################################
        # Print Out Configuration:
        logger.debug(80 * "#")
        logger.debug("---------------------")
        logger.debug("General Configuration")
        logger.debug("---------------------")
        logger.debug(yaml.dump(self._general_cfg))
        logger.debug("--------------------")
        logger.debug("PyCMOR Configuration:")
        logger.debug("--------------------")
        # This isn't actually the config, it's the "App" object. Everett is weird about this...
        pymor_config = PycmorConfig()
        # NOTE(PG): This variable is for demonstration purposes:
        _pymor_config_dict = {}
        for namespace, key, value, option in get_runtime_config(self._pymor_cfg, pymor_config):
            full_key = generate_uppercase_key(key, namespace)
            _pymor_config_dict[full_key] = value
        logger.info(yaml.dump(_pymor_config_dict))
        # Avoid confusion:
        del pymor_config
        logger.info(80 * "#")
        ################################################################################

        ################################################################################
        # NOTE(PG): Curious about the configuration? Add a breakpoint here and print
        #           out the variable _pymor_config_dict to see EVERYTHING that is
        #           available to you in the configuration.
        # breakpoint()
        ################################################################################

        ################################################################################
        # Post_Init:
        if self._pycmor_cfg("enable_dask"):
            logger.debug("Setting up dask configuration...")
            self._post_init_configure_dask()
            logger.debug("...done!")
            logger.debug("Creating dask cluster...")
            self._post_init_create_dask_cluster()
            logger.debug("...done!")
        self._post_init_create_pipelines()
        self._post_init_create_rules()
        self._post_init_create_data_request_tables()
        self._post_init_create_data_request()
        self._post_init_create_cmip7_interface()
        self._post_init_populate_rules_with_tables()
        self._post_init_populate_rules_with_dimensionless_unit_mappings()
        self._post_init_populate_rules_with_aux_files()
        self._post_init_populate_rules_with_data_request_variables()
        self._post_init_create_controlled_vocabularies()
        self._post_init_populate_rules_with_controlled_vocabularies()
        self._post_init_create_global_attributes_on_rules()
        logger.debug("...post-init done!")
        ################################################################################

    def __del__(self):
        """Gracefully close the cluster if it exists"""
        if self._cluster is not None:
            self._cluster.close()

    @staticmethod
    def _ensure_dask_slurm_account(jobqueue_cfg):
        slurm_jobqueue_cfg = jobqueue_cfg.get("slurm", {})
        if slurm_jobqueue_cfg.get("account") is None:
            slurm_jobqueue_cfg["account"] = os.environ.get("SLURM_JOB_ACCOUNT")
        return jobqueue_cfg

    def _post_init_configure_dask(self):
        """
        Sets up configuration for Dask-Distributed

        See Also
        --------
        https://docs.dask.org/en/stable/configuration.html?highlight=config#directly-within-python
        """
        # Needed to pre-populate config
        import dask.distributed  # noqa: F401
        import dask_jobqueue  # noqa: F401

        jobqueue_cfg = self._dask_cfg.get("jobqueue", {})
        jobqueue_cfg = self._ensure_dask_slurm_account(jobqueue_cfg)

        self._dask_cfg = {
            "distributed": self._dask_cfg.get("distributed", {}),
            "jobqueue": jobqueue_cfg,
        }

        logger.info("Updating Dask configuration. Changed values will be:")
        logger.info(yaml.dump(self._dask_cfg))
        dask.config.update(dask.config.config, self._dask_cfg)
        logger.info("Dask configuration updated!")

    def _post_init_create_dask_cluster(self):
        # FIXME: In the future, we can support PBS, too.
        logger.info("Setting up dask cluster...")
        cluster_name = self._pymor_cfg("dask_cluster")
        ClusterClass = CLUSTER_MAPPINGS[cluster_name]
        self._cluster = ClusterClass()
        set_dashboard_link(self._cluster)
        cluster_scaling_mode = self._pymor_cfg.get("dask_cluster_scaling_mode", "adapt")
        if cluster_scaling_mode == "adapt":
            if CLUSTER_ADAPT_SUPPORT[cluster_name]:
                min_jobs = self._pymor_cfg.get("dask_cluster_scaling_minimum_jobs", 1)
                max_jobs = self._pymor_cfg.get("dask_cluster_scaling_maximum_jobs", 10)
                self._cluster.adapt(minimum_jobs=min_jobs, maximum_jobs=max_jobs)
            else:
                logger.warning(f"{self._cluster} does not support adaptive scaling!")
        elif cluster_scaling_mode == "fixed":
            if CLUSTER_SCALE_SUPPORT[cluster_name]:
                jobs = self._pymor_cfg.get("dask_cluster_scaling_fixed_jobs", 5)
                self._cluster.scale(jobs=jobs)
            else:
                logger.warning(f"{self._cluster} does not support fixed scaing")
        else:
            raise ValueError("You need to specify adapt or fixed for pymor.dask_cluster_scaling_mode")
        # FIXME: Include the gateway option if possible
        # FIXME: Does ``Client`` needs to be available here?
        logger.info(f"Cluster can be found at: {self._cluster=}")
        logger.info(f"Dashboard {self._cluster.dashboard_link}")

        username = getpass.getuser()
        nodename = getattr(os.uname(), "nodename", "UNKNOWN")
        logger.info(
            "To see the dashboards run the following command in your computer's "
            "terminal:\n"
            f"\tpycmor ssh-tunnel --username {username} --compute-node "
            f"{nodename}"
        )

        dask_extras = 0
        messages = []
        messages.append("Importing Dask Extras...")
        if self._pymor_cfg.get("enable_flox", True):
            dask_extras += 1
            messages.append("...flox...")
            import flox  # noqa: F401
            import flox.xarray  # noqa: F401
        messages.append(f"...done! Imported {dask_extras} libraries.")
        if messages:
            for message in messages:
                logger.info(message)
        else:
            logger.info("No Dask extras specified...")

    def _post_init_create_data_request_tables(self):
        """
        Loads all the tables from table directory as a mapping object.
        A shortened version of the filename (i.e., ``CMIP6_Omon.json`` -> ``Omon``) is used as the mapping key.
        The same key format is used in CMIP6_table_id.json

        For CMIP7, CMIP_Tables_Dir is optional since tables are loaded from packaged data.
        """
        data_request_table_factory = create_factory(DataRequestTable)
        DataRequestTableClass = data_request_table_factory.get(self.cmor_version)
        # CMIP7 uses packaged data, so CMIP_Tables_Dir is optional
        table_dir = Path(self._general_cfg.get("CMIP_Tables_Dir", "."))
        tables = DataRequestTableClass.table_dict_from_directory(table_dir)
        self._general_cfg["tables"] = self.tables = tables

    def _post_init_create_data_request(self):
        """
        Creates a DataRequest object from the tables directory.

        For CMIP7, CMIP_Tables_Dir is optional since data request is loaded from packaged data.
        """
        # CMIP7 uses packaged data, so CMIP_Tables_Dir is optional
        table_dir = self._general_cfg.get("CMIP_Tables_Dir", ".")
        data_request_factory = create_factory(DataRequest)
        DataRequestClass = data_request_factory.get(self.cmor_version)
        self.data_request = DataRequestClass.from_directory(table_dir)

    def _post_init_create_cmip7_interface(self):
        """
        Initialize CMIP7 interface if available and configured.

        This method creates an optional CMIP7Interface instance that can be used
        for advanced queries and metadata lookups. The interface is only created
        if:
        1. The CMOR version is CMIP7
        2. The CMIP7 Data Request API is available

        Uses CMIP7MetadataLoader with priority chain:
        1. User-specified CMIP7_DReq_metadata path
        2. XDG cache directory
        3. Generated via export_dreq_lists_json
        4. Packaged resources
        5. Vendored data

        Configuration example:
            general:
                cmor_version: CMIP7
                CMIP7_DReq_metadata: /path/to/metadata.json  # optional
                CMIP7_DReq_version: v1.2.2.2  # optional, defaults to v1.2.2.2
                cmip7_experiments_file: /path/to/experiments.json  # optional
        """
        if self.cmor_version == "CMIP7" and CMIP7_API_AVAILABLE:
            # Use CMIP7MetadataLoader to get metadata file via priority chain
            user_metadata_path = self._general_cfg.get("CMIP7_DReq_metadata")
            dreq_version = self._general_cfg.get("CMIP7_DReq_version")

            loader = CMIP7MetadataLoader(version=dreq_version, user_path=user_metadata_path)
            metadata_file = loader.load()

            if metadata_file and metadata_file.exists():
                logger.info(f"Loading CMIP7 interface with metadata: {metadata_file}")
                self.cmip7_interface = CMIP7Interface()
                self.cmip7_interface.load_metadata(metadata_file=str(metadata_file))

                # Optionally load experiments data if configured
                experiments_file = self._general_cfg.get("cmip7_experiments_file")
                if experiments_file and Path(experiments_file).exists():
                    self.cmip7_interface.load_experiments_data(str(experiments_file))
                    logger.info("CMIP7 interface initialized with experiments data")
                else:
                    logger.info("CMIP7 interface initialized (without experiments data)")
            else:
                self.cmip7_interface = None
                logger.warning(
                    "Could not load CMIP7 metadata from any source. "
                    "CMIP7 interface will not be available. "
                    "Make sure export_dreq_lists_json is installed or specify CMIP7_DReq_metadata."
                )
        else:
            self.cmip7_interface = None
            if self.cmor_version == "CMIP7" and not CMIP7_API_AVAILABLE:
                logger.warning(
                    "CMIP7 Data Request API not available. " "Install with: pip install CMIP7-data-request-api"
                )

    def _post_init_populate_rules_with_tables(self):
        """
        Populates the rules with the tables in which the variable described by that rule is found.
        """
        tables = self._general_cfg["tables"]
        for rule in self.rules:
            for tbl in tables.values():
                if rule.cmor_variable in tbl.variables:
                    rule.add_table(tbl.table_id)

    def _post_init_populate_rules_with_data_request_variables(self):
        for drv in self.data_request.variables.values():
            rule_for_var = self.find_matching_rule(drv)
            if rule_for_var is None:
                continue
            if rule_for_var.data_request_variables == []:
                rule_for_var.data_request_variables = [drv]
            else:
                rule_for_var.data_request_variables.append(drv)
        # FIXME: This needs a better name...
        # Cluster might need to be copied:
        with DaskContext.set_cluster(self._cluster):
            self._rules_expand_drvs()
            self._rules_depluralize_drvs()

    def _post_init_create_controlled_vocabularies(self):
        """
        Reads the controlled vocabularies from the directory tree rooted at
        ``<tables_dir>/CMIP6_CVs`` and stores them in the ``controlled_vocabularies``
        attribute. This is done after the rules have been populated with the
        tables and data request variables, which may be used to lookup the
        controlled vocabularies.

        If CV_Dir is not provided in config, ResourceLoader will use fallback chain:
        1. User-specified path
        2. XDG cache
        3. Remote git download
        4. Packaged resources
        5. Vendored submodules
        """
        # CV_Dir and CV_version are optional - None triggers ResourceLoader fallback
        table_dir = self._general_cfg.get("CV_Dir")
        cv_version = self._general_cfg.get("CV_version")
        controlled_vocabularies_factory = create_factory(ControlledVocabularies)
        ControlledVocabulariesClass = controlled_vocabularies_factory.get(self.cmor_version)
        self.controlled_vocabularies = ControlledVocabulariesClass.load(table_dir, cv_version)

    def _post_init_populate_rules_with_controlled_vocabularies(self):
        for rule in self.rules:
            rule.controlled_vocabularies = self.controlled_vocabularies

    def _post_init_populate_rules_with_aux_files(self):
        """Attaches auxiliary files to the rules"""
        for rule in self.rules:
            attach_files_to_rule(rule)

    def _post_init_populate_rules_with_dimensionless_unit_mappings(self):
        """
        Reads the dimensionless unit mappings from a configuration file and
        updates the rules with these mappings.

        This method reads the dimensionless unit mappings from a file specified
        in the configuration. If the file is not specified or does not exist,
        an empty dictionary is used. The mappings are then added to each rule
        in the `rules` attribute.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        pymor_cfg = self._pymor_cfg
        unit_map_file = pymor_cfg.get("dimensionless_mapping_table", DIMENSIONLESS_MAPPING_TABLE)
        if unit_map_file is None:
            logger.warning("No dimensionless unit mappings file specified!")
            dimensionless_unit_mappings = {}
        else:
            with open(unit_map_file, "r") as f:
                dimensionless_unit_mappings = yaml.safe_load(f)
        # Add to rules:
        for rule in self.rules:
            rule.dimensionless_unit_mappings = dimensionless_unit_mappings

    def _match_pipelines_in_rules(self, force=False):
        for rule in self.rules:
            rule.match_pipelines(self.pipelines, force=force)

    def find_matching_rule(self, data_request_variable: DataRequestVariable) -> Rule or None:
        matches = []
        attr_criteria = [("cmor_variable", "variable_id")]
        for rule in self.rules:
            if all(
                getattr(rule, r_attr) == getattr(data_request_variable, drv_attr)
                for (r_attr, drv_attr) in attr_criteria
            ):
                matches.append(rule)
        if len(matches) == 0:
            msg = f"No rule found for {data_request_variable}"
            if self._pymor_cfg.get("raise_on_no_rule", False):
                raise ValueError(msg)
            elif self._pymor_cfg.get("warn_on_no_rule", False):
                logger.warning(msg)
            return None
        if len(matches) > 1:
            msg = f"Need only one rule to match to {data_request_variable}. Found {len(matches)}."
            if self._pymor_cfg.get("raise_on_multiple_rules", True):
                raise ValueError(msg)
            else:
                logger.critical(msg)
                logger.critical(
                    """
                    This should lead to a program crash! Exception due to:

                    >> pymor_cfg['raise_on_multiple_rules'] = False <<
                    """
                )
                logger.warning("Returning the first match.")
        return matches[0]

    # FIXME: This needs a better name...
    def _rules_expand_drvs(self):
        new_rules = []
        for rule in self.rules:
            if len(rule.data_request_variables) == 1:
                new_rules.append(rule)
            else:
                cloned_rules = rule.expand_drvs()
                for rule in cloned_rules:
                    # Rule has a table_id or a table_name, so it should only
                    # match that table
                    if hasattr(rule, "table_id"):
                        if isinstance(rule.table_id, str):
                            rule.table_id = [
                                rule.table_id,
                            ]
                        logger.info(f"Specified table_id as {rule.table_id=}")
                        for drv in rule.data_request_variables:
                            if drv.table_header.table_id in rule.table_id:
                                logger.info(f"Adding rule/table combo for {drv}")
                                new_rules.append(rule)
                    elif hasattr(rule, "table_name"):
                        if isinstance(rule.table_name, str):
                            rule.table_name = [
                                rule.table_name,
                            ]
                        logger.info(f"Specified table_name as {rule.table_name=}")
                        for drv in rule.data_request_variables:
                            if drv.table_header.table_id in rule.table_name:
                                logger.info(f"Adding rule/table combo for {drv}")
                                new_rules.append(rule)
                    else:
                        new_rules.append(rule)
        self.rules = new_rules

    def _rules_depluralize_drvs(self):
        """Ensures that only one data request variable is assigned to each rule"""
        for rule in self.rules:
            rule.depluralize_drvs()

    def _post_init_create_pipelines(self):
        pipelines = []
        for p in self.pipelines:
            if isinstance(p, Pipeline):
                pipelines.append(p)
            elif isinstance(p, dict):
                p["workflow_backend"] = p.get(
                    "workflow_backend",
                    self._pymor_cfg("pipeline_workflow_orchestrator"),
                )
                pl = Pipeline.from_dict(p)
                if self._cluster is not None:
                    pl.assign_cluster(self._cluster)
                pipelines.append(Pipeline.from_dict(p))
            else:
                raise ValueError(f"Invalid pipeline configuration for {p}")
        self.pipelines = pipelines

    def _post_init_create_rules(self):
        _rules = []
        for p in self.rules:
            if isinstance(p, Rule):
                _rules.append(p)
            elif isinstance(p, dict):
                _rules.append(Rule.from_dict(p))
            else:
                raise TypeError("rule must be an instance of Rule or dict")
        self.rules = _rules
        self._post_init_inherit_rules()
        self._post_init_attach_pymor_config_rules()

    def _post_init_attach_pymor_config_rules(self):
        for rule in self.rules:
            # NOTE(PG): **COPY** (don't assign) the configuration to the rule
            rule._pycmor_cfg = copy.deepcopy(self._pycmor_cfg)
            rule._pymor_cfg = rule._pycmor_cfg  # For backward compatibility

    def _post_init_inherit_rules(self):
        for rule_attr, rule_value in self._inherit_cfg.items():
            for rule in self.rules:
                rule.set(rule_attr, rule_value)

    def validate(self):
        """Performs validation on files if they are suitable for use with the pipeline requirements"""
        # Sanity Checks:
        # :PS: @PG the following functions are not defined yet
        # self._check_rules_for_table()
        # self._check_rules_for_output_dir()
        # FIXME(PS): Turn off this check, see GH #59 (https://tinyurl.com/3z7d8uuy)
        # self._check_is_subperiod()
        logger.debug("Starting validate....")
        self._check_units()
        logger.debug("...done!")

    def _check_is_subperiod(self):
        logger.info("checking frequency in netcdf file and in table...")
        errors = []
        for rule in self.rules:
            table_freq = _frequency_from_approx_interval(rule.data_request_variable.table_header.approx_interval)
            # is_subperiod from pandas does not support YE or ME notation
            table_freq = table_freq.rstrip("E")
            for input_collection in rule.inputs:
                data_freq = input_collection.frequency
                if data_freq is None:
                    if not input_collection.files:
                        logger.info("No. input files found. Skipping frequency check.")
                        break
                    data_freq = fc.get(input_collection.files[0]).freq
                is_subperiod = pd.tseries.frequencies.is_subperiod(data_freq, table_freq)
                if not is_subperiod:
                    errors.append(
                        ValueError(
                            f"Freq in source file {data_freq} is not a subperiod of freq in table {table_freq}."
                        ),
                    )
                logger.info(f"Frequency of data {data_freq}. Frequency in tables {table_freq}")
        if errors:
            for err in errors:
                logger.error(err)
            raise errors[0]

    def _check_units(self):
        # TODO (MA): This function needs to be cleaned up if it needs to stay
        # but it will probably be removed soon if we do the validation checks
        # via dryruns of the steps.
        def is_unit_scalar(value):
            if value is None:
                return False
            try:
                x = float(value)
            except ValueError:
                return False
            return (x - 1) == 0

        errors = []
        for rule in self.rules:
            for input_collection in rule.inputs:
                try:
                    filename = input_collection.files[0]
                except IndexError:
                    break
                model_unit = rule.get("model_unit") or fc.get(filename).units
                cmor_unit = rule.data_request_variable.units
                cmor_variable = rule.data_request_variables.get("cmor_variable")
                if model_unit is None:
                    if not (is_unit_scalar(cmor_unit) or cmor_unit == "%"):
                        errors.append(
                            ValueError(
                                f"dimensionless variables must have dimensionless units ({model_unit}  {cmor_unit})"
                            )
                        )
                if is_unit_scalar(cmor_unit):
                    if not is_unit_scalar(model_unit):
                        dimless = rule.get("dimensionless_unit_mappings", {})
                        if cmor_unit not in dimless.get(cmor_variable, {}):
                            errors.append(f"Missing mapping for dimensionless variable {cmor_variable}")
        if errors:
            for err in errors:
                logger.error(err)
            raise errors[0]

    @classmethod
    def from_dict(cls, data):
        if "general" in data:
            if not GENERAL_VALIDATOR.validate({"general": data["general"]}):
                raise ValueError(GENERAL_VALIDATOR.errors)
        # Use pycmor config if available, otherwise fall back to pymor for backward compatibility
        pycmor_cfg = data.get("pycmor", data.get("pymor", {}))
        instance = cls(
            pycmor_cfg=pycmor_cfg,
            general_cfg=data.get("general", {}),
            dask_cfg={
                "distributed": data.get("distributed", {}),
                "jobqueue": data.get("jobqueue", {}),
            },
            inherit_cfg=data.get("inherit", {}),
        )
        if "rules" in data:
            if not RULES_VALIDATOR.validate({"rules": data["rules"]}):
                raise ValueError(RULES_VALIDATOR.errors)
        for rule in data.get("rules", []):
            rule_obj = Rule.from_dict(rule)
            instance.add_rule(rule_obj)
            instance._post_init_attach_pymor_config_rules()
        instance._post_init_inherit_rules()
        if "pipelines" in data:
            if not PIPELINES_VALIDATOR.validate({"pipelines": data["pipelines"]}):
                raise ValueError(PIPELINES_VALIDATOR.errors)
        for pipeline in data.get("pipelines", []):
            pipeline["workflow_backend"] = pipeline.get(
                "workflow_backend",
                instance._pymor_cfg("pipeline_workflow_orchestrator"),
            )
            pipeline_obj = Pipeline.from_dict(pipeline)
            instance.add_pipeline(pipeline_obj)

        instance._post_init_populate_rules_with_tables()
        instance._post_init_create_data_request()
        instance._post_init_populate_rules_with_data_request_variables()
        instance._post_init_populate_rules_with_dimensionless_unit_mappings()
        instance._post_init_populate_rules_with_aux_files()
        instance._post_init_populate_rules_with_controlled_vocabularies()
        instance._post_init_create_global_attributes_on_rules()
        logger.debug("Object creation done!")
        return instance

    def add_rule(self, rule):
        if not isinstance(rule, Rule):
            raise TypeError("rule must be an instance of Rule")
        self.rules.append(rule)

    def add_pipeline(self, pipeline):
        if not isinstance(pipeline, Pipeline):
            raise TypeError("pipeline must be an instance of Pipeline")
        if self._cluster is not None:
            # Assign the cluster to this pipeline:
            pipeline.assign_cluster(self._cluster)
        self.pipelines.append(pipeline)

    def _rule_for_filepath(self, filepath):
        filepath = str(filepath)
        matching_rules = []
        for rule in self.rules:
            for pattern in rule.input_patterns:
                if pattern.match(filepath):
                    matching_rules.append(rule)
        return matching_rules

    def _rule_for_cmor_variable(self, cmor_variable):
        matching_rules = []
        for rule in self.rules:
            if rule.cmor_variable == cmor_variable:
                matching_rules.append(rule)
        logger.debug(f"Found {len(matching_rules)} rules to apply for {cmor_variable}")
        return matching_rules

    def check_rules_for_table(self, table_name):
        missing_variables = []
        for cmor_variable in self._cmor_tables[table_name]["variable_entry"]:
            if self._rule_for_cmor_variable(cmor_variable) == []:
                if self._pymor_cfg.get("raise_on_no_rule", False):
                    raise ValueError(f"No rule found for {cmor_variable}")
                elif self._pymor_cfg.get("warn_on_no_rule", True):
                    # FIXME(PG): This should be handled by the logger automatically
                    if not self._pymor_cfg.get("quiet", True):
                        logger.warning(f"No rule found for {cmor_variable}")
                missing_variables.append(cmor_variable)
        if missing_variables:
            logger.warning("This CMORizer may be incomplete or badly configured!")
            logger.warning(f"Missing rules for >> {len(missing_variables)} << variables.")

    def check_rules_for_output_dir(self, output_dir):
        all_files_in_output_dir = [f for f in Path(output_dir).iterdir()]
        for rule in self.rules:
            # Remove files from list when matching a rule
            for filepath in all_files_in_output_dir:
                if self._rule_for_filepath(filepath):
                    all_files_in_output_dir.remove(filepath)
        if all_files_in_output_dir:
            logger.warning("This CMORizer may be incomplete or badly configured!")
            logger.warning(f"Found >> {len(all_files_in_output_dir)} << files in output dir not matching any rule.")
            if questionary.confirm("Do you want to view these files?").ask():
                for filepath in all_files_in_output_dir:
                    logger.warning(filepath)

    def process(self, parallel=None):
        logger.debug("Process start!")
        self._match_pipelines_in_rules()
        if parallel is None:
            parallel = self._pymor_cfg.get("parallel", True)
        if parallel:
            logger.debug("Parallel processing...")
            # FIXME(PG): This is mixed up, hard-coding to prefect for now...
            workflow_backend = self._pymor_cfg.get("pipeline_orchestrator", "prefect")
            logger.debug(f"...with {workflow_backend}...")
            return self.parallel_process(backend=workflow_backend)
        else:
            return self.serial_process()

    def parallel_process(self, backend="prefect"):
        if backend == "prefect":
            logger.debug("About to submit _parallel_process_prefect()")
            return self._parallel_process_prefect()
        elif backend == "dask":
            return self._parallel_process_dask()
        else:
            raise ValueError("Unknown backend for parallel processing")

    def _parallel_process_prefect(self):
        # prefect_logger = get_run_logger()
        # logger = prefect_logger
        # @flow(task_runner=DaskTaskRunner(address=self._cluster.scheduler_address))
        logger.debug("Defining dynamically generated prefect workflow...")

        @flow(name="CMORizer Process")
        def dynamic_flow():
            rule_results = []
            for rule in self.rules:
                rule_results.append(self._process_rule.submit(rule))
            wait(rule_results)
            return rule_results

        logger.debug("...done!")

        logger.debug("About to return dynamic_flow()...")
        with DaskContext.set_cluster(self._cluster):
            # We encapsulate the flow in a context manager to ensure that the
            # Dask cluster is available in the singleton, which could be used
            # during unpickling to reattach it to a Pipeline.
            return dynamic_flow()

    def _parallel_process_dask(self, external_client=None):
        if external_client:
            client = external_client
        else:
            client = Client(cluster=self._cluster)  # start a local Dask client
        if wait_for_workers(client, 1):
            futures = [client.submit(self._process_rule, rule) for rule in self.rules]

            results = client.gather(futures)

            logger.success("Processing completed.")
            return results
        else:
            logger.error("Timeout reached waiting for dask cluster, sorry...")

    def serial_process(self):
        data = {}
        for rule in track(self.rules, description="Processing rules"):
            data[rule.name] = self._process_rule(rule)
        logger.success("Processing completed.")
        return data

    @flow
    def check_prefect(self):
        logger = get_run_logger()
        try:
            self._caching_check()
        except Exception:
            logger.critical("Problem with caching in Prefect detected...")

    @flow
    def _caching_check(self):
        """Checks if workflows are possible to be cached"""
        data = {}
        for rule in self.rules:
            # del rule._pymor_cfg
            # del rule.data_request_variable
            data[rule.name] = self._caching_single_rule(rule)
        return data

    @staticmethod
    @task
    def _caching_single_rule(rule):
        logger.info(f"Starting to try caching on {rule}")
        data = f"Cached call of {rule.name}"
        return data

    @staticmethod
    @task(name="Process rule")
    def _process_rule(rule):
        logger.info(f"Starting to process rule {rule}")
        data = None
        if not len(rule.pipelines) > 0:
            logger.error("No pipeline defined, something is wrong!")
        for pipeline in rule.pipelines:
            logger.info(f"Running {str(pipeline)}")
            data = pipeline.run(data, rule)
        return data

    def _post_init_create_global_attributes_on_rules(self):
        global_attributes_factory = create_factory(GlobalAttributes)
        GlobalAttributesClass = global_attributes_factory.get(self.cmor_version)
        for rule in self.rules:
            rule.create_global_attributes(GlobalAttributesClass)
