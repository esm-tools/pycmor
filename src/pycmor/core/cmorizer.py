import copy
import getpass
import os
import time
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
from .skip import RuleSkipped
from .aux_files import attach_files_to_rule
from .cluster import CLUSTER_ADAPT_SUPPORT, CLUSTER_MAPPINGS, CLUSTER_SCALE_SUPPORT, DaskContext, set_dashboard_link
from .config import PycmorConfig, PycmorConfigManager
from .controlled_vocabularies import ControlledVocabularies
from .factory import create_factory
from .filecache import fc
from .logging import logger
from .pipeline import Pipeline

# ResourceLocator classes imported locally in methods to avoid circular imports
from .rule import Rule
from .utils import wait_for_workers
from .validate import GENERAL_VALIDATOR, PIPELINES_VALIDATOR, RULES_SCHEMA, RuleSectionValidator

DIMENSIONLESS_MAPPING_TABLE = files("pycmor.data").joinpath("dimensionless_mappings.yaml")
"""Path: The dimenionless unit mapping table, used to recreate meaningful units from
dimensionless fractional values (e.g. 0.001 --> g/kg)"""


def _resolve_throttle_caps(pymor_cfg):
    """Resolve per-throttle-group submission caps from env + yaml.

    Resolution order:
    1. ``PYCMOR_THROTTLE_CAPS=group1:N1,group2:N2`` env var
    2. ``throttle_caps`` key in the user yaml ``pymor_cfg`` (a dict)
    3. defaults to an empty dict — any encountered group falls back
       to the hardcoded default in the batch maker (cap=2).

    Returns ``{group_name: cap_int}``.
    """
    caps = {}
    if pymor_cfg:
        yaml_caps = pymor_cfg.get("throttle_caps") if hasattr(pymor_cfg, "get") else None
        if isinstance(yaml_caps, dict):
            for k, v in yaml_caps.items():
                try:
                    caps[str(k)] = int(v)
                except (TypeError, ValueError):
                    continue
    env_val = os.environ.get("PYCMOR_THROTTLE_CAPS", "")
    for entry in env_val.split(","):
        entry = entry.strip()
        if ":" not in entry:
            continue
        k, _, v = entry.partition(":")
        try:
            caps[k.strip()] = int(v.strip())
        except (TypeError, ValueError):
            continue
    return caps


def _is_transient_compute_error(exc):
    """Return True if `exc` is a dask/distributed failure that typically
    recovers on retry. Used by ``_process_rule``'s whole-rule retry loop
    to decide whether to retry vs. fail fast.

    Transient patterns seen in production:
      - FutureCancelledError("scheduler-connection-lost"): driver lost
        connection to its own LocalCluster during finalize (cli30
        cap7_land_5: 19/20 rules succeeded, 1 failed).
      - OSError("Timed out trying to connect to tcp://..."): same shape
        but caught earlier in the stack (cli33 veg_land 1/2/3,
        cli33 lrcs_land mrsofc).
      - ConnectionResetError / ConnectionRefusedError from dask comm.

    Lifted from reverted commit fb639fa where it was scoped to
    ``_safe_to_netcdf``; veg_land's OSError fires above the save call
    (in ``lpjg_yearly_pipeline``) so we apply it at the rule level.
    """
    name = type(exc).__name__
    if name in ("FutureCancelledError", "CancelledError"):
        return True
    msg = str(exc)
    if "scheduler-connection-lost" in msg:
        return True
    if "Timed out trying to connect" in msg:
        return True
    if isinstance(exc, (ConnectionError, ConnectionResetError, TimeoutError)):
        return True
    # OSError covers many distributed-comm flavours
    if isinstance(exc, OSError) and ("connect" in msg.lower() or "timed out" in msg.lower()):
        return True
    return False


class CMORizer:
    _SUPPORTED_CMOR_VERSIONS = ("CMIP6", "CMIP7")
    """tuple : Supported CMOR versions."""

    # [FIXME] I'd like to deprecate the pymor_cfg constructor option, but still keep it around
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
        # [FIXME] This should be a deprecated property!
        self._pymor_cfg = self._pycmor_cfg  # For backward compatibility
        self._dask_cfg = dask_cfg or {}
        self._inherit_cfg = inherit_cfg or {}
        self.rules = rules_cfg or []
        self.pipelines = pipelines_cfg or []
        self._cluster = None  # Dask Cluster, might be set up later
        ################################################################################
        # CMOR Version Settings:

        # [FIXME] This should be done in the validator
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
        logger.debug("PyCMOR Configuration")
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

    def _get_versioned_class(self, base_class):
        """
        Get the appropriate subclass for current CMOR version via factory pattern.

        This helper method eliminates repeated factory boilerplate throughout
        the codebase. It centralizes the pattern of getting version-specific
        implementations.

        Parameters
        ----------
        base_class : type
            Base class with MetaFactory metaclass (e.g., DataRequest, TableLocator)

        Returns
        -------
        type
            Concrete subclass for self.cmor_version (e.g., CMIP6DataRequest)

        Examples
        --------
        Example of how this is used internally::

            DataRequestClass = self._get_versioned_class(DataRequest)
            # Returns CMIP6DataRequest if cmor_version is "CMIP6"
        """
        factory = create_factory(base_class)
        return factory.get(self.cmor_version)

    @staticmethod
    def _ensure_dask_slurm_account(jobqueue_cfg) -> dict:
        """Ensures that the slurm.account setting for Dask configuration matches the environment if not already set"""
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
        # [FIXME] In the future, we can support PBS, too.
        logger.info("Setting up dask cluster...")
        cluster_name = self._pymor_cfg("dask_cluster")
        ClusterClass = CLUSTER_MAPPINGS[cluster_name]
        cluster_kwargs = {}
        if cluster_name == "local":
            n_workers = self._pymor_cfg.get("dask_n_workers", None)
            if n_workers is not None and str(n_workers) != "None":
                cluster_kwargs["n_workers"] = int(n_workers)
            tpw = self._pymor_cfg.get("dask_threads_per_worker", None)
            if tpw is not None and str(tpw) != "None":
                cluster_kwargs["threads_per_worker"] = int(tpw)
            mem = self._pymor_cfg.get("dask_memory_limit", None)
            if mem is not None and str(mem) != "None":
                cluster_kwargs["memory_limit"] = mem
        self._cluster = ClusterClass(**cluster_kwargs)
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

    def _locate_table_dir(self):
        from .resource_locator import TableLocator

        user_table_dir = self._general_cfg.get("CMIP_Tables_Dir")
        table_version = self._general_cfg.get("CMIP_Tables_version")

        TableLocatorClass = self._get_versioned_class(TableLocator)
        locator = TableLocatorClass(version=table_version, user_path=user_table_dir)
        table_dir = locator.locate()
        return table_dir

    def _post_init_create_data_request_tables(self):
        """
        Loads all the tables from table directory using ResourceLocator priority chain.

        Uses 5-level priority to locate tables:
        1. User-specified CMIP_Tables_Dir
        2. XDG cache
        3. Remote git download
        4. Packaged resources
        5. Vendored submodules

        A shortened version of the filename (i.e., ``CMIP6_Omon.json`` -> ``Omon``) is used as the mapping key.
        The same key format is used in CMIP6_table_id.json
        """
        table_dir = self._locate_table_dir()

        if table_dir is None:
            raise FileNotFoundError(
                f"Could not locate {self.cmor_version} tables from any source. "
                "Check that git submodules are initialized or internet connection is available."
            )

        DataRequestTableClass = self._get_versioned_class(DataRequestTable)
        tables = {t.table_id: t for t in DataRequestTableClass.find_all(table_dir)}
        self._general_cfg["tables"] = self.tables = tables
        logger.debug(f"Loaded {len(tables)} CMOR tables from {table_dir}")

    def _post_init_create_data_request(self):
        """
        Creates a DataRequest object from the tables directory using ResourceLocator.

        Uses TableLocator with 5-level priority chain to locate tables.
        For CMIP7, if CMIP7_DReq_metadata is specified, uses that instead.
        """
        DataRequestClass = self._get_versioned_class(DataRequest)

        # For CMIP7, prefer user-specified metadata file
        if self.cmor_version == "CMIP7":
            user_metadata_path = self._general_cfg.get("CMIP7_DReq_metadata")
            if user_metadata_path:
                logger.info(f"Using user-specified CMIP7 metadata: {user_metadata_path}")
                self.data_request = DataRequestClass.from_json_file(user_metadata_path)
                logger.debug(f"Created DataRequest from {user_metadata_path}")
                return

        # Fallback to tables directory
        table_dir = self._locate_table_dir()

        self.data_request = DataRequestClass.from_directory(table_dir)
        logger.debug(f"Created DataRequest from {table_dir}")

    def _post_init_create_cmip7_interface(self):
        """
        Initialize metadata interface using factory pattern.

        This method creates an optional interface instance for metadata queries.
        Uses MetadataLocator with priority chain:
        1. User-specified metadata path
        2. XDG cache directory
        3. Generated/downloaded metadata
        4. Packaged resources
        5. Vendored data

        For CMIP7, creates CMIP7Interface if API is available.
        For CMIP6, metadata_file will be None (expected).

        Configuration example:
            general:
                cmor_version: CMIP7
                CMIP7_DReq_metadata: /path/to/metadata.json  # optional
                CMIP7_DReq_version: v1.2.2.5  # optional
                cmip7_experiments_file: /path/to/experiments.json  # optional
        """
        from .resource_locator import MetadataLocator

        user_metadata_path = self._general_cfg.get("CMIP7_DReq_metadata")
        dreq_version = self._general_cfg.get("CMIP7_DReq_version")

        MetadataLocatorClass = self._get_versioned_class(MetadataLocator)
        locator = MetadataLocatorClass(version=dreq_version, user_path=user_metadata_path)
        metadata_file = locator.locate()

        # For CMIP6, metadata_file will be None (expected)
        if self.cmor_version == "CMIP7" and metadata_file and CMIP7_API_AVAILABLE:
            logger.debug(f"Loading CMIP7 interface with metadata: {metadata_file}")
            self.cmip7_interface = CMIP7Interface()
            self.cmip7_interface.load_metadata(metadata_file=str(metadata_file))

            # Optionally load experiments data if configured
            experiments_file = self._general_cfg.get("cmip7_experiments_file")
            if experiments_file and Path(experiments_file).exists():
                self.cmip7_interface.load_experiments_data(str(experiments_file))
                logger.debug("CMIP7 interface initialized with experiments data")
            else:
                logger.debug("CMIP7 interface initialized (without experiments data)")
        else:
            self.cmip7_interface = None
            if self.cmor_version == "CMIP7" and not metadata_file:
                logger.warning(
                    "Could not locate CMIP7 metadata from any source. "
                    "CMIP7 interface will not be available. "
                    "Make sure export_dreq_lists_json is installed or specify CMIP7_DReq_metadata."
                )
            elif self.cmor_version == "CMIP7" and not CMIP7_API_AVAILABLE:
                logger.warning("CMIP7 Data Request API not available. Install with: pip install CMIP7-data-request-api")

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
        logger.debug(f"Data request has {len(self.data_request.variables)} variables")
        for drv in self.data_request.variables.values():
            # Route to CMIP7-specific matching for exact compound name comparison
            if self.cmor_version == "CMIP7":
                rule_for_var = self.find_matching_rule_cmip7(drv)
            else:
                rule_for_var = self.find_matching_rule(drv)

            if rule_for_var is None:
                continue
            if rule_for_var.data_request_variables == []:
                rule_for_var.data_request_variables = [drv]
            else:
                rule_for_var.data_request_variables.append(drv)
        # [FIXME] This needs a better name...
        # Cluster might need to be copied:
        with DaskContext.set_cluster(self._cluster):
            self._rules_expand_drvs()
            self._rules_depluralize_drvs()

    def _post_init_create_controlled_vocabularies(self):
        """
        Load controlled vocabularies using ResourceLocator priority chain.

        If CV_Dir is not provided in config, CVLocator will use 5-level fallback:
        1. User-specified path
        2. XDG cache
        3. Remote git download
        4. Packaged resources
        5. Vendored submodules
        """
        cv_dir = self._general_cfg.get("CV_Dir")
        cv_version = self._general_cfg.get("CV_version")

        ControlledVocabulariesClass = self._get_versioned_class(ControlledVocabularies)
        self.controlled_vocabularies = ControlledVocabulariesClass.load(cv_dir, cv_version)
        logger.debug(f"Loaded controlled vocabularies from {cv_dir or 'default location'}")

    def _post_init_populate_rules_with_controlled_vocabularies(self):
        for rule in self.rules:
            rule.controlled_vocabularies = self.controlled_vocabularies
            rule.cmor_version = self.cmor_version

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
            # Ensure all matched pipelines have the cluster assigned
            if self._cluster is not None:
                for pl in rule.pipelines:
                    if getattr(pl, "_cluster", None) is None:
                        pl.assign_cluster(self._cluster)

    def find_matching_rule(self, data_request_variable: DataRequestVariable) -> Rule or None:
        matches = []
        drv_id = getattr(data_request_variable, "variable_id", "UNKNOWN")
        logger.debug(f"Looking for rule matching data_request_variable: {drv_id}")
        for rule in self.rules:
            # Determine what to compare: prefer compound_name if available on rule
            compound_name_match = False
            if hasattr(rule, "compound_name") and rule.compound_name is not None:
                rule_value = rule.compound_name
                drv_value = getattr(data_request_variable, "variable_id")
                drv_name = getattr(data_request_variable, "name", drv_value)

                # Extract the variable short name from the rule's compound name
                if "." in rule_value:
                    rule_parts = rule_value.split(".")
                    rule_var = rule_parts[1] if len(rule_parts) >= 2 else rule_value
                else:
                    rule_var = rule_value

                # Extract the variable short name from the DRV
                if "." in str(drv_value):
                    drv_parts = str(drv_value).split(".")
                    drv_var = drv_parts[1] if len(drv_parts) >= 2 else drv_value
                else:
                    # DRV has a plain short name (e.g., "tas") -- compare directly
                    drv_var = drv_value

                # Also check full compound name match for CMIP6/CMIP7
                compound_name_match_cmip6 = (
                    getattr(data_request_variable, "cmip6_compound_name", None) == rule.compound_name
                )
                compound_name_match_cmip7 = (
                    getattr(data_request_variable, "cmip7_compound_name", None) == rule.compound_name
                )
                # Also match rule compound_name directly against drv name
                compound_name_match_name = drv_name == rule_var
                compound_name_match = compound_name_match_cmip6 or compound_name_match_cmip7 or compound_name_match_name
            else:
                # Use cmor_variable with compound name extraction logic
                rule_value = getattr(rule, "cmor_variable")
                drv_value = getattr(data_request_variable, "variable_id")
                # Handle compound names in data request variable
                if "." in str(drv_value) and str(drv_value).count(".") >= 1:
                    parts = str(drv_value).split(".")
                    drv_var = parts[1] if len(parts) >= 2 else drv_value
                else:
                    drv_var = drv_value
                rule_var = rule_value

            if rule_var == drv_var:
                matches.append(rule)
            elif compound_name_match:
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

    def find_matching_rule_cmip7(self, data_request_variable: DataRequestVariable) -> Rule or None:
        """Match rules by exact compound name for CMIP7.

        This method compares full CMIP7 compound names without any extraction,
        preserving branding, frequency, and region information.

        Parameters
        ----------
        data_request_variable : DataRequestVariable
            The CMIP7 data request variable to match.

        Returns
        -------
        Rule or None
            Matched rule or None if no match found.
        """
        matches = []
        drv_compound_name = data_request_variable.variable_id  # Should be full compound name
        logger.debug(f"Looking for rule matching CMIP7 compound name: {drv_compound_name}")

        for rule in self.rules:
            if hasattr(rule, "compound_name") and rule.compound_name:
                # Exact compound name matching for CMIP7
                if rule.compound_name == drv_compound_name:
                    logger.debug(f"  Rule '{rule.name}' matches: {rule.compound_name} == {drv_compound_name}")
                    matches.append(rule)
                else:
                    logger.debug(f"  Rule '{rule.name}' does not match: {rule.compound_name} != {drv_compound_name}")

        if len(matches) == 0:
            msg = f"No rule found for CMIP7 variable {drv_compound_name}"
            if self._pymor_cfg.get("raise_on_no_rule", False):
                raise ValueError(msg)
            elif self._pymor_cfg.get("warn_on_no_rule", False):
                logger.warning(msg)
            return None
        if len(matches) > 1:
            msg = f"Need only one rule to match to {drv_compound_name}. Found {len(matches)}."
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
        logger.debug(f"Expanding {len(self.rules)} rules based on data_request_variables")
        for rule in self.rules:
            num_drvs = len(rule.data_request_variables)
            logger.debug(f"Rule '{rule.name}' has {num_drvs} data_request_variables")

            # Handle zero DRVs - this is always an error
            if len(rule.data_request_variables) == 0:
                if self.cmor_version == "CMIP7":
                    raise ValueError(
                        f"Rule '{rule.name}' with compound_name='{getattr(rule, 'compound_name', 'NOT SET')}' "
                        f"did not match any variables in the CMIP7 data request"
                    )
                else:
                    # CMIP6
                    raise ValueError(
                        f"Rule '{rule.name}' with cmor_variable='{getattr(rule, 'cmor_variable', 'NOT SET')}' "
                        f"did not match any variables in the data request"
                    )

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
        logger.debug(f"After expansion: {len(new_rules)} rules")
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
                pipelines.append(pl)
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
        """Apply ``inherit:`` defaults to rules that do not set the key themselves.

        ``inherit`` is documented as "added to all rules unless the rule
        overrides", and ``from_dict`` already implements that with
        ``{**inherit_cfg, **rule}``. This loop then ran over the finished rules
        and called ``Rule.set``, whose ``force=False`` branch warns and assigns
        anyway, so every per-rule override of an inherit key was silently
        replaced by the tier default.

        It was silent in effect but not in the logs: cli110 emitted 26
        "Attribute grid_label already exists" warnings and shipped 48 files
        whose grid_label named the wrong grid. Since grid_label is part of the
        DRS path, two rules that differ only by grid then collide, and one
        overwrites the other on disk (that is what happened to areacella).
        """
        for rule_attr, rule_value in self._inherit_cfg.items():
            for rule in self.rules:
                if hasattr(rule, rule_attr):
                    continue
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
        # Merge inherit values into rules before validation
        inherit_cfg = data.get("inherit", {})
        rules_with_inherit = []
        for rule in data.get("rules", []):
            # Create a new dict with inherit values, then overlay rule values
            merged_rule = {**inherit_cfg, **rule}
            rules_with_inherit.append(merged_rule)

        if rules_with_inherit:
            # Create a dynamic validator based on CMOR version
            cmor_version = data.get("general", {}).get("cmor_version")
            rules_validator = RuleSectionValidator(RULES_SCHEMA, cmor_version=cmor_version)
            if not rules_validator.validate({"rules": rules_with_inherit}):
                raise ValueError(rules_validator.errors)

        # Use original rules (without inherit merged) for creation
        # The inheritance will be applied later in _post_init_inherit_rules()
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

        logger.debug(f"Loaded {len(instance.rules)} rules from configuration")
        logger.debug(f"Loaded {len(instance.pipelines)} pipelines from configuration")
        instance._post_init_populate_rules_with_tables()
        instance._post_init_create_data_request()
        instance._post_init_create_cmip7_interface()
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

    def _check_rules_for_table(self, table_name):
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

    def _check_rules_for_output_dir(self, output_dir):
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
        logger.debug(f"Processing {len(self.rules)} rules")
        logger.debug(f"Available pipelines: {[getattr(p, 'name', 'unnamed') for p in self.pipelines]}")
        self._match_pipelines_in_rules()
        rules_with_pipelines = sum(1 for rule in self.rules if hasattr(rule, "pipeline") and rule.pipeline is not None)
        logger.debug(f"Matched pipelines to {rules_with_pipelines} rules")
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

        # Bound number of rules in flight to W*TPW. The naive
        # "submit every rule then wait()" path lets every parent fan
        # out via ``distributed.secede()`` inside save_dataset, with
        # the scheduler then holding 50-100 concurrent save graphs and
        # cascading OSError("Timed out trying to connect to scheduler
        # after 30 s") on cap7_land at ~46 min wall. See
        # ``DESIGN_PROPOSAL_subflow_deadlock.md`` §10.5.
        # NOTE: this is the production code path under the current
        # config-key plumbing (``_pymor_cfg.get('pipeline_orchestrator',
        # 'prefect')`` in ``parallel_process()`` always falls through
        # to "prefect" because the schema actually defines
        # ``pipeline_workflow_orchestrator``; the parallel-dask path
        # at ``_parallel_process_dask`` has a parallel throttle for
        # whenever the dispatcher routing is fixed).
        def _int_or_default(key, default):
            v = self._pymor_cfg.get(key, default)
            if v is None or str(v) == "None":
                return default
            return int(v)

        n_workers = _int_or_default("dask_n_workers", 1)
        tpw = _int_or_default("dask_threads_per_worker", 1)
        max_in_flight = max(1, n_workers * tpw)

        # Per-throttle-group concurrency caps. A pipeline-level
        # ``throttle_group: <name>`` declaration (see ``Pipeline.__init__``)
        # joins this group; the group's cap limits how many of its rules
        # can be in the same submission batch.
        #
        # Resolution: PYCMOR_THROTTLE_CAPS env var > inherit yaml
        # ``throttle_caps`` > default cap of 2 for any encountered group.
        #
        # Motivation: lrcs_seaice's 7-rule OIFS-regrid family ran 4 at
        # once on the driver process, hitting 87 GiB RSS and cascading
        # rule failures. See FORENSIC_lrcs_seaice_failure.md.
        throttle_caps = _resolve_throttle_caps(self._pymor_cfg)
        logger.info(f"Throttle caps (per-group rule submission limit): {throttle_caps or 'none'}")

        def _rule_throttle_group(rule):
            # Rule-level annotation wins (per-rule override). Falls back
            # to pipeline-level annotation. This lets unpipelined rules
            # (no ``pipelines:`` key, default pipeline used) join tier
            # throttling via ``inherit: throttle_group: <name>``.
            grp = getattr(rule, "throttle_group", None)
            if grp:
                return grp
            for pl in getattr(rule, "pipelines", None) or []:
                grp = getattr(pl, "throttle_group", None)
                if grp:
                    return grp
            return None

        def _make_batches(rules):
            """Yield batches of up to ``max_in_flight`` rules each, with
            no batch containing more than ``throttle_caps[group]`` rules
            from the same throttle group (default cap 2 for any
            encountered group)."""
            default_cap = 2
            pending = list(rules)
            while pending:
                batch = []
                group_count = {}
                remaining = []
                for rule in pending:
                    if len(batch) >= max_in_flight:
                        remaining.append(rule)
                        continue
                    grp = _rule_throttle_group(rule)
                    if grp is not None:
                        cap = throttle_caps.get(grp, default_cap)
                        if group_count.get(grp, 0) >= cap:
                            remaining.append(rule)
                            continue
                        group_count[grp] = group_count.get(grp, 0) + 1
                    batch.append(rule)
                if not batch:
                    # Should not happen with sensible caps (cap > 0 and
                    # at least one rule with no/un-saturated group), but
                    # guard against infinite loop.
                    raise RuntimeError(
                        f"Cannot make progress: {len(pending)} rules deferred "
                        f"indefinitely. Check throttle caps {throttle_caps} "
                        f"vs max_in_flight={max_in_flight}."
                    )
                yield batch
                pending = remaining

        @flow(name="CMORizer Process")
        def dynamic_flow():
            rules = list(self.rules)
            n = len(rules)
            logger.info(
                f"Submitting rules in batches of up to {max_in_flight} "
                f"(n_workers={n_workers} * tpw={tpw}); total rules={n}"
            )
            rule_results = []
            batches = list(_make_batches(rules))
            for batch_i, batch in enumerate(batches):
                batch_futures = [self._process_rule.submit(r) for r in batch]
                wait(batch_futures)
                rule_results.extend(batch_futures)
                # Per-batch group counts for visibility under throttling.
                group_summary = {}
                for r in batch:
                    g = _rule_throttle_group(r) or "_unthrottled"
                    group_summary[g] = group_summary.get(g, 0) + 1
                logger.info(f"Batch {batch_i + 1}/{len(batches)} done " f"({len(batch)} rules; groups={group_summary})")
            return rule_results

        logger.debug("...done!")

        logger.debug("About to return dynamic_flow()...")
        with DaskContext.set_cluster(self._cluster):
            # We encapsulate the flow in a context manager to ensure that the
            # Dask cluster is available in the singleton, which could be used
            # during unpickling to reattach it to a Pipeline.
            result = dynamic_flow(return_state=True)
            if result.is_failed():
                exc = result.result(raise_on_failure=False)
                if isinstance(exc, BaseException):
                    raise exc
                raise RuntimeError(f"CMORizer parallel processing failed: {exc}")
            # Unwrap individual rule results -- they may be State objects
            # from Prefect's submit() calls
            rule_results = result.result()
            unwrapped = []
            for item in rule_results:
                if hasattr(item, "is_failed") and callable(item.is_failed):
                    if item.is_failed():
                        exc = item.result(raise_on_failure=False)
                        if isinstance(exc, BaseException):
                            raise exc
                        raise RuntimeError(f"Rule processing failed: {exc}")
                    unwrapped.append(item.result())
                else:
                    unwrapped.append(item)
            return unwrapped

    def _parallel_process_dask(self, external_client=None):
        from distributed import as_completed

        if external_client:
            client = external_client
        else:
            client = Client(cluster=self._cluster)  # start a local Dask client
        if not wait_for_workers(client, 1):
            logger.error("Timeout reached waiting for dask cluster, sorry...")
            return

        # Bound the number of parents in flight to W * TPW. Without this,
        # the naive ``[client.submit(...) for rule in self.rules]`` list-
        # comprehension fires every rule simultaneously; once each parent
        # reaches ``save_dataset`` -> ``to_netcdf`` -> ``dask.compute()``,
        # ``distributed.secede()`` releases the parent's worker thread
        # back to the pool, letting dask dispatch the next queued parent.
        # With N=120+ rules of homogeneous heavy pipelines (cap7_land,
        # lpjg_monthly_*) the scheduler ends up holding 50-100 concurrent
        # save graphs; its asyncio loop and TCP accept queue back up,
        # workers fail to (re-)connect, OSError("Timed out trying to
        # connect to scheduler after 30 s") cascades. See
        # ``DESIGN_PROPOSAL_subflow_deadlock.md`` §10.5.
        # Match the everett config quirk used at line ~216 above:
        # dask_n_workers / dask_threads_per_worker may come back as None
        # or the literal string "None" depending on how the yaml parsed.
        def _int_or_default(key, default):
            v = self._pymor_cfg.get(key, default)
            if v is None or str(v) == "None":
                return default
            return int(v)

        n_workers = _int_or_default("dask_n_workers", 1)
        tpw = _int_or_default("dask_threads_per_worker", 1)
        max_in_flight = max(1, n_workers * tpw)
        rule_iter = iter(self.rules)
        futures = []
        for _ in range(max_in_flight):
            try:
                rule = next(rule_iter)
            except StopIteration:
                break
            futures.append(client.submit(self._process_rule, rule))
        logger.info(
            f"Submitting rules with rolling window: "
            f"max_in_flight={max_in_flight} (n_workers={n_workers} * tpw={tpw}); "
            f"total rules={len(self.rules)}"
        )

        results = []
        try:
            ac = as_completed(futures)
            for fut in ac:
                try:
                    results.append(fut.result())
                except Exception as exc:
                    # Per-rule exceptions: log and continue. The behavior
                    # of the prior ``client.gather(futures)`` was to
                    # raise the first exception; matching ``return_when``
                    # semantics here would change ``process()`` callers'
                    # expectations. Easier to log + collect, preserving
                    # the rolling-window throughput so a failing tier
                    # doesn't stall the rest.
                    logger.error(f"rule future raised: {type(exc).__name__}: {exc}")
                fut.release()
                try:
                    rule = next(rule_iter)
                except StopIteration:
                    continue
                ac.add(client.submit(self._process_rule, rule))
        finally:
            # The list ``futures`` holds only the priming wave by now;
            # the rolling-window submissions live on ``ac``. Both are
            # released (priming) or already released (rolling) above,
            # but we still need the worker GC sweep.
            for f in futures:
                try:
                    f.release()
                except Exception:
                    pass
            del futures
            self._cleanup_dask_workers()
        logger.success("Processing completed.")
        return results

    def serial_process(self):
        succeeded = []
        failed = {}
        for rule in track(self.rules, description="Processing rules"):
            try:
                self._process_rule(rule)
                succeeded.append(rule.name)
            except Exception as e:
                logger.error(f"Rule '{rule.name}' failed: {e}")
                failed[rule.name] = e
            # Free Dask worker memory between rules to prevent accumulation
            self._cleanup_dask_workers()
        if failed:
            logger.warning(f"{len(failed)} rule(s) failed: {', '.join(failed.keys())}")
        logger.success(f"Processing completed. {len(succeeded)} succeeded, {len(failed)} failed.")
        return {name: True for name in succeeded}

    def _cleanup_dask_workers(self):
        """Release cached Dask task results and trigger GC on workers AND the
        main process. Threaded-scheduler saves do their compute in main-process
        threads, so cleaning only the workers misses the dominant source of
        inter-rule memory accumulation on HR runs."""
        # Main process cleanup first — this is where threaded-scheduler saves
        # leak refs (dask graph, xarray Datasets, blosc thread-pool buffers).
        try:
            import gc as _gc

            _gc.collect()
            __import__("ctypes").CDLL("libc.so.6").malloc_trim(0)
        except Exception:
            pass
        # Worker-side cleanup (only relevant when compute runs on real workers,
        # e.g. non-lazy trigger_compute or synchronous save_dataset).
        if self._cluster is None:
            return
        try:
            client = Client.current()
        except ValueError:
            try:
                client = Client(self._cluster, set_as_default=False)
            except Exception:
                return
        try:
            import gc as _gc

            client.run(_gc.collect)
            client.run(lambda: __import__("ctypes").CDLL("libc.so.6").malloc_trim(0))
            logger.debug("Dask worker memory cleanup completed")
        except Exception as e:
            logger.debug(f"Dask worker cleanup skipped: {e}")

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
        # Whole-rule retry on transient dask/distributed errors.
        # Why manual loop instead of @task(retries=N): the decorator path
        # forces Prefect to hash the `rule` argument for retry-state
        # cache-key computation, which fails (HashError) on Rule objects
        # and triggered the cli33 regression (atm-tier slowdowns + OOM).
        # Curated transient list lifted from reverted commit fb639fa;
        # broader scope (whole pipeline, not just save) catches the
        # cli33 veg_land OSError that fires from lpjg_yearly_pipeline.
        max_attempts = int(os.environ.get("PYCMOR_RULE_RETRIES", "3"))
        rule_name = getattr(rule, "name", "unnamed")
        for attempt in range(max_attempts):
            try:
                logger.info(
                    f"Starting to process rule {rule}"
                    + (f" (attempt {attempt+1}/{max_attempts})" if attempt > 0 else "")
                )
                data = None
                if not len(rule.pipelines) > 0:
                    logger.error("No pipeline defined, something is wrong!")
                for pipeline in rule.pipelines:
                    logger.info(f"Running {str(pipeline)}")
                    data = pipeline.run(data, rule)
                    if isinstance(data, RuleSkipped):
                        logger.info(f"Rule {rule_name} ends without output: {data.reason}")
                        break
                # Don't ship the final dataset back to the scheduler/driver.
                # Under parallel/dask orchestration the caller does
                # client.gather(futures), which deserialises every rule's
                # return value into the driver process. Even if save_dataset
                # is the last step and "should" return None, intermediate
                # paths can leave a Dataset in `data`; with 50+ rules that
                # accumulates to tens of GB in the driver and OOMs the
                # cgroup before any worker hits its memory cap. Drop the
                # reference and return just the rule name so the gather
                # payload is tiny.
                del data
                return rule_name
            except Exception as exc:
                if attempt + 1 < max_attempts and _is_transient_compute_error(exc):
                    logger.warning(
                        f"Process rule {rule_name}: attempt {attempt+1}/{max_attempts} "
                        f"hit transient {type(exc).__name__}: {exc}; retrying in 30s"
                    )
                    time.sleep(30)
                    continue
                raise

    def _post_init_create_global_attributes_on_rules(self):
        """Create global attributes on rules using factory pattern."""
        GlobalAttributesClass = self._get_versioned_class(GlobalAttributes)
        for rule in self.rules:
            rule.create_global_attributes(GlobalAttributesClass)
