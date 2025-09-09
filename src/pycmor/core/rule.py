import copy
import datetime
import pathlib
import re
import typing
import warnings

import yaml

from ..data_request.table import DataRequestTable
from ..data_request.variable import DataRequestVariable
from . import pipeline
from .gather_inputs import InputFileCollection
from .logging import logger


class Rule:
    def __init__(
        self,
        *,
        name: str = None,
        inputs: typing.List[dict] = None,
        cmor_variable: str,
        pipelines: typing.List[pipeline.Pipeline] = None,
        tables: typing.List[DataRequestTable] = None,
        data_request_variables: typing.List[DataRequestVariable] = None,
        **kwargs,
    ):
        """
        Initialize a Rule object.

        This method can only be called with keyword arguments.

        Parameters
        ----------
        inputs : list of dicts for InputFileCollection
            Dictionaries should contain the keys "path" and "pattern".
        cmor_variable : str
            The CMOR variable name. This is the name of the variable as it should appear in the CMIP archive.
        pipelines : list of Pipeline objects
            A list of Pipeline objects that define the transformations to be applied to the data.
        tables : list of DataRequestTable objects
            A list of data request tables associated with this rule
        data_request_variables : DataRequestVariable or None :
            The DataRequestVariables this rule should create
        """
        self.name = name
        self.inputs = [
            InputFileCollection.from_dict(inp_dict) for inp_dict in (inputs or [])
        ]
        self.cmor_variable = cmor_variable
        self.pipelines = pipelines or [pipeline.DefaultPipeline()]
        self.tables = tables or []
        self.data_request_variables = data_request_variables or []
        # NOTE(PG): I'm not sure I really like this part. It is too magical and makes the object's public API unclear.
        # Attach all keyword arguments to the object
        for key, value in kwargs.items():
            setattr(self, key, value)

        # Internal flags:
        self._pipelines_are_mapped = False

    def __getstate__(self):
        """Custom pickling of a Rule"""
        state = self.__dict__.copy()
        return state

    def get(self, key, default=None):
        """Gets an attribute from the Rule object

        Useful for passing the Rule object to other functions that may not know the
        current structure, e.g. when calling Pipeline steps.

        Parameters
        ----------
        key : str
            The name of the attribute to get.
        default : Any, optional
            The value to return if the attribute does not exist.

        Returns
        -------
        value : Any
            The value of the attribute, or the default value if the attribute does not exist.
        """
        return getattr(self, key, default)

    def set(self, key, value, force=False, warn=True):
        """
        Set a new attribute for the object.

        Parameters
        ----------
        key : str
            The name of the attribute to set.
        value : Any
            The value to set for the attribute.
        force : bool, optional
            If True, the attribute will be overwritten if it already exists.
            If False (default), an AttributeError will be raised if the attribute already exists.
        warn : bool, optional
            If True (default) a warning will be issued if the attribute already exists, and
            it will not be overwritten. If False, an AttributeError will be raised if the attribute
            already exists.

        Returns
        -------
        value : Any
            Returns the value appended to the object. This is the same behaviour as setattr.

        Raises
        ------
        AttributeError
            If the attribute already exists and force and warn are both False.
        """
        if hasattr(self, key) and not force:
            if warn:
                warnings.warn(
                    f"Attribute {key} already exists. Use force=True to overwrite."
                )
            else:
                raise AttributeError(
                    f"Attribute {key} already exists. Use force=True to overwrite."
                )
        return setattr(self, key, value)

    def __str__(self):
        return f"Rule for {self.cmor_variable} with input patterns {self.input_patterns} and pipelines {self.pipelines}"

    def match_pipelines(self, pipelines, force=False):
        """
        Match the pipelines in the rule with the pipelines in the configuration. The pipelines
        should be a list of pipeline instances that can be matched with the rule's required pipelines.

        Parameters
        ----------
        list : list of pipeline.Pipeline
            Available pipelines to use
        force : bool, optional
            If True, the pipelines will be remapped even if they were already mapped.

        Mutates
        -------
        self.pipelines : list of str --> list of pipeline.Pipeline objects
            ``self.pipelines`` will be replaced from a list of strings to a list of
            Pipeline objects. The order of the pipelines will be preserved.
        """
        if self._pipelines_are_mapped and not force:
            logger.debug("Pipelines already mapped, nothing to do")
            return self.pipelines
        known_pipelines = {p.name: p for p in pipelines}
        logger.debug("The following pipelines are known:")
        for pl_name, pl in known_pipelines.items():
            logger.debug(f"{pl_name}: {pl}")
        matched_pipelines = list()
        for pl in self.pipelines:
            logger.debug(f"Working on: {pl}")
            # Pipeline was already matched
            if isinstance(pl, pipeline.Pipeline):
                matched_pipelines.append(pl)
            elif isinstance(pl, str):
                # Pipeline name:
                matched_pipelines.append(known_pipelines[pl])
            else:
                logger.error(f"No known way to match the pipeline {pl}")
                raise TypeError(f"{pl} must be a string or a pipeline.Pipeline object!")
        self.pipelines = matched_pipelines
        self._pipelines_are_mapped = True

    @classmethod
    def from_dict(cls, data):
        """Build a rule object from a dictionary

        The dictionary should have the following keys: "inputs", "cmor_variable",
        "pipelines". Note that the ``"inputs"`` key should contain a list of dictionaries
        that can be used to build InputFileCollection objects. The ``"pipelines"`` key
        should contain a list of dictionaries that can be used to build Pipeline objects, and
        the ``cmor_variable`` is just a string.

        Parameters
        ----------
        data : dict
            A dictionary containing the rule data.
        """
        return cls(
            name=data.pop("name", None),
            inputs=data.pop("inputs"),
            cmor_variable=data.pop("cmor_variable"),
            pipelines=data.pop("pipelines", []),
            **data,
        )

    @classmethod
    def from_yaml(cls, yaml_str):
        """Wrapper around ``from_dict`` for initializing from YAML"""
        return cls.from_dict(yaml.safe_load(yaml_str))

    def add_table(self, tbl):
        """Add a table to the rule"""
        self.tables.append(tbl)
        self.tables = [t for t in self.tables if t is not None]

    def remove_table(self, tbl):
        """Remove a table from the rule"""
        self.tables.remove(tbl)

    def add_input(self, inp_dict):
        """Add an input collection to the rule."""
        self.inputs.append(InputFileCollection.from_dict(inp_dict))

    def add_data_request_variable(self, drv):
        """Add a data request variable to the rule."""
        self.data_request_variables.append(drv)
        # Filter out Nones
        self.data_request_variables = [
            v for v in self.data_request_variable if v is not None
        ]

    def remove_data_request_variable(self, drv):
        """Remove a data request variable from the rule."""
        self.data_request_variables.remove(drv)

    @property
    def input_patterns(self):
        """Return a list of compiled regex patterns for the input files."""
        return [re.compile(f"{inp.path}/{inp.pattern}") for inp in self.inputs]

    def clone(self):
        """Creates a copy of this rule object as it is currently configured."""
        return copy.deepcopy(self)

    def expand_drvs(self):
        """
        Depluralize the rule by creating a new rule for each DataRequestVariable.

        This method clones the current rule object for each DataRequestVariable (``drv``) it contains.
        For each cloned rule, it also clones the corresponding drv and sets its tables, frequencies,
        cell_methods, and cell_measures attributes to the individual elements from the original drv.
        The cloned drv is then set as the only drv of the cloned rule. The method returns a list of all
        these cloned rules.

        Returns
        -------
        list
            A list of cloned rule objects, each containing a single DataRequestVariable.
        """
        clones = []
        for drv in self.data_request_variables:
            rule_clone = self.clone()
            drv_clone = drv.clone()
            # FIXME: This is bad. I need to extract one rule for each table,
            # but the newer API doesn't work as cleanly here...
            rule_clone.data_request_variables = [drv_clone]
            clones.append(rule_clone)
        return clones

    def depluralize_drvs(self):
        """Depluralizes Data Request Variables to just a single entry"""
        assert len(self.data_request_variables) == 1
        self.data_request_variable = self.data_request_variables[0]
        del self.data_request_variables

    def global_attributes_set_on_rule(self):
        attrs = (
            "source_id",
            "grid_label",
            "cmor_variable",
            "variant_label",
            "experiment_id",
            "activity_id",  # optional
            "institution_id",  # optional
            "model_component",  # optional
            "further_info_url",  # optional
        )
        # attribute `creation_date` is the time-stamp of inputs directory
        try:
            afile = next(
                f for file_collection in self.inputs for f in file_collection.files
            )
            afile = pathlib.Path(afile)
            dir_timestamp = datetime.datetime.fromtimestamp(
                afile.parent.stat().st_ctime
            )
        except FileNotFoundError:
            # No input files, so use the current time -- this is a fallback triggered for test cases
            dir_timestamp = datetime.datetime.now()
        time_format = "%Y-%m-%dT%H:%M:%SZ"
        creation_date = dir_timestamp.strftime(time_format)
        result = {attr: getattr(self, attr, None) for attr in attrs}
        result["creation_date"] = creation_date
        return result

    def create_global_attributes(self, GlobalAttributesClass):
        self.ga = GlobalAttributesClass(
            self.data_request_variable,
            self.controlled_vocabularies,
            self.global_attributes_set_on_rule(),
        )
