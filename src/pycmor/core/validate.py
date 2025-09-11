"""
Provides validation of user configuration files by checking against a schema.
"""

import glob
import importlib
import pathlib

from cerberus import Validator


class DirectoryAwareValidator(Validator):
    """
    A Validator that can check if a field is a directory.
    """

    def _validate_is_directory(self, is_directory, field, value):
        """
        Checks if a string can be a pathlib.Path object.

        The rule's arguments are validated against this schema:
        {'type': 'boolean'}
        """
        if is_directory:
            try:
                if glob.has_magic(value):
                    self._error(field, "Must not contain glob characters")
            except TypeError as e:
                self._error(field, f"{e.args[0]}. Must be a string")
            else:
                try:
                    pathlib.Path(value).expanduser().resolve()
                except TypeError as e:
                    self._error(field, f"{e.args[0]}. Must be a string")


class GeneralSectionValidator(DirectoryAwareValidator):
    """A Validator for the general section of the configuration file"""


class PipelineSectionValidator(Validator):
    """
    Validator for pipeline configuration.

    See Also
    --------
    * https://cerberus-sanhe.readthedocs.io/customize.html#class-based-custom-validators
    """

    def _validate_is_qualname_or_script(self, is_qualname, field, value):
        """Test if a string is a Python qualname.

        The rule's arguments are validated against this schema:
        {'type': 'boolean'}
        """
        if is_qualname and not isinstance(value, str):
            self._error(field, "Must be a string")
        if is_qualname:
            if value.startswith("script://"):
                script_path = value.replace("script://", "")
                script_path = script_path.rsplit(":", 1)[0]
                try:
                    pathlib.Path(script_path).expanduser().resolve()
                except TypeError as e:
                    self._error(field, f"{e.args[0]}. Must be a string")
                if not pathlib.Path(script_path).expanduser().resolve().is_file():
                    self._error(field, "Must be a valid file path")
            else:
                parts = value.split(".")
                module_name, attr_name = ".".join(parts[:-1]), parts[-1]
                try:
                    module = importlib.import_module(module_name)
                    if not hasattr(module, attr_name):
                        self._error(field, "Must be a valid Python qualname")
                except (ImportError, ModuleNotFoundError):
                    self._error(field, "Must be a valid Python qualname")

    def _validate(self, document):
        super()._validate(document)
        if "steps" not in document and "uses" not in document:
            self._error(
                "document", 'At least one of "steps" or "uses" must be specified'
            )


class RuleSectionValidator(DirectoryAwareValidator):
    """Validator for rules configuration."""


GENERAL_SCHEMA = {
    "general": {
        "type": "dict",
        "allow_unknown": True,
        "schema": {
            "cmor_version": {
                "type": "string",
                "required": True,
                "allowed": [
                    "CMIP6",
                    "CMIP7",
                ],
            },
            "CV_Dir": {
                "type": "string",
                "required": True,
                "is_directory": True,
            },
            "CMIP_Tables_Dir": {
                "type": "string",
                "required": True,
                "is_directory": True,
            },
        },
    },
}
"""dict : Schema for validating general configuration."""

GENERAL_VALIDATOR = GeneralSectionValidator(GENERAL_SCHEMA)
"""Validator : Validator for general configuration."""


PIPELINES_SCHEMA = {
    "pipelines": {
        "type": "list",
        "schema": {
            "type": "dict",
            "schema": {
                "name": {"type": "string", "required": False},
                "uses": {"type": "string", "excludes": "steps"},
                "steps": {
                    "type": "list",
                    "excludes": "uses",
                    "schema": {"type": "string", "is_qualname_or_script": True},
                },
            },
        },
    },
}
"""dict : Schema for validating pipelines configuration."""

PIPELINES_VALIDATOR = PipelineSectionValidator(PIPELINES_SCHEMA)
"""Validator : Validator for pipelines configuration."""

RULES_SCHEMA = {
    "rules": {
        "type": "list",
        "schema": {
            "type": "dict",
            "allow_unknown": True,
            "schema": {
                "name": {"type": "string", "required": False},
                "cmor_variable": {"type": "string", "required": True},
                "model_variable": {"type": "string", "required": False},
                "input_type": {
                    "type": "string",
                    "required": False,
                    "allowed": [
                        "xr.DataArray",
                        "xr.Dataset",
                    ],
                },
                "input_source": {
                    "type": "string",
                    "required": False,
                    "allowed": [
                        "xr_tutorial",
                    ],
                },
                "inputs": {
                    "type": "list",
                    "schema": {
                        "type": "dict",  # Each item in the list must be a dictionary
                        "schema": {  # Define the required keys in the dictionary
                            "path": {"type": "string", "required": True},
                            "pattern": {"type": "string", "required": True},
                            # Add more keys and their types as needed
                        },
                    },
                    "required": True,
                },
                "enabled": {"type": "boolean", "required": False},
                "description": {"type": "string", "required": False},
                "pipelines": {
                    "type": "list",
                    # FIXME(PG): Should cross-check with pipelines.
                    "schema": {"type": "string"},
                },
                "cmor_unit": {"type": "string", "required": False},
                "model_unit": {"type": "string", "required": False},
                "file_timespan": {"type": "string", "required": False},
                "variant_label": {
                    "type": "string",
                    "required": True,
                    "regex": r"^r\d+i\d+p\d+f\d+$",
                },
                "source_id": {"type": "string", "required": True},
                "output_directory": {
                    "type": "string",
                    "required": True,
                    "is_directory": True,
                },
                "instition_id": {"type": "string", "required": False},
                "experiment_id": {"type": "string", "required": True},
                "adjust_timestamp": {"type": "string", "required": False},
                "further_info_url": {"type": "string", "required": False},
                # "model_component" examples:
                # aerosol, atmos, land, landIce, ocnBgchem, ocean, seaIce
                "model_component": {"type": "string", "required": True},
                "grid_label": {"type": "string", "required": True},
                "array_order": {"type": "list", "required": False},
            },
        },
    },
}
"""dict : Schema for validating rules configuration."""
RULES_VALIDATOR = RuleSectionValidator(RULES_SCHEMA)
