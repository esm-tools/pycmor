"""
This module defines the configuration hierarchy for the pycmor application, using
``everett``'s ``~everett.manager.ConfigManager``. The configuration hierarchy is as follows (lowest to highest
priority):
    1. Hardcoded defaults
    2. User configuration file
    3. Run-specific configuration
    4. Environment variables
    5. Command-line switches

The configuration hierarchy is defined in the ``from_pycmor_cfg`` class method, and
cannot be modified outside the class. You should initialize a ``PycmorConfigManager``
object (probably in your ``CMORizer``) and grab config values from it by calling with the
config key as an argument.

User Configuration File
-----------------------

You can define global configuration options in a user configuration file. The files found at these
locations will be used, in highest to lowest priority order:
    1. ``${PYCMOR_CONFIG_FILE}``
    2. ``${XDG_CONFIG_HOME}/pycmor.yaml``
    3. ``${XDG_CONFIG_HOME}/pycmor/pycmor.yaml``
    4. ``~/.pycmor.yaml``

Note that the ``${XDG_CONFIG_HOME}`` environment variable defaults to ``~/.config`` if it is not set.

Configuration Options
---------------------

You can configure the following:

.. autocomponentconfig:: pycmor.core.config.PycmorConfig
   :case: upper
   :show-table:
   :namespace: pycmor

Usage
-----
Here are some examples of how to use the configuration manager::

    >>> from pycmor.core.config import PycmorConfigManager
    >>> pycmor_cfg = {}
    >>> config = PycmorConfigManager.from_pycmor_cfg(pycmor_cfg)
    >>> engine = config("xarray_open_mfdataset_engine")
    >>> print(f"Using xarray backend: {engine}")
    Using xarray backend: netcdf4
    >>> parallel = config("parallel")
    >>> print(f"Running in parallel: {parallel}")
    Running in parallel: True

You can define a user file at ``${XDG_CONFIG_DIR}/pycmor/pycmor.yaml``. Here's a
conceptual example (not executed in tests):

.. code-block:: python

    import pathlib
    import yaml
    cfg_file = pathlib.Path("~/.config/pycmor/pycmor.yaml").expanduser()
    cfg_file.parent.mkdir(parents=True, exist_ok=True)
    cfg_to_dump = {"xarray_engine": "zarr"}
    with open(cfg_file, "w") as f:
        yaml.dump(cfg_to_dump, f)
    config = PycmorConfigManager.from_pycmor_cfg()
    engine = config("xarray_engine")
    print(f"Using xarray backend: {engine}")
    # Using xarray backend: zarr

See Also
--------
- `Everett Documentation <https://everett.readthedocs.io/en/latest/>`_
"""

import os
import pathlib
from importlib.resources import files

from everett import InvalidKeyError
from everett.ext.yamlfile import ConfigYamlEnv
from everett.manager import ChoiceOf, ConfigDictEnv, ConfigManager, ConfigOSEnv, Option, _get_component_name, parse_bool

DIMENSIONLESS_MAPPING_TABLE = files("pycmor.data").joinpath("dimensionless_mappings.yaml")


def _parse_bool(value):
    if isinstance(value, bool):
        return value
    return parse_bool(value)


# Structured definition of xarray-related configuration options
# Format: Nested dict structure that becomes dot-separated or underscore-separated keys
XARRAY_OPTIONS = {
    "open_mfdataset": {
        "engine": {
            "default": "netcdf4",
            "doc": "Which engine to use for xarray.open_mfdataset().",
            "parser": ChoiceOf(str, choices=["netcdf4", "h5netcdf", "zarr"]),
        },
        "parallel": {
            "default": "no",
            "doc": (
                "Whether to use parallel file opening in xarray.open_mfdataset(). "
                "Note: requires thread-safe HDF5/NetCDF-C libraries. "
                "Use 'no' for safe sequential file opening (Dask still parallelizes computation)."
            ),
            "parser": _parse_bool,
        },
    },
    "default": {
        "dataarray": {
            "attrs": {
                "missing_value": {
                    "default": 1.0e30,
                    "doc": (
                        "Default missing value to use for xarray DataArray "
                        "attributes and encoding. Default is 1e30."
                    ),
                    "parser": float,
                },
            },
            "processing": {
                "skip_unit_attr_from_drv": {
                    "default": "yes",
                    "doc": (
                        "Whether to skip setting the unit attribute from the DataRequestVariable, "
                        "this can be handled via Pint"
                    ),
                    "parser": _parse_bool,
                },
            },
        },
    },
    "time": {
        "dtype": {
            "default": "float64",
            "doc": "The dtype to use for time axis in xarray.",
            "parser": ChoiceOf(str, choices=["float64", "datetime64[ns]"]),
        },
        "enable_set_axis": {
            "default": "yes",
            "doc": "Whether to enable setting the axis for the time axis in xarray.",
            "parser": _parse_bool,
        },
        "remove_fill_value_attr": {
            "default": "yes",
            "doc": "Whether to remove the fill_value attribute from the time axis in xarray.",
            "parser": _parse_bool,
        },
        "set_long_name": {
            "default": "yes",
            "doc": "Whether to set the long name for the time axis in xarray.",
            "parser": _parse_bool,
        },
        "set_standard_name": {
            "default": "yes",
            "doc": "Whether to set the standard name for the time axis in xarray.",
            "parser": _parse_bool,
        },
        "taxis_str": {
            "default": "T",
            "doc": "Which axis to set for the time axis in xarray.",
            "parser": str,
        },
        "unlimited": {
            "default": "yes",
            "doc": "Whether the time axis is unlimited in xarray.",
            "parser": _parse_bool,
        },
    },
}


def _flatten_nested_dict(nested_dict, parent_key="", sep="_"):
    """
    Flatten a nested dictionary into dot-separated and underscore-separated keys.

    Parameters
    ----------
    nested_dict : dict
        Nested dictionary to flatten
    parent_key : str
        Parent key for recursion
    sep : str
        Separator for keys (default: '_')

    Yields
    ------
    tuple
        (flat_key, spec_dict) where flat_key is underscore-separated
        and spec_dict contains 'default', 'doc', 'parser'
    """
    for key, value in nested_dict.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key

        # Check if this is a leaf node (has 'default' key)
        if isinstance(value, dict) and "default" in value:
            # This is a leaf - it's an option spec
            yield (new_key, value)
        elif isinstance(value, dict):
            # This is a branch - recurse deeper
            yield from _flatten_nested_dict(value, new_key, sep=sep)


def _make_xarray_option(key_path, spec):
    """
    Factory to create xarray Option with dotted alternate key.

    Parameters
    ----------
    key_path : str
        Underscore-separated key path (e.g., "default_dataarray_attrs_missing_value")
    spec : dict
        Option specification with default, doc, parser

    Returns
    -------
    Option
        Configured Option with alternate_keys for backward compatibility
    """
    # Create dotted notation for YAML nested structure
    dotted_key = f"xarray.{key_path.replace('_', '.')}"
    return Option(
        default=spec["default"],
        doc=f"{spec['doc']} (Dotted key: {dotted_key})",
        parser=spec.get("parser"),
        alternate_keys=[dotted_key],
    )


def _generate_xarray_options(cls):
    """
    Dynamically add xarray options to Config class.

    This decorator generates Option attributes for all xarray-related
    configuration based on the XARRAY_OPTIONS structure, supporting
    arbitrary nesting depth.
    """
    for key_path, option_spec in _flatten_nested_dict(XARRAY_OPTIONS):
        # Create attribute name: xarray_<flattened_path>
        attr_name = f"xarray_{key_path}"
        option = _make_xarray_option(key_path, option_spec)
        setattr(cls.Config, attr_name, option)
    return cls


@_generate_xarray_options
class PycmorConfig:
    class Config:
        # [FIXME] Keep the list of all options alphabetical!
        dask_cluster = Option(
            default="local",
            doc="Dask cluster to use. See: https://docs.dask.org/en/stable/deploying.html",
            parser=ChoiceOf(
                str,
                choices=[
                    "local",
                    "slurm",
                ],
            ),
        )
        dask_cluster_scaling_fixed_jobs = Option(
            default=5,
            doc="Number of jobs to create for Jobqueue-backed Dask Cluster",
            parser=int,
        )
        dask_cluster_scaling_maximum_jobs = Option(
            default=10,
            doc="Maximum number of jobs to create for Jobqueue-backed Dask Clusters (adaptive)",
            parser=int,
        )
        dask_cluster_scaling_minimum_jobs = Option(
            default=1,
            doc="Minimum number of jobs to create for Jobqueue-backed Dask Clusters (adaptive)",
            parser=int,
        )
        dask_cluster_scaling_mode = Option(
            default="adapt",
            doc="Flexible dask cluster scaling",
            parser=ChoiceOf(
                str,
                choices=[
                    "adapt",
                    "fixed",
                ],
            ),
        )
        dimensionless_mapping_table = Option(
            default=DIMENSIONLESS_MAPPING_TABLE,
            doc="Where the dimensionless unit mapping table is defined.",
            parser=str,
        )
        enable_dask = Option(
            default="yes",
            doc="Whether to enable Dask-based processing",
            parser=_parse_bool,
        )
        enable_flox = Option(
            default="yes",
            doc="Whether to enable flox for group-by operation. See: https://flox.readthedocs.io/en/latest/",
            parser=_parse_bool,
        )
        enable_output_subdirs = Option(
            default="no",
            doc="Whether to create subdirectories under output_dir when saving data-sets.",
            parser=_parse_bool,
        )
        file_timespan = Option(
            default="1YS",
            doc="""Default timespan for grouping output files together.

            Use the special flag ``'file_native'`` to use the same grouping as in the input
            files. Otherwise, use a ``pandas``-flavoured string, see: https://tinyurl.com/38wxf8px
            """,
            parser=str,
        )
        parallel = Option(
            default="yes",
            doc="Whether to run in parallel.",
            parser=_parse_bool,
        )
        parallel_backend = Option(
            default="dask",
            doc="Which parallel backend to use.",
        )
        pipeline_workflow_orchestrator = Option(
            default="prefect",
            doc="Which workflow orchestrator to use for running pipelines",
            parser=ChoiceOf(
                str,
                choices=[
                    "native",
                    "prefect",
                ],
            ),
        )
        prefect_task_runner = Option(
            default="thread_pool",
            doc="Which runner to use for Prefect flows.",
            parser=ChoiceOf(
                str,
                choices=[
                    "thread_pool",
                    "dask",
                ],
            ),
        )
        quiet = Option(
            default=False,
            doc="Whether to suppress output.",
            parser=_parse_bool,
        )
        raise_on_no_rule = Option(
            default="no",
            doc="Whether or not to raise an error if no rule is found for every single DataRequestVariable",
            parser=_parse_bool,
        )
        warn_on_no_rule = Option(
            default="no",
            doc="Whether or not to issue a warning if no rule is found for every single DataRequestVariable",
            parser=_parse_bool,
        )
        # NOTE: xarray_* options are dynamically generated by @_generate_xarray_options decorator
        # See XARRAY_OPTIONS structure above for definitions


class PycmorConfigManager(ConfigManager):
    """
    Custom ConfigManager for Pycmor, with a predefined hierarchy and
    support for injecting run-specific configuration.
    """

    _XDG_CONFIG_HOME = os.environ.get("XDG_CONFIG_HOME", "~/.config")
    """str : The XDG configuration directory."""
    _NAMESPACE = "pycmor"
    """str : The namespace for all configuration keys."""
    _CONFIG_FILES = [
        str(f)
        for f in [
            # Prefer new env var, fall back to legacy
            os.environ.get("PYCMOR_CONFIG_FILE") or os.environ.get("PYMOR_CONFIG_FILE"),
            # Prefer new locations
            pathlib.Path(f"{_XDG_CONFIG_HOME}/pycmor.yaml").expanduser(),
            pathlib.Path(f"{_XDG_CONFIG_HOME}/pycmor/pycmor.yaml").expanduser(),
            pathlib.Path("~/.pycmor.yaml").expanduser(),
            # Legacy fallbacks
            pathlib.Path(f"{_XDG_CONFIG_HOME}/pymor.yaml").expanduser(),
            pathlib.Path(f"{_XDG_CONFIG_HOME}/pymor/pymor.yaml").expanduser(),
            pathlib.Path("~/.pymor.yaml").expanduser(),
        ]
        if f
    ]
    """List[str] : The list of configuration files to check for user configuration."""

    @classmethod
    def _create_environments(cls, run_specific_cfg=None):
        """
        Build the environment stack in priority order (highest first).

        Parameters
        ----------
        run_specific_cfg : dict, optional
            Run-specific configuration overrides.

        Returns
        -------
        list
            List of environment objects in priority order (first has highest priority).
        """
        return [
            ConfigOSEnv(),  # Highest: Environment variables
            ConfigDictEnv(run_specific_cfg or {}),  # Run-specific configuration
            ConfigYamlEnv(cls._CONFIG_FILES),  # Lowest: User config file
        ]

    @classmethod
    def _configure_manager(cls, manager):
        """
        Apply namespace and options to manager.

        Parameters
        ----------
        manager : PycmorConfigManager
            The manager instance to configure.

        Returns
        -------
        PycmorConfigManager
            The configured manager with namespace and options applied.
        """
        return manager.with_namespace(cls._NAMESPACE).with_options(PycmorConfig)

    @classmethod
    def from_pycmor_cfg(cls, run_specific_cfg=None):
        """
        Create a fully configured PycmorConfigManager.

        Parameters
        ----------
        run_specific_cfg : dict, optional
            Run-specific configuration overrides.

        Returns
        -------
        PycmorConfigManager
            Fully configured manager instance.
        """
        environments = cls._create_environments(run_specific_cfg)
        manager = cls(environments=environments)
        return cls._configure_manager(manager)

    # NOTE(PG): Need to override this method, the original implementation in the parent class
    # explicitly uses ConfigManager (not cls) to create the clone instance.
    def clone(self):
        my_clone = PycmorConfigManager(
            environments=list(self.envs),
            doc=self.doc,
            msg_builder=self.msg_builder,
            with_override=self.with_override,
        )
        my_clone.namespace = list(self.namespace)
        my_clone.bound_component = self.bound_component
        my_clone.bound_component_prefix = []
        my_clone.bound_component_options = self.bound_component_options

        my_clone.original_manager = self.original_manager

        return my_clone

    def __repr__(self) -> str:
        if self.bound_component:
            name = _get_component_name(self.bound_component)
            return f"<PycmorConfigManager({name}): namespace:{self.get_namespace()}>"
        else:
            return f"<PycmorConfigManager: namespace:{self.get_namespace()}>"

    def get(self, key, default=None, parser=None):
        """
        Get a configuration value by key, with a default value.

        Parameters
        ----------
        key : str
            The configuration key to get.
        default : Any
            The default value to return if the key is not found.
        parser : Callable
            Optional. A callable to parse the configuration value.

        Returns
        -------
        Any
            The configuration value.
        """
        try:
            return self(key, parser=parser)
        except InvalidKeyError:
            return default


# ---------------------------------------------------------------------------
# Configuration injection decorator
# ---------------------------------------------------------------------------


def config_injector(config_manager=None, type_to_prefix_map=None):
    """
    Decorator that automatically injects config values into function calls based on parameter types.

    This creates "dynamic partial functions" where the config system fills in arguments
    automatically based on type annotations. If a parameter has a type annotation like
    ``xarray.DataArray``, the decorator will look for config keys matching the pattern
    ``xarray_<type_name>_<param_name>`` and inject those values if they exist.

    Parameters
    ----------
    config_manager : PycmorConfigManager, optional
        The config manager to use. If None, creates one with from_pycmor_cfg()
    type_to_prefix_map : dict, optional
        Mapping from type objects to config key prefixes.
        Example: {xr.DataArray: "xarray_default_dataarray"}

    Returns
    -------
    decorator
        A decorator that injects config values into function calls

    Examples
    --------
    >>> import xarray as xr
    >>> from pycmor.core.config import config_injector
    >>>
    >>> # Define type mapping
    >>> type_map = {xr.DataArray: "xarray_default_dataarray"}
    >>>
    >>> @config_injector(type_to_prefix_map=type_map)
    ... def process_data(data: xr.DataArray, attrs_missing_value: float = None):
    ...     # If attrs_missing_value not provided, decorator injects from config:
    ...     # xarray_default_dataarray_attrs_missing_value
    ...     print(f"Missing value: {attrs_missing_value}")
    ...     return data
    >>>
    >>> # Call without providing attrs_missing_value - it gets injected from config
    >>> result = process_data(my_data)  # Uses config value
    >>> # Or override it
    >>> result = process_data(my_data, attrs_missing_value=999)  # Uses 999
    """
    import functools
    import inspect

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get config manager
            cfg = config_manager or PycmorConfigManager.from_pycmor_cfg()

            # Get function signature
            sig = inspect.signature(func)

            # Build a mapping of parameter names to their positions
            param_names = list(sig.parameters.keys())

            # Determine which parameters were provided
            provided_params = set()
            for i, arg in enumerate(args):
                if i < len(param_names):
                    provided_params.add(param_names[i])
            provided_params.update(kwargs.keys())

            # Find which type prefix to use by looking at parameter type annotations
            active_prefix = None
            if type_to_prefix_map:
                for param in sig.parameters.values():
                    if param.annotation in type_to_prefix_map:
                        active_prefix = type_to_prefix_map[param.annotation]
                        break

            # Build new kwargs by injecting config values
            new_kwargs = dict(kwargs)

            # If we found a matching type, inject config for all unprovided parameters
            if active_prefix:
                for param_name, param in sig.parameters.items():
                    # Skip if already provided
                    if param_name in provided_params:
                        continue

                    # Skip if no type annotation (e.g., *args, **kwargs)
                    if param.annotation is inspect.Parameter.empty:
                        continue

                    # Build config key
                    config_key = f"{active_prefix}_{param_name}"

                    # Try to get value from config
                    try:
                        value = cfg(config_key)
                        new_kwargs[param_name] = value
                    except InvalidKeyError:
                        # Key doesn't exist in config, skip (let default handle it)
                        pass

            return func(*args, **new_kwargs)

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# Backward compatibility aliases (to be removed in a future release)
# ---------------------------------------------------------------------------
PymorConfig = PycmorConfig
PymorConfigManager = PycmorConfigManager

# Legacy constructor compatibility
setattr(
    PycmorConfigManager,
    "from_pymor_cfg",
    classmethod(lambda cls, run_specific_cfg=None: cls.from_pycmor_cfg(run_specific_cfg)),
)
