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
    >>> engine = config("xarray_engine")
    >>> print(f"Using xarray backend: {engine}")
    Using xarray backend: h5netcdf
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
            default="yes",
            doc="Whether or not to issue a warning if no rule is found for every single DataRequestVariable",
            parser=_parse_bool,
        )
        xarray_default_missing_value = Option(
            default=1.0e30,
            doc="Which missing value to use for xarray. Default is 1e30.",
            parser=float,
        )
        xarray_open_mfdataset_engine = Option(
            default="netcdf4",
            doc="Which engine to use for xarray.open_mfdataset().",
            parser=ChoiceOf(
                str,
                choices=[
                    "netcdf4",
                    "h5netcdf",
                    "zarr",
                ],
            ),
        )
        xarray_open_mfdataset_parallel = Option(
            default="yes",
            doc=(
                "Whether to use parallel processing when opening multiple files "
                "with xarray.open_mfdataset(). Default is True."
            ),
            parser=_parse_bool,
        )
        xarray_skip_unit_attr_from_drv = Option(
            default="yes",
            doc="Whether to skip setting the unit attribute from the DataRequestVariable, this can be handled via Pint",
            parser=_parse_bool,
        )
        xarray_time_dtype = Option(
            default="float64",
            doc="The dtype to use for time axis in xarray.",
            parser=ChoiceOf(
                str,
                choices=[
                    "float64",
                    "datetime64[ns]",
                ],
            ),
        )
        xarray_time_enable_set_axis = Option(
            default="yes",
            doc="Whether to enable setting the axis for the time axis in xarray.",
            parser=_parse_bool,
        )
        xarray_time_remove_fill_value_attr = Option(
            default="yes",
            doc="Whether to remove the fill_value attribute from the time axis in xarray.",
            parser=_parse_bool,
        )
        xarray_time_set_long_name = Option(
            default="yes",
            doc="Whether to set the long name for the time axis in xarray.",
            parser=_parse_bool,
        )
        xarray_time_set_standard_name = Option(
            default="yes",
            doc="Whether to set the standard name for the time axis in xarray.",
            parser=_parse_bool,
        )
        xarray_time_taxis_str = Option(
            default="T",
            doc="Which axis to set for the time axis in xarray.",
            parser=str,
        )
        xarray_time_unlimited = Option(
            default="yes",
            doc="Whether the time axis is unlimited in xarray.",
            parser=_parse_bool,
        )
        netcdf_enable_chunking = Option(
            default="yes",
            doc="Whether to enable internal NetCDF chunking for optimized I/O performance.",
            parser=_parse_bool,
        )
        netcdf_chunk_algorithm = Option(
            default="simple",
            doc="Algorithm to use for calculating chunk sizes.",
            parser=ChoiceOf(
                str,
                choices=[
                    "simple",
                    "even_divisor",
                    "iterative",
                ],
            ),
        )
        netcdf_chunk_size = Option(
            default="100MB",
            doc="Target chunk size for NetCDF files. Can be specified as bytes (int) or string like '100MB'.",
            parser=str,
        )
        netcdf_chunk_tolerance = Option(
            default=0.5,
            doc="Tolerance for chunk size matching (0.0-1.0). Used by even_divisor and iterative algorithms.",
            parser=float,
        )
        netcdf_chunk_prefer_time = Option(
            default="yes",
            doc="Whether to prefer chunking along the time dimension for better I/O performance.",
            parser=_parse_bool,
        )
        netcdf_compression_level = Option(
            default=4,
            doc="Compression level for NetCDF files (1-9). Higher values give better compression but slower I/O.",
            parser=int,
        )
        netcdf_enable_compression = Option(
            default="yes",
            doc="Whether to enable zlib compression for NetCDF files.",
            parser=_parse_bool,
        )


class PycmorConfigManager(ConfigManager):
    """
    Custom ConfigManager for Pycmor, with a predefined hierarchy and
    support for injecting run-specific configuration.
    """

    _XDG_CONFIG_HOME = os.environ.get("XDG_CONFIG_HOME", "~/.config")
    """str : The XDG configuration directory."""
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
    def from_pycmor_cfg(cls, run_specific_cfg=None):
        """
        Create a PycmorConfigManager with the appropriate hierarchy.

        Parameters
        ----------
        run_specific_cfg : dict
            Optional. Overrides specific values for this run.
        """
        # Configuration higherarchy (highest to lowest priority):
        # 5. Command-line switches
        # Not implemented here
        # 4. Environment variables
        env_vars = ConfigOSEnv()
        # 3. Run-specific configuration
        run_specific = ConfigDictEnv(run_specific_cfg or {})

        # 2. User config file
        user_file = ConfigYamlEnv(cls._CONFIG_FILES)
        # 1. Hardcoded defaults
        # Handled by ``manager.with_options`` below

        # Combine everything into a new PycmorConfigManager instance
        manager = cls(
            environments=[user_file, run_specific, env_vars],
        )
        manager = manager.with_options(PycmorConfig)
        return manager

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
