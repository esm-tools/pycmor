"""
Functionality for gathering possible inputs from a user directory
"""

import os
import pathlib
import re
from typing import List

import deprecation
import dpath
import numpy as np
import xarray as xr

from .filecache import register_cache  # noqa: F401
from .logging import logger

# Prefer new pycmor keys; keep legacy pymor as fallback
_PATTERN_ENV_VAR_NAME_ADDRS = [
    "/pycmor/pattern_env_var_name",
    "/pymor/pattern_env_var_name",
]
"""list[str]: Addresses in the YAML file for the env var name used for the pattern (new, legacy)."""
_PATTERN_ENV_VAR_NAME_DEFAULTS = [
    "PYCMOR_INPUT_PATTERN",
    "PYMOR_INPUT_PATTERN",
]
"""list[str]: Defaults for env var name (new, legacy)."""
_PATTERN_ENV_VAR_VALUE_ADDRS = [
    "/pycmor/pattern_env_var_value",
    "/pymor/pattern_env_var_value",
]
"""list[str]: Addresses in the YAML file for the env var value (new, legacy)."""
_PATTERN_ENV_VAR_VALUE_DEFAULT = ".*"  # Default: match anything
"""str: Default value for the environment variable's value to be used if not set."""


class InputFileCollection:
    def __init__(self, path, pattern, frequency=None, time_dim_name=None):
        self.path = pathlib.Path(path)
        self.pattern_str = pattern  # Store original pattern string
        try:
            self.pattern = re.compile(pattern)  # Compile the regex pattern
        except re.error:
            # Pattern may be a glob (e.g. LPJ-GUESS "*/run1/*.out") — not valid regex.
            # Store None; pipelines using glob will read pattern_str directly.
            self.pattern = None
        self.frequency = frequency
        self.time_dim_name = time_dim_name

    @property
    def files(self):
        if self.pattern is None:
            # Glob-style pattern — use pathlib.glob instead of regex
            return sorted(self.path.glob(self.pattern_str))
        files = []
        for file in list(self.path.iterdir()):
            if self.pattern.match(file.name):  # Check if the filename matches the pattern
                files.append(file)
        return files

    @classmethod
    def from_dict(cls, d):
        return cls(d["path"], d["pattern"], d.get("frequency"), d.get("time_dim_name"))


def _input_pattern_from_env(config: dict) -> re.Pattern:
    """
    Get the input pattern from the environment variable.

    This function retrieves the name of the environment variable from the configuration dictionary
    using the dpath library. It then gets the value of this environment variable, which is expected
    to be a regular expression pattern. This pattern is then compiled and returned.

    Parameters
    ----------
    config : dict
        The configuration dictionary. This dictionary should contain the keys
        `pattern_env_var_name` and `pattern_env_value_default`, which are used to locate
        the environment variable name and default value respectively. If not gives, these default
        Prefer `PYCMOR_INPUT_PATTERN` and `.*` respectively. Legacy `PYMOR_INPUT_PATTERN` is also supported.

    Returns
    -------
    re.Pattern
        The compiled regular expression pattern.

    Examples
    --------
    >>> config_bare = { "pycmor": {} }
    >>> config_only_env_name = {
    ...     "pycmor": {
    ...         'pattern_env_var_name': 'CMOR_PATTERN',
    ...     }
    ... }
    >>> config_only_env_value = {
    ...     "pymor": {
    ...         'pattern_env_var_default': 'test*nc',
    ...   }
    ... }
    >>> pattern = _input_pattern_from_env(config_bare)
    >>> pattern
    re.compile('.*')
    >>> bool(pattern.match('test'))
    True
    >>> os.environ["CMOR_PATTERN"] = "test*nc"
    >>> pattern = _input_pattern_from_env(config_only_env_name)
    >>> pattern
    re.compile('test*nc')
    >>> bool(pattern.match('test'))
    False
    >>> del os.environ["CMOR_PATTERN"]
    >>> pattern = _input_pattern_from_env(config_only_env_value)
    >>> pattern
    re.compile('.*')
    >>> bool(pattern.match('test'))
    True
    """
    # Resolve env var name, preferring pycmor key and default but falling back to legacy
    env_var_name = None
    for addr, default in zip(_PATTERN_ENV_VAR_NAME_ADDRS, _PATTERN_ENV_VAR_NAME_DEFAULTS):
        try:
            env_var_name = dpath.get(config, addr)
            if env_var_name:
                break
        except KeyError:
            # not present; try next
            env_var_name = env_var_name or default
    # Resolve env var value default from config (new first, then legacy)
    env_var_default = None
    for addr in _PATTERN_ENV_VAR_VALUE_ADDRS:
        try:
            env_var_default = dpath.get(config, addr)
            if env_var_default is not None:
                break
        except KeyError:
            continue
    if env_var_default is None:
        env_var_default = _PATTERN_ENV_VAR_VALUE_DEFAULT
    env_var_value = os.getenv(env_var_name, env_var_default)
    return re.compile(env_var_value)


def _input_files_in_path(path: pathlib.Path or str, pattern: re.Pattern) -> list:
    """
    Get a list of files in a directory that match a pattern.

    This function takes a directory path and a regular expression pattern. It then
    returns a list of all files in the directory that match the pattern.

    Parameters
    ----------
    path : pathlib.Path or str
        The path to the directory to search for files.

    pattern : re.Pattern

    Returns
    -------
    list
        A list of files in the directory that match the pattern.
    """
    path = pathlib.Path(path)
    return [f for f in path.iterdir() if f.is_file() and pattern.match(f.name)]


def _resolve_symlinks(files: List[pathlib.Path]) -> List[pathlib.Path]:
    """
    Filters out symbolic links from a list of pathlib.Path objects.

    Parameters
    ----------
    files : list
        A list of pathlib.Path objects.

    Returns
    -------
    list
        A list of pathlib.Path objects excluding any symbolic links.

    Raises
    ------
    TypeError
        If any element in the input list is not a pathlib.Path object.

    Examples
    --------
    >>> from pathlib import Path
    >>> files = [Path('/path/to/file1'), Path('/path/to/file2')]
    >>> paths = _resolve_symlinks(files)
    >>> [str(p) for p in paths]  # Convert to strings for doctest
    ['/path/to/file1', '/path/to/file2']
    """
    if not all(isinstance(f, pathlib.Path) for f in files):
        logger.error("All files must be pathlib.Path objects. Got the following:")
        for f in files:
            logger.error(f"{f} {type(f)}")
        raise TypeError("All files must be pathlib.Path objects")
    return [f.resolve() if f.is_symlink() else f for f in files]


def _filter_by_year(
    files: List[pathlib.Path], fpattern: re.Pattern, year_start: int, year_end: int
) -> List[pathlib.Path]:
    """
    Filters a list of files by the year in their name.

    Parameters
    ----------
    files : list of pathlib.Path
        A list of files to filter.
    fpattern : re.Pattern
        The regular expression pattern to match the files.
    year_start : int
        The start year to filter by.
    year_end : int
        The end year to filter by.
    """
    return [f for f in files if year_start <= int(fpattern.match(f.name).group("year")) <= year_end]


def _sort_by_year(files: List[pathlib.Path], fpattern: re.Pattern) -> List[pathlib.Path]:
    """
    Sorts a list of files by the year in their name.
    """
    return sorted(files, key=lambda f: int(fpattern.match(f.name).group("year")))


def _files_to_string(files: List[pathlib.Path], sep=",") -> str:
    """
    Converts a list of pathlib.Path objects to a string.

    Parameters
    ----------
    files : list
        A list of pathlib.Path objects.
    sep : str
        The separator to use between the paths. Defaults to a comma.

    Returns
    -------
    str
        A string representation of the list of files.
    """
    return sep.join(str(f) for f in files)


def _validate_rule_has_marked_regex(rule: dict, required_marks: List[str] = ["year"]) -> bool:
    """
    Validates that a rule has a marked regular expression.

    This function takes a rule dictionary and a list of required marks. It then checks that
    the rule has a regular expression pattern that has been marked with all of the required marks.

    Parameters
    ----------
    rule : dict
        The rule dictionary.
    required_marks : list
        A list of strings representing the required marks.

    Returns
    -------
    bool
        True if the rule has a marked regular expression, False otherwise.

    Examples
    --------
    >>> rule = { 'pattern': 'test(?P<year>[0-9]{4})' }
    >>> _validate_rule_has_marked_regex(rule)
    True
    >>> rule = { 'pattern': 'test' }
    >>> _validate_rule_has_marked_regex(rule)
    False
    """
    pattern = rule.get("pattern")
    if pattern is None:
        return False
    return all(re.search(rf"\(\?P<{mark}>", pattern) for mark in required_marks)


def _filter_files_by_year_range(files, year_start, year_end):
    """
    Filter files whose year range overlaps with [year_start, year_end].

    Extracts all 4-digit numbers from each filename and checks if any
    fall within the requested range. Filenames like ``var_1900-1905.nc``
    will match if any year in their range overlaps.

    Parameters
    ----------
    files : list of pathlib.Path
        Files to filter.
    year_start : int
        First year to include.
    year_end : int
        Last year to include.

    Returns
    -------
    list of pathlib.Path
        Filtered and sorted list of files.
    """
    year_pattern = re.compile(r"\d{4}")
    filtered = []
    for f in files:
        years = [int(y) for y in year_pattern.findall(f.name)]
        if not years:
            # No years in filename — include to be safe
            filtered.append(f)
            continue
        file_start = min(years)
        file_end = max(years)
        # Include if the file's year range overlaps with the requested range
        if file_start <= year_end and file_end >= year_start:
            filtered.append(f)
    return sorted(filtered, key=lambda f: f.name)


def _check_compatible_schemas(files, rule_spec):
    """Fail fast when files in the same gather have incompatible primary dims.

    ``open_mfdataset`` lazily concatenates the file list; when two files share
    a coordinate name but disagree on its size (e.g. a native unstructured
    ``nod2`` file and a regridded ``lat``/``lon`` variant both matched by a
    loose ``.*`` pattern), xarray's ``merge_collected`` tries to broadcast-
    equate the coords and blows up with a multi-petabyte allocation request
    inside ``dask.tokenize``. Reading just the headers up front turns that
    failure into an actionable error.

    Opt-out via ``skip_input_schema_check: true`` on the rule when files
    legitimately differ (e.g. concatenating an areacello fx with monthly
    data — though that combination would normally use separate steps).
    """
    if len(files) < 2:
        return
    if rule_spec.get("skip_input_schema_check", False):
        return
    try:
        reference = None
        ref_path = None
        for f in files:
            with xr.open_dataset(f, decode_times=False, engine="netcdf4") as ds:
                dims = {k: int(v) for k, v in ds.sizes.items() if k != "time"}
            if reference is None:
                reference = dims
                ref_path = f
                continue
            for k, v in dims.items():
                if k in reference and reference[k] != v:
                    raise ValueError(
                        "input file list has incompatible schemas. "
                        f"dim '{k}' = {reference[k]} in {ref_path} "
                        f"but = {v} in {f}. tighten the rule's input "
                        "pattern to one grid family, or set "
                        "skip_input_schema_check: true on the rule if "
                        "this is intentional."
                    )
            for k, v in dims.items():
                reference.setdefault(k, v)
    except (OSError, FileNotFoundError) as exc:
        logger.warning(f"schema pre-check skipped: {exc}")


def filter_files_by_year_range(files, year_start, year_end):
    """Public year-range filter. Accepts paths or strings.

    Wraps :func:`_filter_files_by_year_range` for use from step functions
    that resolve secondary input lists (e.g. ``second_input_pattern``,
    ``hnode_pattern``, ``salt_pattern``). Returns the same element type as
    the input list.
    """
    import pathlib as _pl

    files = list(files)
    return_str = bool(files) and isinstance(files[0], str)
    paths = [_pl.Path(f) for f in files]
    filtered = _filter_files_by_year_range(paths, int(year_start), int(year_end))
    if return_str:
        return [str(p) for p in filtered]
    return filtered


def load_mfdataset(data, rule_spec):
    """
    Load a dataset from a list of files using xarray.

    Optional perf tuning (default off, opt-in via rule attrs or
    pycmor config keys; see OPTIMIZATION_PLAN.md round 1):

    - ``xarray_open_mfdataset_engine_override`` (str): override the
      backend engine per-rule, e.g. ``"h5netcdf"``. h5netcdf is
      "often faster" than the default netcdf4 backend for
      ``open_mfdataset`` per the xarray docs, especially with many
      small chunks.

    - ``xarray_open_mfdataset_inline_array`` (bool): pass
      ``inline_array=True`` to ``xr.open_mfdataset``. Compacts the
      dask task graph by inlining chunks as values rather than
      separate task references — useful when the input has many
      small chunks (XIOS outputs at 5840–8760 chunks/file).

    NOTE: HDF5 chunk-cache tuning (``rdcc_nbytes``) was investigated
    but requires a custom H5NetCDFStore wrapper to plumb through
    xarray's backend kwargs filter; deferred to round 1.5 if engine
    swap alone proves a win.

    Parameters
    ----------
    data : Any
        Data in the pipeline flow thus far.
    rule_spec : Rule
        Rule being handled
    """
    engine = rule_spec._pymor_cfg("xarray_open_mfdataset_engine")
    parallel = rule_spec._pymor_cfg("xarray_open_mfdataset_parallel")

    # Round-1 perf knobs (opt-in)
    def _cfg_first(*keys, default=None):
        for k in keys:
            if hasattr(rule_spec, "get"):
                v = rule_spec.get(k)
                if v is not None:
                    return v
            try:
                v = rule_spec._pymor_cfg(k)
                if v is not None:
                    return v
            except Exception:
                pass
        return default

    inline_array = bool(_cfg_first("xarray_open_mfdataset_inline_array", default=False))
    # Allow override of engine via rule attr (e.g. "h5netcdf")
    engine_override = _cfg_first("xarray_open_mfdataset_engine_override")
    if engine_override:
        engine = engine_override

    all_files = []
    for file_collection in rule_spec.inputs:
        for f in file_collection.files:
            all_files.append(f)
    all_files = _resolve_symlinks(all_files)
    # Filter by year range if specified in rule or inherit. Rules with
    # centennial input4MIPs forcing files (e.g. ``..._1750-2022.nc`` whose
    # range doesn't overlap the simulation year) can opt out via
    # ``skip_input_year_filter: true`` on the rule.
    year_start = rule_spec.get("year_start", None)
    year_end = rule_spec.get("year_end", None)
    skip_filter = rule_spec.get("skip_input_year_filter", False)
    if year_start is not None and year_end is not None and not skip_filter:
        all_files = _filter_files_by_year_range(all_files, int(year_start), int(year_end))
        logger.info(f"Year filter: {year_start}–{year_end}, {len(all_files)} files after filtering")

    open_kwargs = dict(parallel=parallel, use_cftime=True, engine=engine)
    if inline_array:
        open_kwargs["inline_array"] = True

    logger.info(
        f"Loading {len(all_files)} files using {engine} backend "
        f"(inline_array={inline_array}) on xarray..."
    )
    for f in all_files:
        logger.info(f"  * {f}")
    _check_compatible_schemas(all_files, rule_spec)
    mf_ds = xr.open_mfdataset(all_files, **open_kwargs)
    # Rename non-standard time dimension if specified in rule (e.g., OpenIFS uses different names).
    # If unspecified, auto-detect the XIOS/NEMO ``time_counter`` convention so
    # downstream steps (timeavg, set_time_bounds, custom compute_X) get a
    # ``time`` dim without each rule needing to set time_dimname explicitly.
    # OpenIFS-XIOS 1hr/3hr/6hr files all ship time_counter; without auto-rename
    # the data-level year filter below skips them, and arithmetic with a
    # renamed secondary input drops time entirely.
    time_dimname = rule_spec.get("time_dimname")
    if not time_dimname or time_dimname not in mf_ds.dims:
        for cand in ("time_counter", "time_centered"):
            if cand in mf_ds.dims and "time" not in mf_ds.dims:
                time_dimname = cand
                break
    if time_dimname and time_dimname in mf_ds.dims and "time" not in mf_ds.dims:
        mf_ds = mf_ds.rename({time_dimname: "time"})
        # Companion bnds variable
        for bnds_old, bnds_new in ((f"{time_dimname}_bounds", "time_bounds"),
                                   (f"{time_dimname}_bnds", "time_bnds")):
            if bnds_old in mf_ds.variables and bnds_new not in mf_ds.variables:
                mf_ds = mf_ds.rename({bnds_old: bnds_new})

    # Data-level year filter (boundary-spill fix).
    # XIOS per-year input files at 3hr / day cadence include a single
    # trailing timestep that lands on the NEXT year — e.g. an
    # ``atmos_3h_..._1851-1851.nc`` file ends at 1852-01-01 01:30:00.
    # The file-name filter above keeps the file, but pycmor's
    # split_data_timespan later groups output by timestamp year and emits
    # a 1-timestep spillover file labelled 1852. Trim those timesteps
    # here so the year filter is enforced at the data level too.
    if (
        year_start is not None
        and year_end is not None
        and not skip_filter
        and "time" in mf_ds.dims
        and "time" in mf_ds.coords
    ):
        import pandas as _pd  # local: gather_inputs.py runs once per rule
        time_vals = mf_ds["time"].values
        try:
            years = _pd.Series(time_vals).dt.year.to_numpy()
        except (AttributeError, TypeError):
            # cftime objects don't go through pandas .dt
            years = np.fromiter(
                (t.year for t in time_vals), dtype=np.int64, count=len(time_vals)
            )
        mask = (years >= int(year_start)) & (years <= int(year_end))
        if not mask.all():
            n_dropped = int((~mask).sum())
            logger.info(
                f"Data-level year filter: trimming {n_dropped}/{len(time_vals)} "
                f"timesteps outside [{year_start}, {year_end}]"
            )
            mf_ds = mf_ds.isel(time=mask)

    return mf_ds


@deprecation.deprecated(details="Use load_mfdataset in your pipeline instead!")
def gather_inputs(config: dict) -> dict:
    """
    Gather possible inputs from a user directory.

    This function takes a configuration dictionary and returns a list of pathlib.Path objects
    representing the files in the directory that match the pattern specified in the configuration.

    Parameters
    ----------
    config : dict
        The configuration dictionary. This dictionary should contain the keys
        `pattern_env_var_name` and `pattern_env_value_default`, which are used to locate
        the environment variable name and default value respectively. If not gives, these default
        to `PYMOR_INPUT_PATTERN` and `.*` respectively.

    Returns
    -------
    config:
        The configuration dictionary with the input files added.

    """
    # NOTE(PG): Example removed from docstring as it is scheduled for deprecation.
    rules = config.get("rules", [])
    for rule in rules:
        input_patterns = rule.get("input_patterns", [])
        input_files = {}
        year_start = rule.get("year_start")
        year_end = rule.get("year_end")
        if year_start is not None:
            year_start = int(year_start)
        if year_end is not None:
            year_end = int(year_end)
        for input_pattern in input_patterns:
            if _validate_rule_has_marked_regex(rule):
                pattern = re.compile(rule["pattern"])
            else:
                # FIXME(PG): This needs to be thought through...
                # If the pattern is not marked, use the environment variable
                pattern = _input_pattern_from_env(config)
            files = _input_files_in_path(input_pattern, pattern)
            files = _resolve_symlinks(files)
            if year_start is not None and year_end is not None:
                files = _filter_by_year(files, pattern, year_start, year_end)
                files = _sort_by_year(files, pattern, year_start, year_end)
            input_files[input_pattern] = files
        rule["input_files"] = input_files
    return config
