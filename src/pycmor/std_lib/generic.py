"""
Generic
=======
This module, `generic.py`, provides functionalities for transforming and standardizing NetCDF files
according to CMOR.

It contains several functions and classes:

Functions (can be used as actions in `Rule` objects):
- `linear_transform`: Applies a linear transformation to the data of a NetCDF file.
- `invert_z_axis`: Inverts the z-axis of a NetCDF file.

The Full CMOR (yes, bad pun):
    * Applied if no other rule sets are given for a file
    * Adds CMOR metadata to the file
    * Converts units
    * Performs time averaging
"""

import re
import tempfile
from pathlib import Path

import xarray as xr

from ..core.logging import logger


def load_data(data, rule_spec, *args, **kwargs):
    """
    Loads data described by the rule_spec.

    Parameters
    ----------
    data : Any
        Initial data (ignored, replaced by loaded data)
    rule_spec : dict or Rule
        Rule specification with input_patterns attribute

    Returns
    -------
    xr.Dataset
        Concatenated dataset from all input patterns

    Examples
    --------
    >>> # This function requires input files to exist
    >>> # Example demonstrates the expected interface
    >>> rule_spec = {
    ...     'input_patterns': [
    ...         '/path/to/model_output_*.nc'
    ...     ]
    ... }
    >>> # Load data from pattern-matched files
    >>> data = load_data(None, rule_spec)  # doctest: +SKIP
    >>> print("OUTPUT type:", type(data).__name__)  # doctest: +SKIP
    OUTPUT type: Dataset
    >>> print("OUTPUT has time dimension:", 'time' in data.dims)  # doctest: +SKIP
    OUTPUT has time dimension: True

    Note
    ----
    This function requires existing NetCDF files matching input_patterns.
    Use +SKIP in doctests to avoid file dependency.
    """
    ds_list = []
    for pattern in rule_spec["input_patterns"]:
        ds = xr.open_mfdataset(pattern, combine="by_coords")
        ds_list.append(ds)
    data = xr.concat(ds_list, dim="time")
    return data


def linear_transform(filepath: Path, execute: bool = False, slope: float = 1, offset: float = 0):
    """
    Applies a linear transformation to the data of a NetCDF file.

    Parameters
    ----------
    filepath : Path
        Path to the input file.
    execute : bool, optional
    slope: float, optional
    offset: float, optional

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> # Create simple dataset
    >>> data = xr.Dataset({
    ...     'temperature': xr.DataArray(
    ...         np.array([10.0, 20.0, 30.0]),
    ...         dims=['time']
    ...     )
    ... })
    >>> print("INPUT:")
    >>> print(data.temperature.values)
    INPUT:
    [10. 20. 30.]
    >>> # Apply transformation: Celsius to Kelvin (slope=1, offset=273.15)
    >>> transformed = data * 1 + 273.15
    >>> print("OUTPUT (C to K):")
    >>> print(transformed.temperature.values)
    OUTPUT (C to K):
    [283.15 293.15 303.15]
    >>> # Apply transformation: Double and add 5
    >>> transformed = data * 2 + 5
    >>> print("OUTPUT (2x + 5):")
    >>> print(transformed.temperature.values)
    OUTPUT (2x + 5):
    [25. 45. 65.]
    """
    if execute:
        ds = xr.open_dataset(filepath)
        ds = ds * slope + offset
        logger.info(f"Applied linear transformation to {filepath}")
        ds.to_netcdf(filepath)
    else:
        logger.info(f"Would apply linear transformation to {filepath}")
        logger.info(f"slope: {slope}, offset: {offset}")
        logger.info("Use `execute=True` to apply changes")


def invert_z_axis(filepath: Path, execute: bool = False, flip_sign: bool = False):
    """
    Inverts the z-axis of a NetCDF file.

    Parameters
    ----------
    filepath : Path
        Path to the input file.
    execute : bool, optional
        If True, the function will execute the inversion. If False, it will
        only print the changes that would be made.

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> # Create dataset with z-axis
    >>> data = xr.Dataset({
    ...     'temperature': xr.DataArray(
    ...         np.array([[10.0, 15.0], [20.0, 25.0], [30.0, 35.0]]),
    ...         dims=['z', 'x'],
    ...         coords={'z': [0, 10, 20], 'x': [0, 1]}
    ...     )
    ... })
    >>> print("INPUT z-axis:")
    >>> print(data.z.values)
    INPUT z-axis:
    [ 0 10 20]
    >>> print("INPUT temperature:")
    >>> print(data.temperature.values)  # doctest: +NORMALIZE_WHITESPACE
    INPUT temperature:
    [[10. 15.]
     [20. 25.]
     [30. 35.]]
    >>> # Invert z-axis order
    >>> inverted = data.reindex(z=data.z[::-1])
    >>> print("OUTPUT z-axis (inverted order):")
    >>> print(inverted.z.values)
    OUTPUT z-axis (inverted order):
    [20 10  0]
    >>> print("OUTPUT temperature (inverted):")
    >>> print(inverted.temperature.values)  # doctest: +NORMALIZE_WHITESPACE
    OUTPUT temperature (inverted):
    [[30. 35.]
     [20. 25.]
     [10. 15.]]
    >>> # Flip sign of z-axis
    >>> inverted['z'] = inverted.z * -1
    >>> print("OUTPUT z-axis (flipped sign):")
    >>> print(inverted.z.values)
    OUTPUT z-axis (flipped sign):
    [-20 -10   0]
    """
    if execute:
        ds = xr.open_dataset(filepath)
        ds = ds.reindex(z=ds.z[::-1])
        logger.info(f"Inverted order of z-axis of {filepath}")
        if flip_sign:
            ds["z"] *= -1
            logger.info(f"Flipped sign of z-axis of {filepath}")
        ds.to_netcdf(filepath)
    else:
        logger.info(f"Would invert z-axis of {filepath}")
        if flip_sign:
            logger.info("Would flip sign of z-axis")
        logger.info("Use `execute=True` to apply changes")


def create_cmor_directories(config: dict) -> dict:
    """
    Creates the directory structure for the CMORized files.

    Parameters
    ----------
    config : dict
        The pymor configuration dictionary

    Returns
    -------
    dict
        Updated config with output_dir key added

    Examples
    --------
    >>> import tempfile
    >>> from pathlib import Path
    >>> # Create a temporary directory for output
    >>> temp_root = tempfile.mkdtemp()
    >>> # Define CMOR configuration
    >>> config = {
    ...     'output_root': temp_root,
    ...     'mip_era': 'CMIP6',
    ...     'activity_id': 'CMIP',
    ...     'institution_id': 'AWI',
    ...     'source_id': 'AWI-ESM-1-1-LR',
    ...     'experiment_id': 'historical',
    ...     'member_id': 'r1i1p1f1',
    ...     'table_id': 'Amon',
    ...     'variable_id': 'tas',
    ...     'grid_label': 'gn',
    ...     'version': 'v20191018'
    ... }
    >>> print("INPUT config keys:")
    >>> print(sorted([k for k in config.keys() if k != 'output_root']))  # doctest: +ELLIPSIS
    INPUT config keys:
    ['activity_id', 'experiment_id', 'grid_label', 'institution_id', 'member_id', ...]
    >>> # Create directory structure
    >>> result = create_cmor_directories(config)  # doctest: +SKIP
    >>> print("OUTPUT has output_dir:", 'output_dir' in result)  # doctest: +SKIP
    OUTPUT has output_dir: True
    >>> print("OUTPUT directory exists:", result['output_dir'].exists())  # doctest: +SKIP
    OUTPUT directory exists: True
    >>> # Check directory structure
    >>> expected_parts = ['CMIP6', 'CMIP', 'AWI', 'AWI-ESM-1-1-LR', 'historical',  # doctest: +SKIP
    ...                   'r1i1p1f1', 'Amon', 'tas', 'gn', 'v20191018']  # doctest: +SKIP
    >>> path_parts = result['output_dir'].parts  # doctest: +SKIP
    >>> print("OUTPUT path contains expected parts:", all(p in path_parts for p in expected_parts))  # doctest: +SKIP
    OUTPUT path contains expected parts: True

    Note
    ----
    This function creates directories on the filesystem.
    Use +SKIP in doctests to avoid filesystem side effects.

    See Also
    --------
    https://docs.google.com/document/d/1h0r8RZr_f3-8egBMMh7aqLwy3snpD6_MrDz1q8n5XUk/edit
    """
    # Directory structure =
    # <mip_era>/
    #  <activity_id>/ # an exception for this exists in section "Directory structure
    #                 # template": "If multiple activities are listed in the global
    #                 # attribute, the first one is used in the directory structure."
    #   <institution_id>/
    #     <source_id>/
    #     <experiment_id>/
    #      <member_id>/
    #       <table_id>/
    #        <variable_id>/
    #         <grid_label>/
    #          <version>
    mip_era = config["mip_era"]
    activity_id = config["activity_id"]
    institution_id = config.get("institution_id", "AWI")
    source_id = config.get("source_id", "AWI-ESM-1-1-LR")
    experiment_id = config["experiment_id"]
    member_id = config["member_id"]
    table_id = config["table_id"]
    variable_id = config["variable_id"]
    grid_label = config["grid_label"]
    version = config["version"]

    output_root = config["output_root"]
    output_dir = (
        Path(output_root)
        / mip_era
        / activity_id
        / institution_id
        / source_id
        / experiment_id
        / member_id
        / table_id
        / variable_id
        / grid_label
        / version
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created directory structure for CMORized files in {output_dir}")
    config["output_dir"] = output_dir
    return config


def dummy_load_data(data, rule_spec, *args, **kwargs):
    """
    A dummy function for testing. Loads the xarray tutorial data.

    Parameters
    ----------
    data : Any
        Initial data (ignored, replaced with tutorial data)
    rule_spec : dict or Rule
        Rule specification with optional input_source, input_type, and da_name

    Returns
    -------
    xr.Dataset or xr.DataArray
        Tutorial dataset or data array

    Examples
    --------
    >>> from types import SimpleNamespace
    >>> # Load dataset (default behavior)
    >>> rule_spec = SimpleNamespace()
    >>> rule_spec.get = lambda key, default=None: {'input_source': 'xr_tutorial'}.get(key, default)
    >>> data = dummy_load_data(None, rule_spec)  # doctest: +SKIP
    >>> print("OUTPUT type:", type(data).__name__)  # doctest: +SKIP
    OUTPUT type: Dataset
    >>> print("OUTPUT has 'air' variable:", 'air' in data.data_vars)  # doctest: +SKIP
    OUTPUT has 'air' variable: True

    Note
    ----
    This function requires network access to download tutorial data.
    Use +SKIP in doctests to avoid network dependency.
    """
    logger.info("Loading data")
    input_source = rule_spec.get("input_source", "xr_tutorial")
    if input_source == "xr_tutorial":
        data = xr.tutorial.open_dataset("air_temperature")
    if rule_spec.get("input_type") == "xr.DataArray":
        data = getattr(data, rule_spec.get("da_name", "air"))
    return data


def dummy_logic_step(data, rule_spec, *args, **kwargs):
    """
    A dummy function for testing. Prints data to screen and adds a dummy attribute to the data.

    Parameters
    ----------
    data : xr.DataArray or xr.Dataset
        Input data to modify
    rule_spec : Rule
        Rule specification (not used in current implementation)

    Returns
    -------
    xr.DataArray or xr.Dataset
        Data with added dummy_attribute

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> from types import SimpleNamespace
    >>> # Create simple data
    >>> data = xr.DataArray(
    ...     np.array([1.0, 2.0, 3.0]),
    ...     dims=['time'],
    ...     attrs={'original': 'value'}
    ... )
    >>> print("INPUT attributes:")
    >>> print(data.attrs)
    INPUT attributes:
    {'original': 'value'}
    >>> # Add dummy attribute
    >>> rule_spec = SimpleNamespace()
    >>> result = dummy_logic_step(data, rule_spec)
    >>> print("OUTPUT attributes:")
    >>> print(dict(result.attrs))  # doctest: +ELLIPSIS
    OUTPUT attributes:
    {...'dummy_attribute': 'dummy_value'...}
    >>> print("Has dummy_attribute:", 'dummy_attribute' in result.attrs)
    Has dummy_attribute: True
    >>> print("dummy_attribute value:", result.attrs['dummy_attribute'])
    dummy_attribute value: dummy_value
    """
    logger.info(data)
    logger.info("Adding dummy attribute to data")
    data.attrs["dummy_attribute"] = "dummy_value"
    logger.info(f"Data attributes: {data.attrs}")
    return data


def dummy_save_data(data, rule_spec, *args, **kwargs):
    """
    A dummy function for testing. Saves the data to a netcdf file.

    Parameters
    ----------
    data : xr.DataArray or xr.Dataset
        Input data to save
    rule_spec : Rule
        Rule specification (not used in current implementation)

    Returns
    -------
    xr.DataArray or xr.Dataset
        Unmodified input data

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> from types import SimpleNamespace
    >>> import os
    >>> # Create simple data
    >>> data = xr.DataArray(
    ...     np.array([1.0, 2.0, 3.0]),
    ...     dims=['time']
    ... )
    >>> print("INPUT:")
    >>> print(data.values)
    INPUT:
    [1. 2. 3.]
    >>> # Save data (creates temporary file)
    >>> rule_spec = SimpleNamespace()
    >>> result = dummy_save_data(data, rule_spec)  # doctest: +SKIP
    >>> print("OUTPUT (unchanged):")  # doctest: +SKIP
    >>> print(result.values)  # doctest: +SKIP
    OUTPUT (unchanged):
    [1. 2. 3.]

    Note
    ----
    This function creates temporary files that are not automatically cleaned up.
    Use +SKIP in doctests to avoid filesystem side effects.
    """
    ofile = tempfile.mktemp(suffix=".nc")
    data.to_netcdf(ofile)
    logger.success(f"Data saved to {ofile}")
    return data


def dummy_sleep(data, rule_spec, *arg, **kwargs):
    """
    A dummy function for testing. Sleeps for 5 seconds.

    Parameters
    ----------
    data : xr.DataArray or xr.Dataset
        Input data (passed through unchanged)
    rule_spec : Rule
        Rule specification (not used in current implementation)

    Returns
    -------
    xr.DataArray or xr.Dataset
        Unmodified input data

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> from types import SimpleNamespace
    >>> # Create simple data
    >>> data = xr.DataArray(
    ...     np.array([1.0, 2.0, 3.0]),
    ...     dims=['time']
    ... )
    >>> print("INPUT:")
    >>> print(data.values)
    INPUT:
    [1. 2. 3.]
    >>> # Sleep function (skipped to avoid delays in tests)
    >>> rule_spec = SimpleNamespace()
    >>> result = dummy_sleep(data, rule_spec)  # doctest: +SKIP
    >>> print("OUTPUT (unchanged after sleep):")  # doctest: +SKIP
    >>> print(result.values)  # doctest: +SKIP
    OUTPUT (unchanged after sleep):
    [1. 2. 3.]

    Note
    ----
    This function sleeps for 5 seconds, so use +SKIP in doctests.
    """
    import time

    time.sleep(5)
    return data


def show_data(data, rule_spec, *args, **kwargs):
    """
    Prints data to screen. Useful for debugging.

    Parameters
    ----------
    data : xr.DataArray or xr.Dataset
        Input data to display
    rule_spec : Rule
        Rule specification (not used in current implementation)

    Returns
    -------
    xr.DataArray or xr.Dataset
        Unmodified input data

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> from types import SimpleNamespace
    >>> # Create simple data
    >>> data = xr.DataArray(
    ...     np.array([1.0, 2.0, 3.0]),
    ...     dims=['time'],
    ...     name='temperature'
    ... )
    >>> print("INPUT:")
    >>> print(data.values)
    INPUT:
    [1. 2. 3.]
    >>> # show_data returns data unchanged
    >>> rule_spec = SimpleNamespace()
    >>> result = show_data(data, rule_spec)
    >>> print("OUTPUT (unchanged):")
    >>> print(result.values)
    OUTPUT (unchanged):
    [1. 2. 3.]
    >>> print("OUTPUT equals INPUT:", np.array_equal(result.values, data.values))
    OUTPUT equals INPUT: True
    """
    logger.info("Printing data...")
    logger.info(data)
    return data


def get_variable(data, rule_spec, *args, **kwargs):
    """
    Gets a particular variable out of a xr.Dataset

    Parameters
    ----------
    data : xr.Dataset
        Assumes data is a dataset already. No checks are done
        for this!!
    rule_spec : Rule
        Rule describing the DataRequestVariable for this pipeline run

    Returns
    -------
    xr.DataArray

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> from types import SimpleNamespace
    >>> # Create dataset with multiple variables
    >>> data = xr.Dataset({
    ...     'temperature': xr.DataArray([20.0, 25.0, 30.0], dims=['time']),
    ...     'pressure': xr.DataArray([1013.0, 1015.0, 1012.0], dims=['time']),
    ...     'humidity': xr.DataArray([60.0, 65.0, 70.0], dims=['time'])
    ... })
    >>> print("INPUT dataset variables:")
    >>> print(list(data.data_vars))
    INPUT dataset variables:
    ['temperature', 'pressure', 'humidity']
    >>> print("INPUT temperature values:")
    >>> print(data['temperature'].values)
    [20. 25. 30.]
    >>> # Create mock rule_spec with model_variable attribute
    >>> rule_spec = SimpleNamespace(model_variable='temperature')
    >>> # Extract specific variable
    >>> result = get_variable(data, rule_spec)
    >>> print("OUTPUT (extracted 'temperature'):")
    >>> print(result.values)
    OUTPUT (extracted 'temperature'):
    [20. 25. 30.]
    >>> print("OUTPUT type:")
    >>> print(type(result).__name__)
    OUTPUT type:
    DataArray
    >>> print("OUTPUT name:")
    >>> print(result.name)
    OUTPUT name:
    temperature
    >>> # Extract a different variable
    >>> rule_spec2 = SimpleNamespace(model_variable='pressure')
    >>> result2 = get_variable(data, rule_spec2)
    >>> print("OUTPUT (extracted 'pressure'):")
    >>> print(result2.values)
    OUTPUT (extracted 'pressure'):
    [1013. 1015. 1012.]
    """
    return data[rule_spec.model_variable]


def resample_monthly(data, rule_spec, *args, **kwargs):
    """
    Compute monthly means per year.

    Parameters
    ----------
    data : xr.DataArray or xr.Dataset
        Input data with time dimension
    rule_spec : Rule
        Rule specification (not used in current implementation)

    Returns
    -------
    xr.DataArray or xr.Dataset
        Monthly averaged data

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> import pandas as pd
    >>> from types import SimpleNamespace
    >>> # Create daily data for 3 months with known values
    >>> times = pd.date_range('2020-01-01', '2020-03-31', freq='D')
    >>> # Create temperature data: constant 10 in Jan, 20 in Feb, 30 in Mar
    >>> values = np.concatenate([
    ...     np.full(31, 10.0),  # January
    ...     np.full(29, 20.0),  # February (2020 is leap year)
    ...     np.full(31, 30.0)   # March
    ... ])
    >>> data = xr.DataArray(
    ...     values,
    ...     dims=['time'],
    ...     coords={'time': times}
    ... )
    >>> print("INPUT time range:")
    >>> print(f"{str(data.time.values[0])[:10]} to {str(data.time.values[-1])[:10]}")
    INPUT time range:
    2020-01-01 to 2020-03-31
    >>> print("INPUT data points:", len(data))
    INPUT data points: 91
    >>> print("INPUT first 3 values (Jan):")
    >>> print(data.values[:3])
    [10. 10. 10.]
    >>> # Resample to monthly means
    >>> rule_spec = SimpleNamespace()
    >>> monthly = resample_monthly(data, rule_spec)
    >>> print("OUTPUT data points:", len(monthly))
    OUTPUT data points: 3
    >>> print("OUTPUT monthly means:")
    >>> print(monthly.values)
    [10. 20. 30.]
    >>> print("OUTPUT time dimension preserved:", 'time' in monthly.dims)
    OUTPUT time dimension preserved: True
    """
    mm = data.resample(time="ME", **kwargs).mean(dim="time")
    # cdo adjusts timestamp to mean-time-value.
    # with xarray timestamp defaults to end_time. Re-adjusting timestamp to mean-time-value like cdo
    # adjust_timestamp = rule_spec.get("adjust_timestamp", True)
    # if adjust_timestamp:
    #     t = pd.to_datetime(mm.time.dt.strftime("%Y-%m-15").to_pandas())
    #     mm["time"] = t
    return mm


def resample_yearly(data, rule_spec, *args, **kwargs):
    """
    Compute yearly means.

    Parameters
    ----------
    data : xr.DataArray or xr.Dataset
        Input data with time dimension
    rule_spec : Rule
        Rule specification (not used in current implementation)

    Returns
    -------
    xr.DataArray or xr.Dataset
        Yearly averaged data

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> import pandas as pd
    >>> from types import SimpleNamespace
    >>> # Create monthly data for 3 years with known values
    >>> times = pd.date_range('2020-01-01', '2022-12-31', freq='MS')
    >>> # Create data: 10 for 2020, 20 for 2021, 30 for 2022
    >>> values = np.concatenate([
    ...     np.full(12, 10.0),  # 2020
    ...     np.full(12, 20.0),  # 2021
    ...     np.full(12, 30.0)   # 2022
    ... ])
    >>> data = xr.DataArray(
    ...     values,
    ...     dims=['time'],
    ...     coords={'time': times}
    ... )
    >>> print("INPUT time range:")
    >>> print(f"{str(data.time.values[0])[:10]} to {str(data.time.values[-1])[:10]}")
    INPUT time range:
    2020-01-01 to 2022-12-01
    >>> print("INPUT data points:", len(data))
    INPUT data points: 36
    >>> print("INPUT first 3 values (2020):")
    >>> print(data.values[:3])
    [10. 10. 10.]
    >>> # Resample to yearly means
    >>> rule_spec = SimpleNamespace()
    >>> yearly = resample_yearly(data, rule_spec)
    >>> print("OUTPUT data points:", len(yearly))
    OUTPUT data points: 3
    >>> print("OUTPUT yearly means:")
    >>> print(yearly.values)
    [10. 20. 30.]
    >>> print("OUTPUT time dimension preserved:", 'time' in yearly.dims)
    OUTPUT time dimension preserved: True
    """
    ym = data.resample(time="YE", **kwargs).mean(dim="time")
    # cdo adjusts timestamp to mean-time-value.
    # with xarray timestamp defaults to end_time. Re-adjusting timestamp to mean-time-value like cdo
    # adjust_timestamp = rule_spec.get("adjust_timestamp", True)
    # if adjust_timestamp:
    #     t = pd.to_datetime(mm.time.dt.strftime("%Y-%m-15").to_pandas())
    #     mm["time"] = t
    return ym


def multiyear_monthly_mean(data, rule_spec, *args, **kwargs):
    """
    Compute multi-year monthly climatology (mean for each month across all years).

    Parameters
    ----------
    data : xr.DataArray or xr.Dataset
        Input data with time dimension
    rule_spec : Rule
        Rule specification (not used in current implementation)

    Returns
    -------
    xr.DataArray or xr.Dataset
        Monthly climatology with 12 values (one per month)

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> import pandas as pd
    >>> from types import SimpleNamespace
    >>> # Create monthly data for 2 years (Jan: 10, Feb: 20, Mar: 30, etc)
    >>> times = pd.date_range('2020-01-01', '2021-12-31', freq='MS')
    >>> # Create data where each month has a consistent pattern
    >>> # Year 1: [10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30]
    >>> # Year 2: [10, 20, 30, 10, 20, 30, 10, 20, 30, 10, 20, 30]
    >>> values = np.tile([10.0, 20.0, 30.0], 8)[:24]
    >>> data = xr.DataArray(
    ...     values,
    ...     dims=['time'],
    ...     coords={'time': times}
    ... )
    >>> print("INPUT time points:", len(data))
    INPUT time points: 24
    >>> print("INPUT first 6 values:")
    >>> print(data.values[:6])
    [10. 20. 30. 10. 20. 30.]
    >>> print("INPUT covers 2 years: 2020 and 2021")
    INPUT covers 2 years: 2020 and 2021
    >>> # Compute multi-year monthly mean (climatology)
    >>> rule_spec = SimpleNamespace()
    >>> climatology = multiyear_monthly_mean(data, rule_spec)
    >>> print("OUTPUT months:", len(climatology))
    OUTPUT months: 12
    >>> print("OUTPUT climatology values (repeating pattern):")
    >>> print(climatology.values)
    [10. 20. 30. 10. 20. 30. 10. 20. 30. 10. 20. 30.]
    >>> print("OUTPUT has 'month' coordinate:", 'month' in climatology.coords)
    OUTPUT has 'month' coordinate: True
    >>> print("OUTPUT month range:")
    >>> print(climatology.month.values)
    [ 1  2  3  4  5  6  7  8  9 10 11 12]
    """
    multiyear_monthly_mean = data.groupby("time.month").mean(dim="time")
    return multiyear_monthly_mean


def trigger_compute(data, rule_spec, *args, **kwargs):
    """
    Triggers computation of lazy/dask-backed data.

    Parameters
    ----------
    data : xr.DataArray or xr.Dataset
        Input data, possibly with lazy operations
    rule_spec : Rule
        Rule specification (not used in current implementation)

    Returns
    -------
    xr.DataArray or xr.Dataset
        Data with all lazy operations computed

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> from types import SimpleNamespace
    >>> # Create data (in real use, this might be dask-backed)
    >>> data = xr.DataArray(
    ...     np.array([1.0, 2.0, 3.0]),
    ...     dims=['time']
    ... )
    >>> print("INPUT:")
    >>> print(data.values)
    INPUT:
    [1. 2. 3.]
    >>> # Trigger compute (no-op for eager numpy arrays)
    >>> rule_spec = SimpleNamespace()
    >>> result = trigger_compute(data, rule_spec)
    >>> print("OUTPUT:")
    >>> print(result.values)
    OUTPUT:
    [1. 2. 3.]
    >>> # Create lazy data with simple operation
    >>> lazy_data = data + 10  # This might be lazy in dask
    >>> computed = trigger_compute(lazy_data, rule_spec)
    >>> print("OUTPUT (computed):")
    >>> print(computed.values)
    OUTPUT (computed):
    [11. 12. 13.]
    """
    if hasattr(data, "compute"):
        return data.compute()
    # Data doesn't have a compute method, do nothing
    return data


def rename_dims(data, rule_spec):
    """
    Renames the dimensions of the array based on the key/values of rule_spec["model_dim"].

    Parameters
    ----------
    data : xr.DataArray or xr.Dataset
        Input data with dimensions to rename
    rule_spec : Rule
        Rule specification with model_dim attribute mapping old names to new names

    Returns
    -------
    xr.DataArray or xr.Dataset
        Data with renamed dimensions

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> from types import SimpleNamespace
    >>> # Create data with model-specific dimension names
    >>> data = xr.DataArray(
    ...     np.arange(60).reshape(3, 4, 5),
    ...     dims=['lev', 'rlat', 'rlon'],
    ...     coords={'lev': [0, 10, 20], 'rlat': [0, 1, 2, 3], 'rlon': [0, 1, 2, 3, 4]}
    ... )
    >>> print("INPUT dimensions:")
    >>> print(list(data.dims))
    INPUT dimensions:
    ['lev', 'rlat', 'rlon']
    >>> print("INPUT shape:", data.shape)
    INPUT shape: (3, 4, 5)
    >>> print("INPUT coordinates:")
    >>> print(list(data.coords))
    ['lev', 'rlat', 'rlon']
    >>> # Create rule_spec with dimension mapping (model names -> CMOR names)
    >>> rule_spec = SimpleNamespace(
    ...     model_dim={'lev': 'plev', 'rlat': 'lat', 'rlon': 'lon'}
    ... )
    >>> rule_spec.get = lambda key, default=None: getattr(rule_spec, key, default)
    >>> # Rename dimensions
    >>> renamed = rename_dims(data, rule_spec)
    >>> print("OUTPUT dimensions:")
    >>> print(list(renamed.dims))
    OUTPUT dimensions:
    ['plev', 'lat', 'lon']
    >>> print("OUTPUT shape (unchanged):", renamed.shape)
    OUTPUT shape (unchanged): (3, 4, 5)
    >>> print("OUTPUT coordinates:")
    >>> print(list(renamed.coords))
    OUTPUT coordinates:
    ['plev', 'lat', 'lon']
    >>> # Verify coordinate values are preserved
    >>> print("OUTPUT plev values:")
    >>> print(renamed.plev.values)
    [ 0 10 20]
    >>> # Test with no model_dim attribute (no-op)
    >>> rule_spec_no_dim = SimpleNamespace()
    >>> rule_spec_no_dim.get = lambda key, default=None: None
    >>> unchanged = rename_dims(data, rule_spec_no_dim)
    >>> print("OUTPUT (no rename) dimensions:")
    >>> print(list(unchanged.dims))
    OUTPUT (no rename) dimensions:
    ['lev', 'rlat', 'rlon']
    """
    # Check if the rule_spec has a model_dim attribute
    if rule_spec.get("model_dim"):
        model_dim = rule_spec.model_dim
        # Rename the dimensions in the encoding if they exist:
        del_encodings = []
        for dim in data.dims:
            if dim in data.encoding:
                del_encodings.append(dim)
                data.encoding[model_dim[dim]] = data.encoding[dim]
        for dim in del_encodings:
            del data.encoding[dim]
        # If it does, rename the dimensions of the array based on the key/values of rule_spec["model_dim"]
        data = data.rename({k: v for k, v in model_dim.items()})
    return data


def sort_dimensions(data, rule_spec):
    """
    Sorts the dimensions of a DataArray based on the array_order attribute of the
    rule_spec. If the array_order attribute is not present, it is inferred from the
    dimensions attribute of the data request variable.

    Parameters
    ----------
    data : xr.DataArray or xr.Dataset
        Input data with dimensions to reorder
    rule_spec : Rule
        Rule specification with array_order attribute or data_request_variable.dimensions

    Returns
    -------
    xr.DataArray or xr.Dataset
        Data with dimensions transposed to match array_order

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> from types import SimpleNamespace
    >>> # Create data with dimensions in arbitrary order
    >>> data = xr.DataArray(
    ...     np.arange(24).reshape(2, 3, 4),
    ...     dims=['lon', 'lat', 'time'],
    ...     coords={'lon': [0, 1], 'lat': [0, 1, 2], 'time': [0, 1, 2, 3]}
    ... )
    >>> print("INPUT dimensions:")
    >>> print(list(data.dims))
    INPUT dimensions:
    ['lon', 'lat', 'time']
    >>> print("INPUT shape:", data.shape)
    INPUT shape: (2, 3, 4)
    >>> print("INPUT data[0, 0, :]:", data.values[0, 0, :])
    [0 1 2 3]
    >>> # Create rule_spec with desired dimension order
    >>> rule_spec = SimpleNamespace(array_order=['time', 'lat', 'lon'])
    >>> rule_spec.get = lambda key, default=None: getattr(rule_spec, key, default)
    >>> # Sort dimensions to CMOR standard order (time, lat, lon)
    >>> sorted_data = sort_dimensions(data, rule_spec)
    >>> print("OUTPUT dimensions:")
    >>> print(list(sorted_data.dims))
    OUTPUT dimensions:
    ['time', 'lat', 'lon']
    >>> print("OUTPUT shape:", sorted_data.shape)
    OUTPUT shape: (4, 3, 2)
    >>> # Verify data is correctly transposed
    >>> print("OUTPUT data[:, 0, 0]:", sorted_data.values[:, 0, 0])
    [0 1 2 3]
    >>> # Test with string dimensions (space-separated)
    >>> drv = SimpleNamespace(dimensions="time lat lon")
    >>> rule_spec2 = SimpleNamespace(data_request_variable=drv)
    >>> rule_spec2.get = lambda key, default=None: getattr(rule_spec2, key, default)
    >>> sorted_data2 = sort_dimensions(data, rule_spec2)
    >>> print("OUTPUT dimensions (from string):")
    >>> print(list(sorted_data2.dims))
    OUTPUT dimensions (from string):
    ['time', 'lat', 'lon']
    """
    missing_dims = rule_spec.get("sort_dimensions_missing_dims", "raise")

    if hasattr(rule_spec, "array_order"):
        array_order = rule_spec.array_order
    else:
        dimensions = rule_spec.data_request_variable.dimensions
        # Pattern to match a valid array_order (e.g. "time lat lon", but not
        # "[time lat lon]" or "time,lat,lon")
        pattern = r"^(?!\[.*\]$)(?!.*,.*)(?:\S+\s*)+$"
        if isinstance(dimensions, str) and re.fullmatch(pattern, dimensions):
            array_order = dimensions.split(" ")
        elif isinstance(dimensions, list) or isinstance(dimensions, tuple):
            array_order = dimensions
        else:
            logger.error("Invalid dimensions in data request variable: " f"{rule_spec.data_request_variable}")
            raise ValueError("Invalid dimensions in data request variable")

    logger.info(f"Transposing dimensions of data from {data.dims} to {array_order}")
    data = data.transpose(*array_order, missing_dims=missing_dims)

    return data
