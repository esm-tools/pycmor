"""
==========================
The Pycmor Standard Library
==========================
The standard library contains functions that are included in the default
pipelines, and are generally used as ``step`` functions. We expose several
useful ones:

* Unit Conversion
* Time Averaging
* Dataset Loading
* Variable Extraction
* Temporal Resampling
* Trigger Compute
* Show Data
* Global Attributes
* Variable Attributes

See the documentation for each of the steps for more details.
"""

from typing import Union

from xarray import DataArray, Dataset

from ..core.logging import logger
from ..core.rule import Rule
from .bounds import add_vertical_bounds as _add_vertical_bounds
from .coordinate_attributes import (
    set_coordinate_attributes as _set_coordinate_attributes,
)
from .dataset_helpers import freq_is_coarser_than_data, get_time_label, has_time_axis
from .exceptions import (
    PycmorResamplingError,
    PycmorResamplingTimeAxisIncompatibilityError,
)
from .generic import load_data as _load_data
from .generic import show_data as _show_data
from .generic import trigger_compute as _trigger_compute
from .global_attributes import set_global_attributes as _set_global_attributes
from .timeaverage import timeavg
from .units import handle_unit_conversion
from .variable_attributes import set_variable_attrs

__all__ = [
    "convert_units",
    "time_average",
    "load_data",
    "get_variable",
    "temporal_resample",
    "trigger_compute",
    "show_data",
    "set_global_attributes",
    "set_variable_attributes",
    "set_coordinate_attributes",
    "checkpoint_pipeline",
    "add_vertical_bounds",
]


def convert_units(
    data: Union[DataArray, Dataset], rule: Rule
) -> Union[DataArray, Dataset]:
    """
    Convert units of a DataArray or Dataset based upon the Data Request Variable you
    have selected. Automatically handles chemical elements and dimensionless units.


    Parameters
    ----------
    data : xarray.DataArray or xarray.Dataset
        The data to convert.
    rule : Rule
        The rule containing the units to convert to.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        The converted data.
    """
    return handle_unit_conversion(data, rule)


def time_average(
    data: Union[DataArray, Dataset], rule: Rule
) -> Union[DataArray, Dataset]:
    """
    Compute the time average of a DataArray or Dataset based upon the Data Request Variable you
    have selected.

    Parameters
    ----------
    data : xarray.DataArray or xarray.Dataset
        The data to average.
    rule : Rule
        The rule specifying parameters for time averaging, such as the time period
        or method to use for averaging.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        The averaged data.
    """
    return timeavg(data, rule)


def load_data(
    data: Union[DataArray, Dataset, None], rule: Rule
) -> Union[DataArray, Dataset]:
    """
    Load data from files according to the rule specification.

    This function opens and combines data from multiple files that match the pattern
    specified in the rule. It's useful for loading time series data that may be
    spread across multiple files.

    Parameters
    ----------
    data : xarray.DataArray or xarray.Dataset or None
        Existing data (if any) to incorporate with loaded data.
    rule : Rule
        The rule containing the input patterns and other specifications
        for loading the data.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        The loaded data combined into a single Dataset or DataArray.

    Notes
    -----
    The rule_spec dictionary should contain an ``input_patterns`` key with a list
    of file patterns to match, e.g., [``path/to/data/*.nc``].
    """
    return _load_data(data, rule)


def get_variable(
    data: Union[DataArray, Dataset], rule: Rule
) -> Union[DataArray, Dataset]:
    """
    Extract a variable from a dataset as a DataArray.

    Parameters
    ----------
    data : xarray.Dataset
        The dataset containing the variable to extract.
    rule : Rule
        The rule containing the variable name to extract.

    Returns
    -------
    xarray.DataArray
        The extracted variable as a DataArray.

    Raises
    ------
    KeyError
        If the variable specified in the rule does not exist in the dataset.
    """
    if isinstance(data, Dataset):
        variable_name = rule.model_variable
        if variable_name not in data:
            raise KeyError(f"Variable '{variable_name}' not found in dataset")
        return data[variable_name]
    return data


def temporal_resample(
    data: Union[DataArray, Dataset], rule: Rule
) -> Union[DataArray, Dataset]:
    """
    Resample a DataArray or Dataset to a different temporal frequency.

    Parameters
    ----------
    data : xarray.DataArray or xarray.Dataset
        The data to resample.
    rule : Rule
        The rule containing parameters for the resampling operation,
        including the frequency for resampling.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        The resampled data.

    Notes
    -----
    This function resamples time series data to a different frequency.
    The frequency is determined from the rule (typically from data_request_variable.frequency).
    Common frequencies include:
    - 'YS': year start
    - 'MS': month start
    - 'D': daily
    - 'H': hourly

    See Also
    --------
    https://docs.xarray.dev/en/stable/user-guide/time-series.html#resampling-and-grouped-operations
    """
    if not has_time_axis(data):
        return data

    time_dim = get_time_label(data)
    freq = rule.data_request_variable.frequency
    if not freq_is_coarser_than_data(freq, data):
        raise PycmorResamplingTimeAxisIncompatibilityError(
            f"Requested frequency {freq} for cmor variable {rule.cmor_variable} is finer than the dataset's ({rule.model_variable}) inherent frequency. Cannot resample!"  # noqa: E501
        )
    try:
        return data.resample({time_dim: freq}).mean()
    except Exception as e:
        logger.exception(e)
        raise PycmorResamplingError(
            f"Error during resampling model {rule.model_variable} for CMOR {rule.cmor_variable}: {e}"
        )


def trigger_compute(
    data: Union[DataArray, Dataset], rule: Rule
) -> Union[DataArray, Dataset]:
    """
    Trigger computation of lazy (dask-backed) data operations.

    This function is useful to ensure that all pending computations are
    executed before proceeding with the next steps in a pipeline. It's
    particularly important before saving data to files.

    Parameters
    ----------
    data : xarray.DataArray or xarray.Dataset
        The data containing operations to be computed.
    rule : Rule
        The rule containing additional parameters for computation.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        The computed data with all operations applied.
    """
    return _trigger_compute(data, rule)


def show_data(data: Union[DataArray, Dataset], rule: Rule) -> Union[DataArray, Dataset]:
    """
    Print data to screen for inspection and debugging purposes.

    This function is useful during development and debugging to inspect
    the content and structure of DataArrays and Datasets.

    Parameters
    ----------
    data : xarray.DataArray or xarray.Dataset
        The data to display.
    rule : Rule
        The rule containing additional parameters.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        The input data (unchanged).
    """
    return _show_data(data, rule)


def set_global_attributes(
    data: Union[DataArray, Dataset], rule: Rule
) -> Union[DataArray, Dataset]:
    """
    Set global metadata attributes for a Dataset or DataArray.

    This function applies standardized global attributes to the Dataset
    or DataArray based on the specifications in the rule, following
    conventions like CMIP6.

    Parameters
    ----------
    data : xarray.DataArray or xarray.Dataset
        The data to which global attributes will be added.
    rule : Rule
        The rule containing the global attribute specifications.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        The data with updated global attributes.
    """
    return _set_global_attributes(data, rule)


def set_variable_attributes(
    data: Union[DataArray, Dataset], rule: Rule
) -> Union[DataArray, Dataset]:
    """
    Set variable-specific metadata attributes.

    This function applies standardized variable attributes to the Dataset
    or DataArray based on the specifications in the rule, following
    conventions like CMIP6.

    Parameters
    ----------
    data : xarray.DataArray or xarray.Dataset
        The data to which variable attributes will be added.
    rule : Rule
        The rule containing the variable attribute specifications.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        The data with updated variable attributes.
    """
    return set_variable_attrs(data, rule)


def set_coordinate_attributes(
    data: Union[DataArray, Dataset], rule: Rule
) -> Union[DataArray, Dataset]:
    """
    Set CF-compliant metadata attributes on coordinate variables.

    This function applies standardized CF attributes (standard_name, axis,
    units, positive) to coordinate variables (latitude, longitude, vertical
    coordinates, etc.) to ensure proper interpretation by xarray and other
    CF-aware tools.

    Time coordinates are handled separately in the file saving step.

    Parameters
    ----------
    data : xarray.DataArray or xarray.Dataset
        The data to which coordinate attributes will be added.
    rule : Rule
        The rule containing configuration for coordinate attribute setting.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        The data with updated coordinate attributes.

    Notes
    -----
    This function sets:
    - standard_name: CF standard name for the coordinate
    - axis: X, Y, Z, or T designation
    - units: Physical units (degrees_east, degrees_north, Pa, m, etc.)
    - positive: Direction for vertical coordinates (up or down)
    - coordinates: Attribute on data variables listing their coordinates

    Configuration options:
    - xarray_set_coordinate_attributes: Enable/disable coordinate attrs
    - xarray_set_coordinates_attribute: Enable/disable 'coordinates' attr

    Examples
    --------
    >>> ds = xr.Dataset({
    ...     'tas': (['time', 'lat', 'lon'], data),
    ... }, coords={'lat': lats, 'lon': lons})
    >>> ds = set_coordinate_attributes(ds, rule)
    >>> print(ds['lat'].attrs)
    {'standard_name': 'latitude', 'units': 'degrees_north', 'axis': 'Y'}
    """
    return _set_coordinate_attributes(data, rule)


def checkpoint_pipeline(
    data: Union[DataArray, Dataset], rule: Rule
) -> Union[DataArray, Dataset]:
    """
    Insert a checkpoint in the pipeline processing.

    This function allows for state saving during pipeline processing,
    which can be useful for debugging or resuming processing from
    a specific point.

    Parameters
    ----------
    data : xarray.DataArray or xarray.Dataset
        The current data in the pipeline.
    rule : Rule
        The rule containing checkpoint parameters.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        The input data (typically unchanged).

    Notes
    -----
    Depending on the configuration in rule, this function might:
    - Save the current state to disk
    - Log the current state
    - Perform debugging operations
    """
    # Implementation can be added as needed
    return data


def add_vertical_bounds(
    data: Union[DataArray, Dataset], rule: Rule
) -> Union[DataArray, Dataset]:
    """
    Add vertical coordinate bounds to a dataset (similar to cdo genlevelbounds).

    This function automatically calculates and adds bounds for vertical coordinates
    such as pressure levels (plev, plev19, etc.) or depth levels if they don't
    already exist. This is useful for CMIP compliance where vertical bounds are
    required for proper data interpretation.

    Parameters
    ----------
    data : xarray.DataArray or xarray.Dataset
        The data to add vertical bounds to. If a DataArray, it will be converted
        to a Dataset temporarily for processing.
    rule : Rule
        The rule containing additional parameters (currently unused but kept for
        consistency with other pipeline functions).

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        The data with vertical bounds added if vertical coordinates were found.

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> ds = xr.Dataset({
    ...     'ta': (['time', 'plev', 'lat', 'lon'], np.random.rand(10, 8, 5, 6)),
    ... }, coords={
    ...     'plev': [100000, 92500, 85000, 70000, 60000, 50000, 40000, 30000],
    ...     'lat': np.linspace(-90, 90, 5),
    ...     'lon': np.linspace(0, 360, 6),
    ... })
    >>> ds_with_bounds = add_vertical_bounds(ds, rule)
    >>> print('plev_bnds' in ds_with_bounds)
    True

    Notes
    -----
    This function is similar to CDO's genlevelbounds operator. It automatically
    detects common vertical coordinate names including:
    - Pressure levels: plev, plev19, plev8, lev, level, pressure
    - Depth: depth
    - Height: height, alt, altitude

    See Also
    --------
    pycmor.std_lib.bounds.add_vertical_bounds : The underlying implementation
    """
    # Handle DataArray input by converting to Dataset
    if isinstance(data, DataArray):
        var_name = data.name or "data"
        ds = data.to_dataset(name=var_name)
        ds_with_bounds = _add_vertical_bounds(ds)
        return ds_with_bounds[var_name]

    # Dataset input - pass through directly
    return _add_vertical_bounds(data)
