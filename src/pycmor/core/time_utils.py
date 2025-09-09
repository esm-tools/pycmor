"""
Time-related utility functions for working with xarray datasets and coordinates.

This module provides utilities for:
- Detecting datetime types in arrays
- Finding time coordinates in xarray objects
- Checking for time axes in datasets
"""

import cftime
import numpy as np
import xarray as xr


def is_cftime_type(arr: np.ndarray) -> bool:
    """Checks if array elements are cftime objects"""
    if arr.size == 0:
        return False

    # Check if the first element is a cftime object
    try:
        first_item = arr.item(0)
        # Check if it's an instance of cftime.datetime (base class for all cftime types)
        return isinstance(first_item, cftime.datetime)
    except (IndexError, ValueError):
        return False


def is_datetime_type(arr: np.ndarray) -> bool:
    """Checks if array elements are datetime objects or cftime objects"""
    return is_cftime_type(arr) or np.issubdtype(arr.dtype, np.datetime64)


def get_time_label(ds):
    """
    Determines the name of the coordinate in the dataset that can serve as a time label.

    Parameters
    ----------
    ds : xarray.Dataset
        The dataset containing coordinates to check for a time label.

    Returns
    -------
    str or None
        The name of the coordinate that is a datetime type and can serve as a time label,
        or None if no such coordinate is found.

    Example
    -------
    >>> import xarray as xr
    >>> import pandas as pd
    >>> import numpy as np
    >>> ds = xr.Dataset(
    ...     {'temperature': (['time'], [20, 21, 22])},
    ...     coords={'time': pd.date_range('2000-01-01', periods=3)}
    ... )
    >>> get_time_label(ds)
    'time'
    >>> da = xr.DataArray(np.ones(3), coords={'T': ('T', pd.date_range('2000-01-01', periods=3))})
    >>> get_time_label(da)
    'T'
    >>> # The following does not have a valid time coordinate, expected to return None
    >>> ds_no_time = xr.Dataset({'temperature': (['x'], [20, 21, 22])}, coords={'x': [1, 2, 3]})
    >>> get_time_label(ds_no_time) is None
    True
    """
    # Find all datetime coordinates that have dimensions
    datetime_coords = []
    for name, coord in ds.coords.items():
        if is_datetime_type(coord) and coord.dims and name in coord.dims:
            datetime_coords.append(name)

    if not datetime_coords:
        return None

    # For DataArrays, return the first datetime coordinate found
    if isinstance(ds, xr.DataArray):
        return datetime_coords[0]

    # For Datasets, prioritize coordinates that are actually used by data variables
    used_coords = []
    unused_coords = []

    # Get all dimensions used by data variables
    used_dims = set()
    for data_var in ds.data_vars.values():
        used_dims.update(data_var.dims)

    # Separate datetime coordinates into used and unused
    for coord_name in datetime_coords:
        if coord_name in used_dims:
            used_coords.append(coord_name)
        else:
            unused_coords.append(coord_name)

    # Return the first used coordinate, or None if no datetime coords are used
    if used_coords:
        return used_coords[0]
    else:
        return None


def has_time_axis(ds) -> bool:
    """
    Checks if the given dataset has a time axis.

    Parameters
    ----------
    ds : xarray.Dataset or xarray.DataArray
        The dataset to check for a time axis.

    Returns
    -------
    bool
        True if the dataset has a time axis, False otherwise.
    """
    return get_time_label(ds) is not None
