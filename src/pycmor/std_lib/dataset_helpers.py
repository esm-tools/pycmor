from collections import deque

import cftime
import numpy as np
import pandas as pd
import xarray as xr
from xarray.core.utils import is_scalar


def is_datetime_type(arr: np.ndarray) -> bool:
    """
    Checks if array elements are datetime objects or cftime objects.

    Parameters
    ----------
    arr : np.ndarray
        Array to check for datetime type.

    Returns
    -------
    bool
        True if the array contains datetime or cftime objects, False otherwise.

    Examples
    --------
    >>> import numpy as np
    >>> import pandas as pd
    >>> import cftime
    >>> # Test with pandas datetime64
    >>> time_arr = np.array(pd.date_range('2000-01-01', periods=3))
    >>> print(is_datetime_type(time_arr))
    True
    >>> # Test with cftime datetime
    >>> cftime_arr = np.array([cftime.DatetimeNoLeap(2000, 1, 1)])
    >>> print(is_datetime_type(cftime_arr))
    True
    >>> # Test with non-datetime array
    >>> int_arr = np.array([1, 2, 3])
    >>> print(is_datetime_type(int_arr))
    False
    """
    return isinstance(arr.item(0), tuple(cftime._cftime.DATE_TYPES.values())) or np.issubdtype(arr.dtype, np.datetime64)


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

    Examples
    --------
    >>> import xarray as xr
    >>> import pandas as pd
    >>> import numpy as np
    >>> # INPUT: Dataset with standard 'time' coordinate
    >>> ds = xr.Dataset(
    ...     {'temp': ('time', [15.0, 16.0, 17.0])},
    ...     coords={'time': pd.date_range('2000-01-01', periods=3)}
    ... )
    >>> # OUTPUT: Returns the time coordinate name
    >>> print(get_time_label(ds))
    time
    >>> # INPUT: Dataset with non-standard time coordinate name 'T'
    >>> ds_T = xr.Dataset(
    ...     {'temp': ('T', [20.0, 21.0])},
    ...     coords={'T': pd.date_range('2000-01-01', periods=2)}
    ... )
    >>> # OUTPUT: Finds 'T' as the time coordinate
    >>> print(get_time_label(ds_T))
    T
    >>> # INPUT: Dataset without datetime coordinate
    >>> ds_no_time = xr.Dataset(
    ...     {'data': ('x', [1, 2, 3])},
    ...     coords={'x': [10, 20, 30]}
    ... )
    >>> # OUTPUT: Returns None when no time coordinate exists
    >>> print(get_time_label(ds_no_time))
    None
    >>> # INPUT: DataArray with time coordinate
    >>> da = xr.DataArray(
    ...     np.ones(5),
    ...     coords={'time': pd.date_range('2000-01-01', periods=5)},
    ...     dims=['time']
    ... )
    >>> # OUTPUT: Works with DataArrays too
    >>> print(get_time_label(da))
    time
    """
    label = deque()
    for name, coord in ds.coords.items():
        if not is_datetime_type(coord):
            continue
        if not coord.dims:
            continue
        if name in coord.dims:
            label.appendleft(name)
        else:
            label.append(name)
    label.append(None)
    return label.popleft()


def has_time_axis(ds) -> bool:
    """
    Checks if the given dataset has a time axis.

    Parameters
    ----------
    ds : xarray.Dataset or xarray.DataArray
        The dataset to check.

    Returns
    -------
    bool
        True if the dataset has a time axis, False otherwise.

    Examples
    --------
    >>> import xarray as xr
    >>> import pandas as pd
    >>> import numpy as np
    >>> # INPUT: Dataset with time coordinate
    >>> ds_with_time = xr.Dataset(
    ...     {'temperature': ('time', [15.0, 16.0, 17.0])},
    ...     coords={'time': pd.date_range('2000-01-01', periods=3)}
    ... )
    >>> # OUTPUT: Returns True when time axis exists
    >>> print(has_time_axis(ds_with_time))
    True
    >>> # INPUT: Dataset without time coordinate
    >>> ds_no_time = xr.Dataset(
    ...     {'data': ('x', [1, 2, 3])},
    ...     coords={'x': [10, 20, 30]}
    ... )
    >>> # OUTPUT: Returns False when no time axis exists
    >>> print(has_time_axis(ds_no_time))
    False
    >>> # INPUT: DataArray with time dimension
    >>> da = xr.DataArray(
    ...     np.random.rand(10, 5),
    ...     coords={'time': pd.date_range('2000-01-01', periods=10), 'lat': range(5)},
    ...     dims=['time', 'lat']
    ... )
    >>> # OUTPUT: Works with DataArrays
    >>> print(has_time_axis(da))
    True
    """
    return bool(get_time_label(ds))


def needs_resampling(ds, timespan):
    """
    Checks if a given dataset needs resampling based on its time axis.

    Parameters
    ----------
    ds : xr.Dataset or xr.DataArray
        The dataset to check.
    timespan : str
        The time span for which the dataset is to be resampled.
        10YS, 1YS, 6MS, etc.

    Returns
    -------
    bool
        True if the dataset needs resampling, False otherwise.

    Notes
    -----
    After time-averaging step, this function aids in determining if
    splitting into multiple files is required based on provided
    timespan.

    Examples
    --------
    >>> import xarray as xr
    >>> import pandas as pd
    >>> # INPUT: Dataset spanning 25 years, checking if it needs splitting by 10-year chunks
    >>> ds_long = xr.Dataset(
    ...     {'temp': ('time', range(25))},
    ...     coords={'time': pd.date_range('2000-01-01', periods=25, freq='YS')}
    ... )
    >>> # OUTPUT: Returns True because data spans more than 10 years
    >>> print(needs_resampling(ds_long, '10YS'))
    True
    >>> # INPUT: Same dataset, checking with 30-year timespan
    >>> # OUTPUT: Returns False because data fits within 30 years
    >>> print(needs_resampling(ds_long, '30YS'))
    False
    >>> # INPUT: Short dataset (3 years), checking 10-year timespan
    >>> ds_short = xr.Dataset(
    ...     {'temp': ('time', range(3))},
    ...     coords={'time': pd.date_range('2000-01-01', periods=3, freq='YS')}
    ... )
    >>> # OUTPUT: Returns False because data fits within timespan
    >>> print(needs_resampling(ds_short, '10YS'))
    False
    >>> # INPUT: Dataset with None timespan
    >>> # OUTPUT: Returns False when timespan is None
    >>> print(needs_resampling(ds_long, None))
    False
    >>> # INPUT: Dataset without time coordinate
    >>> ds_no_time = xr.Dataset({'data': ('x', [1, 2, 3])})
    >>> # OUTPUT: Returns False when no time axis exists
    >>> print(needs_resampling(ds_no_time, '10YS'))
    False
    """
    if (timespan is None) or (not timespan):
        return False
    time_label = get_time_label(ds)
    if time_label is None:
        return False
    if is_scalar(ds[time_label]):
        return False
    # string representation is need to deal with cftime
    start = pd.Timestamp(str(ds[time_label].data[0]))
    end = pd.Timestamp(str(ds[time_label].data[-1]))
    offset = pd.tseries.frequencies.to_offset(timespan)
    return (start + offset) < end


def freq_is_coarser_than_data(
    freq: str,
    ds: xr.Dataset,
    ref_time: pd.Timestamp = pd.Timestamp("1970-01-01"),
) -> bool:
    """
    Checks if the frequency is coarser than the time frequency of the xarray Dataset.

    Parameters
    ----------
    freq : str
        The frequency to compare (e.g. 'M', 'D', '6H').
    ds : xr.Dataset
        The dataset containing a time coordinate.
    ref_time : pd.Timestamp, optional
        Reference timestamp used to convert frequency to a time delta. Defaults to the beginning of
        the Unix Epoch.

    Returns
    -------
    bool
        True if `freq` is coarser (covers a longer duration) than the dataset's frequency.

    Examples
    --------
    >>> import xarray as xr
    >>> import pandas as pd
    >>> # INPUT: Daily data, checking if monthly frequency is coarser
    >>> ds_daily = xr.Dataset(
    ...     {'temp': ('time', range(30))},
    ...     coords={'time': pd.date_range('2000-01-01', periods=30, freq='D')}
    ... )
    >>> # OUTPUT: Monthly is coarser than daily
    >>> print(freq_is_coarser_than_data('MS', ds_daily))
    True
    >>> # INPUT: Same daily data, checking if hourly frequency is coarser
    >>> # OUTPUT: Hourly is finer than daily (not coarser)
    >>> print(freq_is_coarser_than_data('H', ds_daily))
    False
    >>> # INPUT: Hourly data, checking if daily frequency is coarser
    >>> ds_hourly = xr.Dataset(
    ...     {'temp': ('time', range(48))},
    ...     coords={'time': pd.date_range('2000-01-01', periods=48, freq='H')}
    ... )
    >>> # OUTPUT: Daily is coarser than hourly
    >>> print(freq_is_coarser_than_data('D', ds_hourly))
    True
    >>> # INPUT: Monthly data, checking if yearly frequency is coarser
    >>> ds_monthly = xr.Dataset(
    ...     {'temp': ('time', range(24))},
    ...     coords={'time': pd.date_range('2000-01-01', periods=24, freq='MS')}
    ... )
    >>> # OUTPUT: Yearly is coarser than monthly
    >>> print(freq_is_coarser_than_data('YS', ds_monthly))
    True
    """
    time_label = get_time_label(ds)
    if time_label is None:
        raise ValueError("The dataset does not contain a valid time coordinate.")
    time_index = ds.indexes[time_label]

    data_freq = pd.infer_freq(time_index)
    if data_freq is None:
        raise ValueError("Could not infer frequency from the dataset's time coordinate.")

    delta1 = (ref_time + pd.tseries.frequencies.to_offset(freq)) - ref_time
    delta2 = (ref_time + pd.tseries.frequencies.to_offset(data_freq)) - ref_time

    return delta1 > delta2
