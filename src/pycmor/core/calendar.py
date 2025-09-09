"""
Yet another calendar implementation.

This module provides functions for creating date ranges.

The main components of this module are:

- ``year_bounds_major_digits``: generates a list of year ranges (bounds) where each range starts with a specific digit.
- ``date_ranges_from_bounds``: creates a list of date indexes from bounds
- ``date_ranges_from_year_bounds``: creates a list of date indexes from year bounds
- ``simple_ranges_from_bounds``: creates a list of simple ranges from bounds

Examples
--------
>>> year_bounds = year_bounds_major_digits(2000, 2010, 2, 2)
>>> print(year_bounds)
[[2000, 2001], [2002, 2003], [2004, 2005], [2006, 2007], [2008, 2009], [2010, 2010]]
"""

import pendulum
import xarray as xr

from .logging import logger


def year_bounds_major_digits(first, last, step, binning_digit, return_type=int):
    """
    Generate year ranges with a specific first digit.

    This function generates a list of year ranges (bounds) where each range starts
    with a specific digit (binning_digit). The ranges are generated from a given start
    year (first) to an end year (last) with a specific step size.

    Parameters
    ----------
    first : int
        The first year in the range.
    last : int
        The last year in the range.
    step : int
        The step size for the range.
    binning_digit : int
        The digit that each range should start with.
    return_type : type, optional
        The type of the elements in the returned list, either int or pendulum.DateTime. Defaults to int.

    Returns
    -------
    list
        A list of lists where each inner list is a range of years.

    Raises
    ------
    ValueError
        If the binning_digit is greater than 10.

    Examples
    --------
    >>> year_bounds_major_digits(2000, 2010, 2, 2)
    [[2000, 2001], [2002, 2003], [2004, 2005], [2006, 2007], [2008, 2009], [2010, 2010]]

    >>> year_bounds_major_digits(2000, 2010, 3, 3)
    [[2000, 2002], [2003, 2005], [2006, 2008], [2009, 2010]]

    Notes
    -----
    This function uses a while loop to iterate through the years from first to last.
    It checks the ones digit of the current year and compares it with the binning_digit
    to determine the start of a new range. If the first range is undersized (i.e., the
    binning_digit is in the ones digit of the first few years), the function will
    continue to increment the current year until it hits the binning_digit. If the
    first range is not undersized, the function will continue to increment the current
    year until it hits the next binning_digit. Once a range is completed, it is appended
    to the bounds list and the process continues until the last year is reached.
    """
    # NOTE(PG): This is a bit hacky and difficult to read, but all the tests pass...
    logger.debug(
        f"Running year_bounds_major_digits({first=}, {last=}, {step=}, {binning_digit=})"
    )
    if binning_digit >= 10:
        raise ValueError("Give a binning_digit less than 10")
    bounds = []
    current_location = bin_start = first
    first_bin_is_undersized = binning_digit in [
        i % 10 for i in range(first, first + step)
    ]
    bin_end = "underfull bin" if first_bin_is_undersized else bin_start + step
    logger.debug(f"first_bin_is_undersized: {first_bin_is_undersized}")
    first_bin_empty = True

    while current_location <= last:
        ones_digit = current_location % 10

        if first_bin_empty:
            if first_bin_is_undersized:
                # Go until you hit the binning digit
                if ones_digit != binning_digit:
                    current_location += 1
                    ones_digit = current_location % 10
                else:
                    bounds.append([bin_start, current_location - 1])
                    logger.debug(
                        f"Appending bounds {bin_start=}, {current_location-1=}"
                    )
                    first_bin_empty = False
                    bin_start = current_location
            else:
                # Go until you hit the next binning digit
                if ones_digit == binning_digit:
                    bounds.append([bin_start, current_location - 1])
                    logger.debug(
                        f"Appending bounds {bin_start=}, {current_location-1=}"
                    )
                    first_bin_empty = False
                    bin_start = current_location
                else:
                    current_location += 1
        else:
            bin_end = bin_start + step
            current_location += 1
            if current_location == bin_end or current_location > last:
                bounds.append([bin_start, min(current_location - 1, last)])
                logger.debug(
                    f"Appending bounds {bin_start=}, {min(current_location-1, last)=}"
                )
                bin_start = current_location
    if return_type is int:
        return [[int(i) for i in bound] for bound in bounds]
    elif return_type is pendulum.DateTime:
        return [[pendulum.datetime(int(i), 1, 1) for i in bound] for bound in bounds]
    else:
        raise ValueError("return_type must be either int or pendulum.DateTime")


def date_ranges_from_bounds(bounds, freq: str = "M", **kwargs):
    """
    Class method to create a list of instances from a list of start and end bounds.

    Parameters
    ----------
    bounds : list of tuple of str or datetime-like
        A list of strings or datetime-like tuples each containing a start and end bound.
    freq : str, optional
        The frequency of the periods. Defaults to one month.
    **kwargs :
        Additional keyword arguments to pass to the date_range function.

    Returns
    -------
    tuple
        A tuple containing instances of the class for each provided bound.

    Examples
    --------
    >>> bounds = [("2020-01-01", "2020-12-31")]
    >>> date_ranges_from_bounds(bounds, freq="M")
    DatetimeIndex(['2020-01-31', '2020-02-29', ..., '2020-12-31'], dtype='datetime64[ns]', freq='ME')
    """
    objs = []
    for start, end in bounds:
        objs.append(xr.date_range(start=start, end=end, freq=freq, **kwargs))
    if len(objs) == 1:
        return objs[0]
    return (*objs,)


def date_ranges_from_year_bounds(year_bounds, freq: str = "M", **kwargs):
    """
    Class method to create a list of instances from a list of year bounds.

    Parameters
    ----------
    year_bounds : list of lists or tuples
        A list of lists, each containing a start and end year.
    freq : str, optional
        The frequency of the periods. Defaults to one month.
    **kwargs :
        Additional keyword arguments to pass to the date_range function.
    """
    bounds = [
        (pendulum.datetime(start, 1, 1), pendulum.datetime(end, 12, 31))
        for start, end in year_bounds
    ]
    return date_ranges_from_bounds(bounds, freq, **kwargs)


def simple_ranges_from_bounds(bounds):
    """
    Create a list of simple ranges from a list of bounds.
    """
    if len(bounds) == 1:
        start, end = bounds[0]
        return range(start, end + 1)
    return [range(start, end + 1) for start, end in bounds]


def assign_time_axis(da: xr.DataArray, taxis):
    return da.assign_coords(time=taxis)
