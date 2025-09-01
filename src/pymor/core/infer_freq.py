import warnings
from collections import namedtuple

import numpy as np
import pandas as pd
import xarray as xr
from xarray.core.extensions import (
    register_dataarray_accessor,
    register_dataset_accessor,
)

from .logging import logger
from .time_utils import get_time_label

# Result object for frequency inference with metadata
FrequencyResult = namedtuple(
    "FrequencyResult",
    [
        "frequency",  # str or None: inferred frequency string (e.g., 'M', '2D')
        "delta_days",  # float or None: median time delta in days
        "step",  # int or None: step multiplier for the frequency
        "is_exact",  # bool: whether the time series has exact regular spacing
        "status",  # str: status message ('valid', 'irregular', 'no_match', etc.)
    ],
)


def _convert_cftime_to_ordinals(times_values):
    """Convert cftime objects to ordinal values."""
    ref_date = times_values[0]
    ordinals = np.array(
        [
            (t - ref_date).days + (t.hour / 24 + t.minute / 1440 + t.second / 86400)
            for t in times_values
        ]
    )

    # Adjust to make ordinals absolute (add reference ordinal)
    try:
        ref_ordinal = ref_date.toordinal()
        ordinals = ordinals + ref_ordinal
    except (AttributeError, ValueError):
        # If toordinal fails, use a simpler approach
        ordinals = np.array(
            [
                t.year * 365.25
                + t.month * 30.4375
                + t.day
                + t.hour / 24
                + t.minute / 1440
                + t.second / 86400
                for t in times_values
            ]
        )
    return ordinals


def _convert_standard_datetime_to_ordinals(times_values):
    """Convert standard datetime objects to ordinal values."""
    return np.array(
        [
            t.toordinal() + t.hour / 24 + t.minute / 1440 + t.second / 86400
            for t in times_values
        ]
    )


def _convert_numeric_timestamps_to_ordinals(times_values):
    """Convert numeric timestamps (e.g., numpy.datetime64) to ordinal values."""
    return np.array([pd.Timestamp(t).to_julian_date() for t in times_values])


def _convert_times_to_ordinals(times_values):
    """
    Convert various datetime types to ordinal values for frequency analysis.

    This function handles three main datetime types:
    1. cftime objects (with calendar attribute)
    2. Standard datetime objects (with toordinal method)
    3. Numeric timestamps (numpy.datetime64, etc.)

    Parameters
    ----------
    times_values : array-like
        Array of datetime-like objects

    Returns
    -------
    np.ndarray
        Array of ordinal values representing the datetime objects
    """
    if hasattr(times_values[0], "toordinal"):
        if hasattr(times_values[0], "calendar"):
            # cftime objects - convert to days since a reference date
            return _convert_cftime_to_ordinals(times_values)
        else:
            # Standard datetime objects
            return _convert_standard_datetime_to_ordinals(times_values)
    else:
        # Assume numeric timestamps (e.g., numpy.datetime64)
        return _convert_numeric_timestamps_to_ordinals(times_values)


# Core frequency inference
def _infer_frequency_core(
    times, tol=0.05, return_metadata=False, strict=False, calendar="standard", log=False
):
    """
    Infer time frequency from datetime-like array, returning pandas-style frequency strings.

    Parameters
    ----------
    times : array-like
        List of datetime-like objects (cftime or datetime64).
    tol : float, optional
        Tolerance for delta comparisons (in days). Defaults to 0.05.
    return_metadata : bool, optional
        If True, returns (frequency, median_delta, step, is_exact, status)
        instead of just the frequency string. Defaults to False.
    strict : bool, optional
        If True, performs additional checks for irregular time series and
        returns a status message. Defaults to False.
    calendar : str, optional
        Calendar type to use for cftime objects. Defaults to "standard".
    log : bool, optional
        If True, logs the results of the frequency check. Defaults to False.

    Returns
    -------
    str or FrequencyResult
        Inferred frequency string (e.g., 'M') or
        (freq, delta, step, is_exact, status) if return_metadata=True.
    """
    if len(times) < 2:
        if log:
            log_frequency_check(
                "Time Series", None, None, None, False, "too_short", strict
            )
        return (
            FrequencyResult(None, None, None, False, "too_short")
            if return_metadata
            else None
        )

    # Handle both pandas-like objects (with .values) and plain lists/arrays
    try:
        times_values = times.values if hasattr(times, "values") else times
        ordinals = _convert_times_to_ordinals(times_values)

    except (AttributeError, TypeError, ValueError) as e:
        error_status = f"invalid_input: {str(e)}"
        if log:
            log_frequency_check(
                "Time Series", None, None, None, False, error_status, strict
            )
        if return_metadata:
            return FrequencyResult(None, None, None, False, error_status)
        return None
    deltas = np.diff(ordinals)
    median_delta = np.median(deltas)
    std_delta = np.std(deltas)

    days_in_calendar_year = {
        "standard": 365.25,
        "gregorian": 365.25,
        "noleap": 365.0,
        "360_day": 360.0,
    }.get(calendar, 365.25)

    base_freqs = {
        "H": 1 / 24,
        "D": 1,
        "W": 7,
        "M": days_in_calendar_year / 12,
        "Q": days_in_calendar_year / 4,
        "A": days_in_calendar_year,
        "10A": days_in_calendar_year * 10,
    }

    matched_freq = None
    matched_step = None
    for freq, base_days in base_freqs.items():
        for step in range(1, 13):
            test_delta = base_days * step
            if abs(median_delta - test_delta) <= tol * test_delta:
                matched_freq = freq
                matched_step = step
                break
        if matched_freq:
            break

    if matched_freq is None:
        # For irregular time series, try to find the closest match with relaxed tolerance
        relaxed_tol = 0.5  # Much more relaxed tolerance for irregular data
        for freq, base_days in base_freqs.items():
            for step in range(1, 13):
                test_delta = base_days * step
                if abs(median_delta - test_delta) <= relaxed_tol * test_delta:
                    matched_freq = freq
                    matched_step = step
                    break
            if matched_freq:
                break

        if matched_freq is None:
            if log:
                log_frequency_check(
                    "Time Series", None, median_delta, None, False, "no_match", strict
                )
            return (
                FrequencyResult(None, median_delta, None, False, "no_match")
                if return_metadata
                else None
            )

    is_exact = std_delta < tol * (base_freqs[matched_freq] * matched_step)
    status = "valid" if is_exact else "irregular"

    if strict:
        expected_steps = (ordinals[-1] - ordinals[0]) / (
            base_freqs[matched_freq] * matched_step
        )
        actual_steps = len(times) - 1
        if not np.all(np.abs(deltas - median_delta) <= tol * median_delta):
            status = "irregular"
            is_exact = False  # Fix: Update is_exact to be consistent
        if abs(expected_steps - actual_steps) >= 1:
            status = "missing_steps"
            is_exact = False  # Fix: Update is_exact to be consistent

    freq_str = f"{matched_step}{matched_freq}" if matched_step > 1 else matched_freq

    # Log the results if requested
    if log:
        log_frequency_check(
            "Time Series",
            freq_str,
            median_delta,
            matched_step,
            is_exact,
            status,
            strict,
        )

    return (
        FrequencyResult(freq_str, median_delta, matched_step, is_exact, status)
        if return_metadata
        else freq_str
    )


# xarray fallback
def infer_frequency(
    times, return_metadata=False, strict=False, calendar="standard", log=False
):
    """
    Infer time frequency from datetime-like array, returning pandas-style frequency strings.

    Parameters
    ----------
    times : array-like
        List of datetime-like objects (cftime or datetime64).
    return_metadata : bool, optional
        If True, returns (frequency, median_delta, step, is_exact, status)
        instead of just the frequency string. Defaults to False.
    strict : bool, optional
        If True, performs additional checks for irregular time series and
        returns a status message. Defaults to False.
    calendar : str, optional
        Calendar type to use for cftime objects. Defaults to "standard".
    log : bool, optional
        If True, logs the results of the frequency check. Defaults to False.

    Returns
    -------
    str or FrequencyResult
        Inferred frequency string (e.g., 'M') or (freq, delta, step, is_exact, status)
        if return_metadata=True.
    """
    # Extract values from xarray objects if needed
    if hasattr(times, "values"):
        times_values = times.values
    else:
        times_values = times
    try:
        freq = xr.infer_freq(times_values)
        if freq is not None:
            if log:
                log_frequency_check("Time Series", freq, None, 1, True, "valid", strict)
            return (
                FrequencyResult(freq, None, 1, True, "valid")
                if return_metadata
                else freq
            )
    except Exception:
        pass
    return _infer_frequency_core(
        times_values,
        return_metadata=return_metadata,
        strict=strict,
        calendar=calendar,
        log=log,
    )


# Logger
def log_frequency_check(name, freq, delta, step, exact, status, strict=False):
    """
    Log the results of the frequency check.
    """
    logger.info(f"[Freq Check] {name}")
    logger.info(f"  → Inferred Frequency : {freq or 'None'}")
    logger.info(f"  → Step Multiple      : {step or 'None'}")

    # Handle None delta values safely
    if delta is not None:
        logger.info(f"  → Median Δ (days)    : {delta:.2f}")
    else:
        logger.info("  → Median Δ (days)    : None")

    logger.info(f"  → Regular Spacing    : {'✅' if exact else '❌'}")
    logger.info(f"  → Strict Mode        : {'✅' if strict else '❌'}")
    logger.info(f"  → Status             : {status}")
    logger.info("-" * 40)


def approx_interval_to_frequency_str(approx_interval, tolerance=0.1):
    """
    Convert an approximate interval in days to a pandas-style frequency string.

    This function uses algorithmic logic to determine the most appropriate frequency
    string based on common time patterns, rather than hardcoded mappings. It handles
    sub-daily, daily, weekly, monthly, and yearly frequencies intelligently.

    Parameters
    ----------
    approx_interval : float
        Approximate interval in days
    tolerance : float, optional
        Relative tolerance for matching standard frequencies, by default 0.1 (10%)

    Returns
    -------
    str or None
        Pandas-style frequency string (e.g., 'D', 'M', '3M', 'Y') or None for
        time-invariant data (0.0 days)

    Examples
    --------
    >>> approx_interval_to_frequency_str(1.0)  # Daily
    'D'
    >>> approx_interval_to_frequency_str(30.0)  # Monthly
    'M'
    >>> approx_interval_to_frequency_str(91.3)  # 3-Monthly (approx)
    '3M'
    >>> approx_interval_to_frequency_str(365.0)  # Yearly
    'Y'
    >>> approx_interval_to_frequency_str(0.041667)  # Hourly
    'H'
    """
    # Handle special case: time-invariant/fixed data
    if approx_interval == 0.0:
        return None

    # Define standard reference intervals for common frequencies
    MINUTES_PER_DAY = 24 * 60
    HOURS_PER_DAY = 24
    DAYS_PER_WEEK = 7
    DAYS_PER_MONTH = 30.0  # CMIP6 standard
    DAYS_PER_YEAR = 365.0  # CMIP6 standard

    def is_close(value, target, tolerance):
        """Check if value is within relative tolerance of target."""
        if target == 0:
            return value == 0
        return abs(value - target) / target <= tolerance

    # 1. Sub-daily frequencies (< 1 day)
    if approx_interval < 1.0:
        # Convert to hours
        hours = approx_interval * HOURS_PER_DAY

        # Check for common hourly frequencies
        for h in [1, 2, 3, 4, 6, 8, 12]:
            if is_close(hours, h, tolerance):
                return f"{h}H" if h > 1 else "H"

        # Check for sub-hourly (minutes)
        minutes = approx_interval * MINUTES_PER_DAY

        # Common sub-hourly intervals
        for m in [15, 20, 25, 30, 45]:
            if is_close(minutes, m, tolerance):
                return f"{m}T"

        # Fall back to rounded hours or minutes
        if hours >= 1:
            return f"{int(round(hours))}H"
        else:
            return f"{int(round(minutes))}T"

    # 2. Daily frequencies (1-6 days)
    elif approx_interval < DAYS_PER_WEEK:
        days = round(approx_interval)
        return "D" if days == 1 else f"{days}D"

    # 3. Weekly frequencies (7-27 days)
    elif approx_interval < DAYS_PER_MONTH - 3:  # ~27 days
        weeks = approx_interval / DAYS_PER_WEEK

        # Check for exact weekly matches
        for w in [1, 2]:
            if is_close(weeks, w, tolerance):
                return "W" if w == 1 else f"{w}W"

        # Fall back to days
        days = int(round(approx_interval))
        return f"{days}D"

    # 4. Monthly frequencies (28-400 days)
    elif approx_interval < 400:
        # First check if it's close to a year (prioritize yearly over 12M)
        years = approx_interval / DAYS_PER_YEAR
        if is_close(years, 1, tolerance):
            return "Y"

        months = approx_interval / DAYS_PER_MONTH

        # Check for common monthly frequencies (excluding 12 since we handle yearly above)
        for m in [1, 2, 3, 4, 5, 6, 9]:
            if is_close(months, m, tolerance):
                return "M" if m == 1 else f"{m}M"

        # Fall back to rounded months
        months_rounded = int(round(months))
        if months_rounded >= 1:
            return "M" if months_rounded == 1 else f"{months_rounded}M"
        else:
            # Very close to monthly but not quite - use days
            days = int(round(approx_interval))
            return f"{days}D"

    # 5. Yearly and longer frequencies (> 400 days)
    else:
        years = approx_interval / DAYS_PER_YEAR

        # Check for common yearly frequencies
        for y in [1, 2, 5, 10, 20, 50, 100]:
            if is_close(years, y, tolerance):
                return "Y" if y == 1 else f"{y}Y"

        # Fall back to rounded years
        years_rounded = int(round(years))
        if years_rounded >= 1:
            return "Y" if years_rounded == 1 else f"{years_rounded}Y"
        else:
            # Less than a year but more than 400 days - use days
            days = int(round(approx_interval))
            return f"{days}D"


# Compare with CMIP6 approx_interval
def is_resolution_fine_enough(
    times,
    target_approx_interval,
    calendar="standard",
    strict=True,
    tolerance=0.01,
    log=True,
):
    """
    Determines if the temporal resolution of a time series is sufficient for resampling.

    Parameters
    ----------
    times : list or array-like
        Array of datetime-like objects representing the time series.
    target_approx_interval : float
        Expected interval in days for the target frequency.
    calendar : str, optional
        Calendar type to use for cftime objects, by default "standard".
    strict : bool, optional
        If True, performs additional checks for irregular time series and
        includes status messages. Defaults to True.
    tolerance : float, optional
        Tolerance for comparing time intervals. Defaults to 0.01.
    log : bool, optional
        If True, logs the results of the frequency check. Defaults to True.

    Returns
    -------
    dict
        Contains the inferred interval, comparison status, validity for resampling,
        and status message.

    Notes
    -----
    The function infers the frequency using `infer_frequency` and compares it
    against the target interval, considering the specified tolerance. The result
    includes a status indicating whether the time series is suitable for resampling.
    """

    result = infer_frequency(
        times, return_metadata=True, strict=strict, calendar=calendar, log=False
    )

    if result is None:
        if log:
            logger.error("[Temporal Resolution Check]")
            logger.error("  → Error: Could not infer frequency from time data")
            logger.error("-" * 40)
        return {
            "inferred_interval": None,
            "comparison_status": "unknown",
            "is_valid_for_resampling": False,
        }

    freq = result.frequency
    delta = result.delta_days
    exact = result.is_exact
    status = result.status

    if delta is None:
        if log:
            logger.warning("[Temporal Resolution Check]")
            logger.warning(f"  → Inferred Frequency     : {freq or 'unknown'}")
            logger.warning(f"  → Status                 : {status}")
            logger.warning(
                "  → Valid for Resampling   : ❌ (could not determine time delta)"
            )
            logger.warning("-" * 40)
        return {
            "inferred_interval": None,
            "comparison_status": status,
            "is_valid_for_resampling": False,
        }

    comparison_status = status
    if not exact or status in ("irregular", "missing_steps"):
        is_valid = False
    elif delta < target_approx_interval - tolerance:
        comparison_status = "finer"
        is_valid = True
    elif abs(delta - target_approx_interval) <= tolerance:
        comparison_status = "equal"
        is_valid = True
    else:
        comparison_status = "coarser"
        is_valid = False

    if log:
        target_freq_str = approx_interval_to_frequency_str(target_approx_interval)
        target_display = f"{target_approx_interval:.4f} days"
        if target_freq_str:
            target_display += f" (~{target_freq_str})"

        logger.info("[Temporal Resolution Check]")
        logger.info(
            f"  → Inferred Frequency     : {freq or 'unknown'} (Δ ≈ {delta:.4f} days)"
        )
        logger.info(f"  → Target Approx Interval : {target_display}")
        logger.info(f"  → Comparison Status      : {comparison_status}")
        logger.info(f"  → Valid for Resampling   : {'✅' if is_valid else '❌'}")
        if status not in (None, "valid"):
            logger.info(f"  → Status Message        : {status}")
        logger.info("-" * 40)

    return {
        "inferred_interval": delta,
        "comparison_status": comparison_status,
        "is_valid_for_resampling": is_valid,
        "status": status,
    }


# xarray accessor is named "timefreq" at the moment instead of "pymor" as
# project name is not yet finalized.


@register_dataarray_accessor("timefreq")
class TimeFrequencyAccessor:
    def __init__(self, xarray_obj):
        self._obj = xarray_obj

    def infer_frequency(
        self,
        strict=False,
        calendar="standard",
        log=True,
        time_dim=None,
        return_metadata=True,
    ):
        """
        Infer time frequency from datetime-like array, returning pandas-style
        frequency strings.

        Parameters
        ----------
        strict : bool, optional
            If True, performs additional checks for irregular time series and
            returns a status message. Defaults to False.
        calendar : str, optional
            Calendar type to use for cftime objects. Defaults to "standard".
        log : bool, optional
            If True, logs the results of the frequency check. Defaults to False.
        time_dim : str, optional
            Name of the time dimension in the DataArray. If None, automatically
            detects the time dimension using `get_time_label`. Defaults to None.
        return_metadata : bool, optional
            If True, returns (freq, delta, step, is_exact, status)
            instead of just the frequency string. Defaults to True.
        Returns
        -------
        str or FrequencyResult
            Inferred frequency string (e.g., 'M') or
            (freq, delta, step, is_exact, status) if return_metadata=True.
        """
        # Auto-detect time dimension if not provided
        if time_dim is None:
            time_dim = get_time_label(self._obj)
            if time_dim is None:
                raise ValueError(
                    "No datetime coordinate found in DataArray."
                    " Please specify time_dim manually."
                )

        # Check if this is a DataArray with time coordinates or a time coordinate itself
        if hasattr(self._obj, "dims") and time_dim in self._obj.dims:
            # This is a DataArray with a time dimension - get the time coordinate
            times = self._obj.coords[time_dim].values
        else:
            # This is likely a time coordinate DataArray itself
            times = self._obj.values

        result = infer_frequency(
            times,
            return_metadata=True,
            strict=strict,
            calendar=calendar,
            log=False,
        )
        if log:
            log_frequency_check(
                self._obj.name or "Unnamed Time Axis",
                result.frequency,
                result.delta_days,
                result.step,
                result.is_exact,
                result.status,
                strict,
            )
        if return_metadata:
            return result
        else:
            return result.frequency

    def check_resolution(
        self,
        target_approx_interval,
        calendar="standard",
        strict=True,
        tolerance=0.01,
        log=True,
        time_dim=None,
    ):
        """
        Check if the time resolution is fine enough for resampling.

        Parameters
        ----------
        target_approx_interval : float
            Expected interval in days for the target frequency
        calendar : str, optional
            Calendar type, by default "standard"
        strict : bool, optional
            If True, performs additional checks for irregular time series and
            returns a status message. Defaults to True.
        tolerance : float, optional
            Tolerance for time interval comparison, by default 0.01
        log : bool, optional
            If True, logs the results of the frequency check. Defaults to True.
        time_dim : str, optional
            Name of the time dimension. If None, automatically detects
            the time dimension using get_time_label. Defaults to None.

        Returns
        -------
        dict
            Dictionary containing the inferred interval, comparison status,
            and validity for resampling.
        """
        # Auto-detect time dimension if not provided
        if time_dim is None:
            time_dim = get_time_label(self._obj)
            if time_dim is None:
                raise ValueError(
                    "No datetime coordinate found in DataArray."
                    " Please specify time_dim manually."
                )

        # Check if this is a DataArray with time coordinates or a time coordinate itself
        if hasattr(self._obj, "dims") and time_dim in self._obj.dims:
            # This is a DataArray with a time dimension - get the time coordinate
            times = self._obj.coords[time_dim].values
        else:
            # This is likely a time coordinate DataArray itself
            times = self._obj.values

        return is_resolution_fine_enough(
            times, target_approx_interval, calendar, strict, tolerance, log
        )

    def resample_safe(
        self,
        target_approx_interval=None,
        freq_str=None,
        calendar="standard",
        method="mean",
        time_dim=None,
        tolerance=0.01,
        **resample_kwargs,
    ):
        """Safely resample time series data after checking temporal resolution.

        Users can specify the target frequency in two ways:
        1. Provide target_approx_interval (float in days) - will be converted to freq_str
        2. Provide freq_str (pandas frequency string) - used directly for resampling

        If both are provided, freq_str takes precedence for resampling, and
        target_approx_interval is used for validation.

        Parameters
        ----------
        target_approx_interval : float, optional
            Expected interval in days for the target frequency. If provided without
            freq_str, this will be converted to an appropriate frequency string.
            If provided with freq_str, this is used for validation only.
        freq_str : str, optional
            Target frequency string (e.g., 'M' for monthly, '3H' for 3-hourly).
            If provided, this takes precedence for resampling operations.
        calendar : str, optional
            Calendar type, by default "standard"
        method : str or dict, optional
            Resampling method, by default "mean"
        time_dim : str, optional
            Name of the time dimension. If None, automatically detects
            the time dimension using get_time_label. Defaults to None.
        tolerance : float, optional
            Tolerance for time interval comparison, by default 0.01
        **resample_kwargs
            Additional arguments passed to xarray's resample

        Returns
        -------
        xarray.DataArray
            Resampled data

        Raises
        ------
        ValueError
            If neither target_approx_interval nor freq_str is provided, or if the
            time resolution is too coarse for the target frequency

        Examples
        --------
        # Using approximate interval (will be converted to frequency string)
        data.timefreq.resample_safe(target_approx_interval=30.0)  # ~monthly

        # Using frequency string directly
        data.timefreq.resample_safe(freq_str='3M')  # 3-monthly

        # Using both (freq_str used for resampling, target_approx_interval for validation)
        data.timefreq.resample_safe(target_approx_interval=90.0, freq_str='3M')
        """
        warnings.warn("resample_safe is incomplete, use resample instead", stacklevel=1)
        # Validate input arguments
        if target_approx_interval is None and freq_str is None:
            raise ValueError(
                "Either target_approx_interval or freq_str must be provided"
            )

        # Determine the frequency string to use for resampling
        if freq_str is not None:
            # Validate the provided frequency string
            try:
                # Test if pandas can understand the frequency string
                pd.Timedelta(freq_str)
            except (ValueError, TypeError):
                # Try with a simple date range to validate frequency string
                try:
                    pd.date_range("2000-01-01", periods=2, freq=freq_str)
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Invalid frequency string '{freq_str}': {e}")

            resampling_freq = freq_str
        else:
            # Convert target_approx_interval to frequency string
            resampling_freq = approx_interval_to_frequency_str(target_approx_interval)
            if resampling_freq is None:
                raise ValueError(
                    f"Cannot convert target_approx_interval={target_approx_interval} "
                    "to a valid frequency string (possibly time-invariant data)"
                )

        # Auto-detect time dimension if not provided
        if time_dim is None:
            time_dim = get_time_label(self._obj)
            if time_dim is None:
                raise ValueError(
                    "No datetime coordinate found in DataArray."
                    " Please specify time_dim manually."
                )

        # Perform resolution check if target_approx_interval is provided
        if target_approx_interval is not None:
            check = self.check_resolution(
                target_approx_interval=target_approx_interval,
                calendar=calendar,
                strict=True,
                tolerance=tolerance,
                log=True,
            )

            if not check["is_valid_for_resampling"]:
                # For test compatibility, use the expected error message format
                raise ValueError("time resolution too coarse")

        # If we get here, it's safe to resample
        resampled = self._obj.resample({time_dim: resampling_freq}, **resample_kwargs)

        # Apply the specified method (mean, sum, etc.)
        if isinstance(method, str):
            resampled = getattr(resampled, method)()
        elif isinstance(method, dict):
            resampled = resampled.agg(method)
        else:
            raise ValueError(
                f"Unsupported method type: {type(method)}. Expected str or dict."
            )

        return resampled


@register_dataset_accessor("timefreq")
class DatasetFrequencyAccessor:
    def __init__(self, ds):
        self._ds = ds

    def infer_frequency(self, time_dim=None, **kwargs):
        """
        Infer time frequency from datetime-like array, returning pandas-style
        frequency strings.

        Parameters
        ----------
        time_dim : str, optional
            Name of the time dimension in the Dataset. If None, automatically
            detects the time dimension using get_time_label. Defaults to None.
        **kwargs
            Additional arguments passed to infer_frequency.

        Returns
        -------
        str or FrequencyResult
            Inferred frequency string (e.g., 'M') or
            (freq, delta, step, is_exact, status) if return_metadata=True.
        """
        # Auto-detect time dimension if not provided
        if time_dim is None:
            time_dim = get_time_label(self._ds)
            if time_dim is None:
                raise ValueError(
                    "No datetime coordinate found in Dataset."
                    " Please specify time_dim manually."
                )

        if time_dim not in self._ds:
            raise ValueError(f"Time dimension '{time_dim}' not found.")
        return self._ds[time_dim].timefreq.infer_frequency(time_dim=time_dim, **kwargs)

    def resample_safe(
        self,
        target_approx_interval=None,
        freq_str=None,
        time_dim=None,
        calendar="standard",
        method="mean",
        tolerance=0.01,
        **resample_kwargs,
    ):
        """Safely resample dataset time series data after checking temporal
        resolution.

        Users can specify the target frequency in two ways:
        1. Provide target_approx_interval (float in days) - will be converted to freq_str
        2. Provide freq_str (pandas frequency string) - used directly for resampling

        If both are provided, freq_str takes precedence for resampling, and
        target_approx_interval is used for validation.

        Parameters
        ----------
        target_approx_interval : float, optional
            Expected interval in days for the target frequency. If provided without
            freq_str, this will be converted to an appropriate frequency string.
            If provided with freq_str, this is used for validation only.
        freq_str : str, optional
            Target frequency string (e.g., 'M' for monthly, '3H' for 3-hourly).
            If provided, this takes precedence for resampling operations.
        time_dim : str, optional
            Name of the time dimension. If None, automatically detects
            the time dimension using get_time_label. Defaults to None.
        calendar : str, optional
            Calendar type, by default "standard"
        method : str or dict, optional
            Resampling method, by default "mean"
        tolerance : float, optional
            Tolerance for time interval comparison, by default 0.01
        **resample_kwargs
            Additional arguments passed to xarray's resample

        Returns
        -------
        xarray.Dataset
            Resampled dataset

        Raises
        ------
        ValueError
            If neither target_approx_interval nor freq_str is provided, or if the
            time resolution is too coarse for the target frequency

        Examples
        --------
        # Using approximate interval (will be converted to frequency string)
        dataset.timefreq.resample_safe(target_approx_interval=30.0)  # ~monthly

        # Using frequency string directly
        dataset.timefreq.resample_safe(freq_str='3M')  # 3-monthly

        # Using both (freq_str used for resampling, target_approx_interval for validation)
        dataset.timefreq.resample_safe(target_approx_interval=90.0, freq_str='3M')
        """
        # Validate input arguments
        if target_approx_interval is None and freq_str is None:
            raise ValueError(
                "Either target_approx_interval or freq_str must be provided"
            )

        # Determine the frequency string to use for resampling
        if freq_str is not None:
            # Validate the provided frequency string
            try:
                # Test if pandas can understand the frequency string
                pd.Timedelta(freq_str)
            except (ValueError, TypeError):
                # Try with a simple date range to validate frequency string
                try:
                    pd.date_range("2000-01-01", periods=2, freq=freq_str)
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Invalid frequency string '{freq_str}': {e}")

            resampling_freq = freq_str
        else:
            # Convert target_approx_interval to frequency string
            resampling_freq = approx_interval_to_frequency_str(target_approx_interval)
            if resampling_freq is None:
                raise ValueError(
                    f"Cannot convert target_approx_interval={target_approx_interval} "
                    "to a valid frequency string (possibly time-invariant data)"
                )

        # Auto-detect time dimension if not provided
        if time_dim is None:
            time_dim = get_time_label(self._ds)
            if time_dim is None:
                raise ValueError(
                    "No datetime coordinate found in Dataset."
                    " Please specify time_dim manually."
                )

        if time_dim not in self._ds:
            raise ValueError(f"Time dimension '{time_dim}' not found in dataset.")

        # Perform resolution check if target_approx_interval is provided
        if target_approx_interval is not None:
            check = self._ds[time_dim].timefreq.check_resolution(
                target_approx_interval=target_approx_interval,
                calendar=calendar,
                strict=True,
                tolerance=tolerance,
                log=True,
            )

            if not check["is_valid_for_resampling"]:
                # For test compatibility, use the expected error message format
                raise ValueError("time resolution too coarse")

        # If we get here, it's safe to resample the entire dataset
        resampled = self._ds.resample({time_dim: resampling_freq}, **resample_kwargs)

        # Apply the specified method (mean, sum, etc.)
        if isinstance(method, str):
            resampled_ds = getattr(resampled, method)()
        elif isinstance(method, dict):
            resampled_ds = resampled.agg(method)
        else:
            raise ValueError(
                f"Unsupported method type: {type(method)}. Expected str or dict."
            )

        return resampled_ds

    def check_resolution(self, target_approx_interval, time_dim=None, **kwargs):
        """
        Check if the time resolution is fine enough for resampling.

        Parameters
        ----------
        target_approx_interval : float
            Expected interval in days for the target frequency
        time_dim : str, optional
            Name of the time dimension. If None, automatically detects
            the time dimension using get_time_label. Defaults to None.
        **kwargs
            Additional arguments passed to check_resolution.

        Returns
        -------
        dict
            Dictionary containing the inferred interval, comparison status,
            and validity for resampling.
        """
        # Auto-detect time dimension if not provided
        if time_dim is None:
            time_dim = get_time_label(self._ds)
            if time_dim is None:
                raise ValueError(
                    "No datetime coordinate found in Dataset."
                    " Please specify time_dim manually."
                )

        if time_dim not in self._ds:
            raise ValueError(f"Time dimension '{time_dim}' not found.")
        return self._ds[time_dim].timefreq.check_resolution(
            target_approx_interval, **kwargs
        )
