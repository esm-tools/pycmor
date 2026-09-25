#!/usr/bin/env python
"""
Time Averaging
==============
This module contains functions for time averaging of data arrays.

The approximate interval for time averaging is prescribed in the CMOR tables,
using the key ``'approx_interval'``. This information is also provided
within the library.

Functions
---------
_get_time_method(frequency: str) -> str:
    Determine the time method based on the frequency string from
    rule.data_request_variable.frequency.

_frequency_from_approx_interval(interval: str) -> str:
    Convert an interval expressed in days to a frequency string.

timeavg(da: xr.DataArray, rule: Dict) -> xr.DataArray:
    Time averages data with respect to time-method (mean/climatology/instant.)

Module Variables
----------------
_IGNORED_CELL_METHODS : list
    List of cell_methods to ignore when calculating time averages.

"""

import re

import pandas as pd
import xarray as xr

from ..core.logging import logger


def _get_time_method(frequency: str) -> str:
    """
    Determine the time method based on the frequency string from CMIP6 table for
    a specific variable (rule.data_request_variable.frequency).

    The type of time method influences how the data is processed for time averaging.

    Parameters
    ----------
    frequency : str
        The frequency string from CMIP6 tables (example: "mon").

    Returns
    -------
    str
        The corresponding time method ('INSTANTANEOUS', 'CLIMATOLOGY', or 'MEAN').

    Examples
    --------
    >>> print(_get_time_method("mon"))
    MEAN

    >>> print(_get_time_method("day"))
    MEAN

    >>> print(_get_time_method("3hrPt"))
    INSTANTANEOUS

    >>> print(_get_time_method("6hrPt"))
    INSTANTANEOUS

    >>> print(_get_time_method("monC"))
    CLIMATOLOGY

    >>> print(_get_time_method("1hrCM"))
    CLIMATOLOGY
    """
    if frequency.endswith("Pt"):
        return "INSTANTANEOUS"
    if frequency.endswith("C") or frequency.endswith("CM"):
        return "CLIMATOLOGY"
    return "MEAN"


def _frequency_from_approx_interval(interval: str):
    """
    Convert an interval expressed in days to a frequency string.

    This function takes an interval expressed in days and converts it to a frequency string
    in a suitable time unit (decade, year, month, day, hour, minute, second, millisecond).
    The conversion is based on an approximate number of days for each time unit.

    Parameters
    ----------
    interval : str
        The interval expressed in days.

    Returns
    -------
    str
        The frequency string in a suitable time unit.

    Raises
    ------
    ValueError
        If the interval cannot be converted to a float.

    Examples
    --------
    >>> print(_frequency_from_approx_interval("1.0"))
    1D

    >>> print(_frequency_from_approx_interval("7"))
    7D

    >>> print(_frequency_from_approx_interval("30"))
    1MS

    >>> print(_frequency_from_approx_interval("365"))
    1YS

    >>> print(_frequency_from_approx_interval("0.125"))
    3h

    >>> print(_frequency_from_approx_interval("0.0416666"))
    1h

    >>> try:
    ...     _frequency_from_approx_interval("not_a_number")
    ... except ValueError as e:
    ...     print(f"Error: {e}")
    Error: Invalid interval: not_a_number
    """
    try:
        interval = float(interval)
    except ValueError:
        raise ValueError(f"Invalid interval: {interval}")
    # NOTE: A reference date is needed to calculate
    #       datetime diffs. We use the Unix epoch as
    #       an arbitrary reference date stamp:
    ref = pd.Timestamp("1970-01-01")
    dt = pd.Timedelta(interval, unit="d")
    dt = dt.round(freq="s")
    # handle special case of 60 days
    if dt.days == 60:
        dt = pd.Timedelta(59, unit="d")
    ts = ref + dt
    year = ts.year - ref.year
    # account leap years, add deficit days
    extra_days, reminder = divmod(year, 4)
    if extra_days or reminder:
        extra_days = extra_days + reminder // 2
        ts = ts + pd.Timedelta(extra_days, unit="d")
        year = ts.year - ref.year
    month = ts.month - ref.month
    day = ts.day - ref.day
    hour = ts.hour - ref.hour
    minute = ts.minute - ref.minute
    second = ts.second - ref.second
    result = []
    if year:
        result.append(f"{year}YS")
    if month:
        result.append(f"{month}MS")
    if day:
        if day == 30 and month == 0:
            result.append("1MS")
        else:
            result.append(f"{day}D")
    if hour:
        result.append(f"{hour}h")
    if minute:
        result.append(f"{minute}m")
    if second:
        result.append(f"{second}s")
    return "".join(result)


def timeavg(da: xr.DataArray, rule):
    """Preserve the source time epoch across :func:`_timeavg_impl`.

    ``xr.DataArray.resample`` builds a fresh time index and drops its
    ``encoding``, so ``units``/``calendar`` would otherwise be gone as
    soon as averaging happens. Capture them before the resample and
    restore them afterwards, using ``setdefault`` so anything deliberately
    set downstream still wins.

    This is the first half of keeping one time epoch across the dataset.
    The second half is in ``std_lib.time_bounds.time_bounds``, where
    assigning the ``time_bnds`` coord would otherwise clear the encoding
    again. Neither fix works alone: without this one there is nothing for
    ``time_bounds`` to preserve, and without that one the value restored
    here is wiped before it reaches disk.

    Why it matters: with the epoch lost,
    ``time_bounds._force_canonical_time_encoding`` takes its documented
    "source has no units" fallback and derives a date-only epoch from the
    first timestamp, which differs per file (monthly ``days since
    1851-01-16``, daily ``days since 1851-01-01``, yearly ``days since
    1851-07-02``). The inputs carry no such spread; OIFS and FESOM both
    ship ``seconds since 1850-01-01``. A per-file epoch is not wrong on
    its own, cftime decodes each file correctly, but it leaves the dataset
    with no single time reference, which CMIP7 assumes when it defines
    ``branch_time_in_child`` as being in "the time units and time model of
    the child".
    """
    _src_enc = {}
    try:
        if "time" in getattr(da, "coords", {}):
            for _k in ("units", "calendar"):
                _v = da["time"].encoding.get(_k)
                if _v:
                    _src_enc[_k] = _v
    except Exception as _exc:  # pragma: no cover - defensive
        logger.debug(f"timeavg: could not read source time encoding: {_exc}")

    out = _timeavg_impl(da, rule)

    try:
        if _src_enc and "time" in getattr(out, "coords", {}):
            for _k, _v in _src_enc.items():
                out["time"].encoding.setdefault(_k, _v)
            logger.debug(f"timeavg: restored source time encoding {_src_enc}")
    except Exception as _exc:  # pragma: no cover - defensive
        logger.debug(f"timeavg: could not restore source time encoding: {_exc}")

    return out


def _timeavg_impl(da: xr.DataArray, rule):
    """
    Time averages data with respect to time-method (mean/climatology/instant.)

    This function takes a data array and a rule, computes the timespan of the data
    array, and then performs time averaging based on the time method specified in the
    rule. The time methods can be ``"INSTANTANEOUS"``, ``"MEAN"``, or ``"CLIMATOLOGY"``.

    For ``"MEAN"`` time method, the timestamps can be adjusted using the ``adjust_timestamp``
    parameter in the rule dict.

    This can be either:
    - A float between 0 and 1 representing the position within each period (e.g., 0.5 for mid-point)
    - A string preset: "first"/"start" (0.0), "last"/"end" (1.0), "mid"/"middle" (0.5)
    - A pandas offset string (e.g., "2d" for 2 days offset)
      This feature is useful for setting consistent mid-month dates by setting
      ``adjust_timestamp`` to "14d".

    Parameters
    ----------
    da : xr.DataArray
        The data array to compute the timespan for.
    rule : dict
        The rule dict containing the time method and other parameters.
        For "MEAN" time method, can include 'adjust_timestamp' to control timestamp positioning.

    Returns
    -------
    xr.DataArray
        The time averaged data array.

    Examples
    --------
    First, create a simple daily dataset with temperature data:

    >>> import numpy as np
    >>> import pandas as pd
    >>> import xarray as xr
    >>> from types import SimpleNamespace
    >>> dates = pd.date_range("2023-01-01", periods=90, freq="D")
    >>> temps = 15 + 5 * np.sin(np.arange(90) * 2 * np.pi / 30)
    >>> da = xr.DataArray(
    ...     temps,
    ...     dims=["time"],
    ...     coords={"time": dates},
    ...     name="temperature"
    ... )
    >>> print("INPUT - Daily data:")  # doctest: +ELLIPSIS
    INPUT - Daily data:
    >>> print(f"Time dimension: {len(da.time)} points")  # doctest: +ELLIPSIS
    Time dimension: 90 points
    >>> print(f"Time range: {da.time.values[0]} to {da.time.values[-1]}")  # doctest: +ELLIPSIS
    Time range: 2023-01-01... to 2023-03-31...

    Create a mock rule for monthly mean averaging (30 days):

    >>> mock_table_header = SimpleNamespace(approx_interval="30.0", table_id="Amon")
    >>> mock_drv = SimpleNamespace(frequency="mon", table_header=mock_table_header)
    >>> rule = SimpleNamespace(data_request_variable=mock_drv)
    >>> rule.get = lambda key, default=None: getattr(rule, key, default)

    Apply monthly averaging:

    >>> result = timeavg(da, rule)
    >>> print("OUTPUT - Monthly averaged data:")  # doctest: +ELLIPSIS
    OUTPUT - Monthly averaged data:
    >>> print(f"Time dimension: {len(result.time)} points")  # doctest: +ELLIPSIS
    Time dimension: 3 points
    >>> print(f"Time method: {rule.time_method}")  # doctest: +ELLIPSIS
    Time method: MEAN
    >>> print(f"Frequency: {rule.frequency_str}")  # doctest: +ELLIPSIS
    Frequency: 1MS

    Test with INSTANTANEOUS time method (3-hourly point samples):

    >>> hourly_dates = pd.date_range("2023-01-01", periods=24, freq="h")
    >>> hourly_temps = 15 + 3 * np.sin(np.arange(24) * 2 * np.pi / 24)
    >>> da_hourly = xr.DataArray(
    ...     hourly_temps,
    ...     dims=["time"],
    ...     coords={"time": hourly_dates},
    ...     name="temperature"
    ... )
    >>> print("INPUT - Hourly data:")  # doctest: +ELLIPSIS
    INPUT - Hourly data:
    >>> print(f"Time dimension: {len(da_hourly.time)} points")  # doctest: +ELLIPSIS
    Time dimension: 24 points
    >>> mock_table_header_pt = SimpleNamespace(approx_interval="0.125", table_id="3hrPt")
    >>> mock_drv_pt = SimpleNamespace(frequency="3hrPt", table_header=mock_table_header_pt)
    >>> rule_pt = SimpleNamespace(data_request_variable=mock_drv_pt)
    >>> rule_pt.get = lambda key, default=None: getattr(rule_pt, key, default)
    >>> result_pt = timeavg(da_hourly, rule_pt)
    >>> print("OUTPUT - 3-hourly instantaneous samples:")  # doctest: +ELLIPSIS
    OUTPUT - 3-hourly instantaneous samples:
    >>> print(f"Time dimension: {len(result_pt.time)} points")  # doctest: +ELLIPSIS
    Time dimension: 8 points
    >>> print(f"Time method: {rule_pt.time_method}")  # doctest: +ELLIPSIS
    Time method: INSTANTANEOUS

    Test with adjust_timestamp to shift timestamps to mid-month:

    >>> rule_adjusted = SimpleNamespace(
    ...     data_request_variable=mock_drv,
    ...     adjust_timestamp=0.5
    ... )
    >>> rule_adjusted.get = lambda key, default=None: getattr(rule_adjusted, key, default)
    >>> result_adjusted = timeavg(da, rule_adjusted)
    >>> print("OUTPUT - Monthly mean with mid-month timestamps:")  # doctest: +ELLIPSIS
    OUTPUT - Monthly mean with mid-month timestamps:
    >>> print(f"First timestamp: {result_adjusted.time.values[0]}")  # doctest: +ELLIPSIS
    First timestamp: 2023-01-1...
    """
    # F5 instrumentation (DESIGN_PROPOSAL_recipe_failures_post_cli.md §3.5):
    # the sbl_seaice 12-vs-7 CoordinateValidationError persists across runs.
    # Standalone repro outside the pipeline gives 12 groups; the 7 enters
    # somewhere in the step chain. Log the time-coord size at timeavg entry
    # so we can localize whether shrinkage happens before or inside this
    # function. Drop once F5 is closed.
    try:
        cmor_var = getattr(rule, "cmor_variable", "?")
        if "time" in getattr(da, "coords", {}):
            t = da["time"]
            t_unique = t.to_index().is_unique
            t_size = t.size
            t_first = t.values[0] if t_size else None
            t_last = t.values[-1] if t_size else None
            logger.info(
                f"timeavg [{cmor_var}] entry: da.time.size={t_size} "
                f"is_unique={t_unique} first={t_first} last={t_last}"
            )
        else:
            logger.info(f"timeavg [{cmor_var}] entry: no 'time' coord (frequency={getattr(rule.data_request_variable, 'frequency', '?')})")
    except Exception as _exc:
        logger.warning(f"timeavg instrumentation failed: {_exc}")

    drv = rule.data_request_variable
    if drv.frequency == "fx" or getattr(drv, 'table_header', None) is None or getattr(drv.table_header, 'approx_interval', None) is None:
        logger.info(f"Variable with frequency={drv.frequency!r} has no approx_interval — skipping time averaging")
        rule.frequency_str = getattr(drv, 'frequency', 'fx') or "fx"
        rule.time_method = "FIXED"
        return da
    approx_interval = drv.table_header.approx_interval
    frequency_str = _frequency_from_approx_interval(approx_interval)
    logger.debug(f"{approx_interval=} {frequency_str=}")
    # attach the frequency_str to rule, it is referenced when creating file name
    rule.frequency_str = frequency_str
    time_method = _get_time_method(drv.frequency)
    # CMIP6 marked instantaneous frequencies with a "Pt" suffix (3hrPt,
    # 6hrPt, monPt, ...) so _get_time_method's regex worked. CMIP7
    # dropped the suffix — frequency is just "3hr"/"6hr"/"mon" and the
    # signal lives in cell_methods ("time: point" vs "time: mean").
    # Without this override, every CMIP7 tpt-* sub-daily / monthly rule
    # is treated as MEAN here, which (a) runs .mean() instead of .first()
    # on the resample and (b) applies the +interval*0.5 midpoint shift
    # to instants. Net effect: stamps move +3h for 6hr / +1.5h for 3hr
    # / +0.5h for 1hr, with cell_methods on disk still saying "time:
    # point" — wrong values AND mismatched metadata.
    cm = (getattr(drv, "cell_methods", "") or "").lower()
    if "time: point" in cm and time_method != "INSTANTANEOUS":
        logger.info(
            f"  cell_methods has 'time: point'; overriding time_method "
            f"({time_method} -> INSTANTANEOUS) for CMIP7 tpt rule"
        )
        time_method = "INSTANTANEOUS"
    # Same story for the daily-extremum brandings (tmax-/tmin-). The DReq
    # asks for ``time: maximum`` / ``time: minimum``; without this the rule
    # falls through to MEAN and emits a daily mean under a max/min label.
    # Feed these from a sub-daily source: the extremum is only as good as
    # the sampling, so an hourly input gives an hourly-resolution extremum.
    # (IFS can do better via its mx2t/mn2t accumulators, which track every
    # model timestep, but with output_step_freq=1h XIOS only ever sees
    # hourly samples anyway, and for LR the model timestep *is* hourly so
    # the two coincide.)
    elif "time: maximum" in cm and time_method != "MAXIMUM":
        logger.info(f"  cell_methods has 'time: maximum'; overriding time_method ({time_method} -> MAXIMUM)")
        time_method = "MAXIMUM"
    elif "time: minimum" in cm and time_method != "MINIMUM":
        logger.info(f"  cell_methods has 'time: minimum'; overriding time_method ({time_method} -> MINIMUM)")
        time_method = "MINIMUM"
    rule.time_method = time_method
    # FESOM yearly files and concat'd hemispheric selects can yield a
    # non-monotonic time index, which breaks xr.resample. Sort once if needed.
    if "time" in getattr(da, "coords", {}):
        try:
            if not bool(da.indexes["time"].is_monotonic_increasing):
                logger.warning(
                    f"Time index for {getattr(rule, 'cmor_variable', '<unknown>')} "
                    "is not monotonic; sorting before resample."
                )
                da = da.sortby("time")
        except (KeyError, AttributeError):
            pass
    # Default flox engine is "numpy" (vectorised, zero JIT cold-start).
    # The default flox path ("numbagg") JIT-compiles each aggregator via
    # numba on first use — ~30 s per (aggregator, dtype, worker) triple.
    # On HR runs with fresh Dask workers this dominated wall time. The
    # numpy engine is within a small factor of numbagg once warm.
    # Override per-rule or via config key ``flox_engine`` when needed.
    _flox_engine = rule.get("flox_engine") if hasattr(rule, "get") else None
    if not _flox_engine and hasattr(rule, "_pycmor_cfg"):
        try:
            _flox_engine = rule._pycmor_cfg("flox_engine")
        except Exception:
            _flox_engine = None
    if not _flox_engine:
        _flox_engine = "numpy"
    _resample_kw = {"engine": _flox_engine}
    if time_method == "INSTANTANEOUS":
        # xarray's DataArrayResample.first() does not accept the flox
        # ``engine`` kwarg (no flox aggregator path for the "pick first
        # element per group" reduction). Before the cmip7 cell_methods
        # override added in 6c0a3c59, every CMIP7 tpt rule fell through
        # to MEAN because CMIP6's Pt suffix was dropped; that masked the
        # latent TypeError. Call .first() without the engine kwarg.
        ds = da.resample(time=frequency_str).first()
    elif time_method in ("MAXIMUM", "MINIMUM"):
        # Daily (or other interval) extremum. Like MEAN this describes an
        # interval rather than an instant, so the stamp is placed at the
        # interval midpoint below by falling through to the same offset
        # handling.
        _red = "max" if time_method == "MAXIMUM" else "min"
        ds = getattr(da.resample(time=frequency_str), _red)(**_resample_kw)
        offset = rule.get("adjust_timestamp", "mid")
        offset_presets = {"first": 0, "start": 0, "last": 1, "end": 1, "mid": 0.5}
        _off = offset_presets.get(offset, 0.5) if isinstance(offset, str) else float(offset)
        if _off:
            ds = ds.assign_coords(
                time=ds.time + pd.to_timedelta(float(approx_interval) * _off, unit="D")
            )
    elif time_method == "MEAN":
        ds = da.resample(time=frequency_str).mean(**_resample_kw)
        # CMIP spec: time coordinate of MEAN-averaged data sits at the midpoint
        # of its averaging interval. Default to "mid" unless user overrides.
        offset = rule.get("adjust_timestamp", "mid")
        offset_presets = {
            "first": 0,
            "start": 0,
            "last": 1,
            "end": 1,
            "mid": 0.5,
            "middle": 0.5,
        }
        offset = offset_presets.get(offset, offset)
        if offset is None:
            return ds
        try:
            offset = float(offset)
        except (ValueError, TypeError):
            # dealing with a alphanumerical string. example: "14days"
            try:
                offset = pd.to_timedelta(offset)
            except ValueError:
                # don't know how to deal with it so raise the error
                # supporing datetime kind of string is a bit complex.
                # at the moment, skip supporting it.
                raise ValueError(f"offset ({offset}) can not converted to timedelta")
            else:
                ds["time"] = ds.time.values + offset
                return ds
        else:
            if offset == 0.0:
                return ds
            elif "MS" in frequency_str or "YS" in frequency_str:
                timestamps = []
                magnitude = re.search(r"(\d+(?:\.\d+)?)?", frequency_str).group(0) or 1
                magnitude = float(magnitude)
                # Subtract 1 day only at offset==1.0 (to stay inside the period);
                # for midpoint (offset=0.5) the bare ndays*offset is correct.
                correction = pd.to_timedelta("1d") if offset >= 1.0 else pd.to_timedelta(0)
                if "MS" in frequency_str:
                    for timestamp, grp in da.resample(time=frequency_str):
                        ndays = grp.time.dt.days_in_month.values[0] * magnitude
                        new_offset = pd.to_timedelta(f"{ndays}d") * offset - correction
                        timestamp = timestamp + new_offset
                        timestamps.append(timestamp)
                elif "YS" in frequency_str:
                    for timestamp, grp in da.resample(time=frequency_str):
                        ndays = grp.time.dt.days_in_year.values[0] * magnitude
                        new_offset = pd.to_timedelta(f"{ndays}d") * offset - correction
                        timestamp = timestamp + new_offset
                        timestamps.append(timestamp)
                else:
                    print(
                        "It is not possible to reach this branch."
                        "If you are here, know that Pycmor has gone nuts."
                        f"{frequency_str=} {offset=}"
                    )
                ds["time"] = timestamps
                return ds
            else:
                new_offset = pd.to_timedelta(frequency_str) * offset
                ds["time"] = ds.time.values + new_offset
                return ds
    elif time_method == "CLIMATOLOGY":
        if drv.frequency == "monC":
            ds = da.groupby("time.month").mean("time")
        elif drv.frequency == "1hrCM":
            ds = da.groupby("time.hour").mean("time")
        else:
            raise ValueError(f"Unknown Climatology {drv.frequency} in Table {drv.table_header.table_id}")
    else:
        raise ValueError(f"Unknown time method: {time_method}")
    return ds


def custom_resample(df, freq="ME", offset=0.5, func="mean"):
    """
    Resample a DataFrame and place timestamps at a custom offset within each period.

    Parameters
    ----------
    df : DataFrame
        DataFrame with a DatetimeIndex
    freq : str
        Frequency string (e.g., 'M' for month, 'Y' for year)
    offset : float
        Float between 0 and 1, representing the position within each period
    func : str
        Resampling function (e.g., 'mean', 'sum', 'max')

    Returns
    -------
    DataFrame
        Resampled DataFrame with adjusted timestamps

    Examples
    --------
    First, set up our imports and random seed:

    >>> import numpy as np
    >>> import pandas as pd
    >>> rng = np.random.default_rng(42)
    >>> date_rng = pd.date_range(start="2023-01-01", end="2023-12-31", freq="D")
    >>> df = pd.DataFrame({"value": rng.random(len(date_rng))}, index=date_rng)

    Test mid-month resampling:

    >>> df_month_mid = custom_resample(df, freq="ME", offset=0.5)
    >>> print(df_month_mid.head())
                            value
    2023-01-16 00:00:00  0.565127
    2023-02-14 12:00:00  0.484111
    2023-03-16 00:00:00  0.434221
    2023-04-15 12:00:00  0.510354
    2023-05-16 00:00:00  0.443399

    Test mid-year resampling:

    >>> df_year_mid = custom_resample(df, freq="YE", offset=0.5)
    >>> print(df_year_mid)
                   value
    2023-07-02  0.492457

    Test mid-week resampling:

    >>> df_week_mid = custom_resample(df, freq="W", offset=0.5)
    >>> print(df_week_mid.head())
                   value
    2023-01-01  0.773956
    2023-01-05  0.658835
    2023-01-12  0.540872
    2023-01-19  0.488221
    2023-01-26  0.500237

    Test one-third through each month:

    >>> df_month_third = custom_resample(df, freq="ME", offset=1/3)
    >>> print(df_month_third.head())
                            value
    2023-01-11 00:00:00  0.565127
    2023-02-10 00:00:00  0.484111
    2023-03-11 00:00:00  0.434221
    2023-04-10 16:00:00  0.510354
    2023-05-11 00:00:00  0.443399

    Test quarter-end resampling:

    >>> df_quarter_end = custom_resample(df, freq="QE", offset=1)
    >>> print(df_quarter_end)
                   value
    2023-03-31  0.494832
    2023-06-30  0.496207
    2023-09-30  0.461806
    2023-12-31  0.517077

    Test with irregular time series:

    >>> irregular_dates = pd.date_range("2023-01-01", periods=100, freq="D").tolist()
    >>> irregular_dates += pd.date_range("2023-05-01", periods=50, freq="2D").tolist()
    >>> irregular_dates += pd.date_range("2023-07-01", periods=30, freq="3D").tolist()
    >>> df_irregular = pd.DataFrame({"value": rng.random(len(irregular_dates))}, index=irregular_dates)
    >>> df_irregular_month = custom_resample(df_irregular, freq="ME", offset=0.5)
    >>> print(df_irregular_month.head())
                            value
    2023-01-16 00:00:00  0.543549
    2023-02-14 12:00:00  0.485275
    2023-03-16 00:00:00  0.513365
    2023-04-05 12:00:00  0.558554
    2023-05-16 00:00:00  0.447175
    """
    # Perform the resampling
    resampled = getattr(df.resample(freq), func)()

    # Adjust the timestamps
    new_index = []
    for name, group in df.groupby(pd.Grouper(freq=freq)):
        if not group.empty:
            period_start = group.index[0]
            period_end = group.index[-1]
            new_timestamp = period_start + (period_end - period_start) * offset
            new_index.append(new_timestamp)

    resampled.index = pd.DatetimeIndex(new_index)
    return resampled


_IGNORED_CELL_METHODS = """
area: depth: time: mean
area: mean
area: mean (comment: over land and sea ice) time: point
area: mean time: maximum
area: mean time: maximum within days time: mean over days
area: mean time: mean within days time: mean over days
area: mean time: mean within hours time: maximum over hours
area: mean time: mean within years time: mean over years
area: mean time: minimum
area: mean time: minimum within days time: mean over days
area: mean time: point
area: mean time: sum
area: mean where crops time: maximum
area: mean where crops time: maximum within days time: mean over days
area: mean where crops time: minimum
area: mean where crops time: minimum within days time: mean over days
area: mean where grounded_ice_sheet
area: mean where ice_free_sea over sea time: mean
area: mean where ice_sheet
area: mean where land
area: mean where land over all_area_types time: mean
area: mean where land over all_area_types time: point
area: mean where land over all_area_types time: sum
area: mean where land time: mean
area: mean where land time: mean (with samples weighted by snow mass)
area: mean where land time: point
area: mean where sea
area: mean where sea depth: sum where sea (top 100m only) time: mean
area: mean where sea depth: sum where sea time: mean
area: mean where sea time: mean
area: mean where sea time: point
area: mean where sea_ice (comment: mask=siconc) time: point
area: mean where sector time: point
area: mean where snow over sea_ice area: time: mean where sea_ice
area: point
area: point time: point
area: sum
area: sum where ice_sheet time: mean
area: sum where sea time: mean
area: time: mean
area: time: mean (comment: over land and sea ice)
area: time: mean where cloud
area: time: mean where crops (comment: mask=cropFrac)
area: time: mean where floating_ice_shelf (comment: mask=sftflf)
area: time: mean where grounded_ice_sheet (comment: mask=sfgrlf)
area: time: mean where ice_sheet
area: time: mean where natural_grasses (comment: mask=grassFrac)
area: time: mean where pastures (comment: mask=pastureFrac)
area: time: mean where sea_ice (comment: mask=siconc)
area: time: mean where sea_ice (comment: mask=siconca)
area: time: mean where sea_ice (comment: mask=siitdconc)
area: time: mean where sea_ice_melt_pond (comment: mask=simpconc)
area: time: mean where sea_ice_ridges (comment: mask=sirdgconc)
area: time: mean where sector
area: time: mean where shrubs (comment: mask=shrubFrac)
area: time: mean where snow (comment: mask=snc)
area: time: mean where trees (comment: mask=treeFrac)
area: time: mean where unfrozen_soil
area: time: mean where vegetation (comment: mask=vegFrac)
longitude: mean time: mean
longitude: mean time: point
longitude: sum (comment: basin sum [along zig-zag grid path]) depth: sum time: mean
time: mean
time: mean grid_longitude: mean
time: point
""".strip().split(
    "\n"
)
"""list: cell_methods to ignore when calculating time averages"""
