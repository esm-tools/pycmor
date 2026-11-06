"""Decadal means from a year-by-year cmorization.

The HR submission processes one model year per job and pins every input
pattern to that year. A decadal mean needs ten. Rather than keep state
about which decades are done, every yearly job carries the dec rules and
:func:`decadal_gate` decides again each time:

- the run's first year is recounted from the input files on disk,
- decades are counted from that year (1850-1859, 1860-1869, ... for a run
  starting in 1850; 2024-2033, ... for one starting in 2024),
- the rule only proceeds in the year that closes a decade, and then reads
  all ten years; in every other year it ends without output.

A trailing partial decade is never written. CMIP7 ``dec`` is one sample per
ten years, and the coordinate checks in cc-plugin-wcrp#81 reject a shorter
cell ("does not match regular 10-year cells").
"""

import re
from pathlib import Path

from ..core.logging import logger
from ..core.skip import RuleSkipped

_PLACEHOLDER = r"\d{4}"


def _years_on_disk(collection):
    grouped = re.compile(collection.pattern_str.replace(_PLACEHOLDER, r"(\d{4})", 1))
    years = set()
    for f in collection.path.iterdir():
        m = grouped.match(f.name)
        if m:
            years.add(int(m.group(1)))
    return years


def decadal_gate(data, rule):
    """Let a decadal rule through only in the year that closes a decade.

    Put this first in the pipeline of every ``dec`` rule. It needs
    ``rule.year``, which ``repoint_hr_year.py`` injects into ``inherit``.

    In the closing year the year-pinned input patterns are widened back to
    all years and ``year_start``/``year_end`` are set to the decade, so
    ``load_mfdataset`` reads exactly those ten years and
    ``set_time_bounds`` writes the decade as the cell. Missing years in a
    closing decade are an error, not a skip.
    """
    year = rule.get("year")
    if year is None:
        raise ValueError(
            f"decadal_gate: rule {rule.get('name')!r} has no 'year'. "
            "repoint_hr_year.py sets it in inherit for every yearly submission."
        )
    year = int(year)

    # A full-workflow planner can supply an explicitly validated, immutable
    # decade. Do not widen its exact primary/secondary filenames or infer a
    # different anchor from unrelated archive years. Legacy HR yearly jobs
    # retain the original discovery/gating behaviour below.
    if rule.get("explicit_decadal_window", False):
        first, last = int(rule.get("year_start")), int(rule.get("year_end"))
        expected = {str(Path(p).resolve()) for p in rule.get("decadal_input_files", [])}
        if last - first != 9 or year != last or not expected:
            raise ValueError("decadal_gate: invalid explicit ten-year window or missing frozen inputs")
        if any(not Path(p).is_file() for p in expected):
            raise FileNotFoundError("decadal_gate: a frozen explicit-decade input has disappeared")
        observed = {str(Path(p).resolve()) for coll in rule.inputs for p in coll.files}
        if not observed or not observed <= expected:
            raise ValueError("decadal_gate: primary input selection differs from frozen decade")
        return data

    per_collection = []
    for coll in rule.inputs:
        if coll.pattern is None:
            raise ValueError(f"decadal_gate: glob input {coll.pattern_str!r} is not supported")
        if _PLACEHOLDER not in coll.pattern_str:
            widened = coll.pattern_str.replace(str(year), _PLACEHOLDER)
            if widened == coll.pattern_str:
                raise ValueError(
                    f"decadal_gate: input pattern {coll.pattern_str!r} contains neither "
                    f"the year {year} nor {_PLACEHOLDER}"
                )
            coll.pattern_str = widened
            coll.pattern = re.compile(widened)
        per_collection.append((coll, _years_on_disk(coll)))

    all_years = set().union(*(y for _, y in per_collection))
    if not all_years:
        raise FileNotFoundError(f"decadal_gate: no input files for rule {rule.get('name')!r}")
    first = min(all_years)
    position = (year - first) % 10
    decade_start = year - position
    if year < first or position != 9:
        logger.info(
            f"decadal_gate: {rule.get('name')}: {year} is year {position + 1} of the decade "
            f"{decade_start}-{decade_start + 9} (run starts {first}), nothing to write"
        )
        return RuleSkipped(f"{year} does not close a decade counted from {first}")

    decade = set(range(decade_start, year + 1))
    for coll, years in per_collection:
        missing = sorted(decade - years)
        if missing:
            raise FileNotFoundError(
                f"decadal_gate: {rule.get('name')}: decade {decade_start}-{year} is incomplete "
                f"for {coll.pattern_str!r} in {coll.path}, missing {missing}"
            )
    rule.year_start = decade_start
    rule.year_end = year
    logger.info(f"decadal_gate: {rule.get('name')}: writing decade {decade_start}-{year} (run starts {first})")
    return data


def explicit_decadal_mean(data, rule):
    """Duration-weighted mean of 120 verified monthly means, evaluated lazily.

    The full workflow freezes exact monthly bounds and timestamps before job
    submission. Retain missing values instead of silently renormalizing a
    truncated time series, and place the sole output at the exact midpoint.
    The following set_time_bounds step uses year_start/year_end for bounds.
    """
    import cftime
    import numpy as np
    import xarray as xr

    if not rule.get("explicit_decadal_window", False):
        raise ValueError("explicit_decadal_mean requires a verified explicit window")
    first, last = int(rule.get("year_start")), int(rule.get("year_end"))
    if last - first != 9 or "time" not in data.dims or data.sizes["time"] != 120:
        raise ValueError("explicit_decadal_mean requires exactly 120 monthly means")
    observed = list(zip(*(getattr(data.time.dt, part).values.tolist()
                          for part in ("year", "month", "day", "hour", "minute", "second"))))
    expected = [tuple(d) for d in rule.get("decadal_dates", [])]
    if observed != expected:
        raise ValueError("explicit_decadal_mean: runtime time coordinates differ from the plan")
    weights = np.asarray(rule.get("decadal_month_weights", []), dtype=float)
    if weights.shape != (120,) or not np.all(np.isfinite(weights)) or not np.all(weights > 0):
        raise ValueError("explicit_decadal_mean: invalid monthly-duration weights")
    calendar = rule.get("decadal_calendar", "standard")
    hi = cftime.datetime(last + 1, 1, 1, calendar=calendar)
    units = f"days since {first:04d}-01-01"
    end = float(cftime.date2num(hi, units, calendar))
    if not np.isclose(weights.sum(), end, rtol=0, atol=1e-8):
        raise ValueError("explicit_decadal_mean: weights do not span the full decade")
    midpoint = cftime.num2date(end / 2, units, calendar, only_use_cftime_datetimes=True)
    w = xr.DataArray(weights, dims=("time",), coords={"time": data.time})
    # Weighted reduction also performs internal arithmetic. In xarray 2025.6
    # the method's keep_attrs=True preserves variable attributes but not
    # coordinate attributes through that arithmetic. Keep them throughout:
    # depth-axis metadata is needed by subsequent CMOR dimension mapping.
    # Scope the option locally so unrelated pipelines keep their own policy.
    with xr.set_options(keep_attrs=True):
        result = data.weighted(w).mean("time", skipna=False, keep_attrs=True)
    result = result.expand_dims(time=[midpoint])
    result.time.attrs = {k: v for k, v in data.time.attrs.items() if k not in ("bounds", "units", "calendar")}
    result.time.encoding.update(calendar=calendar, units=units)
    rule.time_method = "mean"
    rule.frequency_str = "10YS"
    return result
