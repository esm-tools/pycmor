"""Validate simultaneous input variables separately from serial time chunks.

Only time axes and coordinate arrays are read. Scientific data arrays are not
loaded here. Different variables may share the same timestamps; two chunks of
the SAME variable must not duplicate a timestamp.
"""
from collections import defaultdict
from datetime import timedelta
from functools import lru_cache
import hashlib
from pathlib import Path

import cftime
import netCDF4
import numpy as np


def date_tuple(date):
    # Large day counts decoded by cftime can be a few microseconds short of
    # an exact second. Normalize only bounded floating-point roundoff; do not
    # silently accept a genuinely shifted fractional-second time axis.
    microsecond = date.microsecond
    if microsecond:
        if min(microsecond, 1000000 - microsecond) > 1000:
            raise ValueError('Fractional-second timestamp exceeds decoding tolerance')
        date = date.replace(microsecond=0) + timedelta(seconds=microsecond >= 500000)
    return (date.year, date.month, date.day, date.hour, date.minute, date.second)


@lru_cache(maxsize=4096)
def _header_cached(path, mtime_ns, size):
    with netCDF4.Dataset(path) as ds:
        time = next((ds[n] for n in ('time', 'time_counter') if n in ds.variables), None)
        if time is None:
            raise ValueError(f'MISSING_TIME: {path}')
        calendar = getattr(time, 'calendar', 'standard')
        dates = tuple(date_tuple(t) for t in netCDF4.num2date(time[:], time.units, calendar))
        time_dim = time.dimensions[0]
        excluded = set(ds.dimensions)
        for name, var in ds.variables.items():
            if getattr(var, 'standard_name', '') == 'time' or getattr(var, 'axis', '') == 'T':
                excluded.add(name)
            if hasattr(var, 'bounds'):
                excluded.add(var.bounds)
        variables = {}
        for name, var in ds.variables.items():
            if name in excluded or time_dim not in var.dimensions:
                continue
            spatial = {d: len(ds.dimensions[d]) for d in var.dimensions if d != time_dim}
            coords = {}
            for cn, cv in ds.variables.items():
                if time_dim in cv.dimensions or not set(cv.dimensions) <= set(spatial):
                    continue
                if cn not in spatial and getattr(cv, 'standard_name', '') not in (
                        'latitude', 'longitude', 'depth', 'air_pressure'):
                    continue
                values = np.asarray(cv[:])
                coords[cn] = (cv.dimensions, values.shape, str(values.dtype),
                              hashlib.sha256(values.tobytes()).hexdigest())
            variables[name] = dict(dimensions=spatial, coordinates=coords,
                                   units=getattr(var, 'units', None))
        if not variables:
            raise ValueError(f'MISSING_VARIABLE_STREAM: {path}')
        return dict(dates=dates, calendar=calendar, variables=variables)


def stream_header(path):
    path = Path(path)
    stat = path.stat()
    return _header_cached(str(path), stat.st_mtime_ns, stat.st_size)


def _compatible(a, b, description, same_variable=False):
    for dim in a['dimensions'].keys() & b['dimensions'].keys():
        if a['dimensions'][dim] != b['dimensions'][dim]:
            raise ValueError(f'INCOMPATIBLE_GRID: {description}: dimension {dim}')
    for coord in a['coordinates'].keys() & b['coordinates'].keys():
        if a['coordinates'][coord] != b['coordinates'][coord]:
            raise ValueError(f'INCOMPATIBLE_GRID: {description}: coordinate {coord}')
    if same_variable and (a['dimensions'] != b['dimensions'] or a['units'] != b['units']):
        raise ValueError(f'INCOMPATIBLE_STREAM: {description}')


def _cover(dates, year, calendar):
    if not dates or len(set(dates)) != len(dates) or dates != sorted(dates):
        raise ValueError('Duplicate or non-monotonic timestamps within a variable stream')
    days = int(cftime.date2num(cftime.datetime(year + 1, 1, 1, calendar=calendar),
                              f'days since {year}-01-01', calendar))
    frequency = {1: 'yr', 12: 'mon', days: 'day', days * 4: '6hr',
                 days * 8: '3hr', days * 24: '1hr'}.get(len(dates))
    if frequency is None:
        raise ValueError(f'INCOMPLETE_INPUT: {len(dates)} records in stream for {year}')
    if frequency == 'mon':
        if [(d[0], d[1]) for d in dates] != [(year, m) for m in range(1, 13)]:
            raise ValueError('INCOMPLETE_INPUT: expected January through December')
    elif frequency == 'yr':
        if dates[0][0] != year:
            raise ValueError('INCOMPLETE_INPUT: wrong annual timestamp')
    else:
        hours = {'day': 24, '6hr': 6, '3hr': 3, '1hr': 1}[frequency]
        nums = cftime.date2num([cftime.datetime(*d, calendar=calendar) for d in dates],
                               f'hours since {year}-01-01', calendar)
        end = days * 24
        if not np.allclose(np.diff(nums), hours, rtol=0, atol=1e-7):
            raise ValueError('INCOMPLETE_INPUT: gaps in variable stream')
        if not (-1e-8 <= nums[0] <= hours + 1e-8 and
                end - hours - 1e-8 <= nums[-1] <= end + 1e-8):
            raise ValueError('INCOMPLETE_INPUT: dates do not cover requested year')
    return frequency


def validate_input_streams(files, years, target_frequency):
    """Return stream metadata after checking full, synchronized source coverage.

    ``years`` is an iterable of target years. Streams are distinguished by
    physical variable names, NOT by filename. Chunk order is obtained from
    each file's first date; dates within any chunk must already be monotonic.
    """
    years = tuple(years)
    if not years or years != tuple(range(years[0], years[-1] + 1)):
        raise ValueError('Requested years must be a consecutive increasing window')
    streams = defaultdict(list)
    calendar = None
    for path in files:
        h = stream_header(path)
        if calendar is not None and calendar != h['calendar']:
            raise ValueError('INCOMPATIBLE_CALENDAR: input variables/chunks disagree')
        calendar = h['calendar']
        dates = list(h['dates'])
        if not dates or dates != sorted(dates) or len(dates) != len(set(dates)):
            raise ValueError(f'Duplicate or non-monotonic timestamps: {path}')
        for name, schema in h['variables'].items():
            streams[name].append((dates, schema, str(path)))
    if not streams:
        raise ValueError('MISSING_INPUT: no time-dependent variable streams')
    result = {}
    ranking = {'dec': -1, 'yr': 0, 'mon': 1, 'day': 2, '6hr': 3, '3hr': 4, '1hr': 5}
    reference_dates = None
    references = []
    for name, chunks in sorted(streams.items()):
        chunks.sort(key=lambda c: c[0][0])
        dates = [d for chunk, _, _ in chunks for d in chunk]
        if dates != sorted(dates) or len(dates) != len(set(dates)):
            raise ValueError(f'Duplicate or non-monotonic timestamps within stream {name}')
        schema = chunks[0][1]
        for _, other, path in chunks[1:]:
            _compatible(schema, other, f'{name}: {path}', same_variable=True)
        # End-labelled instantaneous output can end exactly on next January 1.
        # The annual validator used by the caller preserves that existing policy.
        frequencies = []
        for year in years:
            selected = [d for d in dates if d[0] == year]
            if len(years) == 1:
                selected = dates
            frequencies.append(_cover(selected, year, calendar))
        if len(set(frequencies)) != 1:
            raise ValueError(f'INCOMPATIBLE_FREQUENCY: {name}')
        native = frequencies[0]
        if ranking[native] < ranking.get(target_frequency, 0):
            raise ValueError(f'MISSING_FREQUENCY: {native} cannot supply {target_frequency}')
        if reference_dates is not None and dates != reference_dates:
            raise ValueError(f'UNSYNCHRONIZED_VARIABLES: time coordinates differ for {name}')
        reference_dates = dates
        for other_name, other in references:
            _compatible(schema, other, f'{name} versus {other_name}')
        references.append((name, schema))
        result[name] = dict(frequency=native, dates=tuple(dates), calendar=calendar,
                            schema=schema, files=[c[2] for c in chunks])
    return result


def validate_previous_day(current_files, previous_files, year, required_variables):
    """Validate a real Dec 31 boundary for a daily storage-difference product."""
    current = validate_input_streams(current_files, [year], 'day')
    previous = {}
    for path in previous_files:
        h = stream_header(path)
        for name, schema in h['variables'].items():
            if name in previous:
                raise ValueError(f'DUPLICATE_BOUNDARY: {name}')
            previous[name] = (h, schema, str(path))
    records = {}
    for name in required_variables:
        if name not in current or name not in previous:
            raise ValueError(f'MISSING_PREVIOUS_DAY: {name}, December 31 {year-1}')
        c = current[name]
        h, schema, path = previous[name]
        if h['calendar'] != c['calendar']:
            raise ValueError(f'INCOMPATIBLE_CALENDAR: boundary for {name}')
        _compatible(c['schema'], schema, f'previous-day {name}', same_variable=True)
        dates = list(h['dates'])
        if dates != sorted(dates) or len(dates) != len(set(dates)):
            raise ValueError(f'Duplicate or non-monotonic boundary timestamps: {name}')
        units = f'hours since {year}-01-01'
        last_day = date_tuple(cftime.num2date(-24, units, c['calendar']))[:3]
        if not dates or dates[-1][:3] != last_day:
            raise ValueError(f'MISSING_PREVIOUS_DAY: {name}, expected {last_day}')
        pair = cftime.date2num([cftime.datetime(*dates[-1], calendar=c['calendar']),
                               cftime.datetime(*c['dates'][0], calendar=c['calendar'])],
                              units, c['calendar'])
        if not np.isclose(pair[1] - pair[0], 24, rtol=0, atol=1e-7):
            raise ValueError(f'INVALID_BOUNDARY_INTERVAL: {name}, expected 24 hours')
        records[name] = dict(file=path, index=len(dates)-1, date=list(dates[-1]))
    return records
