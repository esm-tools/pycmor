"""Verified explicit ten-year windows for the full LR CMORization workflow.

Only monthly means with complete calendar-month bounds are accepted. Discovery
reads coordinates and time metadata, never the large three-dimensional field.
The selected primary and secondary filenames are frozen as exact regexes.
"""
from __future__ import annotations

from collections import defaultdict
from functools import lru_cache
import hashlib
from pathlib import Path
import re

import cftime
import netCDF4
import numpy as np


def window_for_period(period, configured=None):
    end = int(period[9:13])
    if configured is None:
        return end - 9, end
    if (not isinstance(configured, (list, tuple)) or len(configured) != 2
            or any(isinstance(x, bool) or not isinstance(x, int) for x in configured)):
        raise ValueError('decadal_window must be [first_year, last_year]')
    first, last = configured
    if last - first != 9 or first < 1 or last > end:
        raise ValueError('decadal_window must cover exactly ten years, ending no later than the selected leg')
    return first, last


def _stamp(d):
    return d.year, d.month, d.day, d.hour, d.minute, d.second


def _calendar(value):
    return {'standard': 'gregorian', '365_day': 'noleap', '366_day': 'all_leap'}.get(value, value)


def _hash_coordinate(v):
    digest = hashlib.sha256()
    digest.update(str((v.dimensions, v.shape, str(v.dtype), getattr(v, 'units', None))).encode())
    if v.ndim == 0:
        chunks = [v[...]]
    else:
        chunks = (v[i:i + 4096] for i in range(0, v.shape[0], 4096))
    for values in chunks:
        masked = np.ma.asarray(values)
        digest.update(np.ascontiguousarray(masked.filled(0)).tobytes())
        digest.update(np.ascontiguousarray(np.ma.getmaskarray(masked)).tobytes())
    return digest.hexdigest()


@lru_cache(maxsize=256)
def _header(path, size, mtime_ns):
    # size/mtime enter the cache key so a growing/changed archive is not reused.
    with netCDF4.Dataset(path) as ds:
        time_name = next((k for k in ('time', 'time_counter') if k in ds.variables), None)
        if time_name is None:
            raise ValueError(f'DECADAL_MISSING_TIME: {path}')
        t = ds[time_name]
        if t.ndim != 1:
            raise ValueError(f'DECADAL_TIME_DIMENSION: {path}')
        time_dim = t.dimensions[0]
        calendar = _calendar(getattr(t, 'calendar', 'standard'))
        dates = tuple(_stamp(d) for d in netCDF4.num2date(t[:], t.units, calendar))
        if tuple(sorted(dates)) != dates or len(set(dates)) != len(dates):
            raise ValueError(f'DECADAL_NONMONOTONIC_OR_DUPLICATE_TIME: {path}')
        bound_name = getattr(t, 'bounds', None)
        if not bound_name or bound_name not in ds.variables:
            raise ValueError(f'DECADAL_MISSING_MONTHLY_BOUNDS: {path}')
        b = ds[bound_name]
        if b.shape != (len(dates), 2):
            raise ValueError(f'DECADAL_INVALID_BOUNDS: {path}')
        decoded = netCDF4.num2date(b[:], getattr(b, 'units', t.units), calendar)
        bounds = tuple((_stamp(a), _stamp(z)) for a, z in decoded)
        physical = tuple(sorted(k for k, v in ds.variables.items()
                                if time_dim in v.dimensions and k not in (time_name, bound_name)))
        if not physical:
            raise ValueError(f'DECADAL_MISSING_FIELD: {path}')
        shapes = {k: tuple((dim, len(ds.dimensions[dim])) for dim in ds[k].dimensions if dim != time_dim)
                  for k in physical}
        units = {k: getattr(ds[k], 'units', None) for k in physical}
        coords = {k: _hash_coordinate(v) for k, v in ds.variables.items() if time_dim not in v.dimensions}
    return dict(physical=physical, dates=dates, bounds=bounds, calendar=calendar,
                shapes=shapes, units=units, coordinates=coords)


def validate_monthly_streams(files, first, last):
    """Return the shared time metadata after validating each physical stream."""
    streams = defaultdict(list)
    if last - first != 9:
        raise ValueError('DECADAL_WINDOW_LENGTH: expected exactly ten complete years')
    for path in sorted(set(map(Path, files))):
        stat = path.stat()
        if not stat.st_size:
            raise ValueError(f'EMPTY_INPUT: {path}')
        h = _header(str(path), stat.st_size, stat.st_mtime_ns)
        streams[h['physical']].append(h)
    if not streams:
        raise ValueError('DECADAL_EMPTY_WINDOW: no monthly input files')
    reference = None
    for physical, headers in streams.items():
        first_header = headers[0]
        for h in headers[1:]:
            for field in ('calendar', 'shapes', 'units', 'coordinates'):
                if h[field] != first_header[field]:
                    raise ValueError(f'DECADAL_CHANGED_{field.upper()}: {physical}')
        calendar = first_header['calendar']
        entries = sorted((d, b) for h in headers for d, b in zip(h['dates'], h['bounds']))
        dates = [d for d, _ in entries]
        expected = [(y, m) for y in range(first, last + 1) for m in range(1, 13)]
        if len(dates) != 120 or [(d[0], d[1]) for d in dates] != expected:
            raise ValueError(f'DECADAL_INCOMPLETE_MONTHS: {physical}; expected 120 unique months {first}-{last}, got {len(dates)}')
        bounds, weights = [], []
        units = f'days since {first:04d}-01-01'
        for (d, b), (y, m) in zip(entries, expected):
            lo = (y, m, 1, 0, 0, 0)
            hi = (y + (m == 12), 1 if m == 12 else m + 1, 1, 0, 0, 0)
            if b != (lo, hi) or not (lo <= d < hi):
                raise ValueError(f'DECADAL_INVALID_MONTH_BOUNDS: {physical} at {y}-{m:02d}: {b}')
            dn = cftime.date2num([cftime.datetime(*v, calendar=calendar) for v in (lo, hi)], units, calendar)
            bounds.append(b)
            weights.append(float(dn[1] - dn[0]))
        current = dict(calendar=calendar, dates=dates, bounds=bounds, weights=weights)
        if reference is not None and current != reference:
            raise ValueError('DECADAL_UNALIGNED_STREAMS: primary and secondary monthly coordinates differ')
        reference = current
    return reference


def _window_pattern(pattern, first, last, submitted_year):
    alternatives = '(?:' + '|'.join(str(y) for y in range(first, last + 1)) + ')'
    result = (pattern.replace(r'\d{4}', alternatives) if r'\d{4}' in pattern else
              re.sub(rf'(?<!\d){submitted_year}(?!\d)', lambda _: alternatives, pattern))
    # Repointing already pins FESOM patterns; broad legacy patterns are safe
    # only once replaced with exact selected-year variants.
    if result == pattern and r'\.fesom\.' in pattern:
        result = pattern.replace(r'.*\.nc', alternatives + r'\.nc')
    return result


def _resolve(base, pattern, first, last, submitted_year):
    directory = Path(base)
    if not directory.is_dir():
        raise ValueError(f'MISSING_INPUT: {base}')
    regex = re.compile(_window_pattern(pattern, first, last, submitted_year))
    found = sorted(p for p in directory.iterdir() if p.is_file() and regex.fullmatch(p.name))
    if not found:
        raise ValueError(f'DECADAL_MISSING_INPUT: {directory}/{regex.pattern}')
    # Exact frozen alternatives prevent later years or remapped siblings
    # appearing on disk from entering the worker's selection.
    frozen = '^(?:' + '|'.join(re.escape(p.name) for p in found) + ')$'
    return found, frozen


def _spatial_signature(path):
    stat = path.stat()
    header = _header(str(path), stat.st_size, stat.st_mtime_ns)
    # Variable names differ for the vector partners; compare geometry/shapes,
    # not the physical field's name or its display attributes.
    return header['coordinates'], sorted(header['shapes'].values())


def configure_decadal_doc(doc, first, last, repointer=None):
    """Mutate a one-rule document; return every validated frozen dependency.

    Call after normal repointing/new_doc and vector-policy configuration, but
    instead of the annual dependencies() function. Unused inherited auxiliary
    options (notably basin_mask_file) must be pruned by the caller first.
    """
    if last - first != 9:
        raise ValueError('decadal window must contain exactly ten years')
    if len(doc.get('rules', [])) != 1:
        raise ValueError('configure_decadal_doc requires one rule')
    rule = doc['rules'][0]
    if rule['compound_name'].split('.')[-2] != 'dec':
        raise ValueError('configure_decadal_doc only supports dec products')
    effective = dict(doc.get('inherit', {}), **rule)
    submitted = int(effective.get('year', last))
    dependencies, temporal_files = set(), set()
    primary_files, vector_files = [], []
    for inp in rule.get('inputs', []):
        files, inp['pattern'] = _resolve(inp['path'], inp['pattern'], first, last, submitted)
        dependencies.update(files)
        temporal_files.update(files)
        primary_files.extend(files)
    for name, value in effective.items():
        if name.endswith('_pattern') and name[:-8] + '_path' in effective:
            files, frozen = _resolve(effective[name[:-8] + '_path'], value, first, last, submitted)
            rule[name] = frozen
            dependencies.update(files)
            temporal_files.update(files)
            if name == 'vector_partner_pattern':
                vector_files.extend(files)
        if name.endswith('_file') and isinstance(value, str) and value.startswith('/'):
            p = Path(value)
            if '*' in value or '?' in value:
                raise ValueError(f'DECADAL_UNFROZEN_AUXILIARY: {name}={value}')
            if not p.is_file() or not p.stat().st_size:
                raise ValueError(f'MISSING_AUXILIARY: {value}')
            dependencies.add(p)
    info = validate_monthly_streams(temporal_files, first, last)
    if vector_files and _spatial_signature(primary_files[0]) != _spatial_signature(vector_files[0]):
        raise ValueError('DECADAL_VECTOR_GRID_MISMATCH: vector components have different coordinates/levels')
    rule.update(explicit_decadal_window=True, year=last, year_start=first, year_end=last,
                calendar=info['calendar'], decadal_calendar=info['calendar'],
                decadal_dates=[list(d) for d in info['dates']],
                decadal_month_weights=info['weights'],
                decadal_input_files=[str(p) for p in sorted(temporal_files)])
    doc.setdefault('inherit', {}).update(year=last, year_start=first, year_end=last)
    selected = set(rule.get('pipelines', []))
    replaced = False
    for pipeline in doc.get('pipelines', []):
        if pipeline['name'] in selected:
            for i, step in enumerate(pipeline.get('steps', [])):
                if step == 'pycmor.std_lib.timeaverage.timeavg':
                    pipeline['steps'][i] = 'pycmor.std_lib.decadal.explicit_decadal_mean'
                    replaced = True
    if not replaced:
        raise ValueError('DECADAL_UNKNOWN_PIPELINE: cannot install verified weighted mean')
    return sorted(dependencies)


def validate_decadal_output(files, first, last):
    """Require exactly one mean sample with the full decade's actual bounds."""
    records = 0
    for path in files:
        with netCDF4.Dataset(path) as ds:
            if 'time' not in ds.variables:
                raise ValueError('DECADAL_OUTPUT_NO_TIME')
            t = ds['time']
            calendar = getattr(t, 'calendar', 'standard')
            bname = getattr(t, 'bounds', None)
            if not bname or bname not in ds.variables or ds[bname].shape != (1, 2) or len(t) != 1:
                raise ValueError('DECADAL_OUTPUT_BOUNDS: expected one record and two bounds')
            b = ds[bname]
            lo, hi = netCDF4.num2date(b[0], getattr(b, 'units', t.units), calendar)
            if _stamp(lo) != (first, 1, 1, 0, 0, 0) or _stamp(hi) != (last + 1, 1, 1, 0, 0, 0):
                raise ValueError('DECADAL_OUTPUT_WINDOW: incorrect ten-year bounds')
            midpoint = netCDF4.num2date(t[0], t.units, calendar)
            units = f'seconds since {first:04d}-01-01'
            a, z, m = cftime.date2num([lo, hi, midpoint], units, calendar)
            if abs(m - (a + z) / 2) > 1.0:
                raise ValueError('DECADAL_OUTPUT_MIDPOINT: mean timestamp is not interval midpoint')
            records += 1
    if records != 1:
        raise ValueError(f'DECADAL_OUTPUT_RECORDS: expected one record, got {records}')
    return records
