"""Planner hooks for the independently reviewed surface iceberg products."""
import copy
from pathlib import Path

import netCDF4
import numpy as np
import yaml

# XIOS writes node coordinates as float32 recomputed through Cartesian space.
# Latitude is ill-conditioned near the pole: on CORE3 the 29 nodes poleward of
# 87.8 deg differ from mesh.nc by up to 0.003 deg (sst shows the same), and
# longitude differs by more at high latitude. Compare positions on the sphere
# instead: 0.01 deg is far below the 0.069 deg minimum CORE3 node spacing, so a
# reordered or different node still fails.
NODE_TOLERANCE_DEG = 0.01


def node_offset(lon, lat, reference_lon, reference_lat):
    """Great-circle distance in degrees between paired nodes."""
    lon, lat, rlon, rlat = (np.deg2rad(np.asarray(x, dtype='f8')) for x in (lon, lat, reference_lon, reference_lat))
    h = np.sin((lat - rlat) / 2) ** 2 + np.cos(lat) * np.cos(rlat) * np.sin((lon - rlon) / 2) ** 2
    return np.rad2deg(2 * np.arcsin(np.sqrt(np.clip(h, 0, 1))))


def same_nodes(lon, lat, reference_lon, reference_lat):
    lon, lat = (np.ma.filled(np.ma.asarray(x, dtype='f8'), np.nan) for x in (lon, lat))
    if lon.shape != np.shape(reference_lon) or lat.shape != np.shape(reference_lat):
        return False
    offset = node_offset(lon, lat, reference_lon, reference_lat)
    return bool(np.isfinite(offset).all() and offset.max(initial=0) <= NODE_TOLERANCE_DEG)


def rules(rules_directory):
    document = yaml.safe_load((Path(rules_directory) / 'iceberg.yaml').read_text())
    if document.get('schema_version') != 1:
        raise ValueError('Unsupported iceberg rule schema')
    for item in document['rules']:
        rule = copy.deepcopy(document['defaults'])
        rule.update(copy.deepcopy(item))
        yield rule


def install_pipeline(profile, reproduce_directory):
    for name, step in (('lr_iceberg_surface', 'load_surface_freshwater'),
                       ('lr_iceberg_layers', 'load_layer_freshwater')):
        pipeline = copy.deepcopy(next(p for p in profile['pipelines'] if p['name'] == 'lr_linear'))
        pipeline['name'] = name
        pipeline['steps'][0] = f'script://{Path(reproduce_directory)}/iceberg_steps.py:{step}'
        if any(p['name'] == pipeline['name'] for p in profile['pipelines']):
            raise ValueError('Iceberg pipeline installed twice')
        profile['pipelines'].append(pipeline)


def validate_rule(rule):
    """Fail the plan on grid/stream mismatch without reading large flux fields.

    Full monthly completeness is checked by raw.materialize; this additionally
    verifies node order and exact time/bound alignment across the four terms.
    The worker rejects unexplained missing values and cavity fluxes.
    """
    with netCDF4.Dataset(rule['grid_file']) as ds:
        if not {'lon', 'lat', 'cav_nod_mask'}.issubset(ds.variables):
            raise ValueError('ICEBERG_GRID: explicit native cavity mask required')
        lon, lat, cavity = (np.asarray(ds[n][:]) for n in ('lon', 'lat', 'cav_nod_mask'))
        if (lon.ndim != 1 or lat.shape != lon.shape or cavity.shape != lon.shape
                or not all(np.isfinite(x).all() for x in (lon, lat, cavity))
                or not np.isin(cavity, (0, 1)).all()):
            raise ValueError('ICEBERG_GRID: invalid coordinates/cavity mask')
        if rule.get('lr_iceberg_layers'):
            if not {'depth', 'depth_lev', 'cav_nod_lev'}.issubset(ds.variables):
                raise ValueError('ICEBERG_GRID: layer centres and wet-layer range required')
            levels = len(ds['depth'])
            top, bottom = (np.asarray(ds[n][:]).astype(int) for n in ('cav_nod_lev', 'depth_lev'))
            if (top.shape != lon.shape or bottom.shape != lon.shape or (top < 0).any()
                    or (top >= bottom).any() or (bottom > levels).any()):
                raise ValueError('ICEBERG_GRID: invalid wet-layer range')
    names = [t['variable'] for t in rule['lr_terms']]
    if sorted(names) != sorted(('ibfwb', 'ibfwbv', 'ibfwl', 'ibfwe')):
        raise ValueError('ICEBERG_TERMS: expected four reviewed terms')
    reference = None
    for term in rule['lr_terms']:
        records, bounds, calendars = [], [], set()
        for path in term['files']:
            with netCDF4.Dataset(path) as ds:
                field = ds[term['variable']]
                if field.ndim != 2 or field.shape[1] != len(lon) or getattr(field, 'units', '') != 'm s-1':
                    raise ValueError('ICEBERG_INPUT: expected monthly native m s-1 surface field')
                if not same_nodes(ds['lon'][:], ds['lat'][:], lon, lat):
                    raise ValueError('ICEBERG_GRID: node position/order mismatch')
                time = ds['time']
                calendar = getattr(time, 'calendar', 'standard')
                calendar = {'standard': 'gregorian'}.get(calendar, calendar)
                calendars.add(calendar)
                bound = getattr(time, 'bounds', None)
                if not bound or bound not in ds.variables:
                    raise ValueError('ICEBERG_TIME: monthly source bounds required')
                stamp = lambda d: (d.year, d.month, d.day, d.hour, d.minute, d.second)
                dates = netCDF4.num2date(time[:], time.units, calendar)
                decoded = netCDF4.num2date(ds[bound][:], time.units, calendar)
                for date, (lo, hi) in zip(dates, decoded):
                    y, m = date.year, date.month
                    expected_lo = (y, m, 1, 0, 0, 0)
                    expected_hi = (y + (m == 12), 1 if m == 12 else m + 1, 1, 0, 0, 0)
                    if stamp(lo) != expected_lo or stamp(hi) != expected_hi:
                        raise ValueError('ICEBERG_TIME: incomplete monthly averaging interval')
                    records.append(stamp(date))
                    bounds.append((stamp(lo), stamp(hi)))
        signature = (records, bounds, calendars)
        if len(calendars) != 1 or (reference is not None and signature != reference):
            raise ValueError('ICEBERG_ALIGNMENT: monthly times/bounds/calendars differ')
        reference = signature
