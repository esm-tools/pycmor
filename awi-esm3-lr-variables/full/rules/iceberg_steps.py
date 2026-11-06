"""Reviewed native FESOM iceberg freshwater conversion; source files read-only."""
from pathlib import Path

import numpy as np
import xarray as xr

from pycmor.awiesm3_lr import _select_years, monthly_time


TERMS = ('ibfwb', 'ibfwbv', 'ibfwl', 'ibfwe')


def grid_mask(grid_file):
    """Load an explicit binary cavity mask, never infer land/ocean from NaNs."""
    with xr.open_dataset(grid_file, decode_times=False) as grid:
        required = {'lon', 'lat', 'cav_nod_mask'}
        if not required.issubset(grid.variables):
            raise ValueError('ICEBERG_GRID: explicit lon, lat and cav_nod_mask required')
        lon, lat, mask = (np.asarray(grid[n]) for n in ('lon', 'lat', 'cav_nod_mask'))
    if (lon.ndim != 1 or lon.shape != lat.shape or lon.shape != mask.shape
            or not all(np.isfinite(x).all() for x in (lon, lat, mask))
            or not np.isin(mask, (0, 1)).all()):
        raise ValueError('ICEBERG_GRID: invalid coordinates or non-binary cavity mask')
    return lon, lat, mask.astype(bool)


def verify_grid(data, grid):
    lon, lat, cavity = grid
    horizontal = [d for d in data.dims if d != 'time']
    if len(horizontal) != 1 or data.sizes[horizontal[0]] != len(lon):
        raise ValueError('ICEBERG_GRID: expected one native horizontal dimension')
    dim = horizontal[0]
    for coordinate, reference in (('lon', lon), ('lat', lat)):
        if coordinate not in data.coords or data[coordinate].dims != (dim,):
            raise ValueError('ICEBERG_GRID: source requires node-wise lon and lat')
        delta = np.asarray(data[coordinate]) - reference
        if coordinate == 'lon':
            delta = (delta + 180) % 360 - 180
        if not np.isfinite(delta).all() or not np.allclose(delta, 0, rtol=0, atol=1e-4):
            raise ValueError('ICEBERG_GRID: source node order differs from mask')
    return dim, xr.DataArray(cavity, dims=(dim,))


def combine_surface_terms(arrays, grid):
    """Sum the four aligned freshwater terms without filling unknown ocean data."""
    if set(arrays) != set(TERMS):
        raise ValueError('ICEBERG_TERMS: exactly four reviewed melt terms required')
    cleaned = []
    reference = None
    for name in TERMS:
        data = arrays[name]
        if data.attrs.get('units') != 'm s-1':
            raise ValueError(f'ICEBERG_UNITS: {name} must be m s-1')
        dim, cavity = verify_grid(data, grid)
        if reference is not None:
            if (data.dims != reference.dims
                    or not np.array_equal(data.time.values, reference.time.values)):
                raise ValueError('ICEBERG_ALIGNMENT: melt terms have different dimensions or timestamps')
            for coord in ('lon', 'lat'):
                if not np.array_equal(data[coord].values, reference[coord].values):
                    raise ValueError('ICEBERG_ALIGNMENT: melt terms use different node coordinates')
        reference = data
        values = data.astype('float64')
        outside = values.isel({dim: np.flatnonzero(~grid[2])})
        if not bool(np.isfinite(outside).all().compute().item()):
            raise ValueError(f'ICEBERG_MISSING_OCEAN: {name} has missing/nonfinite exposed-ocean values')
        inside = values.isel({dim: np.flatnonzero(grid[2])})
        # Archived cavity streams may be missing or zero, never nonzero flux.
        if bool((np.isinf(inside) | (np.isfinite(inside) & (inside != 0))).any().compute().item()):
            raise ValueError(f'ICEBERG_CAVITY_FLUX: unexpected nonzero/infinite {name} under shelf')
        cleaned.append(values.where(~cavity, 0.0))
    try:
        cleaned = xr.align(*cleaned, join='exact')
    except ValueError as exc:
        raise ValueError('ICEBERG_ALIGNMENT: native coordinates differ') from exc
    result = sum(cleaned) * 1000.0
    result.attrs = dict(units='kg m-2 s-1', positive='down')
    return result


def load_surface_freshwater(data, rule):
    """Load exact planner-frozen files and apply the surface-only conversion."""
    terms = rule.get('lr_terms', [])
    if len(terms) != len(TERMS) or {t['variable'] for t in terms} != set(TERMS):
        raise ValueError('ICEBERG_TERMS: exactly four distinct reviewed terms required')
    arrays, opened = {}, []
    try:
        for term in terms:
            if term.get('units') != 'm s-1' or term.get('coefficient') != 1000.0:
                raise ValueError('ICEBERG_CONVERSION: unreviewed source unit or density')
            files = term.get('files')
            if not files or any(not Path(p).is_file() for p in files):
                raise ValueError('ICEBERG_INPUT: exact frozen source files required')
            ds = xr.open_mfdataset(files, combine='by_coords', join='exact')
            opened.append(ds)
            arrays[term['variable']] = monthly_time(_select_years(ds[term['variable']], rule), rule)
        result = combine_surface_terms(arrays, grid_mask(rule.grid_file))
        # Only surface fields: materialize now so files are closed before later
        # metadata/QC steps and a completed worker cannot leave open handles.
        return result.rename(rule.model_variable).load()
    finally:
        for ds in opened:
            ds.close()
