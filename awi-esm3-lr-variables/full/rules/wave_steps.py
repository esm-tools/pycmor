"""WAM field selection on the audited sea-point grid, then delivery on the registered 1x1 degree grid g150."""
import numpy as np
import xarray as xr

# g150: regular 1x1 degree, latitude centres -89.5..89.5, longitude centres 0..359.
G150_LAT = np.arange(-89.5, 90.0, 1.0)
G150_LON = np.arange(0.0, 360.0, 1.0)


def g150_index(lat, lon_bnds):
    """Map each WAM sea point to the whole-degree cells it covers on a 181 x 360 grid.

    WAM rows sit on whole-degree latitudes; a cell spans the longitudes of its
    bounds, so poleward cells (2 degrees at 60N) cover several 1-degree columns.
    Returns (cell, flat) with flat = row * 360 + column, each flat position once.
    """
    rows = np.rint(np.asarray(lat, dtype=float)).astype(int) + 90
    if np.any(np.abs(rows - 90 - np.asarray(lat, dtype=float)) > 1e-4):
        raise ValueError('WAM rows are not on whole-degree latitudes')
    west, east = np.asarray(lon_bnds, dtype=float).min(1), np.asarray(lon_bnds, dtype=float).max(1)
    cell, flat = [], []
    for k, (r, w, e) in enumerate(zip(rows, west, east)):
        columns = np.arange(np.ceil(w - 1e-6), np.ceil(e - 1e-6)).astype(int) % 360
        cell.append(np.full(columns.size, k))
        flat.append(r * 360 + columns)
    cell, flat = np.concatenate(cell), np.concatenate(flat)
    if np.unique(flat).size != flat.size:
        raise ValueError('WAM cells overlap on the whole-degree grid')
    return cell, flat


def to_g150(values, cell, flat, how='mean'):
    """(time, cells) WAM values -> (time, 180, 360) on g150.

    Each g150 row lies half in two WAM rows: the mean of the available ones
    (one sea value at coasts and the ice edge), the maximum for maximum
    statistics, and the direction of the mean unit vector for directions.
    """
    values = np.asarray(values, dtype=np.float32)
    full = np.full((values.shape[0], 181 * 360), np.nan, dtype=np.float32)
    full[:, flat] = values[:, cell]
    full = full.reshape(-1, 181, 360)
    a, b = full[:, :-1], full[:, 1:]
    with np.errstate(invalid='ignore'):
        if how == 'maximum':
            return np.fmax(a, b)
        if how == 'direction':
            sin = np.nanmean(np.stack([np.sin(np.radians(a)), np.sin(np.radians(b))]), 0)
            cos = np.nanmean(np.stack([np.cos(np.radians(a)), np.cos(np.radians(b))]), 0)
            return (np.degrees(np.arctan2(sin, cos)) % 360).astype(np.float32)
        return np.where(np.isnan(a), b, np.where(np.isnan(b), a, (a + b) / 2))


def remap_wave_to_g150(data, rule):
    """Deliver one WAM field on g150 (see to_g150); land and points WAM masks stay missing."""
    with xr.open_dataset(rule.grid_file) as grid:
        cell, flat = g150_index(grid.lat.values, grid.lon_bnds.values)
    how = rule.get('lr_wave_remap', 'mean')
    if how not in ('mean', 'maximum', 'direction'):
        raise ValueError(f'{rule.name}: unknown lr_wave_remap {how}')
    # Time blocks keep the 181 x 360 intermediates small (3-hourly years are 2920 steps).
    data = data.chunk({'cell': -1, 'time': 124})
    out = xr.apply_ufunc(to_g150, data, kwargs=dict(cell=cell, flat=flat, how=how),
                         input_core_dims=[['cell']], output_core_dims=[['lat', 'lon']],
                         dask='parallelized', output_dtypes=[np.float32],
                         dask_gufunc_kwargs={'output_sizes': {'lat': 180, 'lon': 360}})
    out = out.assign_coords(lat=('lat', G150_LAT, {'units': 'degrees_north', 'standard_name': 'latitude'}),
                            lon=('lon', G150_LON, {'units': 'degrees_east', 'standard_name': 'longitude'}))
    out.attrs = dict(data.attrs)
    return out.rename(data.name)


def select_wave_variable(data, rule):
    """One WAM field, checked against the campaign WAM grid; no averaging here.

    Monthly directions come as XIOS means of sine and cosine; their direction
    is atan2(mean sine, mean cosine), clockwise from north. A 3-hourly window
    statistic is stamped at the end of its window, where the instants are.
    """
    with xr.open_dataset(rule.grid_file) as grid:
        lat, lon = grid.lat.values, grid.lon.values
    cells = int(rule.get('lr_grid_cells', lat.size))
    if lat.size != cells or data.sizes.get('cell') != cells:
        raise ValueError(f'{rule.name}: expected {cells} WAM cells, grid has {lat.size}, data {data.sizes.get("cell")}')
    if (not np.allclose(data.lat.values, lat, rtol=0, atol=1e-5)
            or not np.allclose((data.lon.values - lon + 180) % 360 - 180, 0, rtol=0, atol=1e-5)):
        raise ValueError(f'{rule.name}: WAM cell coordinates differ from the campaign grid')
    expected = rule.get('expected_units')
    names = [rule.model_variable] + ([rule.lr_wave_cosine] if rule.get('lr_wave_cosine') else [])
    for name in names:
        if expected and data[name].attrs.get('units') != expected:
            raise ValueError(f'{rule.name}: {name} in {data[name].attrs.get("units")}, expected {expected}')
    da = data[rule.model_variable]
    if rule.get('lr_wave_cosine'):
        # Missing (ice) months stay missing: XIOS propagates the -999 of the
        # direction into its sine and cosine.
        da = (np.degrees(np.arctan2(da, data[rule.lr_wave_cosine])) % 360).rename(rule.model_variable)
        da.attrs = {'units': 'degree'}
    if rule.get('lr_wave_window_end'):
        bounds = data[data.time.attrs.get('bounds') or data.time.encoding.get('bounds', 'time_bounds')]
        if bounds.sizes[bounds.dims[-1]] != 2:
            raise ValueError(f'{rule.name}: window statistic needs two time bounds')
        da = da.assign_coords(time=bounds.isel({bounds.dims[-1]: 1}).values)
    return da
