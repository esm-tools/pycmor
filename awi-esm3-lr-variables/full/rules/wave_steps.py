"""WAM field selection on the audited sea-point grid (step after load_mfdataset)."""
import numpy as np
import xarray as xr


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
