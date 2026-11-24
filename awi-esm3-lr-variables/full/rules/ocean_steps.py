"""Native FESOM ocean products using the run's actual tracer cells and flux conventions."""
from pathlib import Path
import re

import numpy as np
import xarray as xr


def thickness_input(rule):
    pinned = rule.get('ocean_thickness_file')
    if pinned:
        return Path(pinned)
    years = range(int(rule.get('year_start', rule.get('year'))),
                  int(rule.get('year_end', rule.get('year'))) + 1)
    matches = [p for p in Path(rule.get('hnode_path')).glob('*.nc')
               if re.fullmatch(rule.get('hnode_pattern'), p.name)
               and any(p.name.endswith(f'.{y}.nc') for y in years)]
    if len(matches) != 1:
        raise ValueError('Ocean column reduction requires one annual native hnode file')
    return matches[0]


def column_statistic(tracer, thickness, grid, limit=None, inventory=False):
    """Integrate over actual wet thickness. Unknown tracer in a contributing cell masks its column."""
    tracer, thickness = xr.align(tracer, thickness, join='exact')
    if set(tracer.dims) != {'time', 'nod2', 'nz'} or set(thickness.dims) != set(tracer.dims):
        raise ValueError('Tracer and hnode must share native time, node and layer dimensions')
    upper = np.abs(np.asarray(grid['nz']))[:-1]
    top = np.asarray(grid['ulevels_nod2D'], dtype=int) - 1
    bottom = np.asarray(grid['nlevels_nod2D'], dtype=int) - 1
    if (len(upper) != tracer.sizes['nz'] or len(top) != tracer.sizes['nod2']
            or np.any(top < 0) or np.any(top >= bottom) or np.any(bottom > len(upper))):
        raise ValueError('FESOM geometry does not match the native wet tracer indices')
    level = xr.DataArray(np.arange(len(upper)), dims='nz')
    wet = (level >= xr.DataArray(top, dims='nod2')) & (level < xr.DataArray(bottom, dims='nod2'))
    used = wet
    if limit is not None:
        ceiling = xr.DataArray(np.abs(np.asarray(grid['zbar_n_surface'])), dims='nod2')
        layer_top = xr.where(level == xr.DataArray(top, dims='nod2'),
                             ceiling, xr.DataArray(upper, dims='nz'))
        span = (limit - layer_top).clip(min=0)
        used = wet & (span > 0)
        weight = np.minimum(thickness, span)
    else:
        weight = thickness
    # Never let skipna turn unknown wet-layer data into a shallower integral.
    good = (~used | (np.isfinite(tracer) & np.isfinite(weight) & (weight > 0))).all('nz')
    weight = xr.where(used, weight.astype('float64'), 0.)
    total = (weight * tracer.where(used, 0.)).sum('nz', skipna=False)
    depth = weight.sum('nz', skipna=False)
    value = total * 1030.0 if inventory else total / depth.where(depth > 0)
    return value.where(good & (depth > 0)).transpose('time', 'nod2')


def compute(data, rule):
    operation = rule.get('ocean_physics_operation')
    if operation in ('mean', 'inventory'):
        with xr.open_dataset(thickness_input(rule), chunks={'time': 1, 'nod2': 8192},
                             use_cftime=True) as ds, xr.open_dataset(rule.get('ocean_geometry_file')) as grid:
            thickness = ds['hnode']
            # Some readers present model years as cftime while others use datetime64.
            if not np.array_equal(data.time.dt.strftime('%Y%m%d%H%M%S'),
                                  thickness.time.dt.strftime('%Y%m%d%H%M%S')):
                raise ValueError('Monthly tracer and native thickness timestamps do not match')
            thickness = thickness.assign_coords(time=data.time)
            result = column_statistic(data, thickness, grid, rule.get('ocean_depth_limit'),
                                      inventory=operation == 'inventory')
        units = ('degC kg m-2' if data.name == 'temp' else 'g m-2') if operation == 'inventory' else 'degC'
    elif operation == 'vertical_velocity':
        with xr.open_dataset(rule.get('ocean_geometry_file')) as grid:
            centers = np.abs(np.asarray(grid['nz1']))
        if data.sizes.get('nz1') != len(centers) + 1:
            raise ValueError('Daily vertical velocity must contain both interfaces of every native layer')
        # Independent arrays retain the masks at the surface, shelves, and bottom.
        a = data.isel(nz1=slice(None, -1)).rename(nz1='nz').assign_coords(nz=centers)
        b = data.isel(nz1=slice(1, None)).rename(nz1='nz').assign_coords(nz=centers)
        result = 0.5 * (a + b)
        result.nz.attrs.update(standard_name='depth', long_name='depth below sea level', positive='down', units='m')
        units = 'm s-1'
    elif operation == 'salinity':
        result, units = data, '1e-3'
    elif operation == 'river_salt':
        # Pure-water river input contributes no physical salt, including in cavities.
        from pycmor.core.utils import get_function_from_script
        check_build = get_function_from_script(str(Path(__file__).with_name('ocean_validation.py')), 'fresh_river_profile')
        check_build(rule.get('ocean_fresh_river_binary_sha256'))
        result = xr.zeros_like(data, dtype=np.float32)
        units = 'kg m-2 s-1'
    elif operation == 'water_flux':
        result, units = -1025.0 * data, 'kg m-2 s-1'
    elif operation == 'heat_flux':
        result, units = -data, 'W m-2'
    else:
        raise ValueError(f'Unknown native ocean operation {operation!r}')
    result = result.transpose('time', 'nod2', ...) if 'nod2' in result.dims else result
    result.name = data.name
    result.attrs = {**data.attrs, 'units': units}
    return result
