"""Surface and fixed-depth extraction, column integrals, monthly means of daily
streams and the lowest OpenIFS model level for the LR raw rules."""
import cftime
import numpy as np
import xarray as xr

from pycmor.awiesm3_lr import _select_years, daily_time, monthly_time, transform


def transform_and_surface(data, rule):
    data = transform(data, rule)
    return surface_only(data, rule)


def surface_only(data, rule):
    if rule.get('surface_layer', False):
        dims = [d for d in ('nz','nz1','nz_sfc','depth','olevel') if d in data.dims]
        if len(dims) != 1:
            raise ValueError(f'Expected one ocean depth dimension for surface extraction; found {dims}')
        if dims[0] == 'nz_sfc' and data.sizes['nz_sfc'] != 1:
            raise ValueError('Native daily surface stream must have exactly one nz_sfc layer')
        # First physical layer only; preserve missing values, including cavities.
        data = data.isel({dims[0]: 0}, drop=True)
    return data


def transform_and_target_depth(data, rule):
    return target_depth_only(transform(data, rule), rule)


def target_depth_only(data, rule):
    """One fixed depth (lr_target_depth, m) from CORE3 layer centres or interfaces.

    A level at the target is taken as is; otherwise linear in depth between the
    two bracketing levels. Missing below the sea floor if either level is
    missing; never extrapolated.
    """
    target = float(rule.get('lr_target_depth'))
    dims = [d for d in ('nz_200','nz','nz1','depth','olevel') if d in data.dims]
    if len(dims) != 1:
        raise ValueError(f'Expected one ocean depth dimension for depth selection; found {dims}')
    dim = dims[0]
    depth = np.abs(np.asarray(data[dim].values, dtype='f8'))
    if np.any(np.diff(depth) <= 0):
        raise ValueError('Ocean depth axis must increase downward')
    with xr.open_dataset(rule.get('grid_file'), decode_times=False) as mesh:
        levels = np.concatenate([mesh.depth.values, mesh.depth_bnds.values])
    if not all(np.isclose(levels, d, rtol=0, atol=1e-3).any() for d in depth):
        raise ValueError('Depth levels are not CORE3 layer centres or interfaces')
    attrs, name = dict(data.attrs), data.name
    exact = np.flatnonzero(np.isclose(depth, target, rtol=0, atol=1e-6))
    if exact.size:
        result = data.isel({dim: int(exact[0])}, drop=True)
    else:
        upper = int(np.searchsorted(depth, target))
        if upper == 0 or upper == depth.size:
            raise ValueError(f'{target} m is outside the source levels {depth.min()}-{depth.max()} m')
        weight = (target - depth[upper-1]) / (depth[upper] - depth[upper-1])
        result = (data.isel({dim: upper-1}, drop=True) * (1 - weight)
                  + data.isel({dim: upper}, drop=True) * weight)
    result.attrs, result.name = attrs, name
    return result


def vertical_integral(data, rule):
    """Column sum of a layer field times the layer thickness of the same month."""
    with xr.open_mfdataset(rule.get('lr_thickness_files'), combine='by_coords', join='exact',
                           chunks={'time': 1}) as ds:
        thickness = ds[rule.get('lr_thickness_variable')]
        if thickness.attrs.get('units') != rule.get('lr_thickness_units'):
            raise ValueError(f'Unexpected layer thickness units: {thickness.attrs.get("units")}')
        thickness = monthly_time(_select_years(thickness, rule), rule).astype('float64')
        dims = [d for d in ('nz','nz1','depth') if d in data.dims]
        if len(dims) != 1 or dims[0] not in thickness.dims:
            raise ValueError('Layer field and thickness need the same single vertical dimension')
        attrs, name = dict(data.attrs), data.name
        data, thickness = xr.align(data, thickness, join='exact')
        # Masked (cavity, below-bottom) layers contribute nothing; an all-masked
        # column stays missing.
        result = (data * thickness).sum(dims[0], min_count=1).load()
    result.attrs, result.name = attrs, name
    return result


def daily_to_monthly_mean(data, rule):
    """Monthly mean of a complete daily-mean stream, labelled at mid-month."""
    data = daily_time(data, rule)
    calendar = data.time.dt.calendar
    key = (data.time.dt.year * 100 + data.time.dt.month).rename('month')
    monthly = data.groupby(key).mean('time', keep_attrs=True)
    times = []
    for value in monthly.month.values:
        year, month = divmod(int(value), 100)
        start = cftime.datetime(year, month, 1, calendar=calendar)
        stop = cftime.datetime(year + (month == 12), month % 12 + 1, 1, calendar=calendar)
        times.append(start + (stop - start) / 2)
    monthly = monthly.rename(month='time').assign_coords(time=times)
    monthly.attrs = dict(data.attrs)
    return monthly


def transform_and_lowest_level(data, rule):
    return lowest_model_level(transform(data, rule), rule)


def lowest_model_level(data, rule):
    """OpenIFS model level N (the one next to the surface) of a 1..N, top-first axis."""
    if 'model_levels' not in data.dims:
        raise ValueError('Expected an OpenIFS model_levels dimension')
    levels = np.asarray(data.model_levels.values)
    if not np.array_equal(levels, np.arange(1, levels.size + 1)):
        raise ValueError('Expected model levels 1..N ordered top to surface')
    return data.isel(model_levels=-1, drop=True)
