"""Daily storage changes with a verified previous-day boundary.

The existing HR/LR four-layer storage convention is retained. This step fixes
only lost year-boundary differences; it does not infer missing initial data.
"""
import numpy as np
import xarray as xr


def storage_variables(model_variable):
    if model_variable == 'skin_reservoir':
        return ['src']
    if model_variable == 'sd':
        return ['sd']
    if model_variable in ('soil_moisture', 'total_water'):
        return [f'swvl{i}' for i in range(1, 5)] + (
            ['sd', 'src'] if model_variable == 'total_water' else [])
    raise ValueError(f'Unsupported boundary-difference variable: {model_variable}')


def _storage(data, model_variable, rule):
    if model_variable == 'skin_reservoir':
        return data['src'].astype('float64') * 1000.0
    if model_variable == 'sd':
        return data['sd'].astype('float64') * 1000.0 * float(rule.get('layer_thickness', 1.0)) * float(rule.get('scale_factor', 1.0))
    soil = sum(data[f'swvl{i}'] * depth for i, depth in
               enumerate((0.07, 0.21, 0.72, 1.89), start=1)) * 1000.0
    if model_variable == 'soil_moisture':
        return soil
    if model_variable == 'total_water':
        return soil + 1000.0 * (data['sd'] + data['src'])
    raise ValueError(f'Unsupported boundary-difference variable: {model_variable}')


def compute_temporal_diff_with_boundary(data, rule):
    """Return every target-year daily difference, including January 1.

    The previous-day files are pinned separately from current-year inputs so
    the ordinary annual gather filter cannot discard the boundary. Only one
    slice from each previous-year file is loaded into memory.
    """
    model_variable = rule.get('model_variable')
    records = rule.get('land_boundary_records')
    names = storage_variables(model_variable)
    if not records or set(records) != set(names):
        raise ValueError('Missing validated previous-day storage inputs')
    if 'time' not in data.dims or not data.sizes['time']:
        raise ValueError('Current-year storage has no time axis')
    first = data.time.values[0]
    target_year = int(rule.get('year_start', rule.get('year')))
    if int(data.time.dt.year.values[0]) != target_year:
        raise ValueError('Current storage does not start in the requested year')
    previous = {}
    for name in names:
        entry = records[name]
        with xr.open_dataset(entry['file']) as source:
            time_name = next((n for n in ('time', 'time_counter') if n in source.dims), None)
            if time_name is None or name not in source:
                raise ValueError(f'Missing previous-day stream: {name}')
            part = source[name].isel({time_name: [int(entry['index'])]}).load()
            if time_name != 'time':
                part = part.rename({time_name: 'time'})
        old = part.time.values[0]
        elapsed = first - old
        seconds = (float(elapsed / np.timedelta64(1, 's'))
                   if isinstance(elapsed, np.timedelta64) else elapsed.total_seconds())
        if not np.isclose(seconds, 86400, rtol=0, atol=1e-6):
            raise ValueError(f'Previous-day storage is not exactly one day earlier: {name}')
        if {d: n for d, n in part.sizes.items() if d != 'time'} != {
                d: n for d, n in data[name].sizes.items() if d != 'time'}:
            raise ValueError(f'Previous/current storage dimensions differ: {name}')
        for coordinate in part.coords:
            if 'time' in part[coordinate].dims or coordinate not in data[name].coords:
                continue
            if not np.array_equal(part[coordinate].values, data[name][coordinate].values,
                                  equal_nan=True):
                raise ValueError(f'Previous/current storage coordinates differ: {coordinate}')
        previous[name] = part
    before = _storage(xr.Dataset(previous), model_variable, rule)
    current = _storage(data, model_variable, rule)
    # XIOS also carries a redundant time_centered coordinate. On years outside
    # numpy's nanosecond range it may be a dask object array; concatenating it
    # with an eager one-day cftime array triggers unsupported auto-rechunking.
    # Only the canonical time index is needed for differencing. Reattach the
    # unchanged target-year auxiliary coordinates after the calculation.
    before = before.drop_vars([n for n in before.coords
                               if n != 'time' and 'time' in before[n].dims])
    current = current.drop_vars([n for n in current.coords
                                 if n != 'time' and 'time' in current[n].dims])
    series = xr.concat([before, current], dim='time', join='exact', coords='minimal',
                       compat='equals')
    difference = series.diff('time')
    if difference.sizes['time'] != current.sizes['time']:
        raise ValueError('Boundary handling changed target-year record count')
    difference.attrs.update(units='kg m-2',
        comment='Daily storage change uses the verified previous December 31 value for January 1.')
    difference.name = model_variable
    result = difference.to_dataset()
    for coordinate in data.coords:
        if coordinate not in result.coords and coordinate != 'time':
            result.coords[coordinate] = data.coords[coordinate]
    return result
