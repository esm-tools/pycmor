"""Deliver the requested regular-grid copies with the run's CORE3-to-g131 XIOS weights."""

from copy import deepcopy
from functools import lru_cache
from pathlib import Path

import netCDF4
import numpy as np
from scipy.sparse import csr_matrix
import xarray as xr

from pycmor.core.utils import get_function_from_script


PRODUCTS = {
    'ocean.so.tavg-ol-hxy-sea.day.glb',
    'ocean.wfo.tavg-u-hxy-sea.3hr.glb',
    'ocean.sfriver.tavg-u-hxy-sea.mon.glb',
}
# EMD g131 (WCRP-CMIP/Essential-Model-Documentation issue 1204).
LAT = np.arange(-89.75, 90., 0.5)
LON = np.arange(0.25, 360., 0.5)
NCELLS = 220509


def configure(doc, experiment, period, config, inputs):
    """Attach the g131 variant to the same scientific rule and task as its native copy."""
    original, = doc['rules']
    if original['compound_name'] not in PRODUCTS:
        return
    root = Path(experiment)
    weights = root / f'run_{period}' / 'work/xios_interpolation_weights_fesom_nod2_nodes_reg.nc'
    regular = root / 'outdata/fesom' / f'fw.fesom.gr.{doc["inherit"]["year_start"]}.nc'
    native = Path(original.get('grid_file') or doc['inherit']['grid_file'])
    for path in (weights, regular, native):
        if not path.is_file():
            raise ValueError(f'Missing required ocean grid input: {path}')
    geometry(str(regular))
    pipeline_name, = original['pipelines']
    pipeline = deepcopy(next(p for p in doc['pipelines'] if p['name'] == pipeline_name))
    pipeline['name'] += '_g131'
    steps = pipeline['steps']
    # The scientific step returns a DataArray in the requested units. Time and depth
    # coordinates pass through the remap, before the normal bounds/metadata stages.
    steps.insert(steps.index('pycmor.std_lib.timeaverage.timeavg'), f'script://{Path(__file__).resolve()}:remap')
    rule = deepcopy(original)
    rule.update(name=original['name'] + '_g131', pipelines=[pipeline['name']], grid_label='g131',
                nominal_resolution='50 km', grid='Regular 0.5-degree latitude–longitude grid, remapped from FESOM CORE3 using XIOS first-order overlap weights normalized over valid ocean cells.',
                grid_file=str(regular), ocean_source_mesh=str(native), ocean_spherical_weights=str(weights))
    doc['pipelines'].append(pipeline)
    doc['rules'].append(rule)
    inputs.update((weights, regular, native))


@lru_cache(maxsize=12)
def geometry(path):
    with netCDF4.Dataset(path) as grid:
        if not np.array_equal(grid['lat'][:], LAT) or not np.array_equal(grid['lon'][:], LON):
            raise ValueError(f'{path}: coordinates differ from registered grid g131')
    return LAT, LON


def read_weights(path):
    with netCDF4.Dataset(path) as ds:
        if (len(ds.dimensions['n_src']), len(ds.dimensions['n_dst'])) != (NCELLS, LAT.size * LON.size):
            raise ValueError(f'{path}: unexpected source or target grid size')
        src = np.asarray(ds['src_idx'][:], dtype=np.int64) - 1
        dst = np.asarray(ds['dst_idx'][:], dtype=np.int64) - 1
        weights = np.asarray(ds['weight'][:], dtype=float)
    if (not src.size or src.shape != dst.shape or weights.shape != src.shape
            or src.min() < 0 or src.max() >= NCELLS or dst.min() < 0 or dst.max() >= LAT.size * LON.size
            or not np.all(np.isfinite(weights)) or np.any(weights < 0)):
        raise ValueError(f'{path}: invalid XIOS overlap coefficients')
    totals = np.bincount(dst, weights=weights)
    if not np.allclose(totals[totals > 0], 1., atol=1e-8, rtol=0):
        raise ValueError(f'{path}: XIOS coefficients do not sum to one on represented cells')
    return src, dst, weights


@lru_cache(maxsize=2)
def matrix(path):
    src, dst, weights = read_weights(path)
    return csr_matrix((weights, (dst, src)), shape=(LAT.size * LON.size, NCELLS))


def map_block(values, path):
    weights = matrix(path)
    data = values.reshape(-1, weights.shape[1])
    valid = np.isfinite(data)
    coverage = (weights @ valid.T.astype('float64')).T
    summed = (weights @ np.where(valid, data, 0.).T).T
    out = np.divide(summed, coverage, out=np.full_like(summed, np.nan), where=coverage > 0.)
    return out.reshape(values.shape[:-1] + (LAT.size, LON.size))


def check_nodes(lon, lat, mesh_path):
    with netCDF4.Dataset(mesh_path) as mesh:
        x = np.asarray(mesh['lon'][:]); y = np.asarray(mesh['lat'][:])
    # Use the same spherical node-order check as the native CORE3 quality tests.
    same_nodes = get_function_from_script(str(Path(__file__).with_name('iceberg_validation.py')), '_same_nodes')
    if x.shape != (NCELLS,) or not same_nodes(lon, lat, x, y):
        raise ValueError('Ocean node coordinates/order differ from the native CORE3 mesh')


def remap(data, rule):
    """Renormalize wet contributions separately at every time and depth."""
    if data.sizes.get('nod2') != NCELLS:
        raise ValueError('Expected the CORE3 nodal field before horizontal remapping')
    check_nodes(data.lon.values, data.lat.values, rule.get('ocean_source_mesh'))
    lat, lon = geometry(rule.grid_file)
    chunks = {dim: (-1 if dim == 'nod2' else 1 if dim == 'time' else 4) for dim in data.dims}
    data = data.chunk(chunks)
    result = xr.apply_ufunc(map_block, data, kwargs={'path': rule.get('ocean_spherical_weights')},
                            input_core_dims=[['nod2']], output_core_dims=[['lat', 'lon']],
                            output_dtypes=[np.float64], dask='parallelized', keep_attrs=True,
                            dask_gufunc_kwargs={'output_sizes': {'lat': len(lat), 'lon': len(lon)}})
    result = result.assign_coords(lat=('lat', lat, {'units': 'degrees_north', 'standard_name': 'latitude'}),
                                  lon=('lon', lon, {'units': 'degrees_east', 'standard_name': 'longitude'}))
    # The source variable can carry its unstructured auxiliary coordinate list.
    result.attrs.pop('coordinates', None)
    return result.rename(data.name)


def stamp(path):
    with netCDF4.Dataset(path) as ds:
        time = ds['time']
        return (time.units, getattr(time, 'calendar', 'standard'), len(time), float(time[0]), float(time[-1]))


def coordinate(ds, standard_name, dims):
    return next(v for name,v in ds.variables.items()
                if (getattr(v, 'standard_name', '') == standard_name or name in (standard_name, standard_name[:3]))
                and len(v.dimensions) == 1 and v.dimensions[0] in dims)


def validate(cfg, mapped_files, native_files):
    """Independently reconstruct sampled full horizontal slices from the written native output."""
    mapped, = [r for r in cfg['rules'] if r.get('grid_label') == 'g131']
    name = mapped['compound_name'].split('.')[1]
    src, dst, w = read_weights(mapped['ocean_spherical_weights'])
    pairs = {stamp(path): path for path in native_files}
    if len(pairs) != len(native_files) or len(pairs) != len(mapped_files):
        raise ValueError('Ocean native and g131 files have different or repeated time coverage')
    compared = 0
    slices = 0
    for file in mapped_files:
        with netCDF4.Dataset(file) as out, netCDF4.Dataset(pairs.pop(stamp(file))) as native:
            a, b = native[name], out[name]
            x, y = coordinate(native, 'longitude', a.dimensions), coordinate(native, 'latitude', a.dimensions)
            check_nodes(np.asarray(x[:]), np.asarray(y[:]), mapped['ocean_source_mesh'])
            lat, lon = coordinate(out, 'latitude', b.dimensions), coordinate(out, 'longitude', b.dimensions)
            if not np.array_equal(lat[:], LAT) or not np.array_equal(lon[:], LON) or out.grid_label != 'g131':
                raise ValueError(f'{file}: incorrect regular grid')
            for coord in (lat, lon):
                if not np.allclose(out[coord.bounds][:], np.column_stack((coord[:] - .25, coord[:] + .25)), rtol=0, atol=1e-5):
                    raise ValueError(f'{file}: incorrect {coord.name} bounds')
            if not np.array_equal(out['time'][:],native['time'][:]):
                raise ValueError(f'{file}: native and mapped time coordinates differ')
            node = x.dimensions[0]
            z = [d for d in a.dimensions if d not in ('time',node)]
            oz = [d for d in b.dimensions if d not in ('time',lat.dimensions[0],lon.dimensions[0])]
            if len(z) != len(oz) or len(z) > 1 or (z and not np.array_equal(native[z[0]][:],out[oz[0]][:])):
                raise ValueError(f'{file}: native and mapped vertical coordinates differ')
            if z:
                src_depth, dst_depth = native[z[0]], out[oz[0]]
                if (getattr(src_depth, 'bounds', '') not in native.variables
                        or getattr(dst_depth, 'bounds', '') not in out.variables
                        or not np.array_equal(native[src_depth.bounds][:], out[dst_depth.bounds][:])):
                    raise ValueError(f'{file}: native and mapped vertical bounds differ')
            levels = len(native.dimensions[z[0]]) if z else 1
            for ti in sorted({0, len(out['time']) // 2, len(out['time']) - 1}):
                for zi in sorted({0, levels // 2, levels - 1}):
                    av = np.ma.filled(a[tuple(slice(None) if d == node else ti if d == 'time' else zi for d in a.dimensions)].astype('float64'), np.nan)
                    idx = tuple(ti if d == 'time' else zi if d in oz else slice(None) for d in b.dimensions)
                    bv = np.ma.filled(b[idx].astype('float64'), np.nan)
                    if b.dimensions.index(lat.dimensions[0]) > b.dimensions.index(lon.dimensions[0]):
                        bv = bv.T
                    bv = bv.reshape(-1)
                    finite = np.isfinite(av[src])
                    den = np.bincount(dst[finite], weights=w[finite], minlength=bv.size)
                    num = np.bincount(dst[finite], weights=w[finite] * av[src[finite]], minlength=bv.size)
                    wet = den > 0
                    if not np.array_equal(np.isfinite(bv), wet):
                        raise ValueError(f'{file}: remapped wet mask differs from native at time {ti}, layer {zi}')
                    # Absolute weighted magnitude accounts for cancellation of signed fluxes
                    # after the native and mapped outputs have each been quantized.
                    mag = np.bincount(dst[finite], weights=w[finite] * np.abs(av[src[finite]]), minlength=bv.size)
                    if np.any(np.abs(bv[wet] - num[wet]/den[wet]) > 5e-5 * mag[wet]/den[wet] + 1e-16):
                        raise ValueError(f'{file}: remapped values differ from native at time {ti}, layer {zi}')
                    compared += int(wet.sum())
                    slices += 1
    if pairs or compared == 0:
        raise ValueError('No matching valid native/g131 fields verified')
    return {'compared_samples': compared, 'horizontal_slices': slices, 'grid_label': 'g131'}
