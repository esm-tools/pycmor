"""Planning and independent sampled source checks for native LR ocean diagnostics."""
from datetime import timedelta
from functools import lru_cache
import json
from pathlib import Path
import re

from netCDF4 import Dataset, num2date
import numpy as np


# Source native units are part of the conversion contract. Input frequency
# always refers to the model archive, including monthly inputs to annual means.
SOURCE = {
    'heat_flux': ('fh', 'W m-2', '3hr'),
    'water_flux': ('fw', 'm s-1', '3hr'), 'river_salt': ('runoff', 'm s-1', 'mon'),
    'salinity': ('salt', '1e-3', 'day'), 'vertical_velocity': ('w', 'm s-1', 'day'),
    'mean': ('temp', 'degC', 'mon'),
}


def decoded(ds, variable):
    t = ds['time']
    return num2date(variable[:], t.units, getattr(t, 'calendar', 'standard'))


def check_grid(left, right):
    for axis in ('lat', 'lon'):
        a = np.asarray(left[axis][:], dtype=float)
        b = np.asarray(right[axis][:], dtype=float)
        if a.ndim != 1 or a.shape != b.shape:
            raise ValueError('Ocean native node coordinates have different shapes')
        delta = (a - b + 180) % 360 - 180 if axis == 'lon' else a - b
        # XIOS lossily stores the native horizontal coordinates.
        if not np.all(np.isfinite(delta) & (np.abs(delta) < 0.0061)):
            raise ValueError('Ocean native node order disagrees with FESOM geometry')


@lru_cache(maxsize=256)
def check_source(path, variable, units, cadence, geometry, year):
    with Dataset(path) as src, Dataset(geometry) as geo:
        v = src[variable]
        dims = ('time', 'nod2')
        if variable in ('salt', 'temp', 'hnode'):
            dims += ('nz',)
        elif variable == 'w':
            dims += ('nz1',)
        if v.dimensions != dims or getattr(v, 'units', '') != units or 'time: mean' not in getattr(v, 'cell_methods', ''):
            raise ValueError(f'Wrong native dimensions, units or statistic for {path}:{variable}')
        check_grid(src, geo)
        if len(dims) == 3:
            z = 'nz' if variable == 'w' else 'nz1'
            if not np.allclose(src[dims[-1]][:], np.abs(geo[z][:]), rtol=1e-6, atol=1e-4):
                raise ValueError(f'Wrong ocean depth coordinates for {path}')
        times = decoded(src, src['time'])
        bounds = decoded(src, src[src['time'].bounds])
    if bounds.shape != (len(times), 2) or not len(times):
        raise ValueError(f'Missing native time bounds in {path}')
    lo, hi = bounds[:, 0], bounds[:, 1]
    if (lo[0].timetuple()[:6] != (year, 1, 1, 0, 0, 0)
            or hi[-1].timetuple()[:6] != (year + 1, 1, 1, 0, 0, 0)
            or np.any(lo[1:] != hi[:-1])):
        raise ValueError(f'Gaps, duplicate periods, or wrong annual coverage in {path}')
    for t, a, b in zip(times, lo, hi):
        valid = (b - a == timedelta(hours=3) if cadence == '3hr' else
                 b - a == timedelta(days=1) if cadence == 'day' else
                 (a.day == b.day == 1 and a.hour == b.hour == 0
                  and (b.year-a.year)*12+b.month-a.month == 1))
        if not valid or t != a + (b-a)/2:
            raise ValueError(f'Wrong {cadence} interval or midpoint in {path}')


def fresh_river_profile(binary_sha256):
    """Identify a registered build of the reviewed freshwater river formulation."""
    review = Path(__file__).resolve().parents[1] / 'config' / 'scientific_review.json'
    profiles = json.loads(review.read_text())['compatible_profiles']
    for name in ('awiesm3-v3.5.2-cc-94377dfc', 'awiesm3-v3.5.2-cc-fec1cda3'):
        if profiles[name]['fesom_sha256'] == binary_sha256:
            return name
    raise ValueError('Physical river-salt zero requires a verified freshwater river build')


def require_fresh_rivers(config):
    if fresh_river_profile(config.get('_verified_fesom_sha256')) != config.get('scientific_profile'):
        raise ValueError('The verified freshwater river build does not match the scientific profile')


def prepare(rule, primary_files, paths, config):
    """Pin the exact annual sources. Prevent use of run-specific salinity assumptions elsewhere."""
    operation = rule['ocean_physics_operation']
    if len(primary_files) != 1:
        raise ValueError('Expected one annual FESOM ocean input')
    src = Path(next(iter(primary_files)))
    match = re.search(r'\.fesom\.(\d{4})\.nc$', src.name)
    if not match:
        raise ValueError(f'Expected a native annual FESOM file: {src}')
    year = int(match.group(1))
    geometry = Path(rule['ocean_geometry_file'])
    variable = rule['model_variable']
    source = SOURCE.get(operation)
    if operation == 'inventory' and variable in ('salt', 'temp'):
        source = (variable, '1e-3' if variable == 'salt' else 'degC', 'mon')
    if source is None or variable != source[0]:
        raise ValueError(f'Unreviewed native ocean operation or variable: {operation} {variable}')
    check_source(str(src), *source, str(geometry), year)
    rule['ocean_source_file'] = str(src)
    paths.add(geometry)
    if operation in ('mean', 'inventory'):
        matching = [Path(p) for p in paths if Path(p).name == f'hnode.fesom.{year}.nc']
        if len(matching) != 1:
            raise ValueError('An exact matching annual hnode is required')
        rule['ocean_thickness_file'] = str(matching[0])
        check_source(str(matching[0]), 'hnode', 'm', 'mon', str(geometry), year)
        # With varying thickness, weighted monthly means cannot in general
        # recover the timestep mean of the integrated inventory.
        if config and config.get('experiment') and config.get('leg'):
            nml = Path(config['experiment'])/f'run_{config["leg"]}'/'work'/'namelist.config'
            if not nml.is_file() or not re.search(r"(?im)^\s*which_ale\s*=\s*['\"]linfs['\"]",
                                                   nml.read_text()):
                raise ValueError('Need the verified linear-free-surface run to derive exact monthly column statistics')
            paths.add(nml)
        else:
            raise ValueError('Ocean column statistics require the archived leg and its namelist.config')
    if operation == 'river_salt':
        require_fresh_rivers(config or {})
        rule['ocean_fresh_river_binary_sha256'] = config['_verified_fesom_sha256']


def field_at(variable, t, node, level=None):
    idx = tuple(t if dim == 'time' else node if dim in ('cell', 'ncells', 'nod2')
                else level for dim in variable.dimensions)
    if any(i is None for i in idx):
        raise ValueError(f'Unexpected ocean dimension {variable.dimensions}')
    value = variable[idx]
    if np.ma.is_masked(value) or not np.isfinite(value):
        return np.ma.masked
    return float(value)


def validate(cfg, files):
    """Compare time-bound keyed output to the original native series and profiles."""
    from contextlib import ExitStack
    rule = cfg['rules'][0]
    op = rule['ocean_physics_operation']
    name = rule['compound_name'].split('.')[1]
    frequency = rule['compound_name'].split('.')[-2]
    comparisons = masks = 0
    reports = []
    with ExitStack() as stack:
        src = stack.enter_context(Dataset(rule['ocean_source_file']))
        geo = stack.enter_context(Dataset(rule['ocean_geometry_file']))
        v = src[rule['model_variable']]
        source_bounds = decoded(src, src[src['time'].bounds])
        intervals = {(a,b): k for k,(a,b) in enumerate(source_bounds)}
        upper = np.asarray(geo['ulevels_nod2D'][:], dtype=int) - 1
        lower = np.asarray(geo['nlevels_nod2D'][:], dtype=int) - 1
        surface = np.abs(np.asarray(geo['zbar_n_surface'][:], dtype=float))
        z = np.abs(np.asarray(geo['nz'][:], dtype=float))
        h = (stack.enter_context(Dataset(rule['ocean_thickness_file']))['hnode']
             if op in ('mean','inventory') else None)
        candidates = [0,len(upper)//3,len(upper)//2,len(upper)-1, np.argmax(lower-upper), np.argmax(upper)]
        for cond in ((upper == 0) & (lower < len(z)//2), (upper > 0) & (surface < 300)):
            points = np.flatnonzero(cond)
            if len(points):
                candidates.append(points[len(points)//2])
        nodes = sorted(set(map(int, candidates)))
        output_intervals = []
        for path in files:
            with Dataset(path) as ds:
                out = ds[name]
                grid_lon = next(ds[k] for k in ('lon','longitude') if k in ds.variables)
                grid_lat = next(ds[k] for k in ('lat','latitude') if k in ds.variables)
                check_grid({'lon':grid_lon, 'lat':grid_lat}, geo)
                if op in ('salinity', 'vertical_velocity'):
                    depth, = [ds[d] for d in out.dimensions if d in ds.variables
                              and getattr(ds[d], 'standard_name', '') == 'depth']
                    if (not np.array_equal(depth[:], np.abs(geo['nz1'][:]))
                            or getattr(depth, 'bounds', '') not in ds.variables
                            or not np.array_equal(ds[depth.bounds][:], np.column_stack((z[:-1], z[1:])))):
                        raise ValueError('Ocean depth centers or bounds differ from native FESOM geometry')
                out_bounds = decoded(ds, ds[ds['time'].bounds])
                output_intervals.extend(map(tuple,out_bounds))
                limit = rule.get('ocean_depth_limit')
                if limit:
                    depth = ds['depth']
                    if (depth.ndim != 0 or float(depth[...]) != limit/2
                            or not np.allclose(ds[depth.bounds][:], [0.,limit])):
                        raise ValueError('Wrong scalar depth coordinate or bounds for upper-ocean mean')
                for ti in sorted({0,len(out_bounds)//2,len(out_bounds)-1}):
                    a,b = out_bounds[ti]
                    if frequency == 'yr':
                        if (a,b) != (source_bounds[0,0],source_bounds[-1,1]):
                            raise ValueError('Annual ocean output does not span the archived input')
                        indices = range(len(source_bounds))
                    else:
                        indices = [intervals[a,b]]
                    for node in nodes:
                        levels = ([0, int(upper[node]), int(lower[node])-1, len(z)-2]
                                  if op in ('vertical_velocity','salinity') else [None])
                        for level in set(levels):
                            series = []
                            for k in indices:
                                if h is not None:
                                    accum = mass = 0.
                                    for dz, value, layer in zip(h[k,node,:], v[k,node,:], range(len(z)-1)):
                                        if not upper[node] <= layer < lower[node]:
                                            continue
                                        top = surface[node] if layer == upper[node] else z[layer]
                                        if limit and top >= limit:
                                            continue
                                        if np.ma.is_masked(dz) or np.ma.is_masked(value) or dz <= 0:
                                            mass = 0.
                                            break
                                        weight = min(float(dz),limit - top) if limit else float(dz)
                                        mass += weight
                                        accum += weight * float(value)
                                    expect = (accum*1030 if op == 'inventory' else accum/mass) if mass else np.ma.masked
                                elif op == 'vertical_velocity':
                                    expect = np.ma.mean(v[k,node,level:level+2]) if upper[node] <= level < lower[node] else np.ma.masked
                                    if upper[node] <= level < lower[node] and np.ma.count(v[k,node,level:level+2]) != 2:
                                        expect = np.ma.masked
                                elif op == 'salinity':
                                    expect = v[k,node,level]
                                elif op == 'river_salt':
                                    expect = 0.
                                else:
                                    x = v[k,node]
                                    expect = x * {'heat_flux':-1., 'water_flux':-1025.}[op]
                                series.append(expect)
                            valid = np.asarray([not np.ma.is_masked(x) for x in series])
                            if np.any(valid):
                                weights = [(source_bounds[k,1]-source_bounds[k,0]).total_seconds()
                                           for k,ok in zip(indices, valid) if ok]
                                expected = np.average([float(x) for x,ok in zip(series,valid) if ok],weights=weights)
                            else:
                                expected = np.ma.masked
                            actual = field_at(out,ti,node,level)
                            if np.ma.is_masked(expected) != np.ma.is_masked(actual):
                                raise ValueError(f'Ocean wet mask differs at {path}, record {ti}, node {node}, layer {level}')
                            if np.ma.is_masked(actual):
                                masks += 1
                            else:
                                if not np.isclose(actual,expected,rtol=4e-5,atol=1e-12):
                                    raise ValueError(f'Native ocean sample mismatch: {path} record {ti} node {node} layer {level}: {actual} vs {expected}')
                                comparisons += 1
                reports.append({'file':str(path),'check':'native nodes, time bounds, masks and source values'})
        wanted = ([(source_bounds[0,0],source_bounds[-1,1])] if frequency == 'yr'
                  else list(map(tuple,source_bounds)))
        if sorted(output_intervals) != wanted or comparisons == 0:
            raise ValueError('Incomplete independently verified ocean time coverage or no finite samples')
    return {'files':reports,'compared_samples':comparisons,'compared_masks':masks,
            'raw_sample_comparison':True,'coverage':'PASS','limitations':[],'publication_ready':False}
