"""Native TCo95 land products requested for the LR piControl output profile."""
import calendar
import copy
from datetime import timedelta
from functools import lru_cache
from io import StringIO
from pathlib import Path
import re
import shutil
import subprocess

import cftime
import netCDF4
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
import xarray as xr


PRODUCTS = {
    'depthl': ('land.depthl.ti-u-hxy-u.fx.glb', 'm'),
    'sftlaf': ('land.sftlaf.ti-u-hxy-u.fx.glb', '%'),
    'lithk': ('landIce.lithk.ti-u-hxy-u.fx.glb', 'm'),
    'mrros': ('land.mrros.tavg-u-hxy-lnd.day.glb', 'kg m-2 s-1'),
    'rsds': ('land.rsds.tavg-u-hxy-lnd.mon.glb', 'W m-2'),
    'snm': ('landIce.snm.tavg-u-hxy-lnd.mon.glb', 'kg m-2 s-1'),
    'vegFrac': ('land.vegFrac.tavg-u-hxy-u.yr.glb', '%'),
    'evspsblveg': ('land.evspsblveg.tavg-u-hxy-u.day.glb', 'kg m-2 s-1'),
    'evspsblsoi': ('land.evspsblsoi.tavg-u-hxy-u.day.glb', 'kg m-2 s-1'),
}
LPJG = {
    'vegFrac': ('vegFrac_yearly.out',),
    'evspsblveg': ('ec_daily.out',),
    'evspsblsoi': ('evspsbl_daily.out', 'tran_daily.out', 'ec_daily.out'),
}
GROUP_COVER = ('treeFrac', 'shrubFrac', 'grassFrac', 'pastureFrac', 'cropFrac')
XIOS = {'snm': 'atmos_day_land_snm', 'mrros': 'atmos_3h_land_mrros', 'rsds': 'atmos_1h_sfc_rsds'}
STEPS_PER_DAY = {'mrros': 8, 'snm': 1, 'rsds': 24}
COMMENTS = {
    'lithk': 'Fixed global zero convention for an ice sheet represented by prescribed orography and mask (CMIP7 DReq).',
    'depthl': 'OpenIFS FLake total depth from initial GRIB dl, averaged over lake area. Missing where the active lake tile has zero area.',
    'sftlaf': 'OpenIFS FLake tile fraction of the entire native grid cell. With FLake active, lake area equals cl for land points, 100% for resolved lake points and zero elsewhere.',
    'mrros': 'Daily duration-weighted mean of the native OpenIFS 3-hourly surface runoff flux; masked outside model land and resolved lakes.',
    'rsds': 'Monthly mean of hourly OpenIFS incident surface shortwave radiation (ssrd/3600) at model land points, including resolved inland lakes. Incident radiation is diagnosed per atmospheric column, before partitioning net absorbed radiation among surface tiles.',
    'snm': 'Monthly duration-weighted mean of daily OpenIFS surface snowmelt. Snow-free model land retains its zero melt; ocean cells are missing.',
    'evspsblveg': 'LPJ-GUESS daily canopy interception evaporation on its binary OASIS whole-cell support. Canopy cover and its flux are zero where the coupled vegetation model has no vegetation.',
    'evspsblsoi': 'LPJ-GUESS soil evaporation from daily total evaporation minus transpiration and interception evaporation. Values describe LPJ-GUESS hydrology. Unsimulated land is missing and open ocean contributes zero.',
    'vegFrac': 'LPJ-GUESS stand-area weighted annual total foliaged projective cover, including moss as well as tree, shrub, grass, pasture and crop. The model uses the full horizontal cell area on its binary OASIS land grid and sets vegetation to zero outside its modelled domain.',
}


def rules():
    for name, (compound, units) in PRODUCTS.items():
        fixed = compound.split('.')[-2] == 'fx'
        rule = dict(name=name, model_variable=name, compound_name=compound, source_units=units,
                   lr_units=units, lr_group='requested_land', grid_label='g235',
                   grid='OpenIFS native reduced Gaussian TCo95 land grid (40320 cells)',
                   time_dimname='time', comment=COMMENTS[name],
                   pipelines=['lr_requested_land_fx' if fixed else 'lr_requested_land'])
        if name == 'sftlaf':
            # BitGroom can round a fully lake-covered cell above 100%.
            rule['netcdf_quantize_mode'] = None
        yield rule


def install_pipelines(profile, reproduce):
    for original, name in [('lr_lpj_yearly', 'lr_requested_land'), ('lr_fx', 'lr_requested_land_fx')]:
        pipeline = copy.deepcopy(next(p for p in profile['pipelines'] if p['name'] == original))
        pipeline['name'] = name
        pipeline['steps'][0] = f'script://{Path(reproduce) / "requested_land.py"}:load'
        profile['pipelines'].append(pipeline)


def _check_file(path):
    path = Path(path)
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError(f'MISSING_INPUT: {path}')
    return str(path.resolve())


@lru_cache(maxsize=32)
def _run_flag(path, name):
    # These two settings are reported in the log's startup namelist, before time stepping.
    with open(path, errors='replace') as stream:
        for index, line in enumerate(stream):
            if match := re.search(rf'\b{name}\s*=\s*\.?([TF])(?:\.|[\s,])', line, re.I):
                return match.group(1).upper() == 'T'
            if index >= 4000:
                break
    raise ValueError(f'Missing {name} in the archived OpenIFS startup namelist: {path}')


def materialize(original, raw, grids, full_grid, config):
    rule = dict(original)
    name = rule['model_variable']
    year, = raw.YEARS
    work = raw.EXP / f'run_{raw.PERIOD}' / 'work'
    grid = _check_file(full_grid)
    rule.update(grid_file=grid, bounds_reference_file=grid, lr_land_year=year)
    aux = [grid]
    if name in LPJG:
        rule['land_gridlist_file'] = _check_file(work / 'ece_gridlist_TCO95.txt')
        aux.append(rule['land_gridlist_file'])
        paths = [_check_file(raw.EXP / 'outdata/lpj_guess' / raw.PERIOD / fn) for fn in LPJG[name]]
        if name == 'vegFrac':
            tables = [_check_file(Path(paths[0]).with_name(group+'_yearly.out')) for group in GROUP_COVER]
            fpc = _check_file(Path(paths[0]).with_name('fpc.out'))
            discrepancy = _annual_closure(paths[0], tuple(tables), fpc).get(year)
            if discrepancy is None:
                raise ValueError(f'No annual native vegetation partitions for {year}')
            if discrepancy > 0.056:
                raise ValueError(f'UNRESOLVED_VEGETATION_CLOSURE: annual total and independently archived plant groups plus area-weighted peat moss differ by {discrepancy:.4f} percentage points in {year}')
            aux.extend([*tables, fpc])
            rule.update(land_cover_group_files=tables, land_moss_file=fpc)
    else:
        log = _check_file(raw.EXP / 'log/oifs' / f'NODE.001_01_{raw.PERIOD}')
        aux.append(log)
        if name == 'lithk':
            if config.get('land_ice_representation') != 'orography_and_mask' or _run_flag(log, 'ECE_LANDICE'):
                raise ValueError('UNREVIEWED_ICE_SHEET: fixed global zero requires a prescribed orography and mask representation without ice-sheet coupling')
            paths = [grid]
        else:
            if not _run_flag(log, 'LEFLAKE'):
                raise ValueError('This native land/lake mask requires OpenIFS LEFLAKE=T')
            program = shutil.which(config.get('grib_get_data', 'grib_get_data'))
            if not program:
                raise ValueError('Missing ecCodes grib_get_data; load ecCodes or set grib_get_data in the campaign config')
            rule.update(land_grib_program=_check_file(program), land_grib_file=_check_file(work / 'ICMGGawi3INIT'))
            aux.extend([rule['land_grib_program'], rule['land_grib_file']])
            lat, lon = _grid(grid)
            _geography(rule['land_grib_program'], rule['land_grib_file'], grid)
            if name in XIOS:
                paths = [_check_file(raw.EXP / 'outdata/oifs' / f'{XIOS[name]}_{year}-{year}.nc')]
                with netCDF4.Dataset(paths[0]) as ds:
                    _check_oifs_stream(ds, name, year, lat, lon)
            else:
                paths = [grid]
    rule['lr_auxiliary_files'] = sorted(set(aux))
    rule['lr_source_files'] = paths
    rule['inputs'] = [dict(path=str(Path(paths[0]).parent), pattern=re.escape(Path(paths[0]).name)+'$')]
    return 'requested_land', rule



@lru_cache(maxsize=8)
def _annual_closure(total_file, group_files, fpc_file):
    # Annual commonoutput.pmoss is an active-stand mean; PEAT_STANDFRAC
    # converts it to whole-cell support. Commonoutput has a coarser precision
    # than CMIPoutput (0.001 FPC and 0.0001 stand area fraction).
    keys = ['Lon', 'Lat', 'Year']
    def annual(path, columns):
        frame = pd.read_csv(path, sep=r'\s+', usecols=keys+columns)
        matrix = frame.set_index(keys, verify_integrity=True).sort_index()
        if not np.isfinite(matrix.to_numpy()).all():
            raise ValueError(f'Missing annual plant fraction in {path}')
        return matrix
    total = annual(total_file,['Total']).Total
    parts = [annual(path,['Total']).Total for path in group_files]
    moss = annual(fpc_file,['pmoss','PEAT_STANDFRAC'])
    if not all(total.index.equals(other.index) for other in [*parts,moss]):
        raise ValueError('Annual plant fractions have inconsistent locations or years')
    if ((total<0)|(total>100)).any() or any(((c<0)|(c>100)).any() for c in parts):
        raise ValueError('Annual vegetation fraction outside [0,100]')
    difference = (total-sum(parts)-100*moss.pmoss*moss.PEAT_STANDFRAC).abs()
    return difference.groupby(level='Year').max().to_dict()


def _grid(path):
    with netCDF4.Dataset(path) as ds:
        return np.asarray(ds['lat'][:]).ravel(), np.asarray(ds['lon'][:]).ravel()


def _same_grid(a_lat, a_lon, b_lat, b_lon):
    return (a_lat.shape == b_lat.shape and a_lon.shape == b_lon.shape
            and np.all(np.abs(a_lat-b_lat) <= 1e-4)
            and np.all(np.abs((a_lon-b_lon+180)%360-180) <= 1e-4))


@lru_cache(maxsize=8)
def _geography(program, grib, grid):
    lat, lon = _grid(grid)
    cmd = [program, '-w', 'shortName=cl/dl/lsm', '-p', 'shortName', '-F', '%.16g',
           '-L', '%.8f %.8f', '-m', 'nan', grib]
    result = subprocess.run(cmd, text=True, capture_output=True)
    if result.returncode != 0:
        raise ValueError(f'ecCodes could not decode land geometry: {result.stderr.strip()}')
    fields = {}
    for block in result.stdout.split('Latitude')[1:]:
        lines = block.splitlines()
        name = lines[1].split()[-1]
        table = np.loadtxt(StringIO(block), skiprows=1, usecols=[0, 1, 2], ndmin=2)
        if not _same_grid(table[:, 0], table[:, 1], lat, lon):
            raise ValueError(f'GRIB {name} coordinates/order differ from the native reporting grid')
        if name in fields and not np.array_equal(fields[name], table[:, 2], equal_nan=True):
            raise ValueError(f'The GRIB has conflicting {name} messages')
        fields[name] = table[:, 2]
    if set(fields) != {'lsm', 'cl', 'dl'}:
        raise ValueError(f'Missing GRIB lake geography; found {sorted(fields)}')
    lake, land = lake_geometry(fields['lsm'], fields['cl'])
    depth = fields['dl']
    if not np.all(np.isfinite(depth[lake > 0]) & (depth[lake > 0] > 0)):
        raise ValueError('Missing/nonpositive model depth at a represented lake')
    return land, lake, depth


def lake_geometry(lsm, cl):
    """Implements surfbc_ctl_mod (LDLAND, LDLAKE and ZPCVLK), with LEFLAKE true."""
    for fractions in (lsm, cl):
        if not np.all(np.isfinite(fractions) & (fractions >= 0) & (fractions <= 1)):
            raise ValueError('Invalid GRIB land/lake fraction')
    lake = np.where(lsm > .5, cl, (cl > .5).astype(float))
    return lake, (lsm > .5) | (lake > 0)


def _bounds(year, frequency):
    start = cftime.DatetimeProlepticGregorian(year, 1, 1)
    stop = cftime.DatetimeProlepticGregorian(year + 1, 1, 1)
    if frequency == 'yr':
        return [(start, stop)]
    if frequency == 'mon':
        starts = [cftime.DatetimeProlepticGregorian(year, m, 1) for m in range(1, 13)] + [stop]
    else:
        days = (stop-start).days
        starts = [start+timedelta(days=i) for i in range(days+1)]
    return list(zip(starts, starts[1:]))


def _check_oifs_stream(ds, name, year, lat, lon):
    var = ds[name]
    if (var.dimensions != ('time_counter', 'cell') or var.units != PRODUCTS[name][1]
            or getattr(var, 'online_operation', '') != 'average'
            or not _same_grid(ds['lat'][:], ds['lon'][:], lat, lon)):
        raise ValueError(f'Unexpected native {name} source grid, units or reduction')
    t = ds['time_counter']
    bounds = ds[t.bounds][:]
    units, cal = t.units, getattr(t, 'calendar', 'standard')
    if cal not in ('standard', 'gregorian', 'proleptic_gregorian'):
        raise ValueError(f'Unsupported OpenIFS calendar {cal}')
    steps = STEPS_PER_DAY[name]
    first, last = netCDF4.date2num([cftime.datetime(year,1,1,calendar=cal),
                                     cftime.datetime(year+1,1,1,calendar=cal)], units, cal)
    n = (365+calendar.isleap(year))*steps
    edges = np.linspace(first, last, n+1)
    if (bounds.shape != (n,2) or not np.allclose(bounds[:,0], edges[:-1], rtol=0, atol=1e-6)
            or not np.allclose(bounds[:,1], edges[1:], rtol=0, atol=1e-6)
            or not np.allclose(t[:], (edges[:-1]+edges[1:])*.5, rtol=0, atol=1e-6)):
        raise ValueError(f'Incomplete or uncentred native averaging intervals: {name} {year}')


def _lpj_domain(rule, lat, lon):
    locations = np.loadtxt(rule.get('land_gridlist_file'), ndmin=2)
    ids = locations[:,3].astype(int)-1
    if (not np.array_equal(ids+1, locations[:,3]) or np.any(ids<0) or np.any(ids>=len(lat))
            or len(np.unique(ids)) != len(ids)
            or not _same_grid(locations[:,1],locations[:,0],lat[ids],lon[ids])):
        raise ValueError('Native LPJG gridlist indices/coordinates disagree with full TCo95 geometry')
    active = (locations[:,2] > 0) & (locations[:,2] <= 7) & (locations[:,1] > -60)
    return ids, ids[active]


def _xyz(lat, lon):
    a,b = np.deg2rad(lat),np.deg2rad(lon)
    return np.column_stack([np.cos(a)*np.cos(b), np.cos(a)*np.sin(b), np.sin(a)])


def _read_lpj_file(path, year, days, tree, ncell):
    daily = days > 1
    columns = ['Year','Lon','Lat','Total'] + (['Day'] if daily else [])
    out = np.empty((days,ncell), dtype=float)
    seen = np.zeros(days*ncell, dtype=bool)
    for chunk in pd.read_csv(path, sep=r'\s+', usecols=columns, chunksize=250000):
        frame = chunk[chunk.Year == year]
        if frame.empty:
            continue
        inv, positions = pd.MultiIndex.from_frame(frame[['Lat','Lon']]).factorize()
        distance, idx = tree.query(_xyz(positions.get_level_values(0).to_numpy(),
                                        positions.get_level_values(1).to_numpy()))
        if np.any(distance > np.deg2rad(6e-5)) or len(np.unique(idx)) != len(idx):
            raise ValueError(f'Unexpected or ambiguous native LPJG cell in {path}')
        if daily:
            jday = frame.Day.to_numpy()
            if not np.all((jday >= 1) & (jday <= days) & (jday == jday.astype(int))):
                raise ValueError(f'Invalid LPJG day of year: {path}')
            flat = (jday.astype(int)-1)*ncell + idx[inv]
        else:
            flat = idx[inv]
        if seen[flat].any() or len(np.unique(flat)) != len(flat):
            raise ValueError(f'Duplicate native cell/time record: {path}')
        values = frame.Total.to_numpy()
        if not np.all(np.isfinite(values) & (values != -999) & (values != -9999)):
            raise ValueError(f'Invalid native LPJG value: {path}')
        seen[flat] = True
        out.ravel()[flat] = values
    if not np.all(seen):
        raise ValueError(f'Missing native LPJG cells/time records for {year}: {path}')
    return out


def _lpj_values(rule, name, year, lat, lon):
    ids, active = _lpj_domain(rule, lat, lon)
    ndays = 1 if name=='vegFrac' else 365+calendar.isleap(year)
    tree = cKDTree(_xyz(lat[active], lon[active]))
    paths = rule.get('lr_source_files')
    component = _read_lpj_file(paths[0], year, ndays, tree, len(active))
    for path in paths[1:]:
        component -= _read_lpj_file(path, year, ndays, tree, len(active))
    if name == 'vegFrac' and np.any((component < 0)|(component > 100)):
        raise ValueError('LPJG annual vegetation percentage outside [0,100]')
    if name == 'evspsblsoi':
        # Three native columns are printed to twelve decimal places.
        if (component < -1.51e-12).any():
            raise ValueError('Canopy and transpiration exceed native LPJG total evaporation')
        component = np.maximum(0., component)
    result = np.zeros((ndays, len(lat)))
    if name == 'evspsblsoi':
        # Soil evaporation was not computed by LPJG on excluded land cells.
        result[:,ids] = np.nan
    result[:,active] = component
    return result


def load(data, rule):
    name, year = rule.model_variable, rule.get('lr_land_year')
    lat, lon = _grid(rule.grid_file)
    frequency = rule.compound_name.split('.')[-2]
    if name in LPJG:
        values = _lpj_values(rule,name,year,lat,lon)
    elif name == 'lithk':
        values = np.zeros_like(lat, dtype=float)
    else:
        land, lake, depth = _geography(rule.get('land_grib_program'),rule.get('land_grib_file'),rule.grid_file)
        if name == 'depthl':
            values = np.where(lake>0,depth,np.nan)
        elif name == 'sftlaf':
            values = 100.*lake
        else:
            bounds = _bounds(year,frequency)
            values = np.empty((len(bounds),len(lat)))
            with netCDF4.Dataset(rule.get('lr_source_files')[0]) as ds:
                _check_oifs_stream(ds,name,year,lat,lon)
                start = 0
                for i,(lo,hi) in enumerate(bounds):
                    stop = start+(hi-lo).days*STEPS_PER_DAY[name]
                    field = np.ma.filled(ds[name][start:stop],np.nan).astype(float)
                    if not np.isfinite(field[:,land]).all():
                        raise ValueError(f'Incomplete land flux for {lo}: {name}')
                    values[i] = np.where(land,field.mean(axis=0),np.nan)
                    start = stop
    coords={'lat': ('ncells',lat), 'lon': ('ncells',lon)}
    dims = ('ncells',)
    if frequency != 'fx':
        bounds = _bounds(year,frequency)
        coords['time'] = [a+(b-a)/2 for a,b in bounds]
        dims = ('time','ncells')
    result = xr.Dataset({name: (dims,values)}, coords=coords)
    result[name].attrs.update(units=rule.source_units,comment=COMMENTS[name])
    result.lat.attrs.update(units='degrees_north',standard_name='latitude')
    result.lon.attrs.update(units='degrees_east',standard_name='longitude')
    if frequency != 'fx':
        result['time_bnds'] = (('time','bnds'),bounds)
        result.time.attrs['bounds'] = 'time_bnds'
        for key in ('time','time_bnds'):
            result[key].encoding.update(units='days since 1850-01-01',calendar='proleptic_gregorian')
        rule.time_method = 'mean'
    return result
