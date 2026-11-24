"""Independent source/mask checks for the requested native land products."""
import calendar
from datetime import date, timedelta
from io import StringIO
from pathlib import Path
import subprocess

import cftime
import netCDF4
import numpy as np
import pandas as pd


def _check(condition, message):
    if not condition:
        raise ValueError(message)


def _grib(rule):
    cmd = [rule['land_grib_program'], '-w', 'shortName=lsm/cl/dl', '-L', '%.8f %.8f',
           '-p', 'shortName', '-F', '%.16g', rule['land_grib_file']]
    text = subprocess.check_output(cmd, text=True)
    vals = {}
    for block in text.split('Latitude')[1:]:
        name = block.splitlines()[1].split()[-1]
        col = np.loadtxt(StringIO(block), skiprows=1, usecols=2)
        if name in vals:
            _check(np.array_equal(vals[name], col), f'Ambiguous GRIB {name}')
        vals[name] = col
    land = vals['lsm'] > .5
    resolved = (~land) & (vals['cl'] > .5)
    lake = np.zeros(len(land))
    lake[land] = vals['cl'][land]
    lake[resolved] = 1.
    return land|resolved, lake, vals['dl']


def _check_field(actual, expected, abs_tol=1.e-16):
    field = np.asarray(np.ma.filled(np.ma.asarray(actual, dtype=float),np.nan))
    _check(field.shape == expected.shape, 'Output field shape differs from source')
    valid = np.isfinite(expected)
    _check(np.array_equal(np.isfinite(field), valid), 'Output land/water mask differs from source')
    _check(np.allclose(field[valid], expected[valid], rtol=2.e-5, atol=abs_tol),
           'Native land value comparison failed')
    return int(valid.sum())


def _lpj_samples(rule, active, points, days, daily):
    # Independent lookup by text coordinates from the original native files.
    # Match only the selected cells, without calling the converter or constructing
    # a whole year by full-grid matrix.
    coords = {tuple(np.round(active[key, :2], 4)): k for k,key in enumerate(points)}
    samples = []
    for path in rule['lr_source_files']:
        values = {}
        use = ['Lon','Lat','Year','Total'] + (['Day'] if daily else [])
        for chunk in pd.read_csv(path, sep=r'\s+', usecols=use, chunksize=250000):
            chosen = chunk[chunk.Year==rule['lr_land_year']]
            if daily:
                chosen = chosen[chosen.Day.isin(days)]
            # Gridlist coordinates are printed to five decimals, output to six.
            # Do not require their rounded keys to coincide at a tie.
            for key, j in coords.items():
                hit = chosen[(abs(chosen.Lat-key[1]) < .00006)
                             & (abs((chosen.Lon-key[0]+180)%360-180) < .00006)]
                for record in hit.itertuples():
                    label = (record.Day if daily else 1, j)
                    _check(label not in values, 'Duplicate sampled LPJG record')
                    values[label] = record.Total
        _check(len(values)==len(points)*len(days), f'Incomplete native comparison in {path}')
        samples.append(values)
    expected = {}
    for key,v in samples[0].items():
        expected[key] = v - sum(row[key] for row in samples[1:])
    return expected


def validate(cfg, rule, files):
    name = rule['model_variable']
    with netCDF4.Dataset(rule['grid_file']) as ref:
        lat,lon = (np.asarray(ref[k][:]) for k in ('lat','lon'))
    frequency = rule['compound_name'].split('.')[-2]
    year = rule['lr_land_year']
    start = cftime.DatetimeProlepticGregorian(year,1,1)
    stop = cftime.DatetimeProlepticGregorian(year+1,1,1)
    if frequency=='day':
        edges=[start+timedelta(days=j) for j in range((stop-start).days+1)]
    elif frequency=='mon':
        edges=[cftime.DatetimeProlepticGregorian(year,m,1) for m in range(1,13)]+[stop]
    else:
        edges=[start,stop]
    stamps = lambda dates: [(d.year,d.month,d.day,d.hour,d.minute,d.second) for d in dates]
    expected_bounds = list(zip(stamps(edges[:-1]),stamps(edges[1:])))
    check_rows,seen,compared=[],[],0
    lpj = 'land_gridlist_file' in rule
    if lpj:
        gridlist = np.loadtxt(rule['land_gridlist_file'],ndmin=2)
        active = gridlist[(gridlist[:,2]>0)&(gridlist[:,2]<=7)&(gridlist[:,1]>-60)]
        ix = active[:,3].astype(int)-1
        # Fixed index and latitude coverage, including the northern/southern margins.
        points=np.unique(np.linspace(0,len(active)-1,min(12,len(active)),dtype=int))
        days = [1] if frequency=='yr' else sorted(set([1,(stop-start).days//2,(stop-start).days]))
        observed = _lpj_samples(rule,active,points,days,frequency=='day')
        omitted = np.setdiff1d(np.arange(len(lat)), ix)
        missing = np.zeros(len(lat),dtype=bool)
        if name == 'evspsblsoi':
            missing[np.setdiff1d(gridlist[:,3].astype(int)-1,ix)] = True
    elif name != 'lithk':
        land,lake,depth = _grib(rule)
    for path in files:
        with netCDF4.Dataset(path) as ds:
            _check(ds.source_id==cfg['inherit']['source_id'] and bool(ds.tracking_id), 'Invalid output identity')
            outlat = np.asarray(ds['latitude'][:])
            outlon = np.asarray(ds['longitude'][:])
            _check(outlat.shape==lat.shape and np.max(np.abs(outlat-lat)) < .0001
                   and np.max(np.abs((outlon-lon+180)%360-180)) < .0001, 'Output grid changed')
            v = ds[name]
            _check(v.units == rule['source_units'], f'Unexpected output units: {v.units}')
            if frequency=='fx':
                _check('time' not in v.dimensions, 'Fixed product acquired a time dimension')
                if name=='lithk':
                    expected=np.zeros(len(lat))
                elif name=='depthl':
                    expected=np.where(lake>0,depth,np.nan)
                else:
                    expected=100*lake
                    actual=np.ma.asarray(v[:])
                    _check(np.all((actual >= 0) & (actual <= 100)), 'Lake fraction outside [0, 100]%')
                    ends=(expected == 0)|(expected == 100)
                    _check(np.array_equal(actual[ends],expected[ends]), 'Lake fraction endpoints changed')
                compared += _check_field(v[:],expected)
            else:
                t=ds['time']
                b=ds[t.bounds]
                starts=netCDF4.num2date(b[:,0],t.units,t.calendar)
                ends=netCDF4.num2date(b[:,1],t.units,t.calendar)
                _check(np.allclose(t[:],(b[:,0]+b[:,1])/2,rtol=0,atol=1.e-8),'Time midpoint mismatch')
                bounds=list(zip(stamps(starts),stamps(ends)))
                seen.extend(bounds)
                if lpj:
                    for i,lo in enumerate(starts):
                        day = (date(lo.year,lo.month,lo.day)-date(year,1,1)).days+1 if frequency=='day' else 1
                        if day not in days:
                            continue
                        values=np.ma.filled(v[i],np.nan).astype(float)
                        _check(np.array_equal(~np.isfinite(values),missing),'LPJG output cell coverage changed')
                        _check(np.all(values[omitted[~missing[omitted]]]==0.),'Missing zero contribution outside LPJG')
                        expected=np.array([observed[(day,j)] for j in range(len(points))])
                        compared += _check_field(values[ix[points]],expected,
                                                 1.51e-12 if name=='evspsblsoi' else 1.e-16)
                else:
                    with netCDF4.Dataset(rule['lr_source_files'][0]) as source:
                        raw=source[name];tb=source['time_counter']
                        for i in sorted({0,len(starts)//2,len(starts)-1}):
                            rawbounds=source[tb.bounds]
                            limits=netCDF4.date2num([starts[i],ends[i]],tb.units,tb.calendar)
                            indices=np.flatnonzero((rawbounds[:,0]>=limits[0]) & (rawbounds[:,1]<=limits[1]))
                            _check(len(indices)==(8 if name=='mrros' else calendar.monthrange(year,starts[i].month)[1]*(24 if name=='rsds' else 1)),
                                   'Incomplete raw comparison window')
                            widths=np.diff(rawbounds[indices],axis=1).ravel()
                            vals=np.ma.filled(raw[indices],np.nan).astype(float)
                            expected=np.where(land,np.average(vals,axis=0,weights=widths),np.nan)
                            compared += _check_field(v[i],expected)
            check_rows.append(dict(file=str(path),check='Native product values, model footprint, identity, horizontal grid and interval'))
    _check(len(files)>0 and compared>0,'Missing requested-land output or independent comparisons')
    if frequency=='fx':
        _check(len(files)==1,'Expected exactly one fixed field')
    else:
        _check(sorted(seen)==expected_bounds,'Missing/duplicate calendar output intervals')
    return dict(files=check_rows,coverage='PASS',raw_sample_comparison=True,
                compared_samples=compared,limitations=[],publication_ready=False)
