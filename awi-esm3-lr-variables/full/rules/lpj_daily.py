"""Read native LPJG Year/Day tables without using damaged remapped dates."""
import calendar
from datetime import timedelta
from pathlib import Path

import cftime
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
import xarray as xr


def _xyz(lon, lat):
    lon, lat = np.deg2rad(lon), np.deg2rad(lat)
    return np.column_stack([np.cos(lat)*np.cos(lon), np.cos(lat)*np.sin(lon), np.sin(lat)])


def load_native_daily(data, rule, _paths=None, _liquid=False):
    paths = _paths or rule.get('lr_source_files')
    if not paths:
        raise ValueError('Native daily LPJG reader requires pinned source files')
    frames = []
    for path in paths:
        for chunk in pd.read_csv(path, sep=r'\s+', chunksize=100000):
            selected = chunk[chunk.Year.between(rule.year_start, rule.year_end)]
            if len(selected):
                frames.append(selected)
    if not frames:
        raise ValueError('Native daily LPJG table has no requested year')
    frame = pd.concat(frames, ignore_index=True)
    columns = [c for c in frame.columns if c.startswith('Depth')]
    layered=bool(columns)
    if not layered:columns=[rule.get('lr_lpj_value_column','Total')]
    if not set(columns+['Lon','Lat','Year','Day']) <= set(frame.columns):
        raise ValueError('Expected native Year/Day and diagnostic columns')
    if layered:
        bottoms = np.asarray([float(c[5:]) for c in columns])
        width = float(rule.get('lr_soil_layer_thickness_m', 0.0))
        if width <= 0 or not np.allclose(bottoms, width*np.arange(1,len(columns)+1), atol=1e-8, rtol=0):
            raise ValueError('Soil layers disagree with the explicitly reviewed model discretization')
    with xr.open_dataset(rule.grid_file, decode_times=False) as grid:
        lon = np.asarray(grid.lon.values, dtype=float)
        lat = np.asarray(grid.lat.values, dtype=float)
    if lon.ndim != 1 or lon.shape != lat.shape:
        raise ValueError('Expected the actual native LPJG cell grid')
    tree=cKDTree(_xyz(lon,lat))
    reference=rule.get('lr_lpj_footprint_files')
    if reference:
        footprints=[]
        for path in reference:
            ref=pd.read_csv(path,sep=r'\s+',usecols=['Lon','Lat','Year'])
            ref=ref[ref.Year.between(rule.year_start,rule.year_end)]
            footprints.append(ref)
        ref=pd.concat(footprints,ignore_index=True)[['Lon','Lat']].drop_duplicates()
        distances,indices=tree.query(_xyz(ref.Lon.to_numpy(),ref.Lat.to_numpy()))
        if distances.max()>np.deg2rad(1e-5) or len(set(indices))!=len(indices):
            raise ValueError('Reference monthly LPJG footprint does not match archived geometry')
        indices=np.sort(indices)
        lon,lat=lon[indices],lat[indices]
        tree=cKDTree(_xyz(lon,lat))
    distance, cells = tree.query(_xyz(frame.Lon.to_numpy(),frame.Lat.to_numpy()))
    # Native output and archived gridlist use different decimal precision.
    # This is a strict sub-metre text-precision match, not spatial remapping.
    if distance.max() > np.deg2rad(1e-5):
        raise ValueError('Native LPJG coordinates do not match the frozen grid')
    offsets, dates = {}, []
    for year in range(rule.year_start, rule.year_end+1):
        offsets[year] = len(dates)
        start = cftime.DatetimeProlepticGregorian(year,1,1)
        dates.extend(start+timedelta(days=i,hours=12) for i in range(365+calendar.isleap(year)))
    years, days = frame.Year.to_numpy(), frame.Day.to_numpy()
    if not np.equal(years,np.floor(years)).all() or not np.equal(days,np.floor(days)).all():
        raise ValueError('Non-integer native Year/Day')
    limits = np.asarray([365+calendar.isleap(int(y)) for y in years])
    if not ((days>=1)&(days<=limits)).all():
        raise ValueError('Invalid native day-of-year, including leap-year boundary')
    times = np.asarray([offsets[int(y)] for y in years])+days.astype(int)-1
    flat = times*len(lon)+cells
    if len(np.unique(flat)) != len(flat):
        raise ValueError('Duplicate native cell/day records')
    if len(flat) != len(dates)*len(lon):
        raise ValueError('Missing native daily records or cells')
    values = np.full((len(dates),len(columns),len(lon)), np.nan, dtype=float)
    raw = frame[columns].to_numpy(dtype=float)
    if not np.isfinite(raw).all():
        raise ValueError('Nonfinite native soil diagnostic; investigate source rather than fill')
    if np.isin(raw, rule.get('lr_native_missing_values', [-9999., -999.])).any():
        raise ValueError('Native missing-value sentinel in a simulated cell; investigate source')
    values[times,:,cells] = raw
    variable = rule.model_variable
    coords={'time':dates,'lon':('ncells',lon),'lat':('ncells',lat)}
    if layered:
        coords['depth']=bottoms-width/2
        result = xr.Dataset({variable:(('time','depth','ncells'),values),
            'depth_bnds':(('depth','bnds'),np.column_stack([bottoms-width,bottoms]))},coords=coords)
        result.depth.attrs.update(standard_name='depth',long_name='Depth below land surface',
            units='m',positive='down',axis='Z',bounds='depth_bnds')
    else:
        result=xr.Dataset({variable:(('time','ncells'),values[:,0,:])},coords=coords)
    result[variable].attrs.update(units=rule.source_units,
        cell_methods='time: mean',
        comment='Native LPJG daily diagnostic; Gregorian dates reconstructed from original Year/Day records, including leap days. Soil-layer labels, when present, denote layer bottoms, not centres.')
    if rule.get('lr_soil_liquid_source_files') and not _liquid:
        liquid=load_native_daily(None,rule,_paths=rule.get('lr_soil_liquid_source_files'),_liquid=True)
        total,water=xr.align(result[variable],liquid[variable],join='exact')
        ice=total-water
        if bool((ice<0).any()):
            raise ValueError('Liquid soil water exceeds total water; cannot derive frozen soil water')
        attrs=dict(result[variable].attrs)
        result[variable]=ice
        result[variable].attrs=attrs
        result[variable].attrs['comment']='Frozen soil water = native total soil water minus native liquid soil water, including below-wilting-point liquid water. Identical cell/day/layer coordinates are required.'
    if rule.get('lr_native_monthly_mean') and not _liquid:
        bounds=result.get('depth_bnds')
        mean=result[variable].resample(time='MS').mean(keep_attrs=True)
        labels=[cftime.DatetimeProlepticGregorian(d.year,d.month,1)+timedelta(days=calendar.monthrange(d.year,d.month)[1]/2)
                for d in mean.time.values]
        result=mean.assign_coords(time=labels).to_dataset()
        if bounds is not None:result['depth_bnds']=bounds
    result.lat.attrs.update(standard_name='latitude',units='degrees_north')
    result.lon.attrs.update(standard_name='longitude',units='degrees_east')
    result.time.encoding.update(units='days since 1850-01-01',calendar='proleptic_gregorian')
    return result
