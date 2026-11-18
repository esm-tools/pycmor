"""Independent NetCDF/calendar and sampled raw-value checks (no pycmor imports)."""
import json
from pathlib import Path
import sys
from functools import lru_cache
from datetime import timedelta
import cftime
import netCDF4
import numpy as np
import yaml


@lru_cache(maxsize=4)
def native_table(paths):
    import pandas as pd
    frame=pd.concat([pd.read_csv(p,sep=r'\s+') for p in paths],ignore_index=True)
    frame['Lon']=(frame.Lon % 360).round(4)
    frame['Lat']=frame.Lat.round(4)
    keys=['Lon','Lat','Year']+(['Mth'] if 'Mth' in frame else [])
    assert not frame.duplicated(keys).any(), 'Duplicate native LPJG records'
    return frame.set_index(keys)


def sample_lpj(rule,year,month,lon,lat,tile=0):
    frame=native_table(tuple(rule['lr_source_files']))
    key=(round(lon%360,4),round(lat,4),year)
    if 'lr_lpj_lut' in rule['pipelines']:
        value=frame.loc[key+(month,),['psl','pst','crp','urb'][tile]]*rule.get('lr_scale',1)
    elif 'lr_lpj_yearly' in rule['pipelines']:
        value=frame.loc[key,rule['model_variable']]
    else:
        value=frame.loc[key,'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()[month-1]]
    return value


def unit_factor(source, target):
    normalize=lambda s: s.replace(' ','').replace('*','').replace('^','')
    if normalize(source)==normalize(target): return 1.,0.
    if source in ('degC','degree_Celsius','degrees_Celsius') and target=='K': return 1.,273.15
    if source in ('1e-3','0.001') and target in ('0.001','1e-3','psu'): return 1.,0.
    if source in ('1','m2 m-2','mol mol-1') and target in ('1','m2 m-2','mol mol-1'): return 1.,0.
    from cf_units import Unit
    vals=Unit(source).convert(np.array([0.,1.]),Unit(target))
    return float(vals[1]-vals[0]),float(vals[0])


def geographic_vector(u, v, lon, lat, angles):
    """Independent basis-vector rotation; no pycmor rotation code imported."""
    if np.ma.is_masked(u) or np.ma.is_masked(v):
        return np.ma.masked, np.ma.masked
    a,b,g=np.deg2rad(angles)
    def rz(t):
        return np.array([[np.cos(t),-np.sin(t),0.],
                         [np.sin(t),np.cos(t),0.],[0.,0.,1.]])
    rx=np.array([[1.,0.,0.],[0.,np.cos(b),np.sin(b)],[0.,-np.sin(b),np.cos(b)]])
    rotation=rz(-g) @ rx @ rz(-a)
    lon,lat=np.deg2rad([lon,lat])
    position=np.array([np.cos(lat)*np.cos(lon),np.cos(lat)*np.sin(lon),np.sin(lat)])
    rotated=rotation @ position
    rlon=np.arctan2(rotated[1],rotated[0]); rlat=np.arcsin(np.clip(rotated[2],-1.,1.))
    east_r=np.array([-np.sin(rlon),np.cos(rlon),0.])
    north_r=np.array([-np.sin(rlat)*np.cos(rlon),-np.sin(rlat)*np.sin(rlon),np.cos(rlat)])
    vector=rotation.T @ (float(u)*east_r+float(v)*north_r)
    east=np.array([-np.sin(lon),np.cos(lon),0.])
    north=np.array([-np.sin(lat)*np.cos(lon),-np.sin(lat)*np.sin(lon),np.cos(lat)])
    return vector @ east, vector @ north


def regional_native_indices(rule, output):
    """Map subset cells by geography, never by their shortened array index."""
    if rule.get('lr_region') != '30S-90S':
        raise ValueError('Unsupported regional raw-value validation')
    def coordinates(dataset):
        lon = next(dataset[n] for n in ('longitude','lon','nav_lon') if n in dataset.variables)
        lat = next(dataset[n] for n in ('latitude','lat','nav_lat') if n in dataset.variables)
        return np.asarray(lon[:]).ravel(), np.asarray(lat[:]).ravel()
    with netCDF4.Dataset(rule['lr_source_files'][0]) as source:
        slon, slat = coordinates(source)
    olon, olat = coordinates(output)
    assert np.all(olat <= -30.0), 'Out-of-region cells in Southern Ocean product'
    def keys(lon, lat):
        # Coordinate values are commonly stored as float32 natively and
        # float64 after CMORization. Promote before modulo and decimal
        # rounding so identical represented values produce identical keys.
        lon=np.asarray(lon,dtype=np.float64)
        lat=np.asarray(lat,dtype=np.float64)
        return list(zip(np.round(lon % 360,5),np.round(lat,5)))
    source_keys = keys(slon,slat)
    assert len(set(source_keys)) == len(source_keys), 'Nonunique native coordinate match'
    lookup = {key:i for i,key in enumerate(source_keys)}
    indices = np.asarray([lookup[key] for key in keys(olon,olat)],dtype=int)
    expected = np.flatnonzero(slat <= -30.0)
    assert len(set(indices)) == len(indices), 'Duplicated regional output cells'
    assert set(indices) == set(expected), 'Regional product has missing or extra native cells'
    return indices


def sample_nc(rule, year, month, horizontal_indices, level, day=None):
    if rule.get('lr_iceberg_surface', False):
        from iceberg_validation import sample_freshwater
        return sample_freshwater(rule, year, month, horizontal_indices['nod2'])
    paths=rule['lr_source_files']
    def one(files,var,coeff,depth=level):
        p=next(p for p in files if str(year) in Path(p).name)
        with netCDF4.Dataset(p) as ds:
            t=next(ds[n] for n in ('time','time_counter') if n in ds.variables)
            dates=netCDF4.num2date(t[:],t.units,getattr(t,'calendar','standard'))
            matches=[i for i,d in enumerate(dates) if (d.year,d.month)==(year,month)
                     and (day is None or d.day==day)]
            if day is not None and len(matches)!=1:
                raise ValueError(f'Expected one source daily record for {year}-{month}-{day}')
            ti=matches[0]
            v=ds[var]; ix=[]
            for d in v.dimensions:
                if d in ('time','time_counter'): ix.append(ti)
                elif d in ('nz','nz1','nz_sfc','depth','lev','pressure_levels'): ix.append(depth)
                elif d in horizontal_indices: ix.append(horizontal_indices[d])
                else: raise ValueError(f'Unknown source axis {d}')
            return np.ma.asarray(v[tuple(ix)],dtype=float)*coeff
    if rule.get('vector_partner_pattern'):
        own=one(paths,rule['model_variable'],1.)
        other=one(rule['vector_source_files'],rule['vector_partner_variable'],1.)
        u,v=(own,other) if rule['vector_component']=='x' else (other,own)
        with netCDF4.Dataset(rule['grid_file']) as mesh:
            j=horizontal_indices['nod2'];lon=float(mesh['lon'][j]);lat=float(mesh['lat'][j])
        east,north=geographic_vector(u,v,lon,lat,[rule[k] for k in ('alpha_euler','beta_euler','gamma_euler')])
        value=(east if rule['vector_component']=='x' else north)*rule.get('lr_scale',1)+rule.get('lr_offset',0)
    elif rule.get('lr_interface_midpoints'):
        value=(one(paths,rule['model_variable'],1.,level)+one(paths,rule['model_variable'],1.,level+1))*0.5
        value=value*rule.get('lr_scale',1)+rule.get('lr_offset',0)
    elif rule.get('lr_terms'):
        value=sum(one(t['files'],t['variable'],t['coefficient']) for t in rule['lr_terms'])
    else:
        value=one(paths,rule['model_variable'],rule.get('lr_scale',1))+rule.get('lr_offset',0)
    return value


def validate(recipe):
    cfg=yaml.safe_load(Path(recipe).read_text()); rule=cfg['rules'][0]
    out=Path(cfg['inherit']['output_directory']); files=sorted(out.rglob('*.nc'))
    name=rule['compound_name'].split('.')[1]
    expected_frequency=rule['compound_name'].split('.')[-2]
    years=range(cfg['inherit']['year_start'],cfg['inherit']['year_end']+1)
    dates=[]; checks=[]; compared_samples=0; calendars=set()
    is_raw_nc=bool(rule.get('lr_source_files')) and all(p.endswith('.nc') for p in rule['lr_source_files'])
    for path in files:
        with netCDF4.Dataset(path) as ds:
            assert name in ds.variables, f'Missing data variable {name}'
            v=ds[name]
            native_indices=regional_native_indices(rule,ds) if rule.get('lr_region') else None
            assert hasattr(v,'units'), 'Missing units'
            assert ds.getncattr('source_id')=='LR-SPINUP-TEST', 'Wrong experiment identity'
            assert hasattr(ds,'tracking_id'), 'Missing tracking_id'
            if expected_frequency=='fx':
                checks.append({'file':str(path),'check':'fixed field header'});continue
            t=ds['time']; decoded=netCDF4.num2date(t[:],t.units,getattr(t,'calendar','standard'))
            calendars.add(decoded[0].calendar)
            assert hasattr(t,'bounds') and t.bounds in ds.variables, 'Missing time bounds'
            bounds=netCDF4.num2date(ds[t.bounds][:],t.units,getattr(t,'calendar','standard'))
            if rule.get('lr_iceberg_surface', False):
                from iceberg_validation import validate_output
                checks.append(validate_output(rule, ds, v, decoded))
            for d,(lo,hi) in zip(decoded,bounds):
                assert lo<d<hi, 'Time label is not within interval'
                if expected_frequency=='day':
                    dates.append((d.year,d.month,d.day))
                    assert (lo.hour,lo.minute,lo.second)==(0,0,0), 'Daily interval must start at midnight'
                    assert hi-lo==timedelta(days=1), 'Daily interval must be exactly one calendar day'
                else:
                    dates.append((d.year,d.month) if expected_frequency=='mon' else (d.year,0))
                    assert lo.day==1 and hi.day==1, 'Unexpected interval boundary'
            # First/last interval and leap day, several cells, first/middle layer.
            samples={0,len(decoded)-1}
            if expected_frequency=='day':
                samples.update(i for i,d in enumerate(decoded) if d.month==2 and d.day==29)
            for ti in sorted(samples):
                level_values=[0]
                zd=next((d for d in v.dimensions if d in ('olevel','plev19','plev','lev','depth','nz','nz1')),None)
                if zd: level_values.append(len(ds.dimensions[zd])//2)
                for level in sorted(set(level_values)):
                    for fraction in (0.031,0.27,0.51,0.79,0.97):
                        ix=[]; source_indices={}; cell_index=None
                        for d in v.dimensions:
                            if d=='time': ix.append(ti)
                            elif d==zd: ix.append(level)
                            elif d in ('nod2','ncells','cell','landpoints'):
                                j=int(fraction*(len(ds.dimensions[d])-1));ix.append(j)
                                source_indices['nod2']=int(native_indices[j]) if native_indices is not None else j
                                cell_index=j
                            elif d in ('lat','latitude'):
                                j=int(fraction*(len(ds.dimensions[d])-1));ix.append(j);source_indices['lat']=j
                            elif d in ('lon','longitude'):
                                j=int((1-fraction)*(len(ds.dimensions[d])-1));ix.append(j);source_indices['lon']=j
                            elif d in ('landuse','sector','type'): ix.append(0)
                            elif len(ds.dimensions[d])==1: ix.append(0)
                            else: raise ValueError(f'Unknown output axis {d}')
                        value=np.ma.asarray(v[tuple(ix)])
                        if is_raw_nc and not rule.get('lr_bottom'):
                            src=sample_nc(rule,decoded[ti].year,decoded[ti].month,source_indices,0 if rule.get('surface_layer') else level,
                                          day=decoded[ti].day if expected_frequency=='day' else None)
                            factor,offset=unit_factor(rule.get('lr_units',rule.get('expected_units')),v.units)
                            src=src*factor+offset
                            if rule.get('lr_ice_free_zero') and np.ma.is_masked(src):
                                # Cavity masking requires the mesh; separate rule-specific checks.
                                continue
                            assert np.ma.is_masked(src)==np.ma.is_masked(value), f'Mask changed: {path.name}'
                            if not np.ma.is_masked(src):
                                assert np.isclose(src,value,rtol=3e-5,atol=max(1e-30,abs(float(src))*3e-5)), (path.name,float(src),float(value))
                                compared_samples+=1
                        elif rule.get('lr_source_files') and all(p.endswith('.out') for p in rule['lr_source_files']):
                            lon=next(ds[n] for n in ('longitude','lon') if n in ds.variables)[cell_index]
                            lat=next(ds[n] for n in ('latitude','lat') if n in ds.variables)[cell_index]
                            src=sample_lpj(rule,decoded[ti].year,decoded[ti].month,float(lon),float(lat))
                            factor,offset=unit_factor(rule.get('lr_units',rule['source_units']),v.units)
                            src=src*factor+offset
                            assert not np.ma.is_masked(value), 'Native finite LPJG record became missing'
                            assert np.isclose(src,value,rtol=3e-5,atol=max(1e-30,abs(float(src))*3e-5)), (path.name,float(src),float(value))
                            compared_samples+=1
                        if not np.ma.is_masked(value): assert np.isfinite(value), 'Nonfinite value'
            checks.append({'file':str(path),'check':'calendar, identity, headers; sampled values'})
    if expected_frequency=='day':
        assert len(calendars)==1, 'Inconsistent daily output calendars'
        calendar=next(iter(calendars)); expected=[]
        for year in years:
            first=cftime.datetime(year,1,1,calendar=calendar)
            stop=cftime.datetime(year+1,1,1,calendar=calendar)
            expected.extend((d.year,d.month,d.day) for d in
                            (first+timedelta(days=i) for i in range((stop-first).days)))
    else:
        expected=[(y,m) for y in years for m in range(1,13)] if expected_frequency=='mon' else [(y,0) for y in years]
    if expected_frequency!='fx': assert sorted(dates)==expected, f'Missing/duplicate records: {len(dates)}, expected {len(expected)}'
    assert files, 'No output'
    return {'recipe':str(recipe),'files':checks,'coverage':'PASS',
            'raw_sample_comparison':compared_samples>0,'compared_samples':compared_samples,
            'limitations':[] if compared_samples else ['No independent raw-value comparison for this transformation; only headers, calendar and finite samples checked.'],
            'publication_ready':False}


if __name__=='__main__':
    result=validate(sys.argv[1])
    Path(sys.argv[2]).write_text(json.dumps(result,indent=2)+'\n')
    print('PASS',result['recipe'])
