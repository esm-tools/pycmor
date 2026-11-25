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


@lru_cache(maxsize=2)
def native_daily_table(paths, year):
    """Independent annual raw lookup; do not reuse the conversion reader."""
    import pandas as pd
    chunks=[]
    for path in paths:
        for chunk in pd.read_csv(path,sep=r'\s+',chunksize=100000):
            selected=chunk[chunk.Year==year]
            if len(selected): chunks.append(selected)
    if not chunks: raise ValueError('No native daily records for independent comparison')
    frame=pd.concat(chunks,ignore_index=True)
    coordinates=frame[['Lon','Lat']].drop_duplicates()
    coordinates['lon_key']=(coordinates.Lon%360).round(3)
    coordinates['lat_key']=coordinates.Lat.round(3)
    assert not coordinates.duplicated(['lon_key','lat_key']).any(), 'Ambiguous native coordinate lookup'
    frame['lon_key']=(frame.Lon%360).round(3)
    frame['lat_key']=frame.Lat.round(3)
    keys=['lon_key','lat_key','Day']
    assert not frame.duplicated(keys).any(), 'Duplicate native daily records'
    return frame.set_index(keys)


def sample_lpj(rule,year,month,lon,lat,tile=0,day=None,level=0):
    if rule.get('lr_lpj_daily'):
        start=cftime.DatetimeProlepticGregorian(year,1,1)
        if rule.get('lr_native_monthly_mean'):
            first=cftime.DatetimeProlepticGregorian(year,month,1)
            stop=cftime.DatetimeProlepticGregorian(year+(month==12),month%12+1,1)
            ordinals=list(range((first-start).days+1,(stop-start).days+1))
        else:
            ordinals=[(cftime.DatetimeProlepticGregorian(year,month,day)-start).days+1]
        def source_values(paths):
            frame=native_daily_table(tuple(paths),year)
            columns=[c for c in frame.columns if c.startswith('Depth')] or [rule.get('lr_lpj_value_column','Total')]
            coordinates=frame[['Lon','Lat']].drop_duplicates()
            delta=(coordinates.Lon.to_numpy()-lon+180)%360-180
            distance=np.hypot(delta*np.cos(np.deg2rad(lat)),coordinates.Lat.to_numpy()-lat)
            nearest=coordinates.iloc[int(np.argmin(distance))]
            assert float(distance.min())<1e-5, 'Sample is not a simulated LPJG cell'
            key=(round(float(nearest.Lon)%360,3),round(float(nearest.Lat),3))
            rows=frame.loc[[key+(ordinal,) for ordinal in ordinals]]
            delta_lon=(rows.Lon.to_numpy()-lon+180)%360-180
            assert np.all(np.hypot(delta_lon*np.cos(np.deg2rad(lat)),rows.Lat.to_numpy()-lat)<1e-5), 'Native coordinate precision mismatch'
            return rows[columns[level]].to_numpy(dtype=float)
        values=source_values(rule['lr_source_files'])
        if rule.get('lr_soil_liquid_source_files'):
            values=values-source_values(rule['lr_soil_liquid_source_files'])
        return float(values.mean())
    frame=native_table(tuple(rule['lr_source_files']))
    key=(round(lon%360,4),round(lat,4),year)
    if 'lr_lpj_lut' in rule['pipelines']:
        value=frame.loc[key+(month,),['psl','pst','crp','urb'][tile]]*rule.get('lr_scale',1)
    elif 'lr_lpj_yearly' in rule['pipelines']:
        value=frame.loc[key,rule['model_variable']]
    else:
        value=frame.loc[key,'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()[month-1]]
    return value


def native_output_cells(rule,year,output):
    """Locate actual simulated cells on the expanded global output grid."""
    frame=native_daily_table(tuple(rule['lr_source_files']),year)
    coordinates=frame[['Lon','Lat']].drop_duplicates()
    lon=np.asarray(next(output[n] for n in ('longitude','lon') if n in output.variables)[:],dtype=float)
    lat=np.asarray(next(output[n] for n in ('latitude','lat') if n in output.variables)[:],dtype=float)
    assert lon.ndim==lat.ndim==1 and lon.shape==lat.shape, 'Expected native unstructured land grid'
    from scipy.spatial import cKDTree
    def xyz(x,y):
        x,y=np.deg2rad(x),np.deg2rad(y)
        return np.column_stack([np.cos(y)*np.cos(x),np.cos(y)*np.sin(x),np.sin(y)])
    distance,indices=cKDTree(xyz(lon,lat)).query(xyz(coordinates.Lon.to_numpy(),coordinates.Lat.to_numpy()))
    assert distance.max()<np.deg2rad(1e-5), 'Output grid omits simulated land cells'
    assert len(set(indices))==len(indices), 'Ambiguous source/full-grid coordinate match'
    delta_lon=(lon[indices]-coordinates.Lon.to_numpy()+180)%360-180
    assert np.all(np.hypot(delta_lon*np.cos(np.deg2rad(lat[indices])),lat[indices]-coordinates.Lat.to_numpy())<1e-5), 'Native/full-grid coordinate precision mismatch'
    return np.sort(indices)


def verify_unsimulated_mask(output,variable,native_cells):
    horizontal=next(d for d in variable.dimensions if d in ('nod2','ncells','cell','landpoints'))
    outside=np.setdiff1d(np.arange(len(output.dimensions[horizontal])),native_cells)
    other_dims=[d for d in variable.dimensions if d!='time']
    axis=other_dims.index(horizontal)
    for ti in range(len(output.dimensions['time'])):
        selection=tuple(ti if d=='time' else slice(None) for d in variable.dimensions)
        values=np.ma.masked_invalid(variable[selection])
        assert np.take(np.ma.getmaskarray(values),outside,axis=axis).all(), 'Values invented outside simulated LPJG footprint'
        assert not np.take(np.ma.getmaskarray(values),native_cells,axis=axis).any(), 'Simulated LPJG values lost during grid expansion'


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
    klon, klat = slon, slat
    grid = rule.get('grid_file')
    if grid and Path(grid).is_file():
        with netCDF4.Dataset(grid) as mesh:
            if 'triag_nodes' in mesh.variables:
                mlon, mlat = coordinates(mesh)
                # pycmor writes the mesh.nc node positions, not the float32 XIOS
                # ones; XIOS keeps the mesh node order, so match by index.
                assert mlon.size == slon.size, 'Source is not on the grid_file mesh'
                dlon = np.abs((np.asarray(mlon, np.float64) - slon + 180) % 360 - 180)
                assert np.all(dlon * np.cos(np.deg2rad(slat)) < 0.01) and np.all(np.abs(mlat - slat) < 0.01), \
                    'Source node positions differ from the mesh'
                klon, klat = mlon, mlat
    olon, olat = coordinates(output)
    assert np.all(olat <= -30.0), 'Out-of-region cells in Southern Ocean product'
    def keys(lon, lat):
        # Coordinate values are commonly stored as float32 natively and
        # float64 after CMORization. Promote before modulo and decimal
        # rounding so identical represented values produce identical keys.
        lon=np.asarray(lon,dtype=np.float64)
        lat=np.asarray(lat,dtype=np.float64)
        return list(zip(np.round(lon % 360,5),np.round(lat,5)))
    source_keys = keys(klon,klat)
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
        return sample_freshwater(rule, year, month, horizontal_indices['nod2'], level)
    paths=rule['lr_source_files']
    # A monthly product of a daily stream is the mean of the month's records.
    month_mean=day is None and rule.get('lr_source_frequency')=='day'
    def one(files,var,coeff,depth=level):
        p=next(p for p in files if str(year) in Path(p).name)
        with netCDF4.Dataset(p) as ds:
            t=next(ds[n] for n in ('time','time_counter') if n in ds.variables)
            dates=netCDF4.num2date(t[:],t.units,getattr(t,'calendar','standard'))
            matches=[i for i,d in enumerate(dates) if (d.year,d.month)==(year,month)
                     and (day is None or d.day==day)]
            if day is not None and len(matches)!=1:
                raise ValueError(f'Expected one source daily record for {year}-{month}-{day}')
            ti=matches if month_mean else matches[0]
            v=ds[var]; ix=[]
            for d in v.dimensions:
                if d in ('time','time_counter'): ix.append(ti)
                # Output model levels are stored surface-first, OpenIFS writes them top-first.
                elif d=='model_levels': ix.append(len(ds.dimensions[d])-1-depth)
                elif d in ('nz','nz1','nz_sfc','nz_200','depth','lev','pressure_levels'): ix.append(depth)
                elif d in horizontal_indices: ix.append(horizontal_indices[d])
                elif d in ('cell','ncells') and 'nod2' in horizontal_indices: ix.append(horizontal_indices['nod2'])
                else: raise ValueError(f'Unknown source axis {d}')
            value=np.ma.asarray(v[tuple(ix)],dtype=float)
            return (value.mean(axis=0) if month_mean else value)*coeff
    def depth_weights(files,var):
        # Independent of the conversion step: levels read from the raw file.
        p=next(p for p in files if str(year) in Path(p).name)
        with netCDF4.Dataset(p) as ds:
            d=next(d for d in ds[var].dimensions if d in ('nz','nz1','nz_200','depth'))
            z=np.abs(np.asarray(ds[d][:],dtype=float))
        target=float(rule['lr_target_depth'])
        exact=np.flatnonzero(np.isclose(z,target,rtol=0,atol=1e-6))
        if exact.size: return [(int(exact[0]),1.)]
        k=int(np.searchsorted(z,target))
        assert 0<k<len(z), 'Target depth outside source levels'
        w=(target-z[k-1])/(z[k]-z[k-1])
        return [(k-1,1.-w),(k,w)]
    terms=rule.get('lr_terms') or [dict(files=paths,variable=rule['model_variable'],coefficient=rule.get('lr_scale',1))]
    if rule.get('vector_partner_pattern'):
        own=one(paths,rule['model_variable'],1.)
        other=one(rule['vector_source_files'],rule['vector_partner_variable'],1.)
        u,v=(own,other) if rule['vector_component']=='x' else (other,own)
        with netCDF4.Dataset(rule['grid_file']) as mesh:
            j=horizontal_indices['nod2'];lon=float(mesh['lon'][j]);lat=float(mesh['lat'][j])
        east,north=geographic_vector(u,v,lon,lat,[rule[k] for k in ('alpha_euler','beta_euler','gamma_euler')])
        value=(east if rule['vector_component']=='x' else north)*rule.get('lr_scale',1)+rule.get('lr_offset',0)
    elif rule.get('lr_target_depth') is not None:
        value=sum(w*one(t['files'],t['variable'],t['coefficient'],k)
                  for t in terms for k,w in depth_weights(t['files'],t['variable']))
        value=value+(0 if rule.get('lr_terms') else rule.get('lr_offset',0))
    elif rule.get('lr_weighted_depth') is not None:
        limit = rule['lr_weighted_depth']
        with netCDF4.Dataset(rule['grid_file']) as grid:
            tops = np.asarray(grid['depth_bnds'][:-1])
        tops = tops[tops < limit]
        upper = slice(0, len(tops))
        limitation = one(paths, rule['model_variable'], 1., upper).filled(np.nan)
        carbon = one(rule['lr_biomass_files'], rule['lr_biomass_variable'], 1., upper).filled(np.nan)
        thickness = one(rule['lr_thickness_files'], rule['lr_thickness_variable'], 1., upper).filled(np.nan)
        wet = np.minimum(thickness, limit - tops)
        valid = (np.isfinite(limitation) & np.isfinite(carbon) & (carbon > 0)
                 & np.isfinite(thickness) & (wet > 0))
        value = (np.average(limitation[valid], weights=carbon[valid] * wet[valid])
                 if valid.any() else np.ma.masked)
    elif rule.get('lr_thickness_files'):
        h=one(rule['lr_thickness_files'],rule['lr_thickness_variable'],1.,slice(None))
        value=np.ma.sum(sum(one(t['files'],t['variable'],t['coefficient'],slice(None)) for t in terms)*h)
    elif rule.get('lr_interface_midpoints'):
        value=(one(paths,rule['model_variable'],1.,level)+one(paths,rule['model_variable'],1.,level+1))*0.5
        value=value*rule.get('lr_scale',1)+rule.get('lr_offset',0)
    elif rule.get('lr_terms'):
        value=sum(one(t['files'],t['variable'],t['coefficient']) for t in rule['lr_terms'])
    else:
        value=one(paths,rule['model_variable'],rule.get('lr_scale',1))+rule.get('lr_offset',0)
    return value


def wam_on_g150(values, lat, lon_bnds, how):
    """Independent g150 reconstruction for the audit: per WAM row, the cell whose longitude bounds contain each
    1-degree column centre; each g150 row from the two whole-degree rows it straddles."""
    values=np.ma.filled(np.ma.asarray(values,dtype=float),np.nan)
    rows=np.full((181,360),np.nan)
    west,east=np.asarray(lon_bnds,dtype=float).min(1),np.asarray(lon_bnds,dtype=float).max(1)
    columns=np.arange(360.)
    for r in np.unique(np.rint(lat)):
        k=np.flatnonzero(np.rint(lat)==r); k=k[np.argsort(west[k])]
        # Same edge rule as the step: a column centre belongs to the cell it is at or after the west edge of.
        w=(west[k]-1e-6)%360; order=np.argsort(w); k,w=k[order],w[order]
        j=np.searchsorted(w,columns,side='right')-1
        inside=((columns-w[j])%360)<(east[k[j]]-west[k[j]])
        rows[int(r)+90]=np.where(inside,values[k[j]],np.nan)
    a,b=rows[:-1],rows[1:]
    with np.errstate(invalid='ignore'):
        if how=='maximum':
            out=np.fmax(a,b)
        elif how=='direction':
            sin=np.nanmean(np.stack([np.sin(np.radians(a)),np.sin(np.radians(b))]),0)
            cos=np.nanmean(np.stack([np.cos(np.radians(a)),np.cos(np.radians(b))]),0)
            out=np.degrees(np.arctan2(sin,cos))%360
        else:
            out=np.nanmean(np.stack([a,b]),0)
    return np.ma.masked_invalid(out)


def validate_wave(cfg, rule, files):
    """WAM on g150: whole sampled output fields against an independent remap of the raw records."""
    name=rule['compound_name'].split('.')[1]; frequency=rule['compound_name'].split('.')[-2]
    years=range(cfg['inherit']['year_start'],cfg['inherit']['year_end']+1)
    key=(lambda d:(d.year,d.month)) if frequency=='mon' else (lambda d:(d.year,d.month,d.day,d.hour,d.minute))
    sources={}
    for path in rule['lr_source_files']:
        with netCDF4.Dataset(path) as ds:
            t=ds['time']; calendar=getattr(t,'calendar','standard')
            # A window statistic belongs to the end of its window, like the instants.
            stamps=ds[t.bounds][:,1] if rule.get('lr_wave_window_end') else t[:]
            for i,d in enumerate(netCDF4.num2date(stamps,t.units,calendar)):
                assert key(d) not in sources, 'Duplicate raw WAM record'
                sources[key(d)]=(path,i)
            source_lat=np.asarray(ds['lat'][:],dtype=float); source_lon_bnds=np.asarray(ds['bounds_lon'][:],dtype=float)
    def raw(path,index):
        with netCDF4.Dataset(path) as ds:
            if rule.get('lr_wave_cosine'):
                sin,cos=(np.ma.masked_invalid(np.ma.asarray(ds[n][index],dtype=float))
                         for n in (rule['model_variable'],rule['lr_wave_cosine']))
                return np.ma.masked_array(np.degrees(np.arctan2(sin.filled(0),cos.filled(0)))%360,
                                          mask=np.ma.getmaskarray(sin)|np.ma.getmaskarray(cos))
            return np.ma.masked_invalid(np.ma.asarray(ds[rule['model_variable']][index],dtype=float))
    dates=[]; checks=[]; compared=0
    assert files, 'No output'
    for path in files:
        with netCDF4.Dataset(path) as ds:
            assert name in ds.variables, f'Missing data variable {name}'
            v=ds[name]
            assert ds.getncattr('source_id')==cfg['inherit']['source_id'], 'Wrong experiment identity'
            assert hasattr(ds,'tracking_id'), 'Missing tracking_id'
            assert v.dimensions[0]=='time' and v.shape[1:]==(180,360), 'WAM output is not on g150'
            assert ds.getncattr('grid_label')=='g150', 'WAM output grid label is not g150'
            out_lat,out_lon=(np.asarray(ds[v.dimensions[i]][:],dtype=float) for i in (1,2))
            assert np.allclose(out_lat,np.arange(-89.5,90.)) and np.allclose(out_lon,np.arange(360.)), 'g150 coordinates'
            factor,offset=unit_factor('degree' if rule.get('lr_wave_cosine') else rule['expected_units'],v.units)
            t=ds['time']; decoded=netCDF4.num2date(t[:],t.units,getattr(t,'calendar','standard'))
            if frequency=='mon':
                bounds=netCDF4.num2date(ds[t.bounds][:],t.units,getattr(t,'calendar','standard'))
                for d,(lo,hi) in zip(decoded,bounds):
                    assert lo<d<hi and lo.day==hi.day==1 and (lo.year,lo.month)==key(d), 'Monthly interval mismatch'
            keys=[key(d) for d in decoded]; dates.extend(keys)
            for ti in sorted({0,len(keys)//2,len(keys)-1}):
                expected=wam_on_g150(raw(*sources[keys[ti]]),source_lat,source_lon_bnds,
                                     rule.get('lr_wave_remap','mean'))*factor+offset
                actual=np.ma.masked_invalid(np.ma.asarray(v[ti],dtype=float))
                masks=np.ma.getmaskarray(expected),np.ma.getmaskarray(actual)
                assert np.array_equal(*masks), f'Mask changed: {path.name}'
                valid=~np.ma.getmaskarray(actual)
                difference=np.abs(actual.data[valid]-expected.data[valid])
                scale=np.abs(expected.data[valid])
                if v.units=='degree':
                    # Circular: 359.99999 and 0.00001 degrees are neighbours.
                    difference=np.minimum(difference,360-difference);scale=360.
                assert np.all(difference<=np.maximum(scale*3e-5,1e-30)), (path.name,float(difference.max()))
                compared+=int(valid.sum())
            checks.append({'file':str(path),'check':'identity, g150 grid, whole first/middle/last fields against an independent remap'})
    if frequency=='mon':
        expected=[(y,m) for y in years for m in range(1,13)]
    else:
        # End-labelled 3-hourly steps; the last one is next January 1 00:00.
        hours=int(frequency[:-2]); start=cftime.datetime(years.start,1,1,calendar=decoded[0].calendar)
        stop=cftime.datetime(years.stop,1,1,calendar=decoded[0].calendar); step=timedelta(hours=hours)
        expected=[key(start+step*(i+1)) for i in range(int((stop-start)/step))]
    assert sorted(dates)==expected, f'Missing/duplicate records: {len(dates)}, expected {len(expected)}'
    return {'files':checks,'coverage':'PASS','raw_sample_comparison':compared>0,'compared_samples':compared,
            'limitations':[],'publication_ready':False}


def validate(recipe):
    cfg=yaml.safe_load(Path(recipe).read_text()); rule=cfg['rules'][0]
    out=Path(cfg['inherit']['output_directory']); files=sorted(out.rglob('*.nc'))
    if rule.get('lr_wave'):
        return dict(validate_wave(cfg,rule,files),recipe=str(recipe))
    if rule.get('lr_group') == 'requested_land':
        import requested_land_validation
        return dict(requested_land_validation.validate(cfg, rule, files), recipe=str(recipe))
    if rule.get('ocean_physics_operation'):
        import ocean_validation
        by_label = {r['grid_label']: [] for r in cfg['rules']}
        for file in files:
            with netCDF4.Dataset(file) as ds:
                by_label[ds.grid_label].append(file)
        native = by_label[rule['grid_label']]
        if not all(by_label.values()):
            raise ValueError('Missing grid for independent ocean value checks')
        result = ocean_validation.validate(cfg, native)
        if len(cfg['rules']) == 2:
            import ocean_spherical
            mapped = by_label[cfg['rules'][1]['grid_label']]
            result['mapped_grid'] = ocean_spherical.validate(cfg, mapped, native)
            result['compared_samples'] += result['mapped_grid']['compared_samples']
        return dict(result, recipe=str(recipe))
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
            land_cells=native_output_cells(rule,years.start,ds) if rule.get('lr_lpj_daily') else None
            if land_cells is not None:verify_unsimulated_mask(ds,v,land_cells)
            assert hasattr(v,'units'), 'Missing units'
            assert ds.getncattr('source_id')==cfg['inherit']['source_id'], 'Wrong experiment identity'
            assert hasattr(ds,'tracking_id'), 'Missing tracking_id'
            if expected_frequency=='fx':
                if rule.get('lr_geothermal_zero'):
                    with netCDF4.Dataset(rule['grid_file']) as grid:
                        wet=np.asarray(grid['depth_lev'][:])>0
                    actual=np.ma.masked_invalid(v[:]).reshape(-1)
                    assert actual.shape==wet.shape, 'Geothermal wet-column shape mismatch'
                    assert not np.ma.getmaskarray(actual)[wet].any(), 'Missing represented geothermal boundary'
                    assert np.equal(np.asarray(actual)[wet],0.).all(), 'Nonzero field contradicts reviewed zero boundary'
                    assert np.ma.getmaskarray(actual)[~wet].all(), 'Non-ocean geothermal values must be missing'
                    compared_samples+=int(wet.sum())
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
                zd=next((d for d in v.dimensions if d in ('olevel','plev19','plev','lev','depth','sdepth','nz','nz1')),None)
                if rule.get('lr_lpj_daily') and zd:
                    with Path(rule['lr_source_files'][0]).open() as source:
                        bottoms=np.array([float(n[5:]) for n in source.readline().split() if n.startswith('Depth')])
                    depth=ds[zd]
                    assert np.allclose(depth[:],bottoms-.05,atol=1e-7), 'Incorrect soil layer centres'
                    assert getattr(depth,'bounds','') in ds.variables, 'Missing soil layer bounds'
                    assert np.allclose(ds[depth.bounds][:],np.column_stack([bottoms-.1,bottoms]),atol=1e-7), 'Incorrect soil layer bounds'
                if zd: level_values.append(len(ds.dimensions[zd])//2)
                for level in sorted(set(level_values)):
                    for fraction in (0.031,0.27,0.51,0.79,0.97):
                        ix=[]; source_indices={}; cell_index=None
                        for d in v.dimensions:
                            if d=='time': ix.append(ti)
                            elif d==zd: ix.append(level)
                            elif d in ('nod2','ncells','cell','landpoints'):
                                j=(int(land_cells[int(fraction*(len(land_cells)-1))]) if land_cells is not None
                                   else int(fraction*(len(ds.dimensions[d])-1)))
                                ix.append(j)
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
                            src=sample_lpj(rule,decoded[ti].year,decoded[ti].month,float(lon),float(lat),
                                           day=decoded[ti].day,level=level)
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
