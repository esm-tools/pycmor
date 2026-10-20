"""AWI-ESM3 LR monthly pipeline steps (TCO95L91 / CORE3, without REcoM).

These opt-in steps accept archived monthly means, not subdaily accumulations.
All source files and shared meshes are read-only. Scaling and mesh choices
must be supplied explicitly by an audited experiment recipe.
"""
from pathlib import Path
import cftime
import numpy as np
import pandas as pd
import xarray as xr


def _select_years(data, rule):
    """Select recipe years before checking completeness of a multi-year archive."""
    name=next((n for n in ('time','time_counter','time_centered') if n in data.dims),None)
    if name is None:
        raise ValueError('Missing time dimension')
    years=data[name].dt.year
    return data.isel({name:np.flatnonzero(
        ((years>=rule.year_start)&(years<=rule.year_end)).values)})


def load_monthly(data, rule):
    """Load the exact files frozen by campaign discovery, regardless of leg size."""
    paths=rule.get('lr_source_files')
    if not paths:
        raise ValueError('Campaign monthly reader requires explicit lr_source_files')
    ds=xr.open_mfdataset(paths,combine='by_coords',join='exact')
    return _select_years(ds[rule.model_variable],rule)


def _lpj_frame(rule):
    paths=rule.get('lr_source_files') or [rule.inputs[0].path/rule.inputs[0].pattern_str]
    frames=[pd.read_csv(path,sep=r'\s+') for path in paths]
    frame=pd.concat(frames,ignore_index=True)
    return frame[frame.Year.between(rule.year_start,rule.year_end)], ', '.join(map(str,paths))


def monthly_time(data, rule):
    """Require complete monthly coverage and a single decoded time coordinate.

    XIOS exposes both time_counter and time_centered. Keep the dimension
    coordinate, check that its auxiliary duplicate agrees, and retain its
    calendar. Do not resample already averaged data or invent missing months.
    """
    if 'time' not in data.dims:
        candidates=[n for n in ('time_counter','time_centered') if n in data.dims]
        if len(candidates)!=1:
            raise ValueError('Expected one monthly time dimension')
        data=data.rename({candidates[0]:'time'})
    if 'time' not in data.coords:
        raise ValueError('Missing decoded monthly time coordinate')
    try:
        years=data.time.dt.year.values
        months=data.time.dt.month.values
    except (AttributeError, TypeError) as exc:
        raise ValueError('Monthly time must be decoded, not numeric') from exc
    expected=[(y,m) for y in range(rule.year_start,rule.year_end+1) for m in range(1,13)]
    if list(zip(years,months))!=expected:
        raise ValueError('Missing, duplicate, unordered or out-of-range monthly records')
    for name in ('time_centered','time_counter'):
        if name in data.coords:
            if data[name].dims!=('time',) or not np.array_equal(data[name].values,data.time.values):
                raise ValueError(f'Ambiguous XIOS time coordinate: {name}')
            data=data.drop_vars(name)
    rule.time_method='mean'
    rule.frequency_str='MS'
    return data

def transform(data, rule):
    """Linear operations commute with monthly averaging; no nonlinear proxies."""
    expected=rule.get('expected_units')
    if expected and data.attrs.get('units') != expected:
        raise ValueError(f'{rule.name}: expected {expected}, got {data.attrs.get("units")}')
    attrs=dict(data.attrs)
    if rule.get('lr_interface_midpoints',False):
        with xr.open_dataset(rule.grid_file,decode_times=False) as mesh:
            centers=mesh.depth.values
            edges=mesh.depth_bnds.values
        dim=next((d for d in data.dims if d in ('nz1','nz','lev','depth')),None)
        if dim is None or data.sizes[dim]!=len(edges):
            raise ValueError('LR interface field must contain all N+1 interfaces')
        if not np.allclose(data[dim],edges,rtol=0,atol=1e-4):
            raise ValueError('Interface coordinates differ from the CORE3 mesh')
        upper=data.isel({dim:slice(None,-1)}).assign_coords({dim:centers})
        lower=data.isel({dim:slice(1,None)}).assign_coords({dim:centers})
        data=(upper+lower)*0.5
        data[dim].attrs.update(standard_name='depth',long_name='Ocean layer midpoint depth',units='m',positive='down',axis='Z')
    if rule.get('lr_bottom',False):
        with xr.open_dataset(rule.grid_file,decode_times=False) as mesh:
            indices=mesh.depth_lev.values.astype(int)-2
        dim=next(d for d in data.dims if d in ('nz1','nz','lev','depth'))
        horizontal=next(d for d in data.dims if d not in ('time',dim))
        if len(indices)!=data.sizes[horizontal] or np.any(indices<0) or np.any(indices>=data.sizes[dim]):
            raise ValueError('Invalid bottom indices for the source vertical grid')
        data=data.isel({dim:xr.DataArray(indices,dims=horizontal)}).drop_vars(dim)
    if rule.get('lr_ice_free_zero',False):
        # CORE3 includes ice-shelf cavities: fill is zero ice only on exposed ocean.
        with xr.open_dataset(rule.grid_file,decode_times=False) as mesh:
            assert np.allclose(data.lat.values,mesh.lat.values,atol=1e-4,rtol=0)
            ocean=xr.DataArray(mesh.cav_nod_mask.values==0,dims=('nod2',))
        data=data.fillna(0).where(ocean)
    if rule.get('lr_scale',1)!=1:
        data=data*rule.get('lr_scale')
    if rule.get('lr_offset',0)!=0:
        data=data+rule.get('lr_offset')
    attrs['units']=rule.get('lr_units',attrs.get('units',''))
    data.attrs=attrs
    # OIFS pressure-level metadata incorrectly says positive=up in some files.
    if 'pressure_levels' in data.coords:
        data['pressure_levels'].attrs.update(standard_name='air_pressure',positive='down',axis='Z')
    return data

def exact_vertical_bounds(data,rule):
    """Attach actual CORE3 interfaces; midpoint extrapolation is not exact."""
    if not rule.get('lr_exact_ocean_depth',False):return data
    ds=data.to_dataset() if isinstance(data,xr.DataArray) else data
    with xr.open_dataset(rule.grid_file,decode_times=False) as mesh:
        centers=mesh.depth.values; edges=mesh.depth_bnds.values
        names=[n for n in ds.coords if ds[n].ndim==1 and ds[n].size==len(centers)]
        assert len(names)==1, names
        name=names[0]
        assert np.allclose(ds[name].values,centers,rtol=0,atol=1e-4)
        ds[name+'_bnds']=xr.DataArray(np.column_stack([edges[:-1],edges[1:]]),dims=(name,'bnds'))
        ds[name].attrs['bounds']=name+'_bnds'
    return ds

def load_lpj(data,rule):
    """Read LR Jan..Dec tables with complete, unique cell/year records."""
    frame,source=_lpj_frame(rule)
    months='Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()
    if not set(months).issubset(frame.columns):
        raise ValueError(f'{source}: this pilot loader requires Jan..Dec columns')
    if frame.duplicated(['Lon','Lat','Year']).any():
        raise ValueError(f'{source}: duplicate cell/year rows')
    expected=set(range(rule.year_start,rule.year_end+1))
    if set(frame.Year.unique())!=expected:
        raise ValueError(f'{source}: missing years')
    cells=frame[['Lon','Lat']].drop_duplicates().sort_values(['Lat','Lon'],ascending=[False,True])
    coords=pd.MultiIndex.from_frame(cells)
    values=[]; times=[]
    for year in sorted(expected):
        yr=frame[frame.Year==year].set_index(['Lon','Lat']).reindex(coords)
        if yr[months].isna().any().any():
            raise ValueError(f'{source}: incomplete grid/year {year}')
        values.extend(yr[months].to_numpy().T)
        for month in range(1,13):
            start=cftime.DatetimeGregorian(year,month,1)
            end=cftime.DatetimeGregorian(year+int(month==12),month%12+1,1)
            times.append(start+(end-start)/2)
    da=xr.DataArray(np.asarray(values),dims=('time','ncells'),coords={'time':times,'lon':('ncells',cells.Lon.to_numpy()),'lat':('ncells',cells.Lat.to_numpy())},name=rule.model_variable)
    da.attrs['units']=rule.get('source_units')
    da.time.encoding.update(units='days since 1850-01-01',calendar='gregorian')
    da.lat.attrs.update(standard_name='latitude',units='degrees_north')
    da.lon.attrs.update(standard_name='longitude',units='degrees_east')
    return da

def load_linear(data,rule):
    """Combine archived monthly fields with explicitly audited coefficients."""
    arrays=[]
    for term in rule.get('lr_terms'):
        paths=term.get('files') or [Path(rule.get('data_path'))/term['file'].format(year=y) for y in range(rule.year_start,rule.year_end+1)]
        ds=xr.open_mfdataset(paths,combine='by_coords',chunks={'time_counter':1})
        da=monthly_time(_select_years(ds[term['variable']],rule),rule)
        if da.attrs.get('units')!=term['units']:
            raise ValueError(f'Unexpected units for {term["variable"]}: {da.attrs.get("units")}')
        arrays.append(da.astype('float64')*term['coefficient'])
    arrays=xr.align(*arrays,join='exact')
    result=sum(arrays).rename(rule.model_variable)
    result.attrs['units']=rule.get('lr_units')
    return result

def load_fx(data,rule):
    """Use the explicitly chosen grid area/geometry, without model integration."""
    with xr.open_dataset(rule.get('grid_file'),decode_times=False) as mesh:
        da=mesh[rule.get('lr_mesh_variable')].load()
        if da.ndim==1:
            dim=da.dims[0]
            da=da.assign_coords(lon=(dim,mesh.lon.values),lat=(dim,mesh.lat.values))
        elif da.dims!=('lat','lon'):
            raise ValueError('Unexpected horizontal area dimensions')
    da=da.rename(rule.model_variable)
    da.attrs={'units':rule.get('lr_units')}
    da.lat.attrs.update(standard_name='latitude',units='degrees_north')
    da.lon.attrs.update(standard_name='longitude',units='degrees_east')
    return da


def load_lpj_yearly(data,rule):
    """Read annual mean Total columns, retaining explicit calendar-year bounds."""
    frame,source=_lpj_frame(rule)
    if frame.duplicated(['Lon','Lat','Year']).any():
        raise ValueError(f'{source}: duplicate cell/year rows')
    cells=frame[['Lon','Lat']].drop_duplicates().sort_values(['Lat','Lon'],ascending=[False,True])
    index=pd.MultiIndex.from_frame(cells)
    values=[];times=[];bounds=[]
    for year in range(rule.year_start,rule.year_end+1):
        row=frame[frame.Year==year].set_index(['Lon','Lat']).reindex(index)[rule.model_variable]
        if row.isna().any() or len(row)==0:
            raise ValueError(f'{source}: missing cells/year {year}')
        start=cftime.DatetimeGregorian(year,1,1);end=cftime.DatetimeGregorian(year+1,1,1)
        times.append(start+(end-start)/2);bounds.append([start,end]);values.append(row.to_numpy())
    da=xr.DataArray(values,dims=('time','ncells'),coords={'time':times,'lat':('ncells',cells.Lat),'lon':('ncells',cells.Lon)},name=rule.model_variable,attrs={'units':rule.source_units})
    ds=da.to_dataset()
    ds['time_bnds']=(('time','bnds'),bounds)
    ds.time.attrs['bounds']='time_bnds'
    for name in ('time','time_bnds'):
        ds[name].encoding.update(units='days since 1850-01-01',calendar='gregorian')
    ds.lat.attrs.update(standard_name='latitude',units='degrees_north')
    ds.lon.attrs.update(standard_name='longitude',units='degrees_east')
    rule.time_method='mean';rule.frequency_str='YS'
    return ds


def load_lpj_lut(data,rule):
    """Read every land-use tile, checking every cell/month instead of filling gaps."""
    frame,source=_lpj_frame(rule)
    labels=['primary_and_secondary_land','pastures','crops','urban']
    columns=['psl','pst','crp','urb']
    if not set(columns+['Mth']).issubset(frame.columns):
        raise ValueError(f'{source}: missing land-use columns')
    if frame.duplicated(['Lon','Lat','Year','Mth']).any():
        raise ValueError(f'{source}: duplicate cell/year/month rows')
    cells=frame[['Lon','Lat']].drop_duplicates().sort_values(['Lat','Lon'],ascending=[False,True])
    index=pd.MultiIndex.from_frame(cells)
    values=[];times=[]
    for year in range(rule.year_start,rule.year_end+1):
        for month in range(1,13):
            row=frame[(frame.Year==year)&(frame.Mth==month)].set_index(['Lon','Lat']).reindex(index)[columns]
            if row.isna().any().any() or len(row)==0:
                raise ValueError(f'{source}: missing cells/month {year}-{month}')
            start=cftime.DatetimeGregorian(year,month,1);end=cftime.DatetimeGregorian(year+int(month==12),month%12+1,1)
            times.append(start+(end-start)/2);values.append(row.to_numpy().T)
    da=xr.DataArray(np.asarray(values)*rule.get('lr_scale',1),dims=('time','landuse','ncells'),
                    coords={'time':times,'lat':('ncells',cells.Lat),'lon':('ncells',cells.Lon),
                            'sector':('landuse',np.asarray(labels,dtype='S'))},name=rule.model_variable,
                    attrs={'units':rule.get('lr_units',rule.source_units)})
    da.time.encoding.update(units='days since 1850-01-01',calendar='gregorian')
    da.lat.attrs.update(standard_name='latitude',units='degrees_north')
    da.lon.attrs.update(standard_name='longitude',units='degrees_east')
    da.sector.attrs.update(standard_name='area_type',long_name='Land use type')
    da.sector.encoding['_FillValue']=None
    return da
