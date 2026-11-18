"""Bounded daily heat integration using the existing LR transport formula.

Keep only one source day and one monthly thickness field in memory. Building
an annual Dask graph of three triangle-node gathers retains too many shared
three-dimensional intermediates during the final write.
"""
from functools import lru_cache

import dask.array as da
from dask import delayed
import netCDF4
import numpy as np
import xarray as xr


def _dates(ds):
    t=ds['time']
    return netCDF4.num2date(t[:],t.units,getattr(t,'calendar','standard'))


def _stamp(date):
    return (date.year,date.month,date.day,date.hour,date.minute,date.second)


@lru_cache(maxsize=2)
def _geometry(path):
    # Same spherical Cartesian triangle-area proxy as _elem_geometry in the
    # existing custom_steps heat transport. This changes execution, not geometry.
    with netCDF4.Dataset(path) as ds:
        lon=np.deg2rad(np.asarray(ds['lon'][:],dtype='f8'))
        lat=np.deg2rad(np.asarray(ds['lat'][:],dtype='f8'))
        tri=np.asarray(ds['triag_nodes'][:],dtype='i8')
    if tri.shape[0]!=3:tri=tri.T
    tri=tri-1
    if tri.shape[0]!=3 or tri.min()<0 or tri.max()>=len(lon):
        raise ValueError('Invalid triangle-node geometry for daily heat transport')
    xyz=np.column_stack((np.cos(lat)*np.cos(lon),np.cos(lat)*np.sin(lon),np.sin(lat)))
    cross=np.cross(xyz[tri[1]]-xyz[tri[0]],xyz[tri[2]]-xyz[tri[0]])
    area=.5*np.linalg.norm(cross,axis=-1)*6371000.0**2
    return np.sqrt(area),tri


@lru_cache(maxsize=1)
def _thickness(path,index):
    with netCDF4.Dataset(path) as ds:
        v=ds['hnode']
        if v.dimensions!=('time','nod2','nz'):
            raise ValueError('Daily heat transport requires native time/nod2/nz thickness')
        return np.ma.masked_invalid(np.ma.asarray(v[index],dtype='f8'))


def _integrate_day(source,variable,index,grid,thickness_file,month_index,factor,block):
    edge,tri=_geometry(grid)
    thickness=_thickness(thickness_file,month_index)
    result=np.empty(len(edge),dtype='f8')
    with netCDF4.Dataset(source) as ds:
        v=ds[variable]
        if v.dimensions!=('time','elem','nz') or v.shape[1:]!=(len(edge),thickness.shape[1]):
            raise ValueError('Daily heat transport source and native geometry disagree')
        v.set_var_chunk_cache(size=16*1024**2,nelems=1009,preemption=.75)
        for first in range(0,len(edge),block):
            last=min(first+block,len(edge))
            values=np.ma.masked_invalid(np.ma.asarray(v[index,first:last,:],dtype='f8'))
            nodes=tri[:,first:last]
            layer=(thickness[nodes[0]]+thickness[nodes[1]]+thickness[nodes[2]])/3.0
            # sum(skipna=True) in the original pipeline returns zero for an
            # entirely missing column. Preserve that convention exactly.
            column=np.ma.filled(np.ma.sum(values*layer,axis=1),0.)
            result[first:last]=column*factor*edge[first:last]
    return result


def compute_daily_heat_transport(data,rule):
    """Return lazy 2-D daily values; never materialize an annual 3-D field."""
    source=rule.get('heat_source_file');thickness=rule.get('heat_hnode_file')
    grid=rule.get('grid_file');variable=rule.get('model_variable')
    if not all((source,thickness,grid)) or not rule.get('vertical_sum',False):
        raise ValueError('Daily heat transport needs frozen primary/thickness files and vertical_sum')
    if tuple(data.dims)!=('time','elem','nz'):
        raise ValueError('Daily heat transport requires native time/elem/nz data')
    with netCDF4.Dataset(source) as ds:
        dates=_dates(ds)
        if ds[variable].shape!=data.shape:
            raise ValueError('Daily heat source shape differs from selected primary data')
    if [_stamp(d) for d in dates]!=[_stamp(d) for d in data.indexes['time']]:
        raise ValueError('Daily heat source timestamps differ from selected primary data')
    with netCDF4.Dataset(thickness) as ds:
        months=[(d.year,d.month) for d in _dates(ds)]
    if len(months)!=len(set(months)):
        raise ValueError('Duplicate monthly thickness records')
    block=int(rule.get('heat_spatial_block',32768))
    if block<1:raise ValueError('heat_spatial_block must be positive')
    factor=float(rule.get('scale_factor'))
    frames=[]
    for index,date in enumerate(dates):
        month=(date.year,date.month)
        if month not in months:raise ValueError(f'Missing thickness month {month}')
        task=delayed(_integrate_day)(source,variable,index,grid,thickness,
                                    months.index(month),factor,block)
        frames.append(da.from_delayed(task,shape=(data.sizes['elem'],),dtype='f8'))
    coords={name:value for name,value in data.coords.items()
            if set(value.dims)<=set(('time','elem'))}
    attrs=dict(data.attrs,units='W')
    return xr.DataArray(da.stack(frames,axis=0),dims=('time','elem'),coords=coords,
                        attrs=attrs,name=data.name)
