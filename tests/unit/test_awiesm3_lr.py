"""Small, offline regressions for the opt-in LR adapters."""
from types import SimpleNamespace

import cftime
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pycmor.awiesm3_lr import exact_vertical_bounds, load_linear, load_lpj, load_lpj_yearly, load_lpj_lut, monthly_time, transform


class Rule(SimpleNamespace):
    def get(self, key, default=None):
        return getattr(self, key, default)


def monthly(year=1852):
    dates=[cftime.DatetimeGregorian(year,m,15) for m in range(1,13)]
    return xr.DataArray(np.arange(12.),dims='time_counter',coords={'time_counter':dates},name='2t')


def test_monthly_time_preserves_calendar_values_and_encoding():
    data=monthly()
    data.time_counter.encoding={'units':'seconds since 1850-01-01','calendar':'gregorian'}
    data=data.assign_coords(time_centered=data.time_counter)
    result=monthly_time(data,Rule(year_start=1852,year_end=1852))
    assert result.dims==('time',)
    assert 'time_centered' not in result.coords
    assert result.time.encoding['calendar']=='gregorian'
    np.testing.assert_array_equal(result.values,data.values)
    np.testing.assert_array_equal(result.time.values,data.time_counter.values)


@pytest.mark.parametrize('indices',[list(range(11)),[0]+list(range(11)),list(reversed(range(12)))])
def test_monthly_time_rejects_missing_duplicate_unordered(indices):
    with pytest.raises(ValueError,match='monthly records'):
        monthly_time(monthly().isel(time_counter=indices),Rule(year_start=1852,year_end=1852))


def test_monthly_time_rejects_numeric_and_conflicting_axes():
    rule=Rule(year_start=1852,year_end=1852)
    with pytest.raises(ValueError,match='decoded'):
        monthly_time(monthly().assign_coords(time_counter=np.arange(12)),rule)
    data=monthly().assign_coords(time_centered=('time_counter',[cftime.DatetimeGregorian(1852,m,16) for m in range(1,13)]))
    with pytest.raises(ValueError,match='Ambiguous'):
        monthly_time(data,rule)


def test_linear_flux_scale_and_units_guard():
    data=monthly();data.attrs['units']='J m-2'
    rule=Rule(name='rlut',expected_units='J m-2',lr_scale=-1/3600,lr_units='W m-2')
    result=transform(data,rule)
    np.testing.assert_allclose(result.values,-data.values/3600)
    assert result.attrs['units']=='W m-2'
    rule.expected_units='W m-2'
    with pytest.raises(ValueError,match='expected'):
        transform(data,rule)


def test_linear_combination_promotes_before_cancellation(monkeypatch):
    # rsus is a difference of two large float32 accumulated fluxes. Promoting
    # only after multiplication/subtraction has already lost the small residual.
    first=monthly().astype('float32')*0+np.float32(2**24+2)
    second=monthly().astype('float32')*0+np.float32(2**24)
    first.attrs['units']=second.attrs['units']='J m-2'
    sources={'a.nc':first.rename('a').to_dataset(), 'b.nc':second.rename('b').to_dataset()}
    monkeypatch.setattr(xr,'open_mfdataset',lambda paths,**kwargs:sources[paths[0].name])
    terms=[{'file':name+'.nc','variable':name,'units':'J m-2','coefficient':sign/3600}
           for name,sign in [('a',1),('b',-1)]]
    result=load_linear(None,Rule(year_start=1852,year_end=1852,data_path='.',lr_terms=terms,model_variable='rsus',lr_units='W m-2'))
    assert result.dtype==np.dtype('float64')
    np.testing.assert_allclose(result,np.full(12,2/3600),rtol=1e-8,atol=0)


def test_ocean_bounds_use_actual_interfaces(tmp_path):
    mesh=tmp_path/'mesh.nc'
    xr.Dataset({'depth':('level',[2.,10.,30.]),'depth_bnds':('interface',[0.,5.,18.,50.])}).to_netcdf(mesh)
    data=xr.DataArray([1.,2.,3.],dims='lev',coords={'lev':[2.,10.,30.]},name='thetao')
    result=exact_vertical_bounds(data,Rule(lr_exact_ocean_depth=True,grid_file=str(mesh)))
    np.testing.assert_array_equal(result.lev_bnds,[[0,5],[5,18],[18,50]])
    assert result.lev.attrs['bounds']=='lev_bnds'


def test_ice_free_zero_does_not_fill_cavities(tmp_path):
    mesh=tmp_path/'mesh.nc'
    xr.Dataset({'lat':('ncells',[70.,75.,-80.]),'cav_nod_mask':('ncells',[0,0,1])}).to_netcdf(mesh)
    data=xr.DataArray([0.5,np.nan,np.nan],dims='nod2',coords={'lat':('nod2',[70.,75.,-80.])},attrs={'units':'1'})
    result=transform(data,Rule(name='siconc',grid_file=str(mesh),lr_ice_free_zero=True,lr_scale=100,lr_units='%'))
    np.testing.assert_allclose(result,[50,0,np.nan],equal_nan=True)


def test_lpj_loader_leap_month_and_duplicate_detection(tmp_path):
    source=tmp_path/'monthly.out'
    months='Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()
    frame=pd.DataFrame([[1.,2.,1852,*range(12)]],columns=['Lon','Lat','Year',*months])
    frame.to_csv(source,sep=' ',index=False)
    rule=Rule(inputs=[SimpleNamespace(path=tmp_path,pattern_str=source.name)],year_start=1852,year_end=1852,model_variable='Total',source_units='kg m-2')
    result=load_lpj(None,rule)
    assert result.time.values[1]==cftime.DatetimeGregorian(1852,2,15,12)
    np.testing.assert_array_equal(result[:,0],np.arange(12))
    pd.concat([frame,frame]).to_csv(source,sep=' ',index=False)
    with pytest.raises(ValueError,match='duplicate'):
        load_lpj(None,rule)


def test_lpj_yearly_bounds_and_lut_tile_order(tmp_path):
    source=tmp_path/'annual.out'
    pd.DataFrame({'Lon':[1,2],'Lat':[5,5],'Year':[1852,1852],'Total':[20,30]}).to_csv(source,sep=' ',index=False)
    rule=Rule(inputs=[SimpleNamespace(path=tmp_path,pattern_str=source.name)],year_start=1852,year_end=1852,model_variable='Total',source_units='%')
    annual=load_lpj_yearly(None,rule)
    assert (annual.time_bnds.values[0,1]-annual.time_bnds.values[0,0]).days==366
    np.testing.assert_array_equal(annual.Total,[[20,30]])
    source=tmp_path/'lut.out'
    rows=[[1,5,1852,m,10,20,30,40] for m in range(1,13)]
    frame=pd.DataFrame(rows,columns=['Lon','Lat','Year','Mth','psl','crp','pst','urb'])
    frame.to_csv(source,sep=' ',index=False)
    rule.inputs[0].pattern_str=source.name
    result=load_lpj_lut(None,rule)
    np.testing.assert_array_equal(result[0,:,0],[10,30,20,40])
    assert result.shape==(12,4,1)
    frame.iloc[:-1].to_csv(source,sep=' ',index=False)
    with pytest.raises(ValueError,match='missing cells/month'):
        load_lpj_lut(None,rule)


def test_lr_complete_interfaces_and_bottom_selection(tmp_path):
    mesh=tmp_path/'mesh.nc'
    xr.Dataset({'depth':('level',[2.5,10.]),'depth_bnds':('interface',[0.,5.,15.]),'depth_lev':('ncells',[3,2])}).to_netcdf(mesh)
    data=xr.DataArray([[0.,2.,4.],[2.,4.,np.nan]],dims=('nod2','nz1'),coords={'nz1':[0.,5.,15.]},attrs={'units':'m s-1'})
    result=transform(data,Rule(name='wo',grid_file=str(mesh),lr_interface_midpoints=True))
    np.testing.assert_allclose(result,[[1,3],[3,np.nan]],equal_nan=True)
    bottom=transform(result,Rule(name='bottom',grid_file=str(mesh),lr_bottom=True))
    np.testing.assert_array_equal(bottom,[3,3])
