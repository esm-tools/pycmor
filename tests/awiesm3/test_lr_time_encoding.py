"""Regression: explicit epoch/calendar must not erase dates before saving."""
from types import SimpleNamespace

import cftime
import netCDF4
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pycmor.core.config import PycmorConfigManager
from pycmor.core.rule import Rule
from pycmor.awiesm3_lr import load_lpj_yearly
from pycmor.std_lib.files import save_dataset


@pytest.mark.parametrize('file_timespan', ['file_native', '1YS'])
def test_explicit_time_encoding_roundtrip(tmp_path, file_timespan):
    dates=[cftime.DatetimeGregorian(1852,m,15) for m in range(1,13)]
    data=xr.DataArray(np.arange(12,dtype=float),dims='time',coords={'time':dates},name='tas',attrs={'units':'K'})
    drv=SimpleNamespace(frequency='mon',table_header=SimpleNamespace(table_id='Amon',approx_interval=30.),
                        cell_methods='time: mean',temporal_shape='time-intv')
    rule=Rule(name='tas',cmor_variable='tas',model_variable='tas',compound_name='atmos.tas.tavg-h2m-hxy-u.mon.glb',
              data_request_variable=drv,cmor_version='CMIP7',variant_label='r1i1p1f1',grid_label='gn',
              source_id='TEST',experiment_id='test',output_directory=str(tmp_path),file_timespan=file_timespan,
              time_units='days since 1850-01-01',time_calendar='gregorian',time_method='mean',approx_interval=30.)
    rule._pycmor_cfg=PycmorConfigManager.from_pycmor_cfg({'enable_output_subdirs':False})
    save_dataset(data,rule)
    files=list(tmp_path.glob('*.nc'))
    assert len(files)==1
    assert files[0].name.endswith('_185201-185212.nc')
    with netCDF4.Dataset(files[0]) as ds:
        t=ds['time'];bounds=ds[t.bounds][:]
        # Existing CMIP7 canonicalization is date-equivalent after 1582.
        assert t.calendar=='proleptic_gregorian'
        assert t.units.startswith('days since 1850-01-01')
        np.testing.assert_allclose(t[:],bounds.mean(axis=1))
        assert bounds[1,1]-bounds[1,0]==29
        assert bounds[-1,1]-bounds[0,0]==366
        np.testing.assert_allclose(ds['tas'][:],np.arange(12),rtol=2e-5,atol=1e-15)
    with xr.open_dataset(files[0]) as ds:
        assert list(ds.time.dt.month.values)==list(range(1,13))


@pytest.mark.parametrize('years,file_timespan', [
    ([1852], '1YS'),
    ([1852], 'file_native'),
    (list(range(1850,1855)), '1YS'),
])
def test_annual_lpj_text_to_netcdf_roundtrip(tmp_path, monkeypatch, years, file_timespan):
    source=tmp_path/'baresoilFrac_yearly.out'
    rows=[[1.,2.,year,20.+year-years[0]] for year in years]
    pd.DataFrame(rows,columns=['Lon','Lat','Year','Total']).to_csv(source,sep=' ',index=False)
    drv=SimpleNamespace(frequency='yr',table_header=SimpleNamespace(table_id='Eyr',approx_interval=365.),
                        cell_methods='area: time: mean',temporal_shape='time-intv')
    rule=Rule(name='baresoilFrac_yr',cmor_variable='baresoilFrac',model_variable='Total',
              compound_name='land.baresoilFrac.tavg-u-hxy-u.yr.glb',source_units='%',
              inputs=[{'path':str(tmp_path),'pattern':source.name}],
              year_start=years[0],year_end=years[-1],data_request_variable=drv,
              cmor_version='CMIP7',variant_label='r1i1p1f1',grid_label='gn',
              source_id='TEST',experiment_id='test',output_directory=str(tmp_path/'output'),
              file_timespan=file_timespan,time_units='days since 1850-01-01',
              time_calendar='gregorian',time_method='mean',approx_interval=365.)
    rule._pycmor_cfg=PycmorConfigManager.from_pycmor_cfg({'enable_output_subdirs':False})
    data=load_lpj_yearly(None,rule).rename({'Total':'baresoilFrac'})
    real_open=xr.open_dataset
    def no_text_as_netcdf(filename,*args,**kwargs):
        assert not str(filename).endswith('.out'), 'Annual text source reopened as NetCDF'
        return real_open(filename,*args,**kwargs)
    monkeypatch.setattr(xr,'open_dataset',no_text_as_netcdf)
    save_dataset(data,rule)
    outputs=sorted((tmp_path/'output').glob('*.nc'))
    assert len(outputs)==len(years)
    for path,year in zip(outputs,years):
        assert path.name.endswith(f'_{year}-{year}.nc')
        with netCDF4.Dataset(path) as ds:
            assert len(ds.dimensions['time'])==1
            t=ds['time'];bounds=ds[t.bounds][:]
            dates=netCDF4.num2date(bounds,t.units,t.calendar)
            assert str(dates[0,0])==f'{year}-01-01 00:00:00'
            assert str(dates[0,1])==f'{year+1}-01-01 00:00:00'
            assert bounds[0,1]-bounds[0,0]==(366 if year==1852 else 365)
            np.testing.assert_allclose(t[:],bounds.mean(axis=1))
            np.testing.assert_allclose(ds['baresoilFrac'][:],[[20.+year-years[0]]])
