"""The full workflow must average every selected year without external state."""
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch
import importlib.util
import tempfile

import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
import netCDF4

from climatology_support import configure


class Rule(SimpleNamespace):
    def get(self,key,default=None):return getattr(self,key,default)


class ClimatologyWindowTests(TestCase):
    @classmethod
    def setUpClass(cls):
        repo=Path(__file__).resolve().parents[3]
        spec=importlib.util.spec_from_file_location('climatology_custom',repo/'examples/custom_steps.py')
        cls.custom=importlib.util.module_from_spec(spec);spec.loader.exec_module(cls.custom)

    def test_equal_year_weighting_and_no_accumulator(self):
        dates=pd.date_range('2000-01-01','2001-12-31',freq='D')
        values=np.where(dates.year==2000,2.,6.)
        data=xr.DataArray(da.from_array(values,chunks=31),dims='time',coords={'time':dates},name='pfull')
        rule=Rule(climatology_window=[2000,2001],model_variable='pfull')
        with patch.object(self.custom,'_climatology_lock',side_effect=AssertionError('no mutable state')):
            result=self.custom.accumulate_monthly_climatology(data,rule)
        self.assertIsNotNone(result.chunks)
        np.testing.assert_allclose(result.compute(),4.)
        self.assertEqual(rule.climatology_years,(2000,2001))
        # Leap-February is averaged within each year, then equally over years.
        self.assertEqual(result.sizes['time'],12)

    def test_missing_year_is_rejected(self):
        dates=pd.date_range('2000-01-01',periods=12,freq='MS')
        data=xr.DataArray(np.ones(12),dims='time',coords={'time':dates})
        with self.assertRaisesRegex(ValueError,'every year'):
            self.custom.accumulate_monthly_climatology(data,Rule(climatology_window=[2000,2001],model_variable='pfull'))

    def test_hybrid_pressure_uses_same_years_and_keeps_lazy_data(self):
        dates=pd.date_range('2000-01-01','2002-01-01',freq='D')
        values=np.where(dates.year==2000,2.,np.where(dates.year==2001,6.,100.))
        ps=xr.DataArray(da.from_array(values[:,None],chunks=(31,1)),dims=('time','cell'),
                        coords={'time':dates},name='sp')
        primary=xr.DataArray(da.ones((12,2,1),chunks=(1,2,1)),dims=('time','model_levels','cell'),
                            coords={'time':pd.date_range('2000-01-01',periods=12,freq='MS')},name='pfull')
        rule=Rule(climatology_window=[2000,2001],model_variable='pfull',oifs_vtable='unused')
        with patch.object(self.custom,'_load_secondary_mf',return_value=ps), \
             patch.object(self.custom,'_read_oifs_vtable',return_value=(np.array([0.,1.,2.]),np.array([0.,.5,1.]))):
            result=self.custom.add_hybrid_sigma_coordinate(primary.to_dataset(),rule)
        self.assertIsNotNone(result.ps.chunks)
        np.testing.assert_allclose(result.ps.compute(),4.)

    def test_configure_freezes_secondary_files_and_all_years(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for year in (1999,2000,2001,2002):
                for name in ('pfull','sp'):
                    dates=pd.date_range(f'{year}-01-01',f'{year+1}-01-01',freq='6h')[1:]
                    if name=='pfull':
                        dates=pd.date_range(f'{year}-01-01',periods=12,freq='MS')+pd.Timedelta(days=14)
                    path=root/f'{name}_{year}-{year}.nc'
                    xr.DataArray(np.ones(len(dates)),dims='time_counter',coords={'time_counter':dates},
                                 name=name).to_dataset().to_netcdf(path)
            rule=dict(inputs=[dict(path=str(root),pattern=r'pfull_2001-2001\.nc')],
                      ps_path=str(root),ps_pattern=r'sp_2001-2001\.nc')
            doc=dict(inherit={},rules=[rule])
            dependencies=configure(doc,2000,2001)
            self.assertEqual({p.name for p in dependencies},
                             {f'{name}_{y}-{y}.nc' for name in ('pfull','sp') for y in (2000,2001)})
            import re
            self.assertIsNone(re.fullmatch(rule['ps_pattern'],'sp_2002-2002.nc'))
            (root/'sp_2001-2001.nc').unlink()
            with self.assertRaisesRegex(ValueError,'INCOMPLETE|MISSING'):
                configure(dict(inherit={},rules=[dict(inputs=[dict(path=str(root),pattern=r'pfull_2001-2001\.nc')],
                              ps_path=str(root),ps_pattern=r'sp_2001-2001\.nc')]),2000,2001)
