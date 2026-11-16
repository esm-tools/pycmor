import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import cftime
import numpy as np
import xarray as xr

from pycmor.std_lib.timeaverage import _duration_weighted_time_mean, timeavg


SPEC=importlib.util.spec_from_file_location('scientific_custom_steps',Path(__file__).resolve().parents[3]/'examples/custom_steps.py')
CUSTOM=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CUSTOM)


class Rule(SimpleNamespace):
    def get(self,key,default=None):return getattr(self,key,default)


class ScientificRepairsTests(unittest.TestCase):
    def test_real_positive_deposition_and_missing_values_are_not_erased(self):
        data=xr.Dataset({'deposition':('cell',[4e-14,5e-11,np.nan,-1e-12])})
        native=CUSTOM.clip_small_negatives(data.copy(),Rule(preserve_native_small_values=True))
        xr.testing.assert_identical(native,data)
        cleaned=CUSTOM.clip_small_negatives(data.copy(),Rule())
        np.testing.assert_allclose(cleaned.deposition[:2],[4e-14,5e-11])
        self.assertTrue(np.isnan(cleaned.deposition[2]))
        self.assertEqual(float(cleaned.deposition[3]),0.)

    def test_water_dewpoint_does_not_become_ice_frostpoint(self):
        air=xr.DataArray([263.15,263.15],dims='cell')
        dew=xr.DataArray([258.15,263.15],dims='cell')
        with patch.object(CUSTOM,'_load_secondary_mf',return_value=dew):
            value=CUSTOM.compute_hurs(air,Rule(model_variable='hurs'))
        self.assertGreater(float(value[0]),73.)
        self.assertLess(float(value[0]),75.)
        self.assertGreater(float(value[1]),100.)

    def test_ice_drag_constant_preserves_its_physical_domain(self):
        data=xr.DataArray([0.,.2,1.,np.nan],dims='cell')
        value=CUSTOM.compute_constant_field(data,Rule(constant_value=.0055,constant_where_positive=True))
        np.testing.assert_allclose(value.values,[np.nan,.0055,.0055,np.nan],equal_nan=True)

    def test_annual_month_weights_include_february_29_and_preserve_missing_cells(self):
        times=[cftime.DatetimeGregorian(y,m,15) for y in (2000,2001) for m in range(1,13)]
        data=xr.DataArray(np.zeros((24,2)),dims=('time','cell'),coords={'time':times},attrs={'units':'W m-2'})
        data[1,0]=100;data[13,0]=100;data[:,1]=np.nan
        result=_duration_weighted_time_mean(data,'YS')
        np.testing.assert_allclose(result[:,0],[2900/366,2800/365])
        self.assertTrue(np.isnan(result[:,1]).all())
        self.assertEqual(result.attrs['units'],'W m-2')

    def test_public_timeavg_reduces_hourly_values_before_daily_output(self):
        from datetime import timedelta
        dates=[cftime.DatetimeGregorian(2000,2,28)+timedelta(hours=i,minutes=30) for i in range(48)]
        data=xr.DataArray(np.tile([1.,3.],24),dims='time',coords={'time':dates})
        drv=SimpleNamespace(frequency='day',cell_methods='time: mean',
                            table_header=SimpleNamespace(approx_interval=1.))
        result=timeavg(data,Rule(cmor_variable='sfcWind',data_request_variable=drv,
            duration_weighted_annual_mean=True))
        np.testing.assert_allclose(result,[2.,2.])
        self.assertEqual((result.time.values[-1].month,result.time.values[-1].day),(2,29))

    def test_public_timeavg_applies_month_weights_and_leap_year_midpoint(self):
        data=xr.DataArray([0.,100.]+[0.]*10,dims='time',
            coords={'time':[cftime.DatetimeGregorian(2000,m,15) for m in range(1,13)]},
            name='Kv',attrs={'units':'m2 s-1'})
        drv=SimpleNamespace(frequency='yr',cell_methods='time: mean',
                            table_header=SimpleNamespace(approx_interval=365.))
        result=timeavg(data,Rule(cmor_variable='difvho',data_request_variable=drv,
            duration_weighted_annual_mean=True))
        self.assertAlmostEqual(float(result[0]),2900/366)
        self.assertEqual(result.name,'Kv')
        self.assertEqual(result.attrs['units'],'m2 s-1')
        midpoint=cftime.DatetimeGregorian(2000,1,1)+(cftime.DatetimeGregorian(2001,1,1)-cftime.DatetimeGregorian(2000,1,1))/2
        self.assertEqual(result.time.values[0],midpoint)
