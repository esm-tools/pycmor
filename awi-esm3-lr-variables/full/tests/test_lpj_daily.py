import calendar
import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import numpy as np
import pandas as pd
import xarray as xr

PATH=Path(__file__).resolve().parents[1]/'rules/lpj_daily.py'
SPEC=importlib.util.spec_from_file_location('tested_lpj_daily',PATH)
DAILY=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DAILY)


class Rule(SimpleNamespace):
    def get(self,key,default=None):
        return getattr(self,key,default)


class NativeDailyLandTests(unittest.TestCase):
    def convert(self, edit=None, footprint=False, scalar=False, frozen=False, monthly=False):
        with tempfile.TemporaryDirectory() as name:
            base=Path(name)
            rows=[]
            for day in range(1,367):
                for cell in range(2):
                    rows.append([12.115385+cell,45.350452,2000,day,day+cell,day+cell+2,day+cell+3])
            frame=pd.DataFrame(rows,columns=['Lon','Lat','Year','Day','Depth0.1','Depth0.2','Depth0.3'])
            if edit is not None:frame=edit(frame)
            if scalar:
                frame=frame.rename(columns={'Depth0.1':'Total'}).drop(columns=['Depth0.2','Depth0.3'])
            source=base/'mrsol_daily.out';frame.to_csv(source,sep=' ',index=False)
            grid=base/'grid.nc'
            xr.Dataset({'lon':('ncells',[12.11539,13.11539]+([14.11539] if footprint else [])),
                        'lat':('ncells',[45.35045]*(3 if footprint else 2))}).to_netcdf(grid)
            rule=Rule(lr_source_files=[str(source)],year_start=2000,year_end=2000,
                      lr_soil_layer_thickness_m=.1,grid_file=str(grid),
                      model_variable='mrsol',source_units='kg m-2')
            if frozen:
                liquid=base/'mrsll_daily.out';other=frame.copy()
                for col in [n for n in other if n.startswith('Depth')]:other[col]*=.25
                other.to_csv(liquid,sep=' ',index=False)
                rule.lr_soil_liquid_source_files=[str(liquid)]
            if monthly:rule.lr_native_monthly_mean=True
            if footprint:
                reference=base/'lai_monthly.out'
                frame[['Lon','Lat','Year']].drop_duplicates().to_csv(reference,sep=' ',index=False)
                rule.lr_lpj_footprint_files=[str(reference)]
            return DAILY.load_native_daily(None,rule)

    def test_leap_day_calendar_layer_bounds_and_raw_values(self):
        data=self.convert()
        self.assertEqual(data.sizes,dict(time=366,depth=3,ncells=2,bnds=2))
        self.assertEqual((data.time.values[59].month,data.time.values[59].day),(2,29))
        self.assertEqual((data.time.values[-1].month,data.time.values[-1].day),(12,31))
        np.testing.assert_allclose(data.depth,[.05,.15,.25])
        np.testing.assert_allclose(data.depth_bnds,[[0,.1],[.1,.2],[.2,.3]])
        self.assertEqual(float(data.mrsol[59,0,1]),61.)
        self.assertEqual(float(data.mrsol[-1,-1,0]),369.)

    def test_missing_cell_day_rejected(self):
        with self.assertRaisesRegex(ValueError,'Missing native daily'):
            self.convert(lambda f:f.iloc[:-1])

    def test_unsimulated_reference_geometry_cells_are_not_invented(self):
        data=self.convert(footprint=True)
        self.assertEqual(data.sizes['ncells'],2)
        self.assertTrue(np.isfinite(data.mrsol).all())

    def test_scalar_daily_diagnostic_preserves_leap_day_and_values(self):
        data=self.convert(scalar=True)
        self.assertEqual(data.mrsol.dims,('time','ncells'))
        self.assertEqual(float(data.mrsol[59,1]),61.)
        self.assertNotIn('depth',data)

    def test_frozen_mass_is_total_minus_all_liquid_water(self):
        data=self.convert(frozen=True)
        self.assertEqual(float(data.mrsol[59,0,0]),45.)
        self.assertEqual(float(data.mrsol[59,0,1]),45.75)

    def test_monthly_frozen_mass_uses_every_day_including_leap_february(self):
        data=self.convert(frozen=True,monthly=True)
        self.assertEqual(data.sizes['time'],12)
        self.assertEqual(float(data.mrsol[0,0,0]),12.)
        self.assertEqual(float(data.mrsol[1,0,0]),34.5)
        self.assertEqual(data.time.values[1].month,2)
        np.testing.assert_allclose(data.depth_bnds,[[0,.1],[.1,.2],[.2,.3]])

    def test_duplicate_cell_day_rejected(self):
        with self.assertRaisesRegex(ValueError,'Duplicate native cell/day'):
            self.convert(lambda f:pd.concat([f,f.iloc[[0]]]))

    def test_impossible_day_rejected(self):
        def edit(frame):
            frame.loc[0,'Day']=367
            return frame
        with self.assertRaisesRegex(ValueError,'Invalid native day'):
            self.convert(edit)

    def test_grid_mismatch_rejected_without_regridding(self):
        def edit(frame):
            frame.loc[0,'Lat']+=.1
            return frame
        with self.assertRaisesRegex(ValueError,'coordinates do not match'):
            self.convert(edit)
