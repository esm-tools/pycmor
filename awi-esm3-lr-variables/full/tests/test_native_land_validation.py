import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import netCDF4
import numpy as np
import pandas as pd

PATH=Path(__file__).resolve().parents[1]/'rules/validate_product.py'
SPEC=importlib.util.spec_from_file_location('independent_native_land',PATH)
CHECK=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(CHECK)


class NativeLandValidationTests(unittest.TestCase):
    def test_samples_real_land_cells_and_checks_every_unsimulated_cell_mask(self):
        frame=pd.DataFrame(dict(Lon=[12.115385,13.115385],Lat=[45.350452]*2,
            lon_key=[12.115,13.115],lat_key=[45.350]*2,Day=[1,1],Depth01=[10.,20.]))
        frame=frame.set_index(['lon_key','lat_key','Day'])
        rule={'lr_source_files':['fixture.out']}
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'expanded.nc'
            with netCDF4.Dataset(path,'w') as output:
                output.createDimension('time',2);output.createDimension('cell',4)
                output.createVariable('longitude','f8',('cell',))[:]=[0,12.11539,15,13.11539]
                output.createVariable('latitude','f8',('cell',))[:]=[0,45.35045,0,45.35045]
                field=output.createVariable('mrsol','f4',('time','cell'),fill_value=1e20)
                values=np.ma.masked_all((2,4));values[:,1]=10;values[:,3]=20;field[:]=values
                with patch.object(CHECK,'native_daily_table',return_value=frame):
                    cells=CHECK.native_output_cells(rule,2000,output)
                np.testing.assert_array_equal(cells,[1,3])
                CHECK.verify_unsimulated_mask(output,field,cells)
                field[1,0]=0.
                with self.assertRaisesRegex(AssertionError,'outside simulated'):
                    CHECK.verify_unsimulated_mask(output,field,cells)
                field[:]=values;field[1,1]=np.ma.masked
                with self.assertRaisesRegex(AssertionError,'lost during grid expansion'):
                    CHECK.verify_unsimulated_mask(output,field,cells)

    def test_raw_sample_lookup_uses_calendar_day_and_layer(self):
        frame=pd.DataFrame(dict(Lon=[12.11549998],Lat=[45.350452],lon_key=[12.115],
            lat_key=[45.350],Day=[60],Depth01=[23.])).set_index(['lon_key','lat_key','Day'])
        frame=frame.rename(columns={'Depth01':'Depth0.1'})
        with patch.object(CHECK,'native_daily_table',return_value=frame):
            value=CHECK.sample_lpj(dict(lr_lpj_daily=True,lr_source_files=['fixture.out']),
                2000,2,12.1155,45.35045,day=29)
        self.assertEqual(value,23.)
