"""Daily filename suffixes do not change the physical variable names."""
from pathlib import Path
import re
from types import SimpleNamespace
from unittest import TestCase

import numpy as np
import xarray as xr
import yaml

from pycmor.std_lib.generic import get_variable


class NativeDailyVariableTests(TestCase):
    def test_soil_proxy_removed_but_groundwater_request_remains_explained(self):
        lr=Path(__file__).resolve().parents[2]
        compound='land.dgw.tavg-u-hxy-lnd.day.glb'
        native=yaml.safe_load((lr/'veg_land/cmip7xml_awiesm3-lr_land.yaml').read_text())
        self.assertNotIn(compound,{r['compound_name'] for r in native['rules']})
        catalogue=yaml.safe_load((lr/'veg_land/cmip7_awiesm3-lr_veg_land.yaml').read_text())
        row=next(r for r in catalogue['not_enabled'] if r['compound_name']==compound)
        self.assertIn('GROUNDWATER_MAPPING',row['reason'])

    def test_daily_rules_select_archived_variable_names(self):
        lr=Path(__file__).resolve().parents[2]
        cases=(('lrcs_ocean','hfx_int_day','utemp'),
               ('lrcs_ocean','hfy_int_day','vtemp'),
               ('lrcs_seaice','sivol_north_day','m_ice'),
               ('lrcs_seaice','sivol_south_day','m_ice'))
        for tier,name,variable in cases:
            with self.subTest(rule=name):
                path=lr/tier/f'cmip7xml_awiesm3-lr_{tier}.yaml'
                doc=yaml.safe_load(path.read_text())
                rule=next(r for r in doc['rules'] if r['name']==name)
                self.assertIsNotNone(re.fullmatch(rule['inputs'][0]['pattern'],
                                                  f'{variable}_day.fesom.2001.nc'))
                self.assertIsNone(re.fullmatch(rule['inputs'][0]['pattern'],
                                               f'{variable}.fesom.2001.nc'))
                data=xr.Dataset({variable:(('time','cell'),np.array([[1.,2.],[3.,4.]]))})
                result=get_variable(data,SimpleNamespace(model_variable=rule['model_variable']))
                xr.testing.assert_equal(result,data[variable])
