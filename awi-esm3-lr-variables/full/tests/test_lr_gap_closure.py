"""Steps and rules added to close the LR atmosphere and land gaps."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest

import numpy as np
import xarray as xr


REPO=Path(__file__).resolve().parents[3]
SPEC=importlib.util.spec_from_file_location('gap_custom_steps',REPO/'examples/custom_steps.py')
CUSTOM=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CUSTOM)


class Rule(SimpleNamespace):
    def get(self,key,default=None):return getattr(self,key,default)


class RootDepthTests(unittest.TestCase):
    def test_deepest_rooted_layer_of_any_vegetated_tile(self):
        # cells: forest, desert type on the low tile only, bare land, ocean,
        # grass that appears in the second month, low tile with cover but no type
        months=lambda *v:xr.DataArray(np.array(v,dtype=float),dims=('time_counter','cell'))
        data=xr.Dataset(dict(
            tvl=months([2,8,0,0,0,0],[2,8,0,0,2,0]),cvl=months([.3,.5,0,0,0,.4],[.3,.5,0,0,.6,.4]),
            tvh=months([3,0,0,0,0,0],[3,0,0,0,0,0]),cvh=months([.6,0,0,0,0,0],[.6,0,0,0,0,0]),
            slt=months([2,1,1,0,3,2],[2,1,1,0,3,2])))
        out=CUSTOM.compute_rootd(data,Rule(model_variable='rootd')).rootd
        self.assertEqual(out.dims,('time_counter','cell'))
        np.testing.assert_allclose(out.isel(time_counter=0),[2.89,.07,.07,np.nan,2.89,.07])
        np.testing.assert_allclose(out.isel(time_counter=1),out.isel(time_counter=0))
        self.assertEqual(out.attrs['units'],'m')

    def test_type_change_within_a_month_is_not_mistaken_for_a_top_layer_type(self):
        data=xr.Dataset({k:('cell',np.array(v,dtype=float)) for k,v in dict(
            tvl=[8.72,8.0],cvl=[.5,.5],tvh=[0,0],cvh=[0,0]).items()})
        out=CUSTOM.compute_rootd(data,Rule(model_variable='rootd')).rootd
        np.testing.assert_allclose(out,[2.89,.07])


if __name__=='__main__':unittest.main()
