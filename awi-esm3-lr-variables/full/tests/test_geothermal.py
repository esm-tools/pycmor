import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import numpy as np
import xarray as xr

PATH=Path(__file__).resolve().parents[1]/'rules/geothermal.py'
SPEC=importlib.util.spec_from_file_location('tested_geothermal',PATH)
GEO=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(GEO)


class Rule(SimpleNamespace):
    def get(self,key,default=None): return getattr(self,key,default)


class GeothermalTests(unittest.TestCase):
    def test_unreviewed_boundary_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'UNREVIEWED'):
            GEO.verify_context(None)

    def test_reviewed_boundary_is_zero_only_in_wet_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); source=root/'source.F90';source.write_text('reviewed source')
            evidence=root/'evidence.json'
            evidence.write_text(json.dumps(dict(represented_flux_W_m2=0.,binary_sha256='abc',
                conclusion='NO_GEOTHERMAL_SOURCE_ZERO_BOTTOM_HEAT_BOUNDARY',
                source_files=[dict(path=str(source),sha256=hashlib.sha256(source.read_bytes()).hexdigest())])))
            grid=root/'grid.nc'
            xr.Dataset(dict(depth_lev=('nod2',[3,0,5]),lon=('nod2',[1,2,3]),lat=('nod2',[4,5,6]))).to_netcdf(grid)
            rule=Rule(grid_file=str(grid),model_variable='hfgeou',lr_geothermal_evidence_file=str(evidence),
                      lr_geothermal_binary_sha256='abc')
            data=GEO.load_zero_boundary(None,rule)
            np.testing.assert_equal(data.values,[0.,np.nan,0.])
            rule.lr_geothermal_binary_sha256='other'
            with self.assertRaisesRegex(ValueError,'does not establish'):
                GEO.load_zero_boundary(None,rule)
            rule.lr_geothermal_binary_sha256='abc';source.write_text('changed')
            with self.assertRaisesRegex(ValueError,'evidence changed'):
                GEO.load_zero_boundary(None,rule)
