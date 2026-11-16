"""Corrected and added LR ocean/sea-ice recipes (gap closure, package P3)."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import numpy as np
import xarray as xr
import yaml

from scientific_review import review_document


LR = Path(__file__).resolve().parents[2]
REPO = LR.parent
SPEC = importlib.util.spec_from_file_location('gap_custom_steps', REPO/'examples/custom_steps.py')
CUSTOM = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CUSTOM)
REVIEW = json.loads((LR/'full/config/scientific_review.json').read_text())
PROFILE = 'awiesm3-v3.5.2-cc-94377dfc'
IDENTITY = dict(fesom_sha256=REVIEW['compatible_profiles'][PROFILE]['fesom_sha256'], vector_frame='geographic')
CONFIG = dict(scientific_profile=PROFILE)


class Rule(SimpleNamespace):
    def get(self, key, default=None):
        return getattr(self, key, default)


def catalogue_rule(compound):
    found = [(path, r) for path in sorted(LR.glob('*/cmip7xml*.yaml'))
             for r in yaml.safe_load(path.read_text())['rules'] if r['compound_name'] == compound]
    if len(found) != 1:
        raise AssertionError(f'{compound}: expected one catalogue rule, found {len(found)}')
    return found[0]


def reviewed(compound):
    path, rule = catalogue_rule(compound)
    doc = yaml.safe_load(path.read_text())
    one = dict(doc, rules=[copy.deepcopy(rule)])
    return review_document(one, IDENTITY, CONFIG)['rules'][0]


class ReviewRepairTests(unittest.TestCase):
    def test_repair_releases_only_the_route_that_declares_it(self):
        compound = 'ocean.volcello.tavg-ol-hxy-sea.mon.glb'
        rule = reviewed(compound)
        self.assertEqual(rule['lr_scientific_review'][0]['action'], 'repaired_recipe')
        self.assertIn('nod_area', rule['lr_scientific_review'][0]['repair'])
        with self.assertRaisesRegex(ValueError, 'SCIENTIFIC_MAPPING_DISABLED: F34'):
            review_document({'rules': [dict(compound_name=compound)]}, IDENTITY, CONFIG)

    def test_finding_text_is_kept(self):
        finding = next(f for f in REVIEW['findings'] if f['id'] == 'F34')
        self.assertEqual(finding['action'], 'blocked_invalid_mapping')
        self.assertIn('4.7068%', finding['detail'])
        self.assertEqual(set(finding['repaired']), set(finding['products']))


def mesh_files(tmp):
    """Two nodes, three layers; node 1 is an ice-shelf cavity starting at layer 1."""
    mesh = xr.Dataset({
        'cell_area': ('ncells', [10., 20.]),
        'depth_bnds': ('nlev_bnds', [0., 10., 30., 60.]),
        'depth': ('nlev', [5., 20., 45.]),
        'depth_lev': ('ncells', [3, 3]),
        'cav_nod_lev': ('ncells', [0., 1.]),
        'cav_nod_mask': ('ncells', [0., 1.]),
        'lat': ('ncells', [-50., -75.]), 'lon': ('ncells', [0., 10.]),
    })
    diag = xr.Dataset({'nod_area': (('nz', 'nod2'), [[9., 0.], [8., 18.], [7., 17.], [0., 0.]])})
    mesh.to_netcdf(Path(tmp)/'mesh.nc')
    diag.to_netcdf(Path(tmp)/'fesom.mesh.diag.nc')
    return Path(tmp)/'mesh.nc', Path(tmp)/'fesom.mesh.diag.nc'


class VolcelloTests(unittest.TestCase):
    def test_fx_uses_depth_dependent_area(self):
        with tempfile.TemporaryDirectory() as tmp:
            mesh, diag = mesh_files(tmp)
            rule = Rule(model_variable='volcello', depth_lev_counts_levels=True, volume_area_file=str(diag))
            out = CUSTOM.compute_volcello_fx(xr.open_dataset(mesh), rule)
            np.testing.assert_allclose(out.values, [[90., np.nan], [160., 360.], [210., 510.]])
            legacy = CUSTOM.compute_volcello_fx(xr.open_dataset(mesh), Rule(model_variable='volcello',
                                                                              depth_lev_counts_levels=True))
            np.testing.assert_allclose(legacy.values, [[100., np.nan], [200., 400.], [300., 600.]])

    def test_time_varying_uses_depth_dependent_area(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, diag = mesh_files(tmp)
            hnode = xr.DataArray([[[10., 20., 30.], [np.nan, 20., 30.]]], dims=('time', 'nod2', 'nz'),
                                 coords={'time': [0]}, name='hnode', attrs={'units': 'm'})
            out = CUSTOM.compute_volcello_time(hnode, Rule(volume_area_file=str(diag)))
            self.assertEqual(out.dims, ('time', 'nod2', 'nz'))
            np.testing.assert_allclose(out.values, [[[90., 160., 210.], [np.nan, 360., 510.]]])
            self.assertEqual(out.attrs['units'], 'm3')

    def test_catalogue_rules(self):
        for compound in ('ocean.volcello.ti-ol-hxy-sea.fx.glb', 'ocean.volcello.tavg-ol-hxy-sea.mon.glb',
                         'ocean.volcello.tavg-ol-hxy-sea.yr.glb', 'ocean.volcello.tavg-ol-hxy-sea.dec.glb'):
            with self.subTest(compound=compound):
                _, rule = catalogue_rule(compound)
                self.assertTrue(rule['volume_area_file'].endswith('fesom.mesh.diag.nc'))
                if compound.split('.')[-2] != 'yr':
                    self.assertIn('repaired_recipe', {r['action'] for r in reviewed(compound)['lr_scientific_review']})


if __name__ == '__main__':
    unittest.main()
