"""Corrected and added LR ocean/sea-ice recipes (gap closure, package P3)."""
import copy
from datetime import timedelta
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import cftime
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


def daily_and_monthly(year=2001):
    days = [cftime.DatetimeGregorian(year, 1, 1, 12) + timedelta(days=i) for i in range(59)]
    months = [cftime.DatetimeGregorian(year, 1, 16, 12), cftime.DatetimeGregorian(year, 2, 15)]
    return days, months


class SeaIceConcentrationTests(unittest.TestCase):
    def setUp(self):
        days, months = daily_and_monthly()
        # node 0: ice only in January (A=0.5 for 31 days); node 1: never ice
        aice = np.zeros((59, 2)); aice[:31, 0] = 0.5
        self.aice = xr.DataArray(aice, dims=('time', 'nod2'), coords={'time': days}, name='a_ice')
        self.monthly = lambda v: xr.DataArray(v, dims=('time', 'nod2'), coords={'time': months}, name='x',
                                              attrs={'units': '1'})

    def run_step(self, data, **rule):
        with patch.object(CUSTOM, '_load_secondary_mf', return_value=self.aice):
            return CUSTOM.combine_with_ice_concentration(data, Rule(**rule))

    def test_pond_fraction_of_ice_area(self):
        out = self.run_step(self.monthly([[0.1, 0.0], [0.0, 0.0]]), ice_concentration_operation='divide')
        np.testing.assert_allclose(out.values, [[0.2, np.nan], [np.nan, np.nan]])
        self.assertEqual(list(out.time.values), list(self.monthly([[0, 0], [0, 0]]).time.values))

    def test_snowfall_onto_ice_is_weighted_and_masked(self):
        out = self.run_step(self.monthly([[2e-8, 2e-8], [2e-8, 2e-8]]), ice_concentration_operation='multiply',
                            ice_concentration_mask=True)
        np.testing.assert_allclose(out.values, [[1e-8, np.nan], [np.nan, np.nan]])

    def test_missing_concentration_record_fails(self):
        with self.assertRaisesRegex(ValueError, 'No sea-ice concentration'):
            self.aice = self.aice.isel(time=slice(0, 31))
            self.run_step(self.monthly([[1., 1.], [1., 1.]]), ice_concentration_operation='divide')

    def test_snow_heat_content_uses_model_snow_density(self):
        m_snow = xr.DataArray([[0.1, 0.0]], dims=('time', 'nod2'), name='m_snow')
        out = CUSTOM.compute_sisnhc(m_snow, Rule(rho_snow=290., L_f=334000., model_variable='sisnhc'))
        np.testing.assert_allclose(out.values, [[-0.1 * 290 * 334000., 0.]])

    def test_ice_heat_content_scales_with_grid_cell_ice_volume(self):
        m_ice = xr.DataArray([[1.0, 0.0]], dims=('time', 'nod2'))
        ist = xr.DataArray([[263.15, 271.]], dims=('time', 'nod2'))
        sss = xr.DataArray([[34., 34.]], dims=('time', 'nod2'))
        with patch.object(CUSTOM, '_load_secondary_mf', side_effect=[ist, sss]):
            out = CUSTOM.compute_sihc(m_ice, Rule(model_variable='sihc'))
        t_mean = (263.15 + 273.15 - 0.054 * 34) / 2
        np.testing.assert_allclose(out.values, [[910. * (2090. * (t_mean - 273.15) - 334000.), 0.]])

    def test_catalogue_routes_are_released(self):
        expected = {'seaIce.sihc.tavg-u-hxy-sea.mon.glb': ('F14', 'm_ice_day'),
                    'seaIce.sisnhc.tavg-u-hxy-si.day.glb': ('F10', 'm_snow'),
                    'seaIce.simpconc.tavg-u-hxy-si.mon.glb': ('F21', 'apnd'),
                    'seaIce.prsn.tavg-u-hxy-si.mon.glb': ('F56', 'snow')}
        for compound, (finding, stream) in expected.items():
            with self.subTest(compound=compound):
                rule = reviewed(compound)
                self.assertIn(stream, rule['inputs'][0]['pattern'])
                actions = {r['id']: r['action'] for r in rule['lr_scientific_review']}
                self.assertEqual(actions[finding], 'repaired_recipe')
        with self.assertRaisesRegex(ValueError, 'F21'):
            review_document({'rules': [dict(compound_name='seaIce.simpeffconc.tavg-u-hxy-si.mon.glb')]},
                            IDENTITY, CONFIG)


class IceShelfTests(unittest.TestCase):
    def test_fixed_and_monthly_ice_shelf_percentage(self):
        with tempfile.TemporaryDirectory() as tmp:
            mesh, _ = mesh_files(tmp)
            rule = Rule(grid_file=str(mesh), model_variable='sftflf')
            fx = CUSTOM.compute_ice_shelf_area_percent(xr.open_dataset(mesh), rule)
            np.testing.assert_allclose(fx.values, [0., 100.])
            fw = xr.DataArray(np.ones((12, 2)), dims=('time', 'nod2'), name='fw')
            mon = CUSTOM.compute_ice_shelf_area_percent(fw, Rule(grid_file=str(mesh), model_variable='fw'))
            np.testing.assert_allclose(mon.values, np.tile([0., 100.], (12, 1)))
            self.assertEqual(mon.attrs['units'], '%')

    def test_inverted_or_inconsistent_mask_value_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            mesh, _ = mesh_files(tmp)
            with self.assertRaisesRegex(ValueError, 'does not mark Antarctic'):
                CUSTOM.compute_ice_shelf_area_percent(xr.open_dataset(mesh), Rule(grid_file=str(mesh),
                                                                                   cavity_mask_value=0))

    def test_land_ice_flux_only_on_cavity_nodes(self):
        with tempfile.TemporaryDirectory() as tmp:
            mesh, _ = mesh_files(tmp)
            fw = xr.DataArray([[1e-6, -5e-8]], dims=('time', 'nod2'), name='fw', attrs={'units': 'm s-1'})
            kept = CUSTOM.keep_ice_shelf_cavity_nodes(fw, Rule(grid_file=str(mesh)))
            out = CUSTOM.scale_by_constant(kept, Rule(scale_factor=-1000., scaled_units='kg m-2 s-1'))
            np.testing.assert_allclose(out.values, [[0., 5e-5]])

    def test_catalogue_rules(self):
        for compound, stream in (('landIce.sftflf.ti-u-hxy-u.fx.glb', 'mesh.nc'),
                                 ('landIce.sftflf.tavg-u-hxy-u.mon.glb', 'fw\\.fesom'),
                                 ('ocean.flandice.tavg-u-hxy-sea.mon.glb', 'fw\\.fesom'),
                                 ('ocean.flandice.tavg-u-hxy-sea.3hr.glb', 'fw_3h\\.fesom')):
            with self.subTest(compound=compound):
                rule = reviewed(compound)
                self.assertTrue(rule['inputs'][0]['pattern'].startswith(stream))


if __name__ == '__main__':
    unittest.main()
