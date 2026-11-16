"""Small non-submitting tests for native iceberg freshwater CMORization."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

import cftime
import numpy as np
import xarray as xr
import yaml


ROOT = Path(__file__).resolve().parents[1]


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


steps = load(ROOT / 'rules/iceberg_steps.py', 'test_iceberg_steps')
support = load(ROOT / 'scripts/iceberg_support.py', 'test_iceberg_support')
audit = load(ROOT / 'rules/iceberg_validation.py', 'test_iceberg_validation')


class Rule(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


class Tests(unittest.TestCase):
    def arrays(self):
        arrays = {}
        for i, name in enumerate(steps.TERMS):
            arrays[name] = xr.DataArray(
                [[(i + 1) * 1e-6, -(i + 1) * 1e-7, np.nan]],
                dims=('time', 'nod2'), coords={'time': [cftime.DatetimeGregorian(2605, 1, 16)],
                    'lon': ('nod2', [10., 20., 30.]), 'lat': ('nod2', [0., 10., -80.])},
                attrs={'units': 'm s-1'})
        return arrays

    def grid(self):
        return np.array([10., 20., 30.]), np.array([0., 10., -80.]), np.array([False, False, True])

    def test_signed_sum_density_and_cavity_zero(self):
        result = steps.combine_surface_terms(self.arrays(), self.grid())
        np.testing.assert_allclose(result.values, [[.01, -.001, 0.]])
        self.assertEqual(result.attrs['units'], 'kg m-2 s-1')
        self.assertEqual(result.attrs['positive'], 'down')

    def test_exposed_missing_not_invented_as_zero(self):
        arrays = self.arrays()
        arrays['ibfwl'][0, 0] = np.nan
        with self.assertRaisesRegex(ValueError, 'ICEBERG_MISSING_OCEAN'):
            steps.combine_surface_terms(arrays, self.grid())

    def test_nonzero_cavity_flux_rejected(self):
        arrays = self.arrays()
        arrays['ibfwb'][0, 2] = 1e-8
        with self.assertRaisesRegex(ValueError, 'ICEBERG_CAVITY_FLUX'):
            steps.combine_surface_terms(arrays, self.grid())

    def test_wrong_units_rejected(self):
        arrays = self.arrays()
        arrays['ibfwb'].attrs['units'] = 'kg m-2 s-1'
        with self.assertRaisesRegex(ValueError, 'ICEBERG_UNITS'):
            steps.combine_surface_terms(arrays, self.grid())

    def test_wrong_node_order_rejected(self):
        arrays = self.arrays()
        arrays['ibfwb'] = arrays['ibfwb'].isel(nod2=[1, 0, 2])
        with self.assertRaisesRegex(ValueError, 'ICEBERG_GRID'):
            steps.combine_surface_terms(arrays, self.grid())

    def test_different_month_timestamps_rejected(self):
        arrays = self.arrays()
        arrays['ibfwe'] = arrays['ibfwe'].assign_coords(time=[cftime.DatetimeGregorian(2605, 1, 17)])
        with self.assertRaisesRegex(ValueError, 'ICEBERG_ALIGNMENT'):
            steps.combine_surface_terms(arrays, self.grid())

    def test_surface_and_layer_rules_share_terms_and_no_3hr_product(self):
        rules = {r['compound_name']: r for r in support.rules(ROOT / 'rules')}
        self.assertEqual(set(rules), {'ocean.ficeberg.tavg-u-hxy-sea.mon.glb',
                                      'ocean.ficeberg.tavg-ol-hxy-sea.mon.glb'})
        layers = rules['ocean.ficeberg.tavg-ol-hxy-sea.mon.glb']
        self.assertTrue(layers['lr_iceberg_layers'] and layers['lr_exact_ocean_depth'])
        self.assertEqual(layers['pipelines'], ['lr_iceberg_layers'])
        for rule in rules.values():
            self.assertEqual({t['variable'] for t in rule['lr_terms']}, set(steps.TERMS))

    def fixture(self, directory, layers=False, lat_shift=0.):
        base = Path(directory)
        lon, lat, cavity = self.grid()
        grid = base / 'grid.nc'
        mesh = xr.Dataset({'lon': ('nod2', lon), 'lat': ('nod2', lat),
                           'cav_nod_mask': ('nod2', cavity.astype(int))})
        # Node 1 has four wet layers, node 0 three; the cavity node starts at layer 1.
        mesh['depth'] = ('nz1', [5., 15., 30., 50.])
        mesh['depth_lev'] = ('nod2', [3, 4, 4])
        mesh['cav_nod_lev'] = ('nod2', [0, 0, 1])
        mesh.to_netcdf(grid)
        starts = [cftime.DatetimeGregorian(2605, m, 1) for m in range(1, 13)]
        ends = starts[1:] + [cftime.DatetimeGregorian(2606, 1, 1)]
        mid = [a + (b-a)/2 for a, b in zip(starts, ends)]
        units = 'days since 2605-01-01'
        rule = Rule(list(support.rules(ROOT / 'rules'))[int(layers)])
        rule.update(year_start=2605, year_end=2605, grid_file=str(grid))
        source_lat = (lat + lat_shift).astype('float32')
        for term in rule['lr_terms']:
            name = term['variable']
            path = base / f'{name}.fesom.2605.nc'
            data = xr.Dataset({name: (('time', 'nod2'), np.tile([1e-6, -1e-7, np.nan], (12, 1))),
                    'time_bounds': (('time', 'bnds'), cftime.date2num(np.array(list(zip(starts, ends))), units))},
                    coords={'time': cftime.date2num(mid, units), 'lat': ('nod2', source_lat),
                            'lon': ('nod2', lon.astype('float32'))})
            data[name].attrs['units'] = 'm s-1'
            data.time.attrs.update(units=units, calendar='gregorian', bounds='time_bounds')
            data.to_netcdf(path)
            term['files'] = [str(path)]
        return rule

    def test_planner_and_worker_accept_complete_exact_months(self):
        with tempfile.TemporaryDirectory() as directory:
            rule = self.fixture(directory)
            support.validate_rule(rule)
            result = steps.load_surface_freshwater(None, rule)
            self.assertEqual(result.shape, (12, 3))
            np.testing.assert_allclose(result.values, np.tile([.004, -.0004, 0.], (12, 1)))

    def test_partial_month_bounds_rejected(self):
        import netCDF4
        with tempfile.TemporaryDirectory() as directory:
            rule = self.fixture(directory)
            with netCDF4.Dataset(rule['lr_terms'][1]['files'][0], 'r+') as ds:
                ds['time_bounds'][0, 1] -= 1
            with self.assertRaisesRegex(ValueError, 'ICEBERG_TIME'):
                support.validate_rule(rule)

    def test_independent_sampler_preserves_negative_and_cavity_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            rule = self.fixture(directory)
            self.assertAlmostEqual(audit.sample_freshwater(rule, 2605, 1, 0), .004)
            self.assertAlmostEqual(audit.sample_freshwater(rule, 2605, 1, 1), -.0004)
            self.assertEqual(audit.sample_freshwater(rule, 2605, 1, 2), 0.)

    def test_independent_audit_rejects_clipped_freezing_and_cavity_missing(self):
        import netCDF4
        with tempfile.TemporaryDirectory() as directory:
            rule = self.fixture(directory)
            source = steps.load_surface_freshwater(None, rule).rename('ficeberg')
            output = Path(directory) / 'output.nc'
            source.to_netcdf(output)
            with netCDF4.Dataset(output) as ds:
                dates = netCDF4.num2date(ds['time'][:], ds['time'].units, ds['time'].calendar)
                checked = audit.validate_output(rule, ds, ds['ficeberg'], dates)
                self.assertEqual(checked['months'][0]['negative_freezing_nodes'], 1)
                self.assertEqual(checked['months'][0]['cavity_zero_nodes'], 1)
            with netCDF4.Dataset(output, 'r+') as ds:
                ds['ficeberg'][0, 1] = 0
            with netCDF4.Dataset(output) as ds:
                with self.assertRaisesRegex(ValueError, 'independent four-term sum'):
                    audit.validate_output(rule, ds, ds['ficeberg'], dates)
            with netCDF4.Dataset(output, 'r+') as ds:
                ds['ficeberg'][0, 1] = -.0004
                ds['ficeberg'][0, 2] = np.nan
            with netCDF4.Dataset(output) as ds:
                with self.assertRaisesRegex(ValueError, 'masked or invalid'):
                    audit.validate_output(rule, ds, ds['ficeberg'], dates)

    def test_mask_is_not_inferred(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'grid.nc'
            xr.Dataset({'lat': ('nod2', [0.]), 'lon': ('nod2', [0.])}).to_netcdf(path)
            with self.assertRaisesRegex(ValueError, 'ICEBERG_GRID'):
                steps.grid_mask(path)

    def test_grid_lon_lat_may_be_coordinates(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'grid.nc'
            xr.Dataset({'cav_nod_mask': ('nod2', [0., 1.])},
                       coords={'lat': ('nod2', [0., -80.]), 'lon': ('nod2', [1., 2.])}).to_netcdf(path)
            np.testing.assert_array_equal(steps.grid_mask(path)[2], [False, True])

    def test_pipeline_is_separate_and_reproducible(self):
        profile = yaml.safe_load((ROOT.parent / 'profile.yaml').read_text())
        support.install_pipeline(profile, Path('/campaign/reproduce'))
        pipeline = next(p for p in profile['pipelines'] if p['name'] == 'lr_iceberg_surface')
        self.assertEqual(pipeline['steps'][0], 'script:///campaign/reproduce/iceberg_steps.py:load_surface_freshwater')
        pipeline = next(p for p in profile['pipelines'] if p['name'] == 'lr_iceberg_layers')
        self.assertEqual(pipeline['steps'][0], 'script:///campaign/reproduce/iceberg_steps.py:load_layer_freshwater')
        with self.assertRaisesRegex(ValueError, 'twice'):
            support.install_pipeline(profile, Path('/campaign/reproduce'))

    def test_float32_polar_coordinates_accepted_but_node_order_strict(self):
        # CORE3 pole node: mesh 89.95876171, XIOS float32 89.955765, lon -4.0300 vs -4.0358.
        mesh_lon, mesh_lat = np.array([-4.03000535, 20.]), np.array([89.95876171, 10.])
        xios_lon, xios_lat = np.array([-4.0357566, 20.], 'f4'), np.array([89.955765, 10.], 'f4')
        self.assertFalse(np.allclose(xios_lat, mesh_lat, rtol=0, atol=1e-4))
        for module in (support, steps):
            self.assertLess(module.node_offset(xios_lon, xios_lat, mesh_lon, mesh_lat).max(), 0.0031)
        self.assertTrue(support.same_nodes(xios_lon, xios_lat, mesh_lon, mesh_lat))
        self.assertTrue(audit._same_nodes(xios_lon, xios_lat, mesh_lon, mesh_lat))
        self.assertFalse(support.same_nodes(xios_lon[::-1], xios_lat[::-1], mesh_lon, mesh_lat))
        self.assertFalse(audit._same_nodes(xios_lon[::-1], xios_lat[::-1], mesh_lon, mesh_lat))
        # Neighbouring CORE3 nodes are at least 0.069 deg apart.
        self.assertFalse(support.same_nodes(mesh_lon, mesh_lat + 0.069, mesh_lon, mesh_lat))

    def test_planner_worker_and_audit_accept_float32_offsets(self):
        with tempfile.TemporaryDirectory() as directory:
            rule = self.fixture(directory, lat_shift=0.003)
            support.validate_rule(rule)
            self.assertEqual(steps.load_surface_freshwater(None, rule).shape, (12, 3))
            self.assertAlmostEqual(audit.sample_freshwater(rule, 2605, 1, 0), .004)

    def test_shifted_node_rejected_everywhere(self):
        with tempfile.TemporaryDirectory() as directory:
            rule = self.fixture(directory, lat_shift=0.05)
            with self.assertRaisesRegex(ValueError, 'ICEBERG_GRID'):
                support.validate_rule(rule)
            with self.assertRaisesRegex(ValueError, 'ICEBERG_GRID'):
                steps.load_surface_freshwater(None, rule)
            with self.assertRaisesRegex(ValueError, 'ICEBERG_QC'):
                audit.sample_freshwater(rule, 2605, 1, 0)

    def test_layer_product_puts_column_flux_in_top_wet_layer(self):
        with tempfile.TemporaryDirectory() as directory:
            rule = self.fixture(directory, layers=True)
            support.validate_rule(rule)
            result = steps.load_layer_freshwater(None, rule)
            self.assertEqual(result.dims, ('time', 'nod2', 'nz'))
            np.testing.assert_array_equal(result.nz.values, [5., 15., 30., 50.])
            self.assertEqual(result.nz.attrs['standard_name'], 'depth')
            np.testing.assert_allclose(result.isel(time=0).values, [[.004, 0., 0., np.nan],
                                                                    [-.0004, 0., 0., 0.],
                                                                    [np.nan, 0., 0., 0.]])
            np.testing.assert_allclose(result.sum('nz').values, np.tile([.004, -.0004, 0.], (12, 1)))
            self.assertEqual(result.attrs['units'], 'kg m-2 s-1')
            self.assertAlmostEqual(audit.sample_freshwater(rule, 2605, 1, 1, 0), -.0004)
            self.assertEqual(audit.sample_freshwater(rule, 2605, 1, 1, 2), 0.)
            self.assertIs(audit.sample_freshwater(rule, 2605, 1, 0, 3), np.ma.masked)
            self.assertIs(audit.sample_freshwater(rule, 2605, 1, 2, 0), np.ma.masked)

    def test_layer_audit_rejects_flux_below_top_or_filled_dry_layer(self):
        import netCDF4
        with tempfile.TemporaryDirectory() as directory:
            rule = self.fixture(directory, layers=True)
            output = Path(directory) / 'output.nc'
            steps.load_layer_freshwater(None, rule).rename('ficeberg').to_netcdf(output)
            with netCDF4.Dataset(output) as ds:
                dates = netCDF4.num2date(ds['time'][:], ds['time'].units, ds['time'].calendar)
                self.assertIn('top wet layer', audit.validate_output(rule, ds, ds['ficeberg'], dates)['check'])
            for node, level, value, message in ((1, 2, 1e-9, 'below the top wet layer'),
                                                (0, 3, 0., 'dry layers filled')):
                with netCDF4.Dataset(output, 'r+') as ds:
                    original = ds['ficeberg'][0, node, level]
                    ds['ficeberg'][0, node, level] = value
                with netCDF4.Dataset(output) as ds:
                    with self.assertRaisesRegex(ValueError, message):
                        audit.validate_output(rule, ds, ds['ficeberg'], dates)
                with netCDF4.Dataset(output, 'r+') as ds:
                    ds['ficeberg'][0, node, level] = original


if __name__ == '__main__':
    unittest.main()
