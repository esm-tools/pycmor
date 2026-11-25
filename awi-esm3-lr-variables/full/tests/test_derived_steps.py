"""Derived LR raw steps: fixed depths, column integrals, monthly means of daily
streams, lowest model level, and their independent validator sampling."""
from datetime import datetime, timedelta
import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import cftime
import netCDF4
import numpy as np
import xarray as xr
import yaml

RULES = Path(__file__).resolve().parents[1] / 'rules'


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, RULES / filename)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


RAW = module('derived_steps_adapter', 'raw_adapter.py')
EXTRA = module('derived_steps', 'extra_steps.py')
VALIDATOR = module('derived_steps_validator', 'validate_product.py')

# CORE3 interfaces from aux3d.out; layer centres are their midpoints.
EDGES = np.array([0, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 115, 135, 160, 190, 230, 280, 340, 410, 490,
                  580, 680, 790, 910, 1040, 1180, 1330, 1500, 1700, 1920, 2150, 2400, 2650, 2900, 3150, 3400,
                  3650, 3900, 4150, 4400, 4650, 4900, 5150, 5400, 5650, 6000, 6250], dtype=float)
CENTRES = 0.5 * (EDGES[:-1] + EDGES[1:])
CARBON = 12.011e-6


class Rule(SimpleNamespace):
    def get(self, key, default=None):
        return getattr(self, key, default)


def mesh(path):
    xr.Dataset({'depth': ('nz1', CENTRES), 'depth_bnds': ('nz', EDGES)}).to_netcdf(path)
    return str(path)


class PipelineTests(unittest.TestCase):
    def test_derived_pipelines_registered_once(self):
        RAW.REPO = RULES.parents[2]
        profile = yaml.safe_load((RULES.parents[1] / 'profile.yaml').read_text())
        RAW.register_daily_pipelines(profile, Path('/campaign'))
        before = len(profile['pipelines'])
        RAW.register_derived_pipelines(profile, Path('/campaign'))
        RAW.register_derived_pipelines(profile, Path('/campaign'))
        pipes = {p['name']: p['steps'] for p in profile['pipelines']}
        self.assertEqual(len(profile['pipelines']), before + 8)
        extra = 'script:///campaign/reproduce/extra_steps.py:'
        self.assertEqual(pipes['lr_diatom_limitation'][2], extra + 'diatom_limitation')
        self.assertIn(extra + 'transform_and_target_depth', pipes['lr_target_depth'])
        self.assertNotIn('pycmor.awiesm3_lr.transform', pipes['lr_target_depth'])
        self.assertIn('pycmor.awiesm3_lr.daily_time', pipes['lr_daily_target_depth'])
        self.assertEqual(pipes['lr_daily_target_depth_linear'][2], extra + 'target_depth_only')
        self.assertEqual(pipes['lr_south30_integral'][2], extra + 'vertical_integral')
        self.assertTrue(pipes['lr_south30_integral'][-3].endswith(':select_southern_hemisphere'))
        self.assertEqual(pipes['lr_daily_to_monthly'][:3], ['pycmor.awiesm3_lr.load_daily',
            extra + 'daily_to_monthly_mean', 'pycmor.awiesm3_lr.monthly_time'])
        steps = pipes['lr_model_levels']
        self.assertEqual(steps.index('pycmor.std_lib.dimensions.map_dimensions') + 1,
                         next(i for i, s in enumerate(steps) if s.endswith(':add_hybrid_sigma_coordinate')))
        self.assertIn(extra + 'transform_and_lowest_level', pipes['lr_lowest_model_level'])


class StepTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.grid = mesh(self.base / 'core3_grid.nc')

    def test_200m_is_linear_between_175_and_210_and_masked_below_floor(self):
        data = xr.DataArray([[[1., 8.], [2., np.nan]]], dims=('time', 'nod2', 'nz_200'),
                            coords={'nz_200': [175., 210.]}, name='O2', attrs={'units': 'mol m-3'})
        result = EXTRA.target_depth_only(data, Rule(lr_target_depth=200., grid_file=self.grid))
        self.assertEqual(result.dims, ('time', 'nod2'))
        self.assertAlmostEqual(float(result[0, 0]), 1. + (200. - 175.) / 35. * 7.)
        self.assertAlmostEqual(float(result[0, 0]), 6.)
        self.assertTrue(np.isnan(float(result[0, 1])))
        self.assertEqual(result.attrs['units'], 'mol m-3')
        self.assertEqual(result.name, 'O2')

    def test_fixed_depth_on_interfaces_takes_exact_level_or_interpolates(self):
        values = np.arange(48.)[None, None, :] * np.ones((1, 2, 48))
        data = xr.DataArray(values, dims=('time', 'nod2', 'nz1'), coords={'nz1': EDGES})
        exact = EXTRA.target_depth_only(data, Rule(lr_target_depth=100., grid_file=self.grid))
        self.assertEqual(float(exact[0, 0]), 11.)
        between = EXTRA.target_depth_only(data, Rule(lr_target_depth=1000., grid_file=self.grid))
        self.assertAlmostEqual(float(between[0, 0]), 24. + 90. / 130.)

    def test_fixed_depth_rejects_extrapolation_and_foreign_levels(self):
        data = xr.DataArray(np.ones((1, 2)), dims=('nod2', 'nz_200'), coords={'nz_200': [175., 210.]})
        with self.assertRaisesRegex(ValueError, 'outside the source levels'):
            EXTRA.target_depth_only(data, Rule(lr_target_depth=250., grid_file=self.grid))
        data = data.assign_coords(nz_200=[176., 210.])
        with self.assertRaisesRegex(ValueError, 'CORE3'):
            EXTRA.target_depth_only(data, Rule(lr_target_depth=200., grid_file=self.grid))

    def monthly(self, stem, variable, values, units):
        path = self.base / f'{stem}.fesom.2000.nc'
        times = [datetime(2000, m, 15) for m in range(1, 13)]
        with netCDF4.Dataset(path, 'w') as ds:
            ds.createDimension('time', 12); ds.createDimension('nod2', values.shape[1])
            ds.createDimension('nz', values.shape[2])
            t = ds.createVariable('time', 'f8', ('time',)); t.units = 'days since 2000-01-01'
            t.calendar = 'gregorian'; t[:] = netCDF4.date2num(times, t.units, t.calendar)
            ds.createVariable('nz', 'f4', ('nz',))[:] = CENTRES[:values.shape[2]]
            ds.createVariable('lat', 'f4', ('nod2',))[:] = np.linspace(-80, 60, values.shape[1])
            ds.createVariable('lon', 'f4', ('nod2',))[:] = np.linspace(0, 300, values.shape[1])
            v = ds.createVariable(variable, 'f4', ('time', 'nod2', 'nz'), fill_value=-9999.)
            v.units = units; v.coordinates = 'lat lon'; v.cell_methods = 'time: mean'; v[:] = values
        return path

    def test_column_integral_uses_matching_month_thickness_and_masks(self):
        conc = np.ma.masked_invalid(np.tile(np.array([[1., 2., 3.], [np.nan, 4., 5.]]), (12, 1, 1)))
        conc[5] *= 2
        hnode = np.ma.masked_invalid(np.tile(np.array([[5., 5., 10.], [np.nan, 5., 10.]]), (12, 1, 1)))
        dic = self.monthly('DIC', 'DIC', conc, '[mmol/m3]')
        thick = self.monthly('hnode', 'hnode', hnode, 'm')
        rule = Rule(year_start=2000, year_end=2000, model_variable='DIC', lr_units='kg m-2', lr_source_files=[str(dic)],
                    lr_terms=[{'files': [str(dic)], 'variable': 'DIC', 'units': '[mmol/m3]', 'coefficient': CARBON}],
                    lr_thickness_files=[str(thick)], lr_thickness_variable='hnode', lr_thickness_units='m')
        from pycmor.awiesm3_lr import load_linear
        result = EXTRA.vertical_integral(load_linear(None, rule), rule)
        self.assertEqual(result.dims, ('time', 'nod2'))
        self.assertAlmostEqual(float(result[0, 0]) / ((5 + 10 + 30) * CARBON), 1., places=6)
        self.assertAlmostEqual(float(result[0, 1]) / ((20 + 50) * CARBON), 1., places=6)
        self.assertAlmostEqual(float(result[5, 0]) / (90 * CARBON), 1., places=6)
        self.assertEqual(result.attrs['units'], 'kg m-2')
        sampled = VALIDATOR.sample_nc(dict(rule.__dict__), 2000, 6, {'nod2': 0}, 0)
        self.assertAlmostEqual(float(sampled) / float(result[5, 0]), 1., places=6)
        rule.lr_thickness_units = 'cm'
        with self.assertRaisesRegex(ValueError, 'thickness units'):
            EXTRA.vertical_integral(load_linear(None, rule), rule)

    def test_monthly_diatom_limitation_weights_only_valid_upper_ocean_biomass(self):
        shape = (12, 6, len(CENTRES))
        lim = np.full(shape, np.nan)
        carbon = np.zeros(shape)
        thickness = np.broadcast_to(np.diff(EDGES), shape).copy()
        lim[:, :, 0], lim[:, :, 10], lim[:, :, 11:] = 0.2, 0.8, 1.
        carbon[:, :, 0], carbon[:, :, 10], carbon[:, :, 11:] = 1., 3., 1000.
        thickness[:, 0, 10] = 12.  # Only 10 m of this layer lies above 100 m.
        thickness[:, 1, 10] = 2.   # Shallow bottom cell.
        carbon[:, 2, :11] = 0.
        lim[:, 3, 10] = np.nan
        thickness[:, 4, :11] = np.nan
        lim[:, 5, :11] = 0.
        carbon[6, 0, 0] = 0.
        source = self.monthly('limNd', 'limNd', np.ma.masked_invalid(lim), '1')
        biomass = self.monthly('DiaC', 'DiaC', carbon, '[mmol/m3]')
        dz = self.monthly('hnode', 'hnode', np.ma.masked_invalid(thickness), 'm')
        rule = Rule(year_start=2000, year_end=2000, grid_file=self.grid, model_variable='limNd',
                    expected_units='1', lr_weighted_depth=100, lr_source_files=[str(source)],
                    lr_biomass_files=[str(biomass)], lr_biomass_variable='DiaC', lr_biomass_units='[mmol/m3]',
                    lr_thickness_files=[str(dz)], lr_thickness_variable='hnode', lr_thickness_units='m')
        expected = [25./35., 5.8/11., np.nan, 0.2, np.nan, 0.]
        with xr.open_dataset(source) as ds:
            result = EXTRA.diatom_limitation(ds.limNd, rule)
            self.assertEqual(result.dims, ('time', 'nod2'))
            self.assertEqual(result.attrs['units'], '1')
            np.testing.assert_allclose(result[0], expected)
            self.assertAlmostEqual(float(result[6, 0]), 0.8)
            for j, value in enumerate(expected):
                sample = VALIDATOR.sample_nc(vars(rule), 2000, 1, {'nod2': j}, 0)
                if np.isnan(value):
                    self.assertTrue(np.ma.is_masked(sample))
                else:
                    self.assertAlmostEqual(float(sample), value)
            with self.assertRaisesRegex(ValueError, 'months, nodes and depth'):
                EXTRA.diatom_limitation(ds.limNd.assign_coords(lon=ds.lon[::-1]), rule)
            with self.assertRaisesRegex(ValueError, 'months, nodes and depth'):
                EXTRA.diatom_limitation(ds.limNd.assign_coords(time=ds.time + np.timedelta64(1, 'D')), rule)
            with self.assertRaisesRegex(ValueError, 'monthly mean for limNd'):
                EXTRA.diatom_limitation(ds.limNd.assign_attrs(cell_methods='time: point'), rule)

    def test_daily_to_monthly_mean_keeps_every_day_and_labels_mid_month(self):
        first = cftime.datetime(2000, 1, 1, 12, calendar='gregorian')
        times = [first + timedelta(days=i) for i in range(366)]
        values = np.arange(366.)[:, None] * np.ones((1, 2))
        data = xr.DataArray(values, dims=('time', 'nod2'), coords={'time': times},
                            name='bflxC', attrs={'units': 'mmolC/m2/s'})
        rule = Rule(year_start=2000, year_end=2000)
        result = EXTRA.daily_to_monthly_mean(data, rule)
        self.assertEqual(result.sizes['time'], 12)
        self.assertAlmostEqual(float(result[1, 0]), np.mean(np.arange(31, 60)))
        self.assertEqual((result.time.values[1].month, result.time.values[1].day), (2, 15))
        self.assertEqual(result.attrs['units'], 'mmolC/m2/s')
        with self.assertRaisesRegex(ValueError, 'daily records'):
            EXTRA.daily_to_monthly_mean(data.isel(time=slice(0, 365)), rule)

    def test_lowest_model_level_is_level_n(self):
        data = xr.DataArray(np.arange(6.).reshape(1, 3, 2), dims=('time', 'model_levels', 'cell'),
                            coords={'model_levels': [1., 2., 3.]})
        result = EXTRA.lowest_model_level(data, Rule())
        np.testing.assert_array_equal(result.values, [[4., 5.]])
        with self.assertRaisesRegex(ValueError, 'top to surface'):
            EXTRA.lowest_model_level(data.assign_coords(model_levels=[3., 2., 1.]), Rule())


class ValidatorTests(unittest.TestCase):
    def test_independent_sampler_interpolates_to_200m_and_averages_daily_months(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'O2_200.fesom.2000.nc'
            times = [datetime(2000, 1, 1, 12) + timedelta(days=i) for i in range(366)]
            with netCDF4.Dataset(path, 'w') as ds:
                ds.createDimension('time', 366); ds.createDimension('nod2', 1); ds.createDimension('nz_200', 2)
                ds.createVariable('nz_200', 'f4', ('nz_200',))[:] = [175., 210.]
                t = ds.createVariable('time', 'f8', ('time',)); t.units = 'days since 2000-01-01'
                t.calendar = 'gregorian'; t[:] = netCDF4.date2num(times, t.units, t.calendar)
                v = ds.createVariable('O2', 'f4', ('time', 'nod2', 'nz_200'))
                v[:] = np.stack([np.arange(366.), np.arange(366.) + 35.], axis=-1)[:, None, :]
            rule = {'lr_source_files': [str(path)], 'model_variable': 'O2', 'lr_scale': 1e-3, 'lr_target_depth': 200.}
            self.assertAlmostEqual(float(VALIDATOR.sample_nc(rule, 2000, 2, {'nod2': 0}, 0, day=29)), (59 + 25) * 1e-3)
            rule = {'lr_source_files': [str(path)], 'model_variable': 'O2', 'lr_scale': 1e-3,
                    'lr_source_frequency': 'day'}
            self.assertAlmostEqual(float(VALIDATOR.sample_nc(rule, 2000, 2, {'nod2': 0}, 0)),
                                   np.mean(range(31, 60)) * 1e-3)


if __name__ == '__main__':
    unittest.main()
