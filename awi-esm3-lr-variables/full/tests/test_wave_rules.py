"""WAM wave rules: mapping, coverage checks, grid, direction and validation on synthetic files."""
import copy
from datetime import timedelta
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


RAW = module('wave_adapter', 'raw_adapter.py')
STEPS = module('wave_steps', 'wave_steps.py')
VALIDATOR = module('wave_validator', 'validate_product.py')

# Independent reference: CMIP7 name -> (3-hourly WAM file/variable, monthly wam_1mo variable).
# XIOS names from file_def_wam_cmip7.xml.j2; directions monthly as sine/cosine pairs.
MAPPING = {
    'swh': ('swh', 'swh', 'm'), 'swhmax': ('swh_max', 'swh_max', 'm'),
    'swhswell': ('shts', 'shts', 'm'), 'swhwindsea': ('shww', 'shww', 'm'),
    'mpw': ('mwp', 'mwp', 's'), 'mpwswell': ('mpts', 'mpts', 's'), 'mpwwindsea': ('mpww', 'mpww', 's'),
    'wpp': ('pp1d', 'pp1d', 's'),
    'wdir': ('mwd', 'mwd_sin', 'degree'), 'wdirswell': ('mdts', 'mdts_sin', 'degree'),
    'wdirwindsea': ('mdww', 'mdww_sin', 'degree'),
    'sduo': ('ust', 'ust', 'm s-1'), 'sdvo': ('vst', 'vst', 'm s-1'),
}


class Rule(SimpleNamespace):
    def get(self, key, default=None):
        return getattr(self, key, default)


def write_wam(path, variables, year, kind, cells=3, values=None):
    """Synthetic XIOS WAM file: kind is 'point' (3h instants), 'max' (3h windows) or 'mon'."""
    calendar = 'gregorian'
    start = cftime.datetime(year, 1, 1, calendar=calendar)
    stop = cftime.datetime(year + 1, 1, 1, calendar=calendar)
    if kind == 'mon':
        lows = [cftime.datetime(year, m, 1, calendar=calendar) for m in range(1, 13)]
        highs = lows[1:] + [stop]
    else:
        step = timedelta(hours=3)
        highs = [start + step * (i + 1) for i in range(int((stop - start) / step))]
        lows = highs if kind == 'point' else [h - step for h in highs]
    units = f'seconds since {year}-01-01 00:00:00'
    low, high = netCDF4.date2num(lows, units, calendar), netCDF4.date2num(highs, units, calendar)
    with netCDF4.Dataset(path, 'w') as ds:
        ds.createDimension('time', None); ds.createDimension('cell', cells)
        ds.createDimension('nvertex', 4); ds.createDimension('axis_nbounds', 2)
        lat = ds.createVariable('lat', 'f4', ('cell',)); lat.units = 'degrees_north'
        lon = ds.createVariable('lon', 'f4', ('cell',)); lon.units = 'degrees_east'
        lat[:] = np.arange(cells) - 1.; lon[:] = np.arange(cells) * 2.
        blat = ds.createVariable('bounds_lat', 'f4', ('cell', 'nvertex'))
        blon = ds.createVariable('bounds_lon', 'f4', ('cell', 'nvertex'))
        blat[:] = np.column_stack([lat[:] - .5, lat[:] - .5, lat[:] + .5, lat[:] + .5])
        blon[:] = np.column_stack([lon[:] - 1, lon[:] + 1, lon[:] + 1, lon[:] - 1])
        t = ds.createVariable('time', 'f8', ('time',))
        t.units, t.calendar, t.bounds = units, calendar, 'time_bounds'
        t[:] = (low + high) / 2
        ds.createVariable('time_bounds', 'f8', ('time', 'axis_nbounds'))[:] = np.column_stack([low, high])
        for name, unit in variables.items():
            v = ds.createVariable(name, 'f4', ('time', 'cell'), fill_value=-999.)
            v.units = unit
            v.cell_methods = {'point': 'time: point', 'max': 'time: maximum (interval: 3600 s)',
                              'mon': 'time: mean (interval: 3600 s)'}[kind]
            data = np.arange(len(high))[:, None] + np.arange(cells)[None, :] / 10. + 1.
            v[:] = data if values is None else values(name, data)
            v[0, 0] = np.ma.masked
    return path


class WaveRuleTests(unittest.TestCase):
    def setUp(self):
        RAW.META = {}
        RAW.CARBON_CONTEXT = {'external_emissions_reader': False}
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.out = self.base / 'outdata/oifs'
        self.out.mkdir(parents=True)
        RAW.EXP, RAW.YEARS = self.base, [2001]

    def rule(self, compound):
        return copy.deepcopy(next(r for r in RAW.wave_rules() if r['compound_name'] == compound))

    def test_rules_cover_the_package_with_reviewed_sources(self):
        rules = list(RAW.wave_rules())
        self.assertEqual(len(rules), 26)
        expected = {f'ocean.{n}.tpt-u-hxy-sea.3hr.glb' for n in MAPPING}
        expected |= {f'ocean.{n}.{"tmax" if n == "swhmax" else "tavg"}-u-hxy-sea.mon.glb' for n in MAPPING}
        self.assertEqual({r['compound_name'] for r in rules}, expected)
        for rule in rules:
            name, frequency = rule['compound_name'].split('.')[1], rule['compound_name'].split('.')[-2]
            point, monthly, units = MAPPING[name]
            with self.subTest(compound=rule['compound_name']):
                self.assertEqual(rule['lr_group'], 'wave')
                self.assertEqual(rule['pipelines'], ['lr_wave'])
                self.assertNotIn('comment', rule)
                if frequency == '3hr':
                    self.assertEqual(rule['model_variable'], point)
                    self.assertEqual(rule['expected_units'], units)
                    self.assertEqual(rule['lr_input']['pattern'], point + r'\.wam_3h\.\d{4}\.nc$')
                    self.assertEqual(bool(rule.get('lr_wave_window_end')), name == 'swhmax')
                else:
                    self.assertEqual(rule['model_variable'], monthly)
                    self.assertEqual(rule['lr_input']['pattern'], r'wam_1mo\.\d{4}\.nc$')
                    if units == 'degree':
                        self.assertEqual(rule['lr_wave_cosine'], monthly.replace('_sin', '_cos'))
                        self.assertEqual(rule['expected_units'], '1')
                    else:
                        self.assertNotIn('lr_wave_cosine', rule)
                        self.assertEqual(rule['expected_units'], units)

    def test_three_hourly_instants_materialize_with_year_end_point(self):
        write_wam(self.out / 'swh.wam_3h.2001.nc', {'swh': 'm'}, 2001, 'point')
        write_wam(self.out / 'swh.wam_3h.2002.nc', {'swh': 'm'}, 2002, 'point')
        group, rule = RAW.materialize(self.rule('ocean.swh.tpt-u-hxy-sea.3hr.glb'), {'wam': 'grid.nc'})
        self.assertEqual(group, 'wave')
        self.assertEqual(rule['grid_label'], 'g150')
        self.assertEqual(rule['grid_file'], 'grid.nc')
        self.assertTrue(rule['retain_year_end_point'])
        self.assertEqual([Path(p).name for p in rule['lr_source_files']], ['swh.wam_3h.2001.nc'])

    def test_three_hourly_window_maximum_is_accepted(self):
        write_wam(self.out / 'swh_max.wam_3h.2001.nc', {'swh_max': 'm'}, 2001, 'max')
        _, rule = RAW.materialize(self.rule('ocean.swhmax.tpt-u-hxy-sea.3hr.glb'), {'wam': 'grid.nc'})
        self.assertTrue(rule['lr_wave_window_end'])

    def test_missing_or_incomplete_three_hourly_input_is_blocked(self):
        with self.assertRaisesRegex(ValueError, r'Missing source: oifs/swh\\.wam_3h'):
            RAW.materialize(self.rule('ocean.swh.tpt-u-hxy-sea.3hr.glb'), {'wam': 'grid.nc'})
        path = write_wam(self.out / 'swh.wam_3h.2001.nc', {'swh': 'm'}, 2001, 'point')
        with netCDF4.Dataset(path, 'a') as ds:
            ds['time_bounds'][5] = ds['time_bounds'][4]
        with self.assertRaisesRegex(ValueError, 'Incomplete/duplicate 3-hourly coverage'):
            RAW.materialize(self.rule('ocean.swh.tpt-u-hxy-sea.3hr.glb'), {'wam': 'grid.nc'})

    def test_mislabelled_window_is_not_an_instant(self):
        write_wam(self.out / 'swh.wam_3h.2001.nc', {'swh': 'm'}, 2001, 'max')
        with self.assertRaisesRegex(ValueError, 'is not a 3h instant'):
            RAW.materialize(self.rule('ocean.swh.tpt-u-hxy-sea.3hr.glb'), {'wam': 'grid.nc'})

    def test_monthly_direction_needs_sine_and_cosine(self):
        write_wam(self.out / 'wam_1mo.2001.nc', {'mwd_sin': '1'}, 2001, 'mon')
        with self.assertRaises((ValueError, IndexError, KeyError)):
            RAW.materialize(self.rule('ocean.wdir.tavg-u-hxy-sea.mon.glb'), {'wam': 'grid.nc'})
        write_wam(self.out / 'wam_1mo.2001.nc', {'mwd_sin': '1', 'mwd_cos': '1'}, 2001, 'mon')
        _, rule = RAW.materialize(self.rule('ocean.wdir.tavg-u-hxy-sea.mon.glb'), {'wam': 'grid.nc'})
        self.assertNotIn('retain_year_end_point', rule)

    def test_missing_geometry_blocks_and_products_go_to_g150(self):
        write_wam(self.out / 'wam_1mo.2001.nc', {'swh': 'm'}, 2001, 'mon')
        with self.assertRaisesRegex(ValueError, 'No audited wam fixed-grid geometry'):
            RAW.materialize(self.rule('ocean.swh.tavg-u-hxy-sea.mon.glb'), {})
        _, rule = RAW.materialize(self.rule('ocean.swh.tavg-u-hxy-sea.mon.glb'), {'wam': 'grid.nc'})
        self.assertEqual(rule['grid_label'], 'g150')

    def test_remap_kind_follows_the_statistic(self):
        kinds = {r['compound_name']: r.get('lr_wave_remap', 'mean') for r in RAW.wave_rules()}
        self.assertEqual({c for c, k in kinds.items() if k == 'maximum'},
                         {'ocean.swhmax.tpt-u-hxy-sea.3hr.glb', 'ocean.swhmax.tmax-u-hxy-sea.mon.glb'})
        self.assertEqual({c.split('.')[1] for c, k in kinds.items() if k == 'direction'},
                         {'wdir', 'wdirswell', 'wdirwindsea'})
        self.assertEqual(sum(k == 'direction' for k in kinds.values()), 6)

    def test_wam_grid_areas_and_box_check(self):
        source = write_wam(self.out / 'wam_1mo.2001.nc', {'swh': 'm'}, 2001, 'mon')
        RAW.wam_grid(source, self.base / 'wam_grid.nc')
        with xr.open_dataset(self.base / 'wam_grid.nc') as grid:
            lat = np.array([-1., 0., 1.])
            area = 6371229.**2 * np.deg2rad(2.) * (np.sin(np.deg2rad(lat + .5)) - np.sin(np.deg2rad(lat - .5)))
            np.testing.assert_allclose(grid.cell_area.values, area, rtol=1e-12)
            self.assertEqual(grid.lat_bnds.dims, ('cell', 'vertices'))
        with netCDF4.Dataset(source, 'a') as ds:
            ds['bounds_lat'][0, 1] = -1.2
        with self.assertRaisesRegex(ValueError, 'not latitude-longitude boxes'):
            RAW.wam_grid(source, self.base / 'other.nc')

    def test_pipeline_is_registered_once_with_existing_step(self):
        profile = {'pipelines': []}
        RAW.register_wave_pipelines(profile)
        RAW.register_wave_pipelines(profile)
        self.assertEqual([p['name'] for p in profile['pipelines']], ['lr_wave'])
        step = next(s for s in profile['pipelines'][0]['steps'] if s.startswith('script://'))
        path, name = step[9:].rsplit(':', 1)
        self.assertTrue(Path(path).is_file())
        self.assertEqual(name, 'select_wave_variable')
        scripts = [s.rsplit(':', 1)[1] for s in profile['pipelines'][0]['steps'] if s.startswith('script://')]
        self.assertEqual(scripts, ['select_wave_variable', 'remap_wave_to_g150'])
        self.assertNotIn('pycmor.std_lib.generic.get_variable', profile['pipelines'][0]['steps'])


class WaveStepTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.grid = self.base / 'wam_grid.nc'
        RAW.wam_grid(write_wam(self.base / 'g.nc', {'swh': 'm'}, 2001, 'mon', cells=4), self.grid)

    def rule(self, **kw):
        return Rule(name='test', grid_file=str(self.grid), lr_grid_cells=4, **kw)

    def test_monthly_direction_from_mean_sine_and_cosine(self):
        sines, cosines = [1., 0., -1., np.nan], [0., -1., 0., np.nan]
        path = write_wam(self.base / 'wam_1mo.2001.nc', {'mwd_sin': '1', 'mwd_cos': '1'}, 2001, 'mon', cells=4,
                         values=lambda name, data: np.tile(sines if name == 'mwd_sin' else cosines, (12, 1)))
        with xr.open_dataset(path, use_cftime=True) as ds:
            da = STEPS.select_wave_variable(ds, self.rule(model_variable='mwd_sin', lr_wave_cosine='mwd_cos',
                                                          expected_units='1'))
            np.testing.assert_allclose(da.values[1], [90., 180., 270., np.nan])
            self.assertEqual(da.attrs, {'units': 'degree'})
            self.assertTrue(np.isnan(da.values[0, 0]))

    def test_window_maximum_is_stamped_at_window_end(self):
        path = write_wam(self.base / 'swh_max.wam_3h.2001.nc', {'swh_max': 'm'}, 2001, 'max', cells=4)
        with xr.open_dataset(path, use_cftime=True) as ds:
            da = STEPS.select_wave_variable(ds, self.rule(model_variable='swh_max', expected_units='m',
                                                          lr_wave_window_end=True))
            self.assertEqual(da.time.values[0], cftime.datetime(2001, 1, 1, 3, calendar='gregorian'))
            self.assertEqual(da.time.values[-1], cftime.datetime(2002, 1, 1, calendar='gregorian'))
            self.assertEqual(float(da.values[1, 1]), float(ds.swh_max.values[1, 1]))

    def test_grid_and_unit_mismatch_are_rejected(self):
        path = write_wam(self.base / 'swh.wam_3h.2001.nc', {'swh': 'm'}, 2001, 'point', cells=4)
        with xr.open_dataset(path, use_cftime=True) as ds:
            with self.assertRaisesRegex(ValueError, 'expected m s-1'):
                STEPS.select_wave_variable(ds, self.rule(model_variable='swh', expected_units='m s-1'))
            moved = ds.assign_coords(lat=ds.lat + 1)
            with self.assertRaisesRegex(ValueError, 'differ from the campaign grid'):
                STEPS.select_wave_variable(moved, self.rule(model_variable='swh', expected_units='m'))
        with xr.open_dataset(path, use_cftime=True) as ds:
            with self.assertRaisesRegex(ValueError, 'expected 5 WAM cells'):
                STEPS.select_wave_variable(ds, Rule(name='t', grid_file=str(self.grid), lr_grid_cells=5,
                                                    model_variable='swh', expected_units='m'))


class WaveValidationTests(unittest.TestCase):
    """The independent check must follow each raw record to its output record."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)

    def output(self, source, shift_hours=0, name='swhmax'):
        """Fake CMOR output of the window maximum, stamped at window end (+ shift)."""
        out = self.base / 'out'; out.mkdir(exist_ok=True)
        with netCDF4.Dataset(source) as src:
            t = src['time']
            ends = netCDF4.num2date(src['time_bounds'][:, 1], t.units, t.calendar)
            values = np.ma.filled(src['swh_max'][:].astype(float), np.nan)
            cell, flat = STEPS.g150_index(src['lat'][:], src['bounds_lon'][:])
        stamps = [e + timedelta(hours=shift_hours) for e in ends]
        with netCDF4.Dataset(out / 'swhmax.nc', 'w') as ds:
            ds.source_id = 'LR-SPINUP-TEST'; ds.tracking_id = 'hdl:test'; ds.grid_label = 'g150'
            ds.createDimension('time', None); ds.createDimension('lat', 180); ds.createDimension('lon', 360)
            ds.createVariable('lat', 'f8', ('lat',))[:] = STEPS.G150_LAT
            ds.createVariable('lon', 'f8', ('lon',))[:] = STEPS.G150_LON
            time = ds.createVariable('time', 'f8', ('time',))
            time.units, time.calendar = 'days since 1850-01-01', 'proleptic_gregorian'
            time[:] = cftime.date2num([cftime.datetime(*s.timetuple()[:6], calendar='proleptic_gregorian')
                                       for s in stamps], time.units, time.calendar)
            v = ds.createVariable(name, 'f4', ('time', 'lat', 'lon'), fill_value=1e20)
            v.units = 'm'
            v[:] = np.ma.masked_invalid(STEPS.to_g150(values, cell, flat, how='maximum'))
        return out

    def config(self, source, out):
        rule = dict(compound_name='ocean.swhmax.tpt-u-hxy-sea.3hr.glb', model_variable='swh_max',
                    expected_units='m', lr_wave=True, lr_wave_window_end=True, lr_wave_remap='maximum',
                    lr_source_files=[str(source)])
        return dict(inherit=dict(year_start=2001, year_end=2001, output_directory=str(out)), rules=[rule])

    def test_window_end_stamps_pass_and_start_stamps_fail(self):
        source = write_wam(self.base / 'swh_max.wam_3h.2001.nc', {'swh_max': 'm'}, 2001, 'max')
        good = self.output(source)
        cfg = self.config(source, good)
        result = VALIDATOR.validate_wave(cfg, cfg['rules'][0], sorted(good.rglob('*.nc')))
        self.assertGreater(result['compared_samples'], 0)
        recipe = self.base / 'recipe.yaml'
        recipe.write_text(yaml.safe_dump(cfg))
        self.assertEqual(VALIDATOR.validate(recipe)['coverage'], 'PASS')
        bad = self.output(source, shift_hours=-3)
        with self.assertRaises((AssertionError, KeyError)):
            VALIDATOR.validate_wave(cfg, cfg['rules'][0], sorted(bad.rglob('*.nc')))


class WaveRemapTests(unittest.TestCase):
    """Delivery on g150: whole-degree WAM rows straddled by half-degree g150 rows."""

    def test_cells_cover_their_columns_across_the_dateline(self):
        lat = np.array([0., 0., 60.])
        lon_bnds = np.array([[-1., 1.], [1., 3.], [10., 12.]])
        cell, flat = STEPS.g150_index(lat, lon_bnds)
        got = {(int(c), int(f // 360) - 90, int(f % 360)) for c, f in zip(cell, flat)}
        self.assertEqual(got, {(0, 0, 359), (0, 0, 0), (1, 0, 1), (1, 0, 2), (2, 60, 10), (2, 60, 11)})
        with self.assertRaisesRegex(ValueError, 'whole-degree'):
            STEPS.g150_index(np.array([0.5]), np.array([[0., 1.]]))

    def test_rows_mean_maximum_direction_and_coast(self):
        lat = np.array([0., 1.])
        lon_bnds = np.array([[-.5, .5], [-.5, .5]])
        cell, flat = STEPS.g150_index(lat, lon_bnds)
        mean = STEPS.to_g150(np.array([[1., 3.]]), cell, flat)[0]
        self.assertEqual(mean.shape, (180, 360))
        self.assertAlmostEqual(float(mean[90, 0]), 2.)      # 0.5N straddles the 0 and 1 rows
        self.assertAlmostEqual(float(mean[89, 0]), 1.)      # 0.5S: the 0 row only, the other is not sea
        self.assertAlmostEqual(float(mean[91, 0]), 3.)
        self.assertTrue(np.isnan(mean[92, 0]) and np.isnan(mean[90, 1]))
        self.assertAlmostEqual(float(STEPS.to_g150(np.array([[1., 3.]]), cell, flat, 'maximum')[0, 90, 0]), 3.)
        direction = STEPS.to_g150(np.array([[350., 10.]]), cell, flat, 'direction')[0, 90, 0]
        self.assertAlmostEqual(float(min(direction, 360 - direction)), 0., places=4)

    def test_step_matches_the_independent_audit_on_a_reduced_grid(self):
        rng = np.random.default_rng(1)
        lat, lon_bnds = [], []
        for row in range(-78, 91):
            n = max(4, int(round(360 * np.cos(np.radians(row)))))
            n = min(n, 360)
            width = 360. / n
            for i in range(n):
                if rng.random() < .7:
                    lat.append(float(row)); lon_bnds.append([i * width - width / 2, i * width + width / 2])
        lat, lon_bnds = np.array(lat), np.array(lon_bnds)
        values = rng.random((2, lat.size)) * 360
        values[:, rng.random(lat.size) < .1] = np.nan
        cell, flat = STEPS.g150_index(lat, lon_bnds)
        for how in ('mean', 'maximum', 'direction'):
            # Directions of neighbouring cells within 120 degrees across north, never near-opposite.
            field = (values / 3 + 300) % 360 if how == 'direction' else values
            step = np.ma.masked_invalid(STEPS.to_g150(field, cell, flat, how)[1])
            audit = VALIDATOR.wam_on_g150(field[1], lat, lon_bnds, how)
            np.testing.assert_array_equal(np.ma.getmaskarray(step), np.ma.getmaskarray(audit))
            difference = np.abs(step.compressed() - audit.compressed())
            if how == 'direction':
                difference = np.minimum(difference, 360 - difference)
            self.assertLess(float(difference.max()), 1e-3)

    def test_remap_step_on_a_dataarray(self):
        grid = Path(tempfile.mkdtemp()) / 'wam_grid.nc'
        RAW.wam_grid(write_wam(grid.with_name('g.nc'), {'swh': 'm'}, 2001, 'mon'), grid)
        da = xr.DataArray(np.array([[1., 2., 3.]] * 2), dims=('time', 'cell'), name='swh', attrs={'units': 'm'})
        out = STEPS.remap_wave_to_g150(da, Rule(name='t', grid_file=str(grid)))
        self.assertEqual(out.dims, ('time', 'lat', 'lon'))
        self.assertEqual(out.shape, (2, 180, 360))
        np.testing.assert_allclose(out.lat.values, STEPS.G150_LAT)
        self.assertEqual(out.attrs['units'], 'm')
        self.assertAlmostEqual(float(out.values[0, 89, 0]), 1.)     # 0.5S: only the -1 row covers 0E
        self.assertAlmostEqual(float(out.values[0, 90, 2]), 2.)     # 0.5N: the 0 row; the 1 row starts at 3E
        self.assertTrue(np.isnan(out.values[0, 120, 0]))


if __name__ == '__main__':
    unittest.main()
