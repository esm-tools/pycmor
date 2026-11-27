"""Small fixtures for representative selection and honest operational QA."""
from contextlib import redirect_stdout
from datetime import datetime
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import netCDF4
import numpy as np

import retest_all_products as r


COMPOUND = 'ocean.tos.tavg-u-hxy-sea.mon.glb'
METADATA = {'units': 'K', 'standard_name': 'sea_surface_temperature',
            'dimensions': 'longitude latitude time'}


class RepresentativeTests(unittest.TestCase):
    def test_repaired_products_run_first_without_omitting_other_products(self):
        ordinary='aerosol.toz.tavg-u-hxy-u.mon.glb'
        repaired='ocean.hfx.tavg-u-hxy-sea.day.glb'
        tasks=[dict(compound=ordinary,year=2610,key='ordinary'),
               dict(compound=repaired,year=2610,key='repaired')]
        self.assertEqual([t['key'] for t in r.representatives(tasks)],['repaired','ordinary'])

    def test_selects_every_compound_not_only_short_variable_name(self):
        surface = 'ocnBgchem.chl.tavg-ols-hxy-sea.mon.glb'
        profile = 'ocnBgchem.chl.tavg-ol-hxy-sea.mon.glb'
        tasks = [dict(compound=surface, year=2596, key='later-surface'),
                 dict(compound=profile, year=2597, key='only-profile'),
                 dict(compound=surface, year=2595, key='first-surface')]
        before = list(tasks)
        selected = r.representatives(tasks)
        self.assertEqual({x['compound'] for x in selected}, {surface, profile})
        self.assertEqual({x['key'] for x in selected},
                         {'first-surface', 'only-profile'})
        self.assertEqual(tasks, before)

    def test_empty_selection(self):
        self.assertEqual(r.representatives([]), [])


class GateTests(unittest.TestCase):
    def test_missing_or_failed_tests_never_submit_full(self):
        for status in (None, 'TEST_FAILED'):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                campaign = Path(directory)
                dest = campaign / 'representative_test'
                r.f.write(dest / 'manifest.json', dict(selected=[dict(key='test')], config={}))
                r.f.write(dest / 'submission.json', dict(required=['test'], full_after_pass=True))
                if status:
                    r.f.write(dest / 'status/test.json', dict(status=status, error='fixture failure'))
                with patch.object(r, 'verify'), patch.object(r, 'report'), \
                     patch.object(r.f, 'report'), patch.object(r.f, 'submit_workers') as submit:
                    self.assertEqual(r.gate(campaign), 1)
                    submit.assert_not_called()
                self.assertEqual(r.f.read(dest / 'gate_result.json')['status'], 'BLOCKED')

    def test_success_requires_unchanged_output_and_input(self):
        with tempfile.TemporaryDirectory() as directory:
            campaign = Path(directory)
            dest = campaign / 'representative_test'
            data = campaign / 'output.nc'
            data.write_bytes(b'fixture')
            state = r.f.file_state([data])
            r.f.write(dest / 'manifest.json', dict(
                selected=[dict(key='test', input_state=state)], config={'fixture': True}))
            r.f.write(dest / 'submission.json', dict(required=['test'], full_after_pass=True))
            r.f.write(dest / 'status/test.json', dict(
                status='CONVERSION_AND_BASIC_CHECKS_PASS', conversion=dict(
                    status='PASS', validation=dict(files=[str(data)], output_state=state))))
            with patch.object(r, 'verify'), patch.object(r, 'report'), \
                 patch.object(r.f, 'report'), patch.object(r.f, 'submit_workers') as submit:
                self.assertEqual(r.gate(campaign), 0)
                submit.assert_called_once_with(campaign, {'fixture': True})
                submit.reset_mock()
                data.write_bytes(b'changed fixture')
                self.assertEqual(r.gate(campaign), 1)
                submit.assert_not_called()


class DataChecksTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'product.nc'
        self.task = dict(compound=COMPOUND, year=2595, key='2595__' + COMPOUND)
        units = 'days since 2595-01-01'
        boundaries = [datetime(2595, month, 1) for month in range(1, 13)]
        boundaries.append(datetime(2596, 1, 1))
        numbers = netCDF4.date2num(boundaries, units, calendar='standard')
        with netCDF4.Dataset(self.path, 'w') as ds:
            for dim, size in [('time', 12), ('cell', 4), ('bnds', 2)]:
                ds.createDimension(dim, size)
            time = ds.createVariable('time', 'f8', ('time',))
            time.units = units
            time.calendar = 'standard'
            time.bounds = 'time_bnds'
            time[:] = (numbers[:-1] + numbers[1:]) / 2
            bounds = ds.createVariable('time_bnds', 'f8', ('time', 'bnds'))
            bounds[:] = np.column_stack((numbers[:-1], numbers[1:]))
            for coord, values in [('longitude', [10, 20, 30, 40]),
                                  ('latitude', [-30, -10, 10, 30])]:
                ds.createVariable(coord, 'f8', ('cell',))[:] = values
            data = ds.createVariable('tos', 'f4', ('time', 'cell'), fill_value=-9999.)
            data.units = 'K'
            data.standard_name = METADATA['standard_name']
            data[:] = np.arange(48).reshape(12, 4) + 273.

    def check(self):
        return r.check_data(self.task, [str(self.path)], METADATA)

    def test_valid_monthly_data(self):
        details = self.check()
        self.assertEqual(len(details), 1)
        self.assertEqual(details[0]['finite_values'], 48)
        self.assertEqual(details[0]['masked_values'], 0)
        self.assertEqual(details[0]['units'], 'K')

    def test_wrong_units_rejected(self):
        with netCDF4.Dataset(self.path, 'r+') as ds:
            ds['tos'].units = 'degC'
        with self.assertRaisesRegex(ValueError, 'Wrong units'):
            self.check()

    def test_reversed_time_rejected_before_sorting(self):
        with netCDF4.Dataset(self.path, 'r+') as ds:
            ds['time'][:] = ds['time'][:][::-1]
            ds['time_bnds'][:] = ds['time_bnds'][:][::-1]
            ds['tos'][:] = ds['tos'][:][::-1]
        with self.assertRaisesRegex(ValueError, 'not strictly increasing'):
            self.check()

    def test_variable_must_use_time_dimension(self):
        with netCDF4.Dataset(self.path, 'r+') as ds:
            ds.renameVariable('tos', 'old_tos')
            data = ds.createVariable('tos', 'f4', ('cell',))
            data.units = 'K'
            data.standard_name = METADATA['standard_name']
            data[:] = 273.
        with self.assertRaisesRegex(ValueError, 'does not use the time coordinate dimension'):
            self.check()

    def test_correct_label_year_does_not_hide_shifted_bounds(self):
        boundaries = [datetime(2594, 12, 1)]
        boundaries.extend(datetime(2595, month, 1) for month in range(1, 13))
        with netCDF4.Dataset(self.path, 'r+') as ds:
            numbers = netCDF4.date2num(boundaries, ds['time'].units)
            # The labels contain every requested month, but the intervals cover
            # December of the previous year through November of this year.
            ds['time'][:] = numbers[1:]
            ds['time_bnds'][:] = np.column_stack((numbers[:-1], numbers[1:]))
        with self.assertRaisesRegex(ValueError, 'exactly the requested calendar year'):
            self.check()

    def test_nonfinite_time_and_bounds_rejected(self):
        with netCDF4.Dataset(self.path, 'r+') as ds:
            original = ds['time'][0]
            ds['time'][0] = np.nan
        with self.assertRaisesRegex(ValueError, 'Nonfinite time coordinate'):
            self.check()
        with netCDF4.Dataset(self.path, 'r+') as ds:
            ds['time'][0] = original
            ds['time_bnds'][0, 0] = np.nan
        with self.assertRaisesRegex(ValueError, 'Nonfinite interval bounds'):
            self.check()

    def test_missing_bounds_rejected(self):
        with netCDF4.Dataset(self.path, 'r+') as ds:
            ds['time'].delncattr('bounds')
        with self.assertRaisesRegex(ValueError, 'Missing time bounds'):
            self.check()

    def test_daily_statistic_uses_climatology_bounds(self):
        with netCDF4.Dataset(self.path, 'r+') as ds:
            ds['time'].climatology = ds['time'].bounds
            ds['time'].delncattr('bounds')
        metadata = dict(METADATA, temporal_shape='monthly-mean-daily-stat')
        self.assertEqual(r.check_data(self.task,[self.path],metadata)[0]['finite_values'],48)
        with netCDF4.Dataset(self.path, 'r+') as ds:
            ds['time_bnds'][0,1] = ds['time_bnds'][1,1]
        with self.assertRaisesRegex(ValueError,'not one calendar month/year'):
            r.check_data(self.task,[self.path],metadata)

    def test_incorrect_month_interval_rejected(self):
        with netCDF4.Dataset(self.path, 'r+') as ds:
            ds['time_bnds'][0, 1] = ds['time_bnds'][1, 1]
        with self.assertRaisesRegex(ValueError, 'not one calendar month/year'):
            self.check()

    def test_all_missing_data_rejected(self):
        with netCDF4.Dataset(self.path, 'r+') as ds:
            ds['tos'][:] = np.ma.masked_all((12, 4))
        with self.assertRaisesRegex(ValueError, 'Invalid/all-missing variable'):
            self.check()

    def test_nan_and_infinity_rejected(self):
        for invalid in (np.nan, np.inf, -np.inf):
            with self.subTest(invalid=invalid):
                with netCDF4.Dataset(self.path, 'r+') as ds:
                    ds['tos'][0, 0] = invalid
                with self.assertRaisesRegex(ValueError, 'Invalid/all-missing variable'):
                    self.check()

    def test_partially_masked_data_is_valid_and_counted(self):
        with netCDF4.Dataset(self.path, 'r+') as ds:
            ds['tos'][0, 0] = np.ma.masked
        details = self.check()
        self.assertEqual(details[0]['finite_values'], 47)
        self.assertEqual(details[0]['masked_values'], 1)

    def test_wrong_standard_name_rejected(self):
        with netCDF4.Dataset(self.path, 'r+') as ds:
            ds['tos'].standard_name = 'air_temperature'
        with self.assertRaisesRegex(ValueError, 'standard_name'):
            self.check()

    def test_no_files_rejected(self):
        with self.assertRaisesRegex(ValueError, 'No output files'):
            r.check_data(self.task, [], METADATA)

    def test_fixed_scalar_has_no_time_requirement(self):
        path = Path(self.directory.name) / 'fixed.nc'
        with netCDF4.Dataset(path, 'w') as ds:
            variable = ds.createVariable('areacello', 'f8')
            variable.units = 'm2'
            variable[...] = 10.
        task = dict(compound='ocean.areacello.ti-u-hxy-sea.fx.glb', year=2595)
        result = r.check_data(task, [path], {'units': 'm2'})
        self.assertEqual(result[0]['finite_values'], 1)

    def decadal_fixture(self):
        path = Path(self.directory.name) / 'decadal.nc'
        units = 'days since 2600-01-01'
        bounds = netCDF4.date2num([datetime(2600, 1, 1), datetime(2610, 1, 1)],
                                 units, calendar='standard')
        with netCDF4.Dataset(path, 'w') as ds:
            ds.createDimension('time', 1)
            ds.createDimension('bnds', 2)
            t = ds.createVariable('time', 'f8', ('time',))
            t.units = units
            t.calendar = 'standard'
            t.bounds = 'time_bnds'
            t[:] = np.mean(bounds)
            ds.createVariable('time_bnds', 'f8', ('time', 'bnds'))[:] = [bounds]
            v = ds.createVariable('tos', 'f4', ('time',))
            v.units = 'K'
            v.standard_name = METADATA['standard_name']
            v[:] = 280.
        task = dict(compound='ocean.tos.tavg-u-hxy-sea.dec.glb', year=2609,
                    window_start=2600, window_end=2609)
        metadata = dict(METADATA, dimensions='time')
        return path, task, metadata

    def test_valid_decadal_mean_checks_window_not_annual_label(self):
        path, task, metadata = self.decadal_fixture()
        with patch.object(r.f, 'cover_dates', side_effect=AssertionError('Annual validator must not run')) as annual:
            details = r.check_data(task, [path], metadata)
        annual.assert_not_called()
        self.assertEqual(details[0]['finite_values'], 1)

    def test_decadal_mean_rejects_mismatched_window(self):
        path, task, metadata = self.decadal_fixture()
        task.update(window_start=2601, window_end=2610)
        with self.assertRaisesRegex(ValueError, 'DECADAL_OUTPUT_WINDOW'):
            r.check_data(task, [path], metadata)


class InstantaneousTimeChecksTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'instantaneous.nc'
        self.task = dict(compound='atmos.hus.tpt-al-hxy-u.6hr.glb', year=2605)
        self.metadata = dict(units='1', standard_name='specific_humidity',
                             dimensions='time1', temporal_shape='time-point',
                             cell_methods='area: mean time: point')
        self.times = np.arange(6., 365 * 24 + 6, 6.)

    def fixture(self, times=None, bounds=False, path=None):
        path = path or self.path
        times = self.times if times is None else np.asarray(times)
        with netCDF4.Dataset(path, 'w') as ds:
            ds.createDimension('time', len(times))
            t = ds.createVariable('time', 'f8', ('time',))
            t.units = 'hours since 2605-01-01'
            t.calendar = 'standard'
            t[:] = times
            if bounds:
                ds.createDimension('bnds', 2)
                t.bounds = 'time_bnds'
                ds.createVariable('time_bnds', 'f8', ('time', 'bnds'))[:] = \
                    np.column_stack((times - 3., times + 3.))
            data = ds.createVariable('hus', 'f4', ('time',))
            data.units = self.metadata['units']
            data.standard_name = self.metadata['standard_name']
            data[:] = 0.01
        return path

    def check(self, paths=None, metadata=None):
        return r.check_data(self.task, paths or [self.path],
                            self.metadata if metadata is None else metadata)

    def test_complete_six_hourly_points_do_not_require_bounds(self):
        self.fixture()
        result = self.check()
        self.assertEqual(result[0]['finite_values'], 1460)

    def test_end_of_year_point_in_separate_file_is_not_a_missing_year(self):
        first = self.fixture(self.times[:-1], path=Path(self.directory.name) / '2605.nc')
        last = self.fixture(self.times[-1:], path=Path(self.directory.name) / '2606.nc')
        result = self.check([first, last])
        self.assertEqual(sum(row['finite_values'] for row in result), 1460)

    def test_missing_timestamp_still_fails(self):
        self.fixture(np.delete(self.times, 20))
        with self.assertRaisesRegex(ValueError, 'Incomplete 6hr coverage'):
            self.check()

    def test_duplicate_and_reversed_timestamps_still_fail(self):
        duplicate = self.times.copy()
        duplicate[20] = duplicate[19]
        for times in (duplicate, self.times[::-1]):
            with self.subTest(duplicate=times is duplicate):
                self.fixture(times)
                with self.assertRaisesRegex(ValueError, 'not strictly increasing'):
                    self.check()

    def test_declared_optional_bounds_must_exist(self):
        self.fixture()
        with netCDF4.Dataset(self.path, 'r+') as ds:
            ds['time'].bounds = 'absent_bounds'
        with self.assertRaisesRegex(ValueError, 'Missing time bounds'):
            self.check()

    def test_declared_optional_bounds_are_validated(self):
        self.fixture(bounds=True)
        self.assertEqual(self.check()[0]['finite_values'], 1460)
        with netCDF4.Dataset(self.path, 'r+') as ds:
            ds['time_bnds'][0, :] = [8., 4.]
        with self.assertRaisesRegex(ValueError, 'Invalid interval bounds'):
            self.check()

    def test_interval_daily_data_still_require_bounds(self):
        self.task['compound'] = 'atmos.hus.tavg-al-hxy-u.day.glb'
        self.fixture(np.arange(12., 365 * 24, 24.))
        metadata = dict(self.metadata, temporal_shape='time-intv', dimensions='time')
        with self.assertRaisesRegex(ValueError, 'Missing time bounds'):
            self.check(metadata=metadata)

    def test_unknown_or_absent_shape_does_not_disable_bounds_checks(self):
        self.fixture()
        for shape in (None, '', 'unknown'):
            with self.subTest(shape=shape):
                metadata = dict(self.metadata, temporal_shape=shape)
                with self.assertRaisesRegex(ValueError, 'Missing time bounds'):
                    self.check(metadata=metadata)

    def test_monthly_points_do_not_require_interval_endpoints(self):
        self.task['compound'] = 'atmos.hus.tpt-al-hxy-u.mon.glb'
        times = netCDF4.date2num([datetime(2605, month, 15) for month in range(1, 13)],
                                 'hours since 2605-01-01', calendar='standard')
        self.fixture(times)
        self.assertEqual(self.check()[0]['finite_values'], 12)
        with self.assertRaisesRegex(ValueError, 'Missing time bounds'):
            self.check(metadata=dict(self.metadata, temporal_shape='time-intv'))


class ReportingTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.campaign = Path(self.directory.name)
        self.attempt = self.campaign / 'attempt'
        self.attempt.mkdir()
        self.dest = self.campaign / 'representative_test'
        (self.dest / 'status').mkdir(parents=True)
        (self.campaign / 'status').mkdir()

    def write(self, path, value):
        r.f.write(path, value)

    def qc_findings(self):
        self.write(self.attempt / 'qc_example.json', {'product.nc': {
            'cf': {'high_count': 1, 'medium_count': 2, 'low_count': 0,
                   'scored_points': 8, 'possible_points': 11}}})

    def test_qc_findings_and_missing_reports_are_not_passes(self):
        self.assertEqual(r.qc_summary(self.attempt)['status'], 'NOT_REPORTED')
        self.qc_findings()
        result = r.qc_summary(self.attempt)
        self.assertEqual(result['status'], 'FINDINGS')
        self.assertEqual(result['checkers'][0]['high'], 1)
        self.assertEqual(result['checkers'][0]['medium'], 2)

    def test_qc_point_deficit_retained_without_priority_counts(self):
        self.write(self.attempt / 'qc_example.json', {'product.nc': {
            'cf': {'scored_points': 8, 'possible_points': 9}}})
        self.assertEqual(r.qc_summary(self.attempt)['status'], 'FINDINGS')

    def test_verify_uses_driver_policy_and_checks_frozen_helpers(self):
        helper = self.campaign / 'helper.py'
        helper.write_text('version = 1\n')
        manifest = {'hashes': {str(helper): r.f.digest(helper)}}
        with patch.object(r.f, 'verify_campaign') as verify:
            r.verify(manifest, self.campaign)
            verify.assert_called_once_with(self.campaign)
            helper.write_text('version = 2\n')
            with self.assertRaisesRegex(ValueError, 'Pinned test input/code/metadata changed'):
                r.verify(manifest, self.campaign)

    def test_worker_preserves_qc_findings_and_does_not_claim_source_audit(self):
        self.qc_findings()
        recipe = self.campaign / 'recipe.yaml'
        recipe.write_text('fixture: true\n')
        task = dict(compound=COMPOUND, year=2595, key='2595__' + COMPOUND,
                    recipe=str(recipe), recipe_sha256=r.f.digest(recipe))
        metadata = self.campaign / 'metadata.json'
        self.write(metadata, {'Compound Name': {COMPOUND: METADATA}})
        self.write(self.dest / 'manifest.json',
                   dict(selected=[task], config={'metadata': str(metadata)}, hashes={}))
        self.write(self.campaign / 'status' / (task['key'] + '.json'),
                   dict(status='PASS', output=str(self.attempt),
                        validation={'files': ['unused.nc']}))
        with patch.dict(os.environ, {'SLURM_JOB_ID': 'test-job', 'PYTHONOPTIMIZE': '1'}), \
             patch.object(r, 'verify') as verify, \
             patch.object(r.f, 'run_one') as run_one, \
             patch.object(r, 'check_data', return_value=[{'finite_values': 48}]), \
             redirect_stdout(io.StringIO()):
            self.assertEqual(r.worker(self.campaign, 0), 0)
            self.assertNotIn('PYTHONOPTIMIZE', os.environ)
        self.assertEqual(verify.call_count, 2)
        run_one.assert_called_once()
        record = r.f.read(self.dest / 'status' / (task['key'] + '.json'))
        self.assertEqual(record['status'], 'CONVERSION_AND_BASIC_CHECKS_PASS')
        self.assertEqual(record['qc']['status'], 'FINDINGS')
        self.assertFalse(record['independent_source_check']['raw_sample_comparison'])
        self.assertEqual(record['independent_source_check']['compared_samples'], 0)
        self.assertTrue(record['independent_source_check']['limitations'])
        self.assertFalse(record['publication_ready'])

    def test_report_keeps_blocked_and_absent_results_separate(self):
        blocked = 'ocean.sos.tavg-u-hxy-sea.mon.glb'
        task = dict(compound=COMPOUND, year=2595, key='2595__' + COMPOUND)
        self.write(self.dest / 'manifest.json', {'selected': [task]})
        self.write(self.campaign / 'coverage.json', [
            dict(compound=COMPOUND, year=2595, status='READY', reason=''),
            dict(compound=blocked, year=2595, status='BLOCKED', reason='Missing input')])
        with redirect_stdout(io.StringIO()):
            r.report(self.campaign)
        summary = r.f.read(self.dest / 'summary.json')
        self.assertEqual(summary['results'], {'BLOCKED': 1, 'PENDING_OR_NO_RESULT': 1})
        self.assertEqual(summary['representative_tests'], 1)
        self.assertEqual(summary['products_with_independent_samples'], 0)
        self.assertFalse(summary['all_requested_products_verified'])
        self.assertFalse(summary['publication_ready'])

    def test_empty_report_is_not_verified(self):
        self.write(self.dest / 'manifest.json', {'selected': []})
        self.write(self.campaign / 'coverage.json', [])
        with redirect_stdout(io.StringIO()):
            r.report(self.campaign)
        summary = r.f.read(self.dest / 'summary.json')
        self.assertEqual(summary['representative_tests'], 0)
        self.assertFalse(summary['all_requested_products_verified'])


if __name__ == '__main__':
    unittest.main()
