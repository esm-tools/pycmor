"""Raw filename compatibility; synthetic headers only, no site configuration."""
import copy
from datetime import datetime
import importlib.util
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

import netCDF4


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('test_raw_filename_adapter', ROOT / 'rules/raw_adapter.py')
RAW = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RAW)


class RawFilenamePatternTests(unittest.TestCase):
    def test_legacy_and_current_names_keep_exact_frequency_and_level(self):
        for frequency in ('1m', '1d', '1h', '3h', '6h'):
            for level in ('', '_pl'):
                for separator in ('-', r'\-'):
                    stream = frequency + level
                    old = rf'atm_remapped_{stream}_co2_{stream}_\d{{4}}{separator}\d{{4}}\.nc$'
                    pattern = RAW.raw_input_pattern('oifs', old)
                    self.assertTrue(pattern.startswith('^'))
                    self.assertTrue(pattern.endswith('$'))
                    self.assertEqual(RAW.raw_input_pattern('oifs', pattern), pattern)
                    for repeat in ('', stream + '_'):
                        name = f'atm_remapped_{stream}_co2_{repeat}2605-2605.nc'
                        with self.subTest(pattern=pattern, name=name):
                            self.assertIsNotNone(re.fullmatch(pattern, name))
                            for invalid in ('prefix_' + name, name + '.bak',
                                            name.replace('co2_', 'co2nbf_'),
                                            name.replace('2605-2605', '26050101-26051231')):
                                self.assertIsNone(re.search(pattern, invalid))
                    other = '1d' if frequency == '1m' else '1m'
                    self.assertIsNone(re.fullmatch(pattern, f'atm_remapped_{other}{level}_co2_2605-2605.nc'))
                    self.assertIsNone(re.fullmatch(pattern, f'atm_remapped_{stream}_co2_{other}{level}_2605-2605.nc'))
                    other_level = '' if level else '_pl'
                    self.assertIsNone(re.fullmatch(pattern, f'atm_remapped_{frequency}{other_level}_co2_2605-2605.nc'))

    def test_unrelated_or_noncanonical_patterns_are_unchanged(self):
        old = r'atm_remapped_1m_co2_1m_\d{4}-\d{4}\.nc$'
        self.assertEqual(RAW.raw_input_pattern('fesom', old), old)
        for pattern in (
                r'CO2f\.fesom\.\d{4}\.nc$', r'cnpp\.out$',
                r'atm_remapped_1m_co2_.*\.nc$',
                r'atm_remapped_1m_co2_1d_\d{4}-\d{4}\.nc$',
                r'atm_remapped_1m_pl_co2_1m_\d{4}-\d{4}\.nc$',
                r'atm_remapped_1m_co2_1m_pl_\d{4}-\d{4}\.nc$',
                r'atm_remapped_1m_co2_1m_2605-2605\.nc$'):
            with self.subTest(pattern=pattern):
                self.assertEqual(RAW.raw_input_pattern('oifs', pattern), pattern)
        self.assertEqual(RAW.raw_input_pattern('oifs', '^' + old), RAW.raw_input_pattern('oifs', old))


class RawFilenameMaterializeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        RAW.EXP = Path(self.directory.name)
        RAW.YEARS = [2605]
        RAW.PERIOD = '26050101-26051231'
        self.base = RAW.EXP / 'outdata/oifs'
        self.base.mkdir(parents=True)
        RAW.CARBON_CONTEXT = {}

    def source(self, variable, legacy=False, frequency='1m', units='kg m-2 s-1',
               months=range(1, 13), year=2605, cells=1, stored_variable=None):
        stream = frequency + ('_pl' if variable == 'co2' else '')
        repeated = stream + '_' if legacy else ''
        path = self.base / f'atm_remapped_{stream}_{variable}_{repeated}{year}-{year}.nc'
        months = list(months)
        with netCDF4.Dataset(path, 'w') as ds:
            ds.createDimension('time_counter', len(months))
            ds.createDimension('ncells', cells)
            time = ds.createVariable('time_counter', 'f8', ('time_counter',))
            time.units = f'days since {year}-01-01'
            time.calendar = 'gregorian'
            time[:] = netCDF4.date2num([datetime(year, month, 15) for month in months],
                                      time.units, time.calendar)
            dims = ('time_counter', 'ncells')
            if variable == 'co2':
                ds.createDimension('plev', 19)
                dims = ('time_counter', 'plev', 'ncells')
            data = ds.createVariable(stored_variable or variable, 'f4', dims)
            data.units = units
            data[:] = 0
        return path

    def rule(self, name='fco2nat'):
        return next(rule for rule in RAW.co2_rules() if rule['name'] == name)

    def test_materializes_both_conventions_without_mutating_rules(self):
        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                paths = [self.source(v, legacy=legacy) for v in ('co2nbf', 'co2fire', 'co2of')]
                original = self.rule()
                before = copy.deepcopy(original)
                group, result = RAW.materialize(original, {})
                self.assertEqual(group, 'co2')
                self.assertEqual(result['lr_source_files'], [str(paths[0])])
                self.assertEqual([term['files'] for term in result['lr_terms']], [[str(p)] for p in paths])
                self.assertEqual(original, before)
                for path in paths:
                    path.unlink()

    def test_normalizes_selector_and_terms_before_driver_replaced_inputs(self):
        paths = [self.source(v) for v in ('co2nbf', 'co2fire', 'co2of')]
        def indexed_inputs(component, pattern):
            self.assertEqual(component, 'oifs')
            self.assertTrue(pattern.startswith('^'))
            return [path for path in paths if re.fullmatch(pattern, path.name)]
        with patch.object(RAW, 'inputs', side_effect=indexed_inputs) as lookup:
            _, result = RAW.materialize(self.rule(), {})
        self.assertEqual(lookup.call_count, 4)
        self.assertEqual([term['files'] for term in result['lr_terms']], [[str(p)] for p in paths])

    def test_pressure_tracer_both_conventions_keep_units_and_scale(self):
        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                path = self.source('co2', legacy=legacy, units='kg*kg-1')
                rule = self.rule('co2_p19')
                _, result = RAW.materialize(rule, {})
                self.assertEqual(result['lr_source_files'], [str(path)])
                self.assertEqual(result['lr_scale'], rule['lr_scale'])
                self.assertEqual(result['expected_units'], rule['expected_units'])
                path.unlink()

    def test_duplicate_main_source_conventions_are_not_silently_selected(self):
        self.source('co2nbf')
        self.source('co2nbf', legacy=True)
        with self.assertRaisesRegex(ValueError, 'Incomplete/duplicate monthly coverage'):
            RAW.materialize(self.rule(), {})

    def test_duplicate_term_conventions_are_not_silently_selected(self):
        for variable in ('co2nbf', 'co2fire', 'co2of'):
            self.source(variable)
        self.source('co2fire', legacy=True)
        with self.assertRaisesRegex(ValueError, 'Incomplete/duplicate monthly coverage'):
            RAW.materialize(self.rule(), {})

    def test_daily_files_do_not_supply_missing_monthly_sources(self):
        self.source('co2nbf', frequency='1d')
        with self.assertRaisesRegex(ValueError, 'Missing source:'):
            RAW.materialize(self.rule(), {})

    def test_daily_files_do_not_mix_with_monthly_sources(self):
        for variable in ('co2nbf', 'co2fire', 'co2of'):
            self.source(variable)
            self.source(variable, frequency='1d')
        _, result = RAW.materialize(self.rule(), {})
        self.assertTrue(all('_1m_' in path for term in result['lr_terms'] for path in term['files']))

    def test_term_units_still_checked(self):
        self.source('co2nbf')
        self.source('co2fire', units='wrong')
        with self.assertRaisesRegex(ValueError, 'units.*expected'):
            RAW.materialize(self.rule(), {})

    def test_missing_variable_still_rejected(self):
        self.source('co2', units='kg*kg-1', stored_variable='not_co2')
        with self.assertRaises((KeyError, IndexError)):
            RAW.materialize(self.rule('co2_p19'), {})

    def test_incomplete_or_duplicate_months_still_rejected(self):
        for months in (range(1, 12), [1, 1] + list(range(3, 13))):
            with self.subTest(months=months):
                self.source('co2', units='kg*kg-1', months=months)
                with self.assertRaisesRegex(ValueError, 'Incomplete/duplicate monthly coverage'):
                    RAW.materialize(self.rule('co2_p19'), {})

    def test_spatial_shape_changes_still_rejected(self):
        RAW.YEARS = [2605, 2606]
        self.source('co2', units='kg*kg-1')
        self.source('co2', units='kg*kg-1', year=2606, cells=2)
        with self.assertRaisesRegex(ValueError, 'Spatial shape changed'):
            RAW.materialize(self.rule('co2_p19'), {})


if __name__ == '__main__':
    unittest.main()
