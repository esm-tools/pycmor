"""Gap-closure raw rules: REcoM CMIP7 diagnostics, 200 m daily level, column
integrals, model-level CO2 and transient tracers. Synthetic files only."""
import copy
from datetime import datetime, timedelta
import importlib.util
from pathlib import Path
import re
import tempfile
import unittest

import netCDF4
import numpy as np
import yaml

RULES = Path(__file__).resolve().parents[1] / 'rules'
SPEC = importlib.util.spec_from_file_location('gap_closure_adapter', RULES / 'raw_adapter.py')
RAW = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RAW)

# CORE3 interfaces from aux3d.out; layer centres are their midpoints.
EDGES = np.array([0, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 115, 135, 160, 190, 230, 280, 340, 410, 490,
                  580, 680, 790, 910, 1040, 1180, 1330, 1500, 1700, 1920, 2150, 2400, 2650, 2900, 3150, 3400,
                  3650, 3900, 4150, 4400, 4650, 4900, 5150, 5400, 5650, 6000, 6250], dtype=float)
CENTRES = 0.5 * (EDGES[:-1] + EDGES[1:])
DAY, MMOL, CARBON = 86400., 1e-3, 12.011e-6

# Independent reference: source field, source units, factor, target units.
REFERENCE = {
    'no3': ('DIN', '[mmol/m3]', MMOL, 'mol m-3'),
    'po4': ('DIN', '[mmol/m3]', MMOL / 16, 'mol m-3'),
    'graz': ('graz', 'mmolC/(m3*d)', MMOL / DAY, 'mol m-3 s-1'),
    'o2sat': ('O2sat', 'mmolO/m3', MMOL, 'mol m-3'),
    'dpo2': ('dpO2s', 'Pa', 1., 'Pa'),
    'limndiat': ('limNd', '1', 1., '1'),
    'limfediat': ('limFed', '1', 1., '1'),
    'expc': ('expC', 'mmolC/m2/s', MMOL, 'mol m-2 s-1'),
    'expc_100m': ('expC', 'mmolC/m2/s', MMOL, 'mol m-2 s-1'),
    'expc_1000m': ('expC', 'mmolC/m2/s', MMOL, 'mol m-2 s-1'),
    'expcalc': ('expCalc', 'mmolC/m2/s', MMOL, 'mol m-2 s-1'),
    'expcalc_100m': ('expCalc', 'mmolC/m2/s', MMOL, 'mol m-2 s-1'),
    'expcalc_1000m': ('expCalc', 'mmolC/m2/s', MMOL, 'mol m-2 s-1'),
    'epn_100m': ('expN', 'mmolN/m2/s', MMOL, 'mol m-2 s-1'),
    'epsi_100m': ('expSi', 'mmolSi/m2/s', MMOL, 'mol m-2 s-1'),
    'expcob_daily': ('bflxC', 'mmolC/m2/s', MMOL, 'mol m-2 s-1'),
    'expcob': ('bflxC', 'mmolC/m2/s', MMOL, 'mol m-2 s-1'),
    'froc': ('bflxC', 'mmolC/m2/s', MMOL, 'mol m-2 s-1'),
    'expcalcob': ('bflxCalc', 'mmolC/m2/s', MMOL, 'mol m-2 s-1'),
    'fric': ('bflxCalc', 'mmolC/m2/s', MMOL, 'mol m-2 s-1'),
    'expnob': ('bflxN', 'mmolN/m2/s', MMOL, 'mol m-2 s-1'),
    'frn': ('bflxN', 'mmolN/m2/s', MMOL, 'mol m-2 s-1'),
    'expsiob': ('bflxSi', 'mmolSi/m2/s', MMOL, 'mol m-2 s-1'),
    'o2min_daily': ('O2min', 'mmolO/m3', MMOL, 'mol m-3'),
    'intpoc_daily': ('intPOC', 'mmolC/m2', CARBON, 'kg m-2'),
    'o2_200': ('O2', '[mmol/m3]', MMOL, 'mol m-3'),
    'ph_200': ('pH', 'total_scale', 1., '1'),
    'co3_200': ('CO3', '[mol/m3]', 1., 'mol m-3'),
    'co3satarag_200': ('CO3satA', '[mol/m3]', 1., 'mol m-3'),
}
LINEAR = {
    'intdic_south30': ({'DIC': CARBON}, 'kg m-2'),
    'intdoc_south30': ({'DOC': CARBON}, 'kg m-2'),
    'intpoc_south30': ({v: CARBON for v in ('PhyC', 'DiaC', 'HetC', 'Zoo2C', 'Zoo3C', 'DetC', 'idetz2c')}, 'kg m-2'),
    'intpcalcite_south30': ({'calcif': MMOL / DAY}, 'mol m-2 s-1'),
    'intpbsi_south30': ({'intpbsi': MMOL / DAY}, 'mol m-2 s-1'),
    'chl_200': ({'PhyChl': 1e-6, 'DiaChl': 1e-6}, 'kg m-3'),
    'phyc_200': ({'PhyC': MMOL, 'DiaC': MMOL}, 'mol m-3'),
    'expfeob': ({'bflxN': None}, 'mol m-2 s-1'),
    'frfe': ({'bflxN': None, 'scavFe': 1e-6 / DAY}, 'mol m-2 s-1'),
}


class RuleCatalogueTests(unittest.TestCase):
    def setUp(self):
        RAW.META = {'ocnBgchem.no3.tavg-ols-hxy-sea.mon.glb': {}}
        RAW.CARBON_CONTEXT = {}

    def test_new_recom_conversions_match_independent_reference(self):
        rules = {r['name']: r for r in RAW.recom_rules()}
        self.assertTrue(set(REFERENCE) | set(LINEAR) <= set(rules))
        for name, (source, units, factor, target) in REFERENCE.items():
            rule = rules[name]
            with self.subTest(name=name):
                self.assertEqual(rule['model_variable'], source)
                self.assertEqual(rule['expected_units'], units)
                self.assertAlmostEqual(rule['lr_scale'] / factor, 1., places=12)
                self.assertEqual(rule['lr_units'], target)
                self.assertEqual(rule['lr_group'], 'recom')
        for name, (terms, target) in LINEAR.items():
            rule = rules[name]
            with self.subTest(name=name):
                self.assertEqual([t['variable'] for t in rule['lr_terms']], list(terms))
                for term in rule['lr_terms']:
                    expected = terms[term['variable']]
                    if expected is None:
                        self.assertIsNone(term['coefficient'])
                        self.assertEqual(term['fe2n_factor'], 1e-6)
                    else:
                        self.assertAlmostEqual(term['coefficient'] / expected, 1., places=12)
                self.assertEqual(rule['lr_units'], target)

    def test_fixed_depths_regions_and_surface_variant(self):
        rules = {r['name']: r for r in RAW.recom_rules()}
        for name, rule in rules.items():
            brand = rule['compound_name'].split('.')[2]
            depth = {'tavg-d100m-hxy-sea': 100., 'tavg-d1000m-hxy-sea': 1000.,
                     'tavg-op20bar-hxy-sea': 200.}.get(brand)
            with self.subTest(name=name):
                self.assertEqual(rule.get('lr_target_depth'), depth)
                if rule['compound_name'].endswith('30S-90S'):
                    self.assertEqual(rule['lr_region'], '30S-90S')
                    self.assertIn('south30', rule['pipelines'][0])
        for name in ('intdic_south30', 'intdoc_south30', 'intpoc_south30', 'intpcalcite_south30'):
            self.assertEqual(rules[name]['pipelines'], ['lr_south30_integral'])
            self.assertEqual(rules[name]['lr_thickness']['variable'], 'hnode')
        self.assertEqual(rules['no3_surface']['compound_name'], 'ocnBgchem.no3.tavg-ols-hxy-sea.mon.glb')
        self.assertTrue(rules['expc']['lr_interface_midpoints'])
        self.assertIn('Redfield', rules['po4']['comment'])
        self.assertEqual(rules['expcob']['lr_source_frequency'], 'day')

    def test_co2_model_level_and_near_surface(self):
        rules = {r['name']: r for r in RAW.co2_rules()}
        ml, near = rules['co2_ml'], rules['co2_h2m']
        self.assertEqual(ml['compound_name'], 'atmos.co2.tavg-al-hxy-u.mon.glb')
        self.assertEqual(ml['lr_scale'], 28.9644 / 44.0095)
        self.assertEqual(ml['lr_units'], 'mol mol-1')
        self.assertTrue(ml['lr_hybrid_levels'])
        self.assertEqual(near['compound_name'], 'atmos.co2.tavg-h2m-hxy-u.mon.glb')
        self.assertAlmostEqual(near['lr_scale'] / (28.9644 / 44.0095 * 1e6), 1., places=12)
        self.assertEqual(near['lr_units'], '1E-06')
        for rule in (ml, near):
            self.assertEqual(rule['grid_label'], 'g235')
            self.assertEqual(rule['lr_input']['pattern'], r'atmos_mon_ml_co2_\d{4}-\d{4}\.nc$')
        self.assertNotIn('co2Clim', {r['name'] for r in rules.values()})
        self.assertFalse(any('tclm' in r['compound_name'] for r in rules.values()))

    def test_transit_tracers(self):
        rules = {r['name']: r for r in RAW.transit_rules()}
        self.assertEqual(set(rules), {'sf6', 'thetao_200'})
        sf6 = rules['sf6']
        self.assertEqual(sf6['compound_name'], 'ocean.sf6.tavg-ol-hxy-sea.mon.glb')
        self.assertEqual((sf6['expected_units'], sf6['lr_units'], sf6['lr_scale']), ('mol m-3', 'mol m-3', 1))
        self.assertEqual(sf6['lr_input']['pattern'], r'sf6\.fesom\.\d{4}\.nc$')
        self.assertIn('zero', sf6['comment'])
        self.assertTrue(all(r['lr_group'] == 'ocean' for r in rules.values()))
        text = (RULES / 'transit.yaml').read_text()
        self.assertIn('# - name: cfc11', text)
        self.assertIn('# - name: cfc12', text)

    def test_every_rule_pipeline_is_registered(self):
        RAW.REPO = RULES.parents[2]
        profile = yaml.safe_load((RULES.parents[1] / 'profile.yaml').read_text())
        RAW.register_daily_pipelines(profile, Path('/campaign'))
        RAW.register_derived_pipelines(profile, Path('/campaign'))
        pipes = {p['name'] for p in profile['pipelines']}
        used = {p for r in list(RAW.recom_rules()) + list(RAW.co2_rules()) + list(RAW.transit_rules())
                for p in r['pipelines']}
        # The two surface pipelines are added by build_catalog itself.
        self.assertTrue(used <= pipes | {'recom_surface', 'recom_surface_linear'})


class MaterializeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        RAW.EXP = Path(self.tmp.name)
        RAW.YEARS = [2000]
        RAW.PERIOD = '20000101-20001231'
        RAW.META = {}
        RAW.CARBON_CONTEXT = {}
        RAW.MODEL_LEVEL_TABLE = None
        self.fesom = RAW.EXP / 'outdata/fesom'
        self.oifs = RAW.EXP / 'outdata/oifs'
        self.fesom.mkdir(parents=True)
        self.oifs.mkdir(parents=True)
        self.rules = {r['name']: r for r in list(RAW.recom_rules()) + list(RAW.co2_rules()) + list(RAW.transit_rules())}

    def write(self, path, variable, units, levels=None, daily=False, dims=('nod2',), time='time'):
        if daily:
            times = [datetime(2000, 1, 1, 12) + timedelta(days=i) for i in range(366)]
        else:
            times = [datetime(2000, m, 15) for m in range(1, 13)]
        with netCDF4.Dataset(path, 'w') as ds:
            ds.createDimension(time, len(times))
            for d in dims:
                ds.createDimension(d, 2)
            shape = [d for d in dims]
            if levels is not None:
                name, values = levels
                ds.createDimension(name, len(values))
                ds.createVariable(name, 'f4', (name,))[:] = values
                shape.append(name)
            t = ds.createVariable(time, 'f8', (time,)); t.units = 'days since 2000-01-01'
            t.calendar = 'gregorian'; t[:] = netCDF4.date2num(times, t.units, t.calendar)
            v = ds.createVariable(variable, 'f4', (time, *shape))
            v.units = units; v.cell_methods = 'time: mean'
            v[:] = 1.
        return path

    def test_existing_streams_materialize_now(self):
        for stem, variable in (('DIN', 'DIN'), ('DIC', 'DIC'), ('hnode', 'hnode')):
            self.write(self.fesom / f'{stem}.fesom.2000.nc', variable,
                       'm' if variable == 'hnode' else '[mmol/m3]', ('nz', CENTRES))
        for name in ('no3', 'po4'):
            _, rule = RAW.materialize(self.rules[name], {'core3': '/fixture/core3.nc'})
            self.assertEqual(rule['lr_source_files'], [str(self.fesom / 'DIN.fesom.2000.nc')])
        group, rule = RAW.materialize(self.rules['intdic_south30'], {'core3': '/fixture/core3.nc'})
        self.assertEqual(group, 'recom')
        thickness = [str(self.fesom / 'hnode.fesom.2000.nc')]
        self.assertEqual(rule['lr_thickness_files'], thickness)
        self.assertEqual(rule['lr_auxiliary_files'], thickness)
        self.assertNotIn('lr_thickness', rule)
        self.assertNotIn('lr_thickness_files', self.rules['intdic_south30'])

    def test_unwritten_streams_report_missing_input_with_pattern(self):
        namelist = RAW.EXP / f'run_{RAW.PERIOD}/config/fesom/namelist.recom'
        namelist.parent.mkdir(parents=True)
        namelist.write_text('&pairon\n fe2n = 0.033\n/\n')
        RAW.MODEL_LEVEL_TABLE = str(RAW.EXP / 'vtable_L91')
        Path(RAW.MODEL_LEVEL_TABLE).write_text('table')
        expected = {'expc': 'expC', 'graz': 'graz', 'o2min_daily': 'O2min', 'intpoc_daily': 'intPOC',
                    'expcob': 'bflxC', 'frfe': 'bflxN', 'o2_200': 'O2_200', 'chl_200': 'PhyChl_200',
                    'thetao_200': 'temp_200', 'sf6': 'sf6', 'co2_ml': 'atmos_mon_ml_co2',
                    'co2_h2m': 'atmos_mon_ml_co2', 'dpo2': 'dpO2s', 'limndiat': 'limNd'}
        for name, stem in expected.items():
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, r'^MISSING_INPUT: .*' + re.escape(stem)):
                    RAW.materialize(self.rules[name], {'core3': '/fixture/core3.nc'})

    def test_missing_term_names_the_variable_and_pattern(self):
        for variable in ('co2nbf', 'co2of'):
            self.write(self.oifs / f'atm_remapped_1m_{variable}_2000-2000.nc', variable, 'kg m-2 s-1',
                       dims=('lat',), time='time_counter')
        with self.assertRaisesRegex(ValueError, r'MISSING_INPUT: co2fire: oifs/\^atm_remapped_1m_co2fire_'):
            RAW.materialize(self.rules['fco2nat'], {})
        name = 'atm_remapped_1m_co2fire_2000-2000.nc'
        pattern = RAW.raw_input_pattern('oifs', self.rules['fco2nat']['lr_terms'][1]['pattern'])
        self.assertIsNotNone(re.fullmatch(pattern, name))
        self.assertIsNotNone(re.fullmatch(pattern, 'atm_remapped_1m_co2fire_1m_2000-2000.nc'))
        self.write(self.oifs / name, 'co2fire', 'kg m-2 s-1', dims=('lat',), time='time_counter')
        _, rule = RAW.materialize(self.rules['fco2nat'], {})
        self.assertEqual(rule['lr_terms'][1]['files'], [str(self.oifs / name)])

    def test_future_streams_materialize_once_written(self):
        grid = {'core3': '/fixture/core3.nc'}
        self.write(self.fesom / 'expC.fesom.2000.nc', 'expC', 'mmolC/m2/s', ('nz1', EDGES))
        _, rule = RAW.materialize(self.rules['expc_1000m'], grid)
        self.assertEqual(rule['lr_target_depth'], 1000.)
        self.write(self.fesom / 'bflxC.fesom.2000.nc', 'bflxC', 'mmolC/m2/s', daily=True)
        for name in ('expcob', 'froc', 'expcob_daily'):
            _, rule = RAW.materialize(self.rules[name], grid)
            self.assertEqual(rule['lr_source_files'], [str(self.fesom / 'bflxC.fesom.2000.nc')])
        for stem, variable in (('PhyChl_200', 'PhyChl'), ('DiaChl_200', 'DiaChl')):
            self.write(self.fesom / f'{stem}.fesom.2000.nc', variable, '[mg/m3]', ('nz_200', [175., 210.]),
                       daily=True)
        _, rule = RAW.materialize(self.rules['chl_200'], grid)
        self.assertEqual([t['files'] for t in rule['lr_terms']],
                         [[str(self.fesom / f'{stem}_200.fesom.2000.nc')] for stem in ('PhyChl', 'DiaChl')])
        self.write(self.fesom / 'sf6.fesom.2000.nc', 'sf6', 'mol m-3', ('nz', CENTRES))
        group, rule = RAW.materialize(self.rules['sf6'], grid)
        self.assertEqual(group, 'ocean')

    def test_iron_flux_reads_leg_fe2n(self):
        namelist = RAW.EXP / f'run_{RAW.PERIOD}/config/fesom/namelist.recom'
        namelist.parent.mkdir(parents=True)
        namelist.write_text('&pairon\n fe2n = 0.033\n fe2n_benthos = 0.15\n/\n')
        self.write(self.fesom / 'bflxN.fesom.2000.nc', 'bflxN', 'mmolN/m2/s')
        self.write(self.fesom / 'scavFe.fesom.2000.nc', 'scavFe', 'umolFe/m2/d')
        original = copy.deepcopy(self.rules['frfe'])
        _, rule = RAW.materialize(self.rules['frfe'], {'core3': '/fixture/core3.nc'})
        np.testing.assert_allclose([t['coefficient'] for t in rule['lr_terms']], [0.033e-6, 1e-6 / DAY], rtol=1e-12)
        self.assertFalse(any('fe2n_factor' in t for t in rule['lr_terms']))
        self.assertIn(str(namelist), rule['lr_auxiliary_files'])
        self.assertEqual(self.rules['frfe'], original)
        _, rule = RAW.materialize(self.rules['expfeob'], {'core3': '/fixture/core3.nc'})
        np.testing.assert_allclose([t['coefficient'] for t in rule['lr_terms']], [0.033e-6], rtol=1e-12)

    def test_model_level_co2_pins_table_and_surface_pressure(self):
        self.write(self.oifs / 'atmos_mon_ml_co2_2000-2000.nc', 'co2', 'kg*kg-1',
                   ('model_levels', np.arange(1., 92.)), dims=('cell',), time='time_counter')
        rule = self.rules['co2_ml']
        with self.assertRaisesRegex(ValueError, 'MISSING_MODEL_LEVEL_TABLE'):
            RAW.materialize(rule, {})
        RAW.MODEL_LEVEL_TABLE = str(RAW.EXP / 'vtable_L91')
        Path(RAW.MODEL_LEVEL_TABLE).write_text('table')
        with self.assertRaisesRegex(ValueError, 'MISSING_INPUT: sp: oifs/atmos_6h_pt_sp_'):
            RAW.materialize(rule, {})
        sp = self.oifs / 'atmos_6h_pt_sp_2000-2000.nc'
        sp.touch()
        group, result = RAW.materialize(rule, {})
        self.assertEqual(group, 'co2')
        self.assertEqual(result['oifs_vtable'], str(Path(RAW.MODEL_LEVEL_TABLE).resolve()))
        self.assertEqual((result['ps_path'], result['ps_variable']), (str(self.oifs), 'sp'))
        self.assertIsNotNone(re.fullmatch(result['ps_pattern'], sp.name))
        self.assertIsNone(re.fullmatch(result['ps_pattern'], 'atmos_6h_pt_sp_2001-2001.nc'))
        self.assertIn(str(sp), result['lr_auxiliary_files'])
        _, near = RAW.materialize(self.rules['co2_h2m'], {})
        self.assertNotIn('oifs_vtable', near)


if __name__ == '__main__':
    unittest.main()
