"""Canonical carbon-rule regression tests; no site config or model data needed."""
import copy
import importlib.util
import json
from pathlib import Path
import re
import unittest
from unittest.mock import patch

import yaml


RULES = Path(__file__).resolve().parents[1] / 'rules'
SPEC = importlib.util.spec_from_file_location('test_full_carbon_adapter', RULES / 'raw_adapter.py')
ADAPTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ADAPTER)

# Independent compact reference for the existing 31 base conversions. These
# constants deliberately do not derive their expected values from the YAML.
BASE = {
    'fgco2': ('CO2f', 'mmolC/m2/d', 12.011e-6/86400, 'kg m-2 s-1', 'u'),
    'dissic': ('DIC', '[mmol/m3]', 1e-3, 'mol m-3', 'ol'),
    'talk': ('Alk', '[mmol/m3]', 1e-3, 'mol m-3', 'ol'),
    'dissoc': ('DOC', '[mmol/m3]', 1e-3, 'mol m-3', 'ol'),
    'dfe': ('DFe', '[mmol/m3]', 1e-6, 'mol m-3', 'ol'),
    'o2': ('O2', '[mmol/m3]', 1e-3, 'mol m-3', 'ol'),
    'si': ('DSi', '[mmol/m3]', 1e-3, 'mol m-3', 'ol'),
    'co3': ('CO3', '[mol/m3]', 1., 'mol m-3', 'ol'),
    'co2': ('CO2_aq', '[mol/m3]', 1., 'mol m-3', 'ol'),
    'ph': ('pH', 'total_scale', 1., '1', 'ol'),
    'spco2': ('pCO2s', 'uatm', 0.101325, 'Pa', 'u'),
    'dpco2': ('dpCO2s', 'uatm', 0.101325, 'Pa', 'u'),
    'fsfe': ('aFe', 'umolFe/m2/s', 1e-6, 'mol m-2 s-1', 'u'),
    'fsn': ('aN', 'mmolN/m2/s', 1e-3, 'mol m-2 s-1', 'u'),
    'intppdiat': ('NPPd', 'mmolC/m2/d', 1e-3/86400, 'mol m-2 s-1', 'u'),
    'intppnano': ('NPPn', 'mmolC/m2/d', 1e-3/86400, 'mol m-2 s-1', 'u'),
    'phydiat': ('DiaC', '[mmol/m3]', 1e-3, 'mol m-3', 'ol'),
    'phynano': ('PhyC', '[mmol/m3]', 1e-3, 'mol m-3', 'ol'),
    'chl': ('PhyChl', '[mg/m3]', 1e-6, 'kg m-3', 'ol'),
    'chldiat': ('DiaChl', '[mg/m3]', 1e-6, 'kg m-3', 'ol'),
    'phyc': ('PhyC', '[mmol/m3]', 1e-3, 'mol m-3', 'ol'),
    'zooc': ('HetC', '[mmol/m3]', 1e-3, 'mol m-3', 'ol'),
    'intpp': ('NPPn', 'mmolC/m2/d', 1e-3/86400, 'mol m-2 s-1', 'u'),
    'calc': ('PhyCalc', '[mmol/m3]', 1e-3, 'mol m-3', 'ol'),
    'bsi': ('DiaSi', '[mmol/m3]', 1e-3, 'mol m-3', 'ol'),
    'poc': ('DetC', '[mmol/m3]', 1e-3, 'mol m-3', 'ol'),
    'detoc': ('DetC', '[mmol/m3]', 1e-3, 'mol m-3', 'ol'),
    'zmicro': ('Zoo3C', '[mmol/m3]', 1e-3, 'mol m-3', 'ol'),
    'zmeso': ('HetC', '[mmol/m3]', 1e-3, 'mol m-3', 'ol'),
    'zmisc': ('Zoo2C', '[mmol/m3]', 1e-3, 'mol m-3', 'ol'),
    'pp': ('NPPn3D', 'mmolC/m2/d', 1e-3/86400, 'mol m-3 s-1', 'ol'),
}
SUMS = {
    'chl': ['PhyChl', 'DiaChl'], 'phyc': ['PhyC', 'DiaC'],
    'zooc': ['HetC', 'Zoo2C', 'Zoo3C'], 'intpp': ['NPPn', 'NPPd'],
    'calc': ['PhyCalc', 'DetCalc', 'idetz2calc'],
    'bsi': ['DiaSi', 'DetSi', 'idetz2si'],
    'poc': ['PhyC', 'DiaC', 'HetC', 'Zoo2C', 'Zoo3C', 'DetC', 'idetz2c'],
    'detoc': ['DetC', 'idetz2c'], 'pp': ['NPPn3D', 'NPPd3D'],
}
SURFACE_NAMES = {'dfe', 'o2', 'si', 'chl', 'phyc', 'calc', 'pp'}


class CarbonRulesTests(unittest.TestCase):
    def setUp(self):
        ADAPTER.META = {}
        ADAPTER.CARBON_CONTEXT = {}

    def test_all_recom_conversions_match_existing_reference(self):
        rules = [r for r in ADAPTER.recom_rules() if '.mon.glb' in r['compound_name']]
        self.assertEqual(len(rules), 31)
        self.assertEqual({r['name'] for r in rules}, set(BASE))
        for rule in rules:
            name = rule['name']
            source, units, scale, target, vertical = BASE[name]
            with self.subTest(name=name):
                self.assertEqual(rule['compound_name'], f'ocnBgchem.{name}.tavg-{vertical}-hxy-sea.mon.glb')
                self.assertEqual(rule['model_variable'], source)
                self.assertEqual(rule['expected_units'], units)
                self.assertEqual(rule['lr_scale'], scale)
                self.assertEqual(rule['lr_units'], target)
                self.assertEqual(rule.get('lr_exact_ocean_depth', False), vertical == 'ol')
                self.assertEqual(rule['lr_input'], {
                    'component': 'fesom', 'pattern': re.escape(source) + r'\.fesom\.\d{4}\.nc$',
                })
                self.assertEqual(rule['time_dimname'], 'time')
                self.assertEqual(rule['lr_grid_key'], 'core3')
                self.assertEqual(rule['lr_group'], 'recom')
                self.assertEqual(rule['grid_label'], 'gn')
                self.assertEqual(rule['grid'], 'CORE3 native 220509 nodes; exact model vertical layers')
                self.assertEqual(rule['pipelines'], ['lr_linear' if name in SUMS else 'lr_monthly'])
                expected_terms = [{'component': 'fesom', 'pattern': re.escape(s) + r'\.fesom\.\d{4}\.nc$',
                                   'variable': s, 'units': units, 'coefficient': scale}
                                  for s in SUMS.get(name, [])]
                self.assertEqual(rule.get('lr_terms', []), expected_terms)

    def test_existing_surface_metadata_preserves_38_products(self):
        ADAPTER.META = {f'ocnBgchem.{n}.tavg-ols-hxy-sea.mon.glb': {} for n in SURFACE_NAMES}
        rules = [r for r in ADAPTER.recom_rules() if '.mon.glb' in r['compound_name']]
        self.assertEqual(len(rules), 38)
        self.assertEqual(len({r['compound_name'] for r in rules}), 38)
        self.assertEqual({r['name'] for r in rules if r.get('surface_layer')},
                         {n + '_surface' for n in SURFACE_NAMES})
        for r in rules:
            if r.get('surface_layer'):
                name = r['name'].removesuffix('_surface')
                self.assertFalse(r['lr_exact_ocean_depth'])
                self.assertEqual(r['pipelines'], ['recom_surface_linear' if name in SUMS else 'recom_surface'])

    def test_surface_variants_follow_selected_metadata_not_fixed_list(self):
        ADAPTER.META = {'ocnBgchem.dissic.tavg-ols-hxy-sea.mon.glb': {}}
        names = {r['name'] for r in ADAPTER.recom_rules() if '.mon.glb' in r['compound_name']}
        self.assertIn('dissic_surface', names)
        self.assertNotIn('chl_surface', names)
        self.assertEqual(len(names), 32)

    def test_unsupported_targets_remain_candidates_without_metadata(self):
        names = {r['name'] for r in ADAPTER.recom_rules()}
        self.assertTrue({'bsi', 'co2', 'co3', 'phynano', 'poc'} <= names)

    def test_flux_sources_sign_and_carbon_conversion_unchanged(self):
        rules = {r['name']: r for r in ADAPTER.co2_rules()}
        expected = {'fco2nat': ['co2nbf', 'co2fire', 'co2of'],
                    'fco2antt': ['co2apf'],
                    'fco2all': ['co2nbf', 'co2fire', 'co2of', 'co2apf']}
        self.assertEqual(set(rules), set(expected) | {'co2_p19'})
        for name, sources in expected.items():
            r = rules[name]
            with self.subTest(name=name):
                self.assertEqual(r['compound_name'], f'atmos.{name}.tavg-u-hxy-u.mon.glb')
                self.assertEqual(r['model_variable'], sources[0])
                self.assertEqual(r['lr_input'], {'component': 'oifs',
                    'pattern': rf'atm_remapped_1m_{sources[0]}_1m_\d{{4}}-\d{{4}}\.nc$'})
                self.assertEqual(r['lr_terms'], [{'variable': s, 'units': 'kg m-2 s-1',
                    'coefficient': 12.01115/44.0095,
                    'pattern': rf'atm_remapped_1m_{s}_1m_\d{{4}}-\d{{4}}\.nc$'} for s in sources])
                self.assertEqual(r['pipelines'], ['lr_linear'])
                self.assertEqual(r['lr_units'], 'kg m-2 s-1')
                self.assertEqual(r['time_dimname'], 'time_counter')
                self.assertEqual(r['grid_label'], 'g271')

    def test_pressure_tracer_conversion_unchanged(self):
        r = next(r for r in ADAPTER.co2_rules() if r['name'] == 'co2_p19')
        self.assertEqual(r['compound_name'], 'atmos.co2.tavg-p19-hxy-air.mon.glb')
        self.assertEqual(r['expected_units'], 'kg*kg-1')
        self.assertEqual(r['lr_scale'], 28.9644/44.0095)
        self.assertEqual(r['lr_units'], 'mol mol-1')
        self.assertEqual(r['lr_input']['pattern'], r'atm_remapped_1m_pl_co2_1m_pl_\d{4}-\d{4}\.nc$')

    def test_scientific_overrides_and_group_identities_remain_documented(self):
        rules = {r['name']: r for r in ADAPTER.recom_rules()}
        self.assertIn('micromol Fe/m3', rules['dfe']['comment'])
        self.assertIn('no additional depth integration', rules['pp']['comment'])
        for name in ('zmicro', 'zmeso', 'zmisc'):
            self.assertIn('recom_sms and recom_init', rules[name]['comment'])

    def test_co2_comments_do_not_generalize_experiment_specific_pathways(self):
        for rule in ADAPTER.co2_rules():
            self.assertNotIn('this spinup', rule['comment'])
            self.assertNotIn('no fossil emissions reader in this experiment', rule['comment'])
            if rule['name'] != 'co2_p19':
                self.assertIn('run-specific review', rule['comment'])

    def test_optional_context_is_provenance_not_a_conversion_change(self):
        original = list(ADAPTER.co2_rules())
        ADAPTER.CARBON_CONTEXT = {'external_emissions_reader': False, 'wetland_ch4_routing': 'reviewed'}
        contextual = list(ADAPTER.co2_rules())
        for before, after in zip(original, contextual):
            self.assertIn('Operator-supplied carbon_context', after['comment'])
            self.assertIn('"external_emissions_reader":false', after['comment'])
            self.assertEqual({k: v for k, v in before.items() if k != 'comment'},
                             {k: v for k, v in after.items() if k != 'comment'})

    def test_configure_copies_optional_context_without_private_config(self):
        config = {'metadata': '/fixture/metadata.json', 'mesh_dir': '/fixture/mesh',
                  'carbon_context': {'external_emissions_reader': False}}
        with patch.object(Path, 'read_text', side_effect=[
                json.dumps({'Compound Name': {}}), yaml.safe_dump({'tier_files': []})]):
            ADAPTER.configure('/fixture/campaign', '/fixture/repo', '/fixture/experiment',
                              '20000101-20041231', config)
        config['carbon_context']['external_emissions_reader'] = True
        self.assertFalse(ADAPTER.CARBON_CONTEXT['external_emissions_reader'])

    def test_configure_rejects_nonmapping_context(self):
        config = {'metadata': '/fixture/metadata.json', 'mesh_dir': '/fixture/mesh',
                  'carbon_context': 'unknown'}
        with patch.object(Path, 'read_text', side_effect=[
                json.dumps({'Compound Name': {}}), yaml.safe_dump({'tier_files': []})]):
            with self.assertRaisesRegex(ValueError, 'carbon_context must be a mapping'):
                ADAPTER.configure('/fixture/campaign', '/fixture/repo', '/fixture/experiment',
                                  '20000101-20041231', config)

    def test_loaded_rules_and_surfaces_do_not_share_mutable_state(self):
        ADAPTER.META = {'ocnBgchem.chl.tavg-ols-hxy-sea.mon.glb': {}}
        rules = {r['name']: r for r in ADAPTER.recom_rules()}
        rules['chl_surface']['lr_terms'][0]['coefficient'] = -99
        self.assertEqual(rules['chl']['lr_terms'][0]['coefficient'], 1e-6)
        next(ADAPTER.recom_rules())['lr_input']['pattern'] = 'mutated'
        self.assertNotEqual(next(ADAPTER.recom_rules())['lr_input']['pattern'], 'mutated')

    def test_loader_rejects_unknown_schema_and_duplicate_rules(self):
        document = yaml.safe_load((RULES / 'co2.yaml').read_text())
        for invalid in (dict(document, schema_version=2),
                        dict(document, rules=document['rules'] + [copy.deepcopy(document['rules'][0])])):
            with patch.object(Path, 'read_text', return_value=yaml.safe_dump(invalid)):
                with self.assertRaises(ValueError):
                    list(ADAPTER.co2_rules())

    def test_yaml_files_have_review_requirements_and_no_personal_paths(self):
        for name in ('recom.yaml', 'co2.yaml'):
            text = (RULES / name).read_text()
            doc = yaml.safe_load(text)
            self.assertEqual(doc['scientific_assumptions']['configuration_key'], 'carbon_context')
            self.assertEqual(doc['scientific_assumptions']['eligibility_profile'], 'awiesm3-v3.5.1-cc-raw-v1')
            self.assertTrue(doc['scientific_assumptions']['configuration_evidence'])
            self.assertNotRegex(text, r'/albedo/(?:home|work/user)|/work/ab\d|/home/[a-z]/')


if __name__ == '__main__':
    unittest.main()
