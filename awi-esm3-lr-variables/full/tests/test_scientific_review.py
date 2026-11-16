import copy
import json
from pathlib import Path
import unittest

from scientific_review import review_document


REVIEW=json.loads((Path(__file__).resolve().parents[1]/'config/scientific_review.json').read_text())
CONFIG={'scientific_profile':REVIEW['profile']}
IDENTITY={'fesom_sha256':REVIEW['fesom_sha256']}


class ScientificReviewTests(unittest.TestCase):
    def doc(self,compound,**fields):
        return {'rules':[dict(compound_name=compound,**fields)]}

    def test_every_confirmed_mismatch_has_an_explicit_action(self):
        confirmed=[r for r in REVIEW['findings'] if r['verdict']=='CONFIRMED_MISMATCH']
        self.assertTrue(confirmed)
        self.assertTrue(all(r['action'] not in ('documented','review_required') for r in confirmed))

    def test_wrong_physical_proxy_is_blocked_on_every_route(self):
        for product in ('atmos.ci.tavg-u-hxy-u.mon.glb','ocean.difmxylo.tavg-ol-hxy-sea.yr.glb'):
            with self.subTest(product=product),self.assertRaisesRegex(ValueError,'SCIENTIFIC_MAPPING_DISABLED'):
                review_document(self.doc(product),IDENTITY,CONFIG)

    def test_repaired_root_depth_is_enabled_and_keeps_its_open_review(self):
        rule=review_document(self.doc('land.rootd.ti-u-hxy-lnd.fx.glb'),IDENTITY,CONFIG)['rules'][0]
        actions={r['id']:r['action'] for r in rule['lr_scientific_review']}
        self.assertEqual(actions['F42'],'reconstructed_htessel_root_layer_depth')
        self.assertEqual(actions['F45'],'review_required')

    def test_constants_are_bound_to_the_reviewed_model_build(self):
        doc=self.doc('seaIce.simass.tavg-u-hxy-si.mon.glb',scale_factor=917.)
        with self.assertRaisesRegex(ValueError,'does not match'):
            review_document(copy.deepcopy(doc),{'fesom_sha256':'other'},CONFIG)
        out=review_document(doc,IDENTITY,CONFIG)
        self.assertEqual(out['rules'][0]['scale_factor'],910.)

    def test_v352_profile_requires_its_exact_build_and_geographic_vectors(self):
        profile='awiesm3-v3.5.2-cc-94377dfc'
        build=REVIEW['compatible_profiles'][profile]
        identity={'fesom_sha256':build['fesom_sha256'],'vector_frame':'geographic'}
        doc=self.doc('ocean.masso.tavg-u-hm-sea.mon.glb',scale_factor=1025.)
        out=review_document(copy.deepcopy(doc),identity,{'scientific_profile':profile})
        self.assertEqual(out['rules'][0]['scale_factor'],1030.)
        for wrong in [dict(identity,vector_frame='rotated'),dict(identity,fesom_sha256=REVIEW['fesom_sha256'])]:
            with self.subTest(identity=wrong),self.assertRaisesRegex(ValueError,'does not match'):
                review_document(copy.deepcopy(doc),wrong,{'scientific_profile':profile})

    def test_v352_retains_wrong_proxy_exclusions(self):
        profile='awiesm3-v3.5.2-cc-94377dfc'
        identity=dict(fesom_sha256=REVIEW['compatible_profiles'][profile]['fesom_sha256'],vector_frame='geographic')
        with self.assertRaisesRegex(ValueError,'SCIENTIFIC_MAPPING_DISABLED'):
            review_document(self.doc('atmos.ci.tavg-u-hxy-u.mon.glb'),identity,{'scientific_profile':profile})

    def test_ocean_and_seaice_water_densities_are_not_conflated(self):
        doc=self.doc('ocean.masso.tavg-u-hm-sea.mon.glb',scale_factor=1025.)
        self.assertEqual(review_document(doc,IDENTITY,CONFIG)['rules'][0]['scale_factor'],1030.)
        doc=self.doc('seaIce.sifb.tavg-u-hxy-si.mon.glb',rho_snow=330.,rho_water=1025.)
        rule=review_document(doc,IDENTITY,CONFIG)['rules'][0]
        self.assertEqual(rule['rho_snow'],290.)
        self.assertEqual(rule['rho_water'],1025.)
        doc=self.doc('ocean.umo.tavg-ol-hxy-sea.mon.glb',reference_density=1025.,scale_factor=.001)
        rule=review_document(doc,IDENTITY,CONFIG)['rules'][0]
        self.assertEqual(rule['reference_density'],1030.)
        self.assertEqual(rule['scale_factor'],.001)

    def test_speed_is_evaluated_before_daily_or_monthly_averaging(self):
        doc=self.doc('atmos.sfcWind.tavg-h10m-hxy-u.mon.glb',inputs=[{'pattern':'monthly'}],second_input_pattern='monthly_v')
        rule=review_document(doc,IDENTITY,CONFIG)['rules'][0]
        self.assertIn('1h_sfc_10u',rule['inputs'][0]['pattern'])
        self.assertIn('1h_sfc_10v',rule['second_input_pattern'])
        self.assertEqual(rule['grid_label'],'g235')

    def test_humidity_is_evaluated_before_daily_or_monthly_averaging(self):
        doc=self.doc('atmos.hurs.tavg-h2m-hxy-u.mon.glb',inputs=[{'pattern':'monthly'}],second_input_pattern='monthly_dewpoint')
        rule=review_document(doc,IDENTITY,CONFIG)['rules'][0]
        self.assertIn('1h_sfc_2t',rule['inputs'][0]['pattern'])
        self.assertIn('1h_sfc_2d',rule['second_input_pattern'])
        self.assertEqual(rule['grid_label'],'g235')
        self.assertIn('sub-hour covariance',rule['variable_attributes']['comment'])
