import json
from pathlib import Path
import tempfile
import unittest

from data_request_coverage import conditional_exclusion, load_request, write_report


class RequestCoverageTests(unittest.TestCase):
    def setUp(self):
        self.global_ = 'ocean.thetao.tavg-ol-hxy-sea.mon.glb'
        self.regional = self.global_.rsplit('.', 1)[0] + '.30S-90S'
        common = dict(units='degC', standard_name='sea_water_potential_temperature',
            dimensions='longitude latitude olevel time', cell_methods='area: mean time: mean',
            temporal_shape='time-intv')
        self.metadata = {self.global_:dict(common), self.regional:dict(common,
            processing_note='This is a regional subset; producing its global equivalent means omit this regional version.')}

    def report(self, rows, change=None):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name)
            request = path/'request.json'
            request.write_text(json.dumps({'experiment':{'piControl':{'Core':[self.global_], 'High':[self.regional]}}}))
            metadata = path/'metadata.json'
            metadata.write_text(json.dumps({'Compound Name':self.metadata}))
            config = dict(metadata=str(metadata), data_request_file=str(request))
            return write_report(path, config, rows)

    def test_only_full_task_completion_covers_a_global_alternative(self):
        ready = [dict(compound=self.global_, status='PASS'), dict(compound=self.global_,status='PENDING')]
        report = self.report(ready)
        self.assertEqual(report['statuses'], {'NOT_WRITTEN':2})
        ready[-1]['status'] = 'PASS'
        report = self.report(ready)
        self.assertEqual(report['statuses'], {'WRITTEN':1,'WRITTEN_GLOBAL_ALTERNATIVE':1})
        self.assertEqual(report['total_written_products'], 1)

    def test_branding_or_unit_mismatch_does_not_cover_regional_request(self):
        self.metadata[self.regional]['units'] = 'K'
        report = self.report([dict(compound=self.global_,status='PASS')])
        self.assertEqual(report['statuses'], {'WRITTEN':1,'NOT_WRITTEN':1})

    def test_duplicate_priorities_rejected(self):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name)/'request.json'
            path.write_text(json.dumps({'experiment':{'piControl':{'Core':[self.global_], 'High':[self.global_]}}}))
            with self.assertRaisesRegex(ValueError,'Duplicate'):
                load_request(path,'piControl',self.metadata)

    def test_conservative_temperature_condition_does_not_exempt_the_daily_even_case(self):
        monthly='ocean.bigthetao.tavg-ol-hxy-sea.mon.glb'
        daily='ocean.bigthetao.tavg-op20bar-hxy-sea.day.glb'
        meta={monthly:dict(comment='Diagnostic should be contributed only for models using conservative temperature as prognostic field.'),
              daily:dict(comment='Diagnostic should be contributed even for models using conservative temperature as prognostic field.')}
        self.assertIn('NOT_APPLICABLE',conditional_exclusion(monthly,meta,{'ocean_temperature':'potential'},{}))
        self.assertIsNone(conditional_exclusion(daily,meta,{'ocean_temperature':'potential'},{}))
        self.assertIsNone(conditional_exclusion(monthly,meta,{'ocean_temperature':'unknown'},{}))
        self.assertIsNone(conditional_exclusion(monthly,meta,{'ocean_temperature':'conservative'},{}))
