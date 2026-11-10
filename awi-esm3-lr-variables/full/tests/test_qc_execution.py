"""Internal checker errors must remain visible even when valid JSON exists."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from pycmor.std_lib import qc


class Tests(TestCase):
    def run_checker(self, directory, code, stderr, refresh=True):
        report=Path(directory)/'qc_tas.json'
        def process(*args,**kwargs):
            if refresh:
                report.write_text(json.dumps({'tas.nc':{'cf':dict(high_count=1,
                    medium_count=0,low_count=0,possible_points=2,scored_points=1)}}))
            return SimpleNamespace(returncode=code,stderr=stderr,stdout='checker stdout')
        with patch.object(qc.subprocess,'run',side_effect=process):
            result=qc._run_cchecker('checker',['cf'],'normal',report,[Path(directory)/'tas.nc'])
        return result,json.loads(qc._execution_path(report).read_text())

    def test_exception_with_valid_json_is_recorded_as_checker_error(self):
        with TemporaryDirectory() as directory:
            message='WARNING: The following exceptions occurred during the cf checker'
            result,evidence=self.run_checker(directory,2,message)
            self.assertEqual(result,(2,message))
            self.assertEqual(evidence['status'],'CHECKER_ERROR')
            self.assertTrue(evidence['checker_exception'])
            self.assertEqual(evidence['attempts'][0]['stderr'],message)
            self.assertEqual(evidence['attempts'][0]['stdout'],'checker stdout')
            self.assertEqual(evidence['requested_suites'],['cf'])

    def test_ordinary_findings_remain_distinct_from_execution_failure(self):
        with TemporaryDirectory() as directory:
            _,evidence=self.run_checker(directory,1,'normal checker progress')
            self.assertEqual(evidence['status'],'COMPLETED_WITH_FINDINGS')
            self.assertFalse(evidence['checker_exception'])

    def test_stale_json_does_not_establish_execution_success(self):
        with TemporaryDirectory() as directory:
            (Path(directory)/'qc_tas.json').write_text('{}')
            _,evidence=self.run_checker(directory,0,'',refresh=False)
            self.assertEqual(evidence['status'],'STALE_REPORT')

    def test_missing_report_preserves_failure_evidence(self):
        with TemporaryDirectory() as directory, patch.object(qc.time,'sleep'):
            _,evidence=self.run_checker(directory,1,'Traceback (most recent call last)',refresh=False)
            self.assertEqual(evidence['status'],'NO_REPORT')
            self.assertTrue(evidence['checker_exception'])
            self.assertEqual(len(evidence['attempts']),3)

    def test_lr_gate_rejects_checker_exception_even_when_findings_are_allowed(self):
        with TemporaryDirectory() as directory:
            root=Path(directory);datafile=root/'tas_example.nc';datafile.touch()
            rule=SimpleNamespace(qc_enabled=True,output_directory=str(root),
                cmor_variable='tas',qc_tests=['cf'],qc_fail_on_mandatory=False,
                qc_fail_on_checker_error=True)
            def process(command,**kwargs):
                report=Path(command[command.index('-o')+1])
                report.write_text(json.dumps({str(datafile):{'cf':dict(high_count=0,
                    medium_count=0,low_count=0,possible_points=1,scored_points=1)}}))
                return SimpleNamespace(returncode=2,stderr='checker failed internally',stdout='')
            with patch.object(qc.shutil,'which',return_value='/fake/checker'), \
                 patch.object(qc,'_strip_leading_underscore_attrs'), \
                 patch.object(qc.subprocess,'run',side_effect=process):
                with self.assertRaisesRegex(qc.QCFailure,'checker execution failed'):
                    qc.run_compliance_checker(None,rule)
