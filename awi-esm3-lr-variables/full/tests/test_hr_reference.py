import tempfile
from pathlib import Path
import unittest

from hr_reference import load_reference, summarize


class ReferenceTests(unittest.TestCase):
    def test_multiple_years_grids_versions_are_one_product(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'files.txt'
            path.write_text('r1i1p1f1/glb/mon/tas/tavg-h2m-hxy-u/g122/v1/a.nc\n'
                            'r1i1p1f1/glb/mon/tas/tavg-h2m-hxy-u/g999/v2/b.nc\n')
            compound = 'atmos.tas.tavg-h2m-hxy-u.mon.glb'
            self.assertEqual(load_reference(path, {'Compound Name': {compound: {}}}), {compound})

    def test_one_pass_not_all_years_complete(self):
        rows = [dict(compound='a', status='PASS'), dict(compound='a', status='BLOCKED', reason='missing year')]
        report = summarize({'a', 'b'}, rows)
        self.assertTrue(report[0]['lr_planned'])
        self.assertFalse(report[0]['lr_complete'])
        self.assertEqual(report[1]['reason'], 'NO_LR_RULE')

    def test_unknown_product_is_not_silently_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'files.txt'
            path.write_text('r1i1p1f1/glb/mon/unknown/brand/g122/v1/a.nc\n')
            with self.assertRaisesRegex(ValueError, 'unknown product'):
                load_reference(path, {'Compound Name': {}})

    def test_empty_reference_is_error(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'files.txt'; path.touch()
            with self.assertRaisesRegex(ValueError, 'no products'):
                load_reference(path, {'Compound Name': {}})
