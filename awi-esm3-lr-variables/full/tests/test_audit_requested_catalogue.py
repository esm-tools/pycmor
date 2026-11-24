"""Catalogue and helper tests need no model archive or site installation."""
from contextlib import redirect_stdout
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import audit_requested_catalogue as audit
import full_cmorization as f


REPO = Path(__file__).resolve().parents[3]


class CatalogueTests(unittest.TestCase):
    def test_portable_catalogue_counts_with_fixture_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            metadata = base / 'metadata.json'
            config_file = base / 'site.json'
            f.write(metadata, {'Compound Name': {}})
            f.write(config_file, {'fixture': True})
            config = dict(repo=str(REPO), metadata=str(metadata), mesh_dir=str(base / 'mesh'))
            with patch.object(f, 'load_config', return_value=config) as load:
                summary, records, coverage = audit.audit(config_file)
            load.assert_called_once_with(config_file)
            self.assertEqual(summary['full_tier_files'], 17)
            self.assertEqual(summary['groups']['full_cmip7xml']['distinct_products'], 620)  # Nine new ocean mappings; two corrected inventories already existed.
            self.assertIn('shared_recom', summary['groups'])
            self.assertIn('shared_co2', summary['groups'])
            self.assertNotIn('private_recom', summary['groups'])
            land_source = audit.WORKFLOW_ROOT / 'rules/requested_land.py'
            self.assertEqual(summary['source_sha256'][str(land_source)], audit.sha256(land_source))
            land = [r for r in records if 'shared_requested_land' in r['catalogue_groups']]
            self.assertEqual({r['variable'] for r in land}, {
                'depthl', 'evspsblsoi', 'evspsblveg', 'lithk', 'mrros', 'rsds',
                'sftlaf', 'snm', 'vegFrac',
            })
            for record in land:
                with self.subTest(compound=record['compound']):
                    self.assertTrue(record['has_implemented_rule'])
                    self.assertEqual(record['observed_status_counts'], {})
                    source, = [s for s in record['sources'] if s['kind'] == 'shared_requested_land']
                    self.assertEqual(source['path'], str(land_source))
                    self.assertEqual(source['tier'], 'requested_land')
            self.assertEqual(summary['repo'], str(REPO))
            self.assertEqual(summary['workflow_root'], str(REPO / 'awi-esm3-lr-variables/full'))
            self.assertTrue(records)
            self.assertEqual(coverage, [])

    def test_cli_uses_configuration_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            (base / 'findings.txt').write_text('fixture findings\n')
            config = base / 'site.json'
            with patch.dict(os.environ, {'PYCMOR_LR_CONFIG': str(config)}), \
                 patch.object(audit, 'audit', return_value=({}, [], [])) as inspect, \
                 patch.object(audit, 'save_reports'), redirect_stdout(io.StringIO()):
                self.assertEqual(audit.main(['--output-dir', str(base)]), 0)
            inspect.assert_called_once_with(config, None)

    def test_report_refuses_source_tree(self):
        with self.assertRaisesRegex(ValueError, 'outside the repository source tree'):
            audit.save_reports(REPO / 'awi-esm3-lr-variables/full/reports',
                               {'repo': str(REPO)}, [], [])


if __name__ == '__main__':
    unittest.main()
