"""A failed source audit must never submit conversions."""
import os
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

import start_verified_leg as s


class VerifiedStartTests(TestCase):
    def test_no_login_node_conversion(self):
        with patch.dict(os.environ, {}, clear=True), self.assertRaisesRegex(ValueError, 'Slurm'):
            s.start(Path('.'), '26100101-26141231', Path('unused'))

    def test_source_audit_failure_blocks_planning_and_submission(self):
        with TemporaryDirectory() as directory:
            root=Path(directory)
            config=dict(results=str(root/'results'),python='python')
            with patch.dict(os.environ, {'SLURM_JOB_ID':'fixture'}), \
                 patch.object(s.f,'load_config',return_value=config), \
                 patch.object(s.f,'select_leg'), \
                 patch.object(s.subprocess,'run',side_effect=subprocess.CalledProcessError(1,'audit')), \
                 patch.object(s.f,'plan') as plan, patch.object(s.r,'submit') as submit:
                with self.assertRaises(subprocess.CalledProcessError):
                    s.start(root,'26100101-26141231',root/'config.json')
                plan.assert_not_called()
                submit.assert_not_called()

    def test_all_ready_products_are_gated_after_fresh_audit(self):
        with TemporaryDirectory() as directory:
            root=Path(directory);campaign=root/'campaign'
            s.f.write(campaign/'representative_test/submission.json',dict(workers='test',gate='gate'))
            config=dict(results=str(root/'results'),python='python')
            with patch.dict(os.environ, {'SLURM_JOB_ID':'fixture'}), \
                 patch.object(s.f,'load_config',return_value=config), \
                 patch.object(s.f,'select_leg') as select, \
                 patch.object(s.subprocess,'run') as audit, \
                 patch.object(s.f,'plan',return_value=campaign) as plan, \
                 patch.object(s.r,'prepare') as prepare, patch.object(s.r,'submit') as submit:
                s.start(root,'26100101-26141231',root/'config.json')
                select.assert_called_once_with(root.resolve(),'26100101-26141231')
                self.assertTrue(audit.call_args.kwargs['check'])
                args,used=plan.call_args.args
                self.assertEqual(args.config.parent,root/'results/launch/26100101-26141231')
                self.assertEqual(used['source_audit_report'],str(args.config.parent/'source_audit.json'))
                prepare.assert_called_once_with(campaign)
                submit.assert_called_once_with(campaign,full_after_pass=True)
