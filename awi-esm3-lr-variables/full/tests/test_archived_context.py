import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from archived_context import load_context


class ArchivedContextTests(unittest.TestCase):
    def test_original_identity_and_geometry_are_required(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); period='20000101-20041231'
            geometry=root/'grid.nc';geometry.write_bytes(b'original geometry')
            plan=root/'plan.json'
            identity={'grid':'native','fesom_sha256':'abc'}
            plan.write_text(json.dumps({'signature':dict(experiment=str(root),period=period,identity=identity)}))
            digest=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
            context=dict(experiment=str(root),period=period,identity=identity,
                original_plan=str(plan),original_plan_sha256=digest(plan),
                files={'grid.nc':dict(path=str(geometry),sha256=digest(geometry))})
            path=root/'context.json';path.write_text(json.dumps(context))
            cfg={'archived_contexts':{period:str(path)}}
            self.assertEqual(load_context(root,period,cfg)['identity'],identity)
            with self.assertRaisesRegex(ValueError,'another experiment'):
                load_context(root/'other',period,cfg)
            bad=copy.deepcopy(context);bad['identity']['grid']='different'
            path.write_text(json.dumps(bad))
            with self.assertRaisesRegex(ValueError,'original frozen identity'):
                load_context(root,period,cfg)
            path.write_text(json.dumps(context));geometry.write_bytes(b'changed geometry')
            with self.assertRaisesRegex(ValueError,'file changed'):
                load_context(root,period,cfg)

    def test_missing_context_does_not_borrow_another_leg(self):
        self.assertIsNone(load_context('/does/not/exist','20000101-20041231',{}))
