"""Data moves preserve files, retain QC references and never overwrite results."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import netCDF4

import output_layout as layout


class Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.campaign = Path(self.temp.name)
        self.attempt = self.campaign / 'attempts/task/attempt_001'
        self.attempt.mkdir(parents=True)

    def data(self, year):
        relative = Path('MIP-DRS7/CMIP7/CMIP/AWI/MODEL/piControl/r1i1p1f1/'
                        'glb/mon/tas/tavg-h2m-hxy-u/g271/v20261109') / (
            f'tas_tavg-h2m-hxy-u_mon_glb_g271_MODEL_piControl_r1i1p1f1_{year}01-{year}12.nc')
        path = self.attempt / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        with netCDF4.Dataset(path, 'w') as ds:
            ds.createDimension('time', 2)
            ds.createVariable('tas', 'f4', ('time',))[:] = [271.5, 282.5]
        return path, self.campaign / relative

    def validation(self, paths):
        return dict(files=list(map(str, paths)), output_state={
            str(p): [p.stat().st_size, p.stat().st_mtime_ns] for p in paths},
            publication_ready=False)

    def test_two_years_share_a_drs_directory_with_unchanged_netcdf_and_qc(self):
        pairs = [self.data(2620), self.data(2621)]
        signatures = {str(p): (p.stat().st_ino, p.stat().st_mtime_ns,
                      hashlib.sha256(p.read_bytes()).hexdigest()) for p, _ in pairs}
        qc = self.attempt / 'qc_tas.json'
        qc.write_text(json.dumps({str(p): {'cf': {'high_count': 2}} for p, _ in pairs}))
        result = layout.promote(self.campaign, self.attempt,
                                self.validation([p for p, _ in pairs]))
        self.assertEqual(result['files'], [str(p) for _, p in pairs])
        self.assertFalse(result['publication_ready'])
        self.assertEqual(set(json.loads(qc.read_text())), set(result['files']))
        self.assertFalse((self.attempt / 'MIP-DRS7').exists())
        for source, target in pairs:
            self.assertFalse(source.exists())
            self.assertEqual(signatures[str(source)], (target.stat().st_ino,
                target.stat().st_mtime_ns, hashlib.sha256(target.read_bytes()).hexdigest()))
            with netCDF4.Dataset(target) as ds:
                self.assertEqual(ds['tas'][:].tolist(), [271.5, 282.5])

    def test_collision_is_detected_before_any_file_is_moved(self):
        pairs = [self.data(2620), self.data(2621)]
        target = pairs[1][1]
        target.parent.mkdir(parents=True)
        target.write_bytes(b'existing result')
        with self.assertRaises(FileExistsError):
            layout.promote(self.campaign, self.attempt,
                           self.validation([p for p, _ in pairs]))
        self.assertTrue(all(p.exists() for p, _ in pairs))
        self.assertFalse(pairs[0][1].exists())
        self.assertEqual(target.read_bytes(), b'existing result')

    def test_partial_move_error_rolls_back(self):
        pairs = [self.data(2620), self.data(2621)]
        rename = Path.rename
        def fail_second(source, target):
            if source == pairs[1][0]:
                raise OSError('simulated storage error')
            return rename(source, target)
        with patch.object(Path, 'rename', fail_second), self.assertRaises(OSError):
            layout.promote(self.campaign, self.attempt,
                           self.validation([p for p, _ in pairs]))
        self.assertTrue(all(p.exists() for p, _ in pairs))
        self.assertTrue(all(not p.exists() for _, p in pairs))

    def test_changed_output_is_rejected(self):
        source, target = self.data(2620)
        validation = self.validation([source])
        source.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, 'changed after validation'):
            layout.promote(self.campaign, self.attempt, validation)
        self.assertFalse(target.exists())

    def test_bad_structure_or_filename_is_rejected(self):
        source, _ = self.data(2620)
        for path in (source.with_name('wrong.nc'), self.attempt / 'other.nc'):
            with self.assertRaises(ValueError):
                layout.drs_relative(path, self.attempt)

    def test_current_records_relocate_values_and_dictionary_keys(self):
        source, target = self.data(2620)
        value = dict(output=str(self.attempt), output_state={str(source): [1, 2]},
                     status='PASS', publication_ready=False)
        result = layout.relocate_references(value, {str(source): str(target)},
                    [(str(self.attempt), '/new/attempt')])
        self.assertEqual(result['output'], '/new/attempt')
        self.assertEqual(result['output_state'], {str(target): [1, 2]})
        self.assertEqual(result['status'], 'PASS')
        self.assertFalse(result['publication_ready'])
        old = '/old/attempt/' + str(source.relative_to(self.attempt))
        self.assertEqual(layout.relocate_references(old,
            {str(source): str(target)}, [('/old/attempt', str(self.attempt))]), str(target))
