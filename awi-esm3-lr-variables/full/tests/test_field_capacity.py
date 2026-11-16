import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import numpy as np
import xarray as xr

from field_capacity import parameters_from_source, reviewed_parameters

SPEC=importlib.util.spec_from_file_location('field_capacity_custom',Path(__file__).resolve().parents[3]/'examples/custom_steps.py')
CUSTOM=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(CUSTOM)
SOURCE='''
ZFAC=100._JPRB/9.8_JPRB
ZPSICAP=-0.10_JPRB*ZFAC
ZMVGALPHA(1:NSOTY)=(/3.83_JPRB,3.14_JPRB,0.83_JPRB,3.67_JPRB,2.65_JPRB,1.300_JPRB,3.14_JPRB/)
ZNFAC(1:NSOTY)=(/1.3774_JPRB,1.1804_JPRB,1.2539_JPRB,1.1012_JPRB,1.1033_JPRB,1.2039_JPRB,1.1804_JPRB/)
ZWSATM(1:NSOTY)=(/0.403_JPRB,0.439_JPRB,0.430_JPRB,0.520_JPRB,0.614_JPRB,0.766_JPRB,0.439_JPRB/)
ZWRES(1:NSOTY)=(/0.025_JPRB,0.010_JPRB,0.010_JPRB,0.010_JPRB,0.010_JPRB,0.010_JPRB,0.010_JPRB/)
IF (NCSS >= 1) YDSOIL%RDAT(1)=0.07_JPRB
IF (NCSS >= 2) YDSOIL%RDAT(2)=0.21_JPRB
IF (NCSS >= 3) YDSOIL%RDAT(3)=0.72_JPRB
IF (NCSS >= 4) YDSOIL%RDAT(4)=1.89_JPRB
'''


class Rule(SimpleNamespace):
    def get(self,key,default=None):return getattr(self,key,default)


class FieldCapacityTests(unittest.TestCase):
    def test_model_parameters_correct_the_large_errors_for_soils_five_to_seven(self):
        table,depth=parameters_from_source(SOURCE)
        np.testing.assert_allclose([table[i] for i in (5,6,7)],
            [.5405183480037093,.6615796893835945,.3459388147977648],rtol=1e-12)
        self.assertAlmostEqual(depth,2.89)
        data=xr.Dataset({'slt':('cell',[5.,6.,7.,0.,np.nan])})
        result=CUSTOM.compute_mrsofc(data,Rule(model_variable='mrsofc',
            field_capacity_theta_by_soil_type=table,soil_column_depth_m=depth))
        np.testing.assert_allclose(result.mrsofc[:4],np.array([table[5],table[6],table[7],0.])*2890)
        self.assertTrue(np.isnan(result.mrsofc[-1]))

    def test_interpolated_categories_are_rejected(self):
        table,depth=parameters_from_source(SOURCE)
        with self.assertRaisesRegex(ValueError,'interpolated'):
            CUSTOM.compute_mrsofc(xr.Dataset({'slt':('cell',[1.5])}),Rule(model_variable='mrsofc',
                field_capacity_theta_by_soil_type=table,soil_column_depth_m=depth))

    def test_actual_run_flags_leg_and_source_are_required(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'sussoil.F90';source.write_text(SOURCE)
            log=root/'NODE.001';log.write_text('LEVGEN = T\nNCSS = 4\n')
            prefix=root/'prefix.txt';prefix.write_bytes(log.read_bytes())
            digest=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
            path=root/'context.json'
            context=dict(kind='htessel_van_genuchten_4layer',period='20000101-20041231',
                source_file=str(source),source_sha256=digest(source),log_file=str(log),
                log_state=[log.stat().st_size,log.stat().st_mtime_ns],log_prefix_file=str(prefix),
                prefix_lines=2,prefix_sha256=digest(prefix))
            path.write_text(json.dumps(context))
            self.assertAlmostEqual(reviewed_parameters(path,context['period'])[1],2.89)
            with self.assertRaisesRegex(ValueError,'this archived leg'):
                reviewed_parameters(path,'20050101-20091231')
            source.write_text(SOURCE+'changed')
            with self.assertRaisesRegex(ValueError,'source changed'):
                reviewed_parameters(path,context['period'])
