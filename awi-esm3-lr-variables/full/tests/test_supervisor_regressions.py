"""Science and memory regressions identified in the November LR review."""
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch, MagicMock
import importlib

from click.testing import CliRunner
import dask.array as da
import xarray as xr

from pycmor.std_lib.generic import trigger_compute
from pycmor.std_lib.files import _safe_to_netcdf
import full_cmorization as f


class SupervisorRegressions(TestCase):
    def test_generated_pipeline_stays_lazy_until_netcdf_write(self):
        recipe=f.new_doc({'general':{},'rules':[{}]},
                         {'metadata':'unused','cv_dir':'unused'},2610,'x')
        data=xr.DataArray(da.arange(360,chunks=30).reshape(12,3,10),
                          dims=('time','depth','cell'),name='detoc')
        with patch.object(xr.DataArray,'compute',side_effect=AssertionError('whole-year gather')):
            lazy=trigger_compute(data,recipe['inherit'])
        self.assertIs(lazy,data)
        with TemporaryDirectory() as tmp,patch.dict('os.environ',{'PYCMOR_WORKER_COMPUTE':'off'}):
            path=Path(tmp)/'stream.nc'
            _safe_to_netcdf(lazy.to_dataset(),path)
            with xr.open_dataset(path) as written:
                xr.testing.assert_equal(written.detoc,data.compute())

    def test_cli_disabled_dask_does_not_create_implicit_cluster(self):
        cli=importlib.import_module('pycmor.cli')
        model=MagicMock();model._cluster=None;model.run_report=None
        with TemporaryDirectory() as tmp:
            recipe=Path(tmp)/'recipe.yaml';recipe.write_text('general: {}\n')
            with patch.object(cli.CMORizer,'from_dict',return_value=model), \
                 patch.object(cli,'Client') as client, \
                 patch('pycmor.core.env_check.run_env_check'):
                result=CliRunner().invoke(cli.cli,['process',str(recipe)])
            self.assertEqual(result.exit_code,0,result.output)
            model.process.assert_called_once()
            client.assert_not_called()

    def test_time_epoch_is_shared_between_years(self):
        docs=[f.new_doc({'general':{},'rules':[{}]},
                        {'metadata':'unused','cv_dir':'unused'},y,'x') for y in (2610,2611)]
        self.assertEqual(docs[0]['inherit']['time_units'],docs[1]['inherit']['time_units'])
