"""Large outputs must stream chunks, not gather a whole annual field."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import dask.array as da
import numpy as np
import xarray as xr

from pycmor.std_lib import files


class StreamingWriteTests(unittest.TestCase):
    def dataset(self):
        return xr.Dataset(
            {"hus": (("time", "lev", "cell"),
                     da.arange(120, chunks=10, dtype="float32").reshape((6, 2, 10)))},
            coords={"time": np.arange(6), "lev": [0.1, 0.9]},
            attrs={"title": "streaming regression"},
        )

    def test_auto_size_decision_is_lazy_and_batch_wide(self):
        # 22 GiB is the real hus-sized failure, but a virtual dask array
        # requires no allocation or real file write for this guard test.
        large = xr.DataArray(da.empty((1460, 91, 40320), chunks=(1, 13, 40320), dtype="f4"))
        with patch.dict(os.environ, {"PYCMOR_WORKER_COMPUTE": "auto",
                                     "PYCMOR_WORKER_COMPUTE_MAX_BYTES": str(1024**3)}):
            with patch.object(xr.DataArray, "compute", side_effect=AssertionError("must remain lazy")):
                self.assertFalse(files._allow_worker_materialization([large]))
        small = self.dataset()
        with patch.dict(os.environ, {"PYCMOR_WORKER_COMPUTE": "auto",
                                     "PYCMOR_WORKER_COMPUTE_MAX_BYTES": str(small.nbytes)}):
            self.assertTrue(files._allow_worker_materialization([small]))
            self.assertFalse(files._allow_worker_materialization([small, small]))

    def test_single_large_write_roundtrips_without_client_compute(self):
        ds = self.dataset()
        client = MagicMock()
        client.compute.side_effect = AssertionError("must stream, not gather")
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "single.nc"
            with patch.dict(os.environ, {"PYCMOR_WORKER_COMPUTE": "auto",
                                         "PYCMOR_WORKER_COMPUTE_MAX_BYTES": "1"}):
                with patch("dask.distributed.get_client", return_value=client) as getter:
                    files._safe_to_netcdf(ds, str(target))
            getter.assert_not_called()
            client.compute.assert_not_called()
            with xr.open_dataset(target) as written:
                xr.testing.assert_identical(written, ds.compute())

    def test_multifile_large_write_roundtrips_without_client_compute(self):
        ds = self.dataset()
        # The production hus write splits off the next January endpoint;
        # ensure the size guard and exact values survive a similar split.
        pieces = [ds.isel(time=slice(0, 5)), ds.isel(time=slice(5, None))]
        client = MagicMock()
        client.compute.side_effect = AssertionError("must stream, not gather")
        with tempfile.TemporaryDirectory() as tmp:
            paths = [str(Path(tmp) / f"part{i}.nc") for i in range(2)]
            with patch.dict(os.environ, {"PYCMOR_WORKER_COMPUTE": "auto",
                                         "PYCMOR_WORKER_COMPUTE_MAX_BYTES": "1"}):
                with patch("dask.distributed.get_client", return_value=client) as getter:
                    files._save_mfdataset_worker_or_sync(
                        pieces, paths, None, {}, True, "synchronous")
            getter.assert_not_called()
            client.compute.assert_not_called()
            for path, expected in zip(paths, pieces):
                with xr.open_dataset(path) as written:
                    xr.testing.assert_identical(written, expected.compute())

    def test_explicit_off_and_on_and_invalid_threshold(self):
        ds = self.dataset()
        with patch.dict(os.environ, {"PYCMOR_WORKER_COMPUTE": "off"}):
            self.assertFalse(files._allow_worker_materialization([ds]))
        with patch.dict(os.environ, {"PYCMOR_WORKER_COMPUTE": "on",
                                     "PYCMOR_WORKER_COMPUTE_MAX_BYTES": "1"}):
            self.assertTrue(files._allow_worker_materialization([ds]))
        for invalid in ("zero", "0", "-1"):
            with self.subTest(invalid=invalid):
                with patch.dict(os.environ, {"PYCMOR_WORKER_COMPUTE": "auto",
                                             "PYCMOR_WORKER_COMPUTE_MAX_BYTES": invalid}):
                    with self.assertRaisesRegex(ValueError, "positive integer"):
                        files._allow_worker_materialization([ds])


if __name__ == "__main__":
    unittest.main()
