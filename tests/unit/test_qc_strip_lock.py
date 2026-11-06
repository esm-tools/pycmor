"""The QC attribute strip opens files with netCDF4 directly, so it has to take
the lock xarray holds around netCDF-C (not thread-safe) itself."""

import threading

import netCDF4
import numpy as np
from xarray.backends.locks import NETCDFC_LOCK

from pycmor.std_lib.qc import _strip_leading_underscore_attrs


def _file(path):
    with netCDF4.Dataset(path, "w") as ds:
        ds.createDimension("x", 3)
        v = ds.createVariable("tas", "f4", ("x",), fill_value=1e20)
        v[:] = np.arange(3)
        v.setncattr("_QuantizeBitGroomNumberOfSignificantDigits", 5)
    return path


def test_strip_removes_leading_underscore_attrs_but_keeps_fillvalue(tmp_path):
    fp = _file(tmp_path / "a.nc")
    _strip_leading_underscore_attrs([fp])
    with netCDF4.Dataset(fp) as ds:
        assert ds["tas"].ncattrs() == ["_FillValue"]


def test_strip_waits_while_xarray_holds_the_netcdf_lock(tmp_path):
    """cap7_land_shard_03 (test-1pctCO2 y1850) aborted inside netCDF-C with
    four rules in this step at once, next to xarray saves in other threads."""
    fp = _file(tmp_path / "a.nc")
    done = threading.Event()
    worker = threading.Thread(target=lambda: (_strip_leading_underscore_attrs([fp]), done.set()))
    with NETCDFC_LOCK:
        worker.start()
        assert not done.wait(1.0), "strip ran while xarray held NETCDFC_LOCK"
    assert done.wait(10.0)
    worker.join()
