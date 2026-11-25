"""Time and time bounds leave save_mfdataset the way cmip7repack would leave them,
so cmip7repack only collates the metadata and does not re-encode the data."""

import netCDF4
import numpy as np
import pandas as pd
import xarray as xr

from pycmor.std_lib import files


def _dataset(n):
    time = pd.date_range("2000-01-01", periods=n, freq="6h")
    bnds = np.stack([time, time + pd.Timedelta("6h")], axis=1)
    ds = xr.Dataset(
        {"va": (("time", "cell"), np.zeros((n, 4), dtype="float32"))},
        coords={"time": time, "time_bnds": (("time", "bnds"), bnds)},
    )
    ds["time"].attrs["bounds"] = "time_bnds"
    return ds


def _layout(path, name):
    with netCDF4.Dataset(path) as nc:
        var = nc[name]
        return var.chunking(), {k: v for k, v in var.filters().items() if v}


def test_long_file_gets_one_compressed_time_chunk_short_file_unchanged(tmp_path):
    datasets = [_dataset(600), _dataset(1)]
    paths = [str(tmp_path / "year.nc"), str(tmp_path / "step.nc")]
    units = "days since 2000-01-01"
    enc = {
        "time": {"units": units, "calendar": "proleptic_gregorian", "dtype": "float64"},
        "time_bnds": {"units": units, "calendar": "proleptic_gregorian", "dtype": "float64"},
    }
    files._save_mfdataset_worker_or_sync(
        datasets, paths, enc, {"unlimited_dims": ["time"]}, is_dask=False, scheduler="synchronous"
    )

    repacked = {"zlib": True, "shuffle": True, "complevel": 4, "fletcher32": True}
    assert _layout(paths[0], "time") == ([600], repacked)
    assert _layout(paths[0], "time_bnds") == ([600, 2], repacked)
    # One chunk already: cmip7repack leaves these alone, and so does the writer.
    assert _layout(paths[1], "time") == ([512], {})
    assert _layout(paths[1], "time_bnds") == ([1, 2], {})
    # The shared encoding still applies to each file.
    for path, ds in zip(paths, datasets):
        with netCDF4.Dataset(path) as nc:
            assert nc["time"].units == units
        with xr.open_dataset(path) as out:
            xr.testing.assert_equal(out["time"], ds["time"])
    assert set(enc) == {"time", "time_bnds"}


def test_time_bounds_keep_no_coordinates_attribute(tmp_path):
    # A scalar coordinate (height, a single lev) would otherwise be listed in a
    # coordinates attribute on time_bnds, which cf §7.1 and AICC004 reject.
    datasets = [_dataset(600).assign_coords(height=2.0), _dataset(1).assign_coords(height=2.0)]
    paths = [str(tmp_path / "year.nc"), str(tmp_path / "step.nc")]
    units = "days since 2000-01-01"
    enc = {
        "time": {"units": units, "calendar": "proleptic_gregorian", "dtype": "float64"},
        "time_bnds": {"units": units, "calendar": "proleptic_gregorian", "dtype": "float64"},
    }
    files._save_mfdataset_worker_or_sync(
        datasets, paths, enc, {"unlimited_dims": ["time"]}, is_dask=False, scheduler="synchronous"
    )
    for path in paths:
        with netCDF4.Dataset(path) as nc:
            assert "coordinates" not in nc["time_bnds"].ncattrs()
            assert nc["va"].coordinates == "height"
