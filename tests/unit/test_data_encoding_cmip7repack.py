"""The data variable leaves the writer in the layout cmip7repack would give it,
so cmip7repack only collates the metadata and does not re-encode the data."""

import netCDF4
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pycmor.std_lib import files

REPACKED = {"zlib": True, "shuffle": True, "complevel": 4, "fletcher32": True}


@pytest.mark.parametrize(
    "shape, chunks, itemsize, expected",
    [
        # The example in cmip7repack's man page.
        ((1800, 144, 192), (1, 144, 192), 4, (37, 144, 192)),
        # LR monthly ocean, 30S-90S: 1048576 // 93000 = 11 of 12 months.
        ((12, 93000), (1, 93000), 4, (11, 93000)),
        # LR monthly atmosphere: 26 months capped at the 12 in the file.
        ((12, 40320), (1, 40320), 4, (12, 40320)),
        # Hourly, trailing chunk kept.
        ((8760, 9344), (24, 9344), 4, (112, 9344)),
        # Double precision halves the leading length.
        ((8760, 9344), (24, 9344), 8, (56, 9344)),
        # 26 levels of 40320 cells are 4193280 B: no larger chunk is possible.
        ((1459, 91, 40320), (1, 26, 40320), 4, None),
        # 4 MiB or more already.
        ((2919, 180, 360), (124, 180, 360), 4, None),
        # One chunk, also when the chunk is longer than an unlimited axis.
        ((12, 40320), (12, 40320), 4, None),
        ((5, 40320), (12, 40320), 4, None),
        # Contiguous, scalar.
        ((12, 40320), None, 4, None),
        ((), (), 4, None),
    ],
)
def test_chunk_rule_matches_cmip7repack(shape, chunks, itemsize, expected):
    assert files._cmip7repack_data_chunks(shape, chunks, itemsize) == expected


@pytest.mark.parametrize(
    "shape, chunks",
    [((1800, 144, 192), (1, 144, 192)), ((12, 93000), (1, 93000)), ((8760, 9344), (24, 9344)), ((3, 7), (1, 7))],
)
def test_repacked_layout_is_left_alone(shape, chunks):
    target = files._cmip7repack_data_chunks(shape, chunks, 4)
    assert target is not None
    assert files._cmip7repack_data_chunks(shape, target, 4) is None


def _dataset(n, cells=1000):
    time = pd.date_range("2000-01-01", periods=n, freq="MS")
    bnds = np.stack([time, time + pd.Timedelta("1D")], axis=1)
    rng = np.random.default_rng(n)
    data = rng.random((n, cells)).astype("float32")
    data[0, :3] = np.nan
    ds = xr.Dataset(
        {"tos": (("time", "cell"), data)},
        coords={"time": time, "time_bnds": (("time", "bnds"), bnds), "height": 2.0},
        attrs={"variable_id": "tos"},
    )
    ds["time"].attrs["bounds"] = "time_bnds"
    return ds


def _layout(path, name):
    with netCDF4.Dataset(path) as nc:
        var = nc[name]
        return var.chunking(), {k: v for k, v in var.filters().items() if v}


def _time_enc():
    units = "days since 2000-01-01"
    return {
        "time": {"units": units, "calendar": "proleptic_gregorian", "dtype": "float64"},
        "time_bnds": {"units": units, "calendar": "proleptic_gregorian", "dtype": "float64"},
    }


def test_small_chunks_are_written_as_cmip7repack_leaves_them(tmp_path, monkeypatch):
    # 1000 float32 cells: 4 KiB per step, so 1048 steps make 4 MiB.
    datasets = [_dataset(1200), _dataset(12), _dataset(1)]
    paths = [str(tmp_path / f"{i}.nc") for i in range(3)]
    enc = _time_enc()
    enc["tos"] = {
        "chunksizes": (1, 1000),
        "zlib": True,
        "complevel": 1,
        "shuffle": True,
        "quantize_mode": "BitGroom",
        "significant_digits": 5,
        "_FillValue": 1.0e20,
    }
    files._save_mfdataset_worker_or_sync(
        datasets, paths, enc, {"unlimited_dims": ["time"]}, is_dask=False, scheduler="synchronous"
    )
    assert _layout(paths[0], "tos") == ([1048, 1000], REPACKED)
    assert _layout(paths[1], "tos") == ([12, 1000], REPACKED)
    # One step is one chunk: cmip7repack leaves it, and so does the writer.
    assert _layout(paths[2], "tos") == ([1, 1000], {"zlib": True, "shuffle": True, "complevel": 1})
    for path, ds in zip(paths, datasets):
        with netCDF4.Dataset(path) as nc:
            var = nc["tos"]
            assert var.getncattr("_FillValue") == np.float32(1.0e20)
            assert var.getncattr("_QuantizeBitGroomNumberOfSignificantDigits") == 5
            assert var.coordinates == "height"
        with xr.open_dataset(path) as out:
            assert np.isnan(out["tos"].values[0, :3]).all()
            np.testing.assert_allclose(out["tos"].values, ds["tos"].values, rtol=1e-4)
    # The other entries still apply to every file.
    assert set(enc) == {"time", "time_bnds", "tos"}


def test_values_match_the_old_layout_bit_for_bit(tmp_path):
    # Quantization happens per write, not per chunk: the stored values do not
    # depend on the chunk shape.
    ds = _dataset(24)
    old_enc = dict(_time_enc(), tos={"chunksizes": (24, 1000), "zlib": True, "complevel": 1, "shuffle": True})
    new_enc = dict(_time_enc(), tos={"chunksizes": (1, 1000), "zlib": True, "complevel": 1, "shuffle": True})
    for enc in (old_enc, new_enc):
        enc["tos"].update(quantize_mode="BitGroom", significant_digits=5)
    old, new = str(tmp_path / "old.nc"), str(tmp_path / "new.nc")
    files._save_mfdataset_worker_or_sync([ds], [old], old_enc, {"unlimited_dims": ["time"]}, False, "synchronous")
    files._save_mfdataset_worker_or_sync([ds], [new], new_enc, {"unlimited_dims": ["time"]}, False, "synchronous")
    assert _layout(new, "tos") == ([24, 1000], REPACKED)
    with netCDF4.Dataset(old) as a, netCDF4.Dataset(new) as b:
        x, y = a["tos"][:].filled(np.nan), b["tos"][:].filled(np.nan)
        assert np.array_equal(x.view("u4"), y.view("u4"))


def test_large_chunks_and_unnamed_data_untouched(tmp_path):
    ds = _dataset(12)
    enc = dict(_time_enc(), tos={"chunksizes": (12, 1000), "zlib": True, "complevel": 1, "shuffle": True})
    path = str(tmp_path / "one.nc")
    files._save_mfdataset_worker_or_sync([ds], [path], enc, {"unlimited_dims": ["time"]}, False, "synchronous")
    assert _layout(path, "tos") == ([12, 1000], {"zlib": True, "shuffle": True, "complevel": 1})

    # Without variable_id cmip7repack has no data variable, nor has the writer.
    ds = _dataset(12)
    del ds.attrs["variable_id"]
    enc = dict(_time_enc(), tos={"chunksizes": (1, 1000), "zlib": True, "complevel": 1, "shuffle": True})
    path = str(tmp_path / "anon.nc")
    files._save_mfdataset_worker_or_sync([ds], [path], enc, {"unlimited_dims": ["time"]}, False, "synchronous")
    assert _layout(path, "tos") == ([1, 1000], {"zlib": True, "shuffle": True, "complevel": 1})


def test_single_file_write(tmp_path):
    ds = xr.Dataset({"areacello": (("lat", "lon"), np.ones((180, 360), "float32"))}, attrs={"variable_id": "areacello"})
    path = str(tmp_path / "fx.nc")
    enc = {"areacello": {"chunksizes": (10, 360), "zlib": True, "complevel": 1, "shuffle": True}}
    files._safe_to_netcdf(ds, path, encoding=enc)
    assert _layout(path, "areacello") == ([180, 360], REPACKED)
    assert enc["areacello"]["chunksizes"] == (10, 360)


def test_unaligned_dask_write_cache():
    var = xr.DataArray(np.zeros((365, 1001), "float32"), dims=("time", "cell"))
    assert files._unaligned_write_cache(var, (4, 1001), 4) is None  # numpy: one write
    # 92 chunks of 4 x 1001 values, the last one partly beyond the data.
    assert files._unaligned_write_cache(var.chunk({"time": 1}), (4, 1001), 4) == (92 * 4 * 1001 * 4, 92)
    assert files._unaligned_write_cache(var.chunk({"time": 8}), (4, 1001), 4) is None
    assert files._unaligned_write_cache(var.chunk({"time": 8, "cell": 500}), (4, 1001), 4) is not None


def _dask_dataset(n, cells):
    ds = _dataset(n, cells)
    ds["tos"] = ds["tos"].chunk({"time": 1})
    return ds


def test_dask_write_keeps_values_and_restores_the_cache(tmp_path):
    # 1001 values per step: merging steps into one write would flip the
    # BitGroom alternation on every other step. The blocks stay as they are.
    ds = _dask_dataset(30, 1001)
    q = dict(quantize_mode="BitGroom", significant_digits=5, zlib=True, complevel=1, shuffle=True)
    before = netCDF4.get_chunk_cache()
    old, new = str(tmp_path / "old.nc"), str(tmp_path / "new.nc")
    files._save_mfdataset_worker_or_sync(
        [ds],
        [old],
        dict(_time_enc(), tos=dict(chunksizes=(30, 1001), **q)),
        {"unlimited_dims": ["time"]},
        True,
        "synchronous",
    )
    files._save_mfdataset_worker_or_sync(
        [ds],
        [new],
        dict(_time_enc(), tos=dict(chunksizes=(1, 1001), **q)),
        {"unlimited_dims": ["time"]},
        True,
        "synchronous",
    )
    assert netCDF4.get_chunk_cache() == before
    assert _layout(new, "tos") == ([30, 1001], REPACKED)
    with netCDF4.Dataset(old) as a, netCDF4.Dataset(new) as b:
        a.set_auto_mask(False)
        b.set_auto_mask(False)
        assert np.array_equal(a["tos"][:].view("u4"), b["tos"][:].view("u4"))


def test_dask_write_over_the_cache_limit_is_left_to_cmip7repack(tmp_path, monkeypatch):
    monkeypatch.setattr(files, "_CMIP7REPACK_WRITE_CACHE_LIMIT", 1000)
    ds = _dask_dataset(30, 1001)
    path = str(tmp_path / "big.nc")
    enc = dict(_time_enc(), tos={"chunksizes": (1, 1001), "zlib": True, "complevel": 1, "shuffle": True})
    files._save_mfdataset_worker_or_sync([ds], [path], enc, {"unlimited_dims": ["time"]}, True, "synchronous")
    assert _layout(path, "tos") == ([1, 1001], {"zlib": True, "shuffle": True, "complevel": 1})
