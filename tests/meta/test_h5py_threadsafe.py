"""
Meta-tests to verify h5py thread-safety configuration.

These tests verify that the test environment is properly configured
with thread-safe HDF5 and h5py to avoid "file signature not found"
errors when using h5netcdf with Dask/Prefect parallel workflows.
"""

import tempfile
import threading
from pathlib import Path

import h5py
import numpy as np
import pytest


def test_h5py_has_threadsafe_config():
    """Verify h5py is built with thread-safety enabled."""
    config = h5py.get_config()
    assert config.threadsafe, "h5py must be built with thread-safety enabled (HDF5_ENABLE_THREADSAFE=1)"


def test_h5py_parallel_file_access():
    """Test actual parallel file access with multiple threads."""
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = Path(tmpdir) / "test.h5"

        # Write test data
        with h5py.File(test_file, "w") as f:
            f.create_dataset("data", data=np.arange(100))

        errors = []

        def read_file(thread_id):
            """Try to read the file from multiple threads."""
            try:
                with h5py.File(test_file, "r") as f:
                    data = f["data"][:]
                    assert len(data) == 100, f"Expected 100 values, got {len(data)}"
            except Exception as e:
                errors.append(f"Thread {thread_id}: {e}")

        # Create and start threads
        threads = []
        num_threads = 5

        for i in range(num_threads):
            thread = threading.Thread(target=read_file, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check for errors
        assert not errors, f"Parallel file access failed: {errors}"


@pytest.mark.parametrize("engine", ["h5netcdf", "netcdf4"])
def test_xarray_engine_with_dask(engine):
    """Test xarray engines (h5netcdf and netcdf4) work with Dask parallel operations."""
    import xarray as xr
    from dask.distributed import Client, LocalCluster

    # Create a small Dask cluster
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, processes=True, silence_logs=False)
    client = Client(cluster)

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / f"test_{engine}.nc"

            # Create test data
            ds = xr.Dataset(
                {"temperature": (["x", "y", "time"], np.random.rand(10, 10, 5))},
                coords={
                    "x": np.arange(10),
                    "y": np.arange(10),
                    "time": np.arange(5),
                },
            )

            # Save with specified engine
            ds.to_netcdf(test_file, engine=engine)

            # Open and perform parallel operations
            ds_read = xr.open_dataset(test_file, engine=engine)
            result = ds_read.temperature.mean().compute()

            assert result.values > 0, f"Computed mean should be positive for {engine}"

            ds_read.close()

    finally:
        client.close()
        cluster.close()


@pytest.mark.parametrize("engine", ["h5netcdf", "netcdf4"])
@pytest.mark.parametrize("parallel", [True, False])
def test_xarray_open_mfdataset_engines(engine, parallel):
    """Test xarray.open_mfdataset with different engines and parallel settings."""
    import xarray as xr

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create multiple test files
        files = []
        for i in range(3):
            test_file = Path(tmpdir) / f"test_{i}.nc"
            ds = xr.Dataset(
                {"data": (["x", "time"], np.random.rand(10, 5))},
                coords={"x": np.arange(10), "time": np.arange(i * 5, (i + 1) * 5)},
            )
            ds.to_netcdf(test_file, engine=engine)
            files.append(str(test_file))

        # Open with open_mfdataset
        ds_multi = xr.open_mfdataset(files, engine=engine, parallel=parallel, combine="nested", concat_dim="time")

        # Verify we got all the data
        assert ds_multi.time.size == 15, f"Should have 15 time steps for {engine} (parallel={parallel})"

        ds_multi.close()


@pytest.mark.parametrize("engine", ["h5netcdf", "netcdf4"])
def test_xarray_open_mfdataset_with_dask_client(engine):
    """Test xarray.open_mfdataset with a Dask client (simulating actual usage)."""
    import xarray as xr
    from dask.distributed import Client, LocalCluster

    # Create a Dask cluster like in actual tests
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, processes=True, silence_logs=False)
    client = Client(cluster)

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create multiple test files
            files = []
            for i in range(3):
                test_file = Path(tmpdir) / f"test_{i}.nc"
                ds = xr.Dataset(
                    {"temperature": (["x", "y", "time"], np.random.rand(10, 10, 2))},
                    coords={
                        "x": np.arange(10),
                        "y": np.arange(10),
                        "time": np.arange(i * 2, (i + 1) * 2),
                    },
                )
                ds.to_netcdf(test_file, engine=engine)
                files.append(str(test_file))

            # Open with open_mfdataset using parallel=True (uses Dask client if available)
            ds_multi = xr.open_mfdataset(
                files, engine=engine, parallel=True, combine="nested", concat_dim="time", use_cftime=True
            )

            # Perform a computation that uses Dask
            mean_temp = ds_multi.temperature.mean().compute()

            # Verify computation succeeded
            assert mean_temp.values > 0, f"Computed mean should be positive for {engine} with Dask client"

            ds_multi.close()

    finally:
        client.close()
        cluster.close()


@pytest.mark.skipif(
    not (
        Path.home()
        / ".cache"
        / "pycmor"
        / "test_data"
        / "awicm_1p0_recom"
        / "awicm_1p0_recom"
        / "awi-esm-1-1-lr_kh800"
        / "piControl"
        / "outdata"
        / "fesom"
        / "thetao_fesom_2686-01-05.nc"
    ).exists(),
    reason="FESOM test file not available",
)
def test_actual_fesom_file_with_h5py():
    """Test opening the actual problematic FESOM file with h5py."""
    test_file = (
        Path.home()
        / ".cache"
        / "pycmor"
        / "test_data"
        / "awicm_1p0_recom"
        / "awicm_1p0_recom"
        / "awi-esm-1-1-lr_kh800"
        / "piControl"
        / "outdata"
        / "fesom"
        / "thetao_fesom_2686-01-05.nc"
    )

    # Try with h5py directly
    with h5py.File(test_file, "r") as f:
        assert len(f.keys()) > 0, "File should contain datasets"


@pytest.mark.skipif(
    not (
        Path.home()
        / ".cache"
        / "pycmor"
        / "test_data"
        / "awicm_1p0_recom"
        / "awicm_1p0_recom"
        / "awi-esm-1-1-lr_kh800"
        / "piControl"
        / "outdata"
        / "fesom"
        / "thetao_fesom_2686-01-05.nc"
    ).exists(),
    reason="FESOM test file not available",
)
@pytest.mark.parametrize("engine", ["h5netcdf", "netcdf4"])
def test_actual_fesom_file_with_xarray(engine):
    """Test opening the actual problematic FESOM file with different xarray engines."""
    import xarray as xr

    test_file = (
        Path.home()
        / ".cache"
        / "pycmor"
        / "test_data"
        / "awicm_1p0_recom"
        / "awicm_1p0_recom"
        / "awi-esm-1-1-lr_kh800"
        / "piControl"
        / "outdata"
        / "fesom"
        / "thetao_fesom_2686-01-05.nc"
    )

    # Try with specified engine
    ds = xr.open_dataset(test_file, engine=engine)
    assert ds is not None, f"Should successfully open dataset with {engine}"
    ds.close()


@pytest.mark.skipif(
    not (
        Path.home()
        / ".cache"
        / "pycmor"
        / "test_data"
        / "awicm_1p0_recom"
        / "awicm_1p0_recom"
        / "awi-esm-1-1-lr_kh800"
        / "piControl"
        / "outdata"
        / "fesom"
    ).exists(),
    reason="FESOM test files not available",
)
@pytest.mark.parametrize("engine", ["h5netcdf", "netcdf4"])
@pytest.mark.parametrize("parallel", [True, False])
def test_actual_fesom_files_with_open_mfdataset(engine, parallel):
    """Test opening actual FESOM files with open_mfdataset using different engines and parallel settings."""
    import glob

    import xarray as xr

    fesom_dir = (
        Path.home()
        / ".cache"
        / "pycmor"
        / "test_data"
        / "awicm_1p0_recom"
        / "awicm_1p0_recom"
        / "awi-esm-1-1-lr_kh800"
        / "piControl"
        / "outdata"
        / "fesom"
    )

    # Get all FESOM NetCDF files
    files = sorted(glob.glob(str(fesom_dir / "*.nc")))

    if len(files) < 2:
        pytest.skip("Not enough FESOM files for mfdataset test")

    # Try to open with open_mfdataset
    ds = xr.open_mfdataset(files, engine=engine, parallel=parallel, combine="by_coords")

    assert ds is not None, f"Should successfully open FESOM files with {engine} (parallel={parallel})"

    ds.close()
