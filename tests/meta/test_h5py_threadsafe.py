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


def test_h5netcdf_with_dask():
    """Test h5netcdf works with Dask parallel operations."""
    import xarray as xr
    from dask.distributed import Client, LocalCluster

    # Create a small Dask cluster
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, processes=True, silence_logs=False)
    client = Client(cluster)

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.nc"

            # Create test data
            ds = xr.Dataset(
                {"temperature": (["x", "y", "time"], np.random.rand(10, 10, 5))},
                coords={
                    "x": np.arange(10),
                    "y": np.arange(10),
                    "time": np.arange(5),
                },
            )

            # Save with h5netcdf
            ds.to_netcdf(test_file, engine="h5netcdf")

            # Open and perform parallel operations
            ds_read = xr.open_dataset(test_file, engine="h5netcdf")
            result = ds_read.temperature.mean().compute()

            assert result.values > 0, "Computed mean should be positive"

            ds_read.close()

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
def test_actual_fesom_file_with_h5netcdf():
    """Test opening the actual problematic FESOM file with h5netcdf."""
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

    # Try with h5netcdf
    ds = xr.open_dataset(test_file, engine="h5netcdf")
    assert ds is not None, "Should successfully open dataset"
    ds.close()
