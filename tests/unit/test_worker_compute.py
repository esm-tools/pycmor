"""Tests for Fix #3 of FORENSIC_lrcs_seaice_failure.md: move dask
compute off the driver process onto LocalCluster workers.

``_safe_to_netcdf`` now:
- If input is eager (numpy) → direct ``to_netcdf`` (unchanged)
- If input is dask-backed AND a distributed.Client is active →
  ``client.compute(ds, sync=True)`` (workers crunch the lazy graph),
  then write the eager result.
- If no Client OR worker compute fails → legacy synchronous-scheduler
  fallback (driver-side compute, same as before).
- If ``PYCMOR_WORKER_COMPUTE=off`` → skip the worker path entirely.

Same logic in ``_save_mfdataset_worker_or_sync`` (multi-file save).
"""
import os
from unittest.mock import patch, MagicMock

import numpy as np
import pytest
import xarray as xr

from pycmor.std_lib import files


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Strip the env var we control."""
    monkeypatch.delenv("PYCMOR_WORKER_COMPUTE", raising=False)


def _eager_dataset():
    return xr.Dataset(
        {"foo": (("time",), np.arange(10, dtype=np.float64))},
        coords={"time": np.arange(10)},
    )


def _dask_dataset():
    """Tiny dask-backed Dataset using dask.array. Doesn't need a Client."""
    import dask.array as da_
    return xr.Dataset(
        {"foo": (("time",), da_.arange(10, chunks=5, dtype=np.float64))},
        coords={"time": np.arange(10)},
    )


# ---------------- eager path: direct to_netcdf ----------------


def test_eager_input_takes_direct_path(tmp_path):
    """Numpy-backed input bypasses both the worker-compute and the
    synchronous-scheduler fallback. Just calls ds.to_netcdf directly."""
    ds = _eager_dataset()
    out = tmp_path / "eager.nc"
    files._safe_to_netcdf(ds, str(out), mode="w", format="NETCDF4")
    assert out.exists()
    with xr.open_dataset(out) as r:
        xr.testing.assert_equal(r, ds)


# ---------------- dask path: no Client → fallback to synchronous ----------------


def test_dask_no_client_falls_back_to_synchronous(tmp_path):
    """No active distributed Client → use the legacy synchronous-scheduler
    path. (This is the historical pycmor behaviour.)"""
    ds = _dask_dataset()
    out = tmp_path / "no_client.nc"

    # Force "no Client" by making get_client raise ValueError.
    def _no_client():
        raise ValueError("no Client active")

    with patch("dask.distributed.get_client", side_effect=_no_client):
        files._safe_to_netcdf(ds, str(out), mode="w", format="NETCDF4")
    assert out.exists()


def test_dask_worker_compute_path_skipped_via_env(tmp_path):
    """PYCMOR_WORKER_COMPUTE=off → skip the Client.compute attempt
    entirely, even if a Client is available."""
    ds = _dask_dataset()
    out = tmp_path / "env_off.nc"
    os.environ["PYCMOR_WORKER_COMPUTE"] = "off"

    fake_client = MagicMock()
    fake_client.compute.side_effect = AssertionError("must not be called")
    with patch("dask.distributed.get_client", return_value=fake_client):
        files._safe_to_netcdf(ds, str(out), mode="w", format="NETCDF4")
    assert out.exists()
    # The Client was reachable but the env switch said skip.
    fake_client.compute.assert_not_called()


# ---------------- dask path: Client available → workers compute ----------------


def test_dask_uses_client_compute_when_available(tmp_path):
    """An active Client → ``client.compute(ds, sync=True)`` is called
    and the result is written eagerly."""
    ds = _dask_dataset()
    out = tmp_path / "client.nc"
    eager_result = _eager_dataset()

    fake_client = MagicMock()
    fake_client.compute.return_value = eager_result

    with patch("dask.distributed.get_client", return_value=fake_client):
        files._safe_to_netcdf(ds, str(out), mode="w", format="NETCDF4")
    assert out.exists()
    fake_client.compute.assert_called_once()
    # ``sync=True`` was passed (the second positional in compute(...) call).
    call_kwargs = fake_client.compute.call_args.kwargs
    assert call_kwargs.get("sync") is True


def test_dask_falls_back_to_sync_when_client_compute_raises(tmp_path):
    """Client.compute raises (e.g. transient distributed error) → fall
    back to the legacy synchronous path so the rule still completes."""
    ds = _dask_dataset()
    out = tmp_path / "fallback.nc"

    fake_client = MagicMock()
    fake_client.compute.side_effect = RuntimeError("simulated worker death")

    with patch("dask.distributed.get_client", return_value=fake_client):
        files._safe_to_netcdf(ds, str(out), mode="w", format="NETCDF4")
    assert out.exists()
    # Client tried, then synchronous fallback succeeded.
    fake_client.compute.assert_called_once()


# ---------------- save_mfdataset variant ----------------


def test_save_mfdataset_helper_eager_input(tmp_path):
    """is_dask=False → direct xr.save_mfdataset, no Client involvement."""
    datasets = [_eager_dataset(), _eager_dataset()]
    paths = [str(tmp_path / f"eager_{i}.nc") for i in range(2)]
    with patch("dask.distributed.get_client", side_effect=AssertionError("must not call")):
        files._save_mfdataset_worker_or_sync(
            datasets, paths, enc=None, extra_kwargs={},
            is_dask=False, scheduler="synchronous",
        )
    assert all(os.path.exists(p) for p in paths)


def test_save_mfdataset_helper_uses_client(tmp_path):
    """is_dask=True with a Client → workers compute, then write eager."""
    datasets = [_dask_dataset(), _dask_dataset()]
    paths = [str(tmp_path / f"client_{i}.nc") for i in range(2)]
    eager_list = [_eager_dataset(), _eager_dataset()]

    fake_client = MagicMock()
    fake_client.compute.return_value = eager_list

    with patch("dask.distributed.get_client", return_value=fake_client):
        files._save_mfdataset_worker_or_sync(
            datasets, paths, enc=None, extra_kwargs={},
            is_dask=True, scheduler="synchronous",
        )
    assert all(os.path.exists(p) for p in paths)
    fake_client.compute.assert_called_once()


def test_save_mfdataset_helper_falls_back(tmp_path):
    """Client.compute fails → fall back to synchronous scheduler."""
    datasets = [_dask_dataset(), _dask_dataset()]
    paths = [str(tmp_path / f"fallback_{i}.nc") for i in range(2)]

    fake_client = MagicMock()
    fake_client.compute.side_effect = RuntimeError("simulated")

    with patch("dask.distributed.get_client", return_value=fake_client):
        files._save_mfdataset_worker_or_sync(
            datasets, paths, enc=None, extra_kwargs={},
            is_dask=True, scheduler="synchronous",
        )
    assert all(os.path.exists(p) for p in paths)


# ---------------- regression: existing tests still pass ----------------


def test_existing_atomic_path_still_works(tmp_path, monkeypatch):
    """The _atomic_to_netcdf 3-stage path wraps _safe_to_netcdf. Make
    sure the worker-compute change didn't break it."""
    ds = _eager_dataset()
    out = tmp_path / "atomic.nc"
    monkeypatch.setenv("PYCMOR_TMPFS_DIR", str(tmp_path))
    monkeypatch.setenv("PYCMOR_TMPFS_STAGING", "on")
    files._atomic_to_netcdf(ds, str(out), mode="w", format="NETCDF4")
    assert out.exists()
