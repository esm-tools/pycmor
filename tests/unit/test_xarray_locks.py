"""xarray's netCDF locks must not stay held for nobody (see pycmor.core.xarray_locks)."""

import threading

import numpy as np
import pytest
import xarray as xr
from xarray.backends import locks
from xarray.backends.locks import CombinedLock, SerializableLock, acquire

import pycmor.std_lib  # noqa: F401  (applies the patches)
from pycmor.core import xarray_locks


@pytest.fixture
def combined():
    lock = CombinedLock([SerializableLock(), SerializableLock()])
    yield lock
    for part in lock.locks:
        if part.locked():
            part.release()


class _RaisingLock:
    """Stands in for a distributed.Lock whose release raises."""

    def __init__(self, on_enter=False):
        self.on_enter = on_enter

    def acquire(self, blocking=True):
        return True

    def release(self):
        raise RuntimeError("Released too often")

    def __enter__(self):
        if self.on_enter:
            raise RuntimeError("scheduler unreachable")

    def __exit__(self, *args):
        self.release()

    def locked(self):
        return False


def test_failed_acquire_releases_the_first_lock(combined):
    first, second = combined.locks
    second.acquire()
    assert not acquire(combined, blocking=False)
    second.release()
    assert not first.locked()


def test_successful_acquire_holds_all(combined):
    assert acquire(combined, blocking=False)
    assert all(part.locked() for part in combined.locks)
    combined.release()
    assert not any(part.locked() for part in combined.locks)


def test_gc_between_the_two_releases_cannot_wedge_the_next_open(combined):
    """The hang: another thread is between releasing the first and the second
    lock when a file manager's __del__ tries the lock without waiting."""
    first, second = combined.locks
    second.acquire()
    acquire(combined, blocking=False)  # __del__ in another thread
    second.release()
    done = threading.Event()

    def next_open():
        with combined:
            done.set()

    threading.Thread(target=next_open, daemon=True).start()
    assert done.wait(5), "the next open waits forever on a leaked lock"


def test_patch_is_skipped_on_fixed_xarray(monkeypatch):
    monkeypatch.setattr(xarray_locks, "_xarray_version", lambda: (2026, 9))
    assert xarray_locks.patch_combined_lock() is False


def test_a_raising_lock_does_not_keep_the_others_on_exit(combined):
    """repro_lock_leak.py: the distributed lock comes first and its release raises."""
    combined.locks = (_RaisingLock(),) + combined.locks
    with pytest.raises(RuntimeError, match="Released too often"):
        with combined:
            pass
    assert not any(part.locked() for part in combined.locks)


def test_a_raising_lock_does_not_keep_the_others_on_release(combined):
    combined.locks = (_RaisingLock(),) + combined.locks
    assert combined.acquire()
    with pytest.raises(RuntimeError):
        combined.release()
    assert not any(part.locked() for part in combined.locks)


def test_enter_takes_all_locks_or_none(combined):
    combined.locks = combined.locks + (_RaisingLock(on_enter=True),)
    with pytest.raises(RuntimeError, match="scheduler unreachable"):
        with combined:
            pass
    assert not any(part.locked() for part in combined.locks)


def test_netcdf_locks_are_tracked():
    assert isinstance(locks.NETCDFC_LOCK.lock, xarray_locks.TrackedLock)
    assert isinstance(locks.HDF5_LOCK.lock, xarray_locks.TrackedLock)
    assert SerializableLock(locks.HDF5_LOCK.token).lock is locks.HDF5_LOCK.lock
    assert xarray_locks.install_lock_guard() is xarray_locks.install_lock_guard()


def test_netcdf_still_round_trips(tmp_path):
    path = tmp_path / "a.nc"
    xr.Dataset({"a": ("x", np.arange(3))}).to_netcdf(path)
    with xr.open_dataset(path) as ds:
        assert ds["a"].values.tolist() == [0, 1, 2]
    assert not locks.NETCDFC_LOCK.locked() and not locks.HDF5_LOCK.locked()


def test_a_lock_whose_frame_returned_is_leaked_and_released(monkeypatch):
    monkeypatch.setattr(xarray_locks, "LEAK_AFTER_SEC", 0)
    lock = xarray_locks.TrackedLock("TEST_LOCK")

    def take_and_forget():
        lock.acquire()

    take_and_forget()
    assert lock.leaked() is not None
    assert xarray_locks._check([lock]) == 1
    assert not lock.locked()


def test_a_lock_held_by_a_live_frame_is_left_alone(monkeypatch):
    monkeypatch.setattr(xarray_locks, "LEAK_AFTER_SEC", 0)
    lock = xarray_locks.TrackedLock("TEST_LOCK")
    held, go = threading.Event(), threading.Event()

    def hold():
        with lock:
            held.set()
            go.wait(5)

    t = threading.Thread(target=hold)
    t.start()
    assert held.wait(5)
    assert lock.leaked() is None
    assert xarray_locks._check([lock]) == 0
    assert lock.locked()
    go.set()
    t.join(5)
    assert not lock.locked()


def test_a_lock_whose_thread_died_is_leaked(monkeypatch):
    monkeypatch.setattr(xarray_locks, "LEAK_AFTER_SEC", 0)
    lock = xarray_locks.TrackedLock("TEST_LOCK")
    t = threading.Thread(target=lock.acquire)
    t.start()
    t.join(5)
    assert lock.leaked() is not None
    assert xarray_locks._check([lock]) == 1
