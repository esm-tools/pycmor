"""A failed non-blocking CombinedLock.acquire must not keep the locks it took
(see pycmor.core.xarray_locks)."""

import threading

import pytest
from xarray.backends.locks import CombinedLock, SerializableLock, acquire

import pycmor.std_lib  # noqa: F401  (applies the patch)
from pycmor.core import xarray_locks


@pytest.fixture
def combined():
    lock = CombinedLock([SerializableLock(), SerializableLock()])
    yield lock
    for part in lock.locks:
        if part.locked():
            part.release()


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
