"""Backport of the xarray fix for a lock that a failed non-blocking acquire leaks.

Up to xarray 2026.07, ``CombinedLock.acquire(blocking=False)`` is
``all(acquire(lock) for lock in self.locks)``: when the first lock is free and
the second is not, it returns False and keeps the first one, for good.
``CachingFileManager.__del__`` makes exactly that call when the garbage
collector frees a netCDF file that was not closed, in whatever thread it runs
in. ``CombinedLock.__exit__`` releases NETCDFC_LOCK before HDF5_LOCK, so a
collection in between takes NETCDFC_LOCK, fails on HDF5_LOCK, and every later
netCDF open or close in the process waits forever.

That is the shard hang: cli126 y1852 lrcs_land_shard_00, where the watchdog's
stack dump showed all six save threads waiting in ``CombinedLock.__enter__``
with no thread holding the lock.

xarray 2026.09.0 fixes it (pydata/xarray#11622). Remove this module once the
production env is on that version.
"""

from xarray.backends import locks as _locks

FIXED_IN = (2026, 9)


def _acquire(self, blocking=True):
    acquired = []
    for lock in self.locks:
        if not _locks.acquire(lock, blocking=blocking):
            for held in reversed(acquired):
                held.release()
            return False
        acquired.append(lock)
    return True


def _xarray_version():
    import xarray

    return tuple(int(part) for part in xarray.__version__.split(".")[:2])


def patch_combined_lock():
    """Make a failed ``CombinedLock.acquire`` release what it took, on xarray < 2026.09."""
    if _xarray_version() >= FIXED_IN:
        return False
    _locks.CombinedLock.acquire = _acquire
    return True
