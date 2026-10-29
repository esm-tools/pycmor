"""xarray's netCDF locks: a backported fix, and a guard against leaked locks.

Every netCDF open, read and close in a process goes through two plain
``threading.Lock`` objects, ``NETCDFC_LOCK`` and ``HDF5_LOCK``. A shard runs
its rules as threads of one process, so if any code path takes one of them and
never gives it back, every later netCDF call in the process waits forever. The
shard then writes nothing until the shard watchdog cancels it.

Backport
--------
Up to xarray 2026.07, ``CombinedLock.acquire(blocking=False)`` is
``all(acquire(lock) for lock in self.locks)``: when the first lock is free and
the second is not, it returns False and keeps the first one, for good.
``CachingFileManager.__del__`` makes exactly that call when the garbage
collector frees a netCDF file that was not closed, in whatever thread it runs
in. ``CombinedLock.__exit__`` releases NETCDFC_LOCK before HDF5_LOCK, so a
collection in between takes NETCDFC_LOCK, fails on HDF5_LOCK, and every later
netCDF open or close in the process waits forever.

That is the shard hang of cli126 y1852 lrcs_land_shard_00, where the
watchdog's stack dump showed all six save threads waiting in
``CombinedLock.__enter__`` with no thread holding the lock. xarray 2026.09.0
fixes it (pydata/xarray#11622).

All or nothing
--------------
``CombinedLock.__enter__`` takes its locks one after the other and keeps them
when a later one raises; ``release`` and ``__exit__`` stop at the first lock
that raises. Writing a file while a dask Client is active puts a
``distributed.Lock`` for the file into the combination, next to NETCDFC_LOCK
and HDF5_LOCK, in set order, so per file at random. That lock talks to the
scheduler and can raise. One way, reproduced with
``examples/shard_debug/repro_lock_leak.py``: the garbage collector frees an
unclosed output file on the Client's event-loop thread, where
``distributed.Lock.acquire`` returns an unawaited coroutine instead of taking
the lock, so ``release`` raises "Released too often". When the distributed
lock comes first, NETCDFC_LOCK and HDF5_LOCK are never released.
:func:`harden_combined_lock` makes entering take all locks or none, and
releasing release every lock before it re-raises.

Leak guard
----------
The backport did not end the hangs: cli129 y1852 cap7_land_shard_02 and
1pctCO2 y1850 cap7_atm_shard_00 wedged with the same signature on code that
has it, so there is at least one more path that leaves a lock held.

``install_lock_guard`` swaps the ``threading.Lock`` inside both locks for a
:class:`TrackedLock`, which remembers the thread and the frame that took it. A
lock is leaked when it is still held but that frame is no longer on the
owner's stack (or the owner thread is gone): the ``with`` block it guarded has
ended, so nobody is inside netCDF on its behalf. The guard thread then logs
the stack it was taken from, which names the leak, and releases it, so the
shard carries on instead of waiting for the watchdog.
"""

import sys
import threading
import time
import traceback

from xarray.backends import locks as _locks

FIXED_IN = (2026, 9)

#: How long a lock must be held by a frame that has gone before it counts as
#: leaked. The test itself is exact; the wait only rules out a race with an
#: owner that is between leaving the frame and releasing.
LEAK_AFTER_SEC = 60.0
CHECK_EVERY_SEC = 15.0

# Frames that take a lock on someone else's behalf. The holder is the first
# frame below them: the one whose ``with`` block the lock guards.
_PLUMBING = ("xarray/backends/locks.py", "contextlib.py", "pycmor/core/xarray_locks.py")
_PLUMBING_FUNCS = {("xarray/backends/file_manager.py", "_optional_lock")}


def _release_all(locks, release=lambda lock: lock.release()):
    """Release every lock, then raise the first error any of them gave."""
    error = None
    for lock in locks:
        try:
            release(lock)
        except Exception as exc:
            error = error or exc
    if error is not None:
        raise error


def _acquire(self, blocking=True):
    acquired = []
    try:
        for lock in self.locks:
            if not _locks.acquire(lock, blocking=blocking):
                break
            acquired.append(lock)
    except BaseException:
        _release_all(reversed(acquired))
        raise
    if len(acquired) < len(self.locks):
        _release_all(reversed(acquired))
        return False
    return True


def _enter(self):
    entered = []
    try:
        for lock in self.locks:
            lock.__enter__()
            entered.append(lock)
    except BaseException:
        _release_all(reversed(entered))
        raise


def _exit(self, *args):
    _release_all(self.locks, lambda lock: lock.__exit__(*args))


def _release(self):
    _release_all(self.locks)


def _xarray_version():
    import xarray

    return tuple(int(part) for part in xarray.__version__.split(".")[:2])


def patch_combined_lock():
    """Make a failed ``CombinedLock.acquire`` release what it took, on xarray < 2026.09."""
    if _xarray_version() >= FIXED_IN:
        return False
    _locks.CombinedLock.acquire = _acquire
    return True


def harden_combined_lock():
    """Make ``CombinedLock`` take all its locks or none, and always release all of them."""
    _locks.CombinedLock.acquire = _acquire
    _locks.CombinedLock.__enter__ = _enter
    _locks.CombinedLock.__exit__ = _exit
    _locks.CombinedLock.release = _release


def _is_plumbing(frame):
    path = frame.f_code.co_filename.replace("\\", "/")
    if path.endswith(_PLUMBING):
        return True
    return any(path.endswith(p) and frame.f_code.co_name == f for p, f in _PLUMBING_FUNCS)


class TrackedLock:
    """A ``threading.Lock`` that knows who holds it."""

    def __init__(self, name):
        self.name = name
        self._lock = threading.Lock()
        self._owner = None  # (thread ident, thread name, since, holder frame id, holder code, stack)

    def acquire(self, blocking=True, timeout=-1):
        if not self._lock.acquire(blocking, timeout):
            return False
        frame = sys._getframe(1)
        while frame is not None and _is_plumbing(frame):
            frame = frame.f_back
        stack = traceback.StackSummary.extract(traceback.walk_stack(frame), limit=30, lookup_lines=False)
        thread = threading.current_thread()
        self._owner = (
            thread.ident,
            thread.name,
            time.monotonic(),
            id(frame),
            frame.f_code if frame is not None else None,
            stack,
        )
        return True

    def release(self):
        self._owner = None
        self._lock.release()

    def locked(self):
        return self._lock.locked()

    __enter__ = acquire

    def __exit__(self, *args):
        self.release()

    def leaked(self, now=None):
        """Return the owner record if the lock is held for nobody, else None."""
        owner = self._owner
        if owner is None or not self._lock.locked():
            return None
        ident, _, since, frame_id, code, _ = owner
        if code is None or (now or time.monotonic()) - since < LEAK_AFTER_SEC:
            return None
        frame = sys._current_frames().get(ident)
        while frame is not None:
            if id(frame) == frame_id and frame.f_code is code:
                return None
            frame = frame.f_back
        return owner

    def force_release(self, owner):
        """Release a leaked lock, unless it changed hands since ``owner`` was read."""
        if self._owner is owner:
            self.release()
            return True
        return False


def _report(lock, owner):
    from .logging import logger

    ident, name, since, _, code, stack = owner
    alive = ident in sys._current_frames()
    where = f"{code.co_filename}:{code.co_name}" if code is not None else "?"
    logger.error(
        f"xarray {lock.name} leaked: held for {time.monotonic() - since:.0f}s by thread {name!r} "
        f"({'alive' if alive else 'gone'}), whose frame {where} no longer holds it. Releasing it. "
        "Taken at (most recent call last):\n" + "".join(stack.format()[::-1])
    )


def _check(tracked):
    """Release and report every leaked lock; return how many there were."""
    released = 0
    for lock in tracked:
        owner = lock.leaked()
        if owner is not None and lock.force_release(owner):
            released += 1
            try:
                _report(lock, owner)
            except Exception:  # pragma: no cover - reporting must not kill the guard
                pass
    return released


def _guard(tracked, every):
    while True:
        time.sleep(every)
        _check(tracked)


_installed = None


def install_lock_guard(every=CHECK_EVERY_SEC):
    """Track NETCDFC_LOCK and HDF5_LOCK, and release them when they leak. Idempotent."""
    global _installed
    if _installed is not None:
        return _installed
    tracked = []
    for name in ("NETCDFC_LOCK", "HDF5_LOCK"):
        serializable = getattr(_locks, name)
        if serializable.locked():  # pragma: no cover - only at import, so nothing should hold it
            continue
        lock = TrackedLock(name)
        serializable.lock = lock
        # Unpickled SerializableLocks look their lock up by token.
        _locks.SerializableLock._locks[serializable.token] = lock
        tracked.append(lock)
    threading.Thread(target=_guard, args=(tracked, every), name="xarray-lock-guard", daemon=True).start()
    _installed = tracked
    return tracked
