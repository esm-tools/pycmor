"""xarray's netCDF locks: one lock order, a backported fix, and a guard against leaked locks.

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

xarray 2026.09.0 fixes it (pydata/xarray#11622).

All or nothing
--------------
``CombinedLock.__enter__`` takes its locks one after the other and keeps them
when a later one raises; ``release`` and ``__exit__`` stop at the first lock
that raises. Writing a file while a dask Client is active puts a
``distributed.Lock`` for the file into the combination, next to NETCDFC_LOCK
and HDF5_LOCK. That lock talks to the scheduler and can raise. One way,
reproduced with ``examples/shard_debug/repro_lock_leak.py``: the garbage
collector frees an unclosed output file on the Client's event-loop thread,
where ``distributed.Lock.acquire`` returns an unawaited coroutine instead of
taking the lock, so ``release`` raises "Released too often". When the
distributed lock comes first, NETCDFC_LOCK and HDF5_LOCK are never released.
:func:`harden_combined_lock` makes entering take all locks or none, and
releasing release every lock before it re-raises.

One order
---------
This is what wedged the shards. ``CombinedLock`` keeps its locks in set order.
Reads go through ``NETCDF4_PYTHON_LOCK``, ``set([NETCDFC_LOCK, HDF5_LOCK])``;
writes through a combination built from that lock's own tuple plus a lock for
the file. When the two locks fall into the same slot of the set's table, who
gets the slot depends on which is inserted first, and the second build inserts
them in the opposite order. A reader then takes HDF5_LOCK and waits for
NETCDFC_LOCK while a writer holds NETCDFC_LOCK and waits for HDF5_LOCK, for
ever. The hash is the object's address, so this is settled at import: 3 of 400
process starts had the orders reversed, about the rate shards wedged at, and
the first read that meets a write ends such a shard, which is why they stopped
on their first saves and why a retry, a new process, always got through.
:func:`_in_order` gives every combination one order.

The watchdog's stack dumps show every save thread waiting in
``CombinedLock.__enter__`` and nobody inside netCDF, which was first read as a
leaked lock (the cli126 and cli129 hangs, then 1pctCO2 y1850 cap7_atm_shard_00).
Replaying cli130 extra_land_shard_00 caught it on the 21st run: the long
hold report named a live reader holding one lock and a live writer the other.
``examples/shard_debug/repro_lock_order.py`` reproduces it on demand.

Leak guard
----------
``install_lock_guard`` swaps the ``threading.Lock`` inside both locks for a
:class:`TrackedLock`, which remembers the thread and the frame that took it,
and a guard thread watches them, in case anything does leave one held.

A lock is leaked when it is still held but that frame is no longer on the
owner's stack (or the owner thread is gone): the ``with`` block it guarded has
ended, so nobody is inside netCDF on its behalf. The guard logs the stack it
was taken from and releases it. The record keeps the frame itself and not its
id: a frame that returned frees its address, and the owner's next call to the
same function gets it back. A hold with no owner on record is released once two
checks have seen it. Any hold longer than ``REPORT_AFTER_SEC`` is reported with
the owner thread's current stack and left alone: a deadlock looks like that.
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
#: A netCDF call holds the lock for seconds. A hold this long is reported once.
REPORT_AFTER_SEC = 600.0

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


def _in_order(locks):
    """Return ``locks`` in the one order every CombinedLock takes them.

    Per-file locks come first, so nobody waits for a file or the scheduler
    while holding the locks the whole process needs.
    """
    rank = {_locks.NETCDFC_LOCK.token: 1, _locks.HDF5_LOCK.token: 2}
    return sorted(locks, key=lambda lock: rank.get(getattr(lock, "token", None), 0))


def _acquire(self, blocking=True):
    acquired = []
    try:
        for lock in _in_order(self.locks):
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
        for lock in _in_order(self.locks):
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
        # (thread ident, thread name, since, holder frame, holder code, stack). The frame itself,
        # not its id: a frame that returned frees its address, and the owner's next call to the
        # same function gets it back, so an id would still be found on the owner's stack.
        self._owner = None
        self._takes = 0  # counts every take, so the guard can tell one hold from the next
        self._taken_at = None
        self._unknown = None  # the take the guard last found held with no owner on record
        self._reported = None  # the take the guard last reported as held long

    def acquire(self, blocking=True, timeout=-1):
        if not self._lock.acquire(blocking, timeout):
            return False
        self._takes += 1
        self._taken_at = time.monotonic()
        self._owner = None
        try:
            self._owner = self._record(sys._getframe(1))
        except Exception:
            pass  # the guard releases a hold with no owner on record
        return True

    def _record(self, frame):
        while frame is not None and _is_plumbing(frame):
            frame = frame.f_back
        stack = traceback.StackSummary.extract(traceback.walk_stack(frame), limit=30, lookup_lines=False)
        thread = threading.current_thread()
        return (
            thread.ident,
            thread.name,
            self._taken_at,
            frame,
            frame.f_code if frame is not None else None,
            stack,
        )

    def release(self):
        self._owner = None
        self._lock.release()

    def held(self):
        """Return (take, seconds held) while the lock is held, else None."""
        if not self._lock.locked():
            return None
        take, since = self._takes, self._taken_at
        return take, time.monotonic() - since

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
        ident, _, since, holder, code, _ = owner
        if code is None or (now or time.monotonic()) - since < LEAK_AFTER_SEC:
            return None
        frame = sys._current_frames().get(ident)
        while frame is not None:
            if frame is holder:
                return None
            frame = frame.f_back
        return owner

    def force_release(self, take):
        """Release a leaked lock, unless it changed hands since ``take``."""
        if self._takes == take and self._lock.locked():
            self.release()
            return True
        return False


def _describe(owner):
    if owner is None:
        return "a holder that was never recorded"
    ident, name, _, _, code, _ = owner
    alive = ident in sys._current_frames()
    where = f"{code.co_filename}:{code.co_name}" if code is not None else "?"
    return f"thread {name!r} ({'alive' if alive else 'gone'}), frame {where}"


def _where_taken(owner):
    if owner is None:
        return ""
    return "\nTaken at (most recent call last):\n" + "".join(owner[5].format()[::-1])


def _report(lock, owner, seconds):
    from .logging import logger

    logger.error(
        f"xarray {lock.name} leaked: held for {seconds:.0f}s by {_describe(owner)}, "
        f"which no longer holds it. Releasing it.{_where_taken(owner)}"
    )


def _report_long_hold(lock, owner, seconds):
    from .logging import logger

    now = ""
    frame = sys._current_frames().get(owner[0]) if owner is not None else None
    if frame is not None:
        now = "\nThe owner thread is now at (most recent call last):\n" + "".join(traceback.format_stack(frame))
    logger.warning(
        f"xarray {lock.name} held for {seconds:.0f}s by {_describe(owner)}, which still looks busy, "
        f"so it is left alone.{_where_taken(owner)}{now}"
    )


def _say(report, *args):
    try:
        report(*args)
    except Exception as exc:  # pragma: no cover - reporting must not kill the guard
        print(f"xarray lock guard: {report.__name__} failed: {exc!r}", file=sys.stderr, flush=True)


def _check(tracked):
    """Release and report every leaked lock, report long holds; return how many were released."""
    released = 0
    for lock in tracked:
        held = lock.held()
        if held is None:
            continue
        take, seconds = held
        owner = lock._owner
        if owner is None:
            # Two checks in a row rule out catching a holder between clearing its record and releasing.
            unknown, lock._unknown = lock._unknown == take, take
            leaked = unknown and seconds >= LEAK_AFTER_SEC
        else:
            leaked = lock.leaked() is not None
        if leaked and lock.force_release(take):
            released += 1
            _say(_report, lock, owner, seconds)
        elif not leaked and seconds >= REPORT_AFTER_SEC and lock._reported != take:
            lock._reported = take
            _say(_report_long_hold, lock, owner, seconds)
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
