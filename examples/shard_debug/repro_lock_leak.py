"""Reproduce the netCDF lock leak behind the wedged shards, with and without pycmor's fix.

With a dask Client active, xarray guards an output file with a CombinedLock of
NETCDFC_LOCK, HDF5_LOCK and a ``distributed.Lock`` for the file, in set order.
When the garbage collector frees an unclosed output file on the Client's
event-loop thread, ``distributed.Lock.acquire`` there returns an unawaited
coroutine, the release that follows raises "Released too often", and with the
distributed lock first in the combination NETCDFC_LOCK and HDF5_LOCK stay
held. After that every netCDF call in the process waits forever.

    python repro_lock_leak.py --without-fix   # LEAK, the plain write hangs
    python repro_lock_leak.py                 # no leak, the plain write works
    python repro_lock_leak.py --guard-only    # LEAK, the lock guard releases it, the write works

Run it on a compute node; a LocalCluster on a login node takes minutes to start.
"""

import argparse
import gc
import sys
import tempfile
import threading
import time

import numpy as np
import xarray as xr
from dask.distributed import Client, LocalCluster
from xarray.backends import locks
from xarray.backends.netCDF4_ import NetCDF4DataStore


def netcdf_locks_held():
    return locks.NETCDFC_LOCK.locked() or locks.HDF5_LOCK.locked()


def distributed_lock_first(store):
    first = store._manager._lock.locks[0]
    return not isinstance(first, locks.SerializableLock)


def free_on_loop_thread(client, holder):
    done = threading.Event()

    def drop():
        holder.clear()
        gc.collect()
        done.set()

    client.loop.add_callback(drop)
    return done.wait(30)


def plain_write_works(path):
    t = threading.Thread(target=lambda: xr.Dataset({"a": ("x", np.arange(3))}).to_netcdf(path), daemon=True)
    t.start()
    t.join(30)
    return not t.is_alive()


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--without-fix", action="store_true", help="do not import pycmor")
    ap.add_argument("--guard-only", action="store_true", help="only the lock guard, CombinedLock left as it is")
    ap.add_argument("--files", type=int, default=20)
    args = ap.parse_args()
    if args.guard_only:
        from pycmor.core import xarray_locks

        xarray_locks.LEAK_AFTER_SEC = 5
        xarray_locks.install_lock_guard(every=1)
    elif not args.without_fix:
        import pycmor.std_lib  # noqa: F401  (hardens CombinedLock, installs the lock guard)

    cluster = LocalCluster(n_workers=1, threads_per_worker=1, dashboard_address=None)
    client = Client(cluster)
    tmp = tempfile.mkdtemp()
    first = 0
    leaked = False
    for i in range(args.files):
        store = NetCDF4DataStore.open(f"{tmp}/w{i}.nc", mode="w")
        store.ds.createDimension("x", 3)  # opened, so it sits unclosed in xarray's file cache
        first += distributed_lock_first(store)
        holder = [store]
        del store
        free_on_loop_thread(client, holder)
        time.sleep(0.1)
        if netcdf_locks_held():
            leaked = True
            print(f"file {i}: LEAK, NETCDFC_LOCK={locks.NETCDFC_LOCK.locked()} HDF5_LOCK={locks.HDF5_LOCK.locked()}")
            break
    print(f"{i + 1} files freed on the loop thread, {first} with the distributed lock first")
    ok = plain_write_works(f"{tmp}/after.nc")
    print("plain write afterwards:", "ok" if ok else "HANGS")
    client.close()
    cluster.close()
    sys.exit(0 if ok and not leaked else 1)


if __name__ == "__main__":
    main()
