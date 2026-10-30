"""Reproduce the netCDF lock-order deadlock behind the wedged shards, with and without pycmor's fix.

xarray reads through ``NETCDF4_PYTHON_LOCK`` and writes through a CombinedLock
built from that lock's own tuple plus a lock for the file. ``CombinedLock``
keeps its locks in set order, so when NETCDFC_LOCK and HDF5_LOCK collide in the
set's table, the second build can swap them. One thread then reads and takes
HDF5_LOCK first, another writes and takes NETCDFC_LOCK first, and both wait
forever. Whether a process is affected is decided by where the two locks landed
in memory at import.

This script swaps the order by hand and pauses briefly after taking each lock,
so it depends neither on that luck nor on a thread switch in a tiny window.

    python repro_lock_order.py --without-fix   # DEADLOCK within seconds
    python repro_lock_order.py                 # all rounds finish
"""

import argparse
import sys
import tempfile
import threading
import time

import numpy as np
import xarray as xr
from xarray.backends import locks, netCDF4_


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--without-fix", action="store_true", help="do not import pycmor")
    ap.add_argument("--rounds", type=int, default=40)
    ap.add_argument("--timeout", type=float, default=120)
    args = ap.parse_args()
    if not args.without_fix:
        import pycmor.std_lib  # noqa: F401  (orders CombinedLock)

    read = netCDF4_.NETCDF4_PYTHON_LOCK
    # The swapped pair a colliding process ends up with: writes take the locks in reverse.
    write = locks.CombinedLock(read.locks)
    write.locks = read.locks[::-1]

    tmp = tempfile.mkdtemp()
    src = f"{tmp}/in.nc"
    xr.Dataset({"a": ("x", np.arange(3))}).to_netcdf(src)

    enter = locks.SerializableLock.__enter__

    def enter_then_pause(self):
        enter(self)
        time.sleep(0.001)

    locks.SerializableLock.__enter__ = enter_then_pause

    finished = threading.Event()

    def reader():
        for _ in range(args.rounds):
            with xr.open_dataset(src) as ds:
                ds["a"].values
        finished.set()

    def writer():
        while not finished.is_set():
            with write:
                pass

    for fn in (reader, writer):
        threading.Thread(target=fn, daemon=True).start()
    ok = finished.wait(args.timeout)
    print("all rounds finished" if ok else f"DEADLOCK: no finish in {args.timeout:.0f}s")
    print(f"NETCDFC_LOCK held={locks.NETCDFC_LOCK.locked()} HDF5_LOCK held={locks.HDF5_LOCK.locked()}", flush=True)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
