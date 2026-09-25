"""Streaming HDF5 chunk-reshape that preserves blosc_zstd-3 compression.

Reads the input in time-slabs, writes to output with new (larger) chunk
shape via netCDF4-python (which has the blosc plugin available in the
pycmor_py312 conda env). Memory profile mirrors pyconcat: heap ~2 GB,
cgroup peak driven by per-slab page cache.

Usage:
    python repack_one.py <input.nc> <output.nc> [--time-chunk N] [--slab N]
"""
import argparse
import gc
import os
import sys
import time
from pathlib import Path

import netCDF4 as nc
import numpy as np


def _fadvise(path):
    try:
        fd = os.open(str(path), os.O_RDONLY)
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)
    except Exception:
        pass


def repack(src_path: str, dst_path: str, time_chunk: int, slab_size: int):
    src_path = str(src_path)
    dst_path = str(dst_path)
    if Path(dst_path).exists():
        Path(dst_path).unlink()
    Path(dst_path).parent.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    with nc.Dataset(src_path, "r") as s:
        # Identify time dim
        time_dim_name = None
        for name in ("time", "time_counter"):
            if name in s.dimensions:
                time_dim_name = name
                break
        if time_dim_name is None:
            for name, d in s.dimensions.items():
                if d.isunlimited():
                    time_dim_name = name
                    break
        if time_dim_name is None:
            raise SystemExit(f"no time-like dim in {src_path}")
        n_time = len(s.dimensions[time_dim_name])
        print(f"[{time.time()-t0:.1f}s] src time dim '{time_dim_name}' size {n_time}")

        with nc.Dataset(dst_path, "w", format="NETCDF4") as d:
            # Copy global attrs
            d.setncatts({k: s.getncattr(k) for k in s.ncattrs()})
            # Copy dimensions; mark time unlimited
            for name, dim in s.dimensions.items():
                d.createDimension(name, None if name == time_dim_name else len(dim))

            # Re-create variables. Reuse source encoding (blosc_zstd-3) but
            # pick new chunksizes for variables that have time as a dim.
            for vname, var in s.variables.items():
                # Source filter info via ncattrs lookup is fragile across
                # netcdf-c versions; query the var directly with filter API.
                filters = var.filters() or {}
                kwargs = {}
                # Enable the same blosc compression family on output. Newer
                # netcdf4-python lets you pass compression="blosc_zstd"
                # directly when the filter is registered.
                if filters.get("blosc"):
                    kwargs["compression"] = "blosc_zstd"
                    kwargs["complevel"] = filters.get("complevel", 3)
                    kwargs["blosc_shuffle"] = filters.get("blosc_shuffle", 1)
                elif filters.get("zstd"):
                    kwargs["compression"] = "zstd"
                    kwargs["complevel"] = filters.get("complevel", 3)
                elif filters.get("zlib"):
                    kwargs["zlib"] = True
                    kwargs["complevel"] = filters.get("complevel", 3)
                    kwargs["shuffle"] = bool(filters.get("shuffle", True))

                # Pick output chunks: time -> time_chunk; non-time dims keep src
                src_chunks = var.chunking() if var.chunking() != "contiguous" else None
                if time_dim_name in var.dimensions and src_chunks is not None:
                    new_chunks = []
                    for dn, sc in zip(var.dimensions, src_chunks):
                        if dn == time_dim_name:
                            new_chunks.append(min(time_chunk, n_time))
                        else:
                            new_chunks.append(len(s.dimensions[dn]))
                    kwargs["chunksizes"] = tuple(new_chunks)
                elif src_chunks is not None:
                    kwargs["chunksizes"] = tuple(src_chunks)

                # Fill value: copy if present
                fv = getattr(var, "_FillValue", None)
                if fv is not None:
                    kwargs["fill_value"] = fv

                v = d.createVariable(vname, var.datatype, var.dimensions, **kwargs)
                v.setncatts({k: var.getncattr(k) for k in var.ncattrs()
                             if k not in ("_FillValue",)})
                print(f"  {vname}: dims={var.dimensions} chunks={kwargs.get('chunksizes')} compression={kwargs.get('compression') or kwargs.get('zlib')}")

            # Stream data slab-by-slab along time
            for vname, var in s.variables.items():
                if time_dim_name not in var.dimensions:
                    # Time-invariant var: copy whole thing
                    d.variables[vname][...] = var[...]
                    continue

            # Copy time-varying vars in slabs
            t_idx = 0
            n_slabs = (n_time + slab_size - 1) // slab_size
            for i in range(n_slabs):
                lo = i * slab_size
                hi = min(lo + slab_size, n_time)
                slab_t0 = time.time()
                for vname, var in s.variables.items():
                    if time_dim_name not in var.dimensions:
                        continue
                    # Build slice
                    t_axis = var.dimensions.index(time_dim_name)
                    src_sl = [slice(None)] * len(var.dimensions)
                    src_sl[t_axis] = slice(lo, hi)
                    chunk_data = var[tuple(src_sl)]
                    dst_sl = [slice(None)] * len(var.dimensions)
                    dst_sl[t_axis] = slice(lo, hi)
                    d.variables[vname][tuple(dst_sl)] = chunk_data
                    del chunk_data
                _fadvise(dst_path)
                gc.collect()
                print(f"[{time.time()-t0:.1f}s] slab {i+1}/{n_slabs} ({lo}:{hi}) wrote in {time.time()-slab_t0:.1f}s")

    _fadvise(src_path)
    _fadvise(dst_path)
    sz = os.path.getsize(dst_path)
    print(f"[{time.time()-t0:.1f}s] DONE -> {dst_path} ({sz/1e9:.2f} GB)")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("src")
    p.add_argument("dst")
    p.add_argument("--time-chunk", type=int, default=120,
                   help="output chunk size along time")
    p.add_argument("--slab", type=int, default=240,
                   help="streaming slab size for the read+write loop")
    args = p.parse_args()
    repack(args.src, args.dst, args.time_chunk, args.slab)


if __name__ == "__main__":
    sys.exit(main())
