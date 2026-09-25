"""Pure-load bench: open dataset, compute the data variable end-to-end,
report wall + RSS. Bypasses the pycmor pipeline so we measure ONLY
the I/O + decompress cost, isolated from save/encode/transform.

Usage:
    python bench_load_only.py <file.nc> <var>
"""
import argparse
import gc
import os
import resource
import sys
import time

import xarray as xr


def now():
    return time.time()


def rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024


def run(path, var):
    print(f"=== bench_load_only ===")
    print(f"file: {path} ({os.path.getsize(path)/1e9:.2f} GB)")
    print(f"var:  {var}")

    t0 = now()
    ds = xr.open_dataset(path, use_cftime=True)
    t_open = now() - t0
    print(f"open_dataset:        {t_open:6.2f} s   rss={rss_mb()} MB")

    da = ds[var]
    print(f"  shape: {da.shape}, dtype: {da.dtype}, raw GB: {da.nbytes/1e9:.1f}")
    print(f"  dask chunks: {[(d, c[0] if c else None) for d, c in zip(da.dims, da.chunks or [None]*da.ndim)]}")

    t0 = now()
    arr = da.values  # forces full read+decompress
    t_load = now() - t0
    print(f"da.values:           {t_load:6.2f} s   rss={rss_mb()} MB   shape={arr.shape}")

    # release
    del arr
    del da
    ds.close()
    del ds
    gc.collect()

    print(f"TOTAL: open {t_open:.2f}s + load {t_load:.2f}s = {t_open+t_load:.2f}s")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("file")
    p.add_argument("var")
    args = p.parse_args()
    run(args.file, args.var)


if __name__ == "__main__":
    sys.exit(main())
