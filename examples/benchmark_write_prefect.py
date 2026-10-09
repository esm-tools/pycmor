"""
Benchmark: realistic netCDF write speed with and without Prefect overhead.

Measures the actual write throughput for a 3D model-level field using three approaches:
  1. Raw xarray write (no dask, no Prefect) — establishes the I/O ceiling
  2. Dask lazy write with synchronous scheduler (no Prefect) — measures dask overhead
  3. Full pycmor pipeline via Prefect (lazy_write path) — measures total overhead

Usage:
    # On a compute node (needs 256GB memory for approach 1):
    python examples/benchmark_write_prefect.py

    # Or via SLURM:
    sbatch examples/benchmark_write_prefect.sh
"""

import os
import time
import tempfile
import shutil

import numpy as np
import xarray as xr
import dask
import dask.array as da

# Use scratch to avoid quota issues
SCRATCH = os.environ.get("TMPDIR", "/tmp")
OUTPUT_DIR = os.path.join(SCRATCH, "write_benchmark")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Source file — 3D model-level field, ~21 GB compressed, ~41 GB raw
SOURCE_FILE = (
    "/work/bb1469/a270092/runtime/awiesm3-develop/"
    "cmip7_output_006/outdata/oifs/atmos_6h_ml_ta_6h_ml_1900-1900.nc"
)


def human_size(nbytes):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(nbytes) < 1024.0:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024.0
    return f"{nbytes:.1f} PB"


def benchmark_raw_xarray(ds_loaded, output_path):
    """
    Approach 1: Write fully loaded (in-memory) dataset with xarray.
    No dask, no Prefect. Pure xarray -> netCDF4 -> HDF5 -> Lustre.
    This is the I/O ceiling for single-threaded compressed writes.
    """
    encoding = {
        "ta": {
            "chunksizes": (1, 91, 192, 400),
            "zlib": True,
            "complevel": 1,
            "shuffle": True,
        }
    }

    t0 = time.time()
    ds_loaded.to_netcdf(output_path, encoding=encoding)
    elapsed = time.time() - t0

    fsize = os.path.getsize(output_path)
    raw_size = ds_loaded["ta"].nbytes
    return elapsed, raw_size, fsize


def benchmark_dask_synchronous(output_path):
    """
    Approach 2: Open lazily with dask, write with synchronous scheduler.
    Dask chunks = source chunks = netCDF chunks (no rechunk).
    Measures dask task graph + synchronous scheduler overhead.
    """
    ds = xr.open_dataset(SOURCE_FILE, chunks={"time_counter": 1}, decode_times=False)

    encoding = {}
    for var in ds.data_vars:
        v = ds[var]
        if v.chunks is not None:
            encoding[var] = {
                "chunksizes": tuple(max(c) for c in v.chunks),
                "zlib": True,
                "complevel": 1,
                "shuffle": True,
            }

    t0 = time.time()
    with dask.config.set(scheduler="synchronous"):
        ds.to_netcdf(output_path, encoding=encoding)
    elapsed = time.time() - t0

    fsize = os.path.getsize(output_path)
    raw_size = ds["ta"].nbytes
    ds.close()
    return elapsed, raw_size, fsize


def benchmark_dask_with_rechunk(output_path):
    """
    Approach 3: Open lazily, rechunk to different target, write synchronously.
    This simulates the OLD behavior where netCDF chunks != dask chunks.
    """
    ds = xr.open_dataset(SOURCE_FILE, chunks={"time_counter": 1}, decode_times=False)

    # Target chunks that DON'T match source — forces expensive rechunk
    target_chunks = {
        "time_counter": 273,
        "model_levels": 17,
        "lat": 35,
        "lon": 74,
    }

    with dask.config.set(
        {"dataframe.shuffle.method": "tasks", "array.rechunk.method": "tasks"}
    ):
        ds_rechunked = ds.chunk(target_chunks)

    encoding = {}
    for var in ds_rechunked.data_vars:
        v = ds_rechunked[var]
        if v.chunks is not None:
            encoding[var] = {
                "chunksizes": tuple(max(c) for c in v.chunks),
                "zlib": True,
                "complevel": 1,
                "shuffle": True,
            }

    t0 = time.time()
    with dask.config.set(scheduler="synchronous"):
        ds_rechunked.to_netcdf(output_path, encoding=encoding)
    elapsed = time.time() - t0

    fsize = os.path.getsize(output_path)
    raw_size = ds["ta"].nbytes
    ds.close()
    return elapsed, raw_size, fsize


def benchmark_prefect_pipeline(output_path):
    """
    Approach 4: Full Prefect pipeline with a single save step.
    Measures Prefect task wrapping overhead on the write path.
    """
    from prefect import flow, task

    ds = xr.open_dataset(SOURCE_FILE, chunks={"time_counter": 1}, decode_times=False)

    encoding = {}
    for var in ds.data_vars:
        v = ds[var]
        if v.chunks is not None:
            encoding[var] = {
                "chunksizes": tuple(max(c) for c in v.chunks),
                "zlib": True,
                "complevel": 1,
                "shuffle": True,
            }

    @task
    def write_task(ds, path, enc):
        with dask.config.set(scheduler="synchronous"):
            ds.to_netcdf(path, encoding=enc)

    @flow
    def write_flow():
        write_task(ds, output_path, encoding)

    t0 = time.time()
    write_flow()
    elapsed = time.time() - t0

    fsize = os.path.getsize(output_path)
    raw_size = ds["ta"].nbytes
    ds.close()
    return elapsed, raw_size, fsize


def run_benchmark(name, func, *args):
    output_path = os.path.join(OUTPUT_DIR, f"bench_{name}.nc")
    if os.path.exists(output_path):
        os.remove(output_path)

    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")

    elapsed, raw_size, fsize = func(*args) if args else func(output_path)

    throughput_raw = raw_size / elapsed
    throughput_compressed = fsize / elapsed
    ratio = raw_size / fsize if fsize > 0 else 0

    print(f"  Time:        {elapsed:8.1f}s")
    print(f"  Raw size:    {human_size(raw_size)}")
    print(f"  File size:   {human_size(fsize)}")
    print(f"  Ratio:       {ratio:.2f}x")
    print(f"  Raw thput:   {human_size(throughput_raw)}/s")
    print(f"  Disk thput:  {human_size(throughput_compressed)}/s")

    # Cleanup
    if os.path.exists(output_path):
        os.remove(output_path)

    return elapsed, raw_size, fsize


if __name__ == "__main__":
    print("Write Performance Benchmark")
    print(f"Source: {SOURCE_FILE}")
    print(f"Output dir: {OUTPUT_DIR}")

    # Check source exists
    if not os.path.exists(SOURCE_FILE):
        print(f"ERROR: Source file not found: {SOURCE_FILE}")
        exit(1)

    results = {}

    # Approach 1: Raw xarray (load into memory first)
    print("\nLoading dataset into memory for raw benchmark...")
    t_load = time.time()
    ds_loaded = xr.open_dataset(SOURCE_FILE)
    ds_loaded.load()
    print(f"Loaded in {time.time()-t_load:.1f}s")

    out1 = os.path.join(OUTPUT_DIR, "bench_raw_xarray.nc")
    results["1_raw_xarray"] = run_benchmark(
        "1. Raw xarray (in-memory, no dask, no Prefect)",
        benchmark_raw_xarray,
        ds_loaded,
        out1,
    )
    del ds_loaded

    # Approach 2: Dask streaming (aligned chunks, no Prefect)
    results["2_dask_streaming"] = run_benchmark(
        "2. Dask streaming (aligned chunks, synchronous, no Prefect)",
        benchmark_dask_synchronous,
    )

    # Approach 3: Dask with rechunk (OLD approach)
    results["3_dask_rechunk"] = run_benchmark(
        "3. Dask with rechunk (misaligned chunks, synchronous, no Prefect)",
        benchmark_dask_with_rechunk,
    )

    # Approach 4: Prefect-wrapped write
    results["4_prefect"] = run_benchmark(
        "4. Dask streaming + Prefect task wrapper",
        benchmark_prefect_pipeline,
    )

    # Summary
    print(f"\n{'='*60}")
    print("  SUMMARY")
    print(f"{'='*60}")
    base_time = results["1_raw_xarray"][0]
    for name, (elapsed, raw, fsize) in results.items():
        overhead = ((elapsed / base_time) - 1) * 100 if base_time > 0 else 0
        print(f"  {name:45s}  {elapsed:7.1f}s  ({overhead:+.0f}% vs raw)")

    # Cleanup
    shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
