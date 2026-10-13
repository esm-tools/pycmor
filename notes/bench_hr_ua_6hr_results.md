# pycmor HR memory-pressure investigation — ua_6hr_pl7h bench

> **Bench scope**: I/O / memory throughput only. Output values are not
> validated for CMIP correctness. Some bench yamls drop unit-conversion
> or scale steps for simplicity (e.g. zg bench skips `handle_unit_conversion`
> because the file holds geopotential `m²/s²` and CMIP needs height `m`).
> The production yamls keep those steps; the slab-loop change is at the
> terminal save step only.


Tracks the cap7_atm heavy-rule investigation (handoff from a prior agent).
Goal: increase end-to-end throughput of the cap7_atm full-yaml run by reducing
per-rule peak so we can run more workers per node. The 32 GB peak target was
arbitrary — throughput is the metric.

Bench rule: `ua_6hr_pl7h`, an OIFS XIOS reduced-Gaussian variable on the FESOM
unstructured cell dim. Yearly file: 13 GB on disk, **17 GB raw float32**
(shape `time_counter=1460 × pressure_levels_7h=7 × cell=421120`). Native NetCDF
chunks (1, 2, 421120) → 5840 chunks/var, blosc_zstd-3 compressed. The original
bench yaml header (42 GB raw, 720×1440 regular grid) was wrong on both counts.

All runs use 1 worker × 1 thread, dask_memory_limit=200 GB, on a 256 GB compute
node. Wall times across runs include OS/Lustre page-cache effects (after the
first read, the 13 GB input is hot — wall is unreliable until invalidated).

## Results

| job | bench | knob diff | peak GB (cgroup) | wall | output | notes |
|---|---|---|---|---|---|---|
| 24674259 | v1 | baseline (threads, no rechunk, lazy_write=true, blosc_zstd-3) | 29.6 | 10:03 | 11.7 GB / 2 files | reference |
| 24675065 | v2 | + rechunk(time:30) + scheduler=sync | 27.6 | 11:48 | 11.7 GB / 2 files | tiny improvement |
| 24675974 | v2-rerun | (repeat of v2) | 29.0 | 8:10 | 11.7 GB / 2 files | reproducibility ~5% |
| 24675800 | v2b | rechunk(time:30) + scheduler=threads | **35.6** | 11:40 | 11 GB / 2 files | rechunk + threads = worse |
| 24675801 | v3 | lazy_write=false | **111.7** | 18:50 (killed) | partial | **disastrous** — eager compute |
| 24675802 | v4 | file_timespan=1MS (12 monthly files) | 28.1 | 12:01 | 11 GB / 13 files | similar peak; granularity didn't help |
| 24675803 | v6 | netcdf_enable_compression=false | 36.3 | 11:42 | 19 GB / 2 files | uncompressed = bigger output, no peak win |
| 24675918 | v7 | file_timespan=1MS + save_per_file (patched) | 28.6 | 7:17 | 11 GB / 13 files | save loop didn't cap peak |
| 24675919 | v8 | load_mfdataset_chunked (chunks at open) | **50.7** | 7:39 | 11 GB / 2 files | chunked load made it WORSE |
| 24675920 | v9 | netcdf_quantize_mode=null (BitGroom off) | 31.1 | 7:29 | 13 GB / 2 files | BitGroom was innocent |
| 24675921 | v10 | save_engine=h5netcdf | 1.5 | 6:22 | failed | encoding incompat (need translation) |
| 24675973 | v1-rerun | (repeat of v1) | n/a (wrong watchdog) | 4:12 | 11.7 GB / 2 files | **MaxRSS = 8.5 GB** (only 8.5 GB of *anon* memory) |

## Conclusions

- **No yaml-level knob moves the cgroup peak below ~28 GB.**
- **Rechunking (smaller or larger graph) doesn't help.** v2/v2b/v8 — all in the 27–50 GB band.
- **Scheduler choice matters for wall time, not peak.** synchronous serialises blosc → slower; threads is faster.
- **`lazy_write=false` is much worse** (111 GB peak) — `data.compute()` materialises the full 17 GB to numpy while still holding the dask source.
- **Compression is innocent.** Disabling blosc didn't drop peak; output just got bigger.
- **BitGroom quantization is innocent.** Turning it off didn't move peak.
- **`save_per_file` (patched) didn't cap peak** even with monthly granularity — because the upstream Dataset (17 GB) stays alive for the duration of `save_dataset`.
- **The `8.5 GB MaxRSS` from `/usr/bin/time -v` on v1-rerun reveals that the cgroup peak (~30 GB) is mostly Linux page cache** (input file + write buffer), not anonymous heap. Dask/Prefect aren't holding 30 GB — the OS is caching the I/O.

## What this implies for the production failure mode

The cgroup peak (which is what hits the 256 GB cgroup hard limit and causes the dask-nanny kills) is dominated by page cache for input + output, NOT anonymous heap that yaml knobs could shrink. With 8 concurrent rules (2 workers × 4 TPW) each touching ~25 GB of file data, the cgroup can easily hit 200+ GB of cache that the kernel may not reclaim aggressively enough.

If this is true, the lever is:
1. **Reduce per-task file footprint:** load only the slab needed, work on it, write it out, then `madvise(DONTNEED)` or close+reopen so the page cache for that slab gets evicted. Peak per-task drops from ~25 GB to ~2–3 GB cache.
2. **Don't rely on yaml knobs:** the fix is structural — split the pipeline into time-slabs.

## Per-slab + fadvise results (v11 onwards)

Custom step `save_dataset_per_slab` (separate files) and
`save_dataset_per_slab_single_file` (append along unlimited time dim).
Both call `posix_fadvise(POSIX_FADV_DONTNEED)` on each just-written file
to encourage page-cache reclaim.

| job | bench | peak GB | wall | output | notes |
|---|---|---|---|---|---|
| 24676636 | v11 (slab=30, separate) | 18.67 | 13:46 | 49 files / 13.7 GB | MaxRSS 525 MB → cgroup peak is page cache |
| 24676951 | v12 (slab=30, append) | 14.23 | 13:02 | 1 file ✓ | MaxRSS 530 MB |
| 24676992 | **v13 (slab=120, separate)** | **16.20** | **10:43** | 13 files (need merge) | MaxRSS 502 MB; **best wall** |
| 24677013 | v14 (slab=120, append) | 15.11 | 13:11 | 1 file ✓ | MaxRSS 510 MB |

**Pareto winner for ua_6hr_pl7h: v13** — peak 45% lower at 7% more wall, but
needs ncrcat post-merge (~30 sec for 13 GB). Best single-file: v14
(49% lower peak at 31% more wall).

Throughput vs other AI's 2×4×64 baseline (8 slots, 245 GB cgroup peak):
- v13: max slots ~12 (186/16.2), wall factor 10:43/10:03 = 1.07. Throughput = 12/8 / 1.07 = **1.4×**.
- v14: max slots ~12 (186/15.1), wall factor 1.31. Throughput = 12/8 / 1.31 = **1.14×**.
- v11: max slots ~10 (186/18.67), wall factor 1.37. Throughput = 10/8 / 1.37 = **0.91×** (worse than baseline).

## Cross-rule: uas_1hr (8760-timestep) and zg_6hr_pl7h

| rule / variant | job | wall | MaxRSS GB (anon) | cgroup peak GB | output |
|---|---|---|---|---|---|
| uas_1hr baseline (no slab) | 24677126 | 10:01 | 6.90 | (no v2 watchdog) | 10.2 GB / 2 files |
| uas_1hr v14-style (slab=120) | 24677127 | RUNNING ~17:30 | n/a | 9.77 (climbing) | 1 file (incomplete) |
| zg_6hr baseline | 24677312 | 5:14 | 11.30 | (no v2 watchdog) | 7.6 GB / 2 files |
| zg_6hr v14-style (slab=120) | 24677313 | RUNNING ~6:30 | n/a | 7.42 (climbing) | 1 file (incomplete) |

Important: **slab_size=120 is too fine for uas_1hr** (8760/120 = 73 slabs).
Each slab pays dask graph + xarray encoding overhead ~5–8 sec → 73 slabs
costs ~7 min of pure overhead. Fix: pick slab_size so n_slabs ~= 12–15
regardless of total time length. For uas → slab_size=720; for ua/zg
6hr → slab_size=120.

Productionization sketch:
```yaml
# heavy rule (yearly file, hourly):
slab_size: 720  # or auto: round(n_timesteps / 12)
```

## Caveat on the baseline measurement

The v1-style runscripts use the OLD cgroup-v1 watchdog path, so their
cgroup_mem_v2.tsv is empty. Only `/usr/bin/time -v` MaxRSS is available
— that's anonymous heap, not page cache. So baseline "peak" comparisons
to v11+ (which measure cgroup memory.current) are apples-to-oranges in
absolute numbers. Within v11+ comparisons (all use the v2 watchdog) the
numbers are directly comparable.

## Next: v11 — per-slab custom step

Single custom step replaces `save_dataset` for heavy rules. Receives the lazy
Dataset, splits along time into N slabs, calls compute() + to_netcdf() per slab,
explicitly `del` and `gc.collect()` between iterations, optionally
`os.posix_fadvise(POSIX_FADV_DONTNEED)` on completed output paths to encourage
page-cache eviction.

This lives entirely in `examples/bench_rechunk.py` (no pycmor patch beyond what's
already in for v7) so it can be A/B'd safely.

Success metric: cgroup peak drops below the v1 baseline AND the cap7_atm full
yaml run completes faster than the 2×4×64 baseline at a tighter config (e.g.
4×4×40).
