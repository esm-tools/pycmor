# pycmor HR memory-pressure investigation — handoff

> Picks up from the previous handoff that produced the
> `examples/cmip7_bench_hr_ua_6hr.yaml` + `run_bench_hr_ua_6hr.sh`
> minimal-example bench. Goal: increase end-to-end throughput of the
> cap7_atm 52-rule HR run (currently 2×4×64 default → 48/52 in 2:57,
> 245 GB cgroup peak).

## TL;DR for the next agent

**Slabs are NOT dead, but they're not the universal win I initially
hoped.** Two facts that took me too long to converge on:

1. **The cgroup peak (memory.current) is mostly Linux page cache, not
   anonymous heap.** Dask-nanny worker kills are triggered by *per-process
   RSS*, not cgroup. So the "30 GB peak" we kept measuring on the bench
   ≠ what actually limits parallelism in production. The actionable
   number is `MaxRSS` from `/usr/bin/time -v`, which on the bench
   baseline is **8.5 GB** (not 30 GB).
2. **A per-slab compute+write loop with `posix_fadvise(POSIX_FADV_DONTNEED)`
   cuts heap from 8.5 GB → 0.5 GB (~16×).** That unlocks 2× parallelism
   per node (drop per-worker `memory_limit` from 64 GB to ~8 GB → run
   4×4=16 slots instead of 2×4=8 slots). But for some rule shapes
   (specifically 1hr-class hourly fields with native chunks of 1 timestep)
   it adds enough wall-time overhead to nullify the parallelism win.

So: **default slab loop ON for 6hr/daily/monthly heavy rules; opt-out
for 1hr-class fields**. Realistic throughput uplift on the full
cap7_atm yaml depends on output layout — see *Post-merge benches*
section below: with current append-mode pycmor patch, **~1.5× faster
on 6hr-class rules** (the slab loop alone wins about 1.87×, but
single-file append eats some of it back; sequential pyconcat would
eat more). For a full **1.87× per heavy rule**, the merge step needs
to be pipelined with the next rule's processing — a Prefect-side
change not yet attempted.

## Bench rule under test

```
file: atmos_6h_pl7h_ua_1587-1587.nc
shape: time_counter=1460 × pressure_levels_7h=7 × cell=421120  float32
size on disk: 13 GB (blosc_zstd-3 compressed)
raw in-memory: 17.2 GB (NOT 42 GB as the original bench yaml header claimed —
                          previous agent assumed 720×1440 regular grid; it's
                          actually the FESOM reduced-Gaussian unstructured cell
                          dim with 421120 nodes)
native NetCDF chunks: (1, 2, 421120) → 5840 chunks per variable
```

Cross-rule benches: `atmos_6h_pl7h_zg_1587-1587.nc` (same shape, different
data; needs `scale_factor: 0.10197162129779283` for unit conversion in
production but bench skips that step), and
`atmos_1h_pt_10u_1587-1587.nc` (uas rule, shape `(8760, 421120)`,
14.8 GB raw, native chunks `(1, 421120)`).

## Variants tested

All runs: 1 worker × 1 thread, dask_memory_limit=200 GB, 256 GB cgroup
on Levante compute node. Wall times across runs include OS/Lustre page-cache
warming effects (after the first read, the 13 GB input is hot in cache —
walls of "rerun" jobs are 30–50 % faster than cold-cache versions of
the same yaml).

### Single-rule (ua_6hr_pl7h) bench grid

| job | bench name | knob diff | peak GB (cgroup) | MaxRSS GB (anon) | wall | output |
|---|---|---|---|---|---|---|
| 24674259 | v1 | baseline (lazy_write=true, threads, blosc_zstd-3, no slab) | 29.6 | (no measure) | 10:03 | 11.7 GB / 2 files |
| 24675065 | v2 | + rechunk(time:30) + scheduler=sync | 27.6 | – | 11:48 | 11.7 GB / 2 files |
| 24675974 | v2 rerun | (repeat of v2) | 28.95 | – | 8:10 | 11.7 GB / 2 files |
| 24675973 | v1 rerun | (repeat of v1) | (wrong watchdog) | **8.5** | 4:12 | 11.7 GB / 2 files |
| 24675800 | v2b | rechunk + scheduler=threads | 35.6 | – | 11:40 | worse |
| 24675801 | **v3** | **lazy_write=false** | **111.7** | – | 18:50 (killed) | **DISASTER** — eager `compute()` materialises while still holding dask source |
| 24675802 | v4 | file_timespan=1MS (12 monthly files) | 28.1 | – | 12:01 | 11 GB / 13 files |
| 24675803 | v6 | netcdf_enable_compression=false | 36.3 | – | 11:42 | 19 GB / 2 files (uncompressed) |
| 24675918 | v7 | file_timespan=1MS + save_per_file (patched _save_loop_or_mf) | 28.6 | – | 7:17 | 11 GB / 13 files |
| 24675919 | **v8** | load_mfdataset_chunked (chunks at open) | **50.7** | – | 7:39 | rechunk shuffle made it WORSE |
| 24675920 | v9 | netcdf_quantize_mode=null (BitGroom off) | 31.1 | – | 7:29 | BitGroom innocent |
| 24675921 | v10 | save_engine=h5netcdf | 1.5 | – | 6:22 | FAILED — encoding incompat |
| 24676636 | v11 | slab=30 + separate files + fadvise | 18.67 | **0.53** | 13:46 | 49 files / 13.7 GB |
| 24676951 | v12 | slab=30 + single-file append | 14.23 | **0.53** | 13:02 | 1 file ✓ |
| 24676992 | **v13** | **slab=120 + separate files** | **16.20** | **0.50** | **10:43** | 13 files (need post-merge) — **best wall** |
| 24677013 | v14 | slab=120 + single-file append | 15.11 | 0.51 | 13:11 | 1 file |

Append mode (writing one file via `mode='a'` along unlimited time dim)
adds ~25–30 % wall vs separate files. HDF5 walks the B-tree on each
append; cost grows with file size.

### Cross-rule benches (uas_1hr — the rule that triggered the worker kill in production)

| job | bench | wall | MaxRSS (anon) | cgroup peak | output |
|---|---|---|---|---|---|
| 24677126 | uas baseline | 10:01 | **6.9 GB** | (no v2 watchdog) | 10.2 GB / 2 files |
| 24677127 | uas v14 (slab=120, append) | 18:38 | 0.81 GB | 10.6 GB | **73 slabs** — per-slab overhead destroyed wall |
| 24677503 | uas v15 (slab=720, append) | 18:25 | 0.62 GB | 13.45 GB | 13 slabs but append HDF5 cost grows with file size |
| 24678046 | **uas v16** (slab=720, **separate**) | **19:38** | 0.62 GB | 14.66 GB | 13 separate files — separate didn't fix wall, source-chunk B-tree is the bottleneck |

**Conclusion for uas-class**: even with right-sized slab and separate files,
wall is 1.96× baseline. uas reads 720 native source chunks per slab and
the HDF5 B-tree traversal of an 8760-chunk source dominates compute time.
Slabs don't help 1hr-class rules.

### zg_6hr_pl7h benches (sanity check, same shape as ua)

| job | bench | wall | MaxRSS | output | notes |
|---|---|---|---|---|---|
| 24677312 | zg baseline | 5:14 | 11.3 GB | 7.6 GB / 2 files | bench skips unit conversion (m²/s² → m), making baseline artificially fast |
| 24677313 | zg v14 (slab=120, append) | 10:10 | 0.51 GB | 5.1 GB / 1 file | comparable to ua v14, confirms shape-similar rules behave similarly |

### Post-merge benches

ncrcat tested but **failed**: NCO 5.0.6 (spack module) and NCO 5.3.3
(conda) on Levante neither has the BLOSC HDF5 filter plugin available,
even with `HDF5_PLUGIN_PATH` pointed at common locations. The slab files
use blosc_zstd-3 compression so ncrcat can't read them.

| job | bench | wall | MaxRSS | cgroup peak | result |
|---|---|---|---|---|---|
| 24678126 | ncrcat (NCO 5.0.6) | 0:00.71 | 17 KB | 0.04 GB | FAILED — no BLOSC plugin |
| 24678232 | (cancelled before run) | – | – | – | – |
| 24678427 | ncrcat-blosc (NCO 5.3.3 + HDF5_PLUGIN_PATH) | 0:00.35 | – | 0.15 GB | FAILED — same blosc issue |
| 24678474 | pyconcat (Python netCDF4) | **3:30** | 2.1 GB | **31.9 GB** | OK — but cgroup peak high (forgot to fadvise inputs) |
| 24678570 | pyconcat retry (+ fadvise inputs) | **4:30** | **2.15 GB** | **19.76 GB** | OK — 38 % drop in cgroup peak from input fadvise; heap cost is small, but page cache during merge still ~17 GB above the ua-slab (v13) peak |

**Production-viable post-merge tool**: pyconcat (Python netCDF4 streaming
copy) at **4:30 wall + 2.15 GB heap + 19.76 GB cgroup peak** (with
both-side fadvise). NCO is blocked on missing BLOSC plugin in Levante's
NCO builds. The merge isn't free: **adding pyconcat per-rule erases
some of the slab-loop wall savings**:

| flow | wall (ua) | wall (uas) | peak heap |
|---|---|---|---|
| baseline (no slab) | 10:03 | 10:01 | 8.5 GB / 6.9 GB |
| slab + append (current pycmor patch) | 13:11 (v14) | 18:25 (v15) | 0.51 GB / 0.62 GB |
| slab + separate + pyconcat | 10:43 + 4:30 = **15:13** | 19:38 + 4:30 = **24:08** | 0.50 / 2.15 GB during merge |

For ua-class, **append mode is actually the better all-in option**
(13:11 vs 15:13). The separate-files variant only wins if pyconcat
can be pipelined with the next rule's processing (Prefect-side change,
not yet attempted).

## What I changed in the codebase

### `src/pycmor/std_lib/files.py`

Added (all opt-in, default behaviour preserved):

- `_rule_get(rule, key, default)`: helper for the dict-vs-attr access pattern.
- `_resolve_slab_size(ds, rule)`: returns slab_size or None. Order of resolution:
  1. `rule.slab_size` (explicit override; `False` or `<=0` opts out)
  2. `rule.slab_target_bytes` (default 1 GB), slab_size = floor(target / bytes_per_step)
  3. Skip slab loop if dataset.nbytes ≤ 2 × target (small enough)
- `_save_one_with_slab_loop(ds, path, encoding, extra_kwargs, rule, slab_size)`:
  per-slab `to_netcdf`, mode='w' + `unlimited_dims=[time]` on first slab,
  `mode='a'` on subsequent slabs, `posix_fadvise(POSIX_FADV_DONTNEED)`
  after each, `gc.collect()` between.
- `_save_loop_or_mf(...)` now branches: if any dataset triggers the slab
  loop, route through `_save_one_with_slab_loop`; else fall through to
  the existing `save_per_file` / `save_mfdataset` paths.

Existing tests: 13/14 pass; 1 pre-existing failure
(`test_save_dataset` — Mock vs int compare in `create_filepath`'s CMIP7
detection at line 694) is unrelated to the slab work.

### `examples/bench_rechunk.py`

Bench-only steps (used by the bench yamls, not by production):
- `dask_rechunk(data, rule)`: explicit `.chunk(rule.dask_rechunk)` step
  (used by v2 / v2b).
- `load_mfdataset_chunked(data, rule)`: opens with explicit `chunks=`
  at open_mfdataset time (used by v8).
- `save_dataset_per_slab(data, rule)`: per-slab separate-file write +
  fadvise (used by v11 / v13).
- `save_dataset_per_slab_single_file(data, rule)`: per-slab append-
  along-unlimited write + fadvise (used by v12 / v14 / v15).

### Bench yamls + runscripts

`examples/cmip7_bench_hr_ua_6hr_v{2,2b,3,4,6,7,8,9,10,11,12,13,14}.yaml`
plus `_v15`, `_v16` for uas, `cmip7_bench_hr_zg_6hr_{baseline,v14style}.yaml`,
`cmip7_bench_hr_uas_1hr_{baseline,v14style,v15,v16}.yaml`. All use the
fixed cgroup-v2 watchdog path:
`/sys/fs/cgroup/system.slice/slurmstepd.scope/job_$JOB/memory.current`.
The original `run_bench_hr_ua_6hr.sh` watchdog uses the OLD cgroup-v1
path and silently produces an empty TSV — that's why v1's cgroup peak
is unmeasured (only `/usr/bin/time -v` MaxRSS available).

## Key insights for the next agent

### 1. cgroup peak ≠ heap

```
v1 baseline: cgroup 30 GB   /  MaxRSS 8.5 GB     → 22 GB is page cache
v11 slab30:  cgroup 18.7 GB / MaxRSS 0.53 GB    → 18 GB is page cache
```

Dask-nanny `memory_limit` watches `psutil.Process().memory_info().rss`,
which is anonymous + mapped, not page cache. So:
- The 245 GB cgroup peak in production = ~70 GB driver bloat + N×heap +
  N×file cache. With N=8 rules × 8.5 GB heap = 68 GB heap, leaves
  ~107 GB of page cache — that's the cgroup OOM trigger, not RSS.
- **Per-worker `memory_limit` can be 8 GB** (not 64) once slab loop is
  on, because the heap drops to ~0.5 GB plus dask scheduler/overhead.

### 2. Slabs win for 6hr-class, lose for 1hr-class

Native chunk count per slab is the killer:
- ua at slab=120: ~480 source chunks/slab → 7 % wall penalty
- uas at slab=720: 720 source chunks/slab → 96 % wall penalty (also 8760 total chunks vs ua's 5840)

The auto-derive in my patch picks slab_size from `bytes_per_step`,
which is the right metric for *peak*. But the wall-time cost is driven
by *source chunks per slab*, which scales differently. **Auto-derive
should also consider input native chunks per slab and skip the loop
when too many.**

Hint for the heuristic:
```python
if n_native_source_chunks_per_slab > 600:
    return None  # skip slab loop, use default save
```

### 3. lazy_write=false is a trap

`trigger_compute` calling `data.compute()` materialises the full array
to numpy *while still holding the dask source*. Peak doubles. Don't
go there for memory pressure.

### 4. Append mode adds ~30 % wall but wins on memory

Single-file output via `mode='a'` along unlimited dim re-walks the
HDF5 B-tree on each append. For 13 slabs of ua: ~30 % wall penalty
vs separate files (13:11 vs 10:43). For 73 slabs: catastrophic
(don't slab 1hr-class fields at all).

**Surprise**: pyconcat post-merge costs more memory than append:

|  | append (v14) | separate + sequential pyconcat |
|---|---|---|
| heap (MaxRSS) | **0.51 GB** | **2.15 GB** during merge (4× higher) |
| cgroup peak | **15.1 GB** | **19.76 GB** during merge |
| total wall (ua) | 13:11 | 10:43 + 4:30 = 15:13 |

So append wins on **all three metrics** (heap, cgroup peak, total wall)
for sequential execution. Separate-files is only better if the merge
can be **pipelined** with the next rule's processing on a different
worker thread / subprocess (so it overlaps).

**Recommendation: default to append mode**, which is what the current
`_save_one_with_slab_loop` patch already does. Pipelined separate+merge
is a higher-ceiling option for later if Prefect-side scheduling is
extended.

### 5. ncrcat is blocked on Levante

NCO builds in `/sw/spack-levante/nco-*` and the conda envs at
`/sw/spack-levante/miniforge3-*/bin/ncrcat` (versions up to 5.3.3) all
fail with "filter id 32001 (Blosc) not available". HDF5_PLUGIN_PATH
search didn't find a working plugin. **pyconcat (Python netCDF4
streaming copy) is the production-ready merge tool.**

## What to try next

In rough priority order:

### a. Smarter auto-derive in `_resolve_slab_size`

Add a chunk-count guard:
```python
# After computing slab_size from bytes-per-step:
n_native_chunks_per_slab = estimate from ds[var].chunks
if n_native_chunks_per_slab > 600:
    logger.info(f"slab loop disabled: {n_native_chunks_per_slab} source chunks/slab")
    return None
```

Then validate on uas_1hr (should NOT engage slab loop) and ua_6hr
(SHOULD engage). Submit a corresponding bench.

### b. At-scale validation on cap7_atm

Submit cap7_atm with my patch in `pycmor/std_lib/files.py`:
- Default config (`slab_target_bytes: 1_000_000_000`)
- Tighter parallel: 4 workers × TPW=4 × `dask_memory_limit: 16GB`
  (vs current 2×4×64)
- Compare to `submit_hr_year.sh` baseline (2:57, 48/52, 245 GB peak)

Acceptance: ≥48/52 rules complete, MaxRSS per worker ≤16 GB, total wall
< 2:30.

### c. Pipelined post-merge (only if append mode is hitting limits)

**Skip this for now** — append mode (current default) wins on all
three metrics: heap (0.51 vs 2.15 GB), cgroup peak (15.1 vs 19.76 GB),
and total wall (13:11 vs 15:13 sequential). Only revisit if (a)
append-mode wall becomes the bottleneck after at-scale validation
in (b), or (b) someone's willing to wire up Prefect-side concurrent
scheduling so pyconcat overlaps the next rule's processing on a
different worker thread / subprocess.

If revisiting, the implementation sketch:
- Slab loop emits N temp files (use `_save_one_with_slab_loop` with
  a `slab_layout: 'separate'` rule attr)
- After `save_dataset` returns, register a follow-up Prefect task
  `pyconcat_merge(temp_paths, dst, time_dim)` and mark it as runnable
  on a different worker
- Delete temp files on merge success

### d. XIOS-side fix for 1hr-class rules

The fundamental uas-class slowness is the 8760-chunk B-tree in the
input file. If XIOS could be configured to write fewer larger chunks
(e.g. `(720, 421120)` instead of `(1, 421120)`), slab loop would help
1hr fields too. That's a model-side change at FESOM/AWI-ESM3 XIOS XML
level, not a pycmor change. Could be combined with `cap7_atm`'s 1hr
fields specifically.

### e. Investigate fadvise effectiveness more

The first pyconcat had cgroup peak 31.9 GB despite fadvise on output;
adding input-side fadvise dropped it to ~5 GB. That suggests fadvise
*does* work but I had to apply it on both sides. The slab loop in my
pycmor patch only fadvises *output* — adding **input-file fadvise after
all reads complete for that slab** could lower cgroup peak further.

The challenge: the slab loop reads via xarray which reads via
netCDF4-python which reads via HDF5; we don't have a clean handle on
the input file path inside the slab loop. Options:
- Pass `rule.inputs` paths into `_save_one_with_slab_loop` and fadvise
  each at the end of every Nth slab.
- Use `fadvise(WILLNEED)` for the next slab's chunks while DONTNEED-ing
  the previous slab's.

## Bench reproducibility

```bash
cd /work/ab0246/a270092/software/pycmor
sbatch examples/run_bench_hr_ua_6hr.sh             # v1 baseline
sbatch examples/run_bench_hr_ua_6hr_v13.sh         # winner for ua-class
sbatch examples/run_bench_hr_uas_1hr_v16.sh        # uas regression check
sbatch examples/run_bench_pyconcat.sh              # post-merge bench
```

All bench output is in `/scratch/a/a270092/pycmor_bench_*/<job_id>/`.
Each has `cgroup_mem_v2.tsv` (5-sec sampling of memory.current).
SLURM logs `pycmor_bench_*_<job_id>.log` next to the project root or
in `examples/` (the dir where sbatch was invoked).

For the older v1-style runs, MaxRSS is in the SLURM log (search
`/usr/bin/time` block); cgroup_mem.tsv is empty due to the cgroup-v1
watchdog bug in `run_bench_hr_ua_6hr.sh`.

## Files

- `src/pycmor/std_lib/files.py` — **REVERTED to pre-slab-loop state (1c7f2d7)**.
  Earlier slab-loop patches removed; nothing left in the production code from
  this investigation.
- `examples/bench_rechunk.py` — bench-only steps (kept as historical record)
- `examples/cmip7_bench_hr_*.yaml` — bench yamls (~20, kept)
- `examples/run_bench_hr_*.sh` — runscripts (~20, kept; the v1 + repacked
  runscripts now use the cgroup-v2 watchdog path)
- `examples/run_bench_pyconcat.sh` — Python netCDF4 streaming concat
- `examples/repack_one.py` + `examples/run_repack_one.sh` — Python streaming
  HDF5 chunk-reshape preserving blosc compression
- `bench_hr_ua_6hr_results.md` — earlier narrative version
- `HANDOFF_memory_pressure.md` — this file

---

## FINAL UPDATE: Investigation closed with negative result

The slab-loop save path was reverted in commit `1c7f2d7`. The follow-up
"input rechunking is the unlock" hypothesis was tested with a 5×5 ensemble
and also fails. **Production stays at 2×4×64 baseline (48/52 rules in 2:57).**

### Why slab loop was reverted

The slab-loop benches that we believed succeeded (v12, v14, v15, v16) were
all **silent truncations**. xarray's `to_netcdf(mode='a')` along an unlimited
time dim refuses any size mismatch — even off-by-one. The partial trailing
slab always failed:

```
ValueError("Unable to update size for existing dimension 'time1' (n != m)")
```

Verification: v12 output is `time1=30` not 1460. v14, v15, v16 each lost
one slab's worth at the end. Every "successful" append-mode run was
short by the trailing partial slab.

Three iterative fixes (size-match guard cd8341f, name-based detection
f8bc0ae, parallel-axis guard 3174c79, slab rebalancing 5d90ccd) addressed
specific manifestations but never the root cause: **xarray's mode='a' is
not a "extend along unlimited" API**, it requires shape-exact compatibility.
The whole approach is unsalvageable as-is. Option D (separate-files +
post-merge) is technically possible but adds wall and the slab loop's
value proposition was already weak (see below).

### Why input rechunking also doesn't help

5-member ensembles, single-rule bench on ua_6hr_pl7h:

| metric | source input (5) | repacked input (5) |
|---|---|---|
| wall mean (warm-cache, runs 2-5) | **2:45** | **3:00** |
| wall first run (cold cache) | 4:54 | 3:07 |
| MaxRSS mean | **~10 GB** | **~120 GB** |
| cgroup peak mean | **~31 GB** | **~125 GB** |

Apples-to-apples (warm cache): **source is 9 % faster**, and repacked is
**12× heavier in heap, 4× heavier in cgroup peak**. The "36 % faster"
single-comparison reading we initially saw was a page-cache artifact —
the source run was the first read of a cold-cache file (4:54), the
repacked run was a warm-cache file (3:07).

Why repacked is heavier: dask reads whole HDF5 chunks at a time. With
source's `(1, 2, 421120)` chunks (~3 MB each), only a few are resident.
With repacked's `(120, 7, 421120)` chunks (~1.4 GB raw each), holding
even a handful in flight blows up heap. The bigger chunks unlock
metadata-walk speed (15 s → 0.03 s file open, real) but at the cost of
much higher in-flight working set.

### What we actually learned

1. **Page cache hit rate dominates single-rule wall**. Cold-cache:
   ~4:54. Warm-cache: ~2:45. That's a 1.7× swing from caching alone,
   far larger than any of our patches moved.

2. **The parallel agent's "1.7 MB/s aggregate read rate" diagnosis
   during P7** likely captured cold-cache contention behavior at the
   start of a multi-rule run. Once the inputs are warm, throughput
   recovers.

3. **MaxRSS ≠ cgroup peak**. The 8.5 GB we measured early as
   "anonymous heap" was correct; the 30 GB cgroup peak is mostly Linux
   page cache and is not what dask-nanny watches. So earlier
   memory-budget math was over-conservative.

4. **The slab-loop heap reduction (8.5 → 0.5 GB MaxRSS) was real**, but:
   - It can't be delivered safely with `mode='a'` on partial trailing
     slabs (silent truncation).
   - The implied parallelism uplift (more workers per node) doesn't
     improve cap7_atm wall, because (P5 vs P6 verified) single-rule
     wall is already the critical path. Adding workers doesn't help
     when N rules each take ~30 min and they don't pipeline.

5. **Input HDF5 chunk count matters under contention but not on
   warm-cache single-rule.** A model-side XIOS XML fix to write fewer,
   larger chunks at simulation time would still be valuable for
   cold-start runs and might shift the multi-rule contention profile.
   This is the only remaining optimization angle worth pursuing, and
   it's owned by the FESOM/AWI-ESM3 model team, not pycmor.

### Recommendation for the next agent

- Don't reopen the slab-loop direction. The append-mode bug is
  fundamental in xarray; option D (separate + post-merge) adds wall
  with marginal benefit since slab loop's value proposition was weak.
- Don't bother with offline input repacking. It buys nothing on warm
  cache and costs heap.
- If anyone wants to chase further wall savings: coordinate with the
  FESOM team on XIOS XML chunking. Otherwise, accept current
  production at 2×4×64 / 48/52 / 2:57.

