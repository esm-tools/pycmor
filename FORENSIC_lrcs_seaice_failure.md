# Forensic: lrcs_seaice tier failed 5 different ways across cli10–cli16

Status: investigation complete (round 2 — incorporates
[REVIEW_lrcs_seaice_failure_forensic.md](REVIEW_lrcs_seaice_failure_forensic.md)
and [ANALYSIS_cgroup_size_after_fixes.md](ANALYSIS_cgroup_size_after_fixes.md)),
fixes proposed, code unchanged.

Audience: implementer for the throttle + cache fix in cli17 onward.

## TL;DR

lrcs_seaice is the only tier where `PYCMOR_PREFECT_COLLAPSE=1` +
`netcdf_write_scheduler: synchronous` + 4 concurrent OIFS-regrid rules
**stack 4 heavyweight pipelines onto the driver process** (Prefect
ThreadPool threads share its RSS), not the LocalCluster workers.
Driver RSS hit **87 GiB** at the cli16 failure point; the subsequent
`MemoryError(4 MiB)` and `OSError [Errno -51] NetCDF: Unknown file
format` cascade are both symptoms of a single poisoned driver
interpreter — *not* corruption, *not* worker OOM, *not* HDF5-plugin
breakage. Files on disk are fine; rerunning them on a fresh process
works.

The fix is to cap concurrent OIFS-regrid rules at `max_in_flight=2` and
cache the secondary `a_ice` input across rules. Together ~30-50 lines
(cache realistically needs eviction + flow-boundary clear handling),
**budget one working day for both fixes plus tests**. Should land
cli17 at the same `4×1×64GB` worker layout, **and recover lrcs_seaice's
cgroup from 512 GB → 256 GB** (see §7 below + the cgroup analysis).

## Failure timeline (the 5 modes)

| Run | Config | Symptom | Real cause |
|---|---|---|---|
| **cli11** | TPW=4, 2×4×64GB | `_day` rad files written as 24 kB NaN stubs | Real thread race in `mask_where_no_seaice` / `regrid_atm_to_fesom_seaice_mask_pipeline` at TPW>1 — separate bug, side-stepped by forcing TPW=1. **Latent — if anyone re-enables TPW>1 for throughput on this tier, this bug reappears. Tracked as out-of-scope follow-up.** |
| **cli12** | TPW=1, 4×1×32GB | `siarea_day_nh` hangs forever (48-byte stub) | Fancy-isel on 1.5 M index against chunked daily `a_ice` produced an O(time_chunks × hemi_idx) dask graph that took >>15 min to schedule. **Fixed** in commit `9bf31f1` (mask-and-multiply rewrite) |
| **cli14** | 8×1×32GB | `siarea` + `simpeffconc` save_dataset 15 min no I/O progress | Watchdog detected the stall but couldn't recover (worker stuck in syscall). Watchdog demoted to diagnostic-only in `17a4cf6` |
| **cli15** | 8×1×32GB | `MemoryError` on rsds_seaice during compute | Same root cause as cli16 but only one driver concurrency batch made it through before OOM |
| **cli16** | 4×1×64GB, TPW=1, all prior fixes applied | **15 rule failures in 27 min**, mixed `MemoryError` + `OSError [Errno -51]` + `RuntimeError: NetCDF: HDF error` | The driver-OOM cascade detailed below |

## What actually happened in cli16

Reading `pycmor_hr_cli_pycmor-hr-lrcs_seaice-y1587-cli16_24811394.log`
in conjunction with the source:

1. **The driver process does the compute, not the cluster workers.**
   Three things conspire:
   - `PYCMOR_PREFECT_COLLAPSE=1` collapses every pipeline's steps into
     a single Prefect task body. `pipeline.py:118-130`
     (`_run_collapsed_pipeline`) then runs all steps (gather → regrid →
     mask → timeavg → save) on whichever Prefect ThreadPool thread
     submitted them — **all threads share the driver process's RSS**;
     they are not the LocalCluster workers.
   - `cmorizer.py:967-969,983-986` sets `max_in_flight = N_workers ×
     TPW = 4 × 1 = 4`. Prefect's ThreadPool submits 4 `_process_rule`
     futures concurrently, all running in the driver process.
   - `files.py:1641-1642` (and the equivalent at 1177) computes the
     dask graph as `delayed.compute()` under
     `dask.config.set(scheduler=_write_sched)` where
     `_write_sched = "synchronous"` (`netcdf_write_scheduler:
     synchronous` in the yaml inherit block, line 383). Every byte of
     regridded data flows through the **driver process's** memory
     (Prefect ThreadPool threads share its RSS), not the LocalCluster
     workers — the workers (`workers=4, threads=4, memory=238 GiB`,
     log line 81) are essentially idle for these rules.

2. **The driver process hits ~87 GiB resident set.** Each
   `regrid_atm_to_fesom_seaice_mask_pipeline` rule:
   - Loads a 6–10 GB OIFS hourly file (`atmos_1h_sfc_*_1587-1587.nc`).
   - KDTree-gathers to a `(8760, 3146761)` float32 array (~110 GiB if
     materialized — kept lazy until compute).
   - Loads the 860 MB `a_ice.fesom.1587.nc` *independently per rule*
     (`_load_secondary_mf` in `custom_steps.py:2145-2187` has no cache;
     log lines 3431/3442/3458 show 4 separate F4-instrument prints for
     the same file).
   - `.where()`s the regrid by the a_ice mask, time-averages, and
     calls `_save_dataset_impl` → `delayed.compute(scheduler="synchronous")`.
   - **All of this happens in driver process memory** because the
     scheduler is `synchronous`.

   With `max_in_flight=4` and 4 concurrent rules each holding their own
   OIFS file + gathered slab + a_ice copy + compute buffers, driver
   process RSS climbs to 87 GiB (`/usr/bin/time -v` line 6561).

3. **The cascade.** rsds / rsus / rsds_day finish (or fail with OOM)
   at t=415 s with `MemoryError(4 MiB)` on `dask/array/_shuffle.py:314`.
   That 4 MiB allocation can't be satisfied because the driver
   process's anonymous-mmap pool is fragmented and at/near the process
   commit limit. At t=415.5 s, `wait(batch_futures)` returns and the
   next Prefect batch starts (rsus_seaice_day, rlds_seaice, rlus_seaice,
   sifllattop). At 0.1 s into those, `gather_inputs.load_mfdataset`
   calls `netCDF4.Dataset(...)` which internally mmaps the file. With
   the driver's mmap arena exhausted, HDF5's `H5FD_sec2_open` fails and
   the netCDF library returns `NC_ENOTNC = -51 ("Unknown file
   format")` — a deeply misleading errno. **Files are NOT corrupt**
   (`ncdump -h` reads them fine after the SLURM job ends).

4. **`RuntimeError: NetCDF: HDF error` is the same cause via a
   different HDF5 path.** Log line 4698 reveals the inner message:
   `Blosc Filter Error: blosc_filter: can't allocate decompression
   buffer`. The blosc plugin is loaded fine (`codecs=['blosc_lz4',
   'blosc_zstd', 'zlib', 'zstd']` at log line 18) — Hypothesis 3
   (plugin-path leak across processes) is ruled out.

## Why ONLY lrcs_seaice

7 rules in the lrcs_seaice yaml use one of
`regrid_atm_to_fesom_seaice_mask_pipeline` /
`regrid_atm_to_fesom_seaice_mask_negate_pipeline`:

- `rsds_seaice`, `rsds_seaice_day`
- `rsus_seaice`, `rsus_seaice_day`
- `rlds_seaice`
- `rlus_seaice`
- `sifllattop`, `siflsenstop`, `sbl`

With `max_in_flight=4`, every Prefect batch in the OIFS-rule phase
fills up entirely with these monsters running concurrently. No other
tier stacks four 10-GB OIFS hourly × FESOM-HR regrid + mask pipelines
onto one driver process.

cap7_atm has bigger files (tas_1hr = 21 GB) but the rules don't share
the regrid+mask chain that lives entirely on the driver in
synchronous mode; cap7_atm rules return early in `_safe_to_netcdf`'s
dask path. extra_atm has the heavy files but only ~20 rules and not
the 7-way OIFS-regrid concurrency. lrcs_seaice is uniquely bad.

## Hypotheses, scored

1. **Driver poisoning by first OOM (correct in spirit)** —
   confirmed, but the poisoned process is the **driver**, not a dask
   worker. The 4 LocalCluster workers were essentially idle.
2. **Lustre transient I/O** — ruled out; files read cleanly after the
   job ends, and the errno path is `H5FD_sec2_open` (mmap), not
   `read()`.
3. **HDF5 plugin path leak** — ruled out; plugins are loaded
   (codec list in log line 18) and the failure is allocation, not
   filter.
4. **Single 'first-rule killer'** — partially right (rsds_seaice is
   indeed the biggest), but the real shape is "any 4-rule batch in
   this pipeline family".
5. **OIFS file shared between rules without caching** — confirmed
   (`_load_secondary_mf` opens a_ice 7 separate times under load,
   primary OIFS file 2-4 times depending on rule structure). Real
   contributor but not the dominant factor.
6. **Atomic tmpfs staging fills /tmp** — ruled out for cli16
   (lrcs_seaice was submitted with `PYCMOR_TMPFS_STAGING=auto`;
   compute-node `/tmp` is the 63 GB tmpfs; the largest staged file
   would be ~1 GB; with `max_in_flight=4`, peak tmpfs use ≤ ~4 GB,
   well under the 63 GB cap and unrelated to the driver-process
   memory exhaustion). Important corollary: PLAN_save_dataset_reliability
   Option A would *not* have helped this run, because the netCDF
   write isn't the proximate cause; driver-side compute is.

## Prioritized fix list

### Fix #1 — throttle OIFS-regrid rules to `max_in_flight=2`

- **Where**: new rule-group/throttle attribute consumed by
  `_parallel_process_prefect` in `cmorizer.py` around lines 962-993.
- **Mechanism choice**: prefer a **pipeline-level attribute**
  (`throttle_group: oifs_regrid` on the pipeline definition itself,
  not the rule), so the throttle key flows naturally from
  pipeline → rule and there's one yaml edit per pipeline (3-4 places)
  instead of per rule (7+ places). Pipeline-name sniffing
  (`"regrid_atm" in pipeline_name`) is rejected — couples cmorizer
  to specific pipeline naming and renames break silently.
- **Algorithm**: in `_parallel_process_prefect` batching, keep a
  per-`throttle_group` semaphore-style counter. Default unlimited;
  `throttle_group=oifs_regrid` caps at 2.
- **Effort**: ~15 LoC core + 3-4 yaml lines.
- **Addresses**: cli11 (driver pileup), cli14 (post-pileup save hangs),
  cli15 (post-pileup MemoryError), cli16 (the cascade). Caps driver
  process RSS at **~45 GiB peak** (per the cgroup analysis).

### Fix #2 — `functools.lru_cache` on `_load_secondary_mf`

- **Where**: `custom_steps.py:2145-2187`.
- **Realistic scope**: the 10-LoC sketch undersells this. Concerns:
  - **Lazy DataArrays hold open file handles.** Each cached entry
    keeps the underlying `Dataset` and its file descriptors live.
    After 20-50 rules × ~3 secondary inputs each, that's hundreds of
    open fds. Use `lru_cache(maxsize=...)` with an explicit eviction
    callback that closes the file.
  - **Cache scope** — clear at end of `_parallel_process_prefect`
    via a context manager or explicit `_load_secondary_mf.cache_clear()`
    call. Per-flow scope is the right answer; per-rule defeats the
    purpose; never-clear leaks files until job-end.
  - **Mutation safety**: callers may do `da.values` or in-place
    modification. Either return `da.copy()` from the cached wrapper
    (cheap; lazy) or assert read-only consumer behavior. Pick the
    former for safety.
  - **Thread safety under Prefect ThreadPool**: two concurrent rules
    missing the cache for the same key both call `open_mfdataset`;
    one wins the cache slot but both opened files. Wasted work, not
    incorrect. `lru_cache` is dict-safe; the file-open isn't
    serialized. Acceptable; worth a comment.
- **Effort**: ~30-40 LoC including eviction policy + flow-boundary
  clear + thread-safety comment. Realistic.
- **Addresses**: ~6 GB driver process RSS savings (a_ice currently
  loaded 7×) and speeds up rule batches.

### Fix #3 — move `_save_dataset_impl`'s `delayed.compute()` off the synchronous scheduler

- **Why exists**: `netcdf_write_scheduler: synchronous` was added to
  dodge the historical HLG-pickling failure:
  - Specific error: `TypeError: Could not serialize object of type
    _HLGExprSequence` → root cause `cannot pickle '_thread.lock'
    object`
  - References: `files.py:1163-1170` (the inline note explaining the
    workaround), `files.py:521 / 1178` (the existing
    `compute=False` + explicit `delayed.compute()` pattern under
    sync scheduler). Find the introducing commit via
    `git log --follow --oneline src/pycmor/std_lib/files.py`
    and the design doc trail starting from
    `DESIGN_PROPOSAL_subflow_deadlock.md` if it exists in the repo.
- **Real fix**: compute on the LocalCluster via
  `distributed.Client(...).compute(delayed)` — not just
  `scheduler="distributed"` (which re-triggers the pickle path). The
  existing `compute=False` + sync pattern is the model fix #3 needs
  to copy onto a distributed Client. **Without this commit-hash
  reference, the implementer of fix #3 will re-discover the HLG bug
  and waste a day.**
- **Effort**: medium-high. Risk of reintroducing the HLG bug if
  miscoded.
- **Addresses**: cli15 and cli16 root cause directly — bytes flow
  through workers, driver process stays small. Strictly stronger
  than fix #1, but not required for the cgroup-shrink win.

### Fix #4 — cache primary `load_mfdataset` too

- rsds / rsds_day / sbl / rsus_seaice / rsus_seaice_day all open the
  same 10 GB OIFS file in the same batch.
- Same `lru_cache` trick keyed on `(file_set, dtype, chunks)`.
- **Effort**: small. Cache lifetime must end at batch boundary to
  avoid holding the file forever; reuse the eviction infrastructure
  from fix #2.

### Fix #5 — make driver `MemoryError` a (best-effort) fatal flow condition

- **Where**: `cmorizer.py:1002-1007`. Kill the flow instead of
  submitting more batches into a poisoned interpreter.
- **Caveat — CPython `MemoryError` catch is best-effort**: once the
  process is at OOM, the interpreter may not have memory to execute
  the `except MemoryError:` handler, format the log message, or
  build the SLURM exit-code trace.
- **What works more reliably as defense-in-depth**:
  - A SLURM watchdog process (sibling sbatch job or a coroutine
    monitoring driver RSS via `/proc/<pid>/status`) that issues
    `scancel --signal=TERM` when RSS exceeds a threshold. Defers
    the out-of-memory work to a separate process that still has
    memory.
  - Hard `os._exit(2)` early in the catch path, so the handler does
    one syscall and no Python-level work.
- **Effort**: small for the catch; medium for the watchdog
  defense-in-depth.
- **Value**: doesn't fix the cause, but turns a 15-rule cascade into
  a 3-rule fail-fast. Makes future debug tractable. Best-effort —
  add the watchdog as defense in depth.

## Cgroup implications after #1 + #2 land

(See [ANALYSIS_cgroup_size_after_fixes.md](ANALYSIS_cgroup_size_after_fixes.md)
for the full table.)

| Scenario | Driver peak | Worker peak | Total | Cgroup needed |
|---|---|---|---|---|
| Current cli16 (no throttle, sync scheduler) | 87 GiB | ~0 (idle) | ~90 GiB | 512 GB |
| **#1 + #2 only** (throttle, lru_cache; sync scheduler) | ~45 GiB | ~0 | ~50 GiB | **256 GB** |
| **#1 + #2 + #3** (also offload compute to workers) | ~10-20 GiB | 4 × ~30 GB = 120 GB | ~140 GB | **256 GB** |

Either path returns lrcs_seaice to 256 GB. Fix #3 isn't strictly
necessary for the cgroup shrink. **Net memory savings**: one tier
shrinks 512 → 256 GB.

**Speed cost**: ~20-40% slower elapsed on lrcs_seaice (running 2
concurrent OIFS-regrid rules instead of 4), partially offset by fix
#2 (lru_cache saves redundant `a_ice` reloads). "20-40% slower but
reliable" beats "fast but loses 15 rules and needs a manual restart
cycle."

Other tiers:

- **extra_atm**: stay at 512 GB until its own forensic exists.
  "Heavy outlier rules" in integration plan, but no per-rule
  forensic comparable to lrcs_seaice's. Run at 256 GB with the
  throttle applied; if MaxRSS stays under ~50 GiB, ship at 256 GB.
- **cap7_ocean**: may need MORE memory if PLAN_save_dataset_reliability
  Option A (tmpfs staging) lands — hfx_3D × 8 workers ≈ 64 GB peak
  tmpfs is a real bite. Three options: per-rule
  `netcdf_tmpfs_staging: false` opt-out for big-3D rules; bump
  cgroup to 384 GB; skip Option A entirely for cap7_ocean. Recommend
  per-rule opt-out before merging Option A.
- **All other tiers**: unchanged at 256 GB / 4×4×16.

## One-line "do this next"

**Implement #1 (per-pipeline `throttle_group` to cap OIFS-regrid at
`max_in_flight=2`) + #2 (`lru_cache` on `_load_secondary_mf` with
per-flow eviction and `.copy()` on hit) — budget one working day for
both fixes plus tests.** Should let lrcs_seaice complete on a
`4×1×64GB / 256GB cgroup` layout (down from cli16's 512 GB).

## Practical sequencing

1. **Land fixes #1 + #2** (~30-50 LoC, one working day with tests).
2. **Retest lrcs_seaice at 256 GB cgroup.** If passes, recovered one
   512 GB tier.
3. **Don't touch extra_atm or cap7_ocean** until you have empirical
   data for each. extra_atm needs a forensic; cap7_ocean needs an
   Option A decision.
4. **If Option A lands**, decide cap7_ocean policy first before
   deploying it.
5. **Fix #3** (distributed-Client offload) ships separately when
   someone has time to chase the HLG-pickle bug history. Not blocking.
6. **Fix #5** (MemoryError catch + RSS watchdog) is defense in depth
   — useful for future-proofing but not blocking either.

## Out-of-scope follow-ups

- **TPW>1 race in `mask_where_no_seaice` / regrid_atm_to_fesom_seaice_mask_pipeline**
  (cli11 cause). Side-stepped by forcing TPW=1 in all post-cli11
  configs. If anyone ever re-enables TPW>1 for throughput on this
  tier, this bug reappears. Track explicitly; don't let the
  workaround quietly become assumed-fixed.
- **extra_atm forensic** to determine whether its 512 GB allocation
  has the same driver-pileup cause as lrcs_seaice or is genuinely
  worker-heap-bound. Until then, 512 GB stays.

## File:line citations

- `/work/ab0246/a270092/software/pycmor/src/pycmor/core/cmorizer.py:962-993` — `_parallel_process_prefect` batch submission, `max_in_flight` derivation
- `/work/ab0246/a270092/software/pycmor/src/pycmor/core/cmorizer.py:1002-1007` — driver-side batch error handling (Fix #5)
- `/work/ab0246/a270092/software/pycmor/src/pycmor/core/pipeline.py:118-130` — `_run_collapsed_pipeline` runs all steps on submitting thread
- `/work/ab0246/a270092/software/pycmor/src/pycmor/std_lib/files.py:521` — existing `compute=False` + sync-scheduler pattern (Fix #3 model)
- `/work/ab0246/a270092/software/pycmor/src/pycmor/std_lib/files.py:1163-1183` — synchronous scheduler workaround comment (Fix #3 context)
- `/work/ab0246/a270092/software/pycmor/src/pycmor/std_lib/files.py:1633-1648` — write-path compute under synchronous scheduler
- `/work/ab0246/a270092/software/pycmor/examples/custom_steps.py:2145-2187` — `_load_secondary_mf`, the no-cache function (Fix #2)
- `/work/ab0246/a270092/software/pycmor/examples/custom_steps.py:3281-3393` — `regrid_oifs_to_fesom`
- `/work/ab0246/a270092/software/pycmor/examples/custom_steps.py:3490-3570` — `mask_where_no_seaice`
- `/work/ab0246/a270092/software/pycmor/awi-esm3-veg-hr-variables/lrcs_seaice/cmip7_awiesm3-veg-hr_lrcs_seaice.yaml:383` — `netcdf_write_scheduler: synchronous` inherit
- `/work/ab0246/a270092/software/pycmor/awi-esm3-veg-hr-variables/lrcs_seaice/cmip7_awiesm3-veg-hr_lrcs_seaice.yaml:982-1133` — the 7 OIFS-regrid rules
- `/work/ab0246/a270092/software/pycmor/examples/run_hr_yaml_cli.sh:46-47,98-108` — N_WORKERS / TPW / MEM_PER_WORKER plumbing
- `/work/ab0246/a270092/software/pycmor/pycmor_hr_cli_pycmor-hr-lrcs_seaice-y1587-cli16_24811394.log:18` — codec list confirming plugins
- `/work/ab0246/a270092/software/pycmor/pycmor_hr_cli_pycmor-hr-lrcs_seaice-y1587-cli16_24811394.log:81` — LocalCluster sizing
- `/work/ab0246/a270092/software/pycmor/pycmor_hr_cli_pycmor-hr-lrcs_seaice-y1587-cli16_24811394.log:3431,3442,3458` — `_load_secondary_mf` repeat opens (the F4 instrumentation)
- `/work/ab0246/a270092/software/pycmor/pycmor_hr_cli_pycmor-hr-lrcs_seaice-y1587-cli16_24811394.log:3673-5428` — the 15-rule cascade
- `/work/ab0246/a270092/software/pycmor/pycmor_hr_cli_pycmor-hr-lrcs_seaice-y1587-cli16_24811394.log:4698` — the real "blosc filter: can't allocate decompression buffer" message that explains the `NetCDF: HDF error`
- `/work/ab0246/a270092/software/pycmor/pycmor_hr_cli_pycmor-hr-lrcs_seaice-y1587-cli16_24811394.log:6561` — driver process MaxRSS 87 GiB
