# pycmor HR optimization — Phase 2 plan

Phase 1 closed with two clean negative results: slab-loop save path
(reverted in `1c7f2d7`) and offline input rechunking (5×5 ensemble
showed source 9 % faster on warm cache, 12× lighter in heap). See
`HANDOFF_memory_pressure.md` for the full record.

This file plans Phase 2: targeted attacks on the bottlenecks we
**actually** measured, ranked by ROI/effort, with parallel-test
groupings called out.

## Confirmed bottlenecks

From the Phase 1 ensemble + parallel agent's runs:

| bottleneck | evidence | plausible mechanism |
|---|---|---|
| Page-cache hit rate dominates single-rule wall | cold 4:54 vs warm 2:45 (1.78×) | first-rule reads from disk, subsequent rules from RAM |
| HDF5 B-tree contention under concurrent reads | parallel agent measured ~1.7 MB/s aggregate at P7 | 8 readers × 5840 small chunks each → metadata seek storm |
| Per-rule pipeline-step orchestration overhead | not directly measured; **Prefect 3.x benchmarks at ~1500 tasks/hour ≈ 2.4 s/task** ([source](https://clankercloud.ai/blog/best-tools-containerized-kubernetes-data-pipelines-2026-benchmark)) | 13 tasks/rule × 52 rules × 2.4 s = ~27 min orchestration overhead per cap7_atm run |
| Many small native HDF5 chunks per file | 5840 chunks/file (6hr) or 8760 (1hr) | structural decision at XIOS (model side); source chunk shape `(1, 2, 421120)` |

Single-rule wall is the critical path (P5 vs P6 verified — adding
workers doesn't help). So every minute saved per heavy rule turns
into a minute saved on cap7_atm wall.

## Variants under consideration

### A — HDF5 chunk cache + h5netcdf engine

**What**: switch `pycmor.core.gather_inputs.load_mfdataset` from the
default `netcdf4` engine to `h5netcdf`, and pass `rdcc_nbytes=256_000_000`
(256 MB) via h5py file kwargs.

**Why**:

1. HDF5's per-dataset chunk cache default is **1 MiB** ([HDF Group](https://forum.hdfgroup.org/t/why-increasing-rdcc-nbytes-and-rdcc-nslots-will-result-in-a-decrease-in-indexing-performance/9062))
   — 1 chunk fits in cache. Our chunks are ~1.5 MB each, so 0.6
   chunks fit. Every read walks the B-tree + decompresses. Bumping
   to 256 MB holds ~170 chunks, so chunks reused across pipeline
   steps (timeavg, set_global, set_variable, set_coordinates,
   map_dimensions, etc.) become cache hits.
2. [xarray docs](https://docs.xarray.dev/en/stable/user-guide/io.html)
   say `engine="h5netcdf"` is "often faster" for `open_mfdataset`.
   One reported workflow saw 4×.

**Implementation**: ~5–10 LOC in `gather_inputs.load_mfdataset`. Pass
`engine="h5netcdf"`, then construct an `h5netcdf` kwarg dict for the
`rdcc_nbytes` value. Optionally make both configurable via the
`pycmor` config block.

**Expected impact**: largest on cold-cache. Warm-cache may also
improve by avoiding HDF5 metadata re-walks within a rule's pipeline.

**Risk**: low. h5netcdf is in pycmor's deps (1.8.1 confirmed). Worst
case it's slower and we revert.

### B — `inline_array=True` for dask graphs

**What**: pass `inline_array=True` to `xr.open_mfdataset(...)` in
`load_mfdataset`.

**Why**: with 5840 chunks per file, the dask task graph has 5840
separate task nodes. `inline_array=True` collapses the graph
representation (chunks become inline values rather than separate task
references). [xarray docs](https://docs.xarray.dev/en/stable/generated/xarray.open_dataset.html)
mention this option specifically for the many-chunks scenario.

**Implementation**: 1-line change.

**Expected impact**: reduces Python overhead in dask graph
evaluation. May be 5–15 % wall for compute-heavy steps. Independent
of A; can layer.

**Risk**: low — flag is a standard xarray kwarg.

### C — Prefect task collapse

**What**: refactor pycmor's pipeline runner so multiple metadata-only
steps (`set_global`, `set_variable`, `set_coordinates`,
`map_dimensions`, attribute touches) are batched into a single
Prefect task. Heavy steps (`load_mfdataset`, `timeavg`,
`save_dataset`) stay as their own tasks.

**Why**: Prefect 3.x ≈ **2.4 s/task** scheduler latency
([benchmark](https://clankercloud.ai/blog/best-tools-containerized-kubernetes-data-pipelines-2026-benchmark)).
13 tasks/rule × 52 rules = 676 task invocations × 2.4 s = **~27 min
of pure orchestration overhead** in the cap7_atm 2:57 baseline.
Collapsing to ~5 tasks/rule (load + apply_metadata + timeavg +
unit_conv + save) would save ~17 min.

**Implementation**: more invasive. Touches `pycmor/core/pipeline.py`
and the cmorizer's task wiring. ~1–2 days. Risk of breaking
Prefect's caching / hashing invariants for task results.

**Expected impact**: ~15 % reduction on cap7_atm wall. Independent
of A and B; layers cleanly.

**Risk**: medium. Behavior change in pycmor's core. Need a careful
test against the existing test suite plus a validation cap7_atm run.

### D — Shared input loading across rules

**What**: when N rules read the same input file, load it once and
dispatch the dataset to each rule instead of re-loading per rule.

**Why**: cap7_atm has many rules reading the same XIOS streams (e.g.
multiple variables from `atmos_1h_pt_*.nc`). The parallel agent's
"1.7 MB/s aggregate" diagnosis at P7 was likely concurrent rules
contending on shared files. Sharing the load step could give 2-4×
I/O reduction on those shared inputs.

**Implementation**: architectural. Touches scheduling (`cmorizer.py`)
and the pipeline runner. ~1 week.

**Expected impact**: significant on heavy-tail rules that share
inputs. Conservative estimate: 15-30 % off cap7_atm wall on top of
A-C. But unverified.

**Risk**: high — biggest behavioral change. Defer until after
A-C measured.

### E — Cache prewarming

**What**: explicit `cat input.nc > /dev/null` on each rule's input
files in parallel before starting that rule's pipeline.

**Why**: production runs are usually cold-cache (year N's data was
just written). Prewarming lets the I/O happen overlapped with any
prior rule's compute, instead of stalling at the start of the new
rule.

**Implementation**: shell wrapper or pycmor-side `&` background read.
Simple in concept, fiddly in practice (need to know which files
the rule will read).

**Expected impact**: limited unless cap7_atm has long compute phases
that overlap. Probably not worth pursuing alone.

**Risk**: low.

### F — Lustre input striping (`lfs migrate -c 8`)

**What**: re-stripe input files across 8 OSTs (currently default,
likely 1 OST per file).

**Why**: with 8 concurrent rules reading the same OST, throughput is
serialized. Striping across 8 OSTs gives parallel disk reads.

**Implementation**: `lfs migrate -c 8 file.nc`. ~minutes per file,
one-time per simulation year.

**Expected impact**: helps under contention. Less under serial /
warm-cache.

**Risk**: low. Reversible.

## Parallel test groupings

Variants split into two independent axes that can be tested in
parallel:

```
Axis 1 (load-step optimizations, share load_mfdataset implementation):
  A:  h5netcdf + rdcc_nbytes
  B:  inline_array=True
  AB: combine A+B in one yaml

Axis 2 (orchestration / scheduler):
  C:  Prefect task collapse

Axis 3 (architectural — defer):
  D:  shared input loading
  E:  cache prewarming
  F:  Lustre input striping
```

Test plan:

1. **Round 1 (load-step axis)**: A, B, AB in parallel. 3 variants ×
   5-member ensemble × ~5 min/run on warm cache = ~75 min wall total
   when batched. Compare to the existing source-input ensemble
   (24694882 + 24695049/51/53/55, mean 2:45 warm-cache).

2. **Round 2 (orchestration)**: C alone, after A/B/AB winner is
   picked. The Prefect collapse is independent of which load-step
   variant we use; we'll layer it on top of the round-1 winner.

3. **Round 3 (validation at scale)**: take the best combo from rounds
   1+2 and run cap7_atm with the parallel agent. Compare to baseline
   (2:57 / 48/52). Expectation: 2:00–2:15 / 48/52.

## What I need

- **Approval to modify `src/pycmor/core/gather_inputs.py`** (axis 1).
  Small change behind opt-in env var or config key; default behavior
  unchanged when the new key isn't set.
- **A clean test rule for round 1**. Use `ua_6hr_pl7h` (already has
  the bench yaml + runscript). Same as Phase 1 source-input ensemble
  — apples-to-apples comparison.
- **5-member ensembles** for each variant in round 1. Compute is
  cheap (~5 min/run × 5 = 25 min wall per variant, fully parallel).
- **No drop_caches between runs needed**. Phase 1 showed ensemble
  variance is dominated by cold-vs-warm-cache; first run cold, rest
  warm. The same pattern across A/B/AB will cancel out as long as
  ensembles are submitted in the same order.

Optional but useful:

- **A second test rule** like `uas_1hr` (8760-timestep, different
  shape, more chunks). Lets us see whether axis-1 wins generalize
  across rule shapes.
- **memray or py-spy on one A/AB run** — confirms whether HDF5
  `_read_chunk` time actually drops with the bigger cache, or
  whether some other cost dominates.

## Decision tree

```
Round 1 (A / B / AB on ua_6hr_pl7h):

  AB wall ≤ 2:30 (≥10% off baseline)
    → ship AB; queue Round 2 (C on top of AB)
  
  AB wall ~2:40-2:45 (no measurable win)
    → check warm-vs-cold split; cold-cache win may exist
    → if no cold-cache win either, drop axis 1; do C standalone
  
  AB wall > 2:45 (regression)
    → diagnose; engine swap rarely regresses but possible
    → revert and only try C
```

## Round 1 result: regression on both variants

5×5 ensemble, ua_6hr_pl7h. Source baseline: ~2:45 warm-cache, ~5:02
cold-cache.

| variant | wall mean | save step | verdict |
|---|---|---|---|
| baseline (netcdf4, default) | 2:45 (warm) / 5:02 (cold) | ~1:46 | reference |
| B (inline_array=True) | 5:30 | n/a | **2× slower** |
| A (h5netcdf engine) — first attempt | 2:35 (failed at save) | crash | save errored on `compression='unknown'` |
| A (h5netcdf engine) — after `_strip_unportable_encoding` fix | **10:42** uniform across 5 runs | **5:00** | **3.9× wall, 2.8× save regression** |

**Why h5netcdf is slower for our case**: it wins on file-open
(measured 0.03 s vs 15 s for source) but loses 2.8× on the chunked
data read during save. xarray docs say "h5netcdf is often faster" for
`open_mfdataset` with many files, where file-open dominates. Our
pattern is 1 large file with many small chunks, where the
per-chunk-read path matters far more than the file-open cost.

**Why inline_array=True is slower**: with our 17 GB array spread
across 5840 chunks, inlining each chunk reference into the dask task
graph makes the graph much larger to evaluate. The intended use case
is small arrays where the graph indirection dominates.

**Patches kept** (small, opt-in, default off):
- `src/pycmor/core/gather_inputs.py`: `xarray_open_mfdataset_engine_override`
  + `xarray_open_mfdataset_inline_array` rule attrs / config keys.
  Default behavior unchanged.
- `src/pycmor/std_lib/files.py`: `_strip_unportable_encoding` —
  drops `compression="unknown"` from coord encoding before save.
  Defensive cleanup; would prevent breakage if anyone explicitly
  opts into the h5netcdf engine in the future.

**Round 1 axis closed.** Load-step optimizations researched online
don't apply to our pattern.

---

## Round 2: Prefect task collapse

Background and motivation in OPTIMIZATION_PLAN.md "Variants under
consideration / variant C". Prefect 3.x at ~2.4 s/task × 676
invocations ≈ 27 min orchestration overhead per cap7_atm run (~15 %
of the 2:57 baseline).

Plan:
1. Map the current 13-task pipeline. Identify which tasks are
   metadata-only (no dataset compute, just attribute touches): likely
   `set_global`, `set_variable`, `set_coordinates`, `map_dimensions`,
   `manual_checkpoint`, `show_data`, possibly the trigger_compute
   no-op when `lazy_write=true`.
2. Collapse those into a single Prefect task `apply_metadata`.
3. Heavy tasks stay separate: `load_mfdataset`, `timeavg`,
   `handle_unit_conversion`, `save_dataset`.
4. Goal: 13 → ~5 tasks/rule, saves ~17 min/run on cap7_atm.

Risk: Prefect's task hashing / caching may rely on per-step
invocation. Need to preserve cache key behavior or document the
break.

Implementation plan to be filled in once the pipeline runner code
is read.

### Round 2 result: also a regression-free non-win

Patched `pycmor.core.pipeline.Pipeline._prefectize_steps` to optionally
collapse all steps into one Prefect Task. Activated via per-pipeline
yaml `collapse_steps: true` or env var `PYCMOR_PREFECT_COLLAPSE=1`.
Two prerequisites needed in addition:
- `pycmor.core.validate`: add `collapse_steps` to the pipelines schema
  (else the yaml fails Cerberus validation).
- The collapsed loop has to **unwrap Prefect State objects** because
  `pycmor.core.caching.manual_checkpoint` returns
  `Completed(data=ds)` when the workflow backend is "prefect", relying
  on the per-step Task chain to unwrap. Inside one collapsed Task,
  the loop has to do `state.result(raise_on_failure=True)` itself.

Controlled pair (same yaml, same node assignment timing,
`ua_6hr_pl7h`):

| variant | wall | MaxRSS | cgroup | n_finished_tasks |
|---|---|---|---|---|
| baseline (13 tasks) | **7:12** | 11.2 GB | 33.9 GB | 13 |
| collapse (1 task) | **7:13** | 10.5 GB | 32.1 GB | 2 |

**1-second wall difference**. No measurable win.

The "2.4 sec/task" Prefect benchmark I cited was for **Kubernetes-orchestrated production Prefect** with API-server telemetry. Our **local Prefect with DaskTaskRunner** has far lower per-task overhead — probably <100 ms. The 27-min-overhead-per-cap7_atm estimate was wildly off; actual overhead is <1 second per rule.

**Round 2 axis closed.** Patches kept (default off, opt-in via yaml/env):
- `src/pycmor/core/pipeline.py`: `collapse_steps` kwarg + env var
- `src/pycmor/core/validate.py`: schema entry
- State-unwrap inside the collapsed loop

These will sit dormant unless someone explicitly opts in.

---

## Investigation closing summary

After Phase 1 (slab loop, input rechunking) and Phase 2 (Round 1
load-step, Round 2 orchestration), all four cheap optimizations
researched failed to deliver a wall-time win on `ua_6hr_pl7h`:

| direction | result |
|---|---|
| slab loop (Phase 1) | xarray `mode='a'` silent-truncates partial trailing slabs; reverted |
| input rechunking (Phase 1) | source 9% faster on warm cache, repacked 12× heavier in heap |
| h5netcdf engine (Round 1A) | save step 2.8× slower (chunked-data read path) |
| inline_array=True (Round 1B) | dask graph blowup on 5840-chunk arrays; 2× wall regression |
| Prefect task collapse (Round 2) | local Prefect overhead is ~zero; 1-second wall diff |

The bottleneck is "the work itself" — I/O (HDF5 chunk reads, blosc
decompress) + compute (xarray pipeline) + write (recompress, save).
All of these are well-optimized at the library level; we have no
cheap leverage at the application level.

**Production stays at 2×4×64 (48/52 rules in 2:57).**

The remaining theoretical wins are **architectural (Round 3)** and
each is a multi-day-to-multi-week engineering effort:

- **D — Shared input loading**: when N rules read the same XIOS file,
  load once and dispatch instead of N separate loads. Owner:
  pycmor cmorizer scheduling logic. Estimate: ~1 week. Reward
  bounded by how much input overlap actually exists in cap7_atm
  (needs an audit).
- **E — Cache prewarming**: read input files into OS page cache
  in parallel before pycmor processes a rule. Doesn't reduce total
  I/O, only overlaps it with prior compute. Limited reward;
  estimate: 1–2 days for a clean implementation.
- **F — Lustre input striping** (`lfs migrate -c 8`): re-stripe input
  files across more OSTs. ~one-time per simulation year. Helps under
  concurrent contention. Estimate: minutes to apply, hours to bench.
- **G — XIOS-side input chunking**: model team owns; needs
  coordination with FESOM/AWI-ESM3 maintainers.

Of these, **Round 3.D** has the highest theoretical reward (could be
2-4× I/O reduction on rules that share files in cap7_atm) but
requires the deepest changes. Round 3.E and 3.F are cheap to test
but have small expected impact. Round 3.G is the right answer in the
long run but is out of pycmor's hands.

---

## Round 3 audit results: all four dead

After auditing each Round 3 candidate against the actual data, all four
turn out to be either low-ceiling, already-in-effect, or unverifiable
without a multi-day at-scale test. Detailed findings below.

### D — Shared input loading: dead

Audit: parsed `awi-esm3-veg-hr-variables/cap7_atm/cmip7_awiesm3-veg-hr_cap7_atm.yaml`.

```
52 rules, 49 distinct input patterns
3 patterns shared by >1 rule:
  2x atmos_1h_sfc_rlds_*.nc       (rlds_day, rlds_1hr)
  2x atmos_3h_prsn_prsn_*.nc      (prsn_day, prsn_3hr)
  2x atm_remapped_1d_2t_*.nc      (hurs_day_max, hurs_day_min)
46 rules have unique inputs (no sharing).
Maximum I/O reduction from shared loading: 5.8 %
```

XIOS naturally writes one variable per stream, so input sharing is
structurally limited across cap7_*. ~1 week of cmorizer scheduling
refactor for ≤6 % I/O reduction → **dead**.

### E — Cache prewarming: dead

Per-step timings from bench logs (lazy_write=true, ua_6hr_pl7h):

| step | duration |
|---|---|
| load_mfdataset | ~0.3 s (metadata only, lazy graph build) |
| get_variable | ~0.02 s |
| timeavg | ~0.4 s |
| handle_unit_conversion | ~0.03 s |
| set_global / set_variable / set_coordinates / map_dimensions | ~2 s combined |
| manual_checkpoint / trigger_compute / show_data | ~0.1 s combined |
| **save_dataset** | **1:46 (warm) to 3:30 (cold)** |

`save_dataset` is **>99 % of single-rule wall** because `lazy_write=true`
defers all real work (chunk reads, decompress, transform, recompress,
write) into the save step. There's effectively no compute-phase to
overlap I/O with via prewarming. Reward ceiling is single-digit
seconds per rule. **Dead**.

### F — Lustre input striping: already in effect

`lfs getstripe` on a heavy input (`atmos_6h_pl7h_ua_1587-1587.nc`)
reveals Lustre Progressive File Layout (PFL):

```
[0, 1 GB):       stripe_count=1, stripe_size=1 MB
[1 GB, 4 GB):    stripe_count=4
[4 GB, EOF]:     stripe_count=16
```

For a 13 GB file: 1 GB on 1 OST, 3 GB on 4 OSTs, 9 GB on 16 OSTs.
Most of the file is already heavily striped. `lfs migrate -c 8` would
be a regression on the bulk of the file. **Dead — Lustre is already
parallelising disk reads via PFL.**

### G — XIOS-side input chunking: empirically unverified

The hypothesis is sound (fewer B-tree walks per concurrent reader →
less metadata serialisation under contention). But:

- Single-rule warm-cache 5×5 ensemble: source mean **2:45**,
  repacked (chunks `(120, 7, 421120)`) mean **3:00**. Repacked
  *slightly slower*.
- Multi-rule contention with repacked inputs has not been measured.
- Repacked file showed 12× heap blowup at the chunk size we tested
  (1.4 GB raw per chunk × dask in-flight = 112 GB MaxRSS). Even if
  the throughput benefit holds at scale, practical chunk size needs
  to be much smaller than (120, 7, 421120) — we don't know the right
  value without further benching.
- And it's owned by the FESOM/AWI-ESM3 model team, not pycmor —
  needs coordination on the model side. **Not actionable from pycmor.**

---

## Final closing summary

After Phase 1 (slab loop, input rechunking), Phase 2 Round 1
(load-step optimizations), Round 2 (Prefect collapse), and Round 3
audit, **every cheap optimization investigated turned out to be a
non-win or out-of-reach**:

| direction | status |
|---|---|
| slab loop | reverted (mode='a' silent truncation) |
| offline input rechunk | repack 12× heavier in heap, no warm-cache wall win |
| h5netcdf engine | save 2.8× slower |
| inline_array=True | 2× wall regression on 5840-chunk arrays |
| Prefect task collapse | 1-second wall delta |
| Round 3.D shared loading | ≤5.8 % I/O reduction; dead |
| Round 3.E cache prewarming | <1 s reward; dead |
| Round 3.F Lustre striping | already in effect via PFL |
| Round 3.G XIOS chunking | unverified, owned by FESOM team |

**The bottleneck is the work itself**: HDF5 chunk reads, blosc
decompression, xarray pipeline compute, recompression, write. Each
is well-optimised at the library level. No application-layer leverage
remains.

**Production stays at 2×4×64 (48/52 rules in 2:57).**

The only remaining path to meaningful wall improvement is **Round
3.G — XIOS-side input chunking** in the FESOM/AWI-ESM3 model
configuration. This is a coordination ask outside pycmor, not a
feature pycmor can ship.

---

## Round 4: contention sweep on mini-cap7

After closing the four direction-specific candidates above, ran a
controlled sweep over `(N_workers, mem_per_worker)` at fixed TPW=4
to characterise where the production default actually sits on the
throughput curve. Mini-cap7 = 7 heaviest cap7_atm rules
(ua_6hr_pl7h, va_6hr_pl7h, ta_6hr_pl7h, hus_6hr_pl7h, zg_6hr_pl7h,
uas_1hr, ts_1hr) on 3 separate `/work` data copies (lfs setstripe
-c 8) per ensemble member to avoid page-cache sharing.

Note: zg_6hr_pl7h's mini-cap7 rule omits the `scale_factor` for
geopotential→height conversion present in the production yaml, so
it always fails on a unit conversion error. This is intentional —
zg is testing the contention mechanism, not the unit pipeline. Max
viable rules = 6, max files ≈ 11 per run.

### Sweep grid

8 configs × 3 ensemble = 24 jobs. Walltime 1:30 per job.

| TAG | W | Mem/worker | total slots | total commit |
|---|---|---|---|---|
| 2x4x64GB | 2 | 64 GB | 8  | 128 GB (production default) |
| 2x4x32GB | 2 | 32 GB | 8  | 64 GB  |
| 3x4x32GB | 3 | 32 GB | 12 | 96 GB  |
| 3x4x48GB | 3 | 48 GB | 12 | 144 GB |
| 4x4x16GB | 4 | 16 GB | 16 | 64 GB  |
| 4x4x24GB | 4 | 24 GB | 16 | 96 GB  |
| 4x4x32GB | 4 | 32 GB | 16 | 128 GB |
| 4x4x40GB | 4 | 40 GB | 16 | 160 GB |

### Results

| config | walls (min) | mean | mean cgrp GB | files / 11 | deadlocks/3 | viable |
|---|---|---|---|---|---|---|
| 2x4x32GB | 30.9, 27.4, 28.6 | 29.0 | 49.3 | 11.0 | 0 | yes (same as default, less mem) |
| 2x4x64GB | 30.9, 27.4, 29.4 | **29.2** | 49.2 | 11.0 | 0 | reference |
| **3x4x48GB** | 27.9, 22.3, 24.3 | **24.8** | 49.4 | 11.0 | 0 | **yes — 15 % faster, comfortable mem** |
| **4x4x16GB** | 25.9, 22.3, 24.2 | **24.1** | 49.7 | 11.0 | 0 | **yes — 17 % faster, tight mem** |
| 4x4x40GB | 33.1, 28.2 | 30.7 | 49.6 | 11.0 | 1 | borderline |
| 3x4x32GB | 24.7, 26.4 | 25.5 | 34.0 | 7.3 | 1 | partial completion |
| 4x4x32GB | 22.3, 28.3 | 25.3 | 34.1 | 7.3 | 1 | partial completion |
| 4x4x24GB | 25.9 | 25.9 | 18.8 | 3.7 | 2 | deadlock prone |

### Findings

1. **Two configs improve over the production default**:
   - `4x4x16GB` (17 % wall reduction, smaller mem)
   - `3x4x48GB` (15 % wall reduction, comfortable mem)

2. **Counter-intuitive memory effect**: `4x4x16GB` (16 GB/worker —
   tightest in the sweep) ran cleanly with no deadlocks and the
   fastest mean wall. The parallel agent's earlier report flagged
   `4x4x32` and `4x4x48` as OOM-cascading on the full cap7_atm,
   so the mini-cap7 result may not generalise to sustained
   52-rule load. Tight-mem configs need cap7_atm validation
   before being declared production-ready.

3. **Probabilistic deadlocks at high concurrency**: at 12 (3W ×
   TPW=4) or 16 (4W × TPW=4) total slots vs 7 simultaneous parent
   tasks each requiring sub-slots for child step-tasks, the
   scheduler over-subscribes and some runs deadlock. `4x4x24GB`
   deadlocked 2/3 ensemble members. `3x4x32GB`, `4x4x32GB`,
   `4x4x40GB` each deadlocked 1/3.

4. **New universal failure surfaced**:
   `TypeError("Could not serialize object of type _HLGExprSequence")`
   in `save_dataset` task across every parallel-mode run (8-13
   occurrences per run). Prefect retries handle it for most rules
   so they eventually succeed, but real compute is being wasted on
   the retries. Should be tracked as a separate bug — not on the
   throughput-optimisation critical path.

### Recommendation

`3x4x48GB` is the conservative production-default candidate:
- 15 % wall reduction at cap7_atm scale (if it generalises).
- Comfortable mem headroom (no OOM cascade risk like 4x4x16/32).
- 0/3 deadlocks in the mini-cap7 ensemble.

Need cap7_atm at-scale validation (1 full run, ~3 h) to confirm
before changing the default. If it lands ≥48/52 in <2:30, ship
the new default.

## Files to add / modify

```
src/pycmor/core/gather_inputs.py     # axis 1: engine + rdcc_nbytes + inline_array
src/pycmor/core/config.py            # optional: knobs in pycmor block
src/pycmor/core/pipeline.py          # axis 2: Prefect task collapse
examples/cmip7_bench_hr_ua_6hr_h5nc.yaml      # axis 1, variant A
examples/cmip7_bench_hr_ua_6hr_inline.yaml    # axis 1, variant B
examples/cmip7_bench_hr_ua_6hr_h5nc_inline.yaml  # axis 1, variant AB
examples/run_bench_hr_ua_6hr_h5nc.sh
examples/run_bench_hr_ua_6hr_inline.sh
examples/run_bench_hr_ua_6hr_h5nc_inline.sh
OPTIMIZATION_PLAN.md                 # this file
```

## Sources

- [HDF5 chunk cache — HDF Group forum](https://forum.hdfgroup.org/t/why-increasing-rdcc-nbytes-and-rdcc-nslots-will-result-in-a-decrease-in-indexing-performance/9062)
- [Improve HDF5 performance using caching — HDF Group](https://www.hdfgroup.org/2022/10/17/improve-hdf5-performance-using-caching/)
- [h5py File Objects (rdcc_nbytes API)](https://docs.h5py.org/en/stable/high/file.html)
- [xarray reading and writing files](https://docs.xarray.dev/en/stable/user-guide/io.html)
- [xarray.open_dataset (inline_array)](https://docs.xarray.dev/en/stable/generated/xarray.open_dataset.html)
- [xarray Dask user guide](https://docs.xarray.dev/en/stable/user-guide/dask.html)
- [Cloud-Performant NetCDF4/HDF5 reading via Zarr lib (Pangeo)](https://medium.com/pangeo/cloud-performant-reading-of-netcdf4-hdf5-data-using-the-zarr-library-1a95c5c92314)
- [h5netcdf documentation](https://h5netcdf.org/index.html)
- [Parallel I/O Characterization and Optimization (arXiv)](https://arxiv.org/html/2501.00203v1)
- [Lustre concurrent I/O handling — HPC SRE](https://hpcadmin.com/2023/03/25/lustres-approach-to-handling-concurrent-read-and-write-operations-efficiently/)
- [Prefect data pipeline benchmark — Clanker Cloud (May 2026)](https://clankercloud.ai/blog/best-tools-containerized-kubernetes-data-pipelines-2026-benchmark)
- [pycmor (esm-tools) — GitHub](https://github.com/esm-tools/pycmor)
