# PLAN: SLURM-level shard isolation for pycmor HR campaigns

**Status**: round 4 — implemented and live. cli20 smoke test on `shared` revealed queue contention + worker undersizing (both anticipated by round-1 review §0 and round-2 review §3). Pivoted to `compute` per the plan's own fallback path.
**Date**: 2026-05-12
**Author**: Jan Streffing + Claude

## Background — why we're doing this

Across ~30 iterations (cli10 → cli19) we've chased successive driver-process
failure modes:

- cli10–11: NaN-stubs from driver OOM mid-write
- cli12: deterministic hang in `integrate_over_hemisphere` (fancy isel)
- cli13–15: cascade failures
- cli16: 87 GiB RSS, 15-rule cascade (driver pileup)
- cli17 core_atm: 0r/3h regression
- cli18b: 20/72 rules, OOM at 233 GiB / 256 GiB cgroup
- cli18 core_atm: deadlocked on `hur` save, 90 min log silence
- **cli19**: 61/72 rules, driver fragmented at 252 GiB / 512 GiB cgroup → 4 MiB allocation fails

Every fix pushed the wall further (0 → 61 rules) but never removed it. After
research:

1. `client.compute(sync=True)` accumulates refs in the driver — known dask
   issue (`dask/distributed` #2464, #5960, #2068, #8164).
2. glibc malloc fragments under repeated large numpy alloc/free — the
   "252 GiB MaxRSS but 4 MiB alloc fails" pattern is textbook glibc arena
   fragmentation. jemalloc helps a little; doesn't solve a 250-GiB-class
   retention problem.
3. dask scheduler-connection-lost under driver pressure cascades into
   permanent deadlocks (matches core_atm cli18's 90-min silence).

The structural problem is **a single Python process processing 70+ rules
sequentially**. CCLM2CMOR (the production COSMO-CLM CMORizer) doesn't do
that; it uses SLURM-level isolation per year / per variable directory.
PCMDI's CMOR docs assume per-variable file isolation.

This plan brings pycmor in line with that pattern.

## Goal

Replace "1 SLURM job runs all 72 rules of a tier in one Python process" with
"1 SLURM array runs N rules per process, M processes per tier, all
independent." Driver memory bounded by N (≈20) instead of N=72+. Failures
are localized to a single shard, not a whole tier.

---

## §0. Partition target

Critical sizing input. From `scontrol show partition` on Levante (verified
2026-05-12):

| Partition | Nodes | Cores/node | Memory/node | OverSubscribe | MaxMemPerCPU | MaxTime |
|---|---|---|---|---|---|---|
| `compute` | 2931 | 128 | 256 / 512 / 1024 GB | **EXCLUSIVE** | 940–3940 MB | 8 h |
| `shared` | 21 | 128 | 256 GB | NO (subnode OK) | **940 MB** | 7 d |
| `interactive` | 30 | 128 | 512 GB | NO | 1940 MB | 12 h |

Implications: requesting `--cpus-per-task=8 --mem=128G` on `compute` still
allocates a **full 128-core node** because it's `EXCLUSIVE`. The
"16× per-shard SLURM footprint shrink" the round-1 plan claimed is only
true at the resource-request level, not at the scheduler-allocation level.

On `shared`, `MaxMemPerCPU=940 MB` caps total per-job memory at
128 × 940 MB ≈ 120 GB. A 128 GB request is not satisfiable. Realistic max
≈ 100–120 GB; conservative target ≈ 60 GB at 64 cores.

### Smoke test on `shared` (cli20, 2026-05-12) → pivoted to `compute`

We submitted year-1587 sharded onto `shared` (34 array tasks, 60 GB /
64 cores / 1 h each). Two anticipated failure modes surfaced together:

- **Queue contention.** `shared` has only 21 nodes. At submit time it
  was at 90–100% allocated across the partition (other Levante users).
  33 of 34 array tasks queued behind unrelated jobs with reason `(None)`.
  Only 1 shard (cap7_atm) was dispatched in the first hour.
- **Worker undersizing.** With `N_WORKERS=2 × MEM_PER_WORKER=8 GB`,
  cap7_atm's 3D rules (zg, ta on the 421k-cell TCo319 grid) hit
  `Client.compute` MemoryErrors — "16 GB of input dependencies, worker
  limit 7.45 GiB" — and fell back to the synchronous-scheduler path.
  Fallback worked but slows the rule and is exactly the risky path
  Fix #3 was meant to avoid.

Both were anticipated:
- Round-1 review §0 noted `shared`'s small partition; round-2 review §3
  flagged that worker sizing needs measurement post-smoke-test.
- The plan named compute-with-full-node as the explicit fallback.

### Decision: use `compute`, one shard per node, full node memory

Trade `shared`'s utilization story for queue throughput and a clean
memory budget:

- **Partition**: `compute` (2931 nodes, EXCLUSIVE allocation). No queue
  contention at our submit scale.
- **Per-shard SLURM allocation**: `--cpus-per-task=128 --mem=0` →
  whole 256 GB node, all 128 cores. Since EXCLUSIVE allocates the whole
  node anyway, ask for everything explicitly. The previous shy
  `--cpus-per-task=8 --mem=128G` request let SLURM allocate the same
  whole node but capped what cgroups would expose to us.
- **Per-shard process budget**: `N_WORKERS=4 × MEM_PER_WORKER=32 GB` =
  128 GB for workers, ~30–40 GB driver, ~80 GB headroom on 256 GB. No
  more worker undersizing.

This is **§0 Option 3 with full-memory tuning** — "compute as-is, 94%
CPU waste" but with the memory headroom that prevents worker fallbacks.
The CPU waste is the price of failure isolation per shard; we accept it
because:

- **CPU isn't the bottleneck** — pycmor's per-shard active footprint is
  ~16 dask threads + driver. On any partition we'd be CPU-idle most of
  the time.
- **Memory is the bottleneck** — `compute` lets us own 256 GB cleanly
  without sub-node math.
- **Queue throughput is what production needs** — `compute` dispatches
  immediately; `shared` was queue-bound at smoke-test submit scale.

### Tradeoffs we accept

- **~190 K extra core-hours over the 2000-yr campaign** vs `shared`-fit
  (74K jobs × 128 cores × 0.75 h vs × 64 cores × 0.75 h). Roughly 2×
  the billing of the `shared` plan, but `shared` was infeasible in
  practice. Still well under status-quo retries.
- **No co-tenancy / shared-node coupling** — `compute` EXCLUSIVE
  isolates each shard at the hardware level. Strictly better failure
  characteristics than Option 2 bundling.
- **Year-overlap submission**: with `compute`'s 2931 nodes, 35-job
  per-year submissions never see queue pressure. The full-barrier
  policy stays as the safety default but year-overlap becomes feasible
  if we ever need higher throughput. **Decision: full-barrier for now;
  re-evaluate after first 10 years complete.**

---

## Sizing decisions (cli21, `compute` partition)

| knob | value | reasoning |
|---|---|---|
| partition | `compute` | EXCLUSIVE allocation, 2931 nodes, immediate dispatch |
| rules per shard (N) | ≤20 (16 typical after even-split) | cli19 ran clean to 61 rules at full mem; N=20 cap ≈ 3× safety |
| shards per tier | ⌈rules/N⌉, even-distributed | lrcs_seaice 64r → 4 shards of 16; core_atm 76r → 4 shards of 19 |
| jobs per year | ~35 | 17 tiers × ~2.1 shards avg (cli21 confirmed: 35 array tasks) |
| jobs over 2000-yr campaign | ~70 K | tractable as job arrays |
| `--cpus-per-task` | 128 | full node — EXCLUSIVE allocates it regardless |
| `--mem` | 0 (= all node memory) | full 256 GB — no reason to leave headroom for co-tenants |
| `N_WORKERS × MEM_PER_WORKER` | 4 × 32 GB = 128 GB | leaves ~30 GB for driver, ~80 GB free for transients |
| walltime per shard | 1:30:00 | covers slowest 3D atmos saves |

**Per-shard active footprint vs. allocation**: N_WORKERS=4 × TPW=4 = 16
dask threads + 1 driver ≈ 17 active cores on a 128-core node. Honest
utilization ~13%. The other 111 cores are EXCLUSIVE-reserved but idle.
We accept this because:

- CPU isn't the bottleneck — pycmor's per-rule work is largely I/O and
  serial-ish dask graphs of moderate parallelism. Adding cores wouldn't
  speed individual rules much.
- Memory headroom is what we need, and `compute` gives us 256 GB
  cleanly.
- Queue throughput dominates wall-clock for the campaign, and `compute`
  has 2931 nodes vs `shared`'s 21 — no contention.

### Core-hours comparison

| approach | core-h per shard | per year | 2000-year campaign |
|---|---:|---:|---:|
| status quo (1 job per tier, 256 GB, 128c, ~3 h) | ~384 | ~6,500 | ~13 M (when it worked; doesn't actually finish) |
| `shared` plan (hypothetical, 64c × 1 h × 35 shards) | ~64 | ~2,240 | ~4.5 M (infeasible — queue saturation) |
| **`compute` actual (128c × ~1 h × 35 shards)** | **~128** | **~4,500** | **~9 M** |

So `compute` shard isolation is ~30% cheaper than status-quo when status
quo works, and infinitely cheaper when status quo doesn't (which has
been the case). The `shared` plan would have been cheapest in pure
core-hour billing but was queue-infeasible at submit scale.

---

## Implementation, four pieces

### 1. Shard-splitter (Python, ~50 LoC)

`examples/shard_tier_yaml.py`:

- Input: tier yaml path, shard size N, output dir, **optional shuffle seed**
- Reads yaml, takes `rules:` list
- **Deterministic shuffle then chunk**:
  ```python
  rules = list(yaml_data["rules"])
  random.Random(seed).shuffle(rules)  # default seed=42
  shards = [rules[i::n_shards] for i in range(n_shards)]
  ```
- Writes `<tier>_shard_00.yaml`, `<tier>_shard_01.yaml`, ... — each is a
  copy of the source yaml with the `rules:` filtered to that shard's subset
- `pipelines`, `general`, `inherit` sections copied verbatim
- Returns list of shard yaml paths
- Also reusable as a fixup-shard generator: pass `rule_names=[…]` to
  produce a single yaml containing only the named rules

**Why shuffle-then-chunk, not naive round-robin**: tier yamls are typically
grouped by something (frequency, dimensionality, realm). Round-robin
alone is only cost-balanced when input order is uncorrelated with cost,
which we cannot rely on. A deterministic shuffle with a known seed:
- defeats any alphabetical or grouped-by-frequency clustering
- stays reproducible across re-runs
- lets us smoke-test with `--shuffle-seed N` to surface heavy-shard
  outliers

**Verification**: after the first real run, check that the slowest shard
isn't ≥1.5× the fastest. If it is, switch to a real cost model:
`(frequency, n_dims) → expected MB` lookup, then bin-pack.

### 2. Shard runner script

`examples/run_hr_shard.sh`, adapted from `run_hr_yaml_cli.sh`:

```bash
#SBATCH --partition=shared
#SBATCH --cpus-per-task=64
#SBATCH --mem=60G
#SBATCH --time=01:00:00
```

- Inputs: shard yaml, run-root, year, output-subdir, shard-index
- `N_WORKERS=2`, `TPW=4`, `MEM_PER_WORKER=8GB` (driver ~28 GB, workers
  ~16 GB, headroom ~16 GB on 60 GB cgroup)
- Output dir: `$OUTROOT/$OUTSUB/` (same as today — all shards' outputs land
  alongside, no path collision because rule names differ)
- Log: `pycmor_hr_shard_<tier>_<shard>_<jobid>.log`
- Keeps Fix #3 (`client.compute(sync=True)` for worker compute)
- Keeps `PYCMOR_PREFECT_COLLAPSE=1` (don't change two things at once)
- **Requires** `--skip-existing` (see step 4)

### 3. Submitter

`examples/submit_hr_year_shards.sh`, adapted from `submit_hr_year.sh`:

```bash
for yaml in "$YAMLS_DIR"/*.yaml; do
  tier=$(basename "$yaml" .yaml)
  python3 shard_tier_yaml.py "$yaml" "$SHARD_SIZE" "$WORKDIR/shards/$tier/"
  num_shards=$(ls "$WORKDIR/shards/$tier/"*.yaml | wc -l)

  # No %K — per-array unlimited. shared partition capacity at our sizing
  # (64 cores × 940 MB per shard) is ~42 concurrent slots, comfortable
  # for one year's 37 shards.
  sbatch --array=1-${num_shards} \
         --partition=shared \
         -J "pycmor-hr-${tier}-y${YEAR}" \
         "$HERE/run_hr_shard.sh" "$tier" "$YEAR" ...
done
```

- One SLURM array per tier; per-array concurrency unlimited; cluster-wide
  throttling comes from `shared` partition capacity (~42 concurrent slots
  at 64 cores/shard)
- Single job ID per tier → easy to track / scancel
- Logs per shard land in `pycmor_hr_shard_<tier>_<arrayidx>_<jobid>.log`

### 3a. Pre-flight (required, runs once per submitter invocation)

Before sbatching any shards, the submitter runs a **sequential** pre-warm
pass:

1. For each unique mesh referenced by any tier yaml: load it via the
   same code path pycmor uses, ensuring `MESH_cache/` is populated.
   Sequential not parallel — 17 concurrent mesh loads on the submission
   node would spike memory.
2. For each tier: parse the CMIP7 CV + DReq for that tier and cache the
   parsed JSON to `MESH_cache/dreq_<tier>.json` (or similar). Same
   contention pattern as the mesh — first shard populates, others block
   on the file lock. Pre-warming makes the actual shard run lockless.

Total pre-flight cost: ~5 min upfront for one year (17 meshes + 17 DReq
parses). Negligible vs. per-shard cost without pre-warm (~17 × 5s × 4
shards/tier = 340s of lock contention per year if skipped).

### 4. Validator / reaper + `--skip-existing`

Two pieces, both required:

**4a. `--skip-existing` flag on `pycmor process`** (small change in
`src/pycmor/core/cmorizer.py`):
- Before running a rule's pipeline, check if its expected output file
  exists and is readable as a valid NetCDF.
- If yes, log "skipping <rule>: output exists" and move on.
- This is the only change inside `src/pycmor/` and is small (~30 LoC).

**Why this is required, not optional**: the validator/reaper emits a
fixup yaml containing the failed rules. If `pycmor process` re-runs every
rule in that yaml regardless of existing outputs, then:
- Successful rules get re-run (waste)
- Worse: a flaky retry might overwrite a good output with bad data

The contract of the fixup pass is "re-run only what's missing"; that
contract isn't deliverable without idempotency.

**4b. `examples/validate_shards.py`** (~30 LoC):
- For each tier, walk the expected output directory
- Check that every rule (across all shards) produced its output file
- Report missing outputs as `tier/rule` pairs
- Emit a fixup yaml via the splitter from step 1
  (`shard_tier_yaml.py --rule-names rule_a,rule_b,...`)
- Submit ONE more shard job to clean up

Together: failure of one shard → 20 rules to re-run worst case, 1 rule in
the typical case (where only one rule failed and `--skip-existing`
short-circuits the rest).

---

## How a single-year campaign plays out (1587, first run)

1. `submit_hr_year_shards.sh Test_06 1587` (~30 sec)
   - Pre-flight: sequentially warms mesh + DReq caches (~5 min)
   - Splits 17 tier yamls → ~37 shard yamls
   - sbatches 17 arrays (37 array tasks total) on `shared`
2. Shards run on `shared`:
   - Typical 60 GB / 64-core / 1-hour jobs
   - Concurrency throttled by partition capacity (~336 concurrent slots)
   - Expect 30–60 min wall-clock per shard
3. Wall-clock for the year: dominated by the slowest single shard
   (~30–60 min), not by 17 sequential tier jobs of 2–3 hours each.
   **Probably 4–6× faster end-to-end** than status quo.
4. Per-shard logs land in `pycmor_hr_shard_*.log`. Each is small (one
   shard ≈ 20 rules ≈ 2k log lines).
5. `validate_shards.py Test_06_y1587` → list of missing outputs. Submit
   fixup if any.

---

## Production-scale (2000 years)

### Year-by-year submission with full barrier

**Don't** submit all 74K jobs up front — Levante's accounting DB will
push back, and tracking 74K outstanding jobs is operationally fragile.

**Policy: full barrier between years.** Year N+1 only starts after year N
has 100% completed (or been resolved via fixup-shards) and been validated.

```python
# year_loop.py (pseudocode)
for year in range(start, end + 1):
    submit_year(year)              # ~37 shards, fits within shared's 42 slots
    wait_for_all_shards(year)      # block until SLURM array completes
    validate_year(year)            # runs validate_shards.py
    if missing_outputs:
        submit_fixup_shard(missing_outputs)
        wait_for_all_shards(year)
        validate_year(year)        # second pass
    # year N is now done; proceed to N+1
```

Why full barrier, not overlap: at 64 cores × 60 GB per shard the `shared`
partition holds only 42 concurrent slots. One year's 37 shards leaves
just 5 slots free. A few stuck shards in year N would crowd year N+1 into
head-of-line blocking on the stalled tail. Full barrier loses some
throughput at year boundaries (~5 min idle per year for the last shards
to drain) but is operationally robust to stalls.

The "≥80% overlap" variant is documented as a follow-up optimization to
try after the baseline is stable. Don't implement it on day 1.

### Campaign math

- Year loop is the outermost layer; each year is independent
- ~37 array jobs per year
- Total: 74K jobs over the campaign, ~99% succeed in one shot
- With ~37 concurrent shards on `shared` (within 42-slot capacity),
  one year completes in ~10 min wall-clock once steady-state →
  **2000 years ≈ 14 days** wall time
- Core-hours: ~74K × 64 cores × 0.75 h ≈ **3.5 M core-hours**

---

## Implementation timeline

| step | effort | output |
|---|---|---|
| 1. `shard_tier_yaml.py` + unit test | 0.5 day | sharded yamls work, shuffle-then-chunk balanced |
| 2. `run_hr_shard.sh` | 0.5 day | one shard runs cleanly for lrcs_seaice on `shared` |
| 3. `submit_hr_year_shards.sh` + pre-flight | 0.5 day | full year (17 tiers) submits as array; mesh+DReq pre-warmed |
| 4a. `--skip-existing` in `cmorizer.py` | 0.5 day | idempotent re-runs |
| 4b. `validate_shards.py` + fixup | 0.5 day | re-run only missing rules |
| 5. End-to-end on year 1587 + comparison to status quo | 1 day | full validation; partition decision confirmed |
| 6. Year-loop submitter with rate-limit | 0.5 day | ready for production |
| 7. Production rollout for 2000-yr campaign | iterative | |

**~4 days of work** to a working bounded-batch architecture, plus
1–2 days of validation. (+0.5 day vs round 1, accounting for `--skip-existing`
and pre-flight being required not optional.)

---

## What this doesn't change

- pycmor's internal pipeline logic — only one small additive change
  (`--skip-existing` in `cmorizer.py`)
- yaml format — shard yamls are just filtered copies
- Fix #3 (`client.compute(sync=True)` on workers) stays in
- All the existing custom_steps.py work (hfbasin, sltbasin, sisnmass,
  integrate_over_hemisphere) stays in
- The sanity_check pipeline stays unchanged

## What it does change

- The shell-level submission flow (new submitter, new runner, pre-flight)
- The target partition (`shared` not `compute`)
- The mental model: "1 job = 1 tier" → "1 job = 1 shard"
- `cmorizer.py` gains `--skip-existing` (only additive change inside
  `src/pycmor/`)

---

## Risks and mitigations

### MESH_cache contention

37 concurrent shards all want to load the FESOM mesh. The cache lookup
is fast but the first one to encounter a fresh tier might re-build cache;
the others would block on the file lock.

**Mitigation**: §3a pre-flight, sequential. Solved upfront.

### CMIP7 CV / DReq parse on every shard

~5 s per shard. Multiplied by 74K jobs = ~100 core-hours, and the
concurrent-first-parse contention pattern is exactly the same as the mesh.

**Mitigation**: §3a pre-flight, sequential, same as mesh. Cached parsed
JSON read instantly by all shards.

### Smoke-test risk: N=20 might not fit in 60 GB

If lrcs_seaice at N=20 needs >60 GB driver RSS, `shared` is too small.

**Mitigation**: smoke-test FIRST (step 5 in timeline), measure peak RSS
across all 4 shards. If it fits, ship. If not, fall back to `compute`
with bundling (Option 2) — see below.

### Lustre I/O contention

37 concurrent shards writing to the same tier output directory may
contend on Lustre metadata.

**Mitigation**: keep the `lfs setstripe -c 8` from `run_hr_yaml_cli.sh`.
Worth measuring after the first real run; not expected to be a problem
at 37 concurrent at this scale, but worth a `--array=1-N%K` concurrency
cap if it shows up.

### Submitting 74K jobs over 2000 years

SLURM accounting DB has limits. Submitting all 74K upfront would
overwhelm it.

**Mitigation**: year-by-year submission with full barrier
(§"Production-scale"). Only ~37 jobs in flight at any time.

---

## Why we didn't bundle (Option 2 from earlier rounds)

Round-1 review proposed an alternative `compute`-with-bundling path:
K=4 shards as parallel subprocesses on a single 128-core node, each
shard getting ~32 cores and ~60 GB of the 256 GB available. This was
supposed to recover utilization vs Option 3's "single shard per
exclusive node, 94% CPU idle."

We didn't take it because:

- The CPU isn't the bottleneck and never was. Recovering 94% → 75%
  utilization is mostly accounting cosmetics.
- Bundling re-introduces shared-node failure coupling: one shard's
  worker OOM can take down the other 3 on the same node. We just
  spent ~30 cli iterations escaping exactly this coupling.
- Bundling adds orchestration complexity (a wrapper that spawns 4
  subprocesses and waits for all to finish; partial-failure recovery
  per-subprocess). Not worth it when `compute` has 2931 nodes free.
- Implementation time penalty was non-trivial vs zero for "just go to
  compute with --mem=0".

If `compute` accounting ever becomes a binding constraint, the bundling
fallback is still on the table — it doesn't require pycmor changes,
only a new wrapper script. For now: not needed.

---

## Open questions (smaller follow-ups)

- Per-rule fine-grained sharding for genuinely expensive single rules
  (e.g. `ta` monthly 3D × 19 levels): could shard further down to
  `--array=N` where N rules are split into ≥2 jobs each. Worth measuring
  after baseline lands; not a v1 feature.
- `MaxArraySize` on Levante — verify it's ≥4 (shouldn't be an issue but
  worth `scontrol show config | grep MaxArraySize` before relying on it).
- **Year-overlap submission**: with `compute`'s 2931 nodes, queue
  pressure at 35 jobs/year is nil. Full-barrier stays as the safety
  default for cli21; re-evaluate after first 10 years complete. If
  no shards stall in 10 consecutive years, switch to overlap.
- **Worker sizing follow-up**: cli21 uses `N_WORKERS=4 × 32 GB`. If
  driver RSS in shards stays under ~30 GB and workers don't approach
  their limit, the per-shard footprint could be trimmed. But the
  marginal saving is meaningless on `compute` EXCLUSIVE.

## Current state (cli21, 2026-05-12)

cli21 submitted year-1587 fully sharded on `compute`:

- 17 SLURM arrays (one per tier), 35 total array tasks
- Per-shard: 128 cores / 256 GB / 1:30:00 walltime, `--mem=0`
- `N_WORKERS=4 × MEM_PER_WORKER=32 GB`, driver bounded at N=16-19 rules
- `--skip-existing` enabled (no-op on fresh dir)
- Output: `/scratch/a/a270092/pycmor_hr/cli21_compute_y1587_shards/cmorized/`
- Job IDs: 24826351-24826367

**Measurements to record once shards finish:**

1. Peak driver RSS per shard (sacct MaxRSS) — should stay well under
   100 GB at N≤20
2. Per-shard wall time
3. Per-shard rule completion count (expecting 16/16 → 19/19 cleanly)
4. Any `SaveTimeout` or `MemoryError` events (Fix #3 should make
   client.compute fallback rare; if it triggers, sizing needs revisit)
5. Cross-shard MESH_cache contention timing (first shard pays mesh
   load; subsequent shards should read from cache instantly)

If cli21 cleanly completes 100% of rules across all shards, the
architecture is shipped. Next step is the year-loop submitter for
multi-year campaigns (steps 6-7 in §"Implementation timeline").
