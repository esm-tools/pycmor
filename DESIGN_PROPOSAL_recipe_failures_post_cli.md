# DESIGN PROPOSAL — clearing the residual recipe failures after the CLI migration

**Status:** draft, revision 3 (round-7 review integrated, ship-ready)
**Author:** [agent]
**Date:** 2026-05-07
**Branch:** feat/cmip7-awiesm3-veg-hr
**Run reference:** Test_03 / y1587 / cli3+cli4 dispatch
  (`/scratch/a/a270092/pycmor_hr/Test_03_postfix_cli_y1587_v3/cmorized`)

## Revision history

- **r3 (2026-05-07)**: integrated round-7 review nits
  ([REVIEW_recipe_failures_post_cli_round7.md](REVIEW_recipe_failures_post_cli_round7.md)).
  Round-7 verdict: "plan is ship-ready". Three formatting consistency fixes:
  - §1 exec summary updated from "~15 min" to "~30 min" so it lock-steps
    with §4's realistic effort estimate.
  - Step 5b dep column reworded — instrumentation lands in step (2),
    so 5b's trigger is "next-run log shows aux index", not a separate
    confirmation step.
  - Step 4 effort separated into "5 min implementation, gated on user
    input" so the wall-clock wait isn't conflated with the work-time.
- **r2 (2026-05-07)**: integrated round-6 review feedback
  ([REVIEW_recipe_failures_post_cli_round6.md](REVIEW_recipe_failures_post_cli_round6.md)).
  Key changes:
  - F1 patches BOTH `rule.get("year")` callers (lines 885 + 917) via a
    shared `_resolve_year(rule)` helper — was originally only patching 917.
  - F4 hypothesis explicitly marked PROVISIONAL; instrumentation in
    `mask_where_no_seaice` precedes any structural change. The localized
    drop in `regrid_oifs_to_fesom` is a first-pass hot-fix; the
    structurally-correct home is `pycmor.core.gather_inputs.load_mfdataset`,
    promoted only after instrumentation confirms.
  - F6 changed from 4×48 GB (= 192 GB, exactly at the 75% budget ceiling
    on a 256 GB cgroup) to 3×48 GB (= 144 GB, headroom retained).
  - F3 comment-out requires an inline yaml block referencing this
    proposal §3.3 + the CMIP-target-unit conflict, so future readers
    don't uncomment thinking the rules were broken.
  - F5 contingency added: if `mask_where_no_seaice` print doesn't show
    7 timestamps, escalate to instrumentation at start of `timeavg`,
    then if still unreproducible, single-rule pdb. Standalone-repro
    provenance noted.
  - §3.2 F2 carries an owner table (per-input).
  - §5.4 walltime margin no longer "fine"; cap7_atm at 88% is a real
    margin pressure that F4's clearing of fast-fail rules will tighten.
- **r1**: initial draft (pre-review).

## §1. Executive summary

After the CLI-override migration (commits `8046000`, `55bb37e`, `961c492`)
and a series of recipe fixes (regrid lazy-isel, vertical_integrate units
via pint normalizer, hfbasin/sltbasin transpose, `_resample_to_match`
helper, `_load_secondary_mf` bounds-skip, `skip_input_year_filter` on
GHG rules), the 17-tier Test_03 cmorize lands at **540 ok / 18 fail**.

13/17 tiers fail-free. The 18 residual failures cluster into 6
fingerprints, of which **3 are pycmor-side and clearable with ~30
minutes of careful work** (revised from initial ~15 min estimate after
round-6 review pushed back on under-counted hardening + interpretation
overhead), **2 wait on user decision or external action**, and **1
needs targeted instrumentation**. After the actionable fixes land,
expected residual is **7 fails (4 model-team, 2 scope, 1 unreproduced)**.

## §2. Failure inventory

Source: cli3 (jobs 24743461–24743477) plus cli4 resubmits for the 3
tiers that hit the migration regressions (jobs 24744094–24744096).
Latest log per tier was used.

| tier | ok | fail | SLURM state | elapsed | residual fingerprint |
|---|---:|---:|---|---:|---|
| cap7_aerosol | 1 | 4 | FAILED | 5m | F1 (×4) |
| cap7_atm | 52 | 0 | COMPLETED | 2h38m | — |
| cap7_land | 119 | 1 | FAILED | 1h01m | F6 |
| cap7_ocean | 7 | 0 | COMPLETED | — | — |
| cap7_seaice | 9 | 0 | COMPLETED | — | — |
| core_atm | 77 | 0 | COMPLETED | 2h35m | — |
| core_land | 11 | 0 | COMPLETED | — | — |
| core_ocean | 28 | 0 | COMPLETED | — | — |
| core_seaice | 9 | 0 | COMPLETED | — | — |
| extra_atm | 20 | 0 | COMPLETED | — | — (--mem=512G) |
| extra_land | 13 | 0 | COMPLETED | — | — |
| lrcs_land | 6 | 0 | COMPLETED | — | — |
| lrcs_ocean | 54 | 3 | FAILED | 1h06m | F2 (×1), F3 (×2) |
| lrcs_seaice | 54 | 10 | FAILED | 16m | F2 (×3), F4 (×6), F5 (×1) |
| veg_atm | 20 | 0 | COMPLETED | — | — |
| veg_land | 59 | 0 | COMPLETED | — | — |
| veg_seaice | 1 | 0 | COMPLETED | — | — |
| **total** | **540** | **18** | | | |

Note on SLURM state: a `FAILED` exit-1 means "pycmor process returned
non-zero because at least one rule failed", **not** "the worker crashed
or timed out". cap7_atm and core_atm both finished cleanly within
walltime — every rule in those yamls was dispatched and ran.

## §3. Failure-class catalog

### §3.1 F1 — `broadcast_forcing_year_to_monthly` missing `year` attr (×4)

**Tier/rules:** cap7_aerosol — `cfc11_mon`, `cfc12_mon`, `ch4_mon`,
`n2o_mon`.

**Pipeline:** `ghg_scalar_pipeline`.

**Log:**
```
ERROR: Pipeline 'ghg_scalar_pipeline' FAILED for rule 'cfc11_mon' after 2.0s:
  ValueError: broadcast_forcing_year_to_monthly requires both `year`
  (model run year) and `forcing_year` (year to read from forcing file)
```

**Stack:**
```
File "examples/custom_steps.py", line 917, in broadcast_forcing_year_to_monthly
    year = rule.get("year") if hasattr(rule, "get") else getattr(rule, "year", None)
...
ValueError: ...
```

**Root cause.** `broadcast_forcing_year_to_monthly` reads `rule.get("year")`.
The CLI flow (commit `8046000`) writes `year_start` and `year_end` to
each rule, but not `year`. The legacy `repoint_hr_year.py` set
`inherit.year: <year>`; that step is no longer in the flow.
`forcing_year: 1850` is in the inherit block; only `year` is missing.

`select_year` (custom_steps.py:885) reads the same attribute via
`rule.get("year")` then falls back to `getattr(rule, "year_start", None)`
— a pattern that mostly works but uses a different attribute-access
style and is easy to miss in future refactors.

**Class:** RECIPE / cli-flow-gap.

**Fix (revised per round-6 review).** Both callers now resolve year via
a single shared helper; future drift is impossible.

```python
# examples/custom_steps.py — new helper near _resample_to_match:
def _resolve_year(rule):
    """Return the cmorize year as int, or None if unresolvable.

    Preference order: rule.year (legacy / explicit) → rule.year_start
    when year_start == year_end (CLI single-year case). Multi-year
    chunked dispatch (year_start != year_end) returns None — callers
    must handle that case explicitly (via per-chunk-year iteration).
    """
    if not hasattr(rule, "get"):
        return None
    y = rule.get("year")
    if y is not None:
        return int(y)
    ys, ye = rule.get("year_start"), rule.get("year_end")
    if ys is not None and ys == ye:
        return int(ys)
    return None
```

Both `select_year` (line 885) and `broadcast_forcing_year_to_monthly`
(line 917) call `_resolve_year(rule)` instead of inline reads. Single
source of truth.

**Multi-year chunked cmorize** (1700-year run mentioned in
`DESIGN_PROPOSAL_secondary_input_globs.md`) is a separate pattern — it
wants *per-chunk-year* output, which the loop in
`broadcast_forcing_year_to_monthly` already handles via `time_name`-based
slicing. The helper deliberately returns None when `year_start != year_end`
so multi-year callers fall through to the chunk dispatcher's mechanism
unchanged.

**Effort:** 5 min.

**Clears:** 4 fails.

---

### §3.2 F2 — Missing model-side input streams (×4)

**Tier/rules:**
- lrcs_ocean — `vsfcorr` (1)
- lrcs_seaice — `sicompstren`, `sifllattop`, `siflsenstop` (3)

**Log:**
```
ERROR: Pipeline 'FrozenPipeline' FAILED for rule 'vsfcorr' after 0.1s:
  OSError: no files to open
```

**Root cause per rule:**

| rule | input expected | model-side requirement | owner | status |
|---|---|---|---|---|
| vsfcorr | `relaxsalt.fesom.*.nc` | enable `relaxsalt` in FESOM `namelist.io` io_list | next FESOM rerun (whoever schedules Test_NN) | namelist.io + file_def_fesom.xml.j2 enabled this session in `~/esm_tools/namelists/fesom2/CMIP7_HR/` + `~/esm_tools/namelists/fesom2/xios_xml_cmip7/`; pending model rerun. Ships ~0 in coupled HR — that zero is a positive demonstration that no SSS-restoring correction is applied (per AWI-CM3 coupled config). |
| sicompstren | `strength_ice.fesom.*.nc` | one-line patch in `ice_maEVP.F90` to populate `ice%work%ice_strength` alongside the local `pressure_fac` variable (mEVP whichEVP=1 doesn't write it; only whichEVP=0 in `ice_EVP.F90:518-522` does) | separate AI tasked | open |
| sifllattop | `atmos_mon_land_slhf_*.nc` | add `slhf` to the `atmos_mon_land` XIOS group in `~/esm_tools/namelists/oifs/48r1/xios/cmip7/` | user (file_def owner) | open |
| siflsenstop | `atmos_mon_land_sshf_*.nc` | add `sshf` to the same group | user | open |

**Class:** MODEL.

**Pycmor-side action:** none. The rules are correct; they'll succeed on
the next FESOM/OIFS rerun with the enabled diagnostics. Recipes already
carry the right yaml for when the inputs land (verified during
`xios_xml_cmip7` enable session).

**Tracking.** Each row above has an explicit owner. "Wait" is not an
action — these are gated on external work and should be tracked
independently of pycmor's CI/CD.

**Effort (model-side):** unrelated to this proposal.

---

### §3.3 F3 — `hfx_int_day` / `hfy_int_day` units mismatch with CMIP target (×2)

**Tier/rules:** lrcs_ocean — `hfx_int_day`, `hfy_int_day`.

**Pipeline:** `scale_and_integrate_pipeline`.

**Log:**
```
ERROR: Pipeline 'scale_and_integrate_pipeline' FAILED for rule
  'hfx_int_day' after 787.7s:
  ValueError: Cannot convert variables:
    incompatible units for variable 'utemp': Cannot convert from
    'watt / meter' ([mass] * [length] / [time] ** 3) to
    'watt' ([mass] * [length] ** 2 / [time] ** 3)
```

**Root cause.** The G fix landed correctly: `vertical_integrate` now
updates `attrs["units"]` from `"W m-2"` to `"W m-1"` (= `W / m`) after
multiplying by thickness `m` and summing. The remaining mismatch is
**recipe-scope**: the CMIP target `hfx`
(`compound_name: ocean.hfx.tavg-u-hxy-sea.day.GLB`) expects units `W`
(full meridional / zonal heat transport):
```
hfx [W] = ∫_z ∫_x  (ρ_w · c_p · u · T)  dz dx
```
A vertically-integrated column heat flux is `W/m` (per unit
along-transect length) — fundamentally not the same physical quantity.
Producing CMIP `hfx` requires a horizontal integration step that the
pipeline does not have.

**Class:** RECIPE / SCOPE.

**Decision options:**

1. **Drop the rules** (recommended). `hfx_int_day` / `hfy_int_day` are
   probably not CMIP-required at daily frequency — daily depth-integrated
   heat transport per unit width isn't a standard CMIP variable, and the
   monthly counterparts in cap7_ocean (`hfx`, `hfy`) likely cover the
   data-request need. Comment out the 2 rules in
   `awi-esm3-veg-hr-variables/lrcs_ocean/cmip7_awiesm3-veg-hr_lrcs_ocean.yaml`.
2. **Add a horizontal-integration step.** Implement
   `compute_hfx_horizontal_integral` that aggregates the W/m result
   along latitude transects → W. Requires lat-binning logic (similar to
   `_basin_lat_crossing_sum`) plus careful sign convention review.
   Effort: 2-4 hours plus math review.
3. **Change `compound_name`** to a CMIP variable whose target unit is
   `W m-1`. Would need a CMIP6 atlas lookup; unclear whether such a
   variant exists.

**Recommendation:** option (1). User decision required.

**Yaml comment requirement (per round-6 review).** If option (1) is
chosen, the comment-out MUST carry an inline explanation referencing
this proposal and the unit conflict, otherwise the rules get
groundhog-day-uncommented in 6 months by someone "fixing recipe
failures." Required block:

```yaml
# DEACTIVATED 2026-05-07 — vertical_integrate produces W/m (depth-integrated
# heat flux per unit along-transect length); CMIP hfx/hfy require W (full
# along-transect integral), which the current scale_and_integrate_pipeline
# does not produce. Reinstate by adding compute_hfx_horizontal_integral;
# see DESIGN_PROPOSAL_recipe_failures_post_cli.md §3.3 for full analysis.
#
# - name: hfx_int_day
#   ...
```

**Effort:** 5 min if (1); 2-4 hours if (2); unknown if (3).

**Clears:** 2 fails (if option 1 or 2).

---

### §3.4 F4 — `rsds`/`rsus`/`rlds`/`rlus_seaice` family duplicate-time-index (×6)

**Tier/rules:** lrcs_seaice — `rlds_seaice`, `rlus_seaice`, `rsds_seaice`,
`rsds_seaice_day`, `rsus_seaice`, `rsus_seaice_day`.

**Pipeline:** `regrid_atm_to_fesom_seaice_mask_pipeline`.

**Log:**
```
ERROR: Pipeline 'regrid_atm_to_fesom_seaice_mask_pipeline' FAILED for rule
  'rsds_seaice' after 242.9s:
  ValueError: cannot reindex or align along dimension 'time' because
  the (pandas) index has duplicate values
```

**Stack:**
```
File "examples/custom_steps.py", line 3458, in mask_where_no_seaice
    result = data.where(mask)
File "xarray/structure/alignment.py", line 1031, in deep_align
File "xarray/structure/alignment.py", line 967, in align
File "xarray/structure/alignment.py", line 667, in align
```

**Root cause hypothesis (PROVISIONAL — see review caveat below).** This is
a **NEW REGRESSION** from my D fix (regrid lazy-isel, replacing
`data.values[..., inds]` with `data.isel({source_dim: indexer})` to avoid
110 GB allocation). The isel preserves all coordinates inherited from the
input, including auxiliary OIFS time coords:

- `time_centered` (XIOS hourly-mean stamps, e.g. HH:30)
- `time_instant` (XIOS instant stamps, e.g. HH:00)
- `time_centered_bounds` / `time_instant_bounds` (T × 2 axis_nbounds)
- `time_counter_bounds` (T × 2 axis_nbounds)

After `load_mfdataset` renames `time_counter` → `time` (because the
6 yamls now declare `time_dimname: time_counter`), the auxiliary coords
still carry dim `time`. They have 8760 values (matching the renamed
`time` dim), but those values differ from `time`'s values (HH:30 vs
HH:00). When `mask_where_no_seaice` calls `data.where(mask)`, xarray's
`deep_align` walks all coords sharing dim `time` and tries to reindex.

The pre-D-fix path materialized via `.values` and built a fresh
DataArray with only the time coord (line 3327-3332 of the old code:
`xr.DataArray(out, dims=[...], coords={time_dim: data[time_dim]}, ...)`),
which dropped the auxiliary coords as a side effect. My lazy isel
preserves them — better lazily, worse for downstream alignment.

**Class:** RECIPE / NEW REGRESSION (introduced this session).

**Review caveat (round-6).** The error message specifically blames
`time`'s **pandas index** for having duplicate values. xarray's `align`
walks dim *indexes*, not arbitrary auxiliary coords — `time_centered`
and `time_instant` are coords-on-dim-`time` but aren't promoted to
indexes by default, so they wouldn't trigger the duplicate-pandas-index
error directly. For the aux-coord hypothesis to hold, *something* in
the rename path must be promoting them to indexes; otherwise the real
duplicate source is `data.time`'s index itself (load_mfdataset concat
edge case, or the lazy isel preserving a different pandas index than
the materialised path produced).

The defensive drop is harmless either way, but is **provisional**
until instrumentation confirms.

**Fix (two-stage, per round-6 review).**

Stage A — instrument-first to confirm hypothesis. Add to
`mask_where_no_seaice` immediately before `data.where(mask)`:

```python
logger.info(
    f"mask_where_no_seaice: data.indexes={dict(data.indexes)} "
    f"data.time.is_unique={data['time'].to_index().is_unique} "
    f"a_ice.time.is_unique={a_ice['time'].to_index().is_unique} "
    f"data.coords={list(data.coords)}"
)
```

Stage B — first-pass localized hot-fix in `regrid_oifs_to_fesom` (drop
OIFS auxiliary time coords before returning):

```python
# examples/custom_steps.py, regrid_oifs_to_fesom, before `return result`:
for aux in ("time_centered", "time_instant",
            "time_centered_bounds", "time_instant_bounds",
            "time_counter_bounds", "time_bounds"):
    if aux in result.coords:
        result = result.drop_vars(aux, errors="ignore")
```

After Stage A's instrumentation log lands, decide:
- **If `data.indexes` shows only `time` and it's unique** → aux-coord
  hypothesis is wrong, drop is harmless but the real bug is elsewhere
  (likely load_mfdataset). Open a follow-up to localize.
- **If `data.indexes` shows `time` AND another aux index** → hypothesis
  confirmed; drop in regrid is correct. Promote the drop to
  `pycmor.core.gather_inputs.load_mfdataset` so every consumer of
  `load_mfdataset` benefits uniformly (the secondary-input path
  `_load_secondary_mf` already does this drop at custom_steps.py:2145;
  load_mfdataset is the one missing it).
- **If `data.time.is_unique == False`** → load_mfdataset is producing a
  duplicate-time index on its own. Investigate concat-merge path (file
  was `atmos_1h_sfc_rsds_1587-1587.nc` — single file, so duplicates
  shouldn't arise from concat).

**Layer note.** The drop in `regrid_oifs_to_fesom` is a localized
hot-fix; it works for the 6 affected lrcs_seaice rules but doesn't help
any other downstream consumer of `load_mfdataset` that later hits
`align`. The structurally-correct home for the drop is
`pycmor.core.gather_inputs.load_mfdataset`. Promote after instrumentation
confirms.

**Effort (revised per review):** 5 min to add instrumentation and the
hot-fix together; +15 min to interpret one rule's instrumentation log
after run; +15 min to promote to load_mfdataset if confirmed. Total
realistic: ~35 min if hypothesis right, ~45 min if wrong.

**Clears:** 6 fails (if hypothesis correct or load_mfdataset is the
real source and gets fixed).

---

### §3.5 F5 — `sbl_seaice` 12-vs-7 cadence (×1)

**Tier/rule:** lrcs_seaice — `sbl_seaice`.

**Pipeline:** `regrid_atm_to_fesom_seaice_mask_pipeline`.

**Log:**
```
ERROR: Pipeline 'regrid_atm_to_fesom_seaice_mask_pipeline' FAILED for rule
  'sbl_seaice' after 243.6s:
  CoordinateValidationError: conflicting sizes for dimension 'time':
  length 12 on the data but length 7 on coordinate 'time'
```

**Stack:**
```
File "src/pycmor/std_lib/timeaverage.py", line 388, in timeavg
    ds["time"] = timestamps
```

**Root cause.** Persistent across 4+ runs (cli3, cli4, prior production,
prior gate-A). Stand-alone reproduction outside the pycmor pipeline
gives 12 resample groups for the input file
`atmos_mon_land_sbl_1587-1587.nc`, NOT 7. The 7 must enter via
interaction inside the pycmor step chain (likely between
`mask_where_no_seaice` and `timeavg`), specific to cftime-on-FESOM-monthly
×  atmos_mon time bounds.

**Standalone-repro provenance.** Verified 2026-05-07, env
`pycmor_py312` + xarray 2024.x + cftime, on
`/work/bb1469/.../Final_CMIP7_IO_Test_03/outdata/oifs/atmos_mon_land_sbl_1587-1587.nc`
opened with `use_cftime=True`. Result: `da.resample(time="MS")` gave
exactly 12 groups, one per month. **That repro was outside the pycmor
pipeline; it does not preclude pipeline-internal interaction.** Needs
re-validation at HEAD inside the step chain (instrument-driven —
covered by the plan below).

**Class:** RECIPE / unreproduced.

**Investigation plan (instrument-first, with contingency per round-6 review).**

Stage 1 — instrumentation. Add to `mask_where_no_seaice` immediately
after the `.sel(method="nearest")` call AND at the start of `timeavg`:

```python
# mask_where_no_seaice, after a_ice = a_ice.sel({time_dim: data[time_dim]}, ...):
logger.info(
    f"mask_where_no_seaice [{rule.get('name','?')}]: "
    f"data.{time_dim} len={data[time_dim].size} unique={data[time_dim].to_index().is_unique} "
    f"a_ice.{time_dim} len={a_ice[time_dim].size}"
)

# src/pycmor/std_lib/timeaverage.py, top of timeavg:
logger.info(
    f"timeavg [{getattr(rule, 'cmor_variable', '?')}]: "
    f"da.time len={len(da.time) if 'time' in da.coords else 'no-time'}"
)
```

Stage 2 — targeted single-rule re-run. Build a one-rule yaml restricted
to `sbl_seaice` against the same Test_03 input. Submit with logging
verbose (PYTHONLOGLEVEL=DEBUG) so the instrumentation output is
captured.

Stage 3 — interpret. Three diagnostic outcomes:
- **`mask` log shows 12 → `timeavg` log shows 7**: shrinkage happens
  between mask exit and timeavg entry. Investigate any pipeline step
  in between (none currently for this pipeline, but Prefect-collapse
  may interpose) — or it's `data * mask` itself dropping non-overlapping
  time coords.
- **`mask` log shows 7 already**: shrinkage happens at the
  `.sel(method="nearest")` call. Likely cftime/calendar interaction
  with the bounds-aware `nearest` matcher. Fix: replace
  `method="nearest"` with explicit groupby-based monthly matching, OR
  drop time_bounds from `a_ice` before `.sel()`.
- **Both logs show 12**: shrinkage happens deeper inside `timeavg`'s
  resample loop (line 371 in [timeaverage.py](src/pycmor/std_lib/timeaverage.py#L371)).
  Escalate to single-rule pdb or add a third instrument inside the
  resample iteration.

**Contingency (round-6 review).** Four prior runs have not localized
this; the 30-min estimate is optimistic. Realistic budget is
**1-2 hours of investigation** including the targeted single-rule
re-run and pdb escalation if Stage 3 doesn't immediately pinpoint the
shrinkage. Lower priority since just 1 rule.

**Effort:** 30 min for Stage 1+2 setup, +30-60 min for Stage 3
interpretation and patch.

**Clears:** 1 fail (if root cause found and patched).

---

### §3.6 F6 — `tas_1hr` KilledWorker (×1)

**Tier/rule:** cap7_land — `tas_1hr`.

**Pipeline:** FrozenPipeline (no custom step; standard load → timeavg →
save).

**Log:**
```
ERROR: Pipeline 'FrozenPipeline' FAILED for rule 'tas_1hr' after 739.0s:
  KilledWorker: Attempted to run task 'finalize-hlgfinalizecompute-...'
  on 4 different workers, but all those workers died while running it.
```

**Root cause.** Hourly OIFS surface temperature: 8760 timesteps × 421120
cells × 4 B = 14.7 GB raw float32. Even with `MEM_PER_WORKER=32GB`
(bumped this session for cap7_land), the finalize stage accumulates
intermediate copies (timeavg resample state, save_dataset encoder
buffer) that overflow the worker heap and dask-nanny kills it.

**Class:** INFRA — runtime knob, not a recipe issue.

**Fix (revised per round-6 review).** Original plan was 4 workers ×
48 GB = 192 GB. The pre-submit budget assertion in
[run_hr_yaml_cli.sh](examples/run_hr_yaml_cli.sh) refuses jobs where
`N_WORKERS × MEM_PER_WORKER > CGROUP_GB × 0.75`; on a 256 GB cgroup
the budget is `256 × 0.75 = 192 GB` — the 4×48 setting hits this
ceiling exactly with zero headroom for OS / driver / page-cache. Two
safer options:

1. **3 workers × 48 GB = 144 GB.** Headroom retained (about 56 GB OS
   space on a 256 GB cgroup). Loses one parallelism slot, but tas_1hr
   is a single rule that monopolizes a worker — three other workers
   make progress on other rules in parallel, so wall-time impact is
   minimal compared to losing a worker to OOM.
2. **Bump cgroup to `--mem=384G` and keep 4×48 GB.** Costs more
   memory allocation but preserves parallelism.

Recommend option 1 (3×48). Same template as we used for extra_atm
(--mem=512G + MEM_PER_WORKER=48GB) but with N_WORKERS=3 to stay under
the 75% budget cap.

```bash
# in submitter loop:
elif [ "$tier" = "cap7_land" ]; then
  jid=$(sbatch --parsable -J ... --time=03:00:00 \
    --export=ALL,N_WORKERS=3,MEM_PER_WORKER=48GB \
    examples/run_hr_yaml_cli.sh ...)
```

**Effort:** 2 min.

**Clears:** 1 fail.

---

## §4. Action plan

Ordered by speed-to-clear and risk. Effort estimates revised per
round-6 review — initial estimates (12 min for 1-3) understated the
hardening + interpretation work.

| step | class | action | effort | clears | dep |
|---|---|---|---:|---:|---|
| 1 | F1 | refactor as `_resolve_year(rule)` helper, patch BOTH callers (custom_steps.py:885 + 917) | 10 min | 4 | none |
| 2 | F4 | first-pass: drop OIFS aux time coords in `regrid_oifs_to_fesom` + add `mask_where_no_seaice` instrumentation logging `data.indexes` and `is_unique` | 10 min | 6 (provisional) | none |
| 3 | F6 | bump cap7_land submitter to N_WORKERS=3 + MEM_PER_WORKER=48GB (3×48=144GB, retains headroom; not 4×48=192GB which hits budget cap exactly) | 2 min | 1 | none |
| 4 | F3 | comment out `hfx_int_day` / `hfy_int_day` (option 1) **with explanatory yaml block** referencing this proposal §3.3 + the CMIP-target-unit conflict | 5 min implementation, gated on user input (§5.3) | 2 | user decision (latency = wall time, not work time) |
| 5 | F5 | instrument `mask_where_no_seaice` + start of `timeavg`; targeted single-rule re-run for `sbl_seaice` | 30 min setup, +30-60 min Stage 3 interpretation | 1 (if hypothesis localizes) | (1)-(3) landed first |
| 5b | F4-followup | promote aux-coord drop from `regrid_oifs_to_fesom` to `pycmor.core.gather_inputs.load_mfdataset` (structural fix) | 15 min | — (durability) | (2) lands instrumentation; next-run log shows aux index → trigger 5b same day |
| 6 | F2 | external work: FESOM rerun (relaxsalt enabled this session; strength_ice via maEVP patch) + OIFS file_def update (slhf, sshf) | — | 4 | external owners (see §3.2 table) |

**Realistic clear-rate per review.** Steps 1+2+3: ~30 min (not 12).
Step 4: 5 min plus user input. Step 5: 30 min setup + 30-60 min
interpretation. Total: about 2 hours of careful work to land the 11
clearable failures durably, plus instrumentation-driven confirmation
of the F4 hypothesis.

After steps 1-3 (12 min total): expected next-run state is
**540+11 = 551 ok / 7 fail**.
After step 4 (assuming user goes with option 1): **553 ok / 5 fail**.
After step 5 (if successful): **554 ok / 4 fail**.
F2's 4 fails resolve when the model rerun + file_def updates land.

## §5. Risks and follow-ups

### 5.1 F4 fix may not fully resolve duplicate-time

The drop-aux-coords hypothesis is the strongest from the alignment
stack frames, but it's not 100% — it's possible `data.time` itself has
duplicates from a load_mfdataset edge case I haven't traced. The
defensive drop is harmless even if not sufficient; if duplicates persist,
the instrumentation step in F4 localizes the actual source.

### 5.2 F5 may need pipeline-internal repro

If the `mask_where_no_seaice` print doesn't show 7 timestamps, the bug
enters earlier — possibly in `regrid_oifs_to_fesom` itself when handling
monthly OIFS (`atmos_mon_land_sbl`) input. A second instrumentation
pass at the start of `timeavg` should localize it cleanly.

### 5.3 F3 option 1 vs option 2

If the user wants daily heat-transport diagnostics for science (not just
CMIP compliance), option 1 (drop) is wrong and option 2 (add horizontal
integration) is required. **User confirmation needed before stepping (4)**.

### 5.4 Walltime margin (revised per round-6 review)

`cap7_atm` ran 2h38m / 3h (88% of walltime) and `core_atm` ran 2h35m /
3h (86%). Both finished, but the margin is tight, not "fine."

**Caveat.** F4's success will INCREASE lrcs_seaice's elapsed time:
6 rules currently abort within 3-5 minutes of start (the duplicate-time
error fires fast). Once those rules survive past mask_where_no_seaice,
each one runs the regrid + mask + timeavg + save chain end-to-end
(~5-10 min per 8760-timestep hourly rule, depending on chunking). That
adds ~30-60 minutes to lrcs_seaice's total. With current 3h walltime,
the tier may approach but not exceed walltime.

**Plan B.** If lrcs_seaice or other tiers hit walltime after F4 lands,
bump walltime to 4h on the heavy-rule tiers (lrcs_seaice, lrcs_ocean,
cap7_atm, core_atm). One-line `--time=04:00:00` in the submit loop.

No walltime change needed for the immediate cli5 resubmit; reassess
after run.

### 5.5 Documentation pending

- DESIGN_PROPOSAL §10 closing chapter (gate-A v3+v4 final, EDQUOT/HDF
  wedge diagnosis, extra_atm@512G verdict, heartbeat-progress-check
  follow-up) — not in repo yet, separate from this proposal.
- HANDOFF_failed_rules.md / HANDOFF_failed_rules_findings.md —
  obsolete after this round, should be retired or archived once F1/F4/F6
  land.

## §6. Decision request

Please confirm (revised per round-6 review):

- (1)-(3) ship as-is (~30 min realistic, clears 11 of 18, plus
  instrumentation that confirms F4 hypothesis on next run).
- (4) F3 — **drop hfx_int_day / hfy_int_day** OR **add horizontal-integration step**?
  Drop comes with the explanatory yaml block in §3.3 so future readers
  don't groundhog-day-uncomment.
- (5) F5 — instrument now (covered by step 2's instrumentation, plus
  start-of-`timeavg` log), or defer the targeted single-rule re-run?
- (5b) F4-followup — if step (2)'s instrumentation confirms the
  aux-coord hypothesis, OK to promote the drop from
  `regrid_oifs_to_fesom` into `pycmor.core.gather_inputs.load_mfdataset`?
  Structural fix; benefits all downstream consumers.
