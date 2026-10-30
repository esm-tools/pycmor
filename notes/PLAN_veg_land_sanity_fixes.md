# Plan: Address LAND/VEG sanity-check feedback (Laszlo + Christian)

Branch: `feat/cmip7-awiesm3-veg-hr`
Source: reviewer feedback on `tools/sanity_check/reports/test06_cli_y1587_v7_html/veg.html`
Review iteration: round 1 incorporated (see `REVIEW_veg_land_sanity_fixes_round1.md`)

Overall picture (per reviewers): most FAILs are HR per-cell peaks tripping LR-tuned
bounds. Global means stay within literature ranges. Loosen bounds rather than
touch the model for the majority. A small number of issues are real pycmor rule
bugs (sign-noise, missing clip, or aggregation/time-step mismatch).

## Execution order (revised after round-1 review)

1. **Phase D4 first — `treeFracNdlDcd`** (CRITICAL: rule-bug suspect).
   Rationale: if the rule is buggy, Phase A bounds tuning would fit bounds to
   bad data, compounding the bug by masking it from future sanity checks.
2. **Phase A** — bounds-only edits in `doc/sanity_check_ranges.md`.
3. **Phase B** — clip-band noise (`clip_small_negatives`).
4. **Phase C** — sign-fix `clip(min=0)` for `mrsol`/`rhSoil`.
5. **Phase D1, D2, D3, D5** — remaining investigations, each in its own pass.
6. **Phase E** — re-run sanity_check **after every meaningful fix lands**, not
   only at the end. One commit per fix; one report-regen per commit.

## Cross-cutting decisions (round-1 review)

- **Clip implementation site**: all Phase B/C clips live in
  [examples/custom_steps.py](examples/custom_steps.py) as named steps and are
  referenced per-rule from the yaml. Rationale: LPJG-specific logic already
  lives there (see `load_lpjguess_*` family), keeps one source of truth.
- **Phase B clip threshold**: use **1e-10** (not 1e-12 as originally drafted).
  Reviewer flagged 1e-12 as a typo — it would leave the -1e-10 .. -1e-12 band
  still negative. Pick 1e-10 or wider; confirm against raw `.out` noise
  distribution before committing.
- **Clip semantics to implement**: `clip_small_negatives(x, threshold)` shall
  set every value in `[-threshold, +threshold]` to 0. Documented in the
  step's docstring so reviewers don't have to read the body.
- **Per-fix re-run cadence**: after each commit that changes data, re-run
  `tools/sanity_check/sanity_check.py` on test06 output and regenerate the
  HTML report. The single end-of-plan re-run is dropped.

## Phase D4 (run first) — `treeFracNdlDcd` annual cycle (CRITICAL)

Investigation complete; full writeup with options matrix, math, edge
cases, and pre-implementation ratio check is in
**`HANDOFF_d4_treeFrac_per_pft.md`** (round-1 review folded in).

Exit criteria: per the handoff. Land treeFrac (total) source-switch
first, run the Option B ratio sanity-check on cli37 data, then implement
per-PFT synthesis pending Laszlo math signoff.

## Phase A — bounds-only edits in `doc/sanity_check_ranges.md`

Pure markdown table edits. No pycmor code touched. Safe to revert.

Each row below records:
- The **reviewer** who flagged the variable as "real, not a bug."
- The **data anchor** (which percentile of cli37 / test06 output the new
  bound brackets). To be filled in by the implementer immediately before
  the edit — opening the JSONL report and reading the `actual_min`,
  `actual_max` columns for each variable.

| Variable | Current min / mean / max | New min / mean / max | Reviewer | Data anchor (fill in pre-commit) |
|---|---|---|---|---|
| nep | -5e-8 / ~0 / 5e-8 | **-5e-6** / ~0 / **2e-7** | Laszlo: Central America wet-tropics drainage spikes, raw == cmor | new max brackets {p99.9} of test06 nep; new min brackets {p0.1} |
| mrrob | 0 / ~1e-5 / 1e-4 | 0 / ~1e-5 / **5e-3** | Laszlo + Christian: singular grid-cell overshoot, field overall fine | new max brackets {p99.99} of test06 mrrob |
| fAnthDisturb | 0 / ~0 / ~0 | 0 / ~0 / **~1e-12** (noise floor) | Laszlo (round 2): transition-driven; state frozen at 1850 → truly ~0. **Bound stays tight; loosen ONLY to clear numerical-noise WARNs, not real flux.** | check actual cli37 values: if >1e-10, that's a real finding for investigation, NOT bound relax |
| fHarvestToAtmos | 0 / ~0 / ~0 | 0 / **~1e-10** / **~1e-8** | Laszlo (round 2): "small but non-zero" — harvest at 1850 cropland levels keeps cycling | independently verify mean+max from test06 |
| fNAnthDisturb | 0 / ~0 / ~0 | 0 / ~0 / **~1e-12** (noise floor) | Laszlo (round 2): transition-driven; state frozen → truly ~0. **Same treatment as fAnthDisturb.** | check actual cli37 values; >1e-10 = real finding, not bound relax |
| fNfert | 0 / ~0 / ~0 | 0 / ~0 / **~1e-12** (noise floor) | Laszlo (round 2): 1850 synthetic-fertilizer rates were essentially zero → truly ~0 | check actual cli37 values; >1e-10 = real finding, not bound relax |
| tsl | **220** / ~285 / 325 | **150** / ~285 / 325 | Laszlo: LPJ-GUESS shallow-layer Tsoil tracks OpenIFS forcing when uninsulated | 1.5% <220K, global mean 284K. **Floor set at 150 K: 64 K Polar Urals outlier must remain a FAIL (physically impossible in nature).** |
| vegHeight | 0 / ~5 / ~50 | unchanged in Phase A | Christian: rationale text wrong | rationale text only; bounds revisit pending D5 outcome |

**fAnthDisturb-family physics (per Laszlo, round 2 — `fixed_LU=1850`)**:

- The LUH3 state is **frozen** at 1850, NOT transient. So:
  - `fAnthDisturb` (transition-driven anthropogenic disturbance flux): **truly ~0** — no transitions in a frozen state.
  - `fNAnthDisturb` (N analogue): **truly ~0** — same reason.
  - `fNfert` (N fertilizer flux): **truly ~0** — 1850 synthetic-fertilizer rates were essentially zero.
  - `fHarvestToAtmos` (harvest flux to atmos): **small but non-zero** — the 1850 state already has ~10% cropland + ~20% pasture, and crop+wood harvest keep cycling at 1850 levels every model year.

Implication: bounds for the three "truly ~0" variables stay tight (~1e-12
ceiling for pure numerical noise). If any of them actually shows ~1e-10 or
larger in cli37, that is a **real finding to investigate** (Phase D
candidate), not a bound relax. Only `fHarvestToAtmos` gets meaningful
bound relaxation.

Round-1 review's concern about copy-pasted bounds is now more relevant
than ever — Laszlo's round-2 clarification reveals the four are
physically distinct.

Procedure for filling data anchors:
```
python -c "
import xarray as xr, glob, numpy as np
for v in ['nep','mrrob','fAnthDisturb','fHarvestToAtmos','fNAnthDisturb','fNfert','tsl']:
    paths = glob.glob(f'/scratch/a/a270092/pycmor_hr/Test_16n_y1587/cmorized/**/{v}_*.nc', recursive=True)
    if not paths: print(f'{v}: no files'); continue
    ds = xr.open_mfdataset(paths)
    arr = ds[v].values
    arr = arr[np.isfinite(arr)]
    print(f'{v}: p0.1={np.quantile(arr,0.001):.3g} p50={np.quantile(arr,0.5):.3g} p99.9={np.quantile(arr,0.999):.3g} p99.99={np.quantile(arr,0.9999):.3g}')
"
```

Skip in Phase A:
- `dsn` — Christian says interval/time-step is the issue, treat as Phase D3.
- `snd` — Christian links peaks north of Greenland to snow-on-seaice issue,
  treat as Phase D2.
- `vegHeight` bounds — rationale-text-only fix here; bounds revisit pending
  D5 outcome. If D5 finds a real rule bug, both the bounds and the rule
  will need follow-up; this is intentional.

After Phase A: re-run sanity_check, regenerate HTML report, count remaining
FAILs. Commit as a single doc-only commit.

## Phase B — clip-band rule fixes (tiny negative float noise)

Reviewer claim: raw `.out` has the same tiny ~1e-10 negative values; clip
in the pycmor rule, do not chase as a model bug.

Variables: `fNnetmin`, `fVegLitterMortality`, `fNloss`, `gpp`, `gppLut`,
`mrtws`, `wetlandCH4`.

Implementation:
1. Add `clip_small_negatives(data, rule, threshold=1e-10)` to
   [examples/custom_steps.py](examples/custom_steps.py). Step signature
   matches existing pycmor convention (`(data, rule) -> data`). Body sets
   values in `[-threshold, +threshold]` to 0 and leaves the rest untouched.
   The threshold is configurable via `rule.clip_threshold` (default 1e-10).
2. For each variable, locate its rule yaml in
   `awi-esm3-veg-hr-variables/veg_land/cmip7_awiesm3-veg-hr_land.yaml` and
   add the step to the pipeline (after the loader, before any unit
   conversion that could amplify the noise).
3. **Pre-commit calibration**: dump min(raw) and the negative-value
   distribution for each variable. Confirm 1e-10 actually catches the
   tail; widen to 1e-9 only if needed. Record the chosen threshold per
   variable in the commit message.

Verify: per-variable raw .out min vs cmor min before and after; re-run
sanity_check after commit.

## Phase C — sign-fix `clip(min=0)`

Reviewer claim: raw `.out` `nneg==0`, cmor has real negatives → pycmor rule bug.

Variables: `mrsol`, `rhSoil`.

Per-variable physical-floor confirmation (so future readers know the clip
is not eating real signal):
- **mrsol** (soil moisture content, kg m-2): 0 is a hard physical floor.
  No soil cell can hold less than zero water. Safe to clip.
- **rhSoil** (heterotrophic soil respiration, kg m-2 s-1): per CMIP
  convention, the variable is defined as ≥0 (a flux out of the soil C
  pool). In principle some models report negative values when soil is
  net C-uptaking — but for LPJ-GUESS rh is a one-sided efflux and any
  observed negatives are numerical artifacts. Confirmed by Laszlo (raw
  `.out` `nneg==0`). Safe to clip.

Implementation: add `clip_min_zero(data, rule)` to
[examples/custom_steps.py](examples/custom_steps.py), reference per-rule
in the yaml — same site/pattern as Phase B.

Verify: raw vs cmor `nneg` count for each variable post-commit; re-run
sanity_check.

## Phase D — remaining investigations

Each is a separate pass. D4 has already run as the first step (above).

### D1. `esn`, `sbl`, `nppLut` — cmor min/max do not match raw

Hypothesis: variant aggregation rule (per-PFT → per-cell) is wrong.

**Magnitude of mismatch (fill before starting):**
- `esn`: raw min/max vs cmor min/max → {tbd}, ratio {tbd}.
- `sbl`: {tbd}.
- `nppLut`: {tbd}.

A small mismatch (~5%) is a weighting/rounding issue. A large mismatch
(10× or more) is an aggregation step entirely wrong. Implementer chooses
priority by magnitude.

Steps: open the rule yaml, find the aggregation step, compare with a
known-good variable (e.g. `npp` if raw==cmor), report finding before
changing code.

### D2. `snd` — raw mean 0.47 vs cmor mean 0.58

Hypotheses:
- Laszlo: fill-value or area-weighting effect.
- Christian: peaks north of Greenland → snow-on-seaice issue, possibly
  linked to the already-known seaice model snow extreme.

Steps: dump per-cell diff map, isolate where cmor > raw, confirm
hypothesis, decide whether the fix is in pycmor (weighting/fill) or
downstream.

### D3. `dsn` — change in snow water equivalent is unphysically large

Christian: 2 tons of snow change per m² per hour is implausible; interval
mismatch.

Steps: check the rule's time-aggregation step. Is `dsn` computed as
`SWE(t) - SWE(t-Δt)` with Δt mismatched to the cmor cadence? Likely a
unit / Δt fix in the rule, not the model.

### D6. `fAnthDisturb`, `fNAnthDisturb`, `fNfert` — non-zero in frozen-LU piControl

Discovered during Phase A data-anchor check. Laszlo (round 2) said these
should be truly ~0 because the LUH3 state is frozen at 1850 (no
transitions, 1850 fertilizer rates were ~0). cli37 cmor shows:

| Variable | cli37 mean | cli37 max | Noise floor (~1e-12) | Above noise by |
|---|---|---|---|---|
| fAnthDisturb | 4.31e-10 | 1.14e-8 | 1e-12 | 100x (mean), 1e4x (max) |
| fNAnthDisturb | 1.47e-11 | 4.85e-10 | 1e-12 | 10x (mean), 100x (max) |
| fNfert | 2.26e-11 | 3.14e-9 | 1e-12 | 10x (mean), 1000x (max) |

`fHarvestToAtmos` shares the magnitude (mean 4.31e-10, max 1.14e-8) and
IS supposed to be small — only it got the bound relax in Phase A.

Hypotheses to test:
- Are these fluxes flowing from sub-grid LUH3 internal-state cycling
  (e.g. crop rotation, wood-product turnover) even with the *macro*
  state frozen? If yes, Laszlo's "truly ~0" was an overstatement and
  bounds should be loosened.
- Is the pycmor rule double-counting / mis-aggregating from a LUH-
  related raw file? Compare raw `.out` to cmor for each.
- Is the LPJ-GUESS configuration accidentally enabling some transition
  pathway that should be off under `fixed_LU=1850`?

Bound stays tight (~0) until investigation completes. The current FAIL
status correctly flags this.

### D5. `vegHeight` — percentiles reach 52m

Christian: trees and grasses likely mixed in the analysis/reference, or
the rule mis-handles the PFT dimension.

Steps: dump per-PFT heights, check if the rule is averaging over the
wrong dim or exposing only a max. If a real rule bug is found, revisit
the Phase A "rationale text only" treatment of `vegHeight` and update
both bounds and rationale.

## Phase E — verification (per-fix, not per-phase)

After every meaningful commit (every A-row group, every B variable, every
C variable, every D fix):

1. Re-run `tools/sanity_check/sanity_check.py` on test06 output.
2. Regenerate HTML report via `tools/sanity_check/build_html_report.py`.
3. Note FAIL/WARN delta in the commit message of the *following* commit
   so the chain is readable: "after previous commit, FAILs dropped from
   N to M."

## Open questions for user

(none — all resolved)

## Resolved from round-1 review and user calls

1. ~~`tsl` expected_min~~ → **150 K**. 64 K Polar Urals outlier must remain
   a FAIL (physically impossible in nature, per user).
2. ~~`dsn` treatment~~ → **Phase D3** (rule fix, time-step). No Phase A
   bound relax for dsn.
3. ~~Phase B/C clip implementation site~~ → **`examples/custom_steps.py`**
   referenced from the rule yaml. (Single source of truth, sits next to
   the existing `load_lpjguess_*` family.)
4. ~~Phase B clip threshold~~ → **1e-10** (default), per-variable
   calibration before commit.
5. ~~D4 ordering~~ → **runs first**, before Phase A bounds tuning.
6. ~~Phase E cadence~~ → **per fix**, not just per phase-bundle.
7. ~~fAnthDisturb-family bounds~~ → **only `fHarvestToAtmos` loosened**;
   the other three stay tight at ~1e-12 noise ceiling. State is frozen
   at 1850 → physically ~0.
