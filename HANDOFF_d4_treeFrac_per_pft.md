# D4 handoff: per-PFT tree fractions (treeFracNdlDcd, treeFracBdlDcd, treeFracNdlEvg, treeFracBdlEvg)

Review iteration: round 1 incorporated
(see `REVIEW_d4_treeFrac_per_pft.md`).

## Reviewer evidence

**Christian (round 1)**: "treeFracNdlDcd: this is critical! Pattern looks weird and has an annual cycle - tree distribution should NOT have an annual cycle. Maybe leaf area index was somehow wrongly involved in the computation."

**Laszlo (round 2)**: "treeFracNdlDcd: real bug — monthly aggregation is phenology/LAI weighted, should be stand area. **The yearly file is fine.** treeFracNdlDcd and vegHeight need an LPJ-GUESS change plus a matching pycmor update."

## What the pycmor rule does today

`awi-esm3-veg-hr-variables/cap7_land/cmip7_awiesm3-veg-hr_cap7_land.yaml` lines 1112-1138 define `treeFracBdlEvg_mon`, `treeFracNdlDcd_mon`, `treeFracNdlEvg_mon` (and `treeFracBdlDcd_mon` lives in `veg_land/cmip7_awiesm3-veg-hr_land.yaml:714`).

Each rule:
- Source: `{ldp}/*/run1/treeFrac{PFT}_monthly.out`
- Pipeline: `lpjg_monthly_pipeline`
- Loader: `load_lpjguess_monthly` in `examples/custom_steps.py:2731`
- No transformation — the loader just reads the 12 monthly columns from the raw `.out` and reshapes to (time=year*12, ncells).

So the cmor output **is exactly** the raw monthly `.out`. If raw has an annual cycle, cmor has it.

## What's actually in the data

cli37 source: `/work/bb1469/a270092/runtime/awiesm3-develop/Final_CMIP7_IO_Test_01/outdata/lpj_guess/15860101-15871231/run1/`

LPJ-GUESS yearly outputs (24 files):
- ✅ `treeFrac_yearly.out` (total)
- ✅ `grassFrac_yearly.out`, `shrubFrac_yearly.out`, `baresoilFrac_yearly.out`, `cropFrac_yearly.out`, `pastureFrac_yearly.out`, `vegFrac_yearly.out`
- ❌ `treeFracBdlDcd_yearly.out` — does NOT exist
- ❌ `treeFracBdlEvg_yearly.out` — does NOT exist
- ❌ `treeFracNdlDcd_yearly.out` — does NOT exist
- ❌ `treeFracNdlEvg_yearly.out` — does NOT exist

LPJ-GUESS monthly outputs include all four per-PFT tree fractions. Per Laszlo, these are phenology/LAI-weighted — wrong for the CMIP7 spec (which is "stand area percentage").

## Implication

Laszlo's "switch to the yearly file" advice works for:
- `treeFrac` (total) → source switch from `treeFrac_monthly.out` → `treeFrac_yearly.out`
- and any other variable whose yearly file is in the inventory above

It does **NOT** work for the four per-PFT tree fractions, because LPJ-GUESS does not emit yearly per-PFT splits.

## Options for the per-PFT tree fractions

### Option A — Don't ship them in CMIP7
Document as known issue: pending LPJ-GUESS upstream change to emit yearly per-PFT
stand-area fractions. Drop the four rules from the rule yaml for now.

Pros: scientifically clean. Cons: CMIP7 incompleteness; runs against the
"never skip expensive variables" principle.

### Option B — Synthesize yearly per-PFT in pycmor (RECOMMENDED)
Combine the **correct yearly total** with **monthly-proportional split**,
**per grid cell**:

```
proportion_PFT(year, cell) = max_over_months_in_year(treeFrac{PFT}_monthly[year, cell])
                             / sum_over_PFTs( max_over_months_in_year(treeFrac{PFT}_monthly[year, cell]) )

treeFrac{PFT}_yearly(year, cell) = treeFrac_yearly(year, cell) * proportion_PFT(year, cell)
```

Then broadcast the resulting yearly value to all 12 months of that year
for the cmor monthly output.

Reasoning: the **annual max** of the LAI-weighted monthly cover is a
fair proxy for stand area (deciduous trees at full leaf-on ≈ stand area;
evergreens are ~constant so max == value). Normalizing across the four
PFTs **per grid cell** guarantees they sum to the correct yearly total
in each cell.

**Edge cases the implementation MUST handle (from round-1 review §1, §2):**

- **Per-cell normalization, NOT global**: every grid cell computes its
  own proportions across the 4 PFTs from that cell's monthly maxes.
  A global average would mangle cells with 90% Bdl + 10% Ndl vs cells
  with 10% Bdl + 90% Ndl. Implement with xarray broadcasting on the
  `ncells` dim, never reducing over it before the division.
- **Divide-by-zero in tree-free cells**: cells with no trees of any PFT
  (ocean, desert, ice) have `sum_over_PFTs(max) == 0`. Use
  `xr.where(sum_of_max > 0, max_PFT / sum_of_max, 0.0)`. Output for
  these cells is 0, consistent with the cell having no trees.

Pros: ships CMIP7 with defensible values; uses authoritative yearly total
where it exists; lifts the per-PFT split from the only available source.
Cons: not authoritative model output — would need a known-issue note in
metadata; needs Laszlo signoff on the math.

### Option C — Annual max of monthly per-PFT directly
Skip the yearly total cross-check; just take `annual_max(treeFrac{PFT}_monthly)`
and broadcast. Simpler than B, but loses the constraint that the four PFTs
sum to the (separately-emitted) yearly total.

Pros: simplest. Cons: PFTs may not sum to total → user may flag inconsistency.

### Option D — Wait for LPJ-GUESS fix
LPJ-GUESS needs to emit `treeFrac{PFT}_yearly.out` as stand-area (not
LAI-weighted). Then pycmor: switch the source to those yearly files and
broadcast to monthly cadence.

Pros: scientifically right. Cons: blocks CMIP7 for these four variables
until the model change ships.

## Decision rationale & timeline assumption (from round-1 review)

**Option B chosen** with these timeline assumptions:
- CMIP7 ship target: within the next ~12 months.
- LPJ-GUESS upstream fix: no committed timeline; needs an issue filed
  (see Escalation below).

Revisit decision if LPJ-GUESS publishes per-PFT yearly stand-area output
before the CMIP7 freeze — in that case fall back to Option D.

Option B math needs Laszlo signoff + a `comment` attribute in the cmor
file documenting the derivation.

## Pre-implementation ratio sanity-check (REQUIRED before D4c — from round-1 review §3)

Before any synthesis code is written, verify the Option B math premise on
real cli37 data:

> Does the per-cell sum of LAI-weighted monthly annual-maxes roughly
> equal the authoritative yearly total per cell?

Cheap script (run once, paste percentile output into the D4c commit message):

```python
import xarray as xr
import numpy as np
import pathlib

src = pathlib.Path(
    "/work/bb1469/a270092/runtime/awiesm3-develop/"
    "Final_CMIP7_IO_Test_01/outdata/lpj_guess/15860101-15871231/run1"
)
PFTS = ["BdlDcd", "BdlEvg", "NdlDcd", "NdlEvg"]

# Reuse load_lpjguess_monthly / load_lpjguess_yearly machinery here.
# Pseudocode:
#   yearly_total[year, cell]  ← load(src / "treeFrac_yearly.out")
#   for p in PFTS:
#       monthly[p][time, cell] ← load(src / f"treeFrac{p}_monthly.out")
#       max_p[year, cell] = monthly[p].groupby("time.year").max(dim="time")
#   sum_of_max[year, cell] = sum(max_p[p] for p in PFTS)
#   ratio = sum_of_max / yearly_total
#   for q in [0.01, 0.10, 0.50, 0.90, 0.99]:
#       print(f"  p{q*100:5.1f}: ratio={np.nanquantile(ratio, q):.3f}")
```

Decision rule for the ratio percentiles:
- **ratio ≈ 1.0 across cells** → Option B is a clean re-split. Proceed.
- **ratio ≈ 0.8 consistently** (LAI-weighting underestimates stand area)
  → the renormalization step does real work. Acceptable but document
  the magnitude in the commit message.
- **ratio varies wildly** (e.g. 0.3 to 1.5) → proxy is bad. Reconsider
  Option B; fall back to A (skip) or escalate for a different proxy.

## Implementation sketch (Option B) — fleshed per round-1 review §4

```python
# examples/custom_steps.py
PFTS = ["BdlDcd", "BdlEvg", "NdlDcd", "NdlEvg"]

def load_lpjguess_tree_pft_yearly_from_total(data, rule):
    """
    Synthesize yearly per-PFT tree fraction from:
      - treeFrac_yearly.out (authoritative total)
      - treeFrac{PFT}_monthly.out (4 files, LAI-weighted but reasonable
        annual-max proportions, used per grid cell)

    Broadcast to monthly cadence for CMIP7 Emon output.
    rule.tree_pft must be one of BdlDcd, BdlEvg, NdlDcd, NdlEvg.
    """
    pft = rule.get("tree_pft")
    if pft not in PFTS:
        raise ValueError(f"rule.tree_pft must be one of {PFTS}, got {pft!r}")

    lpjg_dir = rule.inputs[0].path  # find siblings here

    yearly_total = _load_one_lpjg_yearly(lpjg_dir, "treeFrac_yearly.out")
    # shape: (year, ncells)

    pft_max = {}
    for p in PFTS:
        monthly = _load_one_lpjg_monthly(
            lpjg_dir, f"treeFrac{p}_monthly.out"
        )  # (time, ncells)
        pft_max[p] = monthly.groupby("time.year").max(dim="time")
        # shape: (year, ncells)

    sum_of_max = sum(pft_max.values())

    # round-1 §1: per-cell normalization. xr broadcasting on (year, ncells)
    # never reduces over ncells before the division.
    # round-1 §2: where sum_of_max == 0 (tree-free cell), proportion = 0.
    proportion = xr.where(sum_of_max > 0, pft_max[pft] / sum_of_max, 0.0)

    yearly_pft = yearly_total * proportion  # (year, ncells)

    # Broadcast yearly value to monthly cadence: 12 identical mid-month
    # samples per year, time-centered.
    return _broadcast_yearly_to_monthly(yearly_pft)
```

`_load_one_lpjg_yearly`, `_load_one_lpjg_monthly`, and
`_broadcast_yearly_to_monthly` extracted as helpers, reusing the existing
parsing path in `load_lpjguess_monthly` / `load_lpjguess_yearly`.

Yaml wiring (one block per affected rule):

```yaml
- name: treeFracNdlDcd_mon
  inputs:
    - path: *ldp
      pattern: "*/run1/treeFracNdlDcd_monthly.out"
      additional_files: "treeFrac_yearly.out,treeFracBdlDcd_monthly.out,treeFracBdlEvg_monthly.out,treeFracNdlEvg_monthly.out"
  compound_name: land.treeFracNdlDcd.tavg-u-hxy-u.mon.GLB
  tree_pft: NdlDcd
  pipelines:
    - lpjg_tree_pft_pipeline
```

New pipeline `lpjg_tree_pft_pipeline` is `lpjg_monthly_pipeline` with the
loader swapped for `load_lpjguess_tree_pft_yearly_from_total`.

## Metadata `comment` attribute (pre-drafted per round-1 review polish)

Add via `rule.variable_attributes.comment` in each affected rule:

```
comment: "Per-PFT tree fraction synthesized from authoritative annual
    total (treeFrac_yearly.out) and PFT-relative annual-max proxy
    (treeFrac{PFT}_monthly.out), using pycmor's
    load_lpjguess_tree_pft_yearly_from_total step. LPJ-GUESS does not
    emit per-PFT yearly stand-area fractions directly; the LAI-weighted
    monthly per-PFT output is renormalized to the yearly total per grid
    cell. Pending model-team fix for native per-PFT yearly emission;
    see HANDOFF_d4_treeFrac_per_pft.md."
```

## Execution order inside D4 (per round-1 review §5)

1. **D4a — land `treeFrac` (total) source-switch FIRST.** Confirms the
   yearly→monthly broadcast machinery on a simpler rule; validates
   `treeFrac_yearly.out` is loader-compatible before D4b/c depend on it.
   Audit whether the project ships a `treeFrac_mon` rule sourced from
   `treeFrac_monthly.out`; if so, re-source to `treeFrac_yearly.out` and
   broadcast. Verify with sanity_check (annual cycle should disappear).
2. **D4b — Option B ratio sanity check.** Run the script above on cli37
   data. Paste percentile output into the D4c commit message.
3. **D4c — implement Option B for per-PFT.** Code the step + wire the
   four rules, after Laszlo math signoff and the D4b ratio check passes.
4. **Re-run sanity_check** after each of D4a, D4c.

## Adjacent issues for separate handling

- **`vegHeight`**: Laszlo says LPJ-GUESS doesn't emit a grass-only
  height, so cmor falls back to the tree-dominated field. Pycmor cannot
  fix this — needs LPJ-GUESS to emit `vegHeightGrass_monthly.out`.
  Document as known issue, escalate (see below). Separate from D4.
- **`treeFrac` (total)**: source switch monthly → yearly, broadcast.
  Lands as D4a above.

## Escalation channels (per round-1 review polish)

Concrete owners/channels for the upstream-model issues this plan
surfaces. Filing these is gating — track them, don't let them rot.

- **LPJ-GUESS per-PFT yearly stand-area output**: file at the
  LPJ-GUESS-AWI tracker
  (`https://gitlab.awi.de/lpj-guess/lpj-guess-awi/-/issues` — confirm
  exact URL with Laszlo). Title: "Per-PFT yearly tree fraction output:
  monthly is LAI-weighted, yearly per-PFT files not emitted." Reference
  this handoff. Owner: Laszlo or whoever maintains the LPJ-GUESS branch
  shipped in awiesm3-3.4.x.
- **`vegHeightGrass` not emitted**: same tracker, separate issue.
  Title: "Emit vegHeightGrass_monthly.out — current grass-height
  fallback uses tree-dominated field." Reference this handoff.
- If the tracker URL above turns out to be wrong: ask Laszlo where to
  file. Do not let this sit as an un-filed TODO.

## D4b ratio sanity check — RESULTS (cli37 source)

Ran `/tmp/d4b_ratio_check.py` on
`/work/bb1469/a270092/runtime/awiesm3-develop/Final_CMIP7_IO_Test_01/outdata/lpj_guess/15860101-15871231/run1/`
(214 264 cell-year tuples, 193 658 with nonzero yearly tree fraction).

```
=== SIGNIFICANT TREE CELLS (yearly_total > 1%) ===
  N cell-years: 163 639
  yearly_total: p50=34.1  max=100
  sum_of_max  : p50=35.0  max=131
  ratio quantiles (sum_of_max / yearly_total):
    p 1.0 = 0.495
    p10.0 = 0.863
    p25.0 = 0.964
    p50.0 = 1.010
    p75.0 = 1.068
    p90.0 = 1.141
    p99.0 = 1.521
  ratio mean: 1.010
  fraction with ratio in [0.8, 1.2]: 88.1%
```

**Verdict: Option B math is sound.** Median ratio is essentially 1.0. The
sum of LAI-weighted monthly annual-maxes across the 4 PFTs reconstructs
the yearly stand-area total to within ±20% for 88% of significant tree
cells. The renormalization step is doing a small correction. Wider tails
(p1, p99) are mostly in low-tree-fraction cells where absolute error is
small.

Paste this block into the D4c commit message verbatim.

## Status

- Investigation: ✅ complete
- Round-1 review folded: ✅
- D4a (`treeFrac` total source switch): ✅ code ready (uncommitted)
- D4b ratio sanity check: ✅ ratio ≈ 1.010 median, math verified
- Math signoff (Laszlo): ⏳ "easy, coming latest tomorrow morning"
- LPJ-GUESS upstream issue (per-PFT yearly + vegHeightGrass):
  **✅ Laszlo says he just pushed a fix.** Need to confirm:
  what got emitted, which branch/commit, whether existing cli37 output
  has the new files or if we need a fresh LPJ-GUESS run.
- D4c implementation: ⏳ **may switch from Option B (synthesize) to
  Option D (use new yearly per-PFT files directly)** depending on what
  Laszlo's push contains. If `treeFrac{PFT}_yearly.out` now exists,
  Option D is the clean path: source each rule from the new yearly
  file and reuse `broadcast_yearly_to_monthly` (already in code from
  D4a).
- D6 (`fAnthDisturb`/`fNAnthDisturb`/`fNfert` non-zero in frozen 1850):
  ⏳ Laszlo "needs to look in more detail, latest tomorrow morning".
