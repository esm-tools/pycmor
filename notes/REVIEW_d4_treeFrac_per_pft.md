# Review: HANDOFF_d4_treeFrac_per_pft.md

## Verdict: solid investigation; Option B math needs three edge-case checks

The investigation is properly closed: reviewer claim (Christian:
"shouldn't have annual cycle, LAI involved?") confirmed by tracing
the rule pipeline (it's a pass-through loader → cmor output IS the
LAI-weighted monthly) and verifying the inventory (4 per-PFT yearly
files don't exist in LPJ-GUESS output). The four-option matrix with
pros/cons is the right level of detail for a handoff that wants a
decision.

Option B is the recommendation. The math premise ("annual max of
LAI-weighted monthly ≈ stand area") is plausible but has three
unaddressed edge cases the implementer will hit. Plus a few smaller
items.

---

## 1. Option B — per-cell vs global normalization is unstated

The formula:

```
proportion_PFT(year) = max_over_months(treeFrac{PFT}_monthly)
                       / sum_over_PFTs(max_over_months(treeFrac{PFT}_monthly))
```

implicitly indexed by grid cell, but the doc never says so. If
applied globally (across all cells), grid cells with 90% Bdl + 10%
Ndl and cells with 10% Bdl + 90% Ndl would average out wrong.

The right intent is per-cell normalization — every grid cell computes
its own proportions across the 4 PFTs from that cell's monthly maxes.
Worth one explicit line: "Per grid cell, compute the proportion as
the cell's PFT-max divided by the cell's sum-of-PFT-maxes."

---

## 2. Option B — divide-by-zero when no trees in a cell

If a grid cell has zero tree fraction across all four PFTs (e.g.,
ocean, desert, ice), `sum_over_PFTs(max_over_months(...)) == 0`. The
proportion formula divides by zero. The implementation sketch
doesn't handle this.

Recommended: `where sum_of_max == 0, proportion = 0` (and the cell's
per-PFT output is also 0, consistent with the cell having no trees).
Add to the implementation sketch.

---

## 3. Option B — should verify against authoritative yearly total

The recommendation uses `treeFrac_yearly.out` as the authoritative
total, then splits it by the LAI-weighted-max proxy. But: does the
sum of the four LAI-weighted monthly maxes (annual max of each)
roughly equal the yearly total per cell? If yes, the renormalization
is a clean re-split. If not, there's a systematic conceptual
mismatch (LPJ-GUESS might compute the yearly total via a different
mechanism — e.g., excluding sapling stages, including pasture-trees
separately, area-weighting differently).

A 5-minute pre-implementation check on cli37 data:

```python
sum_of_max = sum(monthly[pft].max(dim='time') for pft in PFTS)
yearly_total = open('treeFrac_yearly.out')
ratio = sum_of_max / yearly_total  # should be close to 1, ideally
```

If `ratio` is consistently ~1 across cells, Option B math is sound.
If `ratio` is consistently e.g. ~0.8 (LAI-weighting underestimates
stand area), then the renormalization step is doing real work — fine
but worth documenting. If `ratio` varies wildly (0.3 to 1.5), the
LAI-max-as-stand-area proxy is bad and Option B should be
reconsidered.

This check is cheap and answers whether Option B is defensible
before any code is written.

---

## 4. Implementation sketch — body is `...`; flesh the core math

The function signature is given but the body is omitted. For a
handoff that's pending implementation-decision, OK; but the math has
enough subtleties (per-cell normalization, divide-by-zero, sibling-
file resolution) that pseudocode would catch issues earlier.

A 10-line pseudocode adding:

```python
def load_lpjguess_tree_pft_yearly_from_total(data, rule):
    pft = rule.tree_pft  # one of BdlDcd, BdlEvg, NdlDcd, NdlEvg
    lpjg_dir = rule.inputs[0].path  # discover siblings here
    yearly_total = load_lpjg(lpjg_dir / "treeFrac_yearly.out")
    pft_maxes = {p: load_lpjg(lpjg_dir / f"treeFrac{p}_monthly.out").groupby("year").max() for p in PFTS}
    sum_of_max = sum(pft_maxes.values())
    proportion = xr.where(sum_of_max > 0, pft_maxes[pft] / sum_of_max, 0)
    yearly_pft = yearly_total * proportion
    # broadcast yearly value to monthly cadence
    return yearly_pft.expand_dims(month=12).stack(time=("year","month"))
```

…lifts the open questions (sibling-file finding, divide-by-zero,
broadcast mechanics) into a single block the implementer can refine.
The current sketch defers all of these.

---

## 5. Adjacent issue — land `treeFrac` (total) source switch FIRST

> `treeFrac` (total): source switch monthly → yearly, broadcast.
> Simple, can land independently of the per-PFT workaround.

Recommend landing this *before* the Option B work, not after. Reasons:

- Confirms the yearly → monthly broadcast machinery on a simpler rule
- Validates that `treeFrac_yearly.out` is actually well-formed and
  loader-compatible before depending on it in Option B
- Closes the simpler half of the issue immediately, reducing the
  cognitive load on the per-PFT work

If `treeFrac` total broadcast fails for some unforeseen reason
(file format, time-coord convention), Option B's math is moot until
that's resolved.

---

## Smaller items

### vegHeight escalation needs a concrete next step

> needs LPJ-GUESS to emit `vegHeightGrass_monthly.out`. Document as
> known issue, escalate to model team.

"Escalate" → to whom? GitLab issue? Email Laszlo? A concrete owner +
channel makes this trackable. Otherwise it sits in this doc as a
TODO that never gets filed.

### LPJ-GUESS upstream issue status

> LPJ-GUESS upstream issue: ⏳ should be filed regardless of pycmor
> choice

Same as vegHeight — concrete "filed where" needed. The cleanest
version: "File issue at [LPJ-GUESS issue tracker URL] referencing
this handoff doc."

### Metadata `comment` wording

The handoff says Option B "needs ... a `comment` attribute in the
cmor file documenting the derivation" but doesn't draft it. Pre-
drafting saves a reviewer cycle. Suggested:

```
comment: "Per-PFT tree fraction synthesized from authoritative
    annual total (treeFrac_yearly.out) and PFT-relative annual-max
    proxy (treeFrac{PFT}_monthly.out), using
    pycmor.load_lpjguess_tree_pft_yearly_from_total step. LPJ-GUESS
    does not emit per-PFT yearly stand-area fractions directly; this
    derivation is documented in HANDOFF_d4_treeFrac_per_pft.md.
    Pending model-team fix for native per-PFT yearly emission."
```

### CMIP7 timeline assumption

Option D ("wait for LPJ-GUESS fix") is dismissed because it "blocks
shipping." But blocks shipping for how long? If the LPJ-GUESS fix is
3 months out and CMIP7 ships in 2 years, Option D becomes viable. If
the model fix is 18 months out and CMIP7 ships in 6 months,
Option B is mandatory.

Worth a one-liner: "Recommended Option B because LPJ-GUESS upstream
fix is estimated [timeline] and CMIP7 deadline is [timeline]; Option D
viable if fix lands within X."

---

## Strong points worth calling out

- **Two-reviewer-corroborated evidence** (Christian's qualitative
  flag → Laszlo's specific mechanism hypothesis → file inventory
  confirms) is the right epistemic structure.
- **File inventory is concrete** — "treeFrac yearly exists, the four
  per-PFT yearly don't" is a verifiable fact, not a guess. Closes
  the question of "why didn't we just do Option D?"
- **Four options laid out** with pros/cons forces a real decision
  rather than vague "TBD."
- **Recommendation is principled** — uses the authoritative number
  where it exists, falls back to a best-available proxy elsewhere,
  acknowledges the limitation in metadata.
- **Adjacent issues separated** (vegHeight is a different problem;
  treeFrac total is a simpler subproblem) keeps the per-PFT work
  scoped.
- **Status table** at the end gives an at-a-glance read of where
  things stand and what's outstanding.

---

## Bottom line

Three substantive items for the implementer:

1. **Sanity-check Option B math** before coding: compute
   `sum_of_max / yearly_total` per cell on cli37 data; if it's
   roughly 1 (or consistently ~0.8), Option B is sound. If it varies
   wildly, reconsider (§3).
2. **Per-cell normalization + divide-by-zero** need to be in the
   implementation sketch explicitly (§1, §2).
3. **Land `treeFrac` (total) source switch FIRST**, then per-PFT
   work (§5).

Plus polish: flesh the implementation sketch (§4), concrete
escalation channels for vegHeight + LPJ-GUESS upstream issue,
pre-draft the `comment` attribute, state the CMIP7-vs-LPJG-fix
timeline assumption.

Investigation is otherwise complete. Math sign-off by Laszlo (already
flagged in status) is the gating step before implementation begins.
