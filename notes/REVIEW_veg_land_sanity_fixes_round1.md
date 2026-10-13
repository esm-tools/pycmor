# Review: PLAN_veg_land_sanity_fixes.md

## Verdict: well-organized punch list; tighten six operational items

Phase structure is right — bounds-only (A) vs clip-band noise (B) vs
sign-fix (C) vs investigations (D) vs verification (E). Clear revert
path for each. Open-questions section at the bottom is good plan
hygiene. No architectural issues; the suggestions below are
operational polish.

---

## 1. Phase A — bounds change rationales need a traceability trail

The table cells give one-line reasons ("Central America wet-tropics
drainage spikes," "LUH2 1850 has ~10% cropland + ~20% pasture") but
no citation back to:

- The specific reviewer comment that confirmed this is real, not
  a pycmor bug
- The sanity-report row that shows the failing data centered around
  the proposed new bound

Without that trail, a future maintainer reading just the bounds doc
sees "bounds were loosened" and doesn't know whether the loosening
was data-driven (good) or hand-waved (bad).

**Fix**: cite the reviewer + the diff sample per row. E.g.,
"nep: Laszlo confirmed Central America spike matches raw .out (see
review note dated 2026-MM-DD); new max 2e-7 brackets the 99.9th
percentile of cli37 output."

---

## 2. Phase A — fAnthDisturb / fHarvestToAtmos / fNAnthDisturb / fNfert all get identical bounds

Four variables get the same `0 / ~1e-10 / ~1e-8` bounds with the
same rationale ("LUH2 1850 state re-applied every year"). Plausible
if they truly share the same magnitude distribution, but worth
confirming each variable's actual data centers on these numbers
rather than copying the bounds across.

A quick `python -c "import xarray; print(xr.open_dataset(f).quantile([0.5, 0.99, 0.9999]))"`
per variable would verify. Otherwise risk: one of the four genuinely
sits at e.g. ~1e-9 mean, and we'd loosen too much, masking a
different real issue.

---

## 3. Phase B — clip threshold semantics need spelling out

> wire in a small clip step `clip_small_negatives(threshold=1e-12)` from
> `examples/custom_steps.py`

Reviewer mentions "~1e-10 negative values" in the raw `.out`. With
threshold=1e-12, values between -1e-10 and -1e-12 are still
negative — the clip doesn't actually clear the noise the reviewer
flagged.

Two interpretations of `threshold`:

- **"Drop magnitudes smaller than threshold to zero"**: needs
  threshold >= 1e-10 to catch the reviewer's noise.
- **"Clip negative values whose magnitude is less than threshold"**:
  needs threshold >= 1e-10 too.

Either way, 1e-12 looks like a typo for 1e-10 (or 1e-9 for safety
margin). Verify against the actual `.out` noise distribution before
committing.

If the function signature is something else (e.g. "set values in
[-threshold, +threshold] to zero"), the plan should say so
explicitly so the reviewer can confirm.

---

## 4. Phase C — `clip(min=0)` needs per-variable physical-floor confirmation

> Variables: `mrsol`, `rhSoil`. Implementation: ... add `clip(min=0)`.

`clip(min=0)` blindly floors negatives at zero. For:

- **mrsol** (soil moisture content): yes, 0 is physical floor.
  Confirmed.
- **rhSoil** (heterotrophic soil respiration): typically ≥0 by
  convention but in principle could be negative if soil is
  net-uptaking C (rare but real edge case in some seasonal
  regimes).

Worth a one-liner per variable: "rhSoil: per CMIP convention,
floored at 0; small negative noise is a known LPJ-GUESS artifact
documented in [link/reference]." Otherwise the next reviewer who
sees `clip(min=0)` wonders if real physical signal got clipped.

If the reviewer who flagged this confirmed the negative values are
purely numerical (not physical), cite that confirmation in the plan.

---

## 5. vegHeight appears in both Phase A and Phase D5

Phase A table row: "vegHeight ... unchanged; per Christian: rationale
text wrong (trees vs grass mixed) — fix wording only."

Phase D5: "vegHeight — percentiles reach 52m, well above bamboo. ...
trees and grasses likely mixed in the analysis/reference, or the rule
mis-handles the PFT dimension."

Two different actions on the same variable, not clearly separated:

- Phase A: fix the prose explaining the bounds (no code change)
- Phase D5: investigate whether the rule's PFT handling is wrong

If D5 reveals the rule IS averaging wrong, Phase A's "unchanged
bounds" might also be wrong. Worth saying explicitly: "Phase A
vegHeight is rationale-text fix only; bounds + rule revisit pending
D5 outcome — if D5 finds a real rule bug, both bounds and rule will
need follow-up."

---

## 6. D4 `treeFracNdlDcd` is CRITICAL; should run BEFORE Phase A bounds tuning

D4 flags an annual cycle in tree fraction as scientifically wrong.
If the rule is buggy, the bounds tuning in Phase A is fitting the
bound to bad data — which compounds the original bug by making it
invisible to sanity checks.

Recommend: run D4 investigation **before** Phase A. If the rule is
broken, fix it first; Phase A bounds-tuning then operates on
corrected data. If D4 shows the rule is fine and the annual cycle
has another cause, Phase A proceeds unchanged.

The plan's current order (A → B → C → D parallel → E) defers D4 to
after A bounds land — backwards for the highest-risk finding.

---

## Smaller items

### D1 variables need magnitude indication

> D1. `esn`, `sbl`, `nppLut` — cmor min/max do not match raw

Worth saying by how much for each variable. A 5% mismatch is one
class of bug (likely a rounding/weighting issue); a 10× mismatch is
another (likely an aggregation step entirely wrong). The implementer
prioritizes differently based on magnitude.

### Open Q3 deserves a recommendation

> Phase B/C clip implementation: prefer a step in
> `examples/custom_steps.py` referenced from the rule yaml, or
> inline in the loader?

The plan notes "custom_steps already has the lpjg loaders" —
that's a strong hint toward the per-step approach. Worth committing
to a recommendation: "Recommend custom_steps.py step, referenced
per-rule; consolidates LPJG-specific logic in one place."

### Phase E re-run cadence

> After A, B, C land: re-run sanity_check on test06 output.

D items aren't included in the Phase E re-run cadence. If D fixes
land separately (per plan: "each is a separate pass"), each D fix
deserves its own sanity-check re-run, not just the A+B+C bundle.

Worth a note: "Phase E re-run after every meaningful fix lands, not
just once after A+B+C."

---

## Strong points worth calling out

- **Phase separation by action type** (bounds vs clip vs sign-fix vs
  investigate) is the right axis to slice by — each phase has a
  distinct revert mechanism and skill requirement.
- **"Skip in Phase A" cross-reference** for dsn/snd explicitly
  routes them to Phase D rather than silently ignoring.
- **Reviewer attribution per variable** ("per Laszlo," "per
  Christian") preserves the chain of expert input.
- **Per-phase commit discipline** ("A: doc-only, B: rule, C: rule,
  D*: per-variable") makes git history navigable when reading the
  sanity-check evolution later.
- **D4 explicitly flagged CRITICAL** — the prioritization is right,
  it just needs to be reflected in execution order (§6 above).
- **Open questions at the bottom**, including the tsl 100K-vs-150K
  question that pulls the user into the call rather than committing
  to a number the reviewer didn't sign off on.

---

## Bottom line

Three substantive cleanups:

1. **Add traceability** to Phase A bounds changes (reviewer cite +
   data percentile per row).
2. **Resolve Phase B clip threshold** — 1e-12 looks like a typo for
   1e-10; clarify semantics.
3. **Move D4 before Phase A** since it's CRITICAL; bounds-tuning on
   buggy rule output compounds the bug.

Plus polish:

4. Confirm fAnthDisturb-family bounds are per-variable correct, not
   just inherited from one (§2).
5. Add physical-floor confirmation per Phase C variable (§4).
6. Clarify vegHeight's two-place treatment (§5).
7. Add magnitude info to D1 variables (smaller).
8. Recommend custom_steps.py for Phase B/C clip (Open Q3).
9. Phase E re-runs per fix, not just per phase-bundle.

Plan is otherwise solid and ready to execute once the user answers
Open Q1 (tsl floor).
