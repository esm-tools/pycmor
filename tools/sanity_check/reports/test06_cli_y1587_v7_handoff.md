# Pycmor Test_06_cli_y1587_v7 — handoff for follow-up fixes

This file lists the **real bugs** flagged by the sanity-check walker on
`/scratch/a/a270092/pycmor_hr/Test_06_cli_y1587_v7/cmorized/` that warrant
investigation in the rule yamls / compute steps. Bounds-too-tight FAILs and
piControl-residual FAILs are not in scope here — see the broader categorised
report at [test06_cli_y1587_v7.md](test06_cli_y1587_v7.md) for those.

The HTML report (`test06_cli_y1587_v7_html/`) has per-variable cards with map
plots, expected vs observed values, the bounds-table source, and an
auto-generated diagnosis paragraph.

## Already applied in this commit

These four bugs were investigated and fixed in the rule yamls. **Verify the
fixes hold by re-running the rules and re-running the sanity walker.**

### A — `snm` (Sea-ice snow melt rate) — sign inverted

* **File**: `awi-esm3-veg-hr-variables/lrcs_seaice/cmip7_awiesm3-veg-hr_lrcs_seaice.yaml`
* **Symptom**: card mean = −8.8e-6 vs CMIP7 expected +1e-6 (positive when
  melting). Time-mean map shows mostly negative values over Antarctic /
  Arctic snow-on-sea-ice — i.e. the field has the **wrong sign**.
* **Root cause**: FESOM's `thdgrsnw` is the **net** thermodynamic snow
  thickness change at each step (`thdgrsnw > 0` ⇒ snow accumulates,
  `thdgrsnw < 0` ⇒ snow melts). CMIP `snm` is **positive** when the snow
  pack is melting and zero otherwise. The old rule scaled by `+330` (ρ_snow)
  and kept the FESOM sign, so melt came out negative and snow accumulation
  episodes leaked into the snm record with the wrong sign.
* **Applied fix**:
  - `scale_factor: 330.0` → `scale_factor: -330.0` (sign-flip at the source).
  - Switch `pipelines: [scale_pipeline]` → `[snm_pipeline]`, a new pipeline
    that adds a `clip_negative_to_zero` step after the scale (so snow
    accumulation, which becomes negative after the sign flip, is zeroed
    out and only true melt survives).
  - New custom step `clip_negative_to_zero` lives in
    `examples/custom_steps.py`.
  - Validation: post-fix, the time-mean map should show large positive
    values over high-latitude summer melt and 0 over winter accumulation
    regions.

### B — `vsfcorr` (Virtual Salt Flux Correction) — all NaN

* **File**: `awi-esm3-veg-hr-variables/lrcs_ocean/cmip7_awiesm3-veg-hr_lrcs_ocean.yaml`
* **Symptom**: card reports `n_finite = 0/37761132` — every value is fill.
* **Root cause**: CMIP7 documents this variable as *"set to zero in models
  which receive a real water flux"*. AWI-CM is such a model
  (`surf_relax_s = 0` in `namelist.tra`), so FESOM does not actually write
  data to the `relaxsalt` stream — the source `relaxsalt.fesom.YYYY.nc` is
  itself all `_FillValue` (verified). Pycmor faithfully passed the fill
  through, but CMIP wants the field to be a constant zero in this case.
* **Applied fix**:
  - New custom step `nan_to_zero` (in `examples/custom_steps.py`) that
    replaces NaN/fill with 0 **only** where the source is non-finite —
    real values pass through unchanged. This way the rule still acts as
    a sanity check if `surf_relax_s` is ever turned on.
  - New pipeline `nan_to_zero_pipeline` in lrcs_ocean.yaml (clone of
    `surface_2d_pipeline` with `nan_to_zero` inserted after `get_variable`).
  - vsfcorr rule now declares `pipelines: [nan_to_zero_pipeline]`.
  - Validation: post-fix, the field is exactly 0 everywhere for AWI-CM
    coupled runs.

### C — `siarea` / `siextent` (Hemispheric sea-ice area / extent) — units 1e12× off

* **File**: `awi-esm3-veg-hr-variables/lrcs_seaice/cmip7_awiesm3-veg-hr_lrcs_seaice.yaml`
* **Symptom**: NH-monthly card reports mean = 1.07e13 (cf. expected
  ~11), max = 1.51e13 (cf. ~16), units field = `"1"`.
* **Root cause**: FESOM's ldiag_cmor scalars (`siarean`, `siareas`,
  `siextentn`, `siextents`) declare `units = "1e12 m2"` in the source NC
  file — mathematically identical to CMIP's `1e6 km2`, but lexically
  different. The four monthly rules did not declare `model_unit`, so
  pycmor's pint pass parsed `"1e12 m2"` as `1e12 × m²`, multiplied
  values through to bare m² (≈1e13), and tagged the result with a
  bogus `"1"` unit string.
* **Applied fix**: added `model_unit: "1e12 m2"` to the four rules
  (`siarea_north`, `siarea_south`, `siextent_north`, `siextent_south`).
  pint now treats the FESOM↔CMIP unit pair as a no-op identity.
  Validation: post-fix the values land in `[0, 16]` with units
  `1e6 km2`, matching the daily rules (which already use the gridded
  `a_ice` integration path and were correct).

## Still open from earlier work (already documented)

These bugs were flagged in earlier reports and are not in scope here, but
listed for completeness. See
[issues_y1587_sanity.md](../../issues_y1587_sanity.md) (committed earlier)
or the current HTML report for full per-variable cards.

| Variable | Symptom | Likely fix |
|---|---|---|
| `sweLut` | values ~1000× too big; declares `m` but field is in `kg m-2` | declare `source_units: kg m-2` so pint converts ÷1000 |
| `pso` | mean ≈ 0 instead of ≈ 1.013e5 Pa; saved as anomaly | compute step needs `ρ·g·η + p_atm`, not just the SSH-derived anomaly |
| `masscello` | negative cell-mass-per-area (impossible) | sign or differencing bug in the rule's compute step |
| `thkcello` | negative cell thickness (impossible) | same family of bug as masscello |
| `ua` (6hr) | one timestep file all-NaN | check XIOS file_def; may be an init-time / output-frequency mismatch |

## How to verify the fixes in this commit

```bash
# Re-run the snm, vsfcorr, siarea_*, siextent_* rules
pycmor process awi-esm3-veg-hr-variables/lrcs_seaice/cmip7_awiesm3-veg-hr_lrcs_seaice.yaml
pycmor process awi-esm3-veg-hr-variables/lrcs_ocean/cmip7_awiesm3-veg-hr_lrcs_ocean.yaml

# Re-walk the new output
NPROC=12 python tools/sanity_check/sanity_check.py \
    --root <new-output-root> \
    --jsonl /tmp/sanity_check_post_fix.jsonl

# Render the HTML
python tools/sanity_check/build_html_report.py \
    --jsonl /tmp/sanity_check_post_fix.jsonl \
    --out-dir tools/sanity_check/reports/post_fix_html

# In the new HTML, the four variables should:
#   * snm           -> PASS (mean ~1e-6 kg m-2 s-1, positive everywhere)
#   * vsfcorr       -> PASS (constant 0)
#   * siarea (mon)  -> PASS (mean ~10, max ~16, units 1e6 km2)
#   * siextent (mon)-> PASS (mean ~12, max ~16, units 1e6 km2)
```

If any of these still flag FAIL after the rule re-run, the symptom + new
diagnostic numbers should be re-investigated against this document.
