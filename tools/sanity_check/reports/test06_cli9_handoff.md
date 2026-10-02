# Pycmor Test_06_cli_y1587_v7 — post-cli9 handoff for follow-up fixes

After the previous round of rule fixes were applied to the run
(cli9 batch), the sanity walker shows mostly-good results but **three
prior handoff fixes did not land** and **two new bugs appeared** in
sea-ice-area-masked radiation variants. This document is for the next
AI to act on those five items.

## Status snapshot

Walked `/scratch/a/a270092/pycmor_hr/Test_06_cli_y1587_v7/cmorized/`
on 2026-05-11. 740 files. Totals: **330 PASS · 258 WARN · 152 FAIL**.

Full HTML report:
[tools/sanity_check/reports/test06_cli_y1587_v7_html/index.html](test06_cli_y1587_v7_html/index.html)

## What landed (verification only — no action required)

These six fixes from the previous handoff
([test06_cli_y1587_v7_handoff.md](test06_cli_y1587_v7_handoff.md))
now show correct values in the cli9 output. They are listed for
audit so the next person can confirm the diagnostic numbers match:

| Variable    | cli7 (broken)                              | cli9 (correct)                              |
|-------------|--------------------------------------------|---------------------------------------------|
| `vsfcorr`   | all-NaN                                    | all-0 (PASS); `nan_to_zero` works           |
| `snm`       | mean −8.8e-6 (wrong sign, includes accum.) | mean +1.9e-6, min ≥ 0 (sign + clip OK)      |
| `siflcondtop` | mean +25 W/m² (wrong sign)               | mean −25 W/m² (positive=down convention)    |
| `masscello` | negative cell mass                         | min +5120, positive everywhere              |
| `pso`       | mean −4.8 kPa (anomaly)                    | mean +96.5 kPa (absolute pressure)          |
| `dsn`       | min −67566 (×10³ unit double-count)        | min −67.6 (correct magnitude)               |

---

## Still open — handoff fixes that did NOT reach the cli9 output

These were fixed in the rule yamls but their cli9 output still shows
the original bug. The most likely explanations are:

1. The yaml change wasn't propagated to the run environment.
2. The rule was not re-run for these variables specifically.
3. A workflow-cache / Prefect cache returned the pre-fix result.

The first action for each is: **confirm the rule yaml on disk in the
run environment matches the committed yaml**, then **re-run the
specific rule** with cache disabled.

### S1. `siarea` / `siextent` (monthly) — unit still off by 10¹²

File pattern: `lrcs_seaice/siarea_tavg-u-hm-u_mon_{nh,sh}_*.nc`,
`lrcs_seaice/siextent_tavg-u-hm-u_mon_{nh,sh}_*.nc`.

Observed cli9 (NH, monthly):
- min `5.43e+12`, mean `1.07e+13`, max `1.51e+13` (declared units `1`)

Expected (CMIP `1e6 km2`): min ~4, mean ~11, max ~16.

Rule fix that should have been picked up (in
`awi-esm3-veg-hr-variables/lrcs_seaice/cmip7_awiesm3-veg-hr_lrcs_seaice.yaml`):

```yaml
- name: siarea_north
  ...
  model_unit: "1e12 m2"   # this line was added — verify it's present
```

(Same for `siarea_south`, `siextent_north`, `siextent_south`.)

**Action:**
1. Check that the yaml on disk in the run environment has the
   `model_unit: "1e12 m2"` line on all four rules.
2. If present, the issue is probably the units pipeline — verify pint
   resolves `"1e12 m2"` as a valid unit string. If not, swap to a
   custom step (e.g. `scale_by_constant(1e-6)` to go from m² to km²
   × 10⁶) or change the declared CMIP unit to `m2`.
3. Re-run with caching disabled.

The daily versions (`siarea_*_day_*`) use a different rule path
(`hemisphere_integral_pipeline` with `a_ice` integration) and are
already correct.

### S2. `sweLut` — Snow Water Equivalent on Land-Use Tile (m vs kg m⁻²)

File: `veg_land/sweLut_tavg-u-hxy-multi_mon_glb_gn_..._{1586,1587}.nc`.

Observed cli9: mean **0.23**, max **10** (declared units `m`).

Expected (CMIP `m` = liquid water equivalent thickness):
mean ~0.02, max ~3 (GlobSnow / ERA5 SWE in m).

The numbers are 100× too big — looks like the rule emits in `kg m-2`
(SWE × 1000 kg/m³) and labels it as `m`. The previous handoff
recommendation was to add `source_units: kg m-2` so pint converts.

**Action:**
1. Open the `sweLut` rule in `awi-esm3-veg-hr-variables/veg_land/cmip7_awiesm3-veg-hr_land.yaml`.
2. Verify whether `source_units: kg m-2` (or equivalent) was added.
3. If present and still broken, the pipeline may need an explicit
   `scale_by_constant(0.001)` step or the model variable may not be
   what the rule thinks it is. Inspect the source JSBACH file.

### S3. `sisnmass` — Snow on Sea-Ice Mass, hemispheric scalar

File pattern: `lrcs_seaice/sisnmass_tavg-u-hm-u_mon_{nh,sh}_*.nc`.

Observed cli9 (NH): min 1.6e11, mean 2.0e12, max 3.9e12 (units `kg`)
— this is roughly **1000× too big** vs expected ~2e15 / 2e16 / 1e17?

Wait — checking the bounds: `sisnmass` is the **total hemispheric
mass of snow on sea ice** in kg. NH ~2 × 10¹⁵ kg is the typical
expectation (snow ~30 cm × ice extent ~10¹² m² × 300 kg/m³).

Observed mean 2e12 is **1000× too small**, not too big. Likely a unit
issue or a missing integration step.

**Action:**
1. Compare with the gridded `sisnmass_*_mon_glb_*` rule (per-cell
   snow-on-ice mass in kg/m²) — that one mean is ~30 kg/m² locally,
   which is correct; integrate over hemispheric area gives ~2×10¹⁵
   kg. So the gridded version is fine; the hemispheric scalar is
   broken.
2. Look at the rule that produces `sisnmass_tavg-u-hm-u_mon_nh`. Is
   it integrating from gridded data or pulling from a FESOM
   ldiag_cmor scalar? If the latter, check its FESOM unit string —
   may be in a non-standard scaling (analogous to `siarean` =
   `1e12 m²` situation).

---

## New issues — sea-ice-masked radiation all-NaN

cli9 introduced new sea-ice-masked variants of `rlds` and `rsus`
(probably to populate the CMIP7 sea-ice variables `rlds_seaIce` etc.
that need surface radiation over the ice fraction). Two of them
output all-NaN:

### N1. `rlds_tavg-u-hxy-si_mon_glb` — all-NaN

File: `lrcs_seaice/rlds_tavg-u-hxy-si_mon_glb_gn_..._158701-158712.nc`.

Observed: all 12 monthly values are NaN.

But the day-frequency sibling `rlds_tavg-u-hxy-si_day_glb` (if it
exists) and the other `rlds` variants (`hxy-u` global) are fine.

**Action:**
1. Open the rule producing this file (look for `compound_name:
   seaIce.rlds.tavg-u-hxy-si.mon.GLB` in
   `awi-esm3-veg-hr-variables/lrcs_seaice/cmip7_awiesm3-veg-hr_lrcs_seaice.yaml`).
2. The rule likely uses `regrid_atm_to_fesom_seaice_mask_pipeline` (or
   a variant): IFS `rlds` regridded to FESOM nodes then masked by
   `a_ice`. Check whether the time-averaging step happens before or
   after the mask. If the mask is fully applied at all timesteps and
   produces no overlap with the monthly mean's denominator, you can
   end up with NaN.
3. Inspect why the daily variant works but the monthly doesn't —
   maybe the monthly rule has a wrong input pattern (matching no
   files) or a wrong masking strategy.

### N2. `rsus_tavg-u-hxy-si_day_glb` — all-NaN

File: `lrcs_seaice/rsus_tavg-u-hxy-si_day_glb_gn_..._15870101-15871231.nc`.

Same as N1, but for daily upwelling shortwave. The monthly variant
of `rsus` (`hxy-si_mon`) IS finite (just WARN), so the issue is
specific to the daily file.

**Action:** identical to N1 but for the daily rule.

---

## How to verify each fix

```bash
# (Re)-cmorise the affected rules with cache disabled:
PYCMOR_DISABLE_CACHE=1 pycmor process \
    awi-esm3-veg-hr-variables/lrcs_seaice/cmip7_awiesm3-veg-hr_lrcs_seaice.yaml \
    awi-esm3-veg-hr-variables/veg_land/cmip7_awiesm3-veg-hr_land.yaml

# Walk the new output (the walker resumes on existing JSONL, so wipe
# the JSONL first to force a fresh walk):
rm tools/sanity_check/reports/test06_cli_y1587_v7.jsonl
NPROC=12 python tools/sanity_check/sanity_check.py \
    --root /scratch/a/a270092/pycmor_hr/Test_06_cli_y1587_v7/cmorized \
    --jsonl tools/sanity_check/reports/test06_cli_y1587_v7.jsonl

# Render HTML
python tools/sanity_check/build_html_report.py \
    --jsonl tools/sanity_check/reports/test06_cli_y1587_v7.jsonl \
    --out-dir tools/sanity_check/reports/test06_cli_y1587_v7_html

# Expected state after S1+S2+S3 fixes:
#   siarea / siextent     PASS (mean ~10-12, units 1e6 km2)
#   sweLut                PASS (mean ~0.02, max ~3, units m)
#   sisnmass              PASS (mean ~2e15 kg NH)
#   rlds_si_mon           PASS or WARN (finite values, correct units)
#   rsus_si_day           PASS or WARN (finite values, correct units)
```

If any of these still fail after the rule re-run, post the new
observed numbers — they'll tell us whether the rule fix was wrong
(numerical answer) or never applied (identical to cli9).
