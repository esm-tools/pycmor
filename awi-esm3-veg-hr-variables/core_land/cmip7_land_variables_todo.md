# CMIP7 Core Land Variables — Rule Implementation TODO

Variables from 3 CSVs: `cmip7_all_core_variables_land.csv` (13), `cmip7_all_core_variables_atmos_land.csv` (2), `cmip7_all_core_variables_landIce_land.csv` (3). Total: 18 rows, 17 unique CMOR variables.
11 implementable from IFS output, 6 deferred to lrcs_land (need LPJ-GUESS or external data).

XIOS field definitions: `field_def_cmip7.xml`
Output config: `file_def_oifs_cmip7_spinup.xml.j2`
Pycmor rules: `cmip7_awiesm3-veg-hr_land.yaml`

## Key conversion patterns

- IFS accumulated fields (m or J m-2) need deaccumulation: ÷21600 for 6h freq_op
- Volumetric soil moisture (m3 m-3) → kg m-2 via ×layer_thickness×1000
- IFS evaporation `e` is negative for actual evaporation → sign flip
- HTESSEL soil layers: 0.07m, 0.21m, 0.72m, 1.89m (total 2.89m)

---

## Monthly land (Lmon) — IFS-producible

### XIOS CMOR-ready (derived fields in field_def)

- [x] **evspsbl** — Evaporation Including Sublimation (`kg m-2 s-1`, Amon) — XIOS expr: `-1000*e/21600`
- [x] **mrro** — Total Runoff (`kg m-2 s-1`, Lmon) — XIOS expr: `1000*ro/21600`
- [x] **mrros** — Surface Runoff (`kg m-2 s-1`, Lmon) — XIOS expr: `1000*sro/21600`
- [x] **snw** — Surface Snow Amount (`kg m-2`, LImon) — XIOS expr: `sd*1000`
- [x] **lai** — Leaf Area Index (`1`, Lmon) — XIOS expr: `lai_lv*cvl + lai_hv*cvh`
- [x] **mrso** — Total Soil Moisture Content (`kg m-2`, Lmon) — XIOS expr: `1000*(swvl1*0.07 + swvl2*0.21 + swvl3*0.72 + swvl4*1.89)`
- [x] **mrsol** — Upper 10cm Soil Moisture (`kg m-2`, Lmon) — XIOS expr: `1000*(swvl1*0.07 + swvl2*0.03)`

### pycmor pipeline computed

- [x] **snc** — Snow Area Fraction (`%`, LImon) — pycmor pipeline: parametric from `sd` (saturation at 15mm water equiv)

## Fixed (fx) — IFS-producible

- [x] **orog** — Surface Altitude (`m`, fx) — XIOS expr: `sz/9.80665`
- [x] **areacella** — Grid-Cell Area (`m2`, fx) — pycmor pipeline: computed from grid coordinates
- [x] **slthick** — Soil Layer Thickness (`m`, fx) — pycmor pipeline: constant [0.07, 0.21, 0.72, 1.89]

## Deferred to lrcs_land (need LPJ-GUESS or external data)

- [x] **evspsblsoi** — Soil Evaporation (`kg m-2 s-1`, Lmon) — Implemented in `lrcs_land` from LPJ-GUESS `evspsblsoi_monthly.out`
- [x] **evspsblveg** — Canopy Evaporation (`kg m-2 s-1`, Lmon) — Implemented in `lrcs_land` from LPJ-GUESS `evspsblveg_monthly.out`
- [x] **rootd** — Maximum Root Depth (`m`, fx) — Implemented in `lrcs_land` via `rootd_pipeline` (Zeng 1998 weighted effective depth from `tvl`/`tvh`)
- [x] **mrsofc** — Soil Field Capacity (`kg m-2`, fx) — Implemented in `lrcs_land` via `mrsofc_pipeline` (Van Genuchten params from IFS soil type)
- [x] **sftgif** — Glacier Area Fraction (`%`, fx) — Implemented in `lrcs_land` via `sftgif_pipeline` (IFS vegetation type 12)
- [x] **mrfso** — Frozen Soil Water Content (`kg m-2`, LImon) — Implemented in `lrcs_land` from LPJ-GUESS `mrfso_monthly.out`

---

## OIFS source code investigation (2026-04-06)

Of the 6 variables deferred to lrcs_land, OIFS source analysis shows:

### No source changes needed (derivable from existing output)
- **rootd** — Per-veg-type root profiles in `srfrootfr_mod.F90` (Zeng 1998). Compute weighted effective depth from `tvl`/`tvh` + lookup table
- **mrsofc** — Field capacity `RWCAP`/`RWCAPM` in `sussoil_mod.F90` from Van Genuchten params. Derive from IFS soil type initial conditions
- **sftgif** — IFS vegetation type 12 = "Ice Caps and Glaciers". Derive from `tvl`/`tvh` fields

### Need GRIB field registration (moderate OIFS source changes)
- **evspsblsoi** — Bare soil evaporation `PDHWLS(:,1,9)` in `srfwexc_mod.F90`. Wire to XIOS via `ptrgfu.F90` + `sucfu.F90` + `cpg_dia.F90`
- **evspsblveg** — Transpiration already available as GRIB field `SURFTRANSPIRATIO`. Interception evaporation `PDHIIS(:,4)` needs registration
- **mrfso** — Frozen soil water `PDHWLS(:,:,2)` in `srfwexc_mod.F90`. Sum over 4 layers and register as GRIB field

See detailed notes in `../lrcs_land/cmip7_lrcs_land_todo.md`.

## Blockers / verification needed

1. **XIOS multi-field expressions** — mrso (4 fields), lai (4 fields), mrsol (2 fields) use multi-field XIOS expressions. Verify these work at runtime
2. **sro field** — Added to file_def monthly output. Verify IFS/FullPos outputs surface runoff separately
3. **sz field** — Added to file_def. Verify surface geopotential is output correctly
4. **lai_lv, lai_hv, cvl, cvh** — Uncommented in file_def monthly output. Were previously disabled

## Research findings

- HTESSEL soil layer thicknesses: 0.07, 0.21, 0.72, 1.89 m (total 2.89m)
- IFS evaporation field `e` has negative sign convention (evaporation from surface is negative)
- IFS `sd` is snow depth in metres of water equivalent, not physical depth
- IFS `ro` includes both surface and subsurface runoff; `sro` is surface only
- `sz` is surface geopotential (m2 s-2), needs division by g for altitude
- LAI requires weighting low/high veg LAI by vegetation cover fractions
- Upper 10cm soil moisture approximation: full layer 1 (7cm) + top 3cm of layer 2
