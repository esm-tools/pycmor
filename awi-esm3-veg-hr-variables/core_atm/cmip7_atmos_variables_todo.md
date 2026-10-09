# CMIP7 Core Atmosphere Variables — Rule Implementation TODO

Variables from `cmip7_all_core_variables_atmos.csv` (76 rows, 45 unique CMOR variables).
76 pycmor rules implemented — one per CSV row.

XIOS field definitions: `field_def_cmip7.xml`
Current output config: `file_def_oifs_cmip7_spinup.xml.j2`
Pycmor rules: `cmip7_awiesm3-veg-hr_atmos.yaml`

## Key conversion patterns

IFS accumulated fields (unit J m-2 or m) need **deaccumulation** to become fluxes (W m-2 or kg m-2 s-1).
With XIOS `freq_op="6h"` and `operation="average"`, deaccumulation = divide by 21600 s.
For sub-daily: 3hr → divide by 10800, 1hr → divide by 3600.
XIOS expressions in field_def_cmip7.xml do deaccum/unit-conversion at output time.
Pycmor rules then read the CMOR-ready output and just add metadata + save.

---

## Monthly 2D surface (Amon)

### Direct or near-direct (raw IFS fields, units already match)

- [x] **tas** — Near-Surface Air Temperature (`K`, Amon) — from `2t`
- [x] **ts** — Surface Temperature (`K`, Amon) — from `skt`
- [x] **psl** — Sea Level Pressure (`Pa`, Amon) — from `msl`
- [x] **ps** — Surface Air Pressure (`Pa`, Amon) — from `sp`
- [x] **prw** — Precipitable Water (`kg m-2`, Amon) — from `tcwv`
- [x] **clivi** — Ice Water Path (`kg m-2`, Amon) — from `tciw`
- [x] **clwvi** — Condensed Water Path (`kg m-2`, Amon) — pycmor pipeline: `tclw + tciw`
- [x] **uas** — Eastward Near-Surface Wind (`m s-1`, Amon) — from `10u`
- [x] **vas** — Northward Near-Surface Wind (`m s-1`, Amon) — from `10v`

### XIOS-converted (CMOR-ready from derived fields)

- [x] **clt** — Total Cloud Cover (`%`, Amon) — XIOS expr: `tcc*100`
- [x] **pr** — Precipitation (`kg m-2 s-1`, Amon) — XIOS expr: `tp*1000/21600`
- [x] **prc** — Convective Precipitation (`kg m-2 s-1`, Amon) — XIOS expr: `cp*1000/21600`
- [x] **prsn** — Snowfall Flux (`kg m-2 s-1`, Amon) — XIOS expr: `sf*1000/21600`

### Radiation — TOA (XIOS-converted)

- [x] **rsdt** — TOA Incoming Shortwave (`W m-2`, Amon) — XIOS expr: `tisr/21600`
- [x] **rsut** — TOA Outgoing Shortwave (`W m-2`, Amon) — XIOS expr: `(tisr-tsr)/21600`
- [x] **rsutcs** — TOA Outgoing SW Clear-Sky (`W m-2`, Amon) — XIOS expr: `(tisr-tsrc)/21600`
- [x] **rlut** — TOA Outgoing Longwave (`W m-2`, Amon) — XIOS expr: `-ttr/21600`
- [x] **rlutcs** — TOA Outgoing LW Clear-Sky (`W m-2`, Amon) — XIOS expr: `-ttrc/21600`

### Radiation — Surface (XIOS-converted)

- [x] **rsds** — Surface Downwelling Shortwave (`W m-2`, Amon) — XIOS expr: `ssrd/21600`
- [x] **rsus** — Surface Upwelling Shortwave (`W m-2`, Amon) — XIOS expr: `(ssrd-ssr)/21600`
- [x] **rlds** — Surface Downwelling Longwave (`W m-2`, Amon) — XIOS expr: `strd/21600`
- [x] **rlus** — Surface Upwelling Longwave (`W m-2`, Amon) — XIOS expr: `(strd-str)/21600`

### Radiation — Surface clear-sky (XIOS-converted, requires ssrdc/strdc in model output)

- [x] **rsdscs** — Surface Downwelling SW Clear-Sky (`W m-2`, Amon) — XIOS expr: `ssrdc/21600`
- [x] **rsuscs** — Surface Upwelling SW Clear-Sky (`W m-2`, Amon) — XIOS expr: `(ssrdc-ssrc)/21600`
- [x] **rldscs** — Surface Downwelling LW Clear-Sky (`W m-2`, Amon) — XIOS expr: `strdc/21600`
- [x] **rluscs** — Surface Upwelling LW Clear-Sky (`W m-2`, Amon) — XIOS expr: `(strdc-strc)/21600`

### Turbulent fluxes (XIOS-converted, sign-flipped)

- [x] **hfls** — Surface Upward Latent Heat Flux (`W m-2`, Amon) — XIOS expr: `-slhf/21600`
- [x] **hfss** — Surface Upward Sensible Heat Flux (`W m-2`, Amon) — XIOS expr: `-sshf/21600`

### Surface stress (XIOS-converted)

- [x] **tauu** — Eastward Surface Stress (`Pa`, Amon) — XIOS expr: `ewss/21600`
- [x] **tauv** — Northward Surface Stress (`Pa`, Amon) — XIOS expr: `nsss/21600`

### Computed via pycmor pipeline

- [x] **sfcWind** — Near-Surface Wind Speed (`m s-1`, Amon) — pycmor pipeline: `sqrt(10u² + 10v²)`
- [x] **hurs** — Near-Surface Relative Humidity (`%`, Amon) — pycmor pipeline: Magnus formula from `2t` + `2d`
- [x] **huss** — Near-Surface Specific Humidity (`1`, Amon) — pycmor pipeline: Tetens formula from `2d` + `sp`
- [x] **sftlf** — Land Area Fraction (`%`, fx) — pycmor pipeline: `lsm × 100`

### Monthly mean of daily extremes

- [x] **tasmax_mon** — Monthly Mean of Daily Max Temperature (`K`, Amon) — read daily max, time-average
- [x] **tasmin_mon** — Monthly Mean of Daily Min Temperature (`K`, Amon) — read daily min, time-average

## Monthly 3D on pressure levels (Amon, plev19)

- [x] **ta** — Air Temperature (`K`, Amon, plev19) — from `t_pl`
- [x] **ua** — Eastward Wind (`m s-1`, Amon, plev19) — from `u_pl`
- [x] **va** — Northward Wind (`m s-1`, Amon, plev19) — from `v_pl`
- [x] **hus** — Specific Humidity (`1`, Amon, plev19) — from `q_pl`
- [x] **wap** — Omega (`Pa s-1`, Amon, plev19) — from `w_pl` (unit fixed: was mislabeled m/s, is actually Pa/s)
- [x] **zg** — Geopotential Height (`m`, Amon, plev19) — XIOS expr: `z_pl/9.80665`
- [x] **hur** — Relative Humidity (`%`, Amon, plev19) — XIOS expr: `r_pl*100`

## Monthly 3D on model levels (Amon, alevel)

- [x] **cl** — Cloud Area Fraction (`%`, Amon, alevel) — XIOS expr: `cc*100` on `regular_ml`
- [x] **cli** — Cloud Ice Content (`kg kg-1`, Amon, alevel) — from `ciwc` on `regular_ml`
- [x] **clw** — Cloud Liquid Water (`kg kg-1`, Amon, alevel) — from `clwc` on `regular_ml`

## Daily surface (day)

### CMOR-ready from XIOS

- [x] **clt** — Total Cloud Cover (`%`, day) — XIOS expr: `tcc*100`
- [x] **rsds** — Surface Downwelling SW (`W m-2`, day) — XIOS expr: `ssrd/21600`
- [x] **pr** — Precipitation (`kg m-2 s-1`, day) — XIOS expr: `tp*1000/21600`

### Raw IFS daily

- [x] **tas** — Near-Surface Air Temperature (`K`, day) — from `2t`
- [x] **psl** — Sea Level Pressure (`Pa`, day) — from `msl`
- [x] **ps** — Surface Air Pressure (`Pa`, day) — from `sp`
- [x] **uas** — Eastward Near-Surface Wind (`m s-1`, day) — from `10u`
- [x] **vas** — Northward Near-Surface Wind (`m s-1`, day) — from `10v`

### Computed via pycmor pipeline

- [x] **sfcWind** — Near-Surface Wind Speed (`m s-1`, day) — pycmor pipeline: `sqrt(10u² + 10v²)`
- [x] **hurs** — Near-Surface Relative Humidity (`%`, day) — pycmor pipeline: Magnus formula
- [x] **huss** — Near-Surface Specific Humidity (`1`, day) — pycmor pipeline: Tetens from `2d` + `sp`

### Daily extremes (XIOS operation=max/min)

- [x] **tasmax** — Daily Maximum Temperature (`K`, day) — XIOS `operation="maximum"` on `2t`
- [x] **tasmin** — Daily Minimum Temperature (`K`, day) — XIOS `operation="minimum"` on `2t`

## Daily 3D on pressure levels (day, plev19)

- [x] **ta** — Air Temperature (`K`, day, plev19) — from `t_pl`
- [x] **ua** — Eastward Wind (`m s-1`, day, plev19) — from `u_pl`
- [x] **va** — Northward Wind (`m s-1`, day, plev19) — from `v_pl`
- [x] **hus** — Specific Humidity (`1`, day, plev19) — from `q_pl`
- [x] **wap** — Omega (`Pa s-1`, day, plev19) — from `w_pl`
- [x] **zg** — Geopotential Height (`m`, day, plev19) — XIOS expr: `z_pl/9.80665`
- [x] **hur** — Relative Humidity (`%`, day, plev19) — XIOS expr: `r_pl*100`

## Sub-daily (3hr, 6hr, 1hr)

### 3-hourly instantaneous (3hrPt)

- [x] **tas** (3hrPt) — from `2t`, `operation="instant"`, `output_freq="3h"`
- [x] **uas** (3hrPt) — from `10u`, `operation="instant"`, `output_freq="3h"`
- [x] **vas** (3hrPt) — from `10v`, `operation="instant"`, `output_freq="3h"`
- [x] **huss** (3hrPt) — pycmor pipeline: Tetens from `2d` + `sp` at 3h instant

### 3-hourly averaged

- [x] **pr** (3hr) — XIOS expr: `tp*1000/10800` with `freq_op="3h"`

### 1-hourly averaged

- [x] **pr** (1hr) — XIOS expr: `tp*1000/3600` with `freq_op="1h"`

### 6-hourly

- [x] **hurs** (6hr) — pycmor pipeline: Magnus formula from `2t` + `2d` at 6h average
- [x] **ta** (6hrPt, plev3) — from `t_pl` on `regular_pl3`, `operation="instant"`
- [x] **ua** (6hrPt, plev3) — from `u_pl` on `regular_pl3`, `operation="instant"`
- [x] **va** (6hrPt, plev3) — from `v_pl` on `regular_pl3`, `operation="instant"`

---

## Blockers / verification needed

1. **ssrdc/strdc** — IFS params 228129/228130 (clear-sky downwelling). Fields added to field_def and file_def, but **NOT registered in the XIOS GRIB→field-name mapping table**. In `oifs-48r1/ifs-source/arpifs/module/yomxios.F90`, the `CSFCFLD`/`IGRBSFCFLD` arrays (126 entries) do not include GRIB codes 228129 (ssrdc) or 228130 (strdc). IFS computes these fields internally (`cpedia.F90` lines 596/599/647/650, via `PFRSODC`/`PFRTHDC`), they are registered as surface diagnostic fields (`field_definitions.F90` `vd_ssrdc`/`vd_strdc`, indices 3072/3073), and they pass through FullPos (`su_surf_flds.F90`, `postphy_layer.F90`). However, when the FullPos output reaches `cxios.F90`, the GRIB code lookup against `IGRBSFCFLD` fails and the field is silently skipped ("UNKNOWN GRIB CODE SKIPPED"). **Fix required**: add `'ssrdc'`/228129 and `'strdc'`/228130 to `CSFCFLD`/`IGRBSFCFLD` in `yomxios.F90` and bump `NSFCFLD` from 126 to 128. The net clear-sky fields `ssrc` (210) and `strc` (211) ARE already in the table and work correctly. This blocks rsdscs, rsuscs, rldscs, rluscs (4 core Amon variables)
2. **Model-level interpolation** — cl/cli/clw use `regular_ml` grid (interpolation from Gaussian to regular). Verify this works in practice and check computational cost
3. **plev3 axis** — Added 3-level pressure axis (850/500/250 hPa) to axis_def.xml for 6hr ta/ua/va. Verify XIOS FullPos can interpolate to arbitrary pressure level sets

## OIFS source code investigation (2026-04-06)

### Available GRIB fields not yet used
- **Transpiration** (`SURFTRANSPIRATIO` / GFP `CTP`) — already registered as accumulated flux in `cpg_dia.F90`. Can be requested via XIOS `field_def.xml` without source changes. Relevant for evspsblveg decomposition in lrcs_land

### HTESSEL internals accessible via source changes
- Bare soil evaporation, interception evaporation, frozen soil water — all computed internally but need GRIB field registration. See `../lrcs_land/cmip7_lrcs_land_todo.md` for details

## Research findings

- IFS `w_pl` is omega (Pa/s), not vertical velocity (m/s). Unit annotation in field_def was wrong — fixed
- IFS has dedicated clear-sky downwelling fields: `ssrdc` (param 228129) and `strdc` (param 228130). No albedo assumption needed
- IFS sign convention: sshf/slhf are downward-positive; CMIP wants upward → XIOS expressions negate
- IFS accumulated fields reset every `freq_op` (6h). Division by 21600 converts J m-2 → W m-2
- Precipitation: m water equiv → kg m-2 s-1 needs ×ρ_water/Δt (×1000/21600 for 6h, ×1000/10800 for 3h, ×1000/3600 for 1h)
- 19 pressure levels already configured in axis_def.xml matching plev19
- plev3 = 850, 500, 250 hPa — added to axis_def.xml and grid_def.xml
- Model-level grid `regular_ml` already defined in grid_def.xml, just needed file_def output sections
- Compound names from CSV: PL variables use `-air` suffix (e.g. `tavg-p19-hxy-air`) for ta/ua/va/wap/zg/hur monthly; surface vars use `-h2m` (tas) and `-h10m` (uas/vas)
- For sub-daily accumulated fields, XIOS expressions need denominator matching freq_op (10800 for 3h, 3600 for 1h)
