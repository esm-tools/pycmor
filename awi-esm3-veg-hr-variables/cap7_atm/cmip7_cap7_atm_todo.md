# CAP7 Atmosphere Variables — AWI-ESM3-VEG-HR

Source CSVs (unfiltered): `cmip7_CAP7_variables_atmos.csv` (226), `cmip7_CAP7_variables_landIce.csv` (1: sbl), `cmip7_CAP7_variables_landIce_land.csv` (6: snc, snd, snw, mrfso), `cmip7_CAP7_variables_atmos_land.csv` (0 data rows).

Total: 233 compound_name entries — 79 already in core/veg/extra/lrcs, 58 new cap7 rules, ~96 blocked

XIOS field definitions: `field_def_cmip7.xml`
XIOS output config: `file_def_oifs_cmip7_spinup.xml.j2`
Pycmor rules: `cmip7_awiesm3-veg-hr_cap7_atm.yaml`

---

## Already in core/veg/extra/lrcs (79 compound entries)

These variables already have matching compound names in other tier configs. No new rules needed.

### core_atm (76)
- [x] **cl** (mon), **cli** (mon), **clivi** (mon), **clt** (day, mon), **clw** (mon), **clwvi** (mon)
- [x] **hfls** (mon), **hfss** (mon), **hur** (day, mon), **hurs** (day, 6hr, mon)
- [x] **hus** (day, mon), **huss** (day, mon, 3hr), **pr** (1hr, 3hr, day, mon), **prc** (mon), **prsn** (mon), **prw** (mon)
- [x] **ps** (day, mon), **psl** (day, mon)
- [x] **rlds** (mon), **rldscs** (mon), **rlus** (mon), **rluscs** (mon), **rlut** (mon), **rlutcs** (mon)
- [x] **rsds** (day, mon), **rsdscs** (mon), **rsdt** (mon), **rsus** (mon), **rsuscs** (mon), **rsut** (mon), **rsutcs** (mon)
- [x] **sfcWind** (day, mon), **sftlf** (fx)
- [x] **ta** (day, mon, 6hr plev3), **tas** (day, mon, 3hr, daily max/min, monthly max/min), **tauu** (mon), **tauv** (mon), **ts** (mon)
- [x] **ua** (day, mon, 6hr plev3), **uas** (day, mon, 3hr), **va** (day, mon, 6hr plev3), **vas** (day, mon, 3hr)
- [x] **wap** (day, mon), **zg** (day, mon)

### core_land (2)
- [x] **snc** (mon), **snw** (mon)

### lrcs_land (1)
- [x] **mrfso** (mon)

---

## Daily 2D surface — CMOR-ready from XIOS (from `_day_cap7`)

- [x] **hfls** — Surface Upward Latent Heat Flux (`W m-2`, day) — XIOS CMOR field
- [x] **hfss** — Surface Upward Sensible Heat Flux (`W m-2`, day) — XIOS CMOR field
- [x] **rlus** — Surface Upwelling Longwave (`W m-2`, day) — XIOS CMOR field
- [x] **rsus** — Surface Upwelling Shortwave (`W m-2`, day) — XIOS CMOR field
- [x] **rluscs** — Surface Upwelling LW Clear-Sky (`W m-2`, day) — XIOS CMOR field
- [x] **rsuscs** — Surface Upwelling SW Clear-Sky (`W m-2`, day) — XIOS CMOR field
- [x] **rlds** — Surface Downwelling Longwave (`W m-2`, day) — XIOS CMOR field
- [x] **rldscs** — Surface Downwelling LW Clear-Sky (`W m-2`, day) — XIOS CMOR field
- [x] **rsdscs** — Surface Downwelling SW Clear-Sky (`W m-2`, day) — XIOS CMOR field
- [x] **rlut** — TOA Outgoing Longwave (`W m-2`, day) — XIOS CMOR field
- [x] **rlutcs** — TOA Outgoing LW Clear-Sky (`W m-2`, day) — XIOS CMOR field
- [x] **rsdt** — TOA Incoming Shortwave (`W m-2`, day) — XIOS CMOR field
- [x] **rsut** — TOA Outgoing Shortwave (`W m-2`, day) — XIOS CMOR field
- [x] **rsutcs** — TOA Outgoing SW Clear-Sky (`W m-2`, day) — XIOS CMOR field
- [x] **prc** — Convective Precipitation (`kg m-2 s-1`, day) — XIOS CMOR field
- [x] **prsn** — Snowfall Flux (`kg m-2 s-1`, day) — XIOS CMOR field
- [x] **prw** — Water Vapor Path (`kg m-2`, day) — XIOS CMOR field from `tcwv`
- [x] **clivi** — Ice Water Path (`kg m-2`, day) — XIOS CMOR field from `tciw`
- [x] **snw** — Surface Snow Amount (`kg m-2`, day) — XIOS CMOR field from `sd*1000`

## Daily 2D surface — pipeline-computed

- [x] **clwvi** — Condensed Water Path (`kg m-2`, day) — pipeline: `tclw + tciw`
- [x] **snc** — Snow Area Fraction (`%`, day) — pipeline: saturation curve from `sd`
- [x] **hurs** (daily max) — Near-Surface Relative Humidity max (`%`, day) — pipeline: Magnus from `2t+2d` (daily avg approximation)
- [x] **hurs** (daily min) — Near-Surface Relative Humidity min (`%`, day) — pipeline: Magnus from `2t+2d` (daily avg approximation)
- [x] **sfcWind** (daily max) — Near-Surface Wind Speed max (`m s-1`, day) — XIOS `operation="maximum"` on `sqrt(10u²+10v²)`

## Daily 3D — single pressure level extraction

- [x] **ta** — Air Temperature at 700 hPa (`K`, day) — pipeline: extract from plev19
- [x] **wap** — Omega at 500 hPa (`Pa s-1`, day) — pipeline: extract from plev19

## 3-hourly

- [x] **prsn** — Snowfall Flux (`kg m-2 s-1`, 3hr) — XIOS CMOR field: `sf*1000/3600`

## 1-hourly

- [x] **huss** — Near-Surface Specific Humidity (`1`, 1hrPt) — pipeline: Tetens from `2d+sp`
- [x] **psl** — Sea Level Pressure (`Pa`, 1hrPt) — from `msl` instantaneous
- [x] **ts** — Surface Temperature (`K`, 1hr) — XIOS CMOR field from `skt`, averaged
- [x] **uas** — Eastward Near-Surface Wind (`m s-1`, 1hrPt) — from `10u` instantaneous
- [x] **vas** — Northward Near-Surface Wind (`m s-1`, 1hrPt) — from `10v` instantaneous
- [x] **ps** — Surface Air Pressure (`Pa`, 1hrPt) — from `sp` (reuses extra_atm output)
- [x] **rlds** — Surface Downwelling Longwave (`W m-2`, 1hr) — reuses extra_atm output
- [x] **rsds** — Surface Downwelling Shortwave (`W m-2`, 1hr) — reuses extra_atm output
- [x] **sfcWind** — Near-Surface Wind Speed (`m s-1`, 1hr) — pipeline: `sqrt(10u²+10v²)` (reuses extra_atm output)
- [x] **wsg** — Maximum Wind Speed of Gust at 10m (`m s-1`, 1hr) — XIOS `operation="maximum"` on `10fg`

## 6-hourly instantaneous surface

- [x] **ps** — Surface Air Pressure (`Pa`, 6hrPt) — from `sp` instantaneous
- [x] **psl** — Sea Level Pressure (`Pa`, 6hrPt) — from `msl` instantaneous
- [x] **ts** — Surface Temperature (`K`, 6hrPt) — from `skt` instantaneous

## 6-hourly instantaneous model levels (from `_6h_ml`)

- [x] **ta** — Air Temperature (`K`, 6hrPt, alevel) — from `t` on `regular_ml`
- [x] **ua** — Eastward Wind (`m s-1`, 6hrPt, alevel) — from `u` on `regular_ml`
- [x] **va** — Northward Wind (`m s-1`, 6hrPt, alevel) — from `v` on `regular_ml`
- [x] **hus** — Specific Humidity (`1`, 6hrPt, alevel) — from `q` on `regular_ml`
- [x] **zg** — Geopotential Height (`m`, 6hrPt, alevel) — XIOS expr: `z/9.80665` on `regular_ml`

## 6-hourly instantaneous plev7h (from `_6h_pl7h`)

New plev7h axis: 1000, 925, 850, 700, 500, 250, 100 hPa (added to `axis_def.xml` and `grid_def.xml`).

- [x] **ta** — Air Temperature (`K`, 6hrPt, plev7h) — from `t_pl` on `regular_pl7h`
- [x] **ua** — Eastward Wind (`m s-1`, 6hrPt, plev7h) — from `u_pl` on `regular_pl7h`
- [x] **va** — Northward Wind (`m s-1`, 6hrPt, plev7h) — from `v_pl` on `regular_pl7h`
- [x] **hus** — Specific Humidity (`1`, 6hrPt, plev7h) — from `q_pl` on `regular_pl7h`
- [x] **zg** — Geopotential Height (`m`, 6hrPt, plev7h) — from `z_pl` on `regular_pl7h`

## Monthly surface

- [x] **rtmt** — Net Downward Radiative Flux at Top of Model (`W m-2`, mon) — pipeline: `rsdt-rsut+rlds-rlus`
- [x] **ci** — Sea-Ice Area Fraction (`1`, mon) — raw `ci` from monthly output
- [x] **sbl** — Surface Snow and Ice Sublimation Flux (`kg m-2 s-1`, mon) — XIOS CMOR field from `es`

## Monthly model levels (from `_mon_ml_cap7`)

- [x] **pfull** — Pressure at Model Full-Levels (`Pa`, mon, alevel) — raw `pres` on model levels
- [x] **ta** — Air Temperature (`K`, mon, alevel) — raw `t` on model levels
- [x] **hus** — Specific Humidity (`1`, mon, alevel) — raw `q` on model levels
- [x] **hur** — Relative Humidity (`%`, mon, alevel) — XIOS expr: `r*100` on model levels

## Monthly land/ice (pipeline-computed)

- [x] **snd** — Snow Depth (`m`, mon) — pipeline: `sd*1000/rsn` (same as veg_land)

---

## Blocked — satellite simulators (no COSP)

- [ ] **albisccp** — ISCCP Mean Cloud Albedo (`1`, day/mon)
- [ ] **clcalipso** — CALIPSO Cloud Fraction (`1`, day/mon at p220/p560/p840/alt40) — 7 entries
- [ ] **clisccp** — ISCCP Cloud Fraction (`1`, mon, plev7c x tau)
- [ ] **clmisr** — MISR Cloud Fraction (`1`, mon, alt16 x tau)
- [ ] **cltcalipso** — CALIPSO Total Cloud Fraction (`1`, day/mon)
- [ ] **cltisccp** — ISCCP Total Cloud Fraction (`1`, day/mon)
- [ ] **pctisccp** — ISCCP Cloud Top Pressure (`Pa`, day/mon)

Total: 17 entries

## Blocked — temperature tendencies (need IFS source changes)

IFS computes tendencies internally but does NOT expose individual process contributions. Would require significant source code changes to decompose.

- [ ] **tnt** — Total Temperature Tendency (`K s-1`, mon, alevel)
- [ ] **tnta** — Temperature Tendency from Advection (`K s-1`, mon, alevel)
- [ ] **tntc** — Temperature Tendency from Convection (`K s-1`, mon, alevel)
- [ ] **tntd** — Temperature Tendency from Diffusion (`K s-1`, mon, alevel)
- [ ] **tntmp** — Temperature Tendency from Microphysics (`K s-1`, mon, alevel)
- [ ] **tntpbl** — Temperature Tendency from PBL (`K s-1`, mon, alevel)
- [ ] **tntr** — Temperature Tendency from Total Radiation (`K s-1`, mon, alevel)
- [ ] **tntrl** — Temperature Tendency from LW Radiation (`K s-1`, mon, alevel)
- [ ] **tntrlcs** — Temperature Tendency from LW Clear-Sky (`K s-1`, mon, alevel)
- [ ] **tntrs** — Temperature Tendency from SW Radiation (`K s-1`, mon, alevel)
- [ ] **tntrscs** — Temperature Tendency from SW Clear-Sky (`K s-1`, mon, alevel)
- [ ] **tntscp** — Temperature Tendency from Stratiform Cloud (`K s-1`, mon, alevel)
- [ ] **tntscpbl** — Temperature Tendency from Stratiform Cloud + PBL (`K s-1`, mon, alevel)

Total: 13 entries

## Blocked — humidity tendencies (need IFS source changes)

- [ ] **tnhus** — Total Humidity Tendency (`s-1`, mon, alevel)
- [ ] **tnhusa** — Humidity Tendency from Advection (`s-1`, mon, alevel)
- [ ] **tnhusc** — Humidity Tendency from Convection (`s-1`, mon, alevel)
- [ ] **tnhusd** — Humidity Tendency from Diffusion (`s-1`, mon, alevel)
- [ ] **tnhusmp** — Humidity Tendency from Microphysics (`s-1`, mon, alevel)
- [ ] **tnhuspbl** — Humidity Tendency from PBL (`s-1`, mon, alevel)
- [ ] **tnhusscp** — Humidity Tendency from Stratiform Cloud (`s-1`, mon, alevel)
- [ ] **tnhusscpbl** — Humidity Tendency from Stratiform Cloud + PBL (`s-1`, mon, alevel)

Total: 8 entries

## Blocked — no prognostic aerosol (MACv2-SP only)

- [ ] **loadbc** — Black Carbon Column Burden (`kg m-2`, day)
- [ ] **loaddust** — Dust Column Burden (`kg m-2`, day)
- [ ] **loadnh4** — NH4 Column Burden (`kg m-2`, day)
- [ ] **loadno3** — NO3 Column Burden (`kg m-2`, day)
- [ ] **loadoa** — Organic Aerosol Column Burden (`kg m-2`, day)
- [ ] **loadpoa** — Primary Organic Aerosol Column Burden (`kg m-2`, day)
- [ ] **loadso4** — SO4 Column Burden (`kg m-2`, day)
- [ ] **loadsoa** — Secondary Organic Aerosol Column Burden (`kg m-2`, day)
- [ ] **loadss** — Sea Salt Column Burden (`kg m-2`, day)

Total: 9 entries

## Blocked — no prognostic CO2

- [ ] **co23D** — CO2 Mole Fraction 3D (`1e-6`, mon, alevel)
- [ ] **co2mass** — Atmospheric CO2 Mass (`kg`, mon, scalar)
- [ ] **fco2antt** — Anthropogenic CO2 Flux (`kg m-2 s-1`, mon)
- [ ] **fco2fos** — Fossil CO2 Flux (`kg m-2 s-1`, mon)
- [ ] **fco2nat** — Natural CO2 Flux (`kg m-2 s-1`, mon)

Total: 5 entries

## Blocked — effective radii (need detailed microphysics output)

- [ ] **reffclic** — Effective Radius of Convective Cloud Ice (`m`, mon, alevel)
- [ ] **reffclis** — Effective Radius of Stratiform Cloud Ice (`m`, mon, alevel)
- [ ] **reffclwc** — Effective Radius of Convective Cloud Liquid (`m`, mon, alevel)
- [ ] **reffclws** — Effective Radius of Stratiform Cloud Liquid (`m`, mon, alevel)

Total: 4 entries

## Blocked — need IFS source changes to expose diagnostics

### Convective/stratiform separation (internal to convection scheme)

- [ ] **ccb** — Convective Cloud Base Pressure (`Pa`, day/mon) — IFS has KCBOT internally
- [ ] **cct** — Convective Cloud Top Pressure (`Pa`, day/mon) — IFS has KCTOP internally
- [ ] **clc** — Convective Cloud Fraction (`1`, mon, alevel) — internal to convection
- [ ] **cls** — Stratiform Cloud Fraction (`1`, mon, alevel) — would be `cl - clc`
- [ ] **clic** — Convective Cloud Ice (`kg kg-1`, mon, alevel) — internal to convection
- [ ] **clis** — Stratiform Cloud Ice (`kg kg-1`, mon, alevel) — would be `cli - clic`
- [ ] **clwc** — Convective Cloud Liquid Water (`kg kg-1`, mon, alevel) — internal
- [ ] **clws** — Stratiform Cloud Liquid Water (`kg kg-1`, mon, alevel) — would be `clw - clwc`
- [ ] **clivic** — In-Convective-Cloud Ice Water Path (`kg m-2`, day) — not separated
- [ ] **clwvic** — In-Convective-Cloud Liquid Water Path (`kg m-2`, day) — not separated

### Convective mass fluxes (internal to convection scheme)

- [ ] **mc** — Total Convective Mass Flux (`kg m-2 s-1`, mon, alevhalf)
- [ ] **mcu** — Updraft Convective Mass Flux (`kg m-2 s-1`, mon, alevhalf)
- [ ] **mcd** — Downdraft Convective Mass Flux (`kg m-2 s-1`, mon, alevhalf)
- [ ] **dmc** — Deep Convective Detrainment (`kg m-2 s-1`, mon, alevhalf)
- [ ] **smc** — Shallow Convective Mass Flux (`kg m-2 s-1`, mon, alevhalf)
- [ ] **evu** — Updraft Entrainment (`s-1`, mon, alevel)
- [ ] **edt** — Downdraft Entrainment (`s-1`, mon, alevel)

### Radiation profiles on half-levels (ecRad computes, not exposed via XIOS)

- [ ] **rld** — LW Downwelling Radiation Profile (`W m-2`, mon, alevhalf)
- [ ] **rldcs** — LW Downwelling Clear-Sky Profile (`W m-2`, mon, alevhalf)
- [ ] **rlu** — LW Upwelling Radiation Profile (`W m-2`, mon, alevhalf)
- [ ] **rlucs** — LW Upwelling Clear-Sky Profile (`W m-2`, mon, alevhalf)
- [ ] **rsd** — SW Downwelling Radiation Profile (`W m-2`, mon, alevhalf)
- [ ] **rsdcs** — SW Downwelling Clear-Sky Profile (`W m-2`, mon, alevhalf)
- [ ] **rsu** — SW Upwelling Radiation Profile (`W m-2`, mon, alevhalf)
- [ ] **rsucs** — SW Upwelling Clear-Sky Profile (`W m-2`, mon, alevhalf)

### Diffuse radiation (ecRad has sw_dn_diffuse_surf_g, not exposed)

- [ ] **rsdsdiff** — Surface Diffuse Downwelling SW (`W m-2`, day)
- [ ] **rsdsdiff** — Surface Diffuse Downwelling SW (`W m-2`, 1hr)
- [ ] **rsdscsdiff** — Surface Diffuse Downwelling SW Clear-Sky (`W m-2`, day)

### 100m wind (IFS does not interpolate to 100 m)

- [ ] **ua** — Eastward Wind at 100m (`m s-1`, 1hrPt, height100m)
- [ ] **va** — Northward Wind at 100m (`m s-1`, 1hrPt, height100m)
- [ ] **wsg** — Maximum Wind Gust at 100m (`m s-1`, 1hr, height100m)

### Cloud droplet/crystal number (no diagnostic available)

- [ ] **cldnci** — In-Cloud Ice Crystal Number (`m-3`, day)
- [ ] **cldnvi** — Column Ice Crystal Number (`m-2`, day)

### Tropopause (IFS computes internally, not exposed via XIOS)

- [ ] **ptp** — Tropopause Air Pressure (`Pa`, mon)
- [ ] **ztp** — Tropopause Geopotential Height (`m`, mon)

### Model-level geometry / other

- [ ] **phalf** — Pressure at Model Half-Levels (`Pa`, mon, alevhalf) — needs alevhalf axis
- [ ] **zfull** — Geopotential Height of Model Full-Levels (`m`, fx, alevel) — needs offline computation
- [ ] **sci** — Fraction of Time Shallow Convection Occurs (`1`, mon) — unclear IFS mapping

## Blocked — CSV artefact

- [ ] **600** — malformed row (dims: 700) — not a real variable

---

## Summary

| Category | Count |
|----------|-------|
| Already in core/veg/extra/lrcs | 79 |
| Producible (new cap7 rules written) | 58 |
| Blocked: COSP satellite simulators | 17 |
| Blocked: temperature tendencies | 13 |
| Blocked: humidity tendencies | 8 |
| Blocked: aerosol loads | 9 |
| Blocked: CO2 tracer | 5 |
| Blocked: effective radii | 4 |
| Blocked: IFS source (convective, radiation, diffuse, 100m, etc.) | ~40 |
| **Total** | **233** |
