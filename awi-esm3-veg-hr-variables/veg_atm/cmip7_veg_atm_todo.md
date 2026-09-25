# CMIP7 VEG Atmosphere/Aerosol Variables -- Rule Implementation TODO

Variables from 5 CSVs in `veg_atm/`: 38 total rows.
These are additional atmosphere and aerosol variables requested for the VEG (vegetation) experiment tier.

Model constraints:
- Aerosol: MACv2-SP only (no CAMS, no M7) -- most aerosol diagnostics NOT available
- Fire: BLAZE (SIMFIRE-driven burned area + fire emissions)
- BVOC: disabled (ifbvoc=0) -- no isoprene/monoterpene emissions
- CO2: concentration-driven, no tracer
- LPJ-GUESS output: plain-text .out files, not XIOS

---

## 3-hourly radiation and flux fields (from atmos CSV)

All producible from OpenIFS via XIOS. These are additional frequencies of fields already defined for monthly output in core_atm.

### 3hr averaged (XIOS expressions with deaccum /3600 (1h IFS→XIOS))

- [x] **hfls** (3hr) -- Surface Upward Latent Heat Flux (`W m-2`) -- XIOS: `-slhf/10800`
- [x] **hfss** (3hr) -- Surface Upward Sensible Heat Flux (`W m-2`) -- XIOS: `-sshf/10800`
- [x] **rlds** (3hr) -- Surface Downwelling LW Radiation (`W m-2`) -- XIOS: `strd/10800`
- [x] **rlus** (3hr) -- Surface Upwelling LW Radiation (`W m-2`) -- XIOS: `(strd-str)/10800`
- [x] **rsds** (3hr) -- Surface Downwelling SW Radiation (`W m-2`) -- XIOS: `ssrd/10800`
- [x] **rsus** (3hr) -- Surface Upwelling SW Radiation (`W m-2`) -- XIOS: `(ssrd-ssr)/10800`

### 3hr instantaneous on surface (already in _3h_pt file)

- [x] **ps** (3hrPt) -- Surface Air Pressure (`Pa`) -- already in _3h_pt file as `sp`

### 3hr instantaneous on plev6 (need new plev6 axis: 950/900/850/800/750/700 hPa)

- [x] **ta** (E3hrPt, plev6) -- Air Temperature (`K`) -- from `t_pl` on plev6
- [x] **ua** (E3hrPt, plev6) -- Eastward Wind (`m s-1`) -- from `u_pl` on plev6
- [x] **va** (E3hrPt, plev6) -- Northward Wind (`m s-1`) -- from `v_pl` on plev6
- [x] **wap** (E3hrPt, plev6) -- Omega (`Pa s-1`) -- from `w_pl` on plev6
- [x] **hus** (E3hrPt, plev6) -- Specific Humidity (`1`) -- from `q_pl` on plev6

## 6-hourly fields (from atmos CSV)

- [x] **prsn** (6hr) -- Snowfall Flux (`kg m-2 s-1`) -- XIOS: `sf*1000/21600` with freq_op="6h"

## Monthly net radiation (from atmos CSV)

- [x] **rls** (Emon) -- Net Longwave Surface Radiation (`W m-2`) -- XIOS: `str/21600` (already have `str`)
- [x] **rss** (Emon) -- Net Shortwave Surface Radiation (`W m-2`) -- XIOS: `ssr/21600` (already have `ssr`)

## 3hr instantaneous boundary layer (from atmos_aerosol_land CSV)

- [x] **bldep** (3hrPt) -- Boundary Layer Depth (`m`) -- from IFS `blh` (already in field_def)

## Daily snow/land variables (from atmos CSV)

These require IFS HTESSEL diagnostics. Some may not be directly available.

### Likely producible from IFS

- [x] **ts** (Eday, snow surface) -- Snow Surface Temperature (`K`) -- from `tsn` (temperature of snow layer)
- [x] **snmsl** (Eday) -- Water Flowing out of Snowpack (`kg m-2 s-1`) -- from `smlt` (snowmelt): XIOS `smlt*1000/21600`
- [ ] **hfdsnb** (Eday) -- Downward Heat Flux at Snow Base (`W m-2`) -- NOT a standard IFS output. Would need OIFS source changes or approximation

### Likely NOT producible from IFS without source changes

- [ ] **prrsn** (Eday) -- Fraction of Rainfall on Snow (`1`) -- IFS doesn't partition precip by surface type
- [ ] **prsnc** (Eday) -- Convective Snowfall Flux (`kg m-2 s-1`) -- IFS has `sf` (total snowfall) but not convective/large-scale split for snow
- [ ] **prsnsn** (Eday) -- Fraction of Snowfall on Snow (`1`) -- IFS doesn't track this
- [ ] **snrefr** (Eday) -- Snow Refreezing Flux (`kg m-2 s-1`) -- HTESSEL internal, not output via XIOS
- [ ] **snwc** (Eday) -- Canopy Snow Amount (`kg m-2`) -- HTESSEL may track intercepted snow but unclear if XIOS-accessible

## Aerosol variables (from aerosol CSV)

### Producible from IFS

- [x] **lwp** (AERmon) -- Liquid Water Path (`kg m-2`) -- = `tclw` (total column liquid water), already in field_def

### NOT producible (need prognostic aerosol model)

- [ ] ~~**ccn** (AERmon)~~ -- Cloud Condensation Nuclei -- requires CAMS/M7
- [ ] ~~**mmrpm2p5** (AERmon, 3D)~~ -- PM2.5 Mass Mixing Ratio -- requires CAMS/M7
- [ ] ~~**od550soa** (AERmon)~~ -- Organic Aerosol AOD at 550nm -- requires CAMS/M7

## Cloud microphysics (from atmos_atmosChem_aerosol CSV)

- [ ] ~~**reffsclwtop** (Emon)~~ -- Cloud-Top Effective Droplet Radius -- IFS computes `reff` internally in cloud scheme but not exposed to XIOS. Would need source changes

## Aerosol/chemistry emission variables (from aerosol_atmosChem CSV)

### NOT producible (need prognostic aerosol model)

- [ ] ~~**conccn** (AERmon, 3D)~~ -- Aerosol Number Concentration -- requires CAMS/M7

### Fire emission variables (from LPJ-GUESS BLAZE)

LPJ-GUESS with BLAZE outputs monthly fire carbon emissions (`fFire`, `fFireAll`, `fFireNat`). However, the specific species-resolved biomass burning emissions below require either:
(a) BLAZE to output species-specific emission factors, or
(b) post-processing with emission factor tables (e.g., Andreae & Merlet 2001)

- [x] **emibbbc** (AERmon) -- BC Emission from Biomass Burning (`kg m-2 s-1`) -- custom pipeline: `load_lpjguess_monthly` → `compute_fire_emission` (Andreae 2019 EF=0.37 g/kgDM)
- [x] **emibbch4** (AERmon) -- CH4 Emission from Biomass Burning (`kg m-2 s-1`) -- custom pipeline (EF=1.94 g/kgDM)
- [x] **emibbco** (AERmon) -- CO Emission from Biomass Burning (`kg m-2 s-1`) -- custom pipeline (EF=63.0 g/kgDM)
- [x] **emibbdms** (AERmon) -- DMS Emission from Biomass Burning (`kg m-2 s-1`) -- custom pipeline (EF=0.68 g/kgDM)
- [x] **emibboa** (AERmon) -- Organic Aerosol from Biomass Burning (`kg m-2 s-1`) -- custom pipeline (EF=2.62 g/kgDM)
- [x] **emibbso2** (AERmon) -- SO2 from Biomass Burning (`kg m-2 s-1`) -- custom pipeline (EF=0.48 g/kgDM)
- [x] **emibbvoc** (AERmon) -- NMVOC from Biomass Burning (`kg m-2 s-1`) -- custom pipeline (EF=3.4 g/kgDM)

---

## Summary

| Category | Count | Done | Blocked |
|----------|-------|------|---------|
| 3hr radiation/flux (XIOS) | 6 | 6 | 0 |
| 3hr surface instant | 1 | 1 (already done) | 0 |
| 3hr plev6 instant | 5 | 5 | 0 |
| 6hr snowfall | 1 | 1 | 0 |
| Monthly net radiation | 2 | 2 | 0 |
| 3hr boundary layer | 1 | 1 | 0 |
| Daily snow/land | 5 | 2 | 3 not producible |
| Aerosol (lwp) | 1 | 1 | 0 |
| Aerosol (need CAMS/M7) | 3 | 0 | 3 blocked |
| Cloud microphysics | 1 | 0 | 1 blocked |
| Aerosol number conc. | 1 | 0 | 1 blocked |
| Fire emissions (BLAZE) | 7 | 7 | 0 |
| **Total** | **38** | **27** | **11 (blocked/not producible)** |

## Implementation status

All 27 producible variables are implemented:
- XIOS fields defined in `field_def_cmip7.xml`
- Output files defined in `file_def_oifs_cmip7_spinup.xml.j2`
- plev6 axis/grid added to `axis_def.xml` and `grid_def.xml`
- pycmor YAML rules in `cmip7_awiesm3-veg-hr_veg_atm.yaml`
- Fire emission custom steps (`load_lpjguess_monthly`, `compute_fire_emission`) in `examples/custom_steps.py`
  - Emission factors from Andreae (2019) Table 1, savanna/grassland
  - Custom LPJ-GUESS .out file loader (reads plain-text Lon/Lat/Year/Jan..Dec format)
