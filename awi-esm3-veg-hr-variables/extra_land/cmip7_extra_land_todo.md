# CMIP7 Extra Land Variables -- Rule Implementation TODO

Variables from 1 CSV in `extra_land/`: 19 total rows.

Model constraints:
- Land surface: HTESSEL (4-layer soil, single-layer snow, no groundwater)
- Vegetation: LPJ-GUESS 4.1.2 (run_landcover=0 → natural veg only, no land-use transitions)
- No irrigation scheme
- No river routing model (only rnfmap redistributes runoff to coast)
- LPJ-GUESS output: plain-text .out files (monthly Jan..Dec format, yearly per-PFT)
- IFS output via XIOS on 0.25deg regular grid

---

## Fixed fields (fx)

### Producible

- [x] **areacellr** (fx) -- Grid-Cell Area for River Model Variables (`m2`) -- same grid as atmosphere (no separate river grid), reuse areacella computation
- [x] **orog** (fx, 30S-90S) -- Surface Altitude southern hemisphere subset (`m`) -- from IFS `sz` (surface geopotential / g), already defined in field_def. Regional subset via pycmor lat selection

## Monthly PFT fractions from LPJ-GUESS

### Producible (Jan..Dec format)

- [x] **c3PftFrac** (mon) -- C3 Plant Functional Type Fraction (`%`) -- needs custom step to sum C3 grass + C3 tree fractions from `grassFracC3_monthly.out` + `treeFracBdlDcd_monthly.out` + `treeFracBdlEvg_monthly.out` + `treeFracNdlDcd_monthly.out` + `treeFracNdlEvg_monthly.out`
- [x] **c4PftFrac** (mon) -- C4 Plant Functional Type Fraction (`%`) -- from `grassFracC4_monthly.out` (no C4 trees in LPJ-GUESS)
- [x] **cropFracC3** (mon) -- C3 Crop Fraction (`%`) -- from `cropFracC3_monthly.out` (all zeros, run_landcover=0)
- [x] **cropFracC4** (mon) -- C4 Crop Fraction (`%`) -- from `cropFracC4_monthly.out` (all zeros, run_landcover=0)
- [x] **pastureFracC3** (mon) -- C3 Pasture Fraction (`%`) -- from `pastureFracC3_monthly.out` (all zeros, run_landcover=0)
- [x] **pastureFracC4** (mon) -- C4 Pasture Fraction (`%`) -- from `pastureFracC4_monthly.out` (all zeros, run_landcover=0)

## Monthly LAI from LPJ-GUESS

- [x] **lai** (day requested, mon available) -- Leaf Area Index (`1`) -- LPJ-GUESS only outputs monthly (`lai_monthly.out`). Daily LAI not available. Provide monthly as best available.

## Daily IFS/HTESSEL hydrology

### Producible (via temporal differencing)

- [x] **dcw** (day) -- Change in Interception Storage (`kg m-2`) -- temporal diff of `src * 1000` (skin reservoir content, m → kg/m2) via `compute_temporal_diff`
- [x] **dslw** (day) -- Change in Soil Moisture (`kg m-2`) -- temporal diff of total soil moisture `1000*(swvl1*0.07+swvl2*0.21+swvl3*0.72+swvl4*1.89)` via `compute_temporal_diff`

### Producible (from existing daily fields)

- [x] **mrsow** (day) -- Total Soil Wetness (`1`) -- ratio of actual to saturated soil moisture. Approximation: `mrso / mrso_sat` where mrso_sat uses porosity. Alternative: output `swvl` ratio directly. Requires custom step.

### NOT producible

- ~~**rzwc** (day)~~ -- Root Zone Soil Moisture (`kg m-2`) -- HTESSEL has fixed soil layers [0.07, 0.21, 0.72, 1.89 m], not defined by root depth. Would need to know root depth distribution per grid cell, which varies by vegetation type and is internal to HTESSEL.

## Daily IFS fields needing 1hr output

### Producible (need new 1hr output file)

- [x] **tas** (1hr, 30S-90S) -- Near-Surface Air Temperature (`K`) -- from `2t`, already in XIOS field_def. Need new 1hr output file. Regional subset via pycmor lat selection.

## Irrigation variables

### NOT producible (no irrigation scheme)

- ~~**irrDem** (day)~~ -- Irrigation Water Demand -- no irrigation scheme in HTESSEL/LPJ-GUESS
- ~~**irrGw** (day)~~ -- Irrigation from Groundwater -- no irrigation scheme
- ~~**irrLut** (day)~~ -- Total Irrigation Withdrawal -- LPJ-GUESS `irrLut_monthly.out` exists but all zeros (run_landcover=0, no crops)
- ~~**irrSurf** (day)~~ -- Irrigation from Surface Water -- no irrigation scheme

## River variables

### NOT producible (no river routing)

- ~~**rivi** (day)~~ -- River Inflow -- no river routing model (only rnfmap redistributes runoff to coast)

---

## Summary

| Category | Count | Done | Blocked |
|----------|-------|------|---------|
| Fixed fields (fx) | 2 | 2 | 0 |
| Monthly PFT fractions (LPJ-GUESS) | 6 | 6 | 0 |
| Monthly LAI (LPJ-GUESS) | 1 | 1 | 0 |
| Daily hydrology (IFS temporal diff) | 2 | 2 | 0 |
| Daily soil wetness (IFS) | 1 | 1 | 0 |
| Root zone moisture | 1 | 0 | 1 (no root depth) |
| Hourly tas (IFS) | 1 | 1 | 0 |
| Irrigation | 4 | 0 | 4 (no irrigation) |
| River inflow | 1 | 0 | 1 (no river routing) |
| **Total** | **19** | **13** | **6** |

## Implementation status

All 13 producible variables implemented:
- pycmor YAML rules in `cmip7_awiesm3-veg-hr_extra_land.yaml`
- LPJ-GUESS monthly variables use existing `load_lpjguess_monthly` loader
- Custom step `compute_c3PftFrac` sums C3 grass + all C3 tree fractions
- `dcw` and `dslw` use existing `compute_temporal_diff` step
- `mrsow` uses custom step `compute_mrsow` (soil wetness ratio)
- `tas` 1hr and `orog` 30S-90S use lat-subsetting step `select_southern_hemisphere`
- `areacellr` reuses `compute_areacella` (same grid)
- New 1hr output file added to `file_def_oifs_cmip7_spinup.xml.j2`
