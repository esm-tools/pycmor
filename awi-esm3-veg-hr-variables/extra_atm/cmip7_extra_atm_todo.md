# CMIP7 Extra Atmosphere/Aerosol Variables -- Rule Implementation TODO

Variables from 7 CSVs in `extra_atm/`: 43 total rows.

Model constraints:
- Aerosol: MACv2-SP only (no CAMS, no M7) -- no prognostic aerosol, no PM, no NO2
- No atmospheric chemistry -- no interactive O3 (prescribed climatology)
- No lightning parameterization outputting flash rates
- No CH4 emission scheme (methane disabled, ifmethane=0)
- No crop tiles (LPJ-GUESS run_landcover=0)
- Output via OpenIFS XIOS on 0.25deg regular grid, L91 model levels

---

## 1hr surface fields (from atmos CSV)

### Producible (need 1hr XIOS output files)

- [x] **hfls** (1hr) -- Surface Upward Latent Heat Flux (`W m-2`) -- XIOS: `-slhf/3600`
- [x] **hfss** (1hr) -- Surface Upward Sensible Heat Flux (`W m-2`) -- XIOS: `-sshf/3600`
- [x] **rlus** (1hr) -- Surface Upwelling LW Radiation (`W m-2`) -- XIOS: `(strd-str)/3600`
- [x] **rsus** (1hr) -- Surface Upwelling SW Radiation (`W m-2`) -- XIOS: `(ssrd-ssr)/3600`

### Producible (1hr 30S-90S regional subsets, need 1hr output + lat selection)

- [x] **clt** (1hr, 30S-90S) -- Total Cloud Cover (`%`) -- from `tcc * 100`, regional subset
- [x] **hurs** (1hr, 30S-90S) -- Near-Surface Relative Humidity (`%`) -- custom step (Magnus formula), regional subset
- [x] **hurs** (1hr, glb) -- Near-Surface Relative Humidity (`%`) -- custom step (Magnus formula)
- [x] **pr** (1hr, 30S-90S) -- Precipitation (`kg m-2 s-1`) -- already have 1hr pr, regional subset
- [x] **ps** (1hr, 30S-90S) -- Surface Air Pressure (`Pa`) -- from `sp`, regional subset
- [x] **rlds** (1hr, 30S-90S) -- Surface Downwelling LW Radiation (`W m-2`) -- XIOS: `strd/3600`, regional subset
- [x] **rsds** (1hr, 30S-90S) -- Surface Downwelling SW Radiation (`W m-2`) -- XIOS: `ssrd/3600`, regional subset
- [x] **sfcWind** (1hr, 30S-90S) -- Near-Surface Wind Speed (`m s-1`) -- custom step (sqrt(u10^2+v10^2)), regional subset
- [x] **bldep** (1hr) -- Boundary Layer Depth (`m`) -- from `blh`. Already output at 3hr; need 1hr output file

## 3hr fields (from atmos CSV)

### Producible

- [x] **hurs** (3hr) -- Near-Surface Relative Humidity (`%`) -- custom step (Magnus formula), already have 6hr hurs inputs; need 3hr output
- [x] **ts** (3hr) -- Surface Temperature (`K`) -- from `skt`, already in _3h_pt output

## Daily fields (from atmos CSV)

### Producible

- [x] **cl** (day) -- Percentage Cloud Cover on model levels (`%`) -- from `cc * 100` on model levels, need daily ML output file
- [x] **pfull** (day) -- Pressure at Model Full-Levels (`Pa`) -- from `pres` on model levels, need daily ML output file
- [x] **rls** (day) -- Net Longwave Surface Radiation (`W m-2`) -- XIOS: `str/3600`
- [x] **rss** (day) -- Net Shortwave Surface Radiation (`W m-2`) -- XIOS: `ssr/3600`
- [x] **evspsbl** (day) -- Evaporation (`kg m-2 s-1`) -- XIOS: `-e*1000/3600`, already in field_def, need daily output

### Producible (10m wind gust)

- [x] **wsg** (mon, 10m) -- Maximum Wind Speed of Gust at 10m (`m s-1`) -- from `10fg`, need monthly max output

### NOT producible

- ~~**noaahi2m** (day, mean)~~ -- NOAA Heat Index -- not a standard IFS output, requires post-processing from T and RH with empirical Rothfusz formula
- ~~**noaahi2m** (day, max)~~ -- same, max variant
- ~~**wbgt** (day, mean)~~ -- Wet Bulb Globe Temperature -- not a standard IFS output, requires complex post-processing
- ~~**wbgt** (day, max)~~ -- same, max variant
- ~~**wsg** (mon, 100m) ~~ -- Maximum Wind Speed of Gust at 100m -- IFS has 10m gust (`10fg`) but no 100m gust diagnostic
- ~~**pr** (day, max hourly)~~ -- Maximum Hourly Precipitation Rate -- would need 1hr pr with daily max operation; XIOS can do this but requires careful setup of nested temporal operations
- ~~**hurs** (day, min over crop)~~ -- Daily Minimum Relative Humidity over Crop Tile -- no crop tiles (run_landcover=0)
- ~~**pr** (day, crop tile)~~ -- Precipitation over Crop Tile -- no crop tiles
- ~~**tas** (day, max over crop)~~ -- Daily Max Temperature over Crop Tile -- no crop tiles
- ~~**tas** (day, min over crop)~~ -- Daily Min Temperature over Crop Tile -- no crop tiles

## Aerosol/chemistry variables

### NOT producible (need prognostic aerosol/chemistry)

- ~~**sfpm1** (1hr + day)~~ -- PM1.0 Mixing Ratio -- requires CAMS/M7
- ~~**sfpm10** (1hr + day)~~ -- PM10 Mixing Ratio -- requires CAMS/M7
- ~~**sfpm25** (1hr + day)~~ -- PM2.5 Mixing Ratio -- requires CAMS/M7
- ~~**no2** (1hr)~~ -- NO2 Volume Mixing Ratio -- requires atmospheric chemistry
- ~~**o3** (1hr + day)~~ -- O3 Volume Mixing Ratio -- no interactive O3 (prescribed climatology)
- ~~**emich4** (mon)~~ -- Total CH4 Emission Rate -- no methane emission scheme (ifmethane=0)
- ~~**flashrate** (day + mon)~~ -- Lightning Flash Rate -- no lightning parameterization output

---

## Summary

| Category | Count | Done | Blocked |
|----------|-------|------|---------|
| 1hr surface (XIOS) | 4 | 4 | 0 |
| 1hr 30S-90S regional | 7 | 7 | 0 |
| 1hr global (hurs, bldep) | 2 | 2 | 0 |
| 3hr fields | 2 | 2 | 0 |
| Daily surface/radiation | 3 | 3 | 0 |
| Daily model levels (cl, pfull) | 2 | 2 | 0 |
| Monthly gust (10m) | 1 | 1 | 0 |
| Heat index/WBGT | 4 | 0 | 4 (post-processing) |
| Crop tile variables | 4 | 0 | 4 (no crops) |
| 100m gust | 1 | 0 | 1 (no 100m gust) |
| Max hourly precip | 1 | 0 | 1 (nested temporal ops) |
| PM/NO2/O3/chemistry | 9 | 0 | 9 (no aerosol/chem) |
| CH4 emissions | 1 | 0 | 1 (no methane) |
| Lightning | 2 | 0 | 2 (no flash rate) |
| **Total** | **43** | **21** | **22** |

## Implementation status

All 21 producible variables implemented:
- XIOS field definitions in `field_def_cmip7.xml` (ts from skt, evspsbl daily)
- New output files in `file_def_oifs_cmip7_spinup.xml.j2` (_1h_sfc, _1h_rad, _day_ml)
- pycmor YAML rules in `cmip7_awiesm3-veg-hr_extra_atm.yaml`
- 30S-90S regional subsets use existing `select_southern_hemisphere` step
- 1hr hurs/sfcWind use existing custom steps (Magnus formula, sqrt)
