# CMIP7 VEG Land Variables -- Rule Implementation TODO

Variables from 3 CSVs in `veg_land/`: 88 total rows, 87 unique (name, freq) pairs.
These are additional land, landIce, and landIce_land variables for the VEG experiment tier.

Model constraints:
- Land surface: HTESSEL (4-layer soil, single-layer snow, no permafrost scheme, no groundwater)
- Vegetation: LPJ-GUESS 4.1.2 (run_landcover=0 → natural veg only, no land-use transitions)
- Fire: BLAZE (SIMFIRE-driven burned area + fire emissions)
- BVOC: disabled (ifbvoc=0), methane: disabled (ifmethane=0)
- No river routing model (rnfmap only redistributes runoff to coast)
- No interactive ice sheet (IFS prescribes glaciated areas as 10m water mass equivalent)
- LPJ-GUESS output: plain-text .out files (3 formats: monthly Jan..Dec, monthly Lut Mth/psl/crp/pst/urb, yearly)

---

## IFS/HTESSEL variables (XIOS output)

### 3hr fields (XIOS derived, deaccum /3600 for 1h IFS→XIOS)

- [x] **mrro** (3hr) -- Total Runoff (`kg m-2 s-1`) -- XIOS: `ro*1000/3600`
- [x] **mrros** (3hr) -- Surface Runoff (`kg m-2 s-1`) -- XIOS: `sro*1000/3600`
- [x] **mrsol** (3hr, sdepth100cm) -- Soil Moisture in Top 1m (`kg m-2`) -- XIOS: `1000*(swvl1*0.07+swvl2*0.21+swvl3*0.72)`
- [x] **esn** (day) -- Snow Evaporation (`kg m-2 s-1`) -- XIOS: `-es*1000/3600`
- [x] **srfrad** (3hr) -- Net Surface Radiation (`W m-2`) -- XIOS: `(ssr+str)/3600`
- [x] **tslsi** (3hr) -- Surface Temperature Land/Sea Ice (`K`) -- from `skt`
- [x] **hfdsl** (3hr) -- Ground Heat Flux (`W m-2`) -- XIOS: `(ssr+str+sshf+slhf)/3600`

### Daily/monthly fields (XIOS derived)

- [x] **evspsblpot** (day) -- Potential Evapotranspiration (`kg m-2 s-1`) -- XIOS: `-pev*1000/3600`
- [x] **evspsblpot** (mon) -- same at monthly
- [x] **mrrob** (day) -- Subsurface Runoff (`kg m-2 s-1`) -- XIOS: `ssro*1000/3600`
- [x] **sbl** (day) -- Sublimation (`kg m-2 s-1`) -- XIOS: `-es*1000/3600`
- [x] **sbl** (mon) -- same at monthly
- [x] **snd** (day) -- Snow Depth (`m`) -- custom step: `sd*1000/rsn` (SWE→physical depth)
- [x] **snm** (day) -- Snow Melt (`kg m-2 s-1`) -- XIOS: `smlt*1000/3600`
- [x] **tsn** (day) -- Snow Internal Temperature (`K`) -- already output daily

### Daily fields with custom pycmor steps (temporal differencing)

- [x] **dgw** (day) -- Change in Groundwater (`kg m-2`) -- `Δ(swvl4*1.89*1000)` via `compute_temporal_diff`
- [x] **dsn** (day) -- Change in SWE (`kg m-2`) -- `Δ(sd*1000)` via `compute_temporal_diff`
- [x] **dsw** (day) -- Change in Surface Water Storage (`kg m-2`) -- via `compute_temporal_diff`
- [x] **mrtws** (day) -- Terrestrial Water Storage (`kg m-2`) -- via `compute_mrtws`

### Snow/ice variables (from landIce_land CSV)

- [x] **hfdsn** (day) -- Downward Heat Flux into Snow (`W m-2`) -- approximation from energy balance or `lambda*(Tsn-Tsoil_L1)/dz_snow` (HIGH priority)
- [x] **hfdsn** (mon) -- same at monthly

### NOT producible from IFS/HTESSEL

- ~~**evspsblsoi** (3hr)~~ -- Bare Soil Evaporation -- internal HTESSEL `PDHWLS(:,1,9)`, needs source code changes
- ~~**evspsblveg** (3hr)~~ -- Canopy Water Evaporation -- internal HTESSEL, not in XIOS
- ~~**tran** (3hr)~~ -- Transpiration -- FullPos field CTP, GRIB code -9999, not accessible via XIOS
- ~~**qgwr** (day)~~ -- Groundwater Recharge -- no groundwater scheme in HTESSEL
- ~~**rivo** (day)~~ -- River Discharge -- no river routing model (only rnfmap)
- ~~**sw** (day)~~ -- Surface Water Storage -- no surface water scheme in HTESSEL
- ~~**wtd** (day)~~ -- Water Table Depth -- no groundwater scheme
- ~~**drivw** (day)~~ -- Change in River Storage -- no river routing model
- ~~**pflw** (day+mon)~~ -- Liquid Water in Permafrost -- no permafrost scheme
- ~~**tpf** (day+mon)~~ -- Permafrost Layer Thickness -- no permafrost scheme
- ~~**lwsnl** (day+mon)~~ -- Liquid Water in Snow Layer -- single-layer snow, no liquid tracking
- ~~**sootsn** (mon)~~ -- Snow Soot Content -- needs CAMS aerosol deposition
- ~~**agesno** (mon)~~ -- Mean Age of Snow -- no snow age tracer in HTESSEL

---

## LPJ-GUESS variables (plain-text .out files)

All need custom `load_lpjguess_monthly` / `load_lpjguess_yearly` / `load_lpjguess_lut_monthly` loaders.
Data path: `.../outdata/lpj_guess/{period}/run1/`

### Yearly fraction variables (Eyr)

- [x] **baresoilFrac** (yr) -- Bare Soil Fraction (`%`) -- from `baresoilFrac_yearly.out`, `Total` column
- [x] **cropFrac** (yr) -- Crop Cover (`%`) -- from `cropFrac_yearly.out` (all zeros, run_landcover=0)
- [x] **grassFrac** (yr) -- Natural Grass (`%`) -- from `grassFrac_yearly.out`
- [x] **shrubFrac** (yr) -- Shrub Cover (`%`) -- from `shrubFrac_yearly.out`
- [x] **treeFrac** (yr) -- Tree Cover (`%`) -- from `treeFrac_yearly.out`

### Yearly land-use tile variables (Eyr) -- note: psl column has data, crp/pst/urb = 0

- [x] **cLitterLut** (yr) -- Litter Carbon (`kg m-2`) -- from `cLitterLut_yearly.out`, Lut format
- [x] **cProductLut** (yr) -- Product Carbon (`kg m-2`) -- from `cProductLut_yearly.out` (all zeros)
- [x] **cSoilLut** (yr) -- Soil Carbon (`kg m-2`) -- from `cSoilLut_yearly.out`
- [x] **cVegLut** (yr) -- Vegetation Carbon (`kg m-2`) -- from `cVegLut_yearly.out`
- [x] **fracLut** (yr) -- Land-Use Tile Fraction (`%`) -- from `fracLut_yearly.out` (psl=100%)
- [x] **fracInLut** (yr) -- Fraction Transferred In (`%`) -- from `fracInLut_yearly.out` (all zeros)
- [x] **fracOutLut** (yr) -- Fraction Transferred Out (`%`) -- from `fracOutLut_yearly.out` (all zeros)

### Monthly land-use tile variables (Emon) -- Lon/Lat/Year/Mth/psl/crp/pst/urb format

- [x] **fracLut** (mon) -- Land-Use Tile Fraction (`%`) -- from `fracLut_monthly.out`
- [x] **gppLut** (mon) -- GPP on Tiles (`kg m-2 s-1`) -- from `gppLut_monthly.out`
- [x] **laiLut** (mon) -- LAI on Tiles (`1`) -- from `laiLut_monthly.out`
- [x] **mrsolLut** (mon) -- Soil Moisture on Tiles (`kg m-2`) -- from `mrsoLut_monthly.out` (note: filename mrsoLut)
- [x] **nppLut** (mon) -- NPP on Tiles (`kg m-2 s-1`) -- from `nppLut_monthly.out`
- [x] **raLut** (mon) -- Autotrophic Resp. on Tiles (`kg m-2 s-1`) -- from `raLut_monthly.out`
- [x] **rhLut** (mon) -- Heterotrophic Resp. on Tiles (`kg m-2 s-1`) -- from `rhLut_monthly.out`
- [x] **irrLut** (mon) -- Irrigation on Tiles (`kg m-2 s-1`) -- from `irrLut_monthly.out` (all zeros)
- [x] **fLulccAtmLut** (mon) -- LULCC Carbon to Atm (`kg m-2 s-1`) -- from `fLulccAtmLut_monthly.out` (all zeros)

### Monthly nitrogen/carbon variables (Emon) -- Jan..Dec format

- [x] **fBNF** (mon) -- Biological N Fixation (`kg m-2 s-1`) -- from `fBNF_monthly.out`
- [x] **fLuc** (mon) -- Net C from Land-Use Change (`kg m-2 s-1`) -- from `fLuc_monthly.out` (all zeros)
- [x] **fNgas** (mon) -- Total N to Atmosphere (`kg m-2 s-1`) -- from `fNgas_monthly.out`
- [x] **fNgasFire** (mon) -- N to Atm from Fire (`kg m-2 s-1`) -- from `fNgasFire_monthly.out`
- [x] **fNLandToOcean** (mon) -- Lateral N Transfer (`kg m-2 s-1`) -- from `fNLandToOcean_monthly.out`
- [x] **fNleach** (mon) -- N Leaching (`kg m-2 s-1`) -- from `fNleach_monthly.out`
- [x] **fNLitterSoil** (mon) -- Litter to Soil N (`kg m-2 s-1`) -- from `fNLitterSoil_monthly.out`
- [x] **fNloss** (mon) -- Total N Loss (`kg m-2 s-1`) -- from `fNloss_monthly.out`
- [x] **fNup** (mon) -- Plant N Uptake (`kg m-2 s-1`) -- from `fNup_monthly.out`
- [x] **nLand** (mon) -- Total Terrestrial N (`kg m-2`) -- from `nLand_monthly.out`
- [x] **nLitter** (mon) -- Litter N (`kg m-2`) -- from `nLitter_monthly.out`
- [x] **nMineral** (mon) -- Mineral N (`kg m-2`) -- from `nMineral_monthly.out`
- [x] **nProduct** (mon) -- Product N (`kg m-2`) -- from `nProduct_monthly.out` (all zeros)
- [x] **nSoil** (mon) -- Soil N (`kg m-2`) -- from `nSoil_monthly.out`
- [x] **nVeg** (mon) -- Vegetation N (`kg m-2`) -- from `nVeg_monthly.out`
- [x] **treeFracBdlDcd** (mon) -- Broadleaf Deciduous Tree Fraction (`%`) -- from `treeFracBdlDcd_monthly.out`

### NOT producible from LPJ-GUESS

- ~~**vegHeight** (mon)~~ -- only `vegHeightTree_monthly.out` exists (tree-only, not grid-cell mean)
- ~~**fNVegSoil** (mon)~~ -- no output file; LPJ-GUESS has fNVegLitter but not direct veg-to-soil
- ~~**hflsLut** (mon)~~ -- surface energy balance variable, not from vegetation model
- ~~**hfssLut** (mon)~~ -- surface energy balance variable, not from vegetation model
- ~~**nbpLut** (mon)~~ -- no per-tile variant; only `nbp_monthly.out` (gridcell total)
- ~~**sweLut** (mon)~~ -- no per-tile variant; only `snw_monthly.out` (gridcell total)
- ~~**tasLut** (mon)~~ -- atmospheric variable, not per-tile from LPJ-GUESS
- ~~**tsLut** (mon)~~ -- soil temperature exists gridcell-only (`tsl_monthly.out`), no Lut
- ~~**gppVgt** (day)~~ -- no daily per-PFT output from LPJ-GUESS
- ~~**laiVgt** (day)~~ -- no daily per-PFT output
- ~~**nppVgt** (day)~~ -- no daily per-PFT output
- ~~**raVgt** (day)~~ -- no daily per-PFT output
- ~~**rhVgt** (day)~~ -- no daily per-PFT output

---

## Summary

| Category | Count | Done | Blocked |
|----------|-------|------|---------|
| IFS 3hr fields (XIOS) | 7 | 7 | 0 |
| IFS daily/monthly (XIOS) | 8 | 8 | 0 |
| IFS daily (custom temporal diff) | 4 | 4 | 0 |
| IFS snow heat flux (approx) | 2 | 2 (approx) | 0 |
| IFS not producible | 17 | 0 | 17 |
| LPJ-GUESS yearly fractions | 5 | 5 | 0 |
| LPJ-GUESS yearly Lut | 7 | 7 | 0 |
| LPJ-GUESS monthly Lut | 9 | 9 | 0 |
| LPJ-GUESS monthly N/C | 16 | 16 | 0 |
| LPJ-GUESS not producible | 13 | 0 | 13 |
| **Total** | **88** | **58** | **30** |

## Implementation status

All 58 producible variables are implemented:
- XIOS field definitions in `field_def_cmip7.xml` (deaccum /3600, 1h IFS→XIOS)
- Output files in `file_def_oifs_cmip7_spinup.xml.j2` (_3h_land, _day_land, _mon_land)
- pycmor YAML rules in `cmip7_awiesm3-veg-hr_land.yaml`
- Custom loaders: `load_lpjguess_yearly`, `load_lpjguess_yearly_lut`, `load_lpjguess_monthly_lut`
- Custom steps: `compute_temporal_diff`, `compute_mrtws`, `compute_snd`
