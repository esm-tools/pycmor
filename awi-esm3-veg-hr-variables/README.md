# AWI-ESM3-VEG-HR CMIP7 Variable Configuration

CMIP7 CMORization configuration for AWI-ESM3-VEG-HR.

## Model Configuration

Reference runtime: `AWI-ESM3-VEG-HR-CMIP7-Spinup_cont2` (awiesm3-v3.4.1)

### OpenIFS (IFS CY48R1)
- **Resolution**: TCo319 spectral, L91 vertical levels
- **Output grid**: 0.25deg regular (1440x720), interpolated via XIOS/FullPos
- **Time step**: 900 s
- **Radiation**: ecRad, called every 3 hours
- **Land surface**: HTESSEL (4-layer soil, snow scheme, Farquhar photosynthesis)
- **Aerosol**: MACv2-SP simple plumes (no CAMS, no M7)
- **CO2**: concentration-driven (no prognostic CO2 tracer)
- **Wave model**: WAM (2-way coupled)
- **I/O**: XIOS 2.5-ece

### FESOM 2.6
- **Mesh**: DARS unstructured (~3.1M surface nodes, ~10 km nominal)
- **Vertical**: 56 z-levels (57 interfaces), linear free surface
- **Sea ice**: Built-in single-category, mEVP rheology, melt ponds enabled
- **Diagnostics**: ldiag_cmor=.true. (CMIP scalar diagnostics)

### LPJ-GUESS 4.1.2
Config from `global.ins` via `run_coupled_4_1_2.ins`:
- **Fire model**: BLAZE (uses SIMFIRE internally for burned area prediction)
- **BVOC**: disabled (ifbvoc=0)
- **Nitrogen cycle**: enabled (ifnlim=1, ifcentury=1)
- **Land cover**: natural vegetation only (run_landcover=0)
- **Methane**: disabled (ifmethane=0)
- **Vegetation mode**: cohort, npatch=15
- **PFTs**: 12 global + 8 arctic shrub/tundra (~20 active natural PFTs)
- **CO2**: concentration-driven via OASIS coupling from atmosphere
- **CMIP output**: extensive monthly + yearly .out files (166 output files defined in `lpjg_output.ins`)
- **Output format**: plain-text .out files (no XIOS)
- **Coupling to IFS**: daily via OASIS-MCT (sends LAI, veg type/fraction; receives T, precip, radiation, soil state)

### Coupling (OASIS3-MCT 5.0)
- **Atm-Ocean**: 2-hourly (7200 s)
- **Atm-Vegetation**: daily (86400 s)
- **Runoff mapping**: rnfmap v1.1

### Ice Sheet
- **No interactive ice sheet model** (no PISM, no Yelmo, no BISICLES)
- IFS prescribes glaciated areas as grid cells with 10 m water mass equivalent
- Greenland/Antarctic ice sheets are static boundary conditions

### What this model does NOT have
- No interactive ice sheet model
- No prognostic aerosol (no CAMS, no M7 -- only MACv2-SP prescribed plumes)
- No atmospheric chemistry
- No interactive ozone (O3 prescribed from climatology, not prognostic)
- No CO2 tracer (concentration-driven)
- No ice thickness distribution (single-category sea ice)
- No icebergs
- No methane cycle
- No BVOC emissions

## XIOS XML Configuration (top level)

These files configure the OpenIFS/XIOS output pipeline. XIOS expressions handle unit conversions, deaccumulation, and sign flips at output time so pycmor only needs to add metadata.

| File | Purpose |
|------|---------|
| `iodef.xml` | XIOS top-level entry point, references context files |
| `context_ifs.xml.j2` | IFS XIOS context, includes all `*_def.xml` and `file_def` |
| `field_def_cmip7.xml` | All CMIP7 field definitions: raw IFS fields + derived expressions |
| `file_def_oifs_cmip7_spinup.xml.j2` | Output file definitions: fields, frequencies, operations (average/instant/min/max) |
| `axis_def.xml` | Vertical axes: plev19, plev3 (850/500/250 hPa), model levels |
| `grid_def.xml` | Grids: regular_sfc, regular_pl, regular_pl3, regular_ml |
| `domain_def.xml.j2` | Domain definitions for reduced Gaussian to regular grid interpolation |

## FESOM Configuration (top level)

| File | Purpose |
|------|---------|
| `namelist.io` | FESOM2 I/O namelist: ocean + sea-ice output variables and frequencies |

## Per-realm Subdirectories

Each subdirectory contains:
- **CSV files** -- CMIP7 Data Request variables for that realm (from CMIP7_DReq_Software)
- **YAML file** -- pycmor rules mapping model output to CMOR-compliant files
- **TODO file** -- implementation status, blockers, research notes, and OIFS source investigation

### Core (CMIP7 mandatory variables)

| Directory | Realm | Model | Rules | Variables | Key notes |
|-----------|-------|-------|-------|-----------|-----------|
| `core_atm/` | Atmosphere | OpenIFS | 76 | 45 unique | Monthly/daily/sub-daily (3hr, 6hr, 1hr); surface, plev19, plev3, model levels |
| `core_land/` | Land | OpenIFS/HTESSEL | 11 | 11 | XIOS-derived + pipeline-computed; 6 variables deferred to lrcs_land |
| `core_ocean/` | Ocean | FESOM 2.6 | 25 | 25 | Monthly 2D/3D, daily, fx; includes mass transport and zostoga pipelines |
| `core_seaice/` | Sea Ice | FESOM 2.6 | 9 | 8 unique | Monthly + daily siconc; velocity rotation via vec_autorotate |

### LRCS (additional priority variables)

| Directory | Realm | Model | Rules | Key notes |
|-----------|-------|-------|-------|-----------|
| `lrcs_ocean/` | Ocean | FESOM 2.6 | 45 | Decadal, yearly tendencies (6 with FESOM source mods), scalar diagnostics; ~12 blocked by basin masks |
| `lrcs_seaice/` | Sea Ice | FESOM 2.6 | 40+ | Heat/salt fluxes, tendencies, melt ponds, stress, hemisphere scalars; some blocked by single-category ice |
| `lrcs_land/` | Land | OIFS/LPJ-GUESS | 6 | 6 deferred variables: 3 from LPJ-GUESS (evspsblsoi, evspsblveg, mrfso), 3 from IFS static fields (sftgif, mrsofc, rootd) |
| `veg_atm/` | Atmos/Aerosol | OpenIFS + LPJ-GUESS | 27 | 38 variables: 27 implemented (3hr rad/flux, plev6, daily snow, lwp, 7 fire emissions), 11 blocked |
| `veg_land/` | Land | OpenIFS/HTESSEL + LPJ-GUESS | 58 | 88 variables: 22 IFS (3hr/day/mon hydrology, snow), 36 LPJ-GUESS (N-cycle, fractions, Lut), 30 blocked |
| `veg_seaice/` | Sea Ice | FESOM 2.6 | 1 | 4 variables: 1 implemented (daily sisnhc from m_snow/a_ice), 3 blocked (2 ITD, 1 missing physics) |
| `extra_land/` | Land | OpenIFS/HTESSEL + LPJ-GUESS | 13 | 19 variables: 2 fx, 7 LPJ-GUESS (PFT fracs, LAI), 3 IFS hydrology, 1 hourly tas; 6 blocked (irrigation, river, root zone) |
| `extra_atm/` | Atmos/Aerosol | OpenIFS | 21 | 43 variables: 13 1hr (fluxes, rad, 30S-90S subsets), 2 3hr, 5 daily, 1 monthly gust; 22 blocked (aerosol/chem, crops, heat index, lightning) |

### CAP7 (high-priority additional variables)

| Directory | Realm | Model | Rules | Key notes |
|-----------|-------|-------|-------|-----------|
| `cap7_atm/` | Atmosphere | OpenIFS | 58 | 233 variables: 79 already in core/veg/extra/lrcs, 58 new (daily radiation/fluxes/precip, 6hr ml+plev7h, 1hr instant, monthly ml); ~96 blocked (17 COSP, 21 tendencies, 9 aerosol, 5 CO2, 4 reff, ~40 IFS source) |
| `cap7_ocean/` | Ocean | FESOM 2.6 | 3 | 43 variables: 26 already in core/lrcs, 3 new (daily tossq, monthly volcello, friver); 14 blocked (no icebergs/SF6/geothermal/bigthetao, basin masks, namelist changes for hfx/hfy/3hr stress) |
| `cap7_seaice/` | Sea Ice | FESOM 2.6 | 9 | 21 variables: 9 already in core/lrcs/veg, 9 new (daily sithick/snd/siu/siv, monthly sieqthick/snw/evspsbl/prra/prsn); 3 blocked (sisali constant, sitempsnic internal, snc single-category) |
| `cap7_land/` | Land | OpenIFS/LPJ-GUESS | 54 | 98 variables: 12 already in core/lrcs/veg/extra, 54 new (49 LPJ-GUESS monthly carbon/flux/fraction, 4 IFS daily mrro/mrso/mrsol/tslsi, 1 IFS 1hr tas); 2 need new custom step (per-soil-layer tsl/mrsol); 30 blocked (15 per-PFT group, 13 no .out file, 2 no depth-resolved cSoil) |
| `cap7_aerosol/` | Aerosol/AtmosChem | OpenIFS | 2 | 52 variables: 2 new (od550aer from MACv2-SP, toz from prescribed ozone); 50 blocked (no prognostic aerosol, no interactive chemistry, no CO2 tracer, no cloud microphysics diag, no deposition) |

## Custom Pipeline Steps

Complex variables that cannot be expressed as XIOS expressions are computed in `../examples/custom_steps.py`.

### Atmosphere pipelines
- **sfcWind**: sqrt(u10^2 + v10^2) from 10u + 10v
- **hurs**: Magnus formula from 2t + 2d
- **huss**: Tetens formula from 2d + sp
- **clwvi**: tclw + tciw (liquid + ice water path)

### Land pipelines
- **snc**: snow cover saturation curve from sd (threshold 15mm water equiv)
- **areacella**: spherical grid cell area from lat/lon coordinates
- **slthick**: constant HTESSEL soil layer thicknesses [0.07, 0.21, 0.72, 1.89] m

### LPJ-GUESS loaders and fire emission pipelines
- **load_lpjguess_monthly**: custom loader for LPJ-GUESS plain-text .out files (Lon/Lat/Year/Jan..Dec)
- **load_lpjguess_yearly**: loader for yearly .out files (Lon/Lat/Year/Total)
- **load_lpjguess_yearly_lut**: loader for yearly Lut .out files (Lon/Lat/Year/psl/crp/pst/urb)
- **load_lpjguess_monthly_lut**: loader for monthly Lut .out files (Lon/Lat/Year/Mth/psl/crp/pst/urb)
- **compute_fire_emission**: converts fFireAll (kgC/m2/s) to species emissions using Andreae (2019) savanna/grassland emission factors (BC, CH4, CO, DMS, OA, SO2, NMVOC)

### Land hydrology/snow custom steps
- **compute_temporal_diff**: temporal differencing for dgw, dsn, dsw, dcw, dslw (daily storage changes)
- **compute_mrtws**: terrestrial water storage summation (soil + snow + skin reservoir)
- **compute_snd**: physical snow depth from SWE and snow density (sd*1000/rsn)
- **compute_mrsow**: total soil wetness ratio (weighted mean swvl / porosity)
- **sum_lpjguess_monthly_files**: load and sum multiple LPJ-GUESS .out files (for c3PftFrac)
- **select_southern_hemisphere**: lat subset for 30S-90S regional variables (orog, tas)

### CAP7 atmosphere custom steps
- **compute_rtmt**: net downward radiative flux at model top (rsdt - rsut - rlut)
- **extract_single_plevel**: extract single pressure level from multi-level dataset (ta@700hPa, wap@500hPa)

### CAP7 sea ice custom steps
- **compute_snd_from_msnow**: snow depth on ice from m_snow/a_ice (unused after h_snow switched to daily)

### Ocean pipelines
- **zostoga**: global thermosteric sea level via gsw/TEOS-10
- **mass transport** (umo/vmo/wmo): Boussinesq approximation (velocity x rho_0 x cell area)
- **bottom/surface extract**: tob, sob from 3D fields; uos, vos from daily 3D
- **vertical integration**: scint, phcint, opottempmint, somint
- **fx pipelines**: areacello, deptho, sftof, thkcello, masscello, volcello from mesh

### Sea ice pipelines
- **siconc/simpconc**: fraction to percent conversion
- **sispeed**: sqrt(uice^2 + vice^2)
- **sihc/sisnhc**: heat content from ice/snow thickness + thermodynamic constants
- **sisnhc (daily)**: derived from daily m_snow/a_ice (h_snow not available daily)
- **sistressave/sistressmax**: stress invariants from sigma tensor components
- **sitempbot**: freezing temperature from SSS
- **sifb**: freeboard from ice/snow thickness and density ratios
- **ice mass transport**: uice/vice x m_ice
- **hemisphere integrals**: sisnmass N/S from m_snow x cell_area

### FESOM2 source code modifications
Six new diagnostic outputs added to `gen_modules_cmor_diag.F90`:
- opottemptend, opottempdiff, opottemprmadvect (temperature tendencies)
- osalttend, osaltdiff, osaltrmadvect (salinity tendencies)
- rsdoabsorb (shortwave absorption by ocean layer)

## Summary of Implementation Status

| Realm | Core done | Core total | LRCS done | LRCS total | Blocked |
|-------|-----------|------------|-----------|------------|---------|
| Atmosphere | 76 | 76 | -- | -- | 3 items need runtime verification |
| Land | 11 | 17 | 0 | 6 | 3 need OIFS source changes, 3 derivable offline |
| Ocean | 25 | 27 | 45 | ~80 | ~12 need basin masks, ~8 need online diag |
| Sea Ice | 9 | 9 | 40+ | ~70 | ITD/age/ridge tracers not enabled |
