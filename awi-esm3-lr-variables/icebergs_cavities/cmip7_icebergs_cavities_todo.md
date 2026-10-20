# CMIP7 iceberg and ice-shelf cavity variables for AWI-ESM3 LR

LR v3.5.1-cc on core3 runs with `use_icebergs` and `use_cavity` on (HR coverage notes assume both off). Roots: ficeberg, hfibthermds, flandice, libmassbf, sftflf, iareafl, licalvf, lifmassbf, tendlicalvf, litempbot, all variants in DReq v1.2.2.5.
Decision by Jan, 2026-10-19: `ocean.ficeberg.tavg-ol-hxy-sea.3hr.glb` is not produced; it is listed and left out of the totals. 3-hourly flandice is undecided and counted.
Regional landIce grids sized on FESOM nodes: `ata` = nodes south of 60S, `grl` = nodes in 58N-90N, 75W-10W. `fis` fields count only cavity nodes as non-fill.

## Summary (per model year, counted variables)

| Priority | Variables | Files/yr | GB/yr (central) | GB/yr (low-high) | GB/yr raw float32 |
|---|---|---|---|---|---|
| High | 5 | 5 | 0.56 | 0.26-0.59 | 3.10 |
| Medium | 32 | 32 | 0.16 | 0.11-0.16 | 0.52 |
| **Total** | **37** | **37** | **0.71** | 0.37-0.75 | 3.63 |

| Frequency | Variables | Files/yr | GB/yr (central) | GB/yr (low-high) |
|---|---|---|---|---|
| yr | 18 | 18 | 0.02 | 0.02-0.02 |
| mon | 18 | 18 | 0.22 | 0.17-0.23 |
| 3hr | 1 | 1 | 0.47 | 0.18-0.50 |

| Priority \ freq | yr | mon | 3hr |
|---|---|---|---|
| High | - | 4 / 0.09 | 1 / 0.47 |
| Medium | 18 / 0.02 | 14 / 0.13 | - |

(cells: variables / GB per year)

## Totals with and without the expensive parts

| Selection | Variables | Files/yr | GB/yr | GB/yr low-high | TB per 100 model years |
|---|---|---|---|---|---|
| All counted variables | 37 | 37 | 0.71 | 0.37-0.75 | 0.07 |
| Without sub-monthly 3D fields | 37 | 37 | 0.71 | 0.37-0.75 | 0.07 |
| Without the 30S-90S subsets | 37 | 37 | 0.71 | 0.37-0.75 | 0.07 |
| Without sub-monthly 3D and without 30S-90S | 37 | 37 | 0.71 | 0.37-0.75 | 0.07 |
| Core + High only | 5 | 5 | 0.56 | 0.26-0.59 | 0.06 |
| Core + High, without sub-monthly 3D | 5 | 5 | 0.56 | 0.26-0.59 | 0.06 |

Once per run (fx, climatologies, not in the per-year numbers): 1 variables, 12.2 MB.

Dropped (listed, not counted):

- `ocean.ficeberg.tavg-ol-hxy-sea.3hr.glb`: dropped (Jan, 2026-10-19) (1035 MB/yr compressed, 121051 MB/yr raw)

## Opportunities

| Opportunity | Variables | H / M / L | Only in this one |
|---|---|---|---|
| 39 Ice sheet mass loss, contributions to sea level rise an | 34 | 4 / 30 / 0 | 30 |
| 51 Paleoclimate Research at the Interface between Past, Pr | 4 | 4 / 0 / 0 | 0 |
| 47 Ocean Changes, Drivers and Impacts | 4 | 0 / 4 / 0 | 2 |
| 16 Climate impacts on marine biodiversity and ecosystems | 2 | 2 / 0 / 0 | 2 |
| 82 Robust Risk Assessment of Tipping Points | 1 | 1 / 0 / 0 | 0 |
| 81 Dynamical Downscaling | 1 | 1 / 0 / 0 | 0 |

Greedy cover (add the opportunity that brings the most new compound names):

| Step | Opportunity | New | Running total |
|---|---|---|---|
| 1 | 39 Ice sheet mass loss, contributions to sea level ri | 34 | 34 |
| 2 | 47 Ocean Changes, Drivers and Impacts | 3 | 37 |
| 3 | 16 Climate impacts on marine biodiversity and ecosyst | 2 | 39 |

## Feasibility filter: pending

Not done in this phase.

Peer pre-check (informational, 25 of 39 entries, flux variables only): W/C/S/N from `notes/lr_peer_session/flux_table.md` (W run writes the inputs, C namelist change, S source change, N not in the model). Where `awi-esm3-veg-hr-variables/lr_fluxes_bgc_co2_ice_todo.md` (the parallel session's newer plan) puts the variable in a different section, its code follows after a slash (W pycmor only, X XML + namelist.io, S source change). That file also lists exclusions it calls decided (coccolithophores, carbon isotopes); they are not applied here, only the 3-hourly 3D ficeberg drop by Jan is.

## Variables

### High (7 entries, 0.56 GB/yr)

- [ ] **sftflf** (fx, glb) `landIce.sftflf.ti-u-hxy-u.fx.glb`: Floating Ice Shelf Area Percentage [%]; dims `longitude latitude`; 12.2 MB per run (ocn_mask_fx); opps 39H 51H; 38 expts
- [ ] **ficeberg** (mon, glb) `ocean.ficeberg.tavg-ol-hxy-sea.mon.glb`: Water Flux into Sea Water from Icebergs [kg m-2 s-1]; dims `longitude latitude olevel time`; 49.6 MB/yr (sparse2d_mon); opps 47M 81H; 72 expts; peer pre-check W
- [ ] **ficeberg** (mon, glb) `ocean.ficeberg.tavg-u-hxy-sea.mon.glb`: Water Flux into Sea Water from Icebergs [kg m-2 s-1]; dims `longitude latitude time`; 14.1 MB/yr (sparse2d_mon); opps 39H 47M 51H; 73 expts; peer pre-check W
- [ ] **flandice** (mon, glb) `ocean.flandice.tavg-u-hxy-sea.mon.glb`: Water Flux into Sea Water from Land Ice [kg m-2 s-1]; dims `longitude latitude time`; 14.1 MB/yr (sparse2d_mon); opps 39H 51H; 38 expts; peer pre-check W
- [ ] **sftflf** (mon, glb) `landIce.sftflf.tavg-u-hxy-u.mon.glb`: Floating Ice Shelf Area Percentage [%]; dims `longitude latitude time`; 12.3 MB/yr (ocn_mask_fx); opps 39H 51H 82H; 47 expts
- [ ] **ficeberg** (3hr, glb) `ocean.ficeberg.tavg-ol-hxy-sea.3hr.glb`: Water Flux into Sea Water from Icebergs [kg m-2 s-1]; dims `longitude latitude olevel time`; 1035.2 MB/yr (sparse2d_mon); opps 16H; 18 expts; **dropped (Jan, 2026-10-19)**, not in totals; peer pre-check S
- [ ] **flandice** (3hr, glb) `ocean.flandice.tavg-u-hxy-sea.3hr.glb`: Water Flux into Sea Water from Land Ice [kg m-2 s-1]; dims `longitude latitude time`; 467.4 MB/yr (sparse2d_day); opps 16H; 18 expts; peer pre-check C/X

### Medium (32 entries, 0.16 GB/yr)

- [ ] **iareafl** (yr, ata) `landIce.iareafl.tavg-u-hm-fis.yr.ata`: Area Covered by Floating Ice Shelves [m2]; dims `time`; 0.0 MB/yr (scalar); opps 39M; 28 expts
- [ ] **iareafl** (yr, grl) `landIce.iareafl.tavg-u-hm-fis.yr.grl`: Area Covered by Floating Ice Shelves [m2]; dims `time`; 0.0 MB/yr (scalar); opps 39M; 28 expts
- [ ] **libmassbf** (yr, ata) `landIce.libmassbf.tavg-u-hxy-fis.yr.ata`: Basal Specific Mass Balance Flux of Floating Ice Shelf [kg m-2 s-1]; dims `longitude latitude time`; 2.4 MB/yr (sparse2d_mon); opps 39M; 28 expts; peer pre-check W
- [ ] **libmassbf** (yr, grl) `landIce.libmassbf.tavg-u-hxy-fis.yr.grl`: Basal Specific Mass Balance Flux of Floating Ice Shelf [kg m-2 s-1]; dims `longitude latitude time`; 0.6 MB/yr (sparse2d_mon); opps 39M; 28 expts; peer pre-check N
- [ ] **libmassbf** (yr, ata) `landIce.libmassbf.tavg-u-hxy-gis.yr.ata`: Basal Specific Mass Balance Flux of Grounded Ice Sheet [kg m-2 s-1]; dims `longitude latitude time`; 2.4 MB/yr (sparse2d_mon); opps 39M; 28 expts; peer pre-check N
- [ ] **libmassbf** (yr, grl) `landIce.libmassbf.tavg-u-hxy-gis.yr.grl`: Basal Specific Mass Balance Flux of Grounded Ice Sheet [kg m-2 s-1]; dims `longitude latitude time`; 0.6 MB/yr (sparse2d_mon); opps 39M; 28 expts; peer pre-check N
- [ ] **licalvf** (yr, ata) `landIce.licalvf.tavg-u-hxy-is.yr.ata`: Land Ice Calving Flux [kg m-2 s-1]; dims `longitude latitude time`; 2.4 MB/yr (sparse2d_mon); opps 39M; 28 expts; peer pre-check N
- [ ] **licalvf** (yr, grl) `landIce.licalvf.tavg-u-hxy-is.yr.grl`: Land Ice Calving Flux [kg m-2 s-1]; dims `longitude latitude time`; 0.6 MB/yr (sparse2d_mon); opps 39M; 28 expts; peer pre-check N
- [ ] **lifmassbf** (yr, ata) `landIce.lifmassbf.tavg-u-hxy-is.yr.ata`: Land Ice Vertical Front Mass Balance Flux [kg m-2 s-1]; dims `longitude latitude time`; 2.4 MB/yr (sparse2d_mon); opps 39M; 28 expts; peer pre-check N
- [ ] **lifmassbf** (yr, grl) `landIce.lifmassbf.tavg-u-hxy-is.yr.grl`: Land Ice Vertical Front Mass Balance Flux [kg m-2 s-1]; dims `longitude latitude time`; 0.6 MB/yr (sparse2d_mon); opps 39M; 28 expts; peer pre-check N
- [ ] **litempbot** (yr, ata) `landIce.litempbot.tavg-u-hxy-fis.yr.ata`: Basal Temperature of Floating Ice Shelf [K]; dims `longitude latitude time`; 2.4 MB/yr (sparse2d_mon); opps 39M; 28 expts
- [ ] **litempbot** (yr, grl) `landIce.litempbot.tavg-u-hxy-fis.yr.grl`: Basal Temperature of Floating Ice Shelf [K]; dims `longitude latitude time`; 0.6 MB/yr (sparse2d_mon); opps 39M; 28 expts
- [ ] **litempbot** (yr, ata) `landIce.litempbot.tavg-u-hxy-gis.yr.ata`: Basal Temperature of Grounded Ice Sheet [K]; dims `longitude latitude time`; 2.4 MB/yr (sparse2d_mon); opps 39M; 28 expts
- [ ] **litempbot** (yr, grl) `landIce.litempbot.tavg-u-hxy-gis.yr.grl`: Basal Temperature of Grounded Ice Sheet [K]; dims `longitude latitude time`; 0.6 MB/yr (sparse2d_mon); opps 39M; 28 expts
- [ ] **sftflf** (yr, ata) `landIce.sftflf.tavg-u-hxy-u.yr.ata`: Floating Ice Shelf Area Percentage [%]; dims `longitude latitude time`; 2.4 MB/yr (ocn_mask_fx); opps 39M; 28 expts
- [ ] **sftflf** (yr, grl) `landIce.sftflf.tavg-u-hxy-u.yr.grl`: Floating Ice Shelf Area Percentage [%]; dims `longitude latitude time`; 0.6 MB/yr (ocn_mask_fx); opps 39M; 28 expts
- [ ] **tendlicalvf** (yr, ata) `landIce.tendlicalvf.tavg-u-hm-is.yr.ata`: Total Calving Flux [kg s-1]; dims `time`; 0.0 MB/yr (scalar); opps 39M; 28 expts; peer pre-check N
- [ ] **tendlicalvf** (yr, grl) `landIce.tendlicalvf.tavg-u-hm-is.yr.grl`: Total Calving Flux [kg s-1]; dims `time`; 0.0 MB/yr (scalar); opps 39M; 28 expts; peer pre-check N
- [ ] **hfibthermds** (mon, glb) `ocean.hfibthermds.tavg-ol-hxy-sea.mon.glb`: Heat Flux into Sea Water Due to Iceberg Thermodynamics [W m-2]; dims `longitude latitude olevel time`; 101.1 MB/yr (sparse2d_mon); opps 47M; 72 expts; peer pre-check W
- [ ] **hfibthermds** (mon, glb) `ocean.hfibthermds.tavg-u-hxy-sea.mon.glb`: Heat Flux into Sea Water Due to Iceberg Thermodynamics [W m-2]; dims `longitude latitude time`; 14.1 MB/yr (sparse2d_mon); opps 47M; 72 expts; peer pre-check W
- [ ] **libmassbf** (mon, ata) `landIce.libmassbf.tavg-u-hxy-fis.mon.ata`: Basal Specific Mass Balance Flux of Floating Ice Shelf [kg m-2 s-1]; dims `longitude latitude time`; 2.5 MB/yr (sparse2d_mon); opps 39M; 28 expts; peer pre-check W
- [ ] **libmassbf** (mon, grl) `landIce.libmassbf.tavg-u-hxy-fis.mon.grl`: Basal Specific Mass Balance Flux of Floating Ice Shelf [kg m-2 s-1]; dims `longitude latitude time`; 0.6 MB/yr (sparse2d_mon); opps 39M; 28 expts; peer pre-check N
- [ ] **libmassbf** (mon, ata) `landIce.libmassbf.tavg-u-hxy-gis.mon.ata`: Basal Specific Mass Balance Flux of Grounded Ice Sheet [kg m-2 s-1]; dims `longitude latitude time`; 2.7 MB/yr (sparse2d_mon); opps 39M; 28 expts; peer pre-check N
- [ ] **libmassbf** (mon, grl) `landIce.libmassbf.tavg-u-hxy-gis.mon.grl`: Basal Specific Mass Balance Flux of Grounded Ice Sheet [kg m-2 s-1]; dims `longitude latitude time`; 0.7 MB/yr (sparse2d_mon); opps 39M; 28 expts; peer pre-check N
- [ ] **licalvf** (mon, ata) `landIce.licalvf.tavg-u-hxy-is.mon.ata`: Land Ice Calving Flux [kg m-2 s-1]; dims `longitude latitude time`; 2.7 MB/yr (sparse2d_mon); opps 39M; 28 expts; peer pre-check N
- [ ] **licalvf** (mon, grl) `landIce.licalvf.tavg-u-hxy-is.mon.grl`: Land Ice Calving Flux [kg m-2 s-1]; dims `longitude latitude time`; 0.7 MB/yr (sparse2d_mon); opps 39M; 28 expts; peer pre-check N
- [ ] **lifmassbf** (mon, ata) `landIce.lifmassbf.tavg-u-hxy-is.mon.ata`: Land Ice Vertical Front Mass Balance Flux [kg m-2 s-1]; dims `longitude latitude time`; 2.7 MB/yr (sparse2d_mon); opps 39M; 28 expts; peer pre-check N
- [ ] **lifmassbf** (mon, grl) `landIce.lifmassbf.tavg-u-hxy-is.mon.grl`: Land Ice Vertical Front Mass Balance Flux [kg m-2 s-1]; dims `longitude latitude time`; 0.7 MB/yr (sparse2d_mon); opps 39M; 28 expts; peer pre-check N
- [ ] **litempbot** (mon, ata) `landIce.litempbot.tavg-u-hxy-fis.mon.ata`: Basal Temperature of Floating Ice Shelf [K]; dims `longitude latitude time`; 2.5 MB/yr (sparse2d_mon); opps 39M; 28 expts
- [ ] **litempbot** (mon, grl) `landIce.litempbot.tavg-u-hxy-fis.mon.grl`: Basal Temperature of Floating Ice Shelf [K]; dims `longitude latitude time`; 0.6 MB/yr (sparse2d_mon); opps 39M; 28 expts
- [ ] **litempbot** (mon, ata) `landIce.litempbot.tavg-u-hxy-gis.mon.ata`: Basal Temperature of Grounded Ice Sheet [K]; dims `longitude latitude time`; 2.7 MB/yr (sparse2d_mon); opps 39M; 28 expts
- [ ] **litempbot** (mon, grl) `landIce.litempbot.tavg-u-hxy-gis.mon.grl`: Basal Temperature of Grounded Ice Sheet [K]; dims `longitude latitude time`; 0.7 MB/yr (sparse2d_mon); opps 39M; 28 expts

## Method

- DReq **v1.2.2.5** (released 2026-07-17, newest tag of CMIP7_DReq_Content; read offline from the data_request_api cache). All 46 opportunities, all priorities. Priority = highest priority any opportunity gives the variable. Experiments = union over all requesting opportunities.
- LR ocean grid core3: 220509 surface nodes, 426403 elements, 48 levels = **47 layers** (aux3d.out), cavity nodes 10047. Wet 3D node-layers 6030718 of 10363923 (58.2 %). 30S-90S: 93000 nodes, 3004277 wet node-layers.
- LR atmosphere/land: TCo95 native O96 grid, 40320 cells, L91; 30S-90S 9344 cells. The LR test run remaps to 400x192 = 76800; writing on that grid scales atmosphere/land sizes by 1.90.
- Steps per year: mon 12, day 365, 3hr 2920, 1hr 8760, subhr 8760 (OIFS TSTEP 3600 s), yr 1. fx and climatologies count once per run, outside the per-year totals. One file per variable per year.
- Compressed size = bytes/value measured on the cmorized HR piControl year 1852 (`/scratch/a/a270092/pycmor_hr/cli123/cmorized/MIP-DRS7`, BitGroom nsd=5 + zlib, HDF5 storage size of the data variable only), times the LR value count, plus a per-file coordinate overhead (lat/lon and cell vertices) scaled from HR per horizontal point. Ocean 3D bytes/value is taken per wet value (HR DARS2 wet fraction 64.1 %) and applied to the LR wet count; ocean 2D classes are applied to open-ocean nodes (fill under ice shelves), HR has no land nodes either.
- Low/high = smallest/largest bytes/value among the HR files of the same class.

### Bytes per value used (HR cli123, measured)

| Class | HR files | median B/value | min-max | overhead per horiz. point (B) | measured on |
|---|---|---|---|---|---|
| ocn3d_mon | 4 | 2.887 | 2.267-2.918 | 206.4 | thetao uo vo wo |
| ocn2d_mon | 3 | 2.120 | 1.653-2.197 | 55.3 | sos tos zos |
| ocn2d_day | 3 | 2.165 | 1.694-2.228 | 55.3 | sos tos zos |
| sparse2d_mon | 46 | 0.768 | 0.138-0.877 | 55.3 | evspsbl prra prsn rlds rlus rsds rsus sfdsi sicompstren siconc sidconcdyn sidconcth sidmas |
| sparse2d_day | 10 | 0.741 | 0.270-0.792 | 55.3 | rsds rsus siconc sisnhc sispeed sithick siu siv snd ts |
| zeros | 3 | 0.017 | 0.017-0.017 | 55.3 | sidragbot sidragtop vsfcorr |
| ocn_fx | 3 | 1.431 | 0.599-2.115 | 62.6 | areacello deptho volcello |
| ocn_mask_fx | 2 | 0.027 | 0.026-0.028 | 62.6 | basin sftof |
| scalar | 28 | 2.954 | 1.000-4.583 | 25 kB/file | cfc11 cfc12 ch4 co2mass masso n2o siarea siextent sisnmass sivol so sos thetao tos volo zo |
| zonal | 4 | 2.083 | 1.896-2.274 | 42 kB/file | hfbasin msftm sltbasin |
| atm2d_mon | 30 | 2.088 | 0.511-2.705 | 19.1 | clt evspsbl fco2antt fco2nat hfls hfss pr prc prsn rlds rldscs rls rlus rluscs rlut rlutcs |
| atm3d_mon | 5 | 1.845 | 1.134-2.045 | 49.7 | hur hus ta zg |
| atm2d_1hr | 16 | 2.198 | 1.414-2.741 | 23.2 | hfls hfss hurs huss pr ps psl rlds rlus rsds rsus sfcWind tas ts uas vas |
| lnd2d_mon | 135 | 0.687 | 0.145-0.876 | 19.1 | baresoilFrac burntFractionAll c3PftFrac c4PftFrac cLand cLeaf cLitter cLitterCwd cLitterSu |
| lnd2d_day | 18 | 0.738 | 0.177-1.733 | 19.2 | dcw dgw dslw dsn dsw esn mrro mrrob mrso mrsow mrtws sbl snc snd snm snw tslsi tsn |
| lnd_multi_mon | 16 | 0.577 | 0.145-0.824 | 19.1 | cSoilPools fLulccAtmLut fracLut gppLut irrLut laiLut landCoverFrac mrsll mrsol mrsolLut nb |
| lnd2d_yr | 12 | 0.597 | 0.145-0.852 | 19.1 | baresoilFrac cLitterLut cProductLut cSoilLut cVegLut cropFrac fracInLut fracLut fracOutLut |

ocn3d_mon is per wet value (HR full-array median 1.851 B/value).

### Does BGC compress like physics?

LR raw REcoM vs FESOM physics output of the same run (awiesm3-v3.5.0-cc pi, 1850, lossless netCDF, HDF5 storage size / values, float32-equivalent):

| | BGC median B/value | BGC range | physics median | ratio BGC/physics |
|---|---|---|---|---|
| 3d (50 BGC fields; physics: temp unod vnod w) | 1.66 | 1.13-1.97 | 2.03 | 0.82 |
| 2d (19 BGC fields; physics: ssh sss sst) | 3.00 | 0.01-3.06 | 2.78 | 1.08 |

For ocnBgchem the HR cmorized physics class (thetao uo vo wo / tos sos zos) is multiplied by this BGC/physics ratio: 3D 2.89 x 0.82 = 2.36 B per wet value, 2D 2.12 x 1.08 = 2.28 B/value. Low/high combine the HR class min/max with the 10th/90th percentile of the BGC ratio (3D 0.64-0.95, 2D 0.83-1.09).

Uncertainty: fields that are zero over large areas (sea-ice algae, bottom-only fluxes, ice-shelf quantities, iceberg melt, fields of switched-off REcoM components such as coccolithophores) compress far better than this (raw LR aN, all zero: 0.01 B/value); 2D fields flagged `sparse` use the HR sea-ice class (~0.75 B/value instead of ~2.2). The central number is therefore an upper-side estimate for such fields. BitGroom (nsd=5) is applied in cmorization only, so the ratio from lossless raw output is a relative measure, not an absolute one. Peer data point: raw LR DetC 3D monthly is 249 MB on disk vs 497 MB raw float32.
- The ocean per-file overhead is large: HR 3D ocean files carry ~206 B/node of coordinates (vertices chunked (N/10,1) compress badly), 2D files ~55 B/node. On core3 that is ~45 MB per 3D file and ~12 MB per 2D file. Fixing the vertex chunking in pycmor would cut most of it.

## Columns in the CSV

DReq metadata columns as in the HR CSVs, then: priority, opportunities (id + H/M/L), opportunity_titles, variable_groups, n_experiments, experiments, status (dropped), peer_precheck (W/C/S/N from notes/lr_peer_session/flux_table.md, informational), size_class, horiz_points, levels, steps_per_year, values (per year, full array), wet_values, bytes_per_value (+low/high), overhead_MB (per file), files_per_year, per_run, MB (per year compressed, central), MB_low, MB_high, raw_MB (float32 uncompressed), note.

Regenerate: `python awi-esm3-lr-variables/obgc/estimate_lr_volume.py ice` (needs `hr_bytes_per_value_measured.csv` from `measure_hr_bytes_per_value.py`).
