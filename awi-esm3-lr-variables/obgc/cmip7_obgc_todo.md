# CMIP7 ocean biogeochemistry (ocnBgchem) for AWI-ESM3 LR: full list and size

All variables whose CMIP7 compound name has realm `ocnBgchem` in DReq v1.2.2.5.
Scope of this phase: the complete list and a compressed size estimate on the LR grids. No REcoM mapping or feasibility yet (see 'REcoM filter: pending').
No ocnBgchem variable is requested at Core priority or by the Baseline opportunity (69).

## Summary (per model year, counted variables)

| Priority | Variables | Files/yr | GB/yr (central) | GB/yr (low-high) | GB/yr raw float32 |
|---|---|---|---|---|---|
| High | 94 | 94 | 7.43 | 5.26-8.35 | 15.67 |
| Medium | 65 | 65 | 14.71 | 9.42-16.85 | 37.28 |
| Low | 19 | 19 | 28.30 | 17.58-33.09 | 79.44 |
| **Total** | **178** | **178** | **50.44** | 32.26-58.29 | 132.39 |

| Frequency | Variables | Files/yr | GB/yr (central) | GB/yr (low-high) |
|---|---|---|---|---|
| mon | 151 | 151 | 9.87 | 7.14-11.07 |
| day | 27 | 27 | 40.56 | 25.12-47.22 |

| Priority \ freq | mon | day |
|---|---|---|
| High | 92 / 7.05 | 2 / 0.38 |
| Medium | 52 / 2.11 | 13 / 12.60 |
| Low | 7 / 0.72 | 12 / 27.58 |

(cells: variables / GB per year)

## Totals with and without the expensive parts

| Selection | Variables | Files/yr | GB/yr | GB/yr low-high | TB per 100 model years |
|---|---|---|---|---|---|
| All counted variables | 178 | 178 | 50.44 | 32.26-58.29 | 5.04 |
| Without sub-monthly 3D fields | 171 | 171 | 13.70 | 9.54-15.06 | 1.37 |
| Without the 30S-90S subsets | 134 | 134 | 50.01 | 31.91-57.85 | 5.00 |
| Without sub-monthly 3D and without 30S-90S | 127 | 127 | 13.28 | 9.19-14.62 | 1.33 |
| Core + High only | 94 | 94 | 7.43 | 5.26-8.35 | 0.74 |
| Core + High, without sub-monthly 3D | 94 | 94 | 7.43 | 5.26-8.35 | 0.74 |

## Opportunities

| Opportunity | Variables | H / M / L | Only in this one |
|---|---|---|---|
| 44 Changes in marine biogeochemical cycles and ecosystem p | 78 | 36 / 42 / 0 | 33 |
| 16 Climate impacts on marine biodiversity and ecosystems | 58 | 36 / 6 / 16 | 12 |
| 10 Benchmarking and Attributing Changes to Global Carbon a | 54 | 19 / 28 / 7 | 7 |
| 18 Constructing a Global Carbon Budget | 47 | 19 / 28 / 0 | 0 |
| 61 Southern Ocean Biogeochemistry to Clouds | 44 | 44 / 0 / 0 | 44 |
| 54 Multi-annual-to-decadal predictability of the Earth Sys | 39 | 4 / 35 / 0 | 12 |
| 81 Dynamical Downscaling | 29 | 29 / 0 / 0 | 4 |
| 49 Ocean Extremes | 9 | 0 / 9 / 0 | 5 |
| 31 Role of fire in the Earth system | 7 | 7 / 0 / 0 | 0 |
| 26 Detection and Attribution | 1 | 1 / 0 / 0 | 0 |
| 55 Rapid Evaluation Framework | 1 | 1 / 0 / 0 | 0 |

Greedy cover (add the opportunity that brings the most new compound names):

| Step | Opportunity | New | Running total |
|---|---|---|---|
| 1 | 44 Changes in marine biogeochemical cycles and ecosys | 78 | 78 |
| 2 | 61 Southern Ocean Biogeochemistry to Clouds | 44 | 122 |
| 3 | 10 Benchmarking and Attributing Changes to Global Car | 18 | 140 |
| 4 | 54 Multi-annual-to-decadal predictability of the Eart | 17 | 157 |
| 5 | 16 Climate impacts on marine biodiversity and ecosyst | 12 | 169 |
| 6 | 49 Ocean Extremes | 5 | 174 |
| 7 | 81 Dynamical Downscaling | 4 | 178 |

## Southern Ocean 30S-90S subsets

- 44 entries are 30S-90S subsets (opportunities: 61H x44), 0.42 GB/yr together.
- 34 of them have the same variable name requested globally in this set (33 at the same frequency), so they could be cut from the global field.
- LR 30S-90S share: 93000 of 220509 ocean nodes (42.2 %), 9344 of 40320 atmosphere cells.

## REcoM filter: pending

Not done in this phase. Next step: mark each entry below as written / derivable / needs output change / not in REcoM, then recompute the totals.

Peer pre-check (informational, 58 of 178 entries, flux variables only): W/C/S/N from `notes/lr_peer_session/flux_table.md` (W run writes the inputs, C namelist change, S source change, N not in the model). Where `awi-esm3-veg-hr-variables/lr_fluxes_bgc_co2_ice_todo.md` (the parallel session's newer plan) puts the variable in a different section, its code follows after a slash (W pycmor only, X XML + namelist.io, S source change). That file also lists exclusions it calls decided (coccolithophores, carbon isotopes); they are not applied here, only the 3-hourly 3D ficeberg drop by Jan is.

## Variables

### High (94 entries, 7.43 GB/yr)

- [ ] **bacc** (mon, 30S-90S) `ocnBgchem.bacc.tavg-ols-hxy-sea.mon.30S-90S`: Surface Bacterial Carbon Concentration [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **calc** (mon, 30S-90S) `ocnBgchem.calc.tavg-ols-hxy-sea.mon.30S-90S`: Surface Calcite Concentration [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **chl** (mon, glb) `ocnBgchem.chl.tavg-ol-hxy-sea.mon.glb`: Mass Concentration of Total Phytoplankton Expressed as Chlorophyll in Sea Water [kg m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10H 16H 18H 31H 44H 54M 81H; 67 expts
- [ ] **chl** (mon, 30S-90S) `ocnBgchem.chl.tavg-ols-hxy-sea.mon.30S-90S`: Surface Mass Concentration of Total Phytoplankton Expressed as Chlorophyll in Sea Water [kg m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **chl** (mon, glb) `ocnBgchem.chl.tavg-ols-hxy-sea.mon.glb`: Surface Mass Concentration of Total Phytoplankton Expressed as Chlorophyll in Sea Water [kg m-3]; dims `longitude latitude time osurf`; 18.0 MB/yr (ocn2d_mon); opps 54H; 14 expts
- [ ] **chlcalc** (mon, 30S-90S) `ocnBgchem.chlcalc.tavg-ols-hxy-sea.mon.30S-90S`: Surface Mass Concentration of Calcareous Phytoplankton Expressed as Chlorophyll in Sea Water [kg m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **chldiat** (mon, glb) `ocnBgchem.chldiat.tavg-ol-hxy-sea.mon.glb`: Mass Concentration of Diatoms Expressed as Chlorophyll in Sea Water [kg m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10M 18M 81H; 52 expts
- [ ] **chldiat** (mon, 30S-90S) `ocnBgchem.chldiat.tavg-ols-hxy-sea.mon.30S-90S`: Surface Mass Concentration of Diatoms Expressed as Chlorophyll in Sea Water [kg m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **chldiaz** (mon, glb) `ocnBgchem.chldiaz.tavg-ol-hxy-sea.mon.glb`: Mass Concentration of Diazotrophs Expressed as Chlorophyll in Sea Water [kg m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10M 18M 81H; 52 expts
- [ ] **chldiaz** (mon, 30S-90S) `ocnBgchem.chldiaz.tavg-ols-hxy-sea.mon.30S-90S`: Surface Mass Concentration of Diazotrophs Expressed as Chlorophyll in Sea Water [kg m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **chlmisc** (mon, glb) `ocnBgchem.chlmisc.tavg-ol-hxy-sea.mon.glb`: Mass Concentration of Other Phytoplankton Expressed as Chlorophyll in Sea Water [kg m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10M 18M 81H; 52 expts
- [ ] **chlmisc** (mon, 30S-90S) `ocnBgchem.chlmisc.tavg-ols-hxy-sea.mon.30S-90S`: Surface Mass Concentration of Other Phytoplankton Expressed as Chlorophyll in Sea Water [kg m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **chlpico** (mon, glb) `ocnBgchem.chlpico.tavg-ol-hxy-sea.mon.glb`: Mass Concentration of Picophytoplankton Expressed as Chlorophyll in Sea Water [kg m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10M 18M 81H; 52 expts
- [ ] **chlpico** (mon, 30S-90S) `ocnBgchem.chlpico.tavg-ols-hxy-sea.mon.30S-90S`: Surface Mass Concentration of Picophytoplankton Expressed as Chlorophyll in Sea Water [kg m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **detoc** (mon, glb) `ocnBgchem.detoc.tavg-ol-hxy-sea.mon.glb`: Detrital Organic Carbon Concentration [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10H 16H 18H 44H; 58 expts
- [ ] **dfe** (mon, glb) `ocnBgchem.dfe.tavg-ol-hxy-sea.mon.glb`: Dissolved Iron Concentration [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10H 16H 18H 44H 54M 81H; 62 expts
- [ ] **dfe** (mon, 30S-90S) `ocnBgchem.dfe.tavg-ols-hxy-sea.mon.30S-90S`: Surface Dissolved Iron Concentration [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **dfe** (mon, glb) `ocnBgchem.dfe.tavg-ols-hxy-sea.mon.glb`: Surface Dissolved Iron Concentration [mol m-3]; dims `longitude latitude time osurf`; 18.0 MB/yr (ocn2d_mon); opps 31H 54H; 32 expts
- [ ] **dissic** (mon, glb) `ocnBgchem.dissic.tavg-ol-hxy-sea.mon.glb`: Dissolved Inorganic Carbon Concentration [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10H 16H 18H 31H 44H 49M 54M 81H; 77 expts
- [ ] **dissic** (mon, 30S-90S) `ocnBgchem.dissic.tavg-ols-hxy-sea.mon.30S-90S`: Surface Dissolved Inorganic Carbon Concentration [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **dissoc** (mon, glb) `ocnBgchem.dissoc.tavg-ol-hxy-sea.mon.glb`: Dissolved Organic Carbon Concentration [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10H 16H 18H 44H 54M 81H; 62 expts
- [ ] **dmso** (mon, 30S-90S) `ocnBgchem.dmso.tavg-ols-hxy-sea.mon.30S-90S`: Surface Mole Concentration of Dimethyl Sulphide in Sea Water [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **dmso** (mon, glb) `ocnBgchem.dmso.tavg-ols-hxy-sea.mon.glb`: Surface Mole Concentration of Dimethyl Sulphide in Sea Water [mol m-3]; dims `longitude latitude time osurf`; 18.0 MB/yr (ocn2d_mon); opps 16H 44H; 24 expts
- [ ] **dpco2** (mon, glb) `ocnBgchem.dpco2.tavg-u-hxy-sea.mon.glb`: Delta CO2 Partial Pressure [Pa]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 16H 44H; 24 expts
- [ ] **dpo2** (mon, glb) `ocnBgchem.dpo2.tavg-u-hxy-sea.mon.glb`: Delta O2 Partial Pressure [Pa]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 16H 44H; 24 expts
- [ ] **expc** (mon, glb) `ocnBgchem.expc.tavg-d1000m-hxy-sea.mon.glb`: Downward Flux of Particulate Organic Carbon at 1000m [mol m-2 s-1]; dims `longitude latitude time depth1000m`; 18.0 MB/yr (ocn2d_mon); opps 16H 44H; 24 expts; peer pre-check S
- [ ] **expc** (mon, glb) `ocnBgchem.expc.tavg-d100m-hxy-sea.mon.glb`: Downward Flux of Particulate Organic Carbon [mol m-2 s-1]; dims `longitude latitude time depth100m`; 18.0 MB/yr (ocn2d_mon); opps 10M 16H 18M 44H 54M; 62 expts; peer pre-check S
- [ ] **expc** (mon, glb) `ocnBgchem.expc.tavg-ol-hxy-sea.mon.glb`: Downward Flux of Particulate Organic Carbon [mol m-2 s-1]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10H 18H 44M 54M; 62 expts; peer pre-check S
- [ ] **expcalc** (mon, glb) `ocnBgchem.expcalc.tavg-d1000m-hxy-sea.mon.glb`: Downward Flux of Calcite at 1000m [mol m-2 s-1]; dims `longitude latitude time depth1000m`; 18.0 MB/yr (ocn2d_mon); opps 16H 44H; 24 expts; peer pre-check S
- [ ] **expcalc** (mon, glb) `ocnBgchem.expcalc.tavg-d100m-hxy-sea.mon.glb`: Downward Flux of Calcite [mol m-2 s-1]; dims `longitude latitude time depth100m`; 18.0 MB/yr (ocn2d_mon); opps 10M 16H 18M 44H 54M; 62 expts; peer pre-check S
- [ ] **expcalc** (mon, glb) `ocnBgchem.expcalc.tavg-ol-hxy-sea.mon.glb`: Downward Flux of Calcite [mol m-2 s-1]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10H 18H 44M; 58 expts; peer pre-check S
- [ ] **fgco2** (mon, glb) `ocnBgchem.fgco2.tavg-u-hxy-sea.mon.glb`: Surface Downward Mass Flux of Carbon as CO2 [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 10H 16H 18H 26H 31H 44H 54H 55H; 69 expts; peer pre-check W
- [ ] **fric** (mon, glb) `ocnBgchem.fric.tavg-u-hxy-sea.mon.glb`: Downward Inorganic Carbon Flux at Ocean Bottom [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 10M 16H 18M 44H; 58 expts; peer pre-check S
- [ ] **froc** (mon, glb) `ocnBgchem.froc.tavg-u-hxy-sea.mon.glb`: Downward Organic Carbon Flux at Ocean Bottom [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 10M 16H 18M 44H; 58 expts; peer pre-check S
- [ ] **fsfe** (mon, glb) `ocnBgchem.fsfe.tavg-u-hxy-sea.mon.glb`: Surface Downward Net Flux of Iron [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 81H; 18 expts; peer pre-check W
- [ ] **fsn** (mon, glb) `ocnBgchem.fsn.tavg-u-hxy-sea.mon.glb`: Surface Downward Net Flux of Nitrogen [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 81H; 18 expts; peer pre-check W
- [ ] **graz** (mon, 30S-90S) `ocnBgchem.graz.tavg-ol-hxy-sea.mon.30S-90S`: Total Grazing of Phytoplankton by Zooplankton [mol m-3 s-1]; dims `longitude latitude olevel time`; 104.4 MB/yr (ocn3d_mon); opps 61H; 58 expts; peer pre-check S
- [ ] **graz** (mon, glb) `ocnBgchem.graz.tavg-ol-hxy-sea.mon.glb`: Total Grazing of Phytoplankton by Zooplankton [mol m-3 s-1]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10M 16H 18M 44H; 58 expts; peer pre-check S
- [ ] **icfriver** (mon, glb) `ocnBgchem.icfriver.tavg-u-hxy-sea.mon.glb`: Flux of Inorganic Carbon into Ocean Surface by Runoff [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 10M 18M 81H; 52 expts; peer pre-check N
- [ ] **intdic** (mon, 30S-90S) `ocnBgchem.intdic.tavg-u-hxy-sea.mon.30S-90S`: Dissolved Inorganic Carbon Content [kg m-2]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **intdoc** (mon, 30S-90S) `ocnBgchem.intdoc.tavg-u-hxy-sea.mon.30S-90S`: Dissolved Organic Carbon Content [kg m-2]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **intparag** (mon, 30S-90S) `ocnBgchem.intparag.tavg-u-hxy-sea.mon.30S-90S`: Aragonite Production [mol m-2 s-1]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts; peer pre-check N
- [ ] **intpbfe** (mon, 30S-90S) `ocnBgchem.intpbfe.tavg-u-hxy-sea.mon.30S-90S`: Iron Production [mol m-2 s-1]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts; peer pre-check W
- [ ] **intpbn** (mon, 30S-90S) `ocnBgchem.intpbn.tavg-u-hxy-sea.mon.30S-90S`: Nitrogen Production [mol m-2 s-1]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts; peer pre-check W
- [ ] **intpbp** (mon, 30S-90S) `ocnBgchem.intpbp.tavg-u-hxy-sea.mon.30S-90S`: Phosphorus Production [mol m-2 s-1]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts; peer pre-check N
- [ ] **intpbsi** (mon, 30S-90S) `ocnBgchem.intpbsi.tavg-u-hxy-sea.mon.30S-90S`: Silicon Production [mol m-2 s-1]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts; peer pre-check S/X
- [ ] **intpcalcite** (mon, 30S-90S) `ocnBgchem.intpcalcite.tavg-u-hxy-sea.mon.30S-90S`: Calcite Production [mol m-2 s-1]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts; peer pre-check W
- [ ] **intpn2** (mon, 30S-90S) `ocnBgchem.intpn2.tavg-u-hxy-sea.mon.30S-90S`: Nitrogen Fixation Rate in Ocean [mol m-2 s-1]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts; peer pre-check N
- [ ] **intpoc** (mon, 30S-90S) `ocnBgchem.intpoc.tavg-u-hxy-sea.mon.30S-90S`: Particulate Organic Carbon Content [kg m-2]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **intpp** (mon, 30S-90S) `ocnBgchem.intpp.tavg-u-hxy-sea.mon.30S-90S`: Primary Organic Carbon Production by All Types of Phytoplankton [mol m-2 s-1]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts; peer pre-check W
- [ ] **intpp** (mon, glb) `ocnBgchem.intpp.tavg-u-hxy-sea.mon.glb`: Primary Organic Carbon Production by All Types of Phytoplankton [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 10H 16H 18H 44H 54H; 62 expts; peer pre-check W
- [ ] **intppcalc** (mon, 30S-90S) `ocnBgchem.intppcalc.tavg-u-hxy-sea.mon.30S-90S`: Net Primary Mole Productivity of Carbon by Calcareous Phytoplankton [mol m-2 s-1]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts; peer pre-check N
- [ ] **intppdiat** (mon, 30S-90S) `ocnBgchem.intppdiat.tavg-u-hxy-sea.mon.30S-90S`: Net Primary Organic Carbon Production by Diatoms [mol m-2 s-1]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts; peer pre-check W
- [ ] **intppdiaz** (mon, 30S-90S) `ocnBgchem.intppdiaz.tavg-u-hxy-sea.mon.30S-90S`: Net Primary Mole Productivity of Carbon by Diazotrophs [mol m-2 s-1]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts; peer pre-check N
- [ ] **intppmisc** (mon, 30S-90S) `ocnBgchem.intppmisc.tavg-u-hxy-sea.mon.30S-90S`: Net Primary Organic Carbon Production by Other Phytoplankton [mol m-2 s-1]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts; peer pre-check N
- [ ] **intppnitrate** (mon, 30S-90S) `ocnBgchem.intppnitrate.tavg-u-hxy-sea.mon.30S-90S`: Primary Organic Carbon Production by Phytoplankton Based on Nitrate Uptake Alone [mol m-2 s-1]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts; peer pre-check N
- [ ] **intpppico** (mon, 30S-90S) `ocnBgchem.intpppico.tavg-u-hxy-sea.mon.30S-90S`: Net Primary Mole Productivity of Carbon by Picophytoplankton [mol m-2 s-1]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts; peer pre-check N
- [ ] **nh4** (mon, glb) `ocnBgchem.nh4.tavg-ol-hxy-sea.mon.glb`: Dissolved Ammonium Concentration [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10H 16H 18H 44H 54M; 62 expts
- [ ] **nh4** (mon, 30S-90S) `ocnBgchem.nh4.tavg-ols-hxy-sea.mon.30S-90S`: Surface Dissolved Ammonium Concentration [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **no3** (mon, glb) `ocnBgchem.no3.tavg-ol-hxy-sea.mon.glb`: Dissolved Nitrate Concentration [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10H 16H 18H 31H 44H 54M 81H; 67 expts
- [ ] **no3** (mon, 30S-90S) `ocnBgchem.no3.tavg-ols-hxy-sea.mon.30S-90S`: Surface Dissolved Nitrate Concentration [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **o2** (mon, glb) `ocnBgchem.o2.tavg-ol-hxy-sea.mon.glb`: Dissolved Oxygen Concentration [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10H 16H 18H 44H 54M 81H; 62 expts
- [ ] **ocfriver** (mon, glb) `ocnBgchem.ocfriver.tavg-u-hxy-sea.mon.glb`: Flux of Organic Carbon into Ocean Surface by Runoff [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 81H; 18 expts; peer pre-check N
- [ ] **ph** (mon, glb) `ocnBgchem.ph.tavg-ol-hxy-sea.mon.glb`: pH [1]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10H 16H 18H 44H 54M; 62 expts
- [ ] **ph** (mon, 30S-90S) `ocnBgchem.ph.tavg-ols-hxy-sea.mon.30S-90S`: Surface pH [1]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **phyc** (mon, glb) `ocnBgchem.phyc.tavg-ol-hxy-sea.mon.glb`: Phytoplankton Carbon Concentration [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10H 16H 18H 44H 54M 81H; 62 expts
- [ ] **phyc** (mon, 30S-90S) `ocnBgchem.phyc.tavg-ols-hxy-sea.mon.30S-90S`: Sea Surface Phytoplankton Carbon Concentration [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **phyc** (mon, glb) `ocnBgchem.phyc.tavg-ols-hxy-sea.mon.glb`: Sea Surface Phytoplankton Carbon Concentration [mol m-3]; dims `longitude latitude time osurf`; 18.0 MB/yr (ocn2d_mon); opps 54M 81H; 26 expts
- [ ] **phycalc** (mon, glb) `ocnBgchem.phycalc.tavg-ol-hxy-sea.mon.glb`: Mole Concentration of Calcareous Phytoplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10M 16H 18M 44H 81H; 58 expts
- [ ] **phycalc** (mon, 30S-90S) `ocnBgchem.phycalc.tavg-ols-hxy-sea.mon.30S-90S`: Surface Mole Concentration of Calcareous Phytoplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **phydiat** (mon, glb) `ocnBgchem.phydiat.tavg-ol-hxy-sea.mon.glb`: Mole Concentration of Diatoms Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10M 16H 18M 44H 81H; 58 expts
- [ ] **phydiat** (mon, 30S-90S) `ocnBgchem.phydiat.tavg-ols-hxy-sea.mon.30S-90S`: Surface Mole Concentration of Diatoms Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **phydiaz** (mon, glb) `ocnBgchem.phydiaz.tavg-ol-hxy-sea.mon.glb`: Mole Concentration of Diazotrophs Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 81H; 18 expts
- [ ] **phydiaz** (mon, 30S-90S) `ocnBgchem.phydiaz.tavg-ols-hxy-sea.mon.30S-90S`: Surface Mole Concentration of Diazotrophs Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **phymisc** (mon, glb) `ocnBgchem.phymisc.tavg-ol-hxy-sea.mon.glb`: Mole Concentration of Miscellaneous Phytoplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10M 16H 18M 44H 81H; 58 expts
- [ ] **phymisc** (mon, 30S-90S) `ocnBgchem.phymisc.tavg-ols-hxy-sea.mon.30S-90S`: Surface Mole Concentration of Miscellaneous Phytoplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **phypico** (mon, glb) `ocnBgchem.phypico.tavg-ol-hxy-sea.mon.glb`: Mole Concentration of Picophytoplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10M 16H 18M 44H 81H; 58 expts
- [ ] **phypico** (mon, 30S-90S) `ocnBgchem.phypico.tavg-ols-hxy-sea.mon.30S-90S`: Surface Mole Concentration of Picophytoplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **po4** (mon, glb) `ocnBgchem.po4.tavg-ol-hxy-sea.mon.glb`: Total Dissolved Inorganic Phosphorus Concentration [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10H 16H 18H 31H 44H 54M 81H; 67 expts
- [ ] **pp** (mon, 30S-90S) `ocnBgchem.pp.tavg-ols-hxy-sea.mon.30S-90S`: Primary Carbon Production by Phytoplankton [mol m-3 s-1]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts; peer pre-check W
- [ ] **si** (mon, glb) `ocnBgchem.si.tavg-ol-hxy-sea.mon.glb`: Total Dissolved Inorganic Silicon Concentration [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10H 16H 18H 44H 54M 81H; 62 expts
- [ ] **spco2** (mon, 30S-90S) `ocnBgchem.spco2.tavg-u-hxy-sea.mon.30S-90S`: Surface Aqueous Partial Pressure of CO2 [Pa]; dims `longitude latitude time`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **spco2** (mon, glb) `ocnBgchem.spco2.tavg-u-hxy-sea.mon.glb`: Surface Aqueous Partial Pressure of CO2 [Pa]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 10H 16H 18H 31H 44H 54M 81H; 67 expts
- [ ] **talk** (mon, glb) `ocnBgchem.talk.tavg-ol-hxy-sea.mon.glb`: Total Alkalinity [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10H 16H 18H 44H 49M 54M 81H; 75 expts
- [ ] **zmeso** (mon, glb) `ocnBgchem.zmeso.tavg-ol-hxy-sea.mon.glb`: Mole Concentration of Mesozooplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10M 16H 18M 44H 81H; 58 expts
- [ ] **zmeso** (mon, 30S-90S) `ocnBgchem.zmeso.tavg-ols-hxy-sea.mon.30S-90S`: Surface Mole Concentration of Mesozooplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **zmicro** (mon, glb) `ocnBgchem.zmicro.tavg-ol-hxy-sea.mon.glb`: Mole Concentration of Microzooplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10M 16H 18M 44H 81H; 58 expts
- [ ] **zmicro** (mon, 30S-90S) `ocnBgchem.zmicro.tavg-ols-hxy-sea.mon.30S-90S`: Surface Mole Concentration of Microzooplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **zmisc** (mon, glb) `ocnBgchem.zmisc.tavg-ol-hxy-sea.mon.glb`: Mole Concentration of Other Zooplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10M 16H 18M 44H 81H; 58 expts
- [ ] **zmisc** (mon, 30S-90S) `ocnBgchem.zmisc.tavg-ols-hxy-sea.mon.30S-90S`: Surface Mole Concentration of Other Zooplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **zooc** (mon, glb) `ocnBgchem.zooc.tavg-ol-hxy-sea.mon.glb`: Zooplankton Carbon Concentration [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10H 16H 18H 44H 54M 81H; 62 expts
- [ ] **zooc** (mon, 30S-90S) `ocnBgchem.zooc.tavg-ols-hxy-sea.mon.30S-90S`: Surface Zooplankton Carbon Concentration [mol m-3]; dims `longitude latitude time osurf`; 7.4 MB/yr (ocn2d_mon); opps 61H; 58 expts
- [ ] **chl** (day, glb) `ocnBgchem.chl.tavg-ols-hxy-sea.day.glb`: Surface Mass Concentration of Total Phytoplankton Expressed as Chlorophyll in Sea Water [kg m-3]; dims `longitude latitude time osurf`; 191.2 MB/yr (ocn2d_day); opps 16H 44H 54M; 32 expts
- [ ] **intpp** (day, glb) `ocnBgchem.intpp.tavg-u-hxy-sea.day.glb`: Primary Organic Carbon Production by All Types of Phytoplankton [mol m-2 s-1]; dims `longitude latitude time`; 191.2 MB/yr (ocn2d_day); opps 16H 44H 54M; 32 expts; peer pre-check C/X

### Medium (65 entries, 14.71 GB/yr)

- [ ] **arag** (mon, glb) `ocnBgchem.arag.tavg-ol-hxy-sea.mon.glb`: Aragonite Concentration [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 49M; 72 expts
- [ ] **arag** (mon, glb) `ocnBgchem.arag.tavg-ols-hxy-sea.mon.glb`: Surface Aragonite Concentration [mol m-3]; dims `longitude latitude time osurf`; 18.0 MB/yr (ocn2d_mon); opps 49M; 72 expts
- [ ] **calc** (mon, glb) `ocnBgchem.calc.tavg-ol-hxy-sea.mon.glb`: Calcite Concentration [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 49M; 72 expts
- [ ] **calc** (mon, glb) `ocnBgchem.calc.tavg-ols-hxy-sea.mon.glb`: Surface Calcite Concentration [mol m-3]; dims `longitude latitude time osurf`; 18.0 MB/yr (ocn2d_mon); opps 49M; 72 expts
- [ ] **chlcalc** (mon, glb) `ocnBgchem.chlcalc.tavg-ol-hxy-sea.mon.glb`: Mass Concentration of Calcareous Phytoplankton Expressed as Chlorophyll in Sea Water [kg m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10M 18M; 50 expts
- [ ] **dmso** (mon, glb) `ocnBgchem.dmso.tavg-ol-hxy-sea.mon.glb`: Mole Concentration of Dimethyl Sulphide in Sea Water [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10M 18M; 50 expts
- [ ] **epn** (mon, glb) `ocnBgchem.epn.tavg-d100m-hxy-sea.mon.glb`: Downward Flux of Particulate Nitrogen [mol m-2 s-1]; dims `longitude latitude time depth100m`; 18.0 MB/yr (ocn2d_mon); opps 10M 18M; 50 expts; peer pre-check S
- [ ] **epp** (mon, glb) `ocnBgchem.epp.tavg-d100m-hxy-sea.mon.glb`: Downward Flux of Particulate Phosphorus [mol m-2 s-1]; dims `longitude latitude time depth100m`; 18.0 MB/yr (ocn2d_mon); opps 10M 18M; 50 expts; peer pre-check N
- [ ] **epsi** (mon, glb) `ocnBgchem.epsi.tavg-d100m-hxy-sea.mon.glb`: Downward Flux of Particulate Silicon [mol m-2 s-1]; dims `longitude latitude time depth100m`; 18.0 MB/yr (ocn2d_mon); opps 54M; 14 expts; peer pre-check S
- [ ] **exparagob** (mon, glb) `ocnBgchem.exparagob.tavg-u-hxy-sea.mon.glb`: Sinking Flux of Aragonite Reaching the Ocean Bottom [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts; peer pre-check N
- [ ] **expcalcob** (mon, glb) `ocnBgchem.expcalcob.tavg-u-hxy-sea.mon.glb`: Sinking Flux of Calcite Reaching the Ocean Bottom [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts; peer pre-check S
- [ ] **expcob** (mon, glb) `ocnBgchem.expcob.tavg-u-hxy-sea.mon.glb`: Sinking Flux of Particulate Organic Carbon Reaching the Ocean Bottom [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts; peer pre-check S
- [ ] **expfeob** (mon, glb) `ocnBgchem.expfeob.tavg-u-hxy-sea.mon.glb`: Sinking Flux of Particulate Iron Reaching the Ocean Bottom [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts; peer pre-check S
- [ ] **expnob** (mon, glb) `ocnBgchem.expnob.tavg-u-hxy-sea.mon.glb`: Sinking Flux of Particulate Organic Nitrogen Reaching the Ocean Bottom [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts; peer pre-check S
- [ ] **exppob** (mon, glb) `ocnBgchem.exppob.tavg-u-hxy-sea.mon.glb`: Sinking Flux of Particulate Organic Phosphorus Reaching the Ocean Bottom [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts; peer pre-check N
- [ ] **expsiob** (mon, glb) `ocnBgchem.expsiob.tavg-u-hxy-sea.mon.glb`: Sinking Flux of Particulate Silicon Reaching the Ocean Bottom [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts; peer pre-check S
- [ ] **fgdms** (mon, glb) `ocnBgchem.fgdms.tavg-u-hxy-sea.mon.glb`: Surface Upward Flux of DMS [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 10M 18M; 50 expts; peer pre-check N
- [ ] **frfe** (mon, glb) `ocnBgchem.frfe.tavg-u-hxy-sea.mon.glb`: Iron Loss to Sediments [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts; peer pre-check S
- [ ] **frn** (mon, glb) `ocnBgchem.frn.tavg-u-hxy-sea.mon.glb`: Nitrogen Loss to Sediments and Through Denitrification [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts; peer pre-check S
- [ ] **intppcalc** (mon, glb) `ocnBgchem.intppcalc.tavg-u-hxy-sea.mon.glb`: Net Primary Mole Productivity of Carbon by Calcareous Phytoplankton [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 10M 16M 18M 44M; 58 expts; peer pre-check N
- [ ] **intppdiat** (mon, glb) `ocnBgchem.intppdiat.tavg-u-hxy-sea.mon.glb`: Net Primary Organic Carbon Production by Diatoms [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 10M 16M 18M 44M; 58 expts; peer pre-check W
- [ ] **intppdiaz** (mon, glb) `ocnBgchem.intppdiaz.tavg-u-hxy-sea.mon.glb`: Net Primary Mole Productivity of Carbon by Diazotrophs [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 10M 16M 18M 44M; 58 expts; peer pre-check N
- [ ] **intppmisc** (mon, glb) `ocnBgchem.intppmisc.tavg-u-hxy-sea.mon.glb`: Net Primary Organic Carbon Production by Other Phytoplankton [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 10M 16M 18M 44M; 58 expts; peer pre-check N
- [ ] **intppnano** (mon, glb) `ocnBgchem.intppnano.tavg-u-hxy-sea.mon.glb`: Net Primary Organic Carbon Production by Nanophytoplankton [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 16M 44M; 24 expts; peer pre-check W
- [ ] **intppnitrate** (mon, glb) `ocnBgchem.intppnitrate.tavg-u-hxy-sea.mon.glb`: Primary Organic Carbon Production by Phytoplankton Based on Nitrate Uptake Alone [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 10M 18M; 50 expts; peer pre-check N
- [ ] **intpppico** (mon, glb) `ocnBgchem.intpppico.tavg-u-hxy-sea.mon.glb`: Net Primary Mole Productivity of Carbon by Picophytoplankton [mol m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 10M 16M 18M 44M; 58 expts; peer pre-check N
- [ ] **limfecalc** (mon, glb) `ocnBgchem.limfecalc.tavg-u-hxy-sea.mon.glb`: Iron Limitation of Calcareous Phytoplankton [1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts
- [ ] **limfediat** (mon, glb) `ocnBgchem.limfediat.tavg-u-hxy-sea.mon.glb`: Iron Limitation of Diatoms [1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts
- [ ] **limfediaz** (mon, glb) `ocnBgchem.limfediaz.tavg-u-hxy-sea.mon.glb`: Iron Limitation of Diazotrophs [1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts
- [ ] **limfemisc** (mon, glb) `ocnBgchem.limfemisc.tavg-u-hxy-sea.mon.glb`: Iron Limitation of Other Phytoplankton [1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts
- [ ] **limfepico** (mon, glb) `ocnBgchem.limfepico.tavg-u-hxy-sea.mon.glb`: Iron Limitation of Picophytoplankton [1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts
- [ ] **limirrcalc** (mon, glb) `ocnBgchem.limirrcalc.tavg-u-hxy-sea.mon.glb`: Irradiance Limitation of Calcareous Phytoplankton [1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts
- [ ] **limirrdiat** (mon, glb) `ocnBgchem.limirrdiat.tavg-u-hxy-sea.mon.glb`: Irradiance Limitation of Diatoms [1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts
- [ ] **limirrdiaz** (mon, glb) `ocnBgchem.limirrdiaz.tavg-u-hxy-sea.mon.glb`: Irradiance Limitation of Diazotrophs [1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts
- [ ] **limirrmisc** (mon, glb) `ocnBgchem.limirrmisc.tavg-u-hxy-sea.mon.glb`: Irradiance Limitation of Other Phytoplankton [1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts
- [ ] **limirrpico** (mon, glb) `ocnBgchem.limirrpico.tavg-u-hxy-sea.mon.glb`: Irradiance Limitation of Picophytoplankton [1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts
- [ ] **limncalc** (mon, glb) `ocnBgchem.limncalc.tavg-u-hxy-sea.mon.glb`: Nitrogen Limitation of Calcareous Phytoplankton [1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts
- [ ] **limndiat** (mon, glb) `ocnBgchem.limndiat.tavg-u-hxy-sea.mon.glb`: Nitrogen Limitation of Diatoms [1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts
- [ ] **limndiaz** (mon, glb) `ocnBgchem.limndiaz.tavg-u-hxy-sea.mon.glb`: Nitrogen Limitation of Diazotrophs [1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts
- [ ] **limnmisc** (mon, glb) `ocnBgchem.limnmisc.tavg-u-hxy-sea.mon.glb`: Nitrogen Limitation of Other Phytoplankton [1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts
- [ ] **limnpico** (mon, glb) `ocnBgchem.limnpico.tavg-u-hxy-sea.mon.glb`: Nitrogen Limitation of Picophytoplankton [1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 44M; 24 expts
- [ ] **no3** (mon, glb) `ocnBgchem.no3.tavg-ols-hxy-sea.mon.glb`: Surface Dissolved Nitrate Concentration [mol m-3]; dims `longitude latitude time osurf`; 18.0 MB/yr (ocn2d_mon); opps 54M; 14 expts
- [ ] **o2** (mon, glb) `ocnBgchem.o2.tavg-ols-hxy-sea.mon.glb`: Surface Dissolved Oxygen Concentration [mol m-3]; dims `longitude latitude time osurf`; 18.0 MB/yr (ocn2d_mon); opps 54M; 14 expts
- [ ] **o2sat** (mon, glb) `ocnBgchem.o2sat.tavg-ol-hxy-sea.mon.glb`: Dissolved Oxygen Concentration at Saturation [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 44M; 24 expts
- [ ] **pp** (mon, glb) `ocnBgchem.pp.tavg-ol-hxy-sea.mon.glb`: Primary Carbon Production by Phytoplankton [mol m-3 s-1]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 44M; 24 expts; peer pre-check W
- [ ] **pp** (mon, glb) `ocnBgchem.pp.tavg-ols-hxy-sea.mon.glb`: Primary Carbon Production by Phytoplankton [mol m-3 s-1]; dims `longitude latitude time osurf`; 18.0 MB/yr (ocn2d_mon); opps 54M; 14 expts; peer pre-check W
- [ ] **si** (mon, glb) `ocnBgchem.si.tavg-ols-hxy-sea.mon.glb`: Surface Total Dissolved Inorganic Silicon Concentration [mol m-3]; dims `longitude latitude time osurf`; 18.0 MB/yr (ocn2d_mon); opps 54M; 14 expts
- [ ] **sialgc** (mon, glb) `ocnBgchem.sialgc.tavg-u-hxy-si.mon.glb`: Total Ice Algal Carbon Amount in Sea Ice [mol m-2]; dims `longitude latitude time`; 14.1 MB/yr (sparse2d_mon); opps 44M; 24 expts
- [ ] **sichl** (mon, glb) `ocnBgchem.sichl.tavg-u-hxy-si.mon.glb`: Amount of Total Ice Algae Expressed as Chlorophyll in Sea Ice [kg m-2]; dims `longitude latitude time`; 14.1 MB/yr (sparse2d_mon); opps 44M; 24 expts
- [ ] **sigpp** (mon, glb) `ocnBgchem.sigpp.tavg-u-hxy-si.mon.glb`: Gross Primary Productivity in Sea Ice as Carbon Amount Flux [kg m-2 s-1]; dims `longitude latitude time`; 14.1 MB/yr (sparse2d_mon); opps 44M; 24 expts; peer pre-check N
- [ ] **sino3** (mon, glb) `ocnBgchem.sino3.tavg-u-hxy-si.mon.glb`: Dissolved Nitrate Amount in Sea Ice [mol m-2]; dims `longitude latitude time`; 14.1 MB/yr (sparse2d_mon); opps 44M; 24 expts
- [ ] **sisi** (mon, glb) `ocnBgchem.sisi.tavg-u-hxy-si.mon.glb`: Dissolved Silicon Amount in Sea Ice [mol m-2]; dims `longitude latitude time`; 14.1 MB/yr (sparse2d_mon); opps 44M; 24 expts
- [ ] **chl** (day, glb) `ocnBgchem.chl.tavg-op20bar-hxy-sea.day.glb`: Mass Concentration of Total Phytoplankton Expressed as Chlorophyll in Sea Water at 200 meters [kg m-3]; dims `longitude latitude time op20bar`; 191.2 MB/yr (ocn2d_day); opps 49M; 72 expts
- [ ] **co3** (day, glb) `ocnBgchem.co3.tavg-ols-hxy-sea.day.glb`: Surface Carbonate Ion Concentration [mol m-3]; dims `longitude latitude time osurf`; 191.2 MB/yr (ocn2d_day); opps 54M; 14 expts
- [ ] **co3** (day, glb) `ocnBgchem.co3.tavg-op20bar-hxy-sea.day.glb`: Carbonate Ion Concentration [mol m-3]; dims `longitude latitude time op20bar`; 191.2 MB/yr (ocn2d_day); opps 54M; 14 expts
- [ ] **co3satarag** (day, glb) `ocnBgchem.co3satarag.tavg-ols-hxy-sea.day.glb`: Surface Mole Concentration of Carbonate Ion in Equilibrium with Pure Aragonite in Sea Water [mol m-3]; dims `longitude latitude time osurf`; 191.2 MB/yr (ocn2d_day); opps 54M; 14 expts
- [ ] **co3satarag** (day, glb) `ocnBgchem.co3satarag.tavg-op20bar-hxy-sea.day.glb`: Mole Concentration of Carbonate Ion in Equilibrium with Pure Aragonite in Sea Water [mol m-3]; dims `longitude latitude time op20bar`; 191.2 MB/yr (ocn2d_day); opps 54M; 14 expts
- [ ] **detoc** (day, glb) `ocnBgchem.detoc.tavg-ol-hxy-sea.day.glb`: Detrital Organic Carbon Concentration [mol m-3]; dims `longitude latitude olevel time`; 5248.4 MB/yr (ocn3d_mon); opps 44M; 24 expts
- [ ] **dissoc** (day, glb) `ocnBgchem.dissoc.tavg-ol-hxy-sea.day.glb`: Dissolved Organic Carbon Concentration [mol m-3]; dims `longitude latitude olevel time`; 5248.4 MB/yr (ocn3d_mon); opps 44M; 24 expts
- [ ] **o2** (day, glb) `ocnBgchem.o2.tavg-ols-hxy-sea.day.glb`: Surface Dissolved Oxygen Concentration [mol m-3]; dims `longitude latitude time osurf`; 191.2 MB/yr (ocn2d_day); opps 16L 54M; 26 expts
- [ ] **o2** (day, glb) `ocnBgchem.o2.tavg-op20bar-hxy-sea.day.glb`: Dissolved Oxygen Concentration at 200 meters [mol m-3]; dims `longitude latitude time op20bar`; 191.2 MB/yr (ocn2d_day); opps 16L 44M 49M 54M; 75 expts
- [ ] **ph** (day, glb) `ocnBgchem.ph.tavg-ols-hxy-sea.day.glb`: Surface pH [1]; dims `longitude latitude time osurf`; 191.2 MB/yr (ocn2d_day); opps 54M; 14 expts
- [ ] **ph** (day, glb) `ocnBgchem.ph.tavg-op20bar-hxy-sea.day.glb`: pH [1]; dims `longitude latitude time op20bar`; 191.2 MB/yr (ocn2d_day); opps 16L 49M 54M; 75 expts
- [ ] **phyc** (day, glb) `ocnBgchem.phyc.tavg-ols-hxy-sea.day.glb`: Sea Surface Phytoplankton Carbon Concentration [mol m-3]; dims `longitude latitude time osurf`; 191.2 MB/yr (ocn2d_day); opps 54M; 14 expts
- [ ] **phyc** (day, glb) `ocnBgchem.phyc.tavg-op20bar-hxy-sea.day.glb`: Phytoplankton Carbon Concentration [mol m-3]; dims `longitude latitude time op20bar`; 191.2 MB/yr (ocn2d_day); opps 16L 54M; 26 expts

### Low (19 entries, 28.30 GB/yr)

- [ ] **dissi13c** (mon, glb) `ocnBgchem.dissi13c.tavg-ol-hxy-sea.mon.glb`: Dissolved Inorganic Carbon-13 Concentration [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10L; 50 expts
- [ ] **dissi13c** (mon, glb) `ocnBgchem.dissi13c.tavg-ols-hxy-sea.mon.glb`: Surface Dissolved Inorganic Carbon-13 Concentration [mol m-3]; dims `longitude latitude time osurf`; 18.0 MB/yr (ocn2d_mon); opps 10L; 50 expts
- [ ] **dissi14c** (mon, glb) `ocnBgchem.dissi14c.tavg-ol-hxy-sea.mon.glb`: Dissolved Inorganic Carbon-14 Concentration [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10L; 50 expts
- [ ] **dissi14cabio** (mon, glb) `ocnBgchem.dissi14cabio.tavg-ol-hxy-sea.mon.glb`: Abiotic Dissolved Inorganic Carbon-14 Concentration [mol m-3]; dims `longitude latitude olevel time`; 216.6 MB/yr (ocn3d_mon); opps 10L; 50 expts
- [ ] **dissi14cabio** (mon, glb) `ocnBgchem.dissi14cabio.tavg-ols-hxy-sea.mon.glb`: Surface Abiotic Dissolved Inorganic Carbon-14 Concentration [mol m-3]; dims `longitude latitude time osurf`; 18.0 MB/yr (ocn2d_mon); opps 10L; 50 expts
- [ ] **fg13co2** (mon, glb) `ocnBgchem.fg13co2.tavg-u-hxy-sea.mon.glb`: Surface Downward Mass Flux of Carbon-13 as 13CO2 [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 10L; 50 expts; peer pre-check N
- [ ] **fg14co2abio** (mon, glb) `ocnBgchem.fg14co2abio.tavg-u-hxy-sea.mon.glb`: Surface Downward Mass Flux of Carbon-14 as Abiotic 14CO2 [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 18.0 MB/yr (ocn2d_mon); opps 10L; 50 expts; peer pre-check N
- [ ] **expcob** (day, glb) `ocnBgchem.expcob.tavg-u-hxy-sea.day.glb`: Sinking Flux of Particulate Organic Carbon Reaching the Ocean Bottom [mol m-2 s-1]; dims `longitude latitude time`; 191.2 MB/yr (ocn2d_day); opps 16L; 18 expts; peer pre-check S
- [ ] **intpoc** (day, glb) `ocnBgchem.intpoc.tavg-u-hxy-sea.day.glb`: Particulate Organic Carbon Content [kg m-2]; dims `longitude latitude time`; 191.2 MB/yr (ocn2d_day); opps 16L; 18 expts
- [ ] **o2min** (day, glb) `ocnBgchem.o2min.tavg-u-hxy-sea.day.glb`: Oxygen Minimum Concentration [mol m-3]; dims `longitude latitude time`; 191.2 MB/yr (ocn2d_day); opps 16L; 18 expts
- [ ] **phycalc** (day, glb) `ocnBgchem.phycalc.tavg-ol-hxy-sea.day.glb`: Mole Concentration of Calcareous Phytoplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude olevel time`; 5248.4 MB/yr (ocn3d_mon); opps 16L; 18 expts
- [ ] **phydiat** (day, glb) `ocnBgchem.phydiat.tavg-ols-hxy-sea.day.glb`: Surface Mole Concentration of Diatoms Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude time osurf`; 191.2 MB/yr (ocn2d_day); opps 16L; 18 expts
- [ ] **phydiaz** (day, glb) `ocnBgchem.phydiaz.tavg-ols-hxy-sea.day.glb`: Surface Mole Concentration of Diazotrophs Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude time osurf`; 191.2 MB/yr (ocn2d_day); opps 16L; 18 expts
- [ ] **phymisc** (day, glb) `ocnBgchem.phymisc.tavg-ols-hxy-sea.day.glb`: Surface Mole Concentration of Miscellaneous Phytoplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude time osurf`; 191.2 MB/yr (ocn2d_day); opps 16L; 18 expts
- [ ] **phynano** (day, glb) `ocnBgchem.phynano.tavg-u-hxy-sea.day.glb`: Mole Concentration of Nanophytoplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude time`; 191.2 MB/yr (ocn2d_day); opps 16L; 18 expts
- [ ] **phypico** (day, glb) `ocnBgchem.phypico.tavg-ol-hxy-sea.day.glb`: Mole Concentration of Picophytoplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude olevel time`; 5248.4 MB/yr (ocn3d_mon); opps 16L; 18 expts
- [ ] **zmeso** (day, glb) `ocnBgchem.zmeso.tavg-ol-hxy-sea.day.glb`: Mole Concentration of Mesozooplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude olevel time`; 5248.4 MB/yr (ocn3d_mon); opps 16L; 18 expts
- [ ] **zmicro** (day, glb) `ocnBgchem.zmicro.tavg-ol-hxy-sea.day.glb`: Mole Concentration of Microzooplankton Expressed as Carbon in Sea Water [mol m-3]; dims `longitude latitude olevel time`; 5248.4 MB/yr (ocn3d_mon); opps 16L; 18 expts
- [ ] **zooc** (day, glb) `ocnBgchem.zooc.tavg-ol-hxy-sea.day.glb`: Zooplankton Carbon Concentration [mol m-3]; dims `longitude latitude olevel time`; 5248.4 MB/yr (ocn3d_mon); opps 16L; 18 expts

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

Regenerate: `python awi-esm3-lr-variables/obgc/estimate_lr_volume.py obgc` (needs `hr_bytes_per_value_measured.csv` from `measure_hr_bytes_per_value.py`).
