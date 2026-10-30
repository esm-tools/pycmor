# CMIP7 CO2 / carbon-cycle variables outside ocnBgchem for AWI-ESM3 LR

Selection: realm is not ocnBgchem and (variable name contains `co2`, excluding the `*4co2` radiation diagnostics of opportunity 72, or the CF standard name contains `carbon_dioxide`, or the variable is a land CO2 exchange flux without that word: gpp, npp, gppLut, nppLut, gppVgt, nppVgt, gppc13, gppc14, rhLitter, rhSoil). Carbon pools (cLand, cVeg, ...) and internal land transfers (fVegLitter, nppLeaf, ...) are left to the land tier.
ocnBgchem CO2 variables (fgco2, spco2 glb + 30S-90S, dpco2, fg13co2) are in `../obgc/`, not here.
`HR:` = HR yaml tier that has a rule with this compound name; `LR hint:` = a field in the LR test run that could feed it (light check, file names only).

## Summary (per model year, counted variables)

| Priority | Variables | Files/yr | GB/yr (central) | GB/yr (low-high) | GB/yr raw float32 |
|---|---|---|---|---|---|
| High | 28 | 28 | 0.29 | 0.16-0.42 | 0.85 |
| Medium | 29 | 29 | 0.26 | 0.08-0.58 | 1.32 |
| Low | 8 | 8 | 0.01 | 0.01-0.01 | 0.02 |
| **Total** | **65** | **65** | **0.57** | 0.24-1.01 | 2.19 |

| Frequency | Variables | Files/yr | GB/yr (central) | GB/yr (low-high) |
|---|---|---|---|---|
| mon | 58 | 58 | 0.25 | 0.16-0.28 |
| day | 4 | 4 | 0.31 | 0.08-0.72 |
| subhr | 3 | 3 | 0.01 | 0.01-0.01 |

| Priority \ freq | mon | day | subhr |
|---|---|---|---|
| High | 24 / 0.21 | 1 / 0.08 | 3 / 0.01 |
| Medium | 26 / 0.03 | 3 / 0.23 | - |
| Low | 8 / 0.01 | - | - |

(cells: variables / GB per year)

## Totals with and without the expensive parts

| Selection | Variables | Files/yr | GB/yr | GB/yr low-high | TB per 100 model years |
|---|---|---|---|---|---|
| All counted variables | 65 | 65 | 0.57 | 0.24-1.01 | 0.06 |
| Without sub-monthly 3D fields | 65 | 65 | 0.57 | 0.24-1.01 | 0.06 |
| Without the 30S-90S subsets | 65 | 65 | 0.57 | 0.24-1.01 | 0.06 |
| Without sub-monthly 3D and without 30S-90S | 65 | 65 | 0.57 | 0.24-1.01 | 0.06 |
| Core + High only | 28 | 28 | 0.29 | 0.16-0.42 | 0.03 |
| Core + High, without sub-monthly 3D | 28 | 28 | 0.29 | 0.16-0.42 | 0.03 |

Once per run (fx, climatologies, not in the per-year numbers): 2 variables, 17.8 MB.

## Opportunities

| Opportunity | Variables | H / M / L | Only in this one |
|---|---|---|---|
| 10 Benchmarking and Attributing Changes to Global Carbon a | 47 | 19 / 20 / 8 | 8 |
| 18 Constructing a Global Carbon Budget | 39 | 19 / 20 / 0 | 0 |
| 31 Role of fire in the Earth system | 10 | 10 / 0 / 0 | 1 |
| 55 Rapid Evaluation Framework | 9 | 9 / 0 / 0 | 0 |
| 54 Multi-annual-to-decadal predictability of the Earth Sys | 7 | 2 / 5 / 0 | 0 |
| 79 Clouds, circulation and climate sensitivity: extension  | 7 | 7 / 0 / 0 | 3 |
| 43 Land use change | 6 | 0 / 6 / 0 | 6 |
| 5 Understanding the role of atmospheric composition for a | 4 | 4 / 0 / 0 | 0 |
| 78 Clouds, circulation and climate sensitivity: baseline | 4 | 4 / 0 / 0 | 0 |
| 53 Plant Phenology | 4 | 1 / 3 / 0 | 4 |
| 19 Agriculture and Food System Impacts | 2 | 0 / 0 / 2 | 0 |
| 26 Detection and Attribution | 2 | 2 / 0 / 0 | 0 |
| 82 Robust Risk Assessment of Tipping Points | 2 | 0 / 2 / 0 | 0 |
| 81 Dynamical Downscaling | 1 | 1 / 0 / 0 | 0 |
| 51 Paleoclimate Research at the Interface between Past, Pr | 1 | 1 / 0 / 0 | 0 |
| 16 Climate impacts on marine biodiversity and ecosystems | 1 | 1 / 0 / 0 | 0 |
| 44 Changes in marine biogeochemical cycles and ecosystem p | 1 | 1 / 0 / 0 | 0 |
| 11 Terrestrial Biodiversity | 1 | 1 / 0 / 0 | 0 |

Greedy cover (add the opportunity that brings the most new compound names):

| Step | Opportunity | New | Running total |
|---|---|---|---|
| 1 | 10 Benchmarking and Attributing Changes to Global Car | 47 | 47 |
| 2 | 79 Clouds, circulation and climate sensitivity: exten | 6 | 53 |
| 3 | 43 Land use change | 6 | 59 |
| 4 | 53 Plant Phenology | 4 | 63 |
| 5 | 31 Role of fire in the Earth system | 2 | 65 |
| 6 | 54 Multi-annual-to-decadal predictability of the Eart | 1 | 66 |
| 7 | 5 Understanding the role of atmospheric composition  | 1 | 67 |

## Feasibility filter: pending

Only the light `HR:` and `LR hint:` notes are given; no judgement yet.

Peer pre-check (informational, 8 of 67 entries, flux variables only): W/C/S/N from `notes/lr_peer_session/flux_table.md` (W run writes the inputs, C namelist change, S source change, N not in the model). Where `awi-esm3-veg-hr-variables/lr_fluxes_bgc_co2_ice_todo.md` (the parallel session's newer plan) puts the variable in a different section, its code follows after a slash (W pycmor only, X XML + namelist.io, S source change). That file also lists exclusions it calls decided (coccolithophores, carbon isotopes); they are not applied here, only the 3-hourly 3D ficeberg drop by Jan is.

## Variables

### High (30 entries, 0.29 GB/yr)

- [ ] **co2** (mon, glb) `atmos.co2.tavg-al-hxy-u.mon.glb`: Mole Fraction of CO2 [mol mol-1]; dims `longitude latitude alevel time`; 83.2 MB/yr (atm3d_mon); opps 5H 81H; 58 expts; LR hint: OIFS CO2 tracer: atm_remapped_1m_pl_co2 (plev19, kg/kg -> mol/mol); model levels / 2 m not in test run
- [ ] **co2** (mon, glb) `atmos.co2.tavg-h2m-hxy-u.mon.glb`: Near-surface atmosphere CO2 [1E-06]; dims `longitude latitude time height2m`; 1.8 MB/yr (atm2d_mon); opps 19L 51H 54M; 57 expts; LR hint: OIFS CO2 tracer: atm_remapped_1m_pl_co2 (plev19, kg/kg -> mol/mol); model levels / 2 m not in test run
- [ ] **co2** (mon, glb) `atmos.co2.tavg-p19-hxy-air.mon.glb`: Mole Fraction of CO2 [mol mol-1]; dims `longitude latitude plev19 time`; 17.7 MB/yr (atm3d_mon); opps 55H 78H 79H; 61 expts; LR hint: OIFS CO2 tracer: atm_remapped_1m_pl_co2 (plev19, kg/kg -> mol/mol); model levels / 2 m not in test run
- [ ] **co2** (mon, glb) `atmos.co2.tclm-p19-hxy-air.mon.glb`: Mole Fraction of CO2 [mol mol-1]; dims `longitude latitude plev19 time2`; 17.7 MB per run (atm3d_mon); opps 31H 78H 79H; 38 expts; LR hint: OIFS CO2 tracer: atm_remapped_1m_pl_co2 (plev19, kg/kg -> mol/mol); model levels / 2 m not in test run
- [ ] **co2** (mon, glb) `atmos.co2.tclm-u-hm-u.mon.glb`: Total Atmospheric Mass of CO2 [mol mol-1]; dims `time2`; 0.0 MB per run (scalar); opps 16H 26H 44H 54H 78H 79H; 48 expts; LR hint: OIFS CO2 tracer: atm_remapped_1m_pl_co2 (plev19, kg/kg -> mol/mol); model levels / 2 m not in test run
- [ ] **co23D** (mon, glb) `atmos.co23D.tavg-al-hxy-u.mon.glb`: 3D-Field of Transported CO2 [kg kg-1]; dims `longitude latitude alevel time`; 83.2 MB/yr (atm3d_mon); opps 10H 18H; 50 expts; LR hint: OIFS CO2 tracer (model levels not in test run; atm_remapped_1m_pl_co2 is plev19)
- [ ] **co2mass** (mon, glb) `atmos.co2mass.tavg-u-hm-u.mon.glb`: Total Atmospheric Mass of CO2 [kg]; dims `time`; 0.0 MB/yr (scalar); opps 10H 11H 18H 55H 78H 79H; 72 expts; HR: cap7_aerosol; LR hint: OIFS CO2 tracer integrated with air mass (needs 3D co2 + ps)
- [ ] **fAnthDisturb** (mon, glb) `land.fAnthDisturb.tavg-u-hxy-lnd.mon.glb`: Carbon Mass Flux from Vegetation, Litter or Soil Pools into the Atmosphere Due to any Human Activity [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10H 18H; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS fAnthDisturb_monthly.out
- [ ] **fDeforestToAtmos** (mon, glb) `land.fDeforestToAtmos.tavg-u-hxy-lnd.mon.glb`: Deforested Biomass That Goes into Atmosphere as a Result of Anthropogenic Land-Use Change [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10H 18H; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS fDeforestToAtmos_monthly.out
- [ ] **fFire** (mon, glb) `land.fFire.tavg-u-hxy-lnd.mon.glb`: Carbon Mass Flux into Atmosphere Due to CO2 Emission from Fire Excluding Land-Use Change [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 5H 10H 18H 31H; 60 expts; HR: cap7_land; LR hint: LPJ-GUESS fFire_monthly.out
- [ ] **fFireAll** (mon, glb) `land.fFireAll.tavg-u-hxy-lnd.mon.glb`: Carbon Mass Flux into Atmosphere Due to CO2 Emission from Fire Including All Sources [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 5H 10H 18H 82M; 67 expts; HR: cap7_land; LR hint: LPJ-GUESS fFireAll_monthly.out
- [ ] **fFireNat** (mon, glb) `land.fFireNat.tavg-u-hxy-lnd.mon.glb`: Carbon Mass Flux into Atmosphere Due to CO2 Emission from Natural Fire [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 5H 10H 18H 82M; 67 expts; HR: cap7_land; LR hint: LPJ-GUESS fFireNat_monthly.out
- [ ] **fHarvestToAtmos** (mon, glb) `land.fHarvestToAtmos.tavg-u-hxy-lnd.mon.glb`: Harvested Biomass That Goes Straight into Atmosphere as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10H 18H 19L; 66 expts; HR: cap7_land; LR hint: LPJ-GUESS fHarvestToAtmos_monthly.out
- [ ] **fLuc** (mon, glb) `land.fLuc.tavg-u-hxy-lnd.mon.glb`: Net Carbon Mass Flux into Atmosphere Due to Land-Use Change [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 31H; 28 expts; HR: veg_land; LR hint: LPJ-GUESS fLuc_monthly.out
- [ ] **fProductDecomp** (mon, glb) `land.fProductDecomp.tavg-u-hxy-lnd.mon.glb`: Decomposition out of Product Pools to CO2 in Atmosphere as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10H 18H; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS fProductDecomp_monthly.out
- [ ] **fco2all** (mon, glb) `atmos.fco2all.tavg-u-hxy-u.mon.glb`: Carbon Mass Flux into Atmosphere Due to All Emissions of CO2 from All Sources [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.8 MB/yr (atm2d_mon); opps 10H 18H; 50 expts; peer pre-check W; LR hint: OIFS co2apf + co2nbf + co2of
- [ ] **fco2antt** (mon, glb) `atmos.fco2antt.tavg-u-hxy-u.mon.glb`: Carbon Mass Flux into Atmosphere Due to All Anthropogenic Emissions of CO2 [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.8 MB/yr (atm2d_mon); opps 10H 18H 55H; 58 expts; peer pre-check W; HR: cap7_land; LR hint: OIFS co2apf (+ LPJ-GUESS fLuc); LPJ-GUESS fco2antt_monthly.out exists
- [ ] **fco2fos** (mon, glb) `atmos.fco2fos.tavg-u-hxy-u.mon.glb`: Carbon Mass Flux into Atmosphere Due to Fossil Fuel Emissions of CO2 [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.8 MB/yr (atm2d_mon); opps 10H 18H; 50 expts; peer pre-check W; LR hint: OIFS co2apf (atm_remapped_1m_co2apf)
- [ ] **fco2nat** (mon, glb) `atmos.fco2nat.tavg-u-hxy-u.mon.glb`: Surface Carbon Mass Flux into the Atmosphere Due to Natural Sources [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.8 MB/yr (atm2d_mon); opps 10H 18H 31H; 57 expts; peer pre-check W; HR: cap7_land; LR hint: OIFS co2nbf + co2of; LPJ-GUESS fco2nat_monthly.out exists
- [ ] **gpp** (mon, glb) `land.gpp.tavg-u-hxy-lnd.mon.glb`: Carbon Mass Flux out of Atmosphere Due to Gross Primary Production on Land [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10H 18H 31H 54M 55H; 63 expts; HR: cap7_land; LR hint: LPJ-GUESS gpp_monthly.out
- [ ] **nbp** (mon, glb) `land.nbp.tavg-u-hxy-lnd.mon.glb`: Carbon Mass Flux out of Atmosphere Due to Net Biospheric Production on Land [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10H 18H 26H 31H 54H 55H; 63 expts; HR: cap7_land; LR hint: LPJ-GUESS nbp_monthly.out
- [ ] **nep** (mon, glb) `land.nep.tavg-u-hxy-lnd.mon.glb`: Net Carbon Mass Flux out of Atmosphere Due to Net Ecosystem Productivity on Land [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 31H 55H; 60 expts; HR: cap7_land; LR hint: LPJ-GUESS nep_monthly.out
- [ ] **netAtmosLandCO2Flux** (mon, glb) `land.netAtmosLandCO2Flux.tavg-u-hxy-lnd.mon.glb`: Net Flux of CO2 Between Atmosphere and Land (Positive into Land) as a Result of All Processes [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10H 18H; 50 expts; peer pre-check W; LR hint: LPJ-GUESS netAtmosLandCO2Flux_monthly.out
- [ ] **npp** (mon, glb) `land.npp.tavg-u-hxy-lnd.mon.glb`: Net Primary Production on Land as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10H 18H 31H 54M 55H; 63 expts; HR: cap7_land; LR hint: LPJ-GUESS npp_monthly.out
- [ ] **ra** (mon, glb) `land.ra.tavg-u-hxy-lnd.mon.glb`: Carbon Mass Flux into Atmosphere Due to Autotrophic (Plant) Respiration on Land [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10H 18H 31H 54M 55H; 63 expts; HR: cap7_land; LR hint: LPJ-GUESS ra_monthly.out
- [ ] **rh** (mon, glb) `land.rh.tavg-u-hxy-lnd.mon.glb`: Total Heterotrophic Respiration on Land as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10H 18H 31H 54M 55H; 63 expts; HR: cap7_land; LR hint: LPJ-GUESS rh_monthly.out
- [ ] **gppVgt** (day, glb) `land.gppVgt.tavg-u-hxy-multi.day.glb`: Gross Primary Production on Vegetation type as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude vegtype time`; 76.8 MB/yr (lnd2d_day); opps 53H; 17 expts
- [ ] **fco2antt** (subhr, glb) `atmos.fco2antt.tpt-u-hs-u.subhr.glb`: Carbon Mass Flux into Atmosphere Due to All Anthropogenic Emissions of CO2 [kgC m-2 s-1] [kg m-2 s-1]; dims `site time1`; 2.6 MB/yr (atm2d_1hr); opps 79H; 5 expts; peer pre-check N
- [ ] **fco2fos** (subhr, glb) `atmos.fco2fos.tpt-u-hs-u.subhr.glb`: Carbon Mass Flux into Atmosphere Due to Fossil Fuel Emissions of CO2 [kgC m-2 s-1] [kg m-2 s-1]; dims `site time1`; 2.6 MB/yr (atm2d_1hr); opps 79H; 5 expts; peer pre-check N
- [ ] **fco2nat** (subhr, glb) `atmos.fco2nat.tpt-u-hs-u.subhr.glb`: Surface Carbon Mass Flux into the Atmosphere Due to Natural Sources [kgC m-2 s-1] [kg m-2 s-1]; dims `site time1`; 2.6 MB/yr (atm2d_1hr); opps 79H; 5 expts; peer pre-check N

### Medium (29 entries, 0.26 GB/yr)

- [ ] **fLitterFire** (mon, glb) `land.fLitterFire.tavg-u-hxy-lnd.mon.glb`: Carbon Mass Flux from Litter, CWD or any non-Living Pool into Atmosphere Due to CO2 Emission from All Fire [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS fLitterFire_monthly.out
- [ ] **fLulccAtmLut** (mon, glb) `land.fLulccAtmLut.tavg-u-hxy-multi.mon.glb`: Carbon Transferred Directly to Atmosphere Due to any Land-Use or Land-Cover Change Activities [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude landuse time`; 1.9 MB/yr (lnd_multi_mon); opps 43M; 17 expts; HR: veg_land; LR hint: LPJ-GUESS fLulccAtmLut_monthly.out
- [ ] **fVegFire** (mon, glb) `land.fVegFire.tavg-u-hxy-lnd.mon.glb`: Carbon Mass Flux from Vegetation into Atmosphere Due to CO2 Emission from All Fire [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS fVegFire_monthly.out
- [ ] **gpp** (mon, glb) `land.gpp.tavg-u-hxy-ng.mon.glb`: Gross Primary Production on Grass Tiles as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS gppGrass_monthly.out
- [ ] **gpp** (mon, glb) `land.gpp.tavg-u-hxy-shb.mon.glb`: Gross Primary Production on Shrub Tiles as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS gppShrub_monthly.out
- [ ] **gpp** (mon, glb) `land.gpp.tavg-u-hxy-tree.mon.glb`: Gross Primary Production on Tree Tiles as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS gppTree_monthly.out
- [ ] **gppLut** (mon, glb) `land.gppLut.tavg-u-hxy-multi.mon.glb`: Gross Primary Production on Land-Use Tile as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude landuse time`; 1.9 MB/yr (lnd_multi_mon); opps 43M; 17 expts; HR: veg_land; LR hint: LPJ-GUESS gppLut_monthly.out
- [ ] **nbpLut** (mon, glb) `land.nbpLut.tavg-u-hxy-multi.mon.glb`: Net Carbon Mass Flux into Land-Use Tile [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude landuse time`; 1.9 MB/yr (lnd_multi_mon); opps 43M; 17 expts; HR: veg_land; LR hint: LPJ-GUESS nbpLut_monthly.out
- [ ] **npp** (mon, glb) `land.npp.tavg-u-hxy-ng.mon.glb`: Net Primary Production on Grass Tiles as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS nppGrass_monthly.out
- [ ] **npp** (mon, glb) `land.npp.tavg-u-hxy-shb.mon.glb`: Net Primary Production on Shrub Tiles as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS nppShrub_monthly.out
- [ ] **npp** (mon, glb) `land.npp.tavg-u-hxy-tree.mon.glb`: Net Primary Production on Tree Tiles as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS nppTree_monthly.out
- [ ] **nppLut** (mon, glb) `land.nppLut.tavg-u-hxy-multi.mon.glb`: Net Primary Production on Land-Use Tile as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude landuse time`; 1.9 MB/yr (lnd_multi_mon); opps 43M; 17 expts; HR: veg_land; LR hint: LPJ-GUESS nppLut_monthly.out
- [ ] **ra** (mon, glb) `land.ra.tavg-u-hxy-ng.mon.glb`: Autotrophic Respiration on Grass Tiles as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS raGrass_monthly.out
- [ ] **ra** (mon, glb) `land.ra.tavg-u-hxy-shb.mon.glb`: Autotrophic Respiration on Shrub Tiles as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS raShrub_monthly.out
- [ ] **ra** (mon, glb) `land.ra.tavg-u-hxy-tree.mon.glb`: Autotrophic Respiration on Tree Tiles as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS raTree_monthly.out
- [ ] **raLeaf** (mon, glb) `land.raLeaf.tavg-u-hxy-lnd.mon.glb`: Total Respiration from Leaves as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS raLeaf_monthly.out
- [ ] **raLut** (mon, glb) `land.raLut.tavg-u-hxy-multi.mon.glb`: Autotrophic Respiration on Land-Use Tile as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude landuse time`; 1.9 MB/yr (lnd_multi_mon); opps 43M; 17 expts; HR: veg_land; LR hint: LPJ-GUESS raLut_monthly.out
- [ ] **raOther** (mon, glb) `land.raOther.tavg-u-hxy-lnd.mon.glb`: Total Respiration from Other Pools (not Leaves Stem or Roots) as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS raOther_monthly.out
- [ ] **raRoot** (mon, glb) `land.raRoot.tavg-u-hxy-lnd.mon.glb`: Total Respiration from Roots as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS raRoot_monthly.out
- [ ] **raStem** (mon, glb) `land.raStem.tavg-u-hxy-lnd.mon.glb`: Total Respiration from Stem as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS raStem_monthly.out
- [ ] **rh** (mon, glb) `land.rh.tavg-u-hxy-ng.mon.glb`: Heterotrophic Respiration on Grass Tiles as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS rhGrass_monthly.out
- [ ] **rh** (mon, glb) `land.rh.tavg-u-hxy-shb.mon.glb`: Heterotrophic Respiration on Shrub Tiles as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS rhShrub_monthly.out
- [ ] **rh** (mon, glb) `land.rh.tavg-u-hxy-tree.mon.glb`: Heterotrophic Respiration on Tree Tiles as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS rhTree_monthly.out
- [ ] **rhLitter** (mon, glb) `land.rhLitter.tavg-u-hxy-lnd.mon.glb`: Carbon Mass Flux into Atmosphere Due to Heterotrophic Respiration from Litter on Land [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS rhLitter_monthly.out
- [ ] **rhLut** (mon, glb) `land.rhLut.tavg-u-hxy-multi.mon.glb`: Heterotrophic Respiration on Land-Use Tile as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude landuse time`; 1.9 MB/yr (lnd_multi_mon); opps 43M; 17 expts; HR: veg_land; LR hint: LPJ-GUESS rhLut_monthly.out
- [ ] **rhSoil** (mon, glb) `land.rhSoil.tavg-u-hxy-lnd.mon.glb`: Carbon Mass Flux into Atmosphere Due to Heterotrophic Respiration from Soil on Land [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10M 18M; 50 expts; HR: cap7_land; LR hint: LPJ-GUESS rhSoil_monthly.out
- [ ] **nppVgt** (day, glb) `land.nppVgt.tavg-u-hxy-multi.day.glb`: Net Primary Production on Vegetation type as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude vegtype time`; 76.8 MB/yr (lnd2d_day); opps 53M; 17 expts
- [ ] **raVgt** (day, glb) `land.raVgt.tavg-u-hxy-multi.day.glb`: Autotrophic Respiration on Vegetation type as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude vegtype time`; 76.8 MB/yr (lnd2d_day); opps 53M; 17 expts
- [ ] **rhVgt** (day, glb) `land.rhVgt.tavg-u-hxy-multi.day.glb`: Heterotrophic Respiration on Vegetation type as Carbon Mass Flux [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude vegtype time`; 76.8 MB/yr (lnd2d_day); opps 53M; 17 expts

### Low (8 entries, 0.01 GB/yr)

- [ ] **gppc13** (mon, glb) `land.gppc13.tavg-u-hxy-lnd.mon.glb`: Carbon-13 Mass Flux out of Atmosphere Due to Gross Primary Production on Land [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10L; 50 expts
- [ ] **gppc14** (mon, glb) `land.gppc14.tavg-u-hxy-lnd.mon.glb`: Carbon-14 Mass Flux out of Atmosphere Due to Gross Primary Production on Land [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10L; 50 expts
- [ ] **netAtmosLandC13Flux** (mon, glb) `land.netAtmosLandC13Flux.tavg-u-hxy-lnd.mon.glb`: Net Mass Flux of 13C Between Atmosphere and Land (Positive into Land) as a Result of All Processes [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10L; 50 expts
- [ ] **netAtmosLandC14Flux** (mon, glb) `land.netAtmosLandC14Flux.tavg-u-hxy-lnd.mon.glb`: Net Mass Flux of 14C Between Atmosphere and Land (Positive into Land) as a Result of All Processes [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10L; 50 expts
- [ ] **rac13** (mon, glb) `land.rac13.tavg-u-hxy-lnd.mon.glb`: Carbon-13 Mass Flux into Atmosphere Due to Autotrophic (Plant) Respiration on Land [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10L; 50 expts
- [ ] **rac14** (mon, glb) `land.rac14.tavg-u-hxy-lnd.mon.glb`: Carbon-14 Mass Flux into Atmosphere Due to Autotrophic (Plant) Respiration on Land [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10L; 50 expts
- [ ] **rhc13** (mon, glb) `land.rhc13.tavg-u-hxy-lnd.mon.glb`: Carbon-13 Mass Flux into Atmosphere Due to Heterotrophic Respiration on Land [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10L; 50 expts
- [ ] **rhc14** (mon, glb) `land.rhc14.tavg-u-hxy-lnd.mon.glb`: Carbon-14 Mass Flux into Atmosphere Due to Heterotrophic Respiration on Land [kgC m-2 s-1] [kg m-2 s-1]; dims `longitude latitude time`; 1.1 MB/yr (lnd2d_mon); opps 10L; 50 expts

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

DReq metadata columns as in the HR CSVs, then: priority, opportunities (id + H/M/L), opportunity_titles, variable_groups, n_experiments, experiments, status (dropped), peer_precheck (W/C/S/N from notes/lr_peer_session/flux_table.md, informational), size_class, horiz_points, levels, steps_per_year, values (per year, full array), wet_values, bytes_per_value (+low/high), overhead_MB (per file), files_per_year, per_run, MB (per year compressed, central), MB_low, MB_high, raw_MB (float32 uncompressed), note. co2 set also: hr_writes (HR yaml tier), hr_cli123_file, lr_source_hint, co2_subset, MB_400x192.

Regenerate: `python awi-esm3-lr-variables/obgc/estimate_lr_volume.py co2` (needs `hr_bytes_per_value_measured.csv` from `measure_hr_bytes_per_value.py`).
