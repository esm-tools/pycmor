#####
The CMIP7 Data Request v1.2.2.5 (the version cached locally) asks for ocean biogeochemistry and CO2 output in these opportunities. Counts are unique compound names. Where a variable sits in groups of different priority, it is counted once at its highest one (H = High, M = Medium, L = Low). "obgc" means the variable's primary realm is `ocnBgchem`. "CO2" means `co2` appears in the variable name. The two overlap, because `fgco2`, `spco2` and `dpco2` sit in the ocnBgchem realm.

| ID | Opportunity | obgc vars | CO2 vars | CO2 variables named |
|---|---|---|---|---|
| 10 | Benchmarking/Attributing Changes to Global Carbon… | H19 M28 L7 | H9 L2 | co23D, co2mass, fco2all, fco2antt, fco2fos, fco2nat, fg13co2, fg14co2abio, fgco2, netAtmosLandCO2Flux, spco2 |
| 18 | Constructing a Global Carbon Budget | H19 M28 | H9 | same as 10 minus the isotope ones |
| 44 | Changes in marine biogeochemical cycles | H36 M42 | H4 | co2, dpco2, fgco2, spco2 |
| 16 | Climate impacts on marine biodiversity | H36 M6 L16 | H4 | co2, dpco2, fgco2, spco2 |
| 61 | Southern Ocean Biogeochemistry to Clouds | H44 | H1 | spco2 |
| 54 | Multi-annual-to-decadal predictability | H4 M35 | H2 M2 | co2, fgco2, spco2 |
| 81 | Dynamical Downscaling | H29 | H2 | co2, spco2 |
| 31 | Role of fire | H7 | H4 | co2, fco2nat, fgco2, spco2 |
| 49 | Ocean Extremes | M9 | none | |
| 26 | Detection and Attribution | H1 | H2 | co2, fgco2 |
| 55 | Rapid Evaluation Framework | H1 | H4 | co2, co2mass, fco2antt, fgco2 |
| 79 | Clouds/climate sensitivity: extension | none | H7 | co2, co2mass, fco2antt, fco2fos, fco2nat |
| 78 | Clouds/climate sensitivity: baseline | none | H4 | co2, co2mass |
| 5 | Atmospheric composition / air quality | none | H1 | co2 |
| 11 | Terrestrial Biodiversity | none | H1 | co2mass |
| 51 | Paleoclimate | none | H1 | co2 |
| 19 | Agriculture and Food System Impacts | none | L1 | co2 |
| 72 | Diagnosing Radiative Forcing | none | (H12) | only the `*4co2` radiation fluxes (rsut4co2 and similar) |

- **Not real CO2 output in 72:** opportunity 72 only matches because its radiation diagnostics are named `*4co2`. They are radiative fluxes from a quadrupled-CO2 calculation, not CO2 concentration or flux fields.
- **Baseline (69) asks for neither.** No obgc or CO2 variables come in through the Baseline Climate Variables opportunity, and nothing in this list is at Core priority.
- **Biggest obgc requests:** 44, 16 and 61 ask for the most obgc variables. 10 and 18 drive the carbon-cycle and CO2 flux set.
- **28 opportunities ask for neither**, including Ocean Changes (47), Sea Ice (73), Earth's Energy Budget (29) and Land use change (43).

The script that builds this is at `/tmp/claude-24456/-work-ab0246-a270092-software-pycmor/2e3e2817-d698-4fda-8ea8-b32ccb7e05f8/scratchpad/opp.py` if you want the full variable lists per opportunity.
#####
Yes, they overlap a lot. Most opportunities pull from the same shared variable groups, so the per-opportunity counts add up to much more than the number of distinct variables.

- **obgc:** the per-opportunity counts sum to 367, but there are only 178 distinct obgc compound names.
- **CO2 (without the `*4co2` radiation fluxes):** the counts sum to 59, but there are only 20 distinct variables.

Of the 178 obgc variables, 117 are requested by just one opportunity. The other 61 are requested by between 2 and 8 opportunities each.

How many obgc variables each opportunity asks for, and how many of those no other opportunity asks for:

| ID | obgc vars | Only in this one |
|---|---|---|
| 44 Marine biogeochemical cycles | 78 | 33 |
| 16 Marine biodiversity | 58 | 12 |
| 10 Benchmarking carbon | 54 | 7 |
| 18 Global Carbon Budget | 47 | 0 (all covered by 10) |
| 61 Southern Ocean BGC to Clouds | 44 | 44 |
| 54 Decadal predictability | 39 | 12 |
| 81 Dynamical Downscaling | 29 | 4 |
| 49 Ocean Extremes | 9 | 5 |
| 31 Fire | 7 | 0 |
| 26, 55 | 1 each | 0 |

Opportunity 61 looks unique only because of how compound names work. All 44 of its variables are monthly fields over 30S-90S, which counts as a separate compound name from the global field. 34 of its 44 variable names are also requested globally elsewhere, so if you write global monthly output you could probably meet most of 61 by cutting out a region. Whether the data request accepts that is a separate question.

Covering every obgc variable needs only 7 opportunities. Adding the one that brings the most new variables each time gives:

| Step | Opportunity added | New variables | Running total |
|---|---|---|---|
| 1 | 44 | 78 | 78 |
| 2 | 61 | 44 | 122 |
| 3 | 10 | 18 | 140 |
| 4 | 54 | 17 | 157 |
| 5 | 16 | 12 | 169 |
| 6 | 49 | 5 | 174 |
| 7 | 81 | 4 | 178 |

18, 31, 26 and 55 bring nothing new once those are included.
#####
Yes. This run has icebergs and ice-shelf cavities switched on (`use_icebergs`, `use_cavity`, mesh core3), which our coverage notes don't account for yet. [variable_coverage.md:691-706](variable_coverage.md#L691-L706) still marks `ficeberg`, `hfibthermds` and `flandice` as ❌ because "use_icebergs=.false.", which was true for the VEG-HR setup only.

**Already in the output, needs only a pycmor rule**

| Variable | Requested by | Source in this run |
|---|---|---|
| `ocean.ficeberg.tavg-u-hxy-sea.mon.glb` | 39H 51H 47M | `(ibfwb+ibfwbv+ibfwl+ibfwe) * 1000` (m/s to kg m-2 s-1). Positive means into the ocean. The fields are NaN under cavities, so fill those with 0. |
| `ocean.ficeberg.tavg-ol-hxy-sea.mon.glb` | 81H 47M | The same 2D flux put in the top level, zeros below. FESOM adds iceberg meltwater only at the surface ([icb_coupling.F90:205](../../model_codes/awiesm3-v3.5.1/fesom-2.8/src/icb_coupling.F90#L205)). |
| `ocean.hfibthermds.tavg-ol-hxy-sea.mon.glb` | 47M | `ibhf` directly. It is 3D (nod2, nz1) in W/m2 per level, and negative means cooling. |
| `ocean.hfibthermds.tavg-u-hxy-sea.mon.glb` | 47M | `ibhf` summed over depth |
| `ocean.flandice.tavg-u-hxy-sea.mon.glb` | 39H 51H | Only partly covered: `-fw * 1000` at cavity nodes (ice-shelf basal melt), 0 elsewhere. Land-ice surface runoff is mixed into the general runoff and can't be separated. `use_landice_water=.false.` |
| `landIce.libmassbf.tavg-u-hxy-fis.{mon,yr}.ata` | 39M | `fw * 1000` at cavity nodes. Under the shelves FESOM sets `water_flux` to the 3-equation melt rate ([cavity_param.F90:308](../../model_codes/awiesm3-v3.5.1/fesom-2.8/src/cavity_param.F90#L308)). Year 2190 averages -3.9e-8 m/s (about -1.2 m/yr), and 94% of cavity nodes are melting, so the sign already matches the convention (negative means mass loss). |
| `landIce.sftflf.ti-u-hxy-u.fx.glb` | 39H 51H | Cavity mask from the mesh (`cavity_nlvls.out`, or simply where `sst` is NaN): 10047 nodes, all south of 60S |
| `landIce.sftflf.tavg-u-hxy-u.mon.glb` and `.yr.ata` | 39H 51H 82H, 39M | The same mask repeated in time, since the cavity geometry is fixed |
| `landIce.iareafl.tavg-u-hm-fis.yr.ata` | 39M | Sum of node areas over the cavity nodes, so a constant |

**Need a small change first**

- **3-hourly `ficeberg` and `flandice` (opportunity 16H):** the `icb` stream hardcodes `1, 'm'` at [io_meandata.F90:2107-2111](../../model_codes/awiesm3-v3.5.1/fesom-2.8/src/io_meandata.F90#L2107-L2111), so 3-hourly iceberg output needs a one-line source change to use `io_list(i)%freq`. For `flandice`, setting `fw` to `3, 'h'` in namelist.io is enough.
- **`litempbot` (39M):** the interface freezing temperature `tf` is computed in `cavity_heat_water_fluxes_3eq` but never saved. Outputting it means a new array plus a `def_stream` call.

**Can't produce**

- **Greenland (`grl`) variants:** core3 has no Greenland cavities, so these are empty or zero.
- **`licalvf`, `lifmassbf`, `tendlicalvf`:** these are calving and ice-front variables on the ice-sheet grid. Calving here is prescribed through `icb_*.dat` (659 seeds). A yearly total could be computed offline from those files, but it isn't model output.
- **`libmassbf`/`litempbot` with `gis` (grounded ice) and `sftgrf`:** these are ice-sheet model quantities.

All of the `landIce` ones come only from opportunity 39 (Ice sheet mass loss), mostly at Medium priority. The High-priority wins are `ficeberg` 2D, `flandice` (partial), `sftflf`, and `ficeberg` 3D for 81.

I haven't updated the todo and coverage files. Should I flip those entries and add the rules?
#####
I made the list from the obgc, CO2 and iceberg/ice-shelf sets discussed above, treating anything flux-shaped as a flux. That's 86 flux variables plus the 3D production and grazing rates (`pp`, `graz`), 91 entries in all. Volumes are uncompressed float32 per model year on the LR native grids (core3: 220,509 nodes × 47 levels; TCo95: 40,320 points). Real 3D files compress to roughly half (`DetC` is 249 MB against 497 MB raw).

**Summary**

| Status | Entries | Raw volume per year |
|---|---|---|
| Written now: the run already outputs the input fields, only a pycmor rule is needed | 26 | 1.6 GB |
| Namelist change only | 2 | 2.9 GB |
| Small FESOM/REcoM source change (the quantity exists internally but isn't output) | 22 | 2.2 GB, plus 121 GB for 3-hourly 3D `ficeberg` |
| Not in the model | 41 | none |

The 121 GB is misleading. FESOM adds iceberg meltwater only at the surface, so 46 of the 47 levels are zeros and the file compresses to about the 2D size (around 2.6 GB).

**Already have pycmor rules:** only `fco2antt` and `fco2nat`, in [cmip7_awiesm3-veg-hr_cap7_land.yaml:793-810](awi-esm3-veg-hr-variables/cap7_land/cmip7_awiesm3-veg-hr_cap7_land.yaml#L793-L810). Both read the LPJ-GUESS land-only files, so they leave out fossil emissions (`co2apf`) and the ocean flux (`co2of`) that the CMIP definitions include. Nothing for ocean BGC, icebergs or cavities exists yet.

### Written now (rule only)

| Variable | Requested by | From | MB/yr |
|---|---|---|---|
| `fgco2` mon | 10H 16H 18H 26H 31H 44H 54H 55H | `CO2f` | 11 |
| `fsfe` mon | 81H | `aFe` | 11 |
| `fsn` mon | 81H | `aN` (no N2 fixation, rivers off) | 11 |
| `intpp` mon glb / 30S-90S | 10H 16H 18H 44H 54H / 61H | `NPPn + NPPd` | 11 / 4 |
| `intppdiat` mon glb / 30S-90S | 10M 16M 18M 44M / 61H | `NPPd` | 11 / 4 |
| `intppnano` mon | 16M 44M | `NPPn` | 11 |
| `intpbn` 30S-90S | 61H | `NNAn + NNAd` | 4 |
| `intpbfe` 30S-90S | 61H | `(NNAn + NNAd) × Fe2N` (fixed 0.033) | 4 |
| `intpcalcite` 30S-90S | 61H | `calcif` summed over depth (check its units label) | 4 |
| `pp` 3D / surface glb / surface 30S-90S | 10M 16M 18M 44M / 16H 44H / 61H | `NPPn3D + NPPd3D` | 497 / 11 / 4 |
| `fco2all`, `fco2antt`, `fco2fos`, `fco2nat` mon | 10H 18H (+55H, 31H) | OIFS `co2apf`, `co2nbf`, `co2fire`, `co2of` | 2 each |
| `netAtmosLandCO2Flux` mon | 10H 18H | LPJ-GUESS `netAtmosLandCO2Flux_monthly.out` | 2 |
| `ficeberg` 2D / 3D mon | 39H 51H 47M / 81H 47M | `ibfwb + ibfwbv + ibfwl + ibfwe` | 11 / 497 (mostly zeros) |
| `hfibthermds` 2D / 3D mon | 47M | `ibhf` | 11 / 497 |
| `flandice` mon | 39H 51H | `-fw` under cavities (ice-shelf basal melt only) | 11 |
| `libmassbf` fis ata mon / yr | 39M | `fw` at cavity nodes | 2 / <1 |

### Namelist change

| Variable | Requested by | Change | MB/yr |
|---|---|---|---|
| `intpp` day | 16H 44H 54M | write `NPPn` and `NPPd` daily | 322 |
| `flandice` 3hr | 16H | write `fw` 3-hourly | 2,576 |

### Source change

| Variable | Requested by | What's missing | MB/yr |
|---|---|---|---|
| `expc`, `expcalc` at 100 m, 1000 m and 3D | 10H 18H 16H 44H (+M) | Sinking fluxes aren't saved (1) | 11 per level, 497 per 3D field |
| `epn`, `epsi` at 100 m | 10M 18M, 54M | Same as above | 11 each |
| `froc`, `fric`, `expcob` (mon and day), `expnob`, `expsiob`, `expcalcob`, `expfeob` | 16H 44H (+M, 16L for day) | Bottom flux into the benthos is computed but has no output stream | 11 each, 322 for `expcob` day |
| `frn`, `frfe` | 44M | Burial, denitrification and scavenging are internal only | 11 each |
| `graz` glb / 30S-90S | 16M 44M / 61H | The `vertgraz*` arrays exist but have no stream | 497 / 210 |
| `intpbsi` | 61H | Diatom Si uptake isn't saved | 4 |
| `ficeberg` 3hr 3D | 16H | The iceberg stream is hardcoded to monthly | 121,051 raw |

(1) An approximation would be possible by adding `wsink_det1/2` to namelist.io and multiplying by detritus concentration. With `use_ballasting = .true.` the sinking speed changes over time, so a monthly-mean product isn't exact.

### Not in the model (41 entries)

| Reason | Variables |
|---|---|
| No phosphorus | `epp`, `exppob`, `intpbp` |
| No aragonite | `exparagob`, `intparag` |
| No DMS | `fgdms` |
| No
#####
Here's the full table, without further checks. In the Status column, W means the run already writes the inputs (only a pycmor rule is needed), C means a namelist change, S a source change, and N not in the model. MB/yr is uncompressed float32 on core3 (220,509 nodes × 47 levels) or TCo95 (40,320 points).

### Ocean biogeochemistry

| Variable | Requested by | Status | Source / reason | MB/yr |
|---|---|---|---|---|
| `ocnBgchem.fgco2.tavg-u-hxy-sea.mon.glb` | 10H 16H 18H 26H 31H 44H 54H 55H | W | CO2f | 11 |
| `ocnBgchem.fsfe.tavg-u-hxy-sea.mon.glb` | 81H | W | aFe (rivers off) | 11 |
| `ocnBgchem.fsn.tavg-u-hxy-sea.mon.glb` | 81H | W | aN (no N2 fixation, rivers off) | 11 |
| `ocnBgchem.intpbfe.tavg-u-hxy-sea.mon.30S-90S` | 61H | W | (NNAn + NNAd) × Fe2N | 4 |
| `ocnBgchem.intpbn.tavg-u-hxy-sea.mon.30S-90S` | 61H | W | NNAn + NNAd | 4 |
| `ocnBgchem.intpcalcite.tavg-u-hxy-sea.mon.30S-90S` | 61H | W | calcif summed over depth | 4 |
| `ocnBgchem.intpp.tavg-u-hxy-sea.mon.30S-90S` | 61H | W | NPPn + NPPd | 4 |
| `ocnBgchem.intpp.tavg-u-hxy-sea.mon.glb` | 10H 16H 18H 44H 54H | W | NPPn + NPPd | 11 |
| `ocnBgchem.intppdiat.tavg-u-hxy-sea.mon.30S-90S` | 61H | W | NPPd | 4 |
| `ocnBgchem.intppdiat.tavg-u-hxy-sea.mon.glb` | 10M 16M 18M 44M | W | NPPd | 11 |
| `ocnBgchem.intppnano.tavg-u-hxy-sea.mon.glb` | 16M 44M | W | NPPn | 11 |
| `ocnBgchem.pp.tavg-ol-hxy-sea.mon.glb` | 10M 16M 18M 44M | W | NPPn3D + NPPd3D | 497 |
| `ocnBgchem.pp.tavg-ols-hxy-sea.mon.30S-90S` | 61H | W | NPPn3D + NPPd3D, top layer | 4 |
| `ocnBgchem.pp.tavg-ols-hxy-sea.mon.glb` | 16H 44H | W | NPPn3D + NPPd3D, top layer | 11 |
| `ocnBgchem.intpp.tavg-u-hxy-sea.day.glb` | 16H 44H 54M | C | NPPn, NPPd daily | 322 |
| `ocnBgchem.epn.tavg-d100m-hxy-sea.mon.glb` | 10M 18M | S | sinking flux not saved | 11 |
| `ocnBgchem.epsi.tavg-d100m-hxy-sea.mon.glb` | 54M | S | sinking flux not saved | 11 |
| `ocnBgchem.expc.tavg-d1000m-hxy-sea.mon.glb` | 16H 44H | S | sinking flux not saved | 11 |
| `ocnBgchem.expc.tavg-d100m-hxy-sea.mon.glb` | 10M 16H 18M 44H 54M | S | sinking flux not saved | 11 |
| `ocnBgchem.expc.tavg-ol-hxy-sea.mon.glb` | 10H 18H 44M 54M | S | sinking flux not saved | 497 |
| `ocnBgchem.expcalc.tavg-d1000m-hxy-sea.mon.glb` | 16H 44H | S | sinking flux not saved | 11 |
| `ocnBgchem.expcalc.tavg-d100m-hxy-sea.mon.glb` | 10M 16H 18M 44H 54M | S | sinking flux not saved | 11 |
| `ocnBgchem.expcalc.tavg-ol-hxy-sea.mon.glb` | 10H 18H 44M | S | sinking flux not saved | 497 |
| `ocnBgchem.expcalcob.tavg-u-hxy-sea.mon.glb` | 44M | S | bottom flux into benthos, no stream | 11 |
| `ocnBgchem.expcob.tavg-u-hxy-sea.day.glb` | 16L | S | bottom flux into benthos, no stream | 322 |
| `ocnBgchem.expcob.tavg-u-hxy-sea.mon.glb` | 44M | S | bottom flux into benthos, no stream | 11 |
| `ocnBgchem.expfeob.tavg-u-hxy-sea.mon.glb` | 44M | S | bottom N flux × Fe2N | 11 |
| `ocnBgchem.expnob.tavg-u-hxy-sea.mon.glb` | 44M | S | bottom flux into benthos, no stream | 11 |
| `ocnBgchem.expsiob.tavg-u-hxy-sea.mon.glb` | 44M | S | bottom flux into benthos, no stream | 11 |
| `ocnBgchem.frfe.tavg-u-hxy-sea.mon.glb` | 44M | S | Fe burial/scavenging, internal only | 11 |
| `ocnBgchem.fric.tavg-u-hxy-sea.mon.glb` | 10M 16H 18M 44H | S | bottom flux into benthos, no stream | 11 |
| `ocnBgchem.frn.tavg-u-hxy-sea.mon.glb` | 44M | S | benthic N loss + denitrification, internal only | 11 |
| `ocnBgchem.froc.tavg-u-hxy-sea.mon.glb` | 10M 16H 18M 44H | S | bottom flux into benthos, no stream | 11 |
| `ocnBgchem.graz.tavg-ol-hxy-sea.mon.30S-90S` | 61H | S | vertgraz* arrays exist, no stream | 210 |
| `ocnBgchem.graz.tavg-ol-hxy-sea.mon.glb` | 16M 44M | S | vertgraz* arrays exist, no stream | 497 |
| `ocnBgchem.intpbsi.tavg-u-hxy-sea.mon.30S-90S` | 61H | S | diatom Si uptake not saved | 4 |
| `ocnBgchem.epp.tavg-d100m-hxy-sea.mon.glb` | 10M 18M | N | no phosphorus | 11 |
| `ocnBgchem.exparagob.tavg-u-hxy-sea.mon.glb` | 44M | N | no aragonite | 11 |
| `ocnBgchem.exppob.tavg-u-hxy-sea.mon.glb` | 44M | N | no phosphorus | 11 |
| `ocnBgchem.fg13co2.tavg-u-hxy-sea.mon.glb` | 10L | N | ciso off, no 13C air-sea flux stream | 11 |
| `ocnBgchem.fg14co2abio.tavg-u-hxy-sea.mon.glb` | 10L | N | transit off, no 14C air-sea flux stream | 11 |
| `ocnBgchem.fgdms.tavg-u-hxy-sea.mon.glb` | 10M 18M | N | no DMS | 11 |
| `ocnBgchem.icfriver.tavg-u-hxy-sea.mon.glb` | 10M 18M 81H | N | rivers off, no stream | 11 |
| `ocnBgchem.intparag.tavg-u-hxy-sea.mon.30S-90S` | 61H | N | no aragonite | 4 |
| `ocnBgchem.intpbp.tavg-u-hxy-sea.mon.30S-90S` | 61H | N | no phosphorus | 4 |
| `ocnBgchem.intpn2.tavg-u-hxy-sea.mon.30S-90S` | 61H | N | no N2 fixation | 4 |
| `ocnBgchem.intppcalc.tavg-u-hxy-sea.mon.30S-90S` | 61H | N | coccos off | 4 |
| `ocnBgchem.intppcalc.tavg-u-hxy-sea.mon.glb` | 10M 16M 18M 44M | N | coccos off | 11 |
| `ocnBgchem.intppdiaz.tavg-u-hxy-sea.mon.30S-90S` | 61H | N | no diazotrophs | 4 |
| `ocnBgchem.intppdiaz.tavg-u-hxy-sea.mon.glb` | 10M 16M 18M 44M | N | no diazotrophs | 11 |
| `ocnBgchem.intppmisc.tavg-u-hxy-sea.mon.30S-90S` | 61H | N | small phyto already counted as nano | 4 |
| `ocnBgchem.intppmisc.tavg-u-hxy-sea.mon.glb` | 10M 16M 18M 44M | N | small phyto already counted as nano | 11 |
| `ocnBgchem.intppnitrate.tavg-u-hxy-sea.mon.30S-90S` | 61H | N | DIN not split into NO3/NH4 | 4 |
| `ocnBgchem.intppnitrate.tavg-u-hxy-sea.mon.glb` | 10M 18M | N | DIN not split into NO3/NH4 | 11 |
| `ocnBgchem.intpppico.tavg-u-hxy-sea.mon.30S-90S` | 61H | N | no picophytoplankton | 4 |
| `ocnBgchem.intpppico.tavg-u-hxy-sea.mon.glb` | 10M 16M 18M 44M | N | no picophytoplankton | 11 |
| `ocnBgchem.ocfriver.tavg-u-hxy-sea.mon.glb` | 81H | N | rivers off, no stream | 11 |
| `ocnBgchem.sigpp.tavg-u-hxy-si.mon.glb` | 44M | N | no sea-ice algae | 11 |

### Atmosphere and land CO2

| Variable | Requested by | Status | Source / reason | MB/yr |
|---|---|---|---|---|
| `atmos.fco2all.tavg-u-hxy-u.mon.glb` | 10H 18H | W | co2apf + co2nbf + co2fire + co2of | 2 |
| `atmos.fco2antt.tavg-u-hxy-u.mon.glb` | 10H 18H 55H | W | co2apf (+ LPJ-GUESS fLuc) | 2 |
| `atmos.fco2fos.tavg-u-hxy-u.mon.glb` | 10H 18H | W | co2apf | 2 |
| `atmos.fco2nat.tavg-u-hxy-u.mon.glb` | 10H 18H 31H | W | co2nbf + co2fire + co2of | 2 |
| `land.netAtmosLandCO2Flux.tavg-u-hxy-lnd.mon.glb` | 10H 18H | W | LPJ-GUESS netAtmosLandCO2Flux_monthly.out | 2 |
| `atmos.fco2antt.tpt-u-hs-u.subhr.glb` | 79H | N | no sub-hourly site output | ~0 |
| `atmos.fco2fos.tpt-u-hs-u.subhr.glb` | 79H | N | no sub-hourly site output | ~0 |
| `atmos.fco2nat.tpt-u-hs-u.subhr.glb` | 79H | N | no sub-hourly site output | ~0 |

### Icebergs and ice shelves

| Variable | Requested by | Status | Source / reason | MB/yr |
|---|---|---|---|---|
| `landIce.libmassbf.tavg-u-hxy-fis.mon.ata` | 39M | W | fw at cavity nodes | 2 |
| `landIce.libmassbf.tavg-u-hxy-fis.yr.ata` | 39M | W | fw at cavity nodes | <1 |
| `ocean.ficeberg.tavg-ol-hxy-sea.mon.glb` | 47M 81H | W | ibfwb + ibfwbv + ibfwl + ibfwe, top level | 497 |
| `ocean.ficeberg.tavg-u-hxy-sea.mon.glb` | 39H 47M 51H | W | ibfwb + ibfwbv + ibfwl + ibfwe | 11 |
| `ocean.flandice.tavg-u-hxy-sea.mon.glb` | 39H 51H | W | partial: fw under cavities only | 11 |
| `ocean.hfibthermds.tavg-ol-hxy-sea.mon.glb` | 47M | W | ibhf | 497 |
| `ocean.hfibthermds.tavg-u-hxy-sea.mon.glb` | 47M | W | ibhf summed over depth | 11 |
| `ocean.flandice.tavg-u-hxy-sea.3hr.glb` | 16H | C | fw 3-hourly | 2,576 |
| `ocean.ficeberg.tavg-ol-hxy-sea.3hr.glb` | 16H | S | iceberg stream hardcoded monthly | 121,051 |
| `landIce.libmassbf.tavg-u-hxy-fis.mon.grl` | 39M | N | no Greenland cavities | ~0 |
| `landIce.libmassbf.tavg-u-hxy-fis.yr.grl` | 39M | N | no Greenland cavities | ~0 |
| `landIce.libmassbf.tavg-u-hxy-gis.mon.ata` | 39M | N | grounded ice, no ice-sheet model | 2 |
| `landIce.libmassbf.tavg-u-hxy-gis.mon.grl` | 39M | N | grounded ice, no ice-sheet model | ~0 |
| `landIce.libmassbf.tavg-u-hxy-gis.yr.ata` | 39M | N | grounded ice, no ice-sheet model | <1 |
| `landIce.libmassbf.tavg-u-hxy-gis.yr.grl` | 39M | N | grounded ice, no ice-sheet model | ~0 |
| `landIce.licalvf.tavg-u-hxy-is.mon.ata` | 39M | N | no ice-sheet model | 2 |
| `landIce.licalvf.tavg-u-hxy-is.mon.grl` | 39M | N | no ice-sheet model | 2 |
| `landIce.licalvf.tavg-u-hxy-is.yr.ata` | 39M | N | no ice-sheet model | <1 |
| `landIce.licalvf.tavg-u-hxy-is.yr.grl` | 39M | N | no ice-sheet model | <1 |
| `landIce.lifmassbf.tavg-u-hxy-is.mon.ata` | 39M | N | no ice-sheet model | 2 |
| `landIce.lifmassbf.tavg-u-hxy-is.mon.grl` | 39M | N | no ice-sheet model | 2 |
| `landIce.lifmassbf.tavg-u-hxy-is.yr.ata` | 39M | N | no ice-sheet model | <1 |
| `landIce.lifmassbf.tavg-u-hxy-is.yr.grl` | 39M | N | no ice-sheet model | <1 |
| `landIce.tendlicalvf.tavg-u-hm-is.yr.ata` | 39M | N | no ice-sheet model | ~0 |
| `landIce.tendlicalvf.tavg-u-hm-is.yr.grl` | 39M | N | no ice-sheet model | ~0 |

### Totals

| Status | Entries | Raw volume per year |
|---|---|---|
| W | 26 | 1.6 GB |
| C | 2 | 2.9 GB |
| S | 22 | 123 GB, of which 121 GB is 3-hourly 3D `ficeberg` (mostly zeros, compresses to roughly 2.6 GB) |
| N | 41 | 0.2 GB |

Of the W fluxes, only `fco2antt` and `fco2nat` have pycmor rules so far, and those read the LPJ-GUESS land-only files.

The table is also saved at `/tmp/claude-24456/-work-ab0246-a270092-software-pycmor/2e3e2817-d698-4fda-8ea8-b32ccb7e05f8/scratchpad/table.md`.
#####
It's the iceberg meltwater flux into the ocean (kg m-2 s-1), every 3 hours, as a 3D field on all ocean levels. Only opportunity 16 (Climate impacts on marine biodiversity) asks for it, at High priority.

For 2000 simulated years:

- **Uncompressed:** 121 GB/yr × 2000 = **about 242 TB**.
- **Compressed:** about **5 TB** at best. FESOM adds iceberg melt only at the surface, so 46 of the 47 levels are zeros. That leaves about the size of a 2D 3-hourly field (2.6 GB/yr), plus chunk overhead. Even the 5 TB contains nothing beyond the surface layer.

I'd skip it. The 3D layout carries no information our model has, because the melt goes into the surface layer. The data request's own note on the 2D `ficeberg` says to report the 2D field if only the vertically integrated flux is available, which is our case. The monthly 2D `ficeberg` (11 MB/yr, 22 GB over 2000 years) already covers what we can provide.

The 3-hourly 2D `flandice` for the same opportunity is about the same size as the compressed case (2.6 GB/yr, about 5 TB over 2000 years). It deserves the same scrutiny before we commit to it.

Opportunity 16 probably doesn't want 3-hourly output for every experiment, so the real number depends on which experiments and how many years it covers. I haven't checked that yet.
#####
No. 3-hourly `ficeberg` isn't part of the baseline. It only comes in if we sign up for opportunity 16, Climate impacts on marine biodiversity and ecosystems. Opportunities are opt-in, and no other opportunity asks for either 3-hourly field.

Inside opportunity 16 it belongs to the variable group `ISIMIP_oceanforcing_3hr` (10 variables, High priority), together with 3-hourly `flandice`. That group covers these experiments:

- **historical:** historical, esm-hist
- **scenarios:** all scen7 and esm-scen7 runs
- **piControl:** piControl, esm-piControl

If we do take part in opportunity 16, we'd still deliver 3D `ficeberg` as the surface value with zeros below. The variable's processing note asks for the native grid, and our iceberg melt only goes into the top layer. Raw, that's about 121 GB/yr; compressed, it's roughly the size of a 2D field. If we don't take part in 16, neither 3-hourly field is needed.
