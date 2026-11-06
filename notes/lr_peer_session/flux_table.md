
### Ocean biogeochemistry

| Variable | Requested by | Status | Source / reason | MB/yr |
|---|---|---|---|---|
| `ocnBgchem.fgco2.tavg-u-hxy-sea.mon.glb` | 10H 16H 18H 26H 31H 44H 54H 55H | W | CO2f (mmolC/m2/d, + into ocean) | 11 |
| `ocnBgchem.fsfe.tavg-u-hxy-sea.mon.glb` | 81H | W | aFe (umolFe/m2/s); rivers off | 11 |
| `ocnBgchem.fsn.tavg-u-hxy-sea.mon.glb` | 81H | W | aN (mmolN/m2/s); no N2 fixation, rivers off | 11 |
| `ocnBgchem.intpbfe.tavg-u-hxy-sea.mon.30S-90S` | 61H | W | (NNAn + NNAd) x Fe2N, fixed ratio | 4 |
| `ocnBgchem.intpbn.tavg-u-hxy-sea.mon.30S-90S` | 61H | W | NNAn + NNAd | 4 |
| `ocnBgchem.intpcalcite.tavg-u-hxy-sea.mon.30S-90S` | 61H | W | calcif summed over depth | 4 |
| `ocnBgchem.intpp.tavg-u-hxy-sea.mon.30S-90S` | 61H | W | NPPn + NPPd (2D, depth-integrated) | 4 |
| `ocnBgchem.intpp.tavg-u-hxy-sea.mon.glb` | 10H 16H 18H 44H 54H | W | NPPn + NPPd (2D, depth-integrated) | 11 |
| `ocnBgchem.intppdiat.tavg-u-hxy-sea.mon.30S-90S` | 61H | W | NPPd | 4 |
| `ocnBgchem.intppdiat.tavg-u-hxy-sea.mon.glb` | 10M 16M 18M 44M | W | NPPd | 11 |
| `ocnBgchem.intppnano.tavg-u-hxy-sea.mon.glb` | 16M 44M | W | NPPn (small phytoplankton) | 11 |
| `ocnBgchem.pp.tavg-ol-hxy-sea.mon.glb` | 10M 16M 18M 44M | W | NPPn3D + NPPd3D | 497 |
| `ocnBgchem.pp.tavg-ols-hxy-sea.mon.30S-90S` | 61H | W | NPPn3D + NPPd3D | 4 |
| `ocnBgchem.pp.tavg-ols-hxy-sea.mon.glb` | 16H 44H | W | NPPn3D + NPPd3D | 11 |
| `ocnBgchem.intpp.tavg-u-hxy-sea.day.glb` | 16H 44H 54M | C | NPPn, NPPd daily in namelist.io | 322 |
| `ocnBgchem.epn.tavg-d100m-hxy-sea.mon.glb` | 10M 18M | S | sinking flux not saved; ballasting makes wsink x mean(Det) only approximate | 11 |
| `ocnBgchem.epsi.tavg-d100m-hxy-sea.mon.glb` | 54M | S | as epn: DetSi, idetz2si | 11 |
| `ocnBgchem.expc.tavg-d1000m-hxy-sea.mon.glb` | 16H 44H | S | as epn: DetC*wsink_det1 + idetz2c*wsink_det2 | 11 |
| `ocnBgchem.expc.tavg-d100m-hxy-sea.mon.glb` | 10M 16H 18M 44H 54M | S | as epn: DetC*wsink_det1 + idetz2c*wsink_det2 | 11 |
| `ocnBgchem.expc.tavg-ol-hxy-sea.mon.glb` | 10H 18H 44M 54M | S | as epn: DetC*wsink_det1 + idetz2c*wsink_det2 | 497 |
| `ocnBgchem.expcalc.tavg-d1000m-hxy-sea.mon.glb` | 16H 44H | S | as epn: DetCalc, idetz2calc | 11 |
| `ocnBgchem.expcalc.tavg-d100m-hxy-sea.mon.glb` | 10M 16H 18M 44H 54M | S | as epn: DetCalc, idetz2calc | 11 |
| `ocnBgchem.expcalc.tavg-ol-hxy-sea.mon.glb` | 10H 18H 44M | S | as epn: DetCalc, idetz2calc | 497 |
| `ocnBgchem.expcalcob.tavg-u-hxy-sea.mon.glb` | 44M | S | as expcob | 11 |
| `ocnBgchem.expcob.tavg-u-hxy-sea.day.glb` | 16L | S | bottom sinking flux into benthos exists internally, no stream | 322 |
| `ocnBgchem.expcob.tavg-u-hxy-sea.mon.glb` | 44M | S | bottom sinking flux into benthos exists internally, no stream | 11 |
| `ocnBgchem.expfeob.tavg-u-hxy-sea.mon.glb` | 44M | S | as expnob x Fe2N (fixed 0.033) | 11 |
| `ocnBgchem.expnob.tavg-u-hxy-sea.mon.glb` | 44M | S | as expcob | 11 |
| `ocnBgchem.expsiob.tavg-u-hxy-sea.mon.glb` | 44M | S | as expcob | 11 |
| `ocnBgchem.frfe.tavg-u-hxy-sea.mon.glb` | 44M | S | Fe burial/scavenging, internal only | 11 |
| `ocnBgchem.fric.tavg-u-hxy-sea.mon.glb` | 10M 16H 18M 44H | S | as expcalcob | 11 |
| `ocnBgchem.frn.tavg-u-hxy-sea.mon.glb` | 44M | S | benthic N loss + denitrification, internal only | 11 |
| `ocnBgchem.froc.tavg-u-hxy-sea.mon.glb` | 10M 16H 18M 44H | S | as expcob | 11 |
| `ocnBgchem.graz.tavg-ol-hxy-sea.mon.30S-90S` | 61H | S | vertgraz* arrays exist, no stream | 210 |
| `ocnBgchem.graz.tavg-ol-hxy-sea.mon.glb` | 16M 44M | S | vertgraz* arrays exist, no stream | 497 |
| `ocnBgchem.intpbsi.tavg-u-hxy-sea.mon.30S-90S` | 61H | S | diatom Si uptake not saved | 4 |
| `ocnBgchem.epp.tavg-d100m-hxy-sea.mon.glb` | 10M 18M | N | no phosphorus in REcoM | 11 |
| `ocnBgchem.exparagob.tavg-u-hxy-sea.mon.glb` | 44M | N | no aragonite | 11 |
| `ocnBgchem.exppob.tavg-u-hxy-sea.mon.glb` | 44M | N | no phosphorus | 11 |
| `ocnBgchem.fg13co2.tavg-u-hxy-sea.mon.glb` | 10L | N | ciso=.false.; with ciso on still needs a 13C air-sea flux stream | 11 |
| `ocnBgchem.fg14co2abio.tavg-u-hxy-sea.mon.glb` | 10L | N | use_transit=.false.; no abiotic 14C air-sea flux stream | 11 |
| `ocnBgchem.fgdms.tavg-u-hxy-sea.mon.glb` | 10M 18M | N | no DMS in REcoM | 11 |
| `ocnBgchem.icfriver.tavg-u-hxy-sea.mon.glb` | 10M 18M 81H | N | userivers=.false.; no riverine DIC stream | 11 |
| `ocnBgchem.intparag.tavg-u-hxy-sea.mon.30S-90S` | 61H | N | no aragonite | 4 |
| `ocnBgchem.intpbp.tavg-u-hxy-sea.mon.30S-90S` | 61H | N | no phosphorus | 4 |
| `ocnBgchem.intpn2.tavg-u-hxy-sea.mon.30S-90S` | 61H | N | no N2 fixation | 4 |
| `ocnBgchem.intppcalc.tavg-u-hxy-sea.mon.30S-90S` | 61H | N | enable_coccos=.false.; NPPc stream is commented out | 4 |
| `ocnBgchem.intppcalc.tavg-u-hxy-sea.mon.glb` | 10M 16M 18M 44M | N | enable_coccos=.false.; NPPc stream is commented out | 11 |
| `ocnBgchem.intppdiaz.tavg-u-hxy-sea.mon.30S-90S` | 61H | N | no diazotrophs | 4 |
| `ocnBgchem.intppdiaz.tavg-u-hxy-sea.mon.glb` | 10M 16M 18M 44M | N | no diazotrophs | 11 |
| `ocnBgchem.intppmisc.tavg-u-hxy-sea.mon.30S-90S` | 61H | N | small phyto already reported as nano | 4 |
| `ocnBgchem.intppmisc.tavg-u-hxy-sea.mon.glb` | 10M 16M 18M 44M | N | small phyto already reported as nano | 11 |
| `ocnBgchem.intppnitrate.tavg-u-hxy-sea.mon.30S-90S` | 61H | N | DIN not split into NO3/NH4 | 4 |
| `ocnBgchem.intppnitrate.tavg-u-hxy-sea.mon.glb` | 10M 18M | N | DIN not split into NO3/NH4 | 11 |
| `ocnBgchem.intpppico.tavg-u-hxy-sea.mon.30S-90S` | 61H | N | no picophytoplankton | 4 |
| `ocnBgchem.intpppico.tavg-u-hxy-sea.mon.glb` | 10M 16M 18M 44M | N | no picophytoplankton | 11 |
| `ocnBgchem.ocfriver.tavg-u-hxy-sea.mon.glb` | 81H | N | userivers=.false.; no riverine DOC stream | 11 |
| `ocnBgchem.sigpp.tavg-u-hxy-si.mon.glb` | 44M | N | no sea-ice algae | 11 |

### Atmosphere/land CO2

| Variable | Requested by | Status | Source / reason | MB/yr |
|---|---|---|---|---|
| `atmos.fco2all.tavg-u-hxy-u.mon.glb` | 10H 18H | W | co2apf + co2nbf + co2fire + co2of (signs to check) | 2 |
| `atmos.fco2antt.tavg-u-hxy-u.mon.glb` | 10H 18H 55H | W | co2apf (+ LPJG fLuc); LPJG also writes fco2antt_monthly.out | 2 |
| `atmos.fco2fos.tavg-u-hxy-u.mon.glb` | 10H 18H | W | co2apf, if it is fossil only | 2 |
| `atmos.fco2nat.tavg-u-hxy-u.mon.glb` | 10H 18H 31H | W | co2nbf + co2fire + co2of; LPJG writes fco2nat_monthly.out (land part) | 2 |
| `land.netAtmosLandCO2Flux.tavg-u-hxy-lnd.mon.glb` | 10H 18H | W | LPJG netAtmosLandCO2Flux_monthly.out | 2 |
| `atmos.fco2antt.tpt-u-hs-u.subhr.glb` | 79H | N | site (hs) sub-hourly output not set up in XIOS | ~0 |
| `atmos.fco2fos.tpt-u-hs-u.subhr.glb` | 79H | N | site (hs) sub-hourly output not set up in XIOS | ~0 |
| `atmos.fco2nat.tpt-u-hs-u.subhr.glb` | 79H | N | site (hs) sub-hourly output not set up in XIOS | ~0 |

### Icebergs and ice shelves

| Variable | Requested by | Status | Source / reason | MB/yr |
|---|---|---|---|---|
| `landIce.libmassbf.tavg-u-hxy-fis.mon.ata` | 39M | W | 1000 x fw at cavity nodes | 2 |
| `landIce.libmassbf.tavg-u-hxy-fis.yr.ata` | 39M | W | 1000 x fw at cavity nodes | <1 |
| `ocean.ficeberg.tavg-ol-hxy-sea.mon.glb` | 47M 81H | W | 1000 x (ibfwb+ibfwbv+ibfwl+ibfwe) | 497 |
| `ocean.ficeberg.tavg-u-hxy-sea.mon.glb` | 39H 47M 51H | W | 1000 x (ibfwb+ibfwbv+ibfwl+ibfwe) | 11 |
| `ocean.flandice.tavg-u-hxy-sea.mon.glb` | 39H 51H | W | partial: -1000 x fw under cavities (shelf basal melt only) | 11 |
| `ocean.hfibthermds.tavg-ol-hxy-sea.mon.glb` | 47M | W | ibhf (3D) / sum over depth | 497 |
| `ocean.hfibthermds.tavg-u-hxy-sea.mon.glb` | 47M | W | ibhf (3D) / sum over depth | 11 |
| `ocean.flandice.tavg-u-hxy-sea.3hr.glb` | 16H | C | fw at 3h in namelist.io; partial as monthly | 2,576 |
| `ocean.ficeberg.tavg-ol-hxy-sea.3hr.glb` | 16H | S | icb stream hardcodes monthly | 121,051 |
| `landIce.libmassbf.tavg-u-hxy-fis.mon.grl` | 39M | N | grounded ice / no Greenland cavities | ~0 |
| `landIce.libmassbf.tavg-u-hxy-fis.yr.grl` | 39M | N | grounded ice / no Greenland cavities | ~0 |
| `landIce.libmassbf.tavg-u-hxy-gis.mon.ata` | 39M | N | grounded ice / no Greenland cavities | 2 |
| `landIce.libmassbf.tavg-u-hxy-gis.mon.grl` | 39M | N | grounded ice / no Greenland cavities | ~0 |
| `landIce.libmassbf.tavg-u-hxy-gis.yr.ata` | 39M | N | grounded ice / no Greenland cavities | <1 |
| `landIce.libmassbf.tavg-u-hxy-gis.yr.grl` | 39M | N | grounded ice / no Greenland cavities | ~0 |
| `landIce.licalvf.tavg-u-hxy-is.mon.ata` | 39M | N | no ice-sheet model; calving prescribed via icb_*.dat | 2 |
| `landIce.licalvf.tavg-u-hxy-is.mon.grl` | 39M | N | no ice-sheet model; calving prescribed via icb_*.dat | 2 |
| `landIce.licalvf.tavg-u-hxy-is.yr.ata` | 39M | N | no ice-sheet model; calving prescribed via icb_*.dat | <1 |
| `landIce.licalvf.tavg-u-hxy-is.yr.grl` | 39M | N | no ice-sheet model; calving prescribed via icb_*.dat | <1 |
| `landIce.lifmassbf.tavg-u-hxy-is.mon.ata` | 39M | N | no ice-sheet model | 2 |
| `landIce.lifmassbf.tavg-u-hxy-is.mon.grl` | 39M | N | no ice-sheet model | 2 |
| `landIce.lifmassbf.tavg-u-hxy-is.yr.ata` | 39M | N | no ice-sheet model | <1 |
| `landIce.lifmassbf.tavg-u-hxy-is.yr.grl` | 39M | N | no ice-sheet model | <1 |
| `landIce.tendlicalvf.tavg-u-hm-is.yr.ata` | 39M | N | no ice-sheet model; total could be summed from icb_*.dat | ~0 |
| `landIce.tendlicalvf.tavg-u-hm-is.yr.grl` | 39M | N | no ice-sheet model; total could be summed from icb_*.dat | ~0 |

W 26 vars 1.64 GB/yr
C 2 vars 2.90 GB/yr
S 22 vars 123.24 GB/yr
N 41 vars 0.20 GB/yr
total 91
