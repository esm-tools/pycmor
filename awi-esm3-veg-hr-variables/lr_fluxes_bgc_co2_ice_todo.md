# AWI-ESM3 LR: ocean BGC, CO2 and iceberg/cavity fluxes for CMIP7

What has to be done to produce every flux from the CMIP7 Data Request that AWI-ESM3 can realistically deliver.

Scope: all flux variables (plus the `pp` and `graz` rates) in the ocnBgchem, CO2 and iceberg/ice-shelf sets of DReq v1.2.2.5, status W, X or S below.
Excluded by decision: coccolithophores (`enable_coccos` stays off), carbon isotopes (OIFS has no 13C/14C), 3-hourly 3D `ficeberg`.

Reference run: `/work/bb1469/a270270/runtime/awiesm3-v3.5.1-cc/05_Coupled_CC_v3.5.1_30yr_2190_2219_fromAlbedo/run_21900101-21941231/work`
FESOM source it was built from: `/work/ab0995/a270270/model_codes/awiesm3-v3.5.1-cc/fesom-2.8` (`dbf28153`)
CMIP7 FESOM XIOS XML: `~/esm_tools/namelists/fesom2/xios_xml_cmip7/`

Status codes:
- **W** the run already writes the inputs, only a pycmor rule is needed
- **X** the stream exists in FESOM, but needs entries in namelist.io and the XIOS XML (`field_def_fesom.xml`, `file_def_fesom.xml.j2`), no rebuild
- **S** FESOM/REcoM source change
- **~** derived offline, approximate

FESOM writes through XIOS in this setup. A field only comes out if it is in namelist.io (so FESOM registers and sends it), in `field_def` and in a `file_def` group. The output frequency comes from the `file_def` group.

Volumes are uncompressed float32 per model year on core3 (220,509 nodes, 47 layers) and TCo95 (40,320 points). Real 3D files compress to about half.

## REcoM setup this is based on

- `enable_3zoo2det = .true.`, `enable_coccos = .false.` (2 phytoplankton: small phytoplankton, diatoms)
- `ciso = .false.`, `userivers = .false.`, `useaeoliann = .false.` (so `aN` is 0), Albani dust iron on
- `use_ballasting = .true.`, `allow_var_sinking = .true.` (sinking speed varies in time)
- CO2 coupled with OIFS through OASIS (`XCO2_oce` in, `FCO2` out)
- `Fe2N = 0.033` (Fe2C 0.005 × 6.625). DFe is carried in µmol/m3 and N in mmol/m3, so this is 3.3e-5 mol Fe per mol N. Verify before using it.

## 1. pycmor rules only (W)

Inputs are all in the reference run. None of these has a rule yet, apart from `fco2antt` and `fco2nat`, which have to be replaced (see 1.2).

### 1.1 Ocean biogeochemistry

| Variable | Opportunities | Input and formula | MB/yr |
|---|---|---|---|
| `ocnBgchem.fgco2.tavg-u-hxy-sea.mon.glb` | 10H 16H 18H 26H 31H 44H 54H 55H | `CO2f` (mmolC m-2 d-1, positive into ocean) × 12.011e-6 / 86400 → kg m-2 s-1 | 11 |
| `ocnBgchem.fsfe.tavg-u-hxy-sea.mon.glb` | 81H | `aFe` (µmol m-2 s-1) × 1e-6 | 11 |
| `ocnBgchem.fsn.tavg-u-hxy-sea.mon.glb` | 81H | `aN` (mmol m-2 s-1) × 1e-3. All zeros while `useaeoliann = .false.` | 11 |
| `ocnBgchem.intpp.tavg-u-hxy-sea.mon.glb` | 10H 16H 18H 44H 54H | `NPPn + NPPd` (mmolC m-2 d-1) × 1e-3 / 86400 | 11 |
| `ocnBgchem.intpp.tavg-u-hxy-sea.mon.30S-90S` | 61H | as above, nodes south of 30S | 4 |
| `ocnBgchem.intppdiat.tavg-u-hxy-sea.mon.glb` | 10M 16M 18M 44M | `NPPd` | 11 |
| `ocnBgchem.intppdiat.tavg-u-hxy-sea.mon.30S-90S` | 61H | `NPPd`, south of 30S | 4 |
| `ocnBgchem.intppnano.tavg-u-hxy-sea.mon.glb` | 16M 44M | `NPPn` | 11 |
| `ocnBgchem.intpbn.tavg-u-hxy-sea.mon.30S-90S` | 61H | `NNAn + NNAd` (mmolN m-2 d-1) × 1e-3 / 86400 | 4 |
| `ocnBgchem.intpbfe.tavg-u-hxy-sea.mon.30S-90S` | 61H | `(NNAn + NNAd)` × Fe2N, same conversion | 4 |
| `ocnBgchem.intpcalcite.tavg-u-hxy-sea.mon.30S-90S` | 61H | `calcif` summed over depth. The 3D stream is labelled mmolC/m2/d, so check whether it needs × layer thickness first | 4 |
| `ocnBgchem.pp.tavg-ol-hxy-sea.mon.glb` | 10M 16M 18M 44M | `NPPn3D + NPPd3D` (mmolC m-3 d-1) × 1e-3 / 86400 | 497 |
| `ocnBgchem.pp.tavg-ols-hxy-sea.mon.glb` | 16H 44H | as above, top layer | 11 |
| `ocnBgchem.pp.tavg-ols-hxy-sea.mon.30S-90S` | 61H | top layer, south of 30S | 4 |

### 1.2 Atmosphere and land CO2

OIFS writes `co2apf`, `co2nbf`, `co2fire`, `co2of` in kg m-2 s-1, presumably of CO2. CMIP wants kg C, so convert with × 12.011 / 44.009. CMIP sign convention is positive into the atmosphere.

| Variable | Opportunities | Input and formula | MB/yr |
|---|---|---|---|
| `atmos.fco2fos.tavg-u-hxy-u.mon.glb` | 10H 18H | `co2apf`, if it is fossil only | 2 |
| `atmos.fco2antt.tavg-u-hxy-u.mon.glb` | 10H 18H 55H | `co2apf` + land-use flux from LPJ-GUESS | 2 |
| `atmos.fco2nat.tavg-u-hxy-u.mon.glb` | 10H 18H 31H | `co2nbf + co2fire + co2of` | 2 |
| `atmos.fco2all.tavg-u-hxy-u.mon.glb` | 10H 18H | sum of all four | 2 |
| `land.netAtmosLandCO2Flux.tavg-u-hxy-lnd.mon.glb` | 10H 18H | LPJ-GUESS `netAtmosLandCO2Flux_monthly.out`, `lpjg_monthly_pipeline` | 2 |

To do before writing these rules:
- [ ] The existing `fco2antt_mon` and `fco2nat_mon` rules in `cap7_land/cmip7_awiesm3-veg-hr_cap7_land.yaml` read the LPJ-GUESS land-only files. They miss fossil emissions and the ocean flux, so replace them with the OIFS-based versions.
- [ ] Find out what `co2apf` contains (fossil only, or fossil plus land use) and whether it is in kg CO2 or kg C.
- [ ] Check the signs of `co2nbf` and `co2fire`. Year 2190 gives global totals of +193 and -187 Pg/yr, which looks like double counting or an inverted sign.
- [ ] Check that the sign of `co2of` is opposite to `fgco2`.
- [ ] Only `atm_remapped_1m_co2*` (regular 192×400) files were found. Use whatever grid the other atmosphere rules use.

### 1.3 Icebergs and ice-shelf cavities

Cavity nodes are where `ulevels > 1` (`cavity_nlvls.out` in the core3 mesh dir), equivalently where `sst` is NaN. That is 10,047 nodes, all south of 60S. Under cavities FESOM's `fw` is the 3-equation basal melt rate (positive upward, negative means melting).

| Variable | Opportunities | Input and formula | MB/yr |
|---|---|---|---|
| `ocean.ficeberg.tavg-u-hxy-sea.mon.glb` | 39H 47M 51H | 1000 × `(ibfwb + ibfwbv + ibfwl + ibfwe)`, positive into ocean. The inputs are NaN under cavities, so set those to 0 | 11 |
| `ocean.ficeberg.tavg-ol-hxy-sea.mon.glb` | 47M 81H | the 2D field in the top layer, zeros below (FESOM adds iceberg melt at the surface only) | 497 raw, compresses to about 2D size |
| `ocean.hfibthermds.tavg-ol-hxy-sea.mon.glb` | 47M | `ibhf` (W m-2 per level) | 497 |
| `ocean.hfibthermds.tavg-u-hxy-sea.mon.glb` | 47M | `ibhf` summed over depth | 11 |
| `ocean.flandice.tavg-u-hxy-sea.mon.glb` | 39H 51H | -1000 × `fw` at cavity nodes, 0 elsewhere. Ice-shelf basal melt only; say so in the `comment` attribute | 11 |
| `landIce.libmassbf.tavg-u-hxy-fis.mon.ata` | 39M | 1000 × `fw` at cavity nodes (negative means mass loss) | 2 |
| `landIce.libmassbf.tavg-u-hxy-fis.yr.ata` | 39M | yearly mean of the above | <1 |

## 2. XIOS XML and namelist.io (X)

No rebuild. Each field needs an entry in namelist.io and in the XIOS XML.

| Change | Gives | MB/yr (CMOR) |
|---|---|---|
| Add `NPPn` and `NPPd` to the `fesom_1d` group | `ocnBgchem.intpp.tavg-u-hxy-sea.day.glb` (16H 44H 54M) | 322 |
| New 3-hourly group with `fw` | `ocean.flandice.tavg-u-hxy-sea.3hr.glb` (16H) | 2,576 |
| Move `TSi_assimDia` out of the `recom_enable_coccos` block in `file_def`, add it to namelist.io | `ocnBgchem.intpbsi.tavg-u-hxy-sea.mon.30S-90S` (61H), ~ as Σ(`TSi_assimDia` × `DiaC` × dz). Exact version is in section 3. | 4 |
| Add `wsink_det1`, `wsink_det2` (3D, interface levels) to `field_def`, `file_def` and namelist.io | export fluxes ~ as `wsink_det1 × Det* + wsink_det2 × idetz2*`, see the list below. Exact version is in section 3. | about 500 per stream of raw model output |

Export fluxes covered by the `wsink` route (approximate, because the monthly mean of w × C is not mean(w) × mean(C) with ballasting):
- `expc` 100 m, 1000 m, 3D (`DetC`, `idetz2c`): 10H 16H 18H 44H
- `expcalc` 100 m, 1000 m, 3D (`DetCalc`, `idetz2calc`): 10H 16H 18H 44H
- `epn` 100 m (`DetN`, `idetz2n`): 10M 18M
- `epsi` 100 m (`DetSi`, `idetz2si`): 54M

CMOR volume for these is 497 MB for each 3D field and 11 MB for each depth-level field.

## 3. FESOM/REcoM source changes (S)

### 3.1 Bottom fluxes from `SinkFlx` (trivial)

`SinkFlx(node, 1:4)` holds the flux into the benthos for N, C, Si and calcite, already in mmol m-2 s-1. It is reset every tracer step (`oce_ale_tracer.F90:300`), so a normal `def_stream` average works.

- [ ] Add four 2D streams in the `__recom` block of `io_meandata.F90` (for example `bflxN`, `bflxC`, `bflxSi`, `bflxCalc`), plus `field_def`, `file_def` and namelist.io entries. `expcob` day also needs `bflxC` in `fesom_1d`.

| Variable | Opportunities | From | MB/yr |
|---|---|---|---|
| `ocnBgchem.froc.tavg-u-hxy-sea.mon.glb` | 10M 16H 18M 44H | `bflxC` | 11 |
| `ocnBgchem.fric.tavg-u-hxy-sea.mon.glb` | 10M 16H 18M 44H | `bflxCalc` | 11 |
| `ocnBgchem.expcob.tavg-u-hxy-sea.mon.glb` | 44M | `bflxC` | 11 |
| `ocnBgchem.expcob.tavg-u-hxy-sea.day.glb` | 16L | `bflxC` daily | 322 |
| `ocnBgchem.expnob.tavg-u-hxy-sea.mon.glb` | 44M | `bflxN` | 11 |
| `ocnBgchem.expsiob.tavg-u-hxy-sea.mon.glb` | 44M | `bflxSi` | 11 |
| `ocnBgchem.expcalcob.tavg-u-hxy-sea.mon.glb` | 44M | `bflxCalc` | 11 |
| `ocnBgchem.expfeob.tavg-u-hxy-sea.mon.glb` | 44M | `bflxN` × Fe2N | 11 |
| `ocnBgchem.frn.tavg-u-hxy-sea.mon.glb` | 44M | `bflxN`. Denitrification is not modelled; say so in `comment` | 11 |

### 3.2 Exact export fluxes (replaces the `wsink` route in section 2)

- [ ] Save the per-level sinking flux (w × C summed over both detritus classes) as 3D arrays for N, C, Si and calcite, computed where `recom_sinking.F90` already forms it. The 100 m and 1000 m fields then come from level selection or interpolation in pycmor.

This produces the same variables and CMOR volume as the `wsink` list in section 2.

### 3.3 Grazing

`vertgrazmicro_tot`, `vertgrazmeso_tot` and `vertgrazmacro_tot` are column-local work arrays (`nl-1`), in mmolC m-3 d-1. `recom_forcing.F90:448` only keeps their column integrals.

- [ ] Add a global 3D array (like `NPPn3D`) filled from the sum of the three, plus a stream.

| Variable | Opportunities | MB/yr |
|---|---|---|
| `ocnBgchem.graz.tavg-ol-hxy-sea.mon.glb` | 16M 44M | 497 |
| `ocnBgchem.graz.tavg-ol-hxy-sea.mon.30S-90S` | 61H | 210 |

### 3.4 Iron loss

- [ ] Save the Fe scavenging loss (`kScavFe` term in `recom_sms.F90`), integrated over depth. `frfe` is that plus `bflxN` × Fe2N.

| Variable | Opportunities | MB/yr |
|---|---|---|
| `ocnBgchem.frfe.tavg-u-hxy-sea.mon.glb` | 44M | 11 |

### 3.5 Exact Si production (optional, replaces the `TSi_assimDia` route in section 2)

- [ ] Integrate `Si_assim × DiaC` over depth inside REcoM and write it as a 2D stream. This gives `intpbsi` 30S-90S (61H) exactly.

## 4. Volume

| Part | Variables | GB/yr (CMOR, raw float32) |
|---|---|---|
| W, pycmor only | 26 | 1.6 |
| X, XML and namelist | 11 (4 changes) | 4.0, of which 2.6 is 3-hourly `flandice` |
| S, source changes | 12 | 1.1 |
| **Total** | **49** | **about 6.7** |

On top of that, the extra raw model output is about 1 GB/yr for `wsink_det1/2` (only if the X route is used), about 0.5 GB/yr for the 3D grazing field, and 2D streams of about 11 MB/yr each.

## 5. Not produced

- Decided: `intppcalc`, `intppmisc` (no coccolithophores/phaeocystis), `fg13co2`, `fg14co2abio` (no isotopes in OIFS), `ficeberg` 3hr 3D.
- Not in REcoM: `intpppico`, `intppdiaz`, `intpn2`, `intppnitrate`, `epp`, `exppob`, `intpbp`, `exparagob`, `intparag`, `fgdms`, `sigpp`.
- No ice-sheet model: `licalvf`, `lifmassbf`, `tendlicalvf`, `libmassbf` gis and grl variants, `sftgrf`.
- No sub-hourly site output: `fco2antt`, `fco2fos`, `fco2nat` subhr (79H).

Open, not decided (each one changes the simulation):
- `useaeoliann = .true.` would make `fsn` nonzero.
- `userivers = .true.` plus a stream for the riverine input would give `icfriver` and `ocfriver` (81H, 10M, 18M).
