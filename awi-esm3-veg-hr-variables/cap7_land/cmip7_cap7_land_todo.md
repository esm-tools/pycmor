# CAP7 Land — Implementation Status

Source: `cmip7_CAP7_variables_land.csv` (98 variable-frequency entries, unfiltered)

## Summary

| Status | Count |
|--------|-------|
| Already in core/lrcs/veg/extra | 12 |
| Implemented (new cap7 rules) | 54 |
| Needs new custom step (per-soil-layer) | 2 |
| Blocked — per-PFT group not in LPJ-GUESS output | 15 |
| Blocked — no LPJ-GUESS output file | 13 |
| Blocked — no depth-resolved cSoil output | 2 |
| **Total** | **98** |

---

## Already implemented in core/lrcs/veg/extra (12)

These variables already have matching compound names in other tiers.
No new rules needed.

- [x] **evspsblsoi** (mon) — `land.evspsblsoi.tavg-u-hxy-lnd.mon.glb` — lrcs_land
- [x] **evspsblveg** (mon) — `land.evspsblveg.tavg-u-hxy-lnd.mon.glb` — lrcs_land
- [x] **lai** (mon) — `land.lai.tavg-u-hxy-lnd.mon.glb` — core_land
- [x] **mrro** (mon) — `land.mrro.tavg-u-hxy-lnd.mon.glb` — core_land
- [x] **mrros** (mon) — `land.mrros.tavg-u-hxy-lnd.mon.glb` — core_land
- [x] **mrso** (mon) — `land.mrso.tavg-u-hxy-lnd.mon.glb` — core_land
- [x] **mrsofc** (fx) — `land.mrsofc.ti-u-hxy-lnd.fx.glb` — lrcs_land
- [x] **mrsol** (mon, d10cm) — `land.mrsol.tavg-d10cm-hxy-lnd.mon.glb` — core_land
- [x] **orog** (fx) — `land.orog.ti-u-hxy-u.fx.glb` — core_land
- [x] **rootd** (fx) — `land.rootd.ti-u-hxy-lnd.fx.glb` — lrcs_land
- [x] **sftgif** (fx) — `land.sftgif.ti-u-hxy-u.fx.glb` — lrcs_land
- [x] **slthick** (fx) — `land.slthick.ti-sl-hxy-lnd.fx.glb` — core_land

---

## Implemented — new cap7 rules (54)

### LPJ-GUESS monthly variables (49)

All use `load_lpjguess_monthly` custom loader from `custom_steps.py`.
Source: `outdata/lpj_guess/YYYYMMDD-YYYYMMDD/run1/<var>_monthly.out`
Format: Lon/Lat/Year/Jan..Dec (model_variable = "Total")

#### Carbon pools (10)

- [x] **cLand** (mon) — `land.cLand.tavg-u-hxy-lnd.mon.glb` — Total carbon in all terrestrial pools. From `cLand_monthly.out`.
- [x] **cLeaf** (mon) — `land.cLeaf.tavg-u-hxy-lnd.mon.glb` — Carbon in leaves. From `cLeaf_monthly.out`.
- [x] **cLitter** (mon) — `land.cLitter.tavg-u-hxy-lnd.mon.glb` — Carbon in litter. From `cLitter_monthly.out`.
- [x] **cLitterCwd** (mon) — `land.cLitterCwd.tavg-u-hxy-lnd.mon.glb` — Carbon in coarse woody debris. From `cLitterCwd_monthly.out`.
- [x] **cLitterSubSurf** (mon) — `land.cLitterSubSurf.tavg-u-hxy-lnd.mon.glb` — Carbon in below-ground litter. From `cLitterSubSurf_monthly.out`.
- [x] **cLitterSurf** (mon) — `land.cLitterSurf.tavg-u-hxy-lnd.mon.glb` — Carbon in surface litter. From `cLitterSurf_monthly.out`.
- [x] **cOther** (mon) — `land.cOther.tavg-u-hxy-lnd.mon.glb` — Carbon in other pools. From `cOther_monthly.out`.
- [x] **cProduct** (mon) — `land.cProduct.tavg-u-hxy-lnd.mon.glb` — Carbon in products. From `cProduct_monthly.out`.
- [x] **cRoot** (mon) — `land.cRoot.tavg-u-hxy-lnd.mon.glb` — Carbon in roots. From `cRoot_monthly.out`.
- [x] **cStem** (mon) — `land.cStem.tavg-u-hxy-lnd.mon.glb` — Carbon in stems. From `cStem_monthly.out`.

#### Carbon pool totals (2)

- [x] **cSoil** (mon, total) — `land.cSoil.tavg-u-hxy-lnd.mon.glb` — Total soil carbon. From `cSoil_monthly.out`.
- [x] **cVeg** (mon, total) — `land.cVeg.tavg-u-hxy-lnd.mon.glb` — Total vegetation carbon. From `cVeg_monthly.out`.

#### Carbon fluxes — fire & disturbance (9)

- [x] **fAnthDisturb** (mon) — `land.fAnthDisturb.tavg-u-hxy-lnd.mon.glb` — Carbon flux from anthropogenic disturbance. From `fAnthDisturb_monthly.out`.
- [x] **fDeforestToAtmos** (mon) — `land.fDeforestToAtmos.tavg-u-hxy-lnd.mon.glb` — Deforestation carbon to atmosphere. From `fDeforestToAtmos_monthly.out`.
- [x] **fDeforestToProduct** (mon) — `land.fDeforestToProduct.tavg-u-hxy-lnd.mon.glb` — Deforestation carbon to product pool. From `fDeforestToProduct_monthly.out`.
- [x] **fFire** (mon) — `land.fFire.tavg-u-hxy-lnd.mon.glb` — Carbon emission from fire. From `fFire_monthly.out`.
- [x] **fFireAll** (mon) — `land.fFireAll.tavg-u-hxy-lnd.mon.glb` — Carbon emission from all fires. From `fFireAll_monthly.out`.
- [x] **fFireNat** (mon) — `land.fFireNat.tavg-u-hxy-lnd.mon.glb` — Carbon emission from natural fires. From `fFireNat_monthly.out`.
- [x] **fHarvestToAtmos** (mon) — `land.fHarvestToAtmos.tavg-u-hxy-lnd.mon.glb` — Harvest carbon to atmosphere. From `fHarvestToAtmos_monthly.out`.
- [x] **fLitterFire** (mon) — `land.fLitterFire.tavg-u-hxy-lnd.mon.glb` — Litter carbon consumed by fire. From `fLitterFire_monthly.out`.
- [x] **fVegFire** (mon) — `land.fVegFire.tavg-u-hxy-lnd.mon.glb` — Vegetation carbon consumed by fire. From `fVegFire_monthly.out`.

#### Carbon fluxes — litter/soil/product (4)

- [x] **fCLandToOcean** (mon) — `land.fCLandToOcean.tavg-u-hxy-lnd.mon.glb` — Carbon flux from land to ocean. From `fCLandToOcean_monthly.out`.
- [x] **fLitterSoil** (mon) — `land.fLitterSoil.tavg-u-hxy-lnd.mon.glb` — Carbon flux litter to soil. From `fLitterSoil_monthly.out`.
- [x] **fProductDecomp** (mon) — `land.fProductDecomp.tavg-u-hxy-lnd.mon.glb` — Product pool decomposition flux. From `fProductDecomp_monthly.out`.
- [x] **fVegLitter** (mon) — `land.fVegLitter.tavg-u-hxy-lnd.mon.glb` — Vegetation to litter carbon flux. From `fVegLitter_monthly.out`.

#### Productivity & respiration — totals (6)

- [x] **gpp** (mon, total) — `land.gpp.tavg-u-hxy-lnd.mon.glb` — Gross primary production. From `gpp_monthly.out`.
- [x] **nbp** (mon) — `land.nbp.tavg-u-hxy-lnd.mon.glb` — Net biome production. From `nbp_monthly.out`.
- [x] **nep** (mon) — `land.nep.tavg-u-hxy-lnd.mon.glb` — Net ecosystem production. From `nep_monthly.out`.
- [x] **npp** (mon, total) — `land.npp.tavg-u-hxy-lnd.mon.glb` — Net primary production. From `npp_monthly.out`.
- [x] **ra** (mon, total) — `land.ra.tavg-u-hxy-lnd.mon.glb` — Autotrophic respiration. From `ra_monthly.out`.
- [x] **rh** (mon, total) — `land.rh.tavg-u-hxy-lnd.mon.glb` — Heterotrophic respiration. From `rh_monthly.out`.

#### Respiration — component (6)

- [x] **raLeaf** (mon) — `land.raLeaf.tavg-u-hxy-lnd.mon.glb` — Leaf autotrophic respiration. From `raLeaf_monthly.out`.
- [x] **raOther** (mon) — `land.raOther.tavg-u-hxy-lnd.mon.glb` — Other autotrophic respiration. From `raOther_monthly.out`.
- [x] **raRoot** (mon) — `land.raRoot.tavg-u-hxy-lnd.mon.glb` — Root autotrophic respiration. From `raRoot_monthly.out`.
- [x] **raStem** (mon) — `land.raStem.tavg-u-hxy-lnd.mon.glb` — Stem autotrophic respiration. From `raStem_monthly.out`.
- [x] **rhLitter** (mon) — `land.rhLitter.tavg-u-hxy-lnd.mon.glb` — Litter heterotrophic respiration. From `rhLitter_monthly.out`.
- [x] **rhSoil** (mon) — `land.rhSoil.tavg-u-hxy-lnd.mon.glb` — Soil heterotrophic respiration. From `rhSoil_monthly.out`.

#### Land cover fractions (8)

- [x] **baresoilFrac** (mon) — `land.baresoilFrac.tavg-u-hxy-u.mon.glb` — Bare soil fraction. From `baresoilFrac_monthly.out`.
- [x] **cropFrac** (mon) — `land.cropFrac.tavg-u-hxy-u.mon.glb` — Crop fraction. From `cropFrac_monthly.out`.
- [x] **grassFrac** (mon) — `land.grassFrac.tavg-u-hxy-u.mon.glb` — Grass fraction. From `grassFrac_monthly.out`.
- [x] **landCoverFrac** (mon) — `land.landCoverFrac.tavg-u-hxy-u.mon.glb` — Land cover fraction. From `landCoverFrac_monthly.out`.
- [x] **pastureFrac** (mon) — `land.pastureFrac.tavg-u-hxy-u.mon.glb` — Pasture fraction. From `pastureFrac_monthly.out`.
- [x] **residualFrac** (mon) — `land.residualFrac.tavg-u-hxy-u.mon.glb` — Residual fraction. From `residualFrac_monthly.out`.
- [x] **shrubFrac** (mon) — `land.shrubFrac.tavg-u-hxy-u.mon.glb` — Shrub fraction. From `shrubFrac_monthly.out`.
- [x] **treeFrac** (mon) — `land.treeFrac.tavg-u-hxy-u.mon.glb` — Tree fraction. From `treeFrac_monthly.out`.

#### Other LPJ-GUESS variables (4)

- [x] **burntFractionAll** (mon) — `land.burntFractionAll.tavg-u-hxy-u.mon.glb` — Burnt fraction all. From `burntFractionAll_monthly.out`.
- [x] **prveg** (mon) — `land.prveg.tavg-u-hxy-lnd.mon.glb` — Precipitation over vegetation. From `prveg_monthly.out`.
- [x] **tran** (mon) — `land.tran.tavg-u-hxy-lnd.mon.glb` — Transpiration. From `tran_monthly.out`.
- [x] **vegFrac** (mon) — `land.vegFrac.tavg-u-hxy-u.mon.glb` — Vegetated fraction. From `vegFrac_monthly.out`.

### IFS daily variables from 3hr/daily XIOS (4)

All from OpenIFS XIOS output. 3hr→daily via pycmor timeavg; daily direct from daily XIOS.

- [x] **mrro** (day) — `land.mrro.tavg-u-hxy-lnd.day.glb` — Total runoff. From `atmos_3h_land_mrro_*.nc`, timeavg 3hr→day.
- [x] **mrso** (day) — `land.mrso.tavg-u-hxy-lnd.day.glb` — Total soil moisture. Requires adding `mrso` field to daily XIOS output (`file_def_oifs_cmip7_spinup.xml.j2`). From `atmos_day_land_mrso_*.nc`.
- [x] **mrsol** (day, d10cm) — `land.mrsol.tavg-d10cm-hxy-lnd.day.glb` — Upper 10cm soil moisture. From `atmos_3h_land_mrsol_*.nc`, timeavg 3hr→day.
- [x] **tslsi** (day) — `land.tslsi.tavg-u-hxy-lsi.day.glb` — Surface temperature where land or sea ice. From `atmos_3h_land_tslsi_*.nc`, timeavg 3hr→day.

### IFS 1hr variable (1)

- [x] **tas** (1hr, global) — `land.tas.tavg-h2m-hxy-u.1hr.glb` — Near-surface air temperature. From `atmos_1h_tas_*.nc`. Same XIOS file as extra_land 30S-90S subset, but global (no lat subsetting). Uses custom step `compute_hurs` from `custom_steps.py` — **no**, just passthrough with rename 2t→tas.

---

## Needs new custom step — per-soil-layer LPJ-GUESS (2)

These require a new custom loader (`load_lpjguess_monthly_depth`) that handles the
depth-resolved format: Lon/Lat/Year/Mth/Depth0.1/Depth0.2/.../Depth1.5 (15 levels).
Implementable but deferred until custom step is written.

- [ ] **tsl** (mon, per-layer) — `land.tsl.tavg-sl-hxy-lnd.mon.glb` — Soil temperature per layer. From `tsl_monthly.out`. 15 depth levels (0.1–1.5 m).
- [ ] **mrsol** (mon, per-layer) — `land.mrsol.tavg-sl-hxy-lnd.mon.glb` — Soil moisture per layer. From `mrsol_monthly.out`. 15 depth levels (0.1–1.5 m).

---

## Blocked — per-PFT group not in LPJ-GUESS output (15)

CMIP7 requests tree/shrub/grass (natural grass) variants. LPJ-GUESS `lpjg_output.ins`
does not define per-PFT-group output files for these variables. Would require LPJ-GUESS
source code changes or output configuration additions.

- [ ] **gpp** (tree) — `land.gpp.tavg-u-hxy-tree.mon.glb` — No `gpp_tree_monthly.out`.
- [ ] **gpp** (shrub) — `land.gpp.tavg-u-hxy-shb.mon.glb` — No `gpp_shrub_monthly.out`.
- [ ] **gpp** (grass) — `land.gpp.tavg-u-hxy-ng.mon.glb` — No `gpp_grass_monthly.out`.
- [ ] **npp** (tree) — `land.npp.tavg-u-hxy-tree.mon.glb` — No `npp_tree_monthly.out`.
- [ ] **npp** (shrub) — `land.npp.tavg-u-hxy-shb.mon.glb` — No `npp_shrub_monthly.out`.
- [ ] **npp** (grass) — `land.npp.tavg-u-hxy-ng.mon.glb` — No `npp_grass_monthly.out`.
- [ ] **ra** (tree) — `land.ra.tavg-u-hxy-tree.mon.glb` — No `ra_tree_monthly.out`.
- [ ] **ra** (shrub) — `land.ra.tavg-u-hxy-shb.mon.glb` — No `ra_shrub_monthly.out`.
- [ ] **ra** (grass) — `land.ra.tavg-u-hxy-ng.mon.glb` — No `ra_grass_monthly.out`.
- [ ] **rh** (tree) — `land.rh.tavg-u-hxy-tree.mon.glb` — No `rh_tree_monthly.out`.
- [ ] **rh** (shrub) — `land.rh.tavg-u-hxy-shb.mon.glb` — No `rh_shrub_monthly.out`.
- [ ] **rh** (grass) — `land.rh.tavg-u-hxy-ng.mon.glb` — No `rh_grass_monthly.out`.
- [ ] **cVeg** (tree) — `land.cVeg.tavg-u-hxy-tree.mon.glb` — No `cVeg_tree_monthly.out`.
- [ ] **cVeg** (shrub) — `land.cVeg.tavg-u-hxy-shb.mon.glb` — No `cVeg_shrub_monthly.out`.
- [ ] **cVeg** (grass) — `land.cVeg.tavg-u-hxy-ng.mon.glb` — No `cVeg_grass_monthly.out`.

---

## Blocked — no LPJ-GUESS output file (13)

These variables are not produced by the current LPJ-GUESS output configuration.
Would require additions to `lpjg_output.ins` and model rerun.

- [ ] **nppLeaf** (mon) — `land.nppLeaf.tavg-u-hxy-lnd.mon.glb` — No `nppLeaf_monthly.out`.
- [ ] **nppRoot** (mon) — `land.nppRoot.tavg-u-hxy-lnd.mon.glb` — No `nppRoot_monthly.out`.
- [ ] **nppStem** (mon) — `land.nppStem.tavg-u-hxy-lnd.mon.glb` — No `nppStem_monthly.out`.
- [ ] **nppOther** (mon) — `land.nppOther.tavg-u-hxy-lnd.mon.glb` — No `nppOther_monthly.out`.
- [ ] **fVegLitterMortality** (mon) — `land.fVegLitterMortality.tavg-u-hxy-lnd.mon.glb` — No output file.
- [ ] **fVegLitterSenescence** (mon) — `land.fVegLitterSenescence.tavg-u-hxy-lnd.mon.glb` — No output file.
- [ ] **fVegSoilMortality** (mon) — `land.fVegSoilMortality.tavg-u-hxy-lnd.mon.glb` — No output file.
- [ ] **fVegSoilSenescence** (mon) — `land.fVegSoilSenescence.tavg-u-hxy-lnd.mon.glb` — No output file.
- [ ] **fVegSoil** (mon) — `land.fVegSoil.tavg-u-hxy-lnd.mon.glb` — No `fVegSoil_monthly.out`.
- [ ] **cSoilPools** (mon) — `land.cSoilPools.tavg-u-hxy-lnd.mon.glb` — No `cSoilPools_monthly.out`. Would need per-pool disaggregation.
- [ ] **cGeologicStorage** (mon) — `land.cGeologicStorage.tavg-u-hxy-u.mon.glb` — No geologic storage model.
- [ ] **fHarvestToGeologicStorage** (mon) — `land.fHarvestToGeologicStorage.tavg-u-hxy-lnd.mon.glb` — No geologic storage model.
- [ ] **fHarvestToProduct** (mon) — `land.fHarvestToProduct.tavg-u-hxy-lnd.mon.glb` — No `fHarvestToProduct_monthly.out`.

---

## Blocked — no depth-resolved cSoil output (2)

LPJ-GUESS `cSoil_monthly.out` only provides total column soil carbon.
No per-layer or depth-integrated variants available.

- [ ] **cSoil** (mon, per-layer) — `land.cSoil.tavg-sl-hxy-lnd.mon.glb` — No per-layer soil carbon output.
- [ ] **cSoil** (mon, d100cm) — `land.cSoil.tavg-d100cm-hxy-lnd.mon.glb` — No top-1m soil carbon output.

---

## XIOS XML changes required

To enable daily mrso output, add `soil_moisture_content__mrso` to the `_day_land` file
in `file_def_oifs_cmip7_spinup.xml.j2`. The field expression already exists in
`field_def_cmip7.xml`.
