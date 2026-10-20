# LR reactivation plan: HR-deactivated rules against the v3.5.1 output config

Stand 2026-10-19. Target: AWI-ESM3 LR, esm_tools `feat/awiesm3-v3.4-co2` at a35303403
(awiesm3 v3.5.1 / v3.5.1-cc), with `oifs.cmip7_cmor_output: True` and
`fesom.cmip7_cmor_output: true`. The esm_tools edits listed here sit uncommitted in the
working tree of `~/esm_tools`.

## Summary

- 44 rules are commented out in `awi-esm3-veg-hr-variables/`. 11 of them duplicate
  an active rule and stay off. 6 (the FESOM 3D tendencies) are still impossible. The
  other 27 can come back for LR.
- Of those 27, 12 already had their stream in v3.5.1. The other 15 needed an esm_tools
  change, which is made: 6-hourly model levels on, plev6 on, 1hrPt `sp`, 3hrPt `ts`,
  and daily `utemp`/`vtemp`.
- **plev3, plev6 and plev7h were never on the right levels, and that includes the
  earlier plev7h "fix".** OpenIFS runs FullPos on one pressure axis only
  (`pressure_axis_id` = `pressure_levels`, `suxios.F90:616-637`). An axis that carries
  nothing but its own `n_glo`/`value` gets no XIOS transformation, so XIOS writes the
  first n FullPos levels under the new labels. I checked this on PICAL_snowalb 1945
  (v3.4, TCO95, axis_def as in v3.5.1). Per-level global means of `atmos_6h_pl7h_ta`
  match `atmos_mon_pl_ta` at 1000, 925, 850, 700, 600, 500 and 400 hPa to 0.01 K, but
  the file labels them 925, 850, 700, 600, 500, 250 and 50 hPa. In the same way,
  `atmos_6h_pl3_t`, labelled 850/500/250, holds 1000/925/850.
  - **This also hits active HR rules.** `core_atm` `ta_6hr`, `ua_6hr` and `va_6hr`
    (`tpt-p3-hxy-air.6hr`, read from `atmos_6h_pl3_*`) publish 1000/925/850 hPa data
    labelled 850/500/250. The HR plev7h data was the first 7 levels as well, not only
    mislabelled at two of them.
  - Fixed for LR. The plev axes are now zooms of the FullPos axis, and the FullPos axis
    has the four plev6 levels that plev19 lacks. Details are in the esm_tools section.
- **FESOM 3D tendencies stay impossible without a FESOM source change.** FESOM 2.8.3
  (v3.5.1) still declares all six as 1-D: `gen_modules_cmor_diag.F90:36,57-61`. No FESOM
  tree under `model_codes/` or any FESOM branch has the 3D version. The handoff note
  says the 3D streams for LR were added to `file_def_fesom.xml.j2`. That is not the
  case, and it could not work anyway: the regridded entries are still on
  `grid_2d_nod_reg`, which is correct for a 2D source.

## Status per deactivated HR rule

Status codes:
- **ready**: the stream is already in v3.5.1 (the rule may still need a new pattern).
- **added**: esm_tools edited in this pass.
- **dup**: produced by another active rule, stays off.
- **impossible**: cannot be produced by output configuration alone.

| tier | rule | compound_name | why off in HR | v3.5.1 stream / LR input pattern | status |
|---|---|---|---|---|---|
| cap7_atm | ta_6hr_ml | atmos.ta.tpt-al-hxy-u.6hr.glb | `atmos_6h_ml` disabled (volume) | `atmos_6h_ml_ta_.*\.nc` | added (stream enabled) |
| cap7_atm | ua_6hr_ml | atmos.ua.tpt-al-hxy-u.6hr.glb | same | `atmos_6h_ml_ua_.*\.nc` | added |
| cap7_atm | va_6hr_ml | atmos.va.tpt-al-hxy-u.6hr.glb | same | `atmos_6h_ml_va_.*\.nc` | added |
| cap7_atm | hus_6hr_ml | atmos.hus.tpt-al-hxy-u.6hr.glb | same | `atmos_6h_ml_hus_.*\.nc` | added |
| cap7_atm | zg_6hr_ml | atmos.zg.tpt-al-hxy-u.6hr.glb | same | `atmos_6h_ml_zg_.*\.nc` (zg_ml = z/g on model levels) | added. FullPos `z` on model levels has never run here, so check the first leg |
| cap7_atm | ta_6hr_pl7h | atmos.ta.tpt-p7h-hxy-air.6hr.glb | wrong levels | `atmos_6h_pl7h_ta_.*\.nc` | added (axis is now a zoom; the earlier relabel alone left the data wrong) |
| cap7_atm | ua_6hr_pl7h | atmos.ua.tpt-p7h-hxy-air.6hr.glb | wrong levels | `atmos_6h_pl7h_ua_.*\.nc` | added |
| cap7_atm | va_6hr_pl7h | atmos.va.tpt-p7h-hxy-air.6hr.glb | wrong levels | `atmos_6h_pl7h_va_.*\.nc` | added |
| cap7_atm | hus_6hr_pl7h | atmos.hus.tpt-p7h-hxy-air.6hr.glb | wrong levels | `atmos_6h_pl7h_hus_.*\.nc` | added |
| cap7_atm | zg_6hr_pl7h | atmos.zg.tpt-p7h-hxy-air.6hr.glb | wrong levels | `atmos_6h_pl7h_zg_.*\.nc` | added |
| veg_atm | ta_3hrPt_plev6 | atmos.ta.tpt-p6-hxy-air.3hr.glb | `atmos_3h_pl6` disabled (volume) | `atmos_3h_pl6_ta_.*\.nc` (HR rule had `atm_remapped_3h_pl6_`, which no stream writes) | added (enabled, plev6 levels on the FullPos axis). Native reduced grid, so use the native grid label |
| veg_atm | ua_3hrPt_plev6 | atmos.ua.tpt-p6-hxy-air.3hr.glb | same | `atmos_3h_pl6_ua_.*\.nc` | added |
| veg_atm | va_3hrPt_plev6 | atmos.va.tpt-p6-hxy-air.3hr.glb | same | `atmos_3h_pl6_va_.*\.nc` | added |
| veg_atm | wap_3hrPt_plev6 | atmos.wap.tpt-p6-hxy-air.3hr.glb | same | `atmos_3h_pl6_wap_.*\.nc` | added |
| veg_atm | hus_3hrPt_plev6 | atmos.hus.tpt-p6-hxy-air.3hr.glb | same | `atmos_3h_pl6_hus_.*\.nc` | added |
| extra_atm | ps_1hr_south30 | atmos.ps.tpt-u-hxy-u.1hr.30S-90S | only an hourly mean of sp existed | `atmos_1h_pt_sp_.*\.nc` (instant) | added |
| extra_atm | ts_3hr | atmos.ts.tpt-u-hxy-u.3hr.glb | only an hourly mean of ts existed | `atmos_3h_pt_ts_.*\.nc` (instant) | added |
| cap7_land | mrsol_day_10cm | land.mrsol.tavg-d10cm-hxy-lnd.day.glb | read the 100 cm field | `atmos_day_land_mrsol10_avg_mrsol_.*\.nc` | ready |
| veg_land | mrsol_3hr_100cm | land.mrsol.tavg-d100cm-hxy-lnd.3hr.glb | 3h stream was instant | `atmos_3h_land_mrsol100_avg_mrsol_.*\.nc` (the HR rule's `atmos_3h_land_mrsol_` must change) | ready |
| veg_land | mrsol_3hr_10cm | land.mrsol.tpt-d10cm-hxy-lnd.3hr.glb | read the 100 cm field | `atmos_3h_land_mrsol10_inst_mrsol_.*\.nc` | ready |
| lrcs_seaice | sistressave | seaIce.sistressave.tpt-u-hxy-si.mon.glb | stress was monthly means | `sgm11_inst\.fesom\.\d{4}\.nc`, `sgm22_inst...` (file_def_fesom lines 122-124) | ready |
| lrcs_seaice | sistressmax | seaIce.sistressmax.tpt-u-hxy-si.mon.glb | same | `sgm11_inst`, `sgm12_inst`, `sgm22_inst` | ready |
| lrcs_ocean | msftmmpa_depth | ocean.msftmmpa.tavg-ol-hyb-sea.mon.glb | fer_gm off in HR | `bolus_w\.fesom\.\d{4}\.nc` (file_def_fesom line 147). `fer_gm = .true.` in the real v3.5.1-cc run's namelist.oce | ready. Zonal-mean output, so check the grid label as for msftm (g239) |
| lrcs_ocean | msftmmpa_density | ocean.msftmmpa.tavg-rho-hyb-sea.mon.glb | same | `std_dens_DIVbolus\.fesom\.\d{4}\.nc` (line 176) | ready, same grid-label note |
| lrcs_ocean | hfx_int_day | ocean.hfx.tavg-u-hxy-sea.day.glb | pipeline gave W m-1, and there was no daily utemp | `utemp_day\.fesom\.\d{4}\.nc` | added. The rule also has to switch to the pipeline the active monthly hfxint uses (`heat_transport_pipeline`, `scaled_units: W`, hnode inputs) |
| lrcs_ocean | hfy_int_day | ocean.hfy.tavg-u-hxy-sea.day.glb | same | `vtemp_day\.fesom\.\d{4}\.nc` | added, same pipeline note |
| cap7_land | vegHeightGrass_mon | land.vegHeight.tavg-u-hxy-**grs**.mon.glb | compound not in the DReq | `*/run1/vegHeightGrass_monthly.out` (lpjg ins line 205) | ready. Change the compound to `land.vegHeight.tavg-u-hxy-ng.mon.glb`, which the DReq does have. This is a rule fix, no output change |
| lrcs_ocean | opottemptend | ocean.opottemptend.tavg-ol-hxy-sea.yr.glb | FESOM writes 2D | `opottemptend.fesom` is still (time, nod2) | impossible: needs the FESOM source change in HANDOFF_fesom_3d_tendencies.md |
| lrcs_ocean | osalttend | ocean.osalttend.tavg-ol-hxy-sea.yr.glb | same | same | impossible |
| lrcs_ocean | opottemprmadvect | ocean.opottemprmadvect.tavg-ol-hxy-sea.yr.glb | same | same | impossible |
| lrcs_ocean | opottempdiff | ocean.opottempdiff.tavg-ol-hxy-sea.yr.glb | same | same | impossible |
| lrcs_ocean | osaltrmadvect | ocean.osaltrmadvect.tavg-ol-hxy-sea.yr.glb | same | same | impossible |
| lrcs_ocean | osaltdiff | ocean.osaltdiff.tavg-ol-hxy-sea.yr.glb | same | same | impossible |
| core_land | lai | land.lai.tavg-u-hxy-lnd.mon.glb | OIFS copy of the LPJ-GUESS LAI, capped at 10 | covered by the LPJ-GUESS lai rule | dup |
| cap7_ocean | sfx | ocean.sfx.tavg-ol-hxy-sea.mon.glb | stub | lrcs_ocean `sfx` is active | dup |
| cap7_ocean | sfy | ocean.sfy.tavg-ol-hxy-sea.mon.glb | stub | lrcs_ocean `sfy` is active | dup |
| cap7_land | evspsbl_mon | atmos.evspsbl.tavg-u-hxy-u.mon.glb | collides with core_land | | dup |
| cap7_land | mrro_mon | land.mrro.tavg-u-hxy-lnd.mon.glb | collides with core_land | | dup |
| cap7_land | mrros_mon | land.mrros.tavg-u-hxy-lnd.mon.glb | collides | | dup |
| cap7_land | mrso_mon | land.mrso.tavg-u-hxy-lnd.mon.glb | collides | | dup |
| cap7_land | mrsos_mon | land.mrsol.tavg-d10cm-hxy-lnd.mon.glb | collides | | dup |
| cap7_land | snc_mon | landIce.snc.tavg-u-hxy-lnd.mon.glb | collides with cap7_atm | | dup |
| cap7_land | snd_mon | landIce.snd.tavg-u-hxy-lnd.mon.glb | collides with cap7_atm | | dup |
| cap7_land | snw_mon | landIce.snw.tavg-u-hxy-lnd.mon.glb | collides with core_land | | dup |

OpenIFS names files `<file name>_<field name>_<years>.nc` under `timeseries="only"`. FESOM
names them `<name>.fesom.<year>.nc`. Check the new patterns against the first year on disk.

## esm_tools changes (uncommitted)

```
 .../fesom2/xios_xml_cmip7/file_def_fesom.xml.j2    |  9 +++++
 namelists/oifs/48r1/xios/cmip7/axis_def.xml        | 42 +++++++++++++++++++---
 .../oifs/48r1/xios/cmip7/field_def_cmip7.xml.j2    |  4 ++-
 .../xios/cmip7/file_def_oifs_cmip7_spinup.xml.j2   | 21 ++++++++---
 namelists/oifs/48r1/xios/cmip7/grid_def.xml        | 12 +++++--
```

- `cmip7/axis_def.xml:11-51`. The FullPos axis `pressure_levels` goes from 19 to 23
  levels by adding 95000, 90000, 80000 and 75000 (JPOS3P = 75, so this is within the
  limit). New `pressure_levels_19` is a zoom with `name="pressure_levels"`, so plev19
  files keep their dimension name. `pressure_levels_3`, `_6`, `_7h` and the old
  `_zoom` are now index zooms of the FullPos axis instead of free-standing value lists.
- `cmip7/grid_def.xml:30-55`. New `reduced_pl_fpos` (FullPos axis). `reduced_pl` and
  `regular_pl` now use `pressure_levels_19`, so every plev19 stream keeps its 19 levels
  with no file_def change.
- `cmip7/field_def_cmip7.xml.j2:421-423`. The `3D_pl` group sends on `reduced_pl_fpos`.
- `cmip7/file_def_oifs_cmip7_spinup.xml.j2`:
  - `ts` in `atmos_3h_pt` (line 151).
  - `atmos_3h_pl6` enabled (lines 208-216).
  - `sp` in `atmos_1h_pt` (line 463).
  - `atmos_6h_ml` enabled (lines 495-501).
  - Comment on plev7h (line 513).
- `xios_xml_cmip7/file_def_fesom.xml.j2:41-48`. Daily `utemp_day`/`vtemp_day` (3D
  elements, `ldiag_trflx` is on in context_fesom.xml). Separate files, so the monthly
  utemp/vtemp are untouched. About 16 GB/year each on core3: the monthly utemp from
  11V is 535 MB for 12 records. The LR volume estimate counted daily hfx/hfy as <3 GB,
  which does not hold for this route. An XIOS-side vertical sum (`reduce_axis` over
  `utemp*helem`) would cut it to 2D size, but it has never been tested here.

All changes are in `cmip7/` files, so the `monthly` and default OpenIFS output modes are
unaffected. Checks run:
- `lint-xios-xml` on the active OpenIFS set (cmip7 four plus `context_ifs.xml.j2` and
  `domain_def.xml.j2`) passes, with and without `general.with_co2_tracer`.
- The FESOM set shows the same 12 warnings and 1 error as before the edit (the
  pre-existing `using_server` duplicate between iodef.xml and context_fesom.xml).
- `xmllint --noout` passes on the rendered edited files.

### Verify on the first LR leg (no test run was possible here)

- `atmos_mon_pl_ta`, `atmos_day_pl_ta` and `atm_remapped_1m_pl_t` still have 19 levels,
  dimension `pressure_levels`, and the same per-level means as before.
- Per-level means of `atmos_6h_pl7h_ta` should equal `atmos_mon_pl_ta` at 92500, 85000,
  70000, 60000, 50000, 25000 and 5000. `atmos_6h_pl3_t` should equal it at
  850/500/250. If the 6h means instead equal the first n plev19 levels, the zoom did not
  apply.
- `atmos_3h_pl6_ta` has 6 distinct levels, with 85000 and 70000 matching plev19.
- `atmos_6h_ml_zg` exists and increases monotonically with height. FullPos `z` on model
  levels has never been exercised.
- regular_pl is now two transformations (domain interpolation plus axis zoom). If XIOS
  rejects that, the run fails at init, so the error is visible rather than silent.

## Time step

The real v3.5.1-cc LR run has `TSTEP = 3600` and `NFRPOS = 1`. `output_step_freq` is
`3600 / time_step * 1` for mip cmip7, so it comes to 1 at LR, and every `freq_op` is
"1ts" = 1 h, the same hourly sampling HR had at 4ts × 900 s. The field_def
de-accumulations (`/3600`) assume 1 h post-processing intervals, which still holds. No
change is needed. Each LR "1h instant" is one model step, and averages over 3 h or 1 d
are means of hourly samples, as in HR.

## Setup prerequisite

`fesom.cmip7_cmor_output` is not tied to `oifs.cmip7_cmor_output` in awiesm3.yaml (only
xios follows the oifs one, line 1998). The LR runscript must set both. Otherwise FESOM
falls back to the minimal `xios_xml` set and none of the FESOM rows above exist.

## Output-blocked todo items

Only items an output-config change could unblock:

- **plev3 (core_atm, active in HR)**: wrong levels, as above. Fixed for LR by the zoom.
  The core_atm todo had "Verify XIOS FullPos can interpolate to arbitrary pressure level
  sets" open. The answer is that it cannot.
- **daily lai (extra_land)**: the todo says LPJ-GUESS writes only monthly lai. v3.5.1's
  `lpjg_default_cmip7_output.ins.j2` writes `lai_daily.out` (daily output on by
  default), so `land.lai.tavg-u-hxy-lnd.day.glb` needs only a rule.
- **cap7_land "no LPJ-GUESS output file" / per-PFT blocks**: these are stale. nppLeaf,
  nppRoot, nppStem, nppOther, fVegLitterMortality, fVegLitterSenescence, cSoilPools,
  fHarvestToProduct and the gpp/npp/ra/rh/cVeg tree/shrub/grass files are all in the
  ins and already have HR rules. fVegSoil, fVegSoilMortality and fVegSoilSenescence
  have no `file_` entry, which would need LPJ-GUESS code, not config.
- **ssrdc/strdc (core_atm blocker 1)**: already registered in v3.5.1 OpenIFS
  (`yomxios.F90`, NSFCFLD = 134). Nothing to do.
- **toz (cap7_aerosol)**: `tco3` is already in `atm_remapped_1m`.
- **CO2 3D / plev19 (cap7_atm "no prognostic CO2")**: in v3.5.1-cc the tracer exists, and
  `co2` is sent on model and pressure levels (C3DFLD). Only the regridded monthly
  `co2_pl` in `atm_remapped_1m_pl` is written. Left alone on purpose, because Jan
  decides on CO2/OBGC/iceberg output after the separate list and size estimate.
- Everything else under "blocked" in the todo files needs physics or source changes:
  COSP, tendencies, convective splits, aerosol, ice-age, sea-ice budget splits, basin
  masks and so on.
- The FESOM opottemp/osalt `padvect`/`pmdiff` items were skipped as "zero, fer_gm off".
  GM is on in LR, but FESOM has no separate eddy-advection tendency diagnostic, so they
  still need source work.
