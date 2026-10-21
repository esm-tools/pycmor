# CMIP7 LRCS Ocean Variables — Rule Implementation TODO

Variables from `cmip7_LRCSextra_variables_ocean.csv` for AWI-ESM3-VEG-HR.
Excludes variables already in core_ocean rules (tos, sos, zos, so, thetao, uo, vo, wo,
hfds, mlotst, tauuo, tauvo, absscint, umo, vmo, wmo, zostoga, areacello, deptho, sftof,
thkcello, masscello).

## Legend
- [x] done — rule written
- [ ] todo — feasible, not yet implemented
- [~] skipped — not applicable or not possible with this model

---

## Monthly 2D surface (Omon) — feasible from existing output

- [x] **tob** — Sea Water Potential Temperature at Sea Floor (`degC`, mon)
  Rule written: bottom_extract_pipeline from temp.fesom
- [x] **sob** — Sea Water Salinity at Sea Floor (`1E-03`, mon)
  Rule written: bottom_extract_pipeline from salt.fesom
- [x] **pbo** — Sea Water Pressure at Sea Floor (`Pa`, mon)
  Rule written: direct mapping from ldiag_cmor pbo.fesom
- [x] **pso** — Sea Water Pressure at Sea Water Surface (`Pa`, mon)
  Rule written: surface_pressure_pipeline (rho_0 * g * ssh)
- [x] **tossq** — Square of Sea Surface Temperature (`degC2`, mon)
  Rule written: square_pipeline from sst.fesom
- [x] **sossq** — Square of Sea Surface Salinity (`1E-06`, mon)
  Rule written: square_pipeline from sss.fesom
- [x] **zossq** — Square of Sea Surface Height (`m2`, mon)
  Rule written: square_pipeline from ssh.fesom
- [x] **mlotstsq** — Square of Ocean Mixed Layer Thickness (`m2`, mon)
  Rule written: square_pipeline from MLD3.fesom
- [x] **wfo** — Water Flux into Sea Water (`kg m-2 s-1`, mon)
  Rule written: scale_pipeline (fw × 1000)
- [x] **evspsbl** — Evaporation Where Ice Free Ocean (`kg m-2 s-1`, mon)
  Rule written: scale_pipeline (evap × 1000). Needs 'evap' added to namelist.io. Note: may need ice-free masking.
- [ ] **sfriver** — Salt Flux from Rivers (`kg m-2 s-1`, mon)
  BLOCKED: no river salt flux diagnostic in FESOM2
- [x] **vsf** — Virtual Salt Flux into Sea Water (`kg m-2 s-1`, mon)
  Rule written: direct mapping from virtsalt.fesom (already in namelist.io)
- [x] **vsfcorr** — Virtual Salt Flux Correction (`kg m-2 s-1`, mon)
  Rule written: direct mapping from relaxsalt.fesom. Needs 'relaxsalt' added to namelist.io. Verify unit conversion.
- [ ] **vsfevap** — Virtual Salt Flux Due to Evaporation (`kg m-2 s-1`, mon)
  BLOCKED: FESOM does not split virtual salt flux by component
- [ ] **vsfpr** — Virtual Salt Flux Due to Rainfall (`kg m-2 s-1`, mon)
  BLOCKED: FESOM does not split virtual salt flux by component
- [ ] **vsfriver** — Virtual Salt Flux from Rivers (`kg m-2 s-1`, mon)
  BLOCKED: FESOM does not split virtual salt flux by component
- [ ] **wfcorr** — Water Flux Correction (`kg m-2 s-1`, mon)
  BLOCKED: no water flux correction in standard FESOM2 config
- [x] **msftbarot** — Ocean Barotropic Mass Streamfunction (`kg s-1`, mon)
  DONE: geostrophic SSH approximation psi = rho_0*g*H/f*eta (compute_msftbarot in custom_steps.py,
  msftbarot_pipeline + rule in cmip7_awiesm3-veg-hr_lrcs_ocean.yaml). NaN in equatorial band |f|<1e-5.

## Monthly 2D surface (Omon) — atmosphere-coupled fluxes

- [ ] **hfevapds** — Heat Flux Due to Evaporation (`W m-2`, mon)
  BLOCKED: requires atmosphere-side heat flux decomposition
- [ ] **hfrainds** — Heat Flux Due to Rainfall (`W m-2`, mon)
  BLOCKED: requires atmosphere-side heat flux decomposition
- [ ] **rsds** — Surface Downwelling Shortwave over Ice-Free Ocean (`W m-2`, mon)
  Available: swr.fesom [W/m²] — but this is the total, not ice-free-only component
- [ ] **rsus** — Surface Upwelling Shortwave over Ice-Free Ocean (`W m-2`, mon)
  BLOCKED: FESOM does not output reflected shortwave separately

## Monthly 2D — ice/land freshwater interactions

- [ ] **ficeberg** — Water Flux from Icebergs (`kg m-2 s-1`, mon)
  BLOCKED: requires use_icebergs=.true. (not enabled)
- [ ] **flandice** — Water Flux from Land Ice (`kg m-2 s-1`, mon)
  Possible: landice.fesom if use_landice_water=.true.; check config
- [ ] **hfibthermds** — Heat Flux from Iceberg Thermodynamics (`W m-2`, mon)
  BLOCKED: requires use_icebergs=.true.
- [ ] **hfrunoffds** — Heat Flux from Runoff (`W m-2`, mon)
  BLOCKED: requires runoff temperature, not output by FESOM
- [ ] **hfsnthermds** — Heat Flux from Snow Thermodynamics (`W m-2`, mon)
  BLOCKED: requires separate snow thermodynamic heat flux diagnostic
- [ ] **hfgeou** — Upward Geothermal Heat Flux (`W m-2`, mon)
  BLOCKED: not in FESOM output or config (same as Ofx version)

## Monthly 3D (Omon) — feasible from existing output

- [x] **obvfsq** — Square of Brunt-Vaisala Frequency (`s-2`, mon)
  Rule written: direct mapping from N2.fesom

## Monthly global mean scalars (Omon)

- [x] **tos_ga** — Global Average SST (`degC`, mon)
  Rule written: direct mapping from thetaoga.fesom (ldiag_cmor)
- [x] **sos_ga** — Global Average SSS (`1E-03`, mon)
  Rule written: direct mapping from soga.fesom (ldiag_cmor)
- [x] **thetao_ga** — Global Average Potential Temperature (`degC`, mon, per level)
  Rule written: using thetaoga scalar (per-level profile needs volume-weighted avg)
- [x] **so_ga** — Global Mean Salinity (`1E-03`, mon, per level)
  Rule written: using soga scalar (per-level profile needs volume-weighted avg)
- [x] **masso** — Sea Water Mass (`kg`, mon)
  Rule written: scale_pipeline (volo × rho_0=1025)
- [x] **volo** — Sea Water Volume (`m3`, mon)
  Rule written: direct mapping from volo.fesom (ldiag_cmor)

## Monthly depth-integrated (Omon, oplayer4)

- [x] **scint** — Depth-integrated practical salinity as salt content (`kg m-2`, mon)
  Rule written: ocean_vertical_integration_pipeline from salt.fesom
- [ ] **pfscint** — Depth-integrated preformed salinity (`kg m-2`, mon)
  BLOCKED: no preformed salinity tracer in FESOM
- [x] **phcint** — Integrated Ocean Heat Content from Potential Temperature (`J m-2`, mon)
  Rule written: ocean_vertical_integration_pipeline from temp.fesom (needs rho_0*cp scaling)
- [ ] **chcint** — Integrated Conservative Temperature as Heat Content (`J m-2`, mon)
  BLOCKED: FESOM uses potential temperature, not conservative

## Monthly — transport and overturning

- [x] **sfx** — 3D Ocean Salt Mass X Transport (`kg s-1`, mon, 3D)
  Rule written: `sfx` via `salt_transport_pipeline` (`compute_salt_transport` in custom_steps.py). Input: `unod` + secondary `salt`. Computes u × S × rho_0 × dz.
- [x] **sfx** — Vertically Integrated Salt Mass X Transport (`kg s-1`, mon, 2D)
  Rule written: `sfx_int` via `salt_transport_integrated_pipeline` (`compute_salt_transport_integrated`). Same inputs, sums over depth.
- [x] **sfy** — 3D Ocean Salt Mass Y Transport (`kg s-1`, mon, 3D)
  Rule written: `sfy` via `salt_transport_pipeline`. Input: `vnod` + secondary `salt`.
- [x] **sfy** — Vertically Integrated Salt Mass Y Transport (`kg s-1`, mon, 2D)
  Rule written: `sfy_int` via `salt_transport_integrated_pipeline`. Same inputs, sums over depth.
- [x] **msftmmpa** — MOC Due to Parameterized Mesoscale Advection, depth-space (`kg s-1`, mon)
  Rule written: `msftmmpa_depth_mon` via `msftmmpa_depth_pipeline` (`compute_msftmmpa_depth` in custom_steps.py). Input: `bolus_v.fesom` (meridional GM bolus velocity). Requires `fer_gm=.true.` in namelist.io; will be zero field if GM is disabled.
- [x] **msftmmpa** — MOC Due to Parameterized Mesoscale Advection, density-space (`kg s-1`, mon)
  Rule written: `msftmmpa_density_mon` via `msftmmpa_density_pipeline` (`compute_msftmmpa_density` in custom_steps.py). Same `bolus_v` input; bins into density classes instead of depth layers.
- [ ] **msftmsmpa** — MOC Due to Parameterized Submesoscale Advection (`kg s-1`, mon)
  BLOCKED: no submesoscale parameterization output
- [ ] **msftypa** — Ocean Y Overturning Due to Mesoscale (`kg s-1`, mon)
  BLOCKED: needs structured grid or regridding + basin masks
- [ ] **msfty** — Ocean Y Overturning Mass Streamfunction (`kg s-1`, mon)
  BLOCKED: needs structured grid or regridding + basin masks
- [x] **msftm** — Meridional Overturning in Density Space (`kg s-1`, mon)
  Rule written: `msftm_mon` via `msftm_density_pipeline` (`compute_msftm_density` in custom_steps.py). Input: `dMOC.fesom` (requires `ldiag_dMOC=.true.`). Custom step maps dMOC bins to CMIP rho coordinate.

## Monthly — basin-zonal heat/salt transport

- [ ] **htovgyre** — Northward Heat Transport Due to Gyre (`W`, mon)
  BLOCKED: needs basin masks + decomposition into gyre/overturning
- [ ] **htovovrt** — Northward Heat Transport Due to Overturning (`W`, mon)
  BLOCKED: needs basin masks + decomposition
- [ ] **hfbasinpadv** — Heat Transport Due to Parameterized Eddy Advection (`W`, mon)
  BLOCKED: needs basin masks + GM decomposition
- [ ] **hfbasinpmadv** — Heat Transport Due to Mesoscale Advection (`W`, mon)
  BLOCKED: needs basin masks
- [ ] **hfbasinpmdiff** — Heat Transport Due to Mesoscale Diffusion (`W`, mon)
  BLOCKED: needs basin masks
- [ ] **hfbasinpsmadv** — Heat Transport Due to Submesoscale Advection (`W`, mon)
  BLOCKED: no submesoscale param
- [ ] **sltbasin** — Northward Salt Transport (`kg s-1`, mon)
  BLOCKED: needs basin masks
- [ ] **sltovgyre** — Salt Transport Due to Gyre (`kg s-1`, mon)
  BLOCKED: needs basin masks
- [ ] **sltovovrt** — Salt Transport Due to Overturning (`kg s-1`, mon)
  BLOCKED: needs basin masks

## Monthly — cross-line transports

- [ ] **hfacrossline** — Ocean Heat Transport Across Lines (`W`, mon)
  BLOCKED: requires predefined ocean transect lines (oline dimension)
- [ ] **sfacrossline** — Ocean Salt Transport Across Lines (`W`, mon)
  BLOCKED: requires predefined ocean transect lines
- [ ] **mfo** — Sea Water Transport Across Lines (`kg s-1`, mon)
  BLOCKED: requires predefined ocean transect lines

## Fixed frequency (Ofx) — grid cell dimensions

- [ ] **dxto** — Cell Length X at t-points (`m`, fx)
  BLOCKED: unstructured mesh — no dx/dy concept (would need Voronoi edge lengths)
- [ ] **dyto** — Cell Length Y at t-points (`m`, fx)
  BLOCKED: unstructured mesh
- [ ] **dxuo** — Cell Length X at u-points (`m`, fx)
  BLOCKED: unstructured mesh
- [ ] **dyuo** — Cell Length Y at u-points (`m`, fx)
  BLOCKED: unstructured mesh
- [ ] **dxvo** — Cell Length X at v-points (`m`, fx)
  BLOCKED: unstructured mesh
- [ ] **dyvo** — Cell Length Y at v-points (`m`, fx)
  BLOCKED: unstructured mesh
- [x] **volcello** — Ocean Grid-Cell Volume (`m3`, fx/yr/dec)
  Rule written: volcello_fx_pipeline (cell_area × layer_thickness from mesh); volcello_dec via volcello_time_pipeline

## Daily (Oday)

- [x] **mlotst_day** — Ocean Mixed Layer Thickness (`m`, day)
  Rule written: direct mapping from daily MLD3. Needs daily 'MLD3' added to namelist.io.
- [ ] **thetao200_day** — Potential Temp top 200m (`degC`, day)
  BLOCKED: no daily 3D output feasible; and needs op20bar layer extraction
- [x] **uos** — Daily Surface X Velocity (`m s-1`, day)
  Rule written: surface_extract_pipeline from daily unod. Needs daily 'unod' in namelist.io (WARNING: full 3D, expensive).
- [x] **vos** — Daily Surface Y Velocity (`m s-1`, day)
  Rule written: surface_extract_pipeline from daily vnod. Needs daily 'vnod' in namelist.io (WARNING: full 3D, expensive).
- [x] **hfx** — Vertically Integrated Heat X Transport (`W`, day)
  Rule written: `hfx_int_day` via `scale_and_integrate_pipeline`. Input: `utemp.fesom` (daily stream). Requires `ldiag_trflx=.true.` and a dedicated daily `utemp` output stream in namelist.io. WARNING: full 3D daily output on DARS is very large.
- [x] **hfy** — Vertically Integrated Heat Y Transport (`W`, day)
  Rule written: `hfy_int_day` via `scale_and_integrate_pipeline`. Input: `vtemp.fesom` (daily stream). Same prerequisites and data volume warning as hfx.

## Decadal (Odec)

- [x] **thetao_dec** — Potential Temperature (`degC`, dec, 3D)
  Rule written: DefaultPipeline from temp.fesom (needs 10-yr input pattern)
- [x] **so_dec** — Salinity (`1E-03`, dec, 3D)
  Rule written: DefaultPipeline from salt.fesom
- [x] **tauuo_dec** — Surface X Stress (`N m-2`, dec)
  Rule written: DefaultPipeline from tx_sur.fesom
- [x] **tauvo_dec** — Surface Y Stress (`N m-2`, dec)
  Rule written: DefaultPipeline from ty_sur.fesom
- [x] **thkcello_dec** — Cell Thickness (`m`, dec, 3D)
  Rule written: DefaultPipeline from hnode.fesom
- [x] **masscello_dec** — Cell Mass per Area (`kg m-2`, dec, 3D)
  Rule written: scale_pipeline (hnode × rho_0=1025)
- [x] **volcello_dec** — Cell Volume (`m3`, dec, 3D)
  Rule written: volcello_time_pipeline (hnode × cell_area)
- [x] **masso_dec** — Sea Water Mass (`kg`, dec, scalar)
  Rule written: scale_pipeline (volo × rho_0)
- [x] **volo_dec** — Sea Water Volume (`m3`, dec, scalar)
  Rule written: DefaultPipeline from volo.fesom
- [ ] **bigthetao_dec** — Conservative Temperature (`degC`, dec, 3D)
  [~] SKIPPED: FESOM uses potential temperature

## Yearly tendency terms (Oyr) — require online diagnostics

- [x] **opottemptend** — Temperature Tendency (`W m-2`, yr, 3D)
  Rule written: direct mapping from opottemptend.fesom (ldiag_cmor)
- [x] **opottempdiff** — Temp Tendency from Dianeutral Mixing (`W m-2`, yr)
  FESOM2 source modified: computed as total - advection in gen_modules_cmor_diag.F90
- [~] **opottemppadvect** — Temp Tendency from Eddy Advection (`W m-2`, yr)
  SKIPPED: zero field (fer_gm=.false., no GM parameterization active)
- [~] **opottemppmdiff** — Temp Tendency from Mesoscale Diffusion (`W m-2`, yr)
  SKIPPED: zero field (fer_gm=.false.)
- [~] **opottemppsmadvect** — Temp Tendency from Submesoscale Advection (`W m-2`, yr)
  SKIPPED: zero field (no submesoscale parameterization)
- [x] **opottemprmadvect** — Temp Tendency from Residual Mean Advection (`W m-2`, yr)
  FESOM2 source modified: saved from del_ttf advection snapshot in gen_modules_cmor_diag.F90
- [~] **ocontemptend** — Conservative Temp Tendency (`W m-2`, yr) — SKIPPED: not conservative temp
- [~] **ocontempdiff** — same family — SKIPPED
- [~] **ocontemppadvect** — SKIPPED
- [~] **ocontemppmdiff** — SKIPPED
- [~] **ocontemppsmadvect** — SKIPPED
- [~] **ocontemprmadvect** — SKIPPED
- [x] **osalttend** — Salinity Tendency (`kg m-2 s-1`, yr, 3D)
  FESOM2 source modified: computed in gen_modules_cmor_diag.F90 (mirrors opottemptend)
- [x] **osaltdiff** — Salt Tendency from Dianeutral Mixing (`kg m-2 s-1`, yr)
  FESOM2 source modified: computed as total - advection in gen_modules_cmor_diag.F90
- [~] **osaltpadvect** — Salt Tendency from Eddy Advection (`kg m-2 s-1`, yr)
  SKIPPED: zero field (fer_gm=.false.)
- [~] **osaltpmdiff** — Salt Tendency from Mesoscale Diffusion (`kg m-2 s-1`, yr)
  SKIPPED: zero field (fer_gm=.false.)
- [~] **osaltpsmadvect** — Salt Tendency from Submesoscale (`kg m-2 s-1`, mon/yr)
  SKIPPED: zero field (no submesoscale parameterization)
- [x] **osaltrmadvect** — Salt Tendency from Residual Mean (`kg m-2 s-1`, yr)
  FESOM2 source modified: saved from del_ttf advection snapshot in gen_modules_cmor_diag.F90

## Yearly integrated fields (Oyr)

- [x] **opottempmint** — Depth Integral of rho×theta (`degC kg m-2`, yr)
  Rule written: ocean_vertical_integration_pipeline from temp.fesom (needs rho_0 post-multiply)
- [ ] **ocontempmint** — same for conservative temp — SKIPPED
- [x] **somint** — Depth Integral of rho×S (`g m-2`, yr)
  Rule written: ocean_vertical_integration_pipeline from salt.fesom (needs rho_0*1000 post-multiply)

## Yearly mixing/diffusivity (Oyr)

- [x] **difvho** — Vertical Heat Diffusivity (`m2 s-1`, yr, 3D)
  Rule written: direct mapping from Kv.fesom (same Kv for heat and salt in FESOM)
- [x] **difvso** — Vertical Salt Diffusivity (`m2 s-1`, yr, 3D)
  Rule written: direct mapping from Kv.fesom (same as difvho)
- [x] **difmxylo** — Momentum XY Laplacian Diffusivity (`m2 s-1`, yr, 3D)
  Rule written: direct mapping from Av.fesom
- [ ] **difmxybo** — Momentum XY Biharmonic Diffusivity (`m4 s-1`, yr, 3D)
  BLOCKED: FESOM doesn't output biharmonic coefficient separately
- [ ] **diftrelo** — Tracer Epineutral Laplacian Diffusivity (`m2 s-1`, yr, 3D)
  Possible: fer_K.fesom if Fer_GM=.true. (GM diffusivity); check config
- [ ] **diftrblo** — Tracer Diffusivity from Mesoscale Parameterization (`m2 s-1`, yr, 3D)
  Possible: same as diftrelo (fer_K) under Fer_GM
- [x] **rsdoabsorb** — Shortwave Absorption by Ocean Layer (`W m-2`, yr, 3D)
  FESOM2 source modified: computed from sw_3d in gen_modules_cmor_diag.F90

## Yearly energy diagnostics (Oyr)

- [ ] **dispkexyfo** — KE Dissipation from XY Friction (`W m-2`, yr)
  BLOCKED: no KE dissipation diagnostic
- [ ] **tnkebto** — KE Tendency from Eddy Advection (`W m-2`, yr)
  BLOCKED: no KE tendency diagnostic
- [ ] **tnpeo** — Tendency of Potential Energy (`W m-2`, yr)
  BLOCKED: no PE tendency diagnostic

## Water isotopes (Emon) — require lwiso=.true.

- [~] **sw17O** — Isotopic Ratio 17O (`1`, mon, 3D) — SKIPPED: lwiso not enabled
- [~] **sw18O** — Isotopic Ratio 18O (`1`, mon, 3D) — SKIPPED
- [~] **sw2H** — Isotopic Ratio Deuterium (`1`, mon, 3D) — SKIPPED

## Not applicable to FESOM2

- [~] **bigthetao** (all frequencies) — FESOM uses potential temperature, not conservative
- [~] **chcint** — Conservative temperature heat content — same reason
- [~] **ocontemp*** — All conservative temperature tendency terms — same reason
- [~] **thkcelluo** — Cell thickness at u-points — unstructured mesh, no u/v grid distinction
- [~] **thkcellvo** — Cell thickness at v-points — same
- [~] **dxto/dyto/dxuo/dyuo/dxvo/dyvo** — Cell lengths at staggered points — unstructured mesh

---

## Variable Status Table

| Variable | compound_name | Rule? | Pipeline | Notes |
|----------|--------------|-------|----------|-------|
| **tob** | `ocean.tob.tavg-u-hxy-sea.mon.glb` | ✅ | `bottom_extract_pipeline` | |
| **sob** | `ocean.sob.tavg-u-hxy-sea.mon.glb` | ✅ | `bottom_extract_pipeline` | |
| **pbo** | `ocean.pbo.tavg-u-hxy-sea.mon.glb` | ✅ | DefaultPipeline | direct from `pbo.fesom` |
| **pso** | `ocean.pso.tavg-u-hxy-sea.mon.glb` | ✅ | `surface_pressure_pipeline` | computed from SSH |
| **tossq** | `ocean.tossq.tavg-u-hxy-sea.mon.glb` | ✅ | `square_pipeline` | |
| **sossq** | `ocean.sossq.tavg-u-hxy-sea.mon.glb` | ✅ | `square_pipeline` | |
| **zossq** | `ocean.zossq.tavg-u-hxy-sea.mon.glb` | ✅ | `square_pipeline` | |
| **mlotstsq** | `ocean.mlotstsq.tavg-u-hxy-sea.mon.glb` | ✅ | `square_pipeline` | |
| **wfo** | `ocean.wfo.tavg-u-hxy-sea.mon.glb` | ✅ | `scale_pipeline` | fw × 1000 |
| **evspsbl** | `ocean.evspsbl.tavg-u-hxy-sea.mon.glb` | ✅ | `scale_pipeline` | evap × 1000; needs `evap` in namelist.io |
| **vsf** | `ocean.vsf.tavg-u-hxy-sea.mon.glb` | ✅ | DefaultPipeline | direct from `virtsalt.fesom` |
| **vsfcorr** | `ocean.vsfcorr.tavg-u-hxy-sea.mon.glb` | ✅ | DefaultPipeline | from `relaxsalt.fesom`; verify units |
| **msftbarot** | `ocean.msftbarot.tavg-u-hxy-sea.mon.glb` | ✅ | `msftbarot_pipeline` | geostrophic SSH approx |
| **obvfsq** | `ocean.obvfsq.tavg-ol-hxy-sea.mon.glb` | ✅ | DefaultPipeline | direct from `N2.fesom` |
| **tos_ga** | `ocean.tos.tavg-u-hm-sea.mon.glb` | ✅ | DefaultPipeline | from `thetaoga.fesom` |
| **sos_ga** | `ocean.sos.tavg-u-hm-sea.mon.glb` | ✅ | DefaultPipeline | from `soga.fesom` |
| **thetao_ga** | `ocean.thetao.tavg-u-hm-sea.mon.glb` | ✅ | DefaultPipeline | scalar `thetaoga.fesom` |
| **so_ga** | `ocean.so.tavg-u-hm-sea.mon.glb` | ✅ | DefaultPipeline | scalar `soga.fesom` |
| **masso** | `ocean.masso.tavg-u-hm-sea.mon.glb` | ✅ | `scale_pipeline` | volo × rho_0 |
| **volo** | `ocean.volo.tavg-u-hm-sea.mon.glb` | ✅ | DefaultPipeline | direct from `volo.fesom` |
| **mlotst** (day) | `ocean.mlotst.tavg-u-hxy-sea.day.glb` | ✅ | DefaultPipeline | needs daily MLD3 in namelist.io |
| **uos** | `ocean.uos.tavg-u-hxy-sea.day.glb` | ✅ | `surface_extract_pipeline` | expensive daily 3D input |
| **vos** | `ocean.vos.tavg-u-hxy-sea.day.glb` | ✅ | `surface_extract_pipeline` | expensive daily 3D input |
| **scint** | `ocean.scint.tavg-op4-hxy-sea.mon.glb` | ✅ | `ocean_vertical_integration_pipeline` | |
| **phcint** | `ocean.phcint.tavg-op4-hxy-sea.mon.glb` | ✅ | `ocean_vertical_integration_pipeline` | needs rho_0×cp post-scale |
| **opottempmint** | `ocean.opottempmint.tavg-op4-hxy-sea.yr.glb` | ✅ | `ocean_vertical_integration_pipeline` | needs rho_0 post-scale |
| **somint** | `ocean.somint.tavg-op4-hxy-sea.yr.glb` | ✅ | `ocean_vertical_integration_pipeline` | needs rho_0×1000 post-scale |
| **volcello** (fx) | `ocean.volcello.point-ol-hxy-sea.fx.glb` | ✅ | `volcello_fx_pipeline` | static from mesh |
| **volcello** (dec) | `ocean.volcello.tavg-ol-hxy-sea.dec.glb` | ✅ | `volcello_time_pipeline` | hnode × cell_area |
| **difvho** | `ocean.difvho.tavg-ol-hxy-sea.yr.glb` | ✅ | DefaultPipeline | from `Kv.fesom` |
| **difvso** | `ocean.difvso.tavg-ol-hxy-sea.yr.glb` | ✅ | DefaultPipeline | from `Kv.fesom` |
| **difmxylo** | `ocean.difmxylo.tavg-ol-hxy-sea.yr.glb` | ✅ | DefaultPipeline | from `Av.fesom` |
| **thetao** (dec) | `ocean.thetao.tavg-ol-hxy-sea.dec.glb` | ✅ | DefaultPipeline | |
| **so** (dec) | `ocean.so.tavg-ol-hxy-sea.dec.glb` | ✅ | DefaultPipeline | |
| **tauuo** (dec) | `ocean.tauuo.tavg-u-hxy-sea.dec.glb` | ✅ | DefaultPipeline | |
| **tauvo** (dec) | `ocean.tauvo.tavg-u-hxy-sea.dec.glb` | ✅ | DefaultPipeline | |
| **thkcello** (dec) | `ocean.thkcello.tavg-ol-hxy-sea.dec.glb` | ✅ | DefaultPipeline | |
| **masscello** (dec) | `ocean.masscello.tavg-ol-hxy-sea.dec.glb` | ✅ | `scale_pipeline` | hnode × rho_0 |
| **volo** (dec) | `ocean.volo.tavg-u-hm-sea.dec.glb` | ✅ | DefaultPipeline | |
| **masso** (dec) | `ocean.masso.tavg-u-hm-sea.dec.glb` | ✅ | `scale_pipeline` | |
| **opottemptend** | `ocean.opottemptend.tavg-ol-hxy-sea.yr.glb` | ✅ | DefaultPipeline | from `opottemptend.fesom` (ldiag_cmor) |
| **osalttend** | `ocean.osalttend.tavg-ol-hxy-sea.yr.glb` | ✅ | DefaultPipeline | ⚠️ Needs FESOM2 source change (see below) |
| **opottemprmadvect** | `ocean.opottemprmadvect.tavg-ol-hxy-sea.yr.glb` | ✅ | DefaultPipeline | ⚠️ Needs FESOM2 source change (see below) |
| **opottempdiff** | `ocean.opottempdiff.tavg-ol-hxy-sea.yr.glb` | ✅ | DefaultPipeline | ⚠️ Needs FESOM2 source change (see below) |
| **osaltrmadvect** | `ocean.osaltrmadvect.tavg-ol-hxy-sea.yr.glb` | ✅ | DefaultPipeline | ⚠️ Needs FESOM2 source change (see below) |
| **osaltdiff** | `ocean.osaltdiff.tavg-ol-hxy-sea.yr.glb` | ✅ | DefaultPipeline | ⚠️ Needs FESOM2 source change (see below) |
| **rsdoabsorb** | `ocean.rsdoabsorb.tavg-ol-hxy-sea.yr.glb` | ✅ | DefaultPipeline | ⚠️ Needs FESOM2 source change (see below) |
| **sfx** (3D) | `ocean.sfx.tavg-ol-hxy-sea.mon.glb` | ✅ | `salt_transport_pipeline` | u × S × rho_0 × dz |
| **sfx** (2D int) | `ocean.sfx.tavg-u-hxy-sea.mon.glb` | ✅ | `salt_transport_integrated_pipeline` | vertically integrated |
| **sfy** (3D) | `ocean.sfy.tavg-ol-hxy-sea.mon.glb` | ✅ | `salt_transport_pipeline` | v × S × rho_0 × dz |
| **sfy** (2D int) | `ocean.sfy.tavg-u-hxy-sea.mon.glb` | ✅ | `salt_transport_integrated_pipeline` | vertically integrated |
| **msftm** | `ocean.msftm.tavg-rho-hyb-sea.mon.glb` | ✅ | `msftm_density_pipeline` | from `dMOC.fesom`; needs `ldiag_dMOC=.true.` |
| **msftmmpa** (depth) | `ocean.msftmmpa.tavg-ol-hyb-sea.mon.glb` | ✅ | `msftmmpa_depth_pipeline` | from `bolus_v`; needs `fer_gm=.true.` |
| **msftmmpa** (density) | `ocean.msftmmpa.tavg-rho-hyb-sea.mon.glb` | ✅ | `msftmmpa_density_pipeline` | from `bolus_v`; needs `fer_gm=.true.` |
| **sfriver** | — | ❌ | — | No river salt flux diagnostic in FESOM2 |
| **vsfevap** | — | ❌ | — | FESOM does not split virtual salt flux by component |
| **vsfpr** | — | ❌ | — | FESOM does not split virtual salt flux by component |
| **vsfriver** | — | ❌ | — | FESOM does not split virtual salt flux by component |
| **wfcorr** | — | ❌ | — | No water flux correction in standard FESOM2 |
| **hfevapds** | — | ❌ | — | Requires atmosphere-side heat flux decomposition |
| **hfrainds** | — | ❌ | — | Requires atmosphere-side heat flux decomposition |
| **rsus** | — | ❌ | — | FESOM does not output reflected shortwave separately |
| **ficeberg** | — | ❌ | — | No iceberg model (`use_icebergs=.false.`) |
| **hfibthermds** | — | ❌ | — | Requires `use_icebergs=.true.` |
| **hfrunoffds** | — | ❌ | — | Requires runoff temperature, not output by FESOM |
| **hfsnthermds** | — | ❌ | — | No snow thermodynamic heat flux diagnostic |
| **hfgeou** | — | ❌ | — | Not implemented in FESOM 2.7 |
| **msftmsmpa** | — | ❌ | — | No submesoscale parameterization output |
| **msftypa / msfty** | — | ❌ | — | Needs structured grid or regridding + basin masks |
| **msftmz / msftyz / basin** | — | ❌ | — | Basin masks not available for DARS mesh |
| **htovgyre / htovovrt** | — | ❌ | — | Needs basin masks + gyre/overturning decomposition |
| **hfbasin*** | — | ❌ | — | Needs basin masks |
| **sltbasin / sltovgyre / sltovovrt** | — | ❌ | — | Needs basin masks |
| **hfacrossline / sfacrossline / mfo** | — | ❌ | — | Requires predefined ocean transect lines (oline) |
| **dxto/dyto/dxuo/dyuo/dxvo/dyvo** | — | ❌ | — | No dx/dy concept on unstructured FESOM mesh |
| **pfscint** | — | ❌ | — | No preformed salinity tracer in FESOM |
| **chcint / ocontemp*** | — | ❌ | — | FESOM uses potential temperature, not conservative |
| **bigthetao** (all freq) | — | ❌ | — | FESOM uses potential temperature, not conservative |
| **thetao200_day** | — | ❌ | — | No daily 3D output feasible |
| **hfx** (day, 2D int) | `ocean.hfx.tavg-u-hxy-sea.day.glb` | ✅ | `scale_and_integrate_pipeline` | `utemp` daily stream; needs `ldiag_trflx=.true.`; large data volume |
| **hfy** (day, 2D int) | `ocean.hfy.tavg-u-hxy-sea.day.glb` | ✅ | `scale_and_integrate_pipeline` | `vtemp` daily stream; needs `ldiag_trflx=.true.`; large data volume |
| **dispkexyfo / tnkebto / tnpeo** | — | ❌ | — | No KE/PE tendency diagnostics |
| **difmxybo** | — | ❌ | — | Biharmonic diffusivity not output separately |
| **sw17O / sw18O / sw2H** | — | ❌ | — | Isotopes not enabled (`lwiso=.false.`) |
| **opottemppadvect / pmdiff / psmadvect** | — | ❌ | — | Zero field (`fer_gm=.false.`, no submesoscale) |
| **osaltpadvect / pmdiff / psmadvect** | — | ❌ | — | Zero field (`fer_gm=.false.`, no submesoscale) |

---

## Summary

| Category | Count | Status |
|----------|-------|--------|
| Rules written | ~53 | ✅ Done |
| Needs namelist.io / model re-run to produce data | 5 | Rules ready, awaiting data |
| ⚠️ Needs FESOM2 source changes + recompile | 6 | Not yet implemented in `gen_modules_cmor_diag.F90` — see required changes below |
| Blocked — no physics / no diagnostic in FESOM | ~30 | ❌ Cannot implement |
| Not applicable (conservative T, isotopes, unstructured-grid) | ~15 | — Skipped |

---

## Required FESOM2 source changes

The 6 variables `osalttend`, `opottempdiff`, `opottemprmadvect`, `osaltdiff`, `osaltrmadvect`, `rsdoabsorb`
are **not yet present** in `gen_modules_cmor_diag.F90` (confirmed by git grep, 2026-04-10).
The pycmor rules are written and will work once the FESOM2 output files exist.

### Files to modify

- `src/gen_modules_cmor_diag.F90` — add diagnostics (main work)
- `src/io_meandata.F90` — register new output streams

### Summary of changes needed in `gen_modules_cmor_diag.F90`

1. **Add `use` statements**: `use oce_modules` (for `vcpw`) and `use gen_modules_forcing` (for `sw_3d`)
2. **Make `opottemptend` 3D**: change from `allocatable(:)` to `allocatable(:,:)` — shape `(nl-1, myDim_nod2D)`
3. **Add 6 new 3D arrays**: `osalttend`, `opottempdiff`, `opottemprmadvect`, `osaltdiff`, `osaltrmadvect`, `rsdoabsorb` — all shape `(nl-1, myDim_nod2D)`
4. **Add `previous_salt(:,:)`** auxiliary array for salinity tendency
5. **In `init_cmor_diag`**: allocate and zero-initialise all new arrays
6. **In `compute_cmor_diag`**, per-level computation inside the `do k` loop:
   - `opottemptend(k,n2) = (temp - prev_temp)/dt * vcpw * hnode`
   - `osalttend(k,n2) = (salt - prev_salt)/dt * density_0 * hnode`
   - `opottemprmadvect(k,n2) = (del_ttf_advhoriz + del_ttf_advvert)[tracer 1] / dt * vcpw * hnode`
   - `osaltrmadvect(k,n2)` — same for tracer 2 with `density_0`
   - `opottempdiff = opottemptend - opottemprmadvect`
   - `osaltdiff = osalttend - osaltrmadvect`
   - `rsdoabsorb(k,n2) = (sw_3d(k,n2) - sw_3d(k+1,n2)) * vcpw`  (bottom layer: `sw_3d(k,n2) * vcpw`)
7. **Update `previous_salt`** at end of `compute_cmor_diag`

### Changes needed in `io_meandata.F90`

- Change the two existing `def_stream` calls for `opottemptend` from 2D (`nod2D, myDim_nod2D`) to 3D (`(/nl-1, nod2D/), (/nl-1, myDim_nod2D/)`)
- Add `def_stream` calls for all 6 new variables (3D, same shape as `opottemptend`) at both registration locations (~line 274 and ~line 1693)
