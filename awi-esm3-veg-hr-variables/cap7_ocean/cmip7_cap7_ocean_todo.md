# CAP7 Ocean — Implementation Status

Source: `cmip7_CAP7_variables_ocean.csv` (43 variable-frequency entries, unfiltered)

## Summary

| Status | Count |
|--------|-------|
| Already in core/lrcs | 26 |
| Implemented (new cap7 rules) | 10 |
| Blocked — no physics | 5 |
| Blocked — basin masks | 3 |
| Blocked — namelist change needed (all resolved) | 0 |
| **Total** | **44** |

---

## Already implemented in core/lrcs (26)

These variables already have matching compound names in core_ocean or lrcs_ocean.
No new rules needed.

- [x] **areacello** (fx) — `ocean.areacello.ti-u-hxy-u.fx.glb` — core
- [x] **deptho** (fx) — `ocean.deptho.ti-u-hxy-sea.fx.glb` — core
- [x] **hfds** (mon, 2D) — `ocean.hfds.tavg-u-hxy-sea.mon.glb` — core
- [x] **masscello** (fx, 3D) — `ocean.masscello.ti-ol-hxy-sea.fx.glb` — core
- [x] **masscello** (mon, 3D) — `ocean.masscello.tavg-ol-hxy-sea.mon.glb` — core
- [x] **mlotst** (mon, 2D) — `ocean.mlotst.tavg-u-hxy-sea.mon.glb` — core
- [x] **sftof** (fx) — `ocean.sftof.ti-u-hxy-u.fx.glb` — core
- [x] **so** (mon, 3D) — `ocean.so.tavg-ol-hxy-sea.mon.glb` — core
- [x] **sos** (day, 2D) — `ocean.sos.tavg-u-hxy-sea.day.glb` — core
- [x] **sos** (mon, 2D) — `ocean.sos.tavg-u-hxy-sea.mon.glb` — core
- [x] **tauuo** (mon, 2D) — `ocean.tauuo.tavg-u-hxy-sea.mon.glb` — core
- [x] **tauvo** (mon, 2D) — `ocean.tauvo.tavg-u-hxy-sea.mon.glb` — core
- [x] **thetao** (mon, 3D) — `ocean.thetao.tavg-ol-hxy-sea.mon.glb` — core
- [x] **thkcello** (fx, 3D) — `ocean.thkcello.ti-ol-hxy-sea.fx.glb` — core
- [x] **thkcello** (mon, 3D) — `ocean.thkcello.tavg-ol-hxy-sea.mon.glb` — core
- [x] **tos** (day, 2D) — `ocean.tos.tavg-u-hxy-sea.day.glb` — core
- [x] **tos** (mon, 2D) — `ocean.tos.tavg-u-hxy-sea.mon.glb` — core
- [x] **uo** (mon, 3D) — `ocean.uo.tavg-ol-hxy-sea.mon.glb` — core
- [x] **umo** (mon, 3D) — `ocean.umo.tavg-ol-hxy-sea.mon.glb` — core
- [x] **vo** (mon, 3D) — `ocean.vo.tavg-ol-hxy-sea.mon.glb` — core
- [x] **vmo** (mon, 3D) — `ocean.vmo.tavg-ol-hxy-sea.mon.glb` — core
- [x] **wo** (mon, 3D) — `ocean.wo.tavg-ol-hxy-sea.mon.glb` — core
- [x] **wmo** (mon, 3D) — `ocean.wmo.tavg-ol-hxy-sea.mon.glb` — core
- [x] **zos** (day, 2D) — `ocean.zos.tavg-u-hxy-sea.day.glb` — core
- [x] **zos** (mon, 2D) — `ocean.zos.tavg-u-hxy-sea.mon.glb` — core
- [x] **zostoga** (mon, scalar) — `ocean.zostoga.tavg-u-hm-sea.mon.glb` — core

---

## Implemented — new cap7 rules (10)

- [x] **tossq** (day, 2D) — `ocean.tossq.tavg-u-hxy-sea.day.glb` — Square of SST. Daily SST from FESOM (`sst`, daily output), squared via `compute_square`. Same approach as monthly tossq in lrcs_ocean but at daily frequency.
- [x] **volcello** (mon, 3D) — `ocean.volcello.tavg-ol-hxy-sea.mon.glb` — Monthly ocean cell volume from `hnode` (layer thickness) x cell area. Same approach as `volcello_dec` in lrcs_ocean but averaged to monthly.
- [x] **friver** (mon, 2D) — `ocean.friver.tavg-u-hxy-sea.mon.glb` — River water flux from `runoff` (newly enabled in namelist.io). Units: m/s -> kg m-2 s-1 (x 1000) via scale_pipeline.
- [x] **msftbarot** (mon, 2D) — `ocean.msftbarot.tavg-u-hxy-sea.mon.glb` — Barotropic mass streamfunction via geostrophic SSH approx: psi = rho_0*g*H/f*eta (`compute_msftbarot` in custom_steps.py). NaN in equatorial band |f| < 1e-5. **Rule and pipeline live in lrcs_ocean.**
- [x] **hfx** (mon, 3D) — `ocean.hfx.tavg-ol-hxy-sea.mon.glb` — 3D ocean heat X transport from FESOM `utemp` (u×T, m/s·°C), scaled by rho_0·cp = 4.096e6 to get W m-2. Requires `ldiag_trflx=.true.` in namelist.
- [x] **hfxint** (mon, 2D) — `ocean.hfx.tavg-u-hxy-sea.mon.glb` — Vertically integrated ocean heat X transport. Same `utemp` input, scale then sum over depth. Requires `ldiag_trflx=.true.`.
- [x] **hfy** (mon, 3D) — `ocean.hfy.tavg-ol-hxy-sea.mon.glb` — 3D ocean heat Y transport from FESOM `vtemp` (v×T, m/s·°C), scaled by rho_0·cp = 4.096e6. Requires `ldiag_trflx=.true.`.
- [x] **hfyint** (mon, 2D) — `ocean.hfy.tavg-u-hxy-sea.mon.glb` — Vertically integrated ocean heat Y transport. Same `vtemp` input, scale then sum over depth. Requires `ldiag_trflx=.true.`.
- [x] **tauuo** (3hr, 2D) — `ocean.tauuo.tavg-u-hxy-sea.3hr.glb` — Rule written (`tauuo_3hr`, DefaultPipeline). Uses `tx_sur` from a dedicated 3-hourly FESOM output stream. **Prerequisite**: enable 3-hourly `tx_sur` output in namelist.io (separate stream from monthly). Same elem-grid caveat as the monthly core rule applies.
- [x] **tauvo** (3hr, 2D) — `ocean.tauvo.tavg-u-hxy-sea.3hr.glb` — Rule written (`tauvo_3hr`, DefaultPipeline). Uses `ty_sur` from a dedicated 3-hourly FESOM output stream. Same elem-grid caveat and namelist prerequisite as `tauuo_3hr`.

---

## Blocked — no physics in model (5)

- [ ] **bigthetao** (mon, 3D) — `ocean.bigthetao.tavg-ol-hxy-sea.mon.glb` — Conservative (potential) temperature. **FESOM2 uses potential temperature, not conservative temperature** — no conversion available without full equation of state inversion (TEOS-10 ct_from_pt would need absolute salinity).
- [ ] **ficeberg** (mon, 3D) — `ocean.ficeberg.tavg-ol-hxy-sea.mon.glb` — Water flux from icebergs. **No iceberg model** in AWI-ESM3-VEG-HR.
- [ ] **hfgeou** (fx, 2D) — `ocean.hfgeou.ti-u-hxy-sea.fx.glb` — Upward geothermal heat flux at sea floor. **Not implemented in FESOM 2.7** — no geothermal heat flux diagnostic.
- [ ] **sf6** (mon, 3D) — `ocean.sf6.tavg-ol-hxy-sea.mon.glb` — SF6 tracer concentration. **No SF6 tracer** — requires biogeochemistry module not in this configuration.
- [ ] **hfbasin** (mon, basin) — `ocean.hfbasin.tavg-u-hyb-sea.mon.glb` — Northward ocean heat transport by basin. Needs both basin masks and basin-integrated heat transport diagnostic.

---

## Blocked — basin masks needed (3)

These require basin mask infrastructure not yet available for FESOM DARS mesh.

- [x] **basin** (fx) — `ocean.basin.ti-u-hxy-u.fx.glb` — Implemented in `core_ocean` via `basin_pipeline` using external basin mask file.
- [ ] **msftmz** (mon, basin+depth) — `ocean.msftm.tavg-ol-hyb-sea.mon.glb` — Ocean meridional overturning mass streamfunction. Needs basin masks + zig-zag path integration on unstructured mesh.
- [ ] **msftyz** (mon, basin+depth) — `ocean.msfty.tavg-ol-ht-sea.mon.glb` — Ocean Y overturning mass streamfunction. Same basin mask + path integration requirement.

---

## Blocked — namelist/config change needed (all resolved)

All variables in this category now have rules written. The model must be rerun with the
updated namelist.io to produce the required output streams before these rules can execute.

- [x] **friver** (mon, 2D) — `ocean.friver.tavg-u-hxy-sea.mon.glb` — **Resolved**: `runoff` now enabled in namelist.io. See implemented section.
- [x] **hfx** (mon, 3D) — `ocean.hfx.tavg-ol-hxy-sea.mon.glb` — Rule written. See implemented section.
- [x] **hfxint** (mon, 2D) — `ocean.hfx.tavg-u-hxy-sea.mon.glb` — Rule written. See implemented section.
- [x] **hfy** (mon, 3D) — `ocean.hfy.tavg-ol-hxy-sea.mon.glb` — Rule written. See implemented section.
- [x] **hfyint** (mon, 2D) — `ocean.hfy.tavg-u-hxy-sea.mon.glb` — Rule written. See implemented section.
- [x] **tauuo** (3hr, 2D) — `ocean.tauuo.tavg-u-hxy-sea.3hr.glb` — Rule written. See implemented section.
- [x] **tauvo** (3hr, 2D) — `ocean.tauvo.tavg-u-hxy-sea.3hr.glb` — Rule written. See implemented section.
