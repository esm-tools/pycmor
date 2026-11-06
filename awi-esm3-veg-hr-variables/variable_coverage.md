# Variable Coverage Report — AWI-ESM3-VEG-HR

**Total:** 866 unique compound names across all CSVs | 506 rules in YAML files

**Categories:** `core`, `cap7`, `veg`, `lrcs`, `extra`

---

## Summary

| Realm | With rules | Total | Coverage |
|---|---|---|---|
| aerosol | 2 | 32 | 6% |
| aerosol atmosChem | 7 | 14 | 50% |
| atmos | 165 | 278 | 59% |
| atmos aerosol | 0 | 4 | 0% |
| atmos aerosol land | 2 | 2 | ✅ |
| atmos atmosChem aerosol | 0 | 4 | 0% |
| atmos land | 3 | 3 | ✅ |
| atmosChem | 4 | 18 | 22% |
| atmosChem aerosol | 0 | 5 | 0% |
| land | 126 | 190 | 66% |
| landIce | 3 | 3 | ✅ |
| landIce land | 9 | 19 | 47% |
| ocean | 93 | 175 | 53% |
| ocean seaIce | 3 | 3 | ✅ |
| seaIce | 78 | 114 | 68% |
| seaIce ocean | 2 | 2 | ✅ |

---

## Realm: aerosol — 2/32 rules

| Variable | Rule | Reason | Categories |
|---|---|---|---|
| `aerosol.abs550aer.tavg-u-hxy-u.mon.glb` | ❌ | No prognostic aerosol | cap7 |
| `aerosol.bry.tavg-p39-hy-air.mon.glb` | ❌ | No atmospheric chemistry | cap7 |
| `aerosol.ccn.tavg-u-hxy-ccl.mon.glb` | ❌ | No prognostic aerosol (MACv2-SP only) | veg |
| `aerosol.cdnc.tavg-al-hxy-u.mon.glb` | ❌ | No atmospheric chemistry | cap7 |
| `aerosol.cfc114.tavg-al-hxy-u.mon.glb` | ❌ | No atmospheric chemistry | cap7 |
| `aerosol.cly.tavg-p39-hy-air.mon.glb` | ❌ | No atmospheric chemistry | cap7 |
| `aerosol.drydust.tavg-u-hxy-u.mon.glb` | ❌ | No deposition scheme | cap7 |
| `aerosol.hcfc22.tavg-al-hxy-u.mon.glb` | ❌ | HCFC22 on model levels: no atmospheric chemistry in IFS | cap7 |
| `aerosol.hcl.tavg-al-hxy-u.mon.glb` | ❌ | No atmospheric chemistry | cap7 |
| `aerosol.hfc125.tavg-al-hxy-u.mon.glb` | ❌ | No atmospheric chemistry | cap7 |
| `aerosol.hfc134a.tavg-al-hxy-u.mon.glb` | ❌ | No atmospheric chemistry | cap7 |
| `aerosol.hno3.tavg-al-hxy-u.mon.glb` | ❌ | No atmospheric chemistry | cap7 |
| `aerosol.lwp.tavg-u-hxy-u.mon.glb` | ✅ |  | veg |
| `aerosol.mmrpm2p5.tavg-al-hxy-u.mon.glb` | ❌ | No prognostic aerosol (MACv2-SP only) | veg |
| `aerosol.no2.tavg-h2m-hxy-u.1hr.glb` | ❌ | No atmospheric chemistry | extra |
| `aerosol.noy.tavg-p39-hy-air.mon.glb` | ❌ | No atmospheric chemistry | cap7 |
| `aerosol.o3.tavg-h2m-hxy-u.1hr.glb` | ❌ | Near-surface ozone: no interactive chemistry in IFS | extra |
| `aerosol.o3.tmax-h2m-hxy-u.day.glb` | ❌ | Near-surface ozone: no interactive chemistry in IFS | extra |
| `aerosol.od550aer.tavg-u-hxy-u.mon.glb` | ❌ | MACv2-SP provides anthropogenic perturbation AOD only, not total AOD | cap7 |
| `aerosol.od550bb.tavg-u-hxy-u.mon.glb` | ❌ | No prognostic aerosol — MACv2-SP has no biomass burning AOD component | cap7 |
| `aerosol.od550bc.tavg-u-hxy-u.mon.glb` | ❌ | No prognostic aerosol | cap7 |
| `aerosol.od550dust.tavg-u-hxy-u.mon.glb` | ❌ | No prognostic aerosol | cap7 |
| `aerosol.od550lt1aer.tavg-u-hxy-u.mon.glb` | ❌ | No prognostic aerosol | cap7 |
| `aerosol.od550no3.tavg-u-hxy-u.mon.glb` | ❌ | No prognostic aerosol — MACv2-SP has no nitrate AOD component | cap7 |
| `aerosol.od550oa.tavg-u-hxy-u.mon.glb` | ❌ | No prognostic aerosol — MACv2-SP has no organic aerosol AOD component | cap7 |
| `aerosol.od550so4.tavg-u-hxy-u.mon.glb` | ❌ | No prognostic aerosol — MACv2-SP has no sulfate AOD component | cap7 |
| `aerosol.od550soa.tavg-u-hxy-u.mon.glb` | ❌ | No prognostic aerosol — MACv2-SP has no secondary organic aerosol AOD component | veg |
| `aerosol.od550ss.tavg-u-hxy-u.mon.glb` | ❌ | No prognostic aerosol | cap7 |
| `aerosol.oh.tavg-al-hxy-u.mon.glb` | ❌ | No atmospheric chemistry in IFS | cap7 |
| `aerosol.so2.tavg-al-hxy-u.mon.glb` | ❌ | No atmospheric chemistry in IFS | cap7 |
| `aerosol.toz.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 |
| `aerosol.wetdust.tavg-u-hxy-u.mon.glb` | ❌ | No deposition scheme | cap7 |

## Realm: aerosol atmosChem — 7/14 rules

| Variable | Rule | Reason | Categories |
|---|---|---|---|
| `aerosol.conccn.tavg-al-hxy-u.mon.glb` | ❌ | MACv2-SP affects CDNC via Twomey parametrization but does not output 3D aerosol number concentration | veg |
| `aerosol.emibbbc.tavg-u-hxy-u.mon.glb` | ✅ | LPJ-GUESS fFireAll_monthly.out × Andreae (2019) EF=0.37 g BC/kgDM | veg |
| `aerosol.emibbch4.tavg-u-hxy-u.mon.glb` | ✅ | LPJ-GUESS fFireAll_monthly.out × Andreae (2019) EF=1.94 g CH4/kgDM | veg |
| `aerosol.emibbco.tavg-u-hxy-u.mon.glb` | ✅ | LPJ-GUESS fFireAll_monthly.out × Andreae (2019) EF=63.0 g CO/kgDM | veg |
| `aerosol.emibbdms.tavg-u-hxy-u.mon.glb` | ✅ | LPJ-GUESS fFireAll_monthly.out × Andreae (2019) EF=0.68 g DMS/kgDM | veg |
| `aerosol.emibboa.tavg-u-hxy-u.mon.glb` | ✅ | LPJ-GUESS fFireAll_monthly.out × Andreae (2019) EF=2.62 g OA/kgDM | veg |
| `aerosol.emibbso2.tavg-u-hxy-u.mon.glb` | ✅ | LPJ-GUESS fFireAll_monthly.out × Andreae (2019) EF for SO2 | veg |
| `aerosol.emibbvoc.tavg-u-hxy-u.mon.glb` | ✅ | LPJ-GUESS fFireAll_monthly.out × Andreae (2019) EF for NMVOC | veg |
| `aerosol.sfpm1.tavg-h2m-hxy-u.1hr.glb` | ❌ | No prognostic aerosol — MACv2-SP has no size-resolved aerosol mass (requires CAMS/M7) | extra |
| `aerosol.sfpm1.tavg-h2m-hxy-u.day.glb` | ❌ | No prognostic aerosol — MACv2-SP has no size-resolved aerosol mass (requires CAMS/M7) | extra |
| `aerosol.sfpm10.tavg-h2m-hxy-u.1hr.glb` | ❌ | No prognostic aerosol — MACv2-SP has no size-resolved aerosol mass (requires CAMS/M7) | extra |
| `aerosol.sfpm10.tavg-h2m-hxy-u.day.glb` | ❌ | No prognostic aerosol — MACv2-SP has no size-resolved aerosol mass (requires CAMS/M7) | extra |
| `aerosol.sfpm25.tavg-h2m-hxy-u.1hr.glb` | ❌ | No prognostic aerosol — MACv2-SP has no size-resolved aerosol mass (requires CAMS/M7) | extra |
| `aerosol.sfpm25.tavg-h2m-hxy-u.day.glb` | ❌ | No prognostic aerosol — MACv2-SP has no size-resolved aerosol mass (requires CAMS/M7) | extra |

## Realm: atmos — 165/278 rules

| Variable | Rule | Reason | Categories |
|---|---|---|---|
| `atmos.albisccp.tavg-u-hxy-cl.day.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.albisccp.tavg-u-hxy-cl.mon.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.ccb.tavg-u-hxy-ccl.day.glb` | ❌ | Needs IFS source changes (KCBOT not exposed via XIOS) | cap7 |
| `atmos.ccb.tavg-u-hxy-ccl.mon.glb` | ❌ | Needs IFS source changes (KCBOT not exposed via XIOS) | cap7 |
| `atmos.cct.tavg-u-hxy-ccl.day.glb` | ❌ | Needs IFS source changes (KCTOP not exposed via XIOS) | cap7 |
| `atmos.cct.tavg-u-hxy-ccl.mon.glb` | ❌ | Needs IFS source changes (KCTOP not exposed via XIOS) | cap7 |
| `atmos.ci.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 |
| `atmos.cl.tavg-al-hxy-u.day.glb` | ✅ |  | extra |
| `atmos.cl.tavg-al-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.clc.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (convective cloud fraction internal to scheme) | cap7 |
| `atmos.clcalipso.tavg-220hPa-hxy-air.day.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.clcalipso.tavg-220hPa-hxy-air.mon.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.clcalipso.tavg-560hPa-hxy-air.day.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.clcalipso.tavg-560hPa-hxy-air.mon.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.clcalipso.tavg-840hPa-hxy-air.day.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.clcalipso.tavg-840hPa-hxy-air.mon.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.clcalipso.tavg-h40-hxy-air.mon.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.cldnci.tavg-u-hxy-cl.day.glb` | ❌ | Needs IFS source changes (no in-cloud ice crystal number diagnostic) | cap7 |
| `atmos.cldnvi.tavg-u-hxy-u.day.glb` | ❌ | Needs IFS source changes (no column ice crystal number diagnostic) | cap7 |
| `atmos.cli.tavg-al-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.clic.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (convective cloud ice internal to scheme) | cap7 |
| `atmos.clis.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (requires clic from convection scheme) | cap7 |
| `atmos.clisccp.tavg-p7c-hxy-air.mon.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.clivi.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.clivi.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.clivic.tavg-u-hxy-u.day.glb` | ❌ | Needs IFS source changes (convective/stratiform IWP not separated) | cap7 |
| `atmos.clmisr.tavg-h16-hxy-air.mon.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.cls.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (requires clc from convection scheme) | cap7 |
| `atmos.clt.tavg-u-hxy-u.1hr.30S-90S` | ✅ |  | extra |
| `atmos.clt.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 core |
| `atmos.clt.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.cltcalipso.tavg-u-hxy-u.day.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.cltcalipso.tavg-u-hxy-u.mon.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.cltisccp.tavg-u-hxy-u.day.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.cltisccp.tavg-u-hxy-u.mon.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.clw.tavg-al-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.clwc.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (convective cloud liquid internal to scheme) | cap7 |
| `atmos.clws.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (requires clwc from convection scheme) | cap7 |
| `atmos.clwvi.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.clwvi.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.clwvic.tavg-u-hxy-u.day.glb` | ❌ | Needs IFS source changes (convective/stratiform LWP not separated) | cap7 |
| `atmos.co23D.tavg-al-hxy-u.mon.glb` | ❌ | No CO2 tracer (concentration-driven) | cap7 |
| `atmos.co2mass.tavg-u-hm-u.mon.glb` | ❌ | No CO2 tracer (concentration-driven) | cap7 |
| `atmos.dmc.tavg-alh-hxy-u.mon.glb` | ❌ | Needs IFS source changes (convective detrainment internal to scheme) | cap7 |
| `atmos.edt.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (convective entrainment internal to scheme) | cap7 |
| `atmos.evu.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (convective entrainment internal to scheme) | cap7 |
| `atmos.fco2antt.tavg-u-hxy-u.mon.glb` | ❌ | No CO2 tracer (concentration-driven) | cap7 |
| `atmos.fco2fos.tavg-u-hxy-u.mon.glb` | ❌ | No CO2 tracer (concentration-driven) | cap7 |
| `atmos.fco2nat.tavg-u-hxy-u.mon.glb` | ❌ | No CO2 tracer | cap7 |
| `atmos.hfdsnb.tavg-u-hxy-lnd.day.glb` | ❌ | Needs IFS source changes (downward heat flux at snow base not a standard OIFS output) | veg |
| `atmos.hfls.tavg-u-hxy-u.1hr.glb` | ✅ |  | extra |
| `atmos.hfls.tavg-u-hxy-u.3hr.glb` | ✅ |  | veg |
| `atmos.hfls.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.hfls.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.hfss.tavg-u-hxy-u.1hr.glb` | ✅ |  | extra |
| `atmos.hfss.tavg-u-hxy-u.3hr.glb` | ✅ |  | veg |
| `atmos.hfss.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.hfss.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.hur.tavg-al-hxy-u.mon.glb` | ✅ |  | cap7 |
| `atmos.hur.tavg-p19-hxy-air.mon.glb` | ✅ |  | cap7 core |
| `atmos.hur.tavg-p19-hxy-u.day.glb` | ✅ |  | cap7 core |
| `atmos.hurs.tavg-h2m-hxy-u.1hr.30S-90S` | ✅ |  | extra |
| `atmos.hurs.tavg-h2m-hxy-u.1hr.glb` | ✅ |  | extra |
| `atmos.hurs.tavg-h2m-hxy-u.6hr.glb` | ✅ |  | cap7 core |
| `atmos.hurs.tavg-h2m-hxy-u.day.glb` | ✅ |  | cap7 core |
| `atmos.hurs.tavg-h2m-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.hurs.tmax-h2m-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.hurs.tmin-h2m-hxy-crp.day.glb` | ❌ | Crop-tile variant (tmin-h2m-hxy-crp): no crop tiles in IFS (run_landcover=0) | extra |
| `atmos.hurs.tmin-h2m-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.hurs.tpt-h2m-hxy-u.3hr.glb` | ✅ |  | extra |
| `atmos.hus.tavg-al-hxy-u.mon.glb` | ✅ |  | cap7 |
| `atmos.hus.tavg-p19-hxy-u.day.glb` | ✅ |  | cap7 core |
| `atmos.hus.tavg-p19-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.hus.tpt-al-hxy-u.6hr.glb` | ✅ |  | cap7 |
| `atmos.hus.tpt-p6-hxy-air.3hr.glb` | ✅ |  | veg |
| `atmos.hus.tpt-p7h-hxy-air.6hr.glb` | ✅ |  | cap7 |
| `atmos.huss.tavg-h2m-hxy-u.day.glb` | ✅ |  | cap7 core |
| `atmos.huss.tavg-h2m-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.huss.tpt-h2m-hxy-u.1hr.glb` | ✅ |  | cap7 |
| `atmos.huss.tpt-h2m-hxy-u.3hr.glb` | ✅ |  | cap7 core |
| `atmos.loadbc.tavg-u-hxy-u.day.glb` | ❌ | No prognostic aerosol | cap7 |
| `atmos.loaddust.tavg-u-hxy-u.day.glb` | ❌ | No prognostic aerosol | cap7 |
| `atmos.loadnh4.tavg-u-hxy-u.day.glb` | ❌ | No prognostic aerosol | cap7 |
| `atmos.loadno3.tavg-u-hxy-u.day.glb` | ❌ | No prognostic aerosol | cap7 |
| `atmos.loadoa.tavg-u-hxy-u.day.glb` | ❌ | No prognostic aerosol | cap7 |
| `atmos.loadpoa.tavg-u-hxy-u.day.glb` | ❌ | No prognostic aerosol | cap7 |
| `atmos.loadso4.tavg-u-hxy-u.day.glb` | ❌ | No prognostic aerosol | cap7 |
| `atmos.loadsoa.tavg-u-hxy-u.day.glb` | ❌ | No prognostic aerosol | cap7 |
| `atmos.loadss.tavg-u-hxy-u.day.glb` | ❌ | No prognostic aerosol | cap7 |
| `atmos.mc.tavg-alh-hxy-u.mon.glb` | ❌ | Needs IFS source changes (convective mass flux internal to scheme) | cap7 |
| `atmos.mcd.tavg-alh-hxy-u.mon.glb` | ❌ | Needs IFS source changes (convective mass flux internal to scheme) | cap7 |
| `atmos.mcu.tavg-alh-hxy-u.mon.glb` | ❌ | Needs IFS source changes (convective mass flux internal to scheme) | cap7 |
| `atmos.noaahi2m.tavg-h2m-hxy-u.day.glb` | ❌ | No heat index scheme (requires Rothfusz formula post-processing) | extra |
| `atmos.noaahi2m.tmax-h2m-hxy-u.day.glb` | ❌ | No heat index scheme (requires Rothfusz formula post-processing) | extra |
| `atmos.pctisccp.tavg-u-hxy-cl.day.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.pctisccp.tavg-u-hxy-cl.mon.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.pfull.tavg-al-hxy-u.day.glb` | ✅ |  | extra |
| `atmos.pfull.tclm-al-hxy-u.mon.glb` | ✅ |  | cap7 |
| `atmos.phalf.tclm-alh-hxy-u.mon.glb` | ❌ | Needs IFS source changes (alevhalf axis not yet configured) | cap7 |
| `atmos.pr.tavg-u-hxy-crp.day.glb` | ❌ | Only 1hr/3hr/day/mon frequency implemented | extra |
| `atmos.pr.tavg-u-hxy-u.1hr.30S-90S` | ✅ |  | extra |
| `atmos.pr.tavg-u-hxy-u.1hr.glb` | ✅ |  | cap7 core |
| `atmos.pr.tavg-u-hxy-u.3hr.glb` | ✅ |  | cap7 core |
| `atmos.pr.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 core |
| `atmos.pr.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.pr.tmax-u-hxy-u.day.glb` | ❌ | Only 1hr/3hr/day/mon frequency implemented | extra |
| `atmos.prc.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.prc.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.prrsn.tavg-u-hxy-lnd.day.glb` | ❌ | Needs IFS source changes (IFS does not partition precip by surface type) | veg |
| `atmos.prsn.tavg-u-hxy-u.3hr.glb` | ✅ |  | cap7 |
| `atmos.prsn.tavg-u-hxy-u.6hr.glb` | ✅ |  | veg |
| `atmos.prsn.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.prsn.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.prsnc.tavg-u-hxy-lnd.day.glb` | ❌ | Needs IFS source changes (IFS has total snowfall only, no convective/LS split) | veg |
| `atmos.prsnsn.tavg-u-hxy-lnd.day.glb` | ❌ | Needs IFS source changes (IFS does not track snowfall fraction on snow) | veg |
| `atmos.prw.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.prw.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.ps.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 core |
| `atmos.ps.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.ps.tpt-u-hxy-u.1hr.30S-90S` | ❌ | production gap: OIFS XIOS emits ``atmos_1h_sfc_sp`` (averaged), no ``atmos_1h_pt_sp`` instant source. CMIP7 asks for ``time: point`` to preserve sub-hourly pressure-wave variability (storms, gravity waves over the Southern Ocean); shipping a 1-hour average mislabeled as instant defeats the scientific purpose of the variable. Production runs already underway; adding the XIOS field would require a model restart. Skipped honestly rather than mislabel. | extra |
| `atmos.ps.tpt-u-hxy-u.1hr.glb` | ✅ |  | cap7 |
| `atmos.ps.tpt-u-hxy-u.3hr.glb` | ✅ |  | veg |
| `atmos.ps.tpt-u-hxy-u.6hr.glb` | ✅ |  | cap7 |
| `atmos.psl.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 core |
| `atmos.psl.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.psl.tpt-u-hxy-u.1hr.glb` | ✅ |  | cap7 |
| `atmos.psl.tpt-u-hxy-u.6hr.glb` | ✅ |  | cap7 |
| `atmos.ptp.tavg-u-hxy-u.mon.glb` | ❌ | Needs IFS source changes (tropopause not exposed via XIOS) | cap7 |
| `atmos.reffclic.tavg-al-hxy-ccl.mon.glb` | ❌ | No cloud microphysics diagnostics | cap7 |
| `atmos.reffclis.tavg-al-hxy-scl.mon.glb` | ❌ | No cloud microphysics diagnostics | cap7 |
| `atmos.reffclwc.tavg-al-hxy-ccl.mon.glb` | ❌ | No cloud microphysics diagnostics | cap7 |
| `atmos.reffclws.tavg-al-hxy-scl.mon.glb` | ❌ | No COSP satellite simulators | cap7 |
| `atmos.rld.tavg-alh-hxy-u.mon.glb` | ❌ | Needs IFS source changes (ecRad half-level profiles not exposed via XIOS) | cap7 |
| `atmos.rldcs.tavg-alh-hxy-u.mon.glb` | ❌ | Needs IFS source changes (ecRad half-level profiles not exposed via XIOS) | cap7 |
| `atmos.rlds.tavg-u-hxy-u.1hr.30S-90S` | ✅ |  | extra |
| `atmos.rlds.tavg-u-hxy-u.1hr.glb` | ✅ |  | cap7 |
| `atmos.rlds.tavg-u-hxy-u.3hr.glb` | ✅ |  | veg |
| `atmos.rlds.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.rlds.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.rldscs.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.rldscs.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.rls.tavg-u-hxy-u.day.glb` | ✅ |  | extra |
| `atmos.rls.tavg-u-hxy-u.mon.glb` | ✅ |  | veg |
| `atmos.rlu.tavg-alh-hxy-u.mon.glb` | ❌ | Needs IFS source changes (ecRad half-level profiles not exposed via XIOS) | cap7 |
| `atmos.rlucs.tavg-alh-hxy-u.mon.glb` | ❌ | Needs IFS source changes (ecRad half-level profiles not exposed via XIOS) | cap7 |
| `atmos.rlus.tavg-u-hxy-u.1hr.glb` | ✅ |  | extra |
| `atmos.rlus.tavg-u-hxy-u.3hr.glb` | ✅ |  | veg |
| `atmos.rlus.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.rlus.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.rluscs.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.rluscs.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.rlut.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.rlut.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.rlutcs.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.rlutcs.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.rsd.tavg-alh-hxy-u.mon.glb` | ❌ | Needs IFS source changes (ecRad half-level profiles not exposed via XIOS) | cap7 |
| `atmos.rsdcs.tavg-alh-hxy-u.mon.glb` | ❌ | Needs IFS source changes (ecRad half-level profiles not exposed via XIOS) | cap7 |
| `atmos.rsds.tavg-u-hxy-u.1hr.30S-90S` | ✅ |  | extra |
| `atmos.rsds.tavg-u-hxy-u.1hr.glb` | ✅ |  | cap7 |
| `atmos.rsds.tavg-u-hxy-u.3hr.glb` | ✅ |  | veg |
| `atmos.rsds.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 core |
| `atmos.rsds.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.rsdscs.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.rsdscs.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.rsdscsdiff.tavg-u-hxy-u.day.glb` | ❌ | Needs IFS source changes (ecRad sw_dn_diffuse_surf_g not exposed) | cap7 |
| `atmos.rsdsdiff.tavg-u-hxy-u.1hr.glb` | ❌ | Needs IFS source changes (ecRad sw_dn_diffuse_surf_g not exposed) | cap7 |
| `atmos.rsdsdiff.tavg-u-hxy-u.day.glb` | ❌ | Needs IFS source changes (ecRad sw_dn_diffuse_surf_g not exposed) | cap7 |
| `atmos.rsdt.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.rsdt.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.rss.tavg-u-hxy-u.day.glb` | ✅ |  | extra |
| `atmos.rss.tavg-u-hxy-u.mon.glb` | ✅ |  | veg |
| `atmos.rsu.tavg-alh-hxy-u.mon.glb` | ❌ | Needs IFS source changes (ecRad half-level profiles not exposed via XIOS) | cap7 |
| `atmos.rsucs.tavg-alh-hxy-u.mon.glb` | ❌ | Needs IFS source changes (ecRad half-level profiles not exposed via XIOS) | cap7 |
| `atmos.rsus.tavg-u-hxy-u.1hr.glb` | ✅ |  | extra |
| `atmos.rsus.tavg-u-hxy-u.3hr.glb` | ✅ |  | veg |
| `atmos.rsus.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.rsus.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.rsuscs.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.rsuscs.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.rsut.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.rsut.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.rsutcs.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.rsutcs.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.rtmt.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 |
| `atmos.sci.tavg-u-hxy-u.mon.glb` | ❌ | Needs IFS source changes (no clear IFS diagnostic for shallow convection fraction) | cap7 |
| `atmos.sfcWind.tavg-h10m-hxy-u.1hr.30S-90S` | ✅ |  | extra |
| `atmos.sfcWind.tavg-h10m-hxy-u.1hr.glb` | ✅ |  | cap7 |
| `atmos.sfcWind.tavg-h10m-hxy-u.day.glb` | ✅ |  | cap7 core |
| `atmos.sfcWind.tavg-h10m-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.sfcWind.tmax-h10m-hxy-u.day.glb` | ✅ |  | cap7 |
| `atmos.sftlf.ti-u-hxy-u.fx.glb` | ✅ |  | cap7 core |
| `atmos.smc.tavg-alh-hxy-u.mon.glb` | ❌ | Needs IFS source changes (shallow convective flux internal to scheme) | cap7 |
| `atmos.snmsl.tavg-u-hxy-lnd.day.glb` | ✅ |  | veg |
| `atmos.snrefr.tavg-u-hxy-lnd.day.glb` | ❌ | Needs IFS source changes (HTESSEL snow refreezing flux not exposed via XIOS) | veg |
| `atmos.snwc.tavg-u-hxy-lnd.day.glb` | ❌ | Needs IFS source changes (HTESSEL canopy snow not confirmed accessible via XIOS) | veg |
| `atmos.ta.tavg-700hPa-hxy-air.day.glb` | ✅ |  | cap7 |
| `atmos.ta.tavg-al-hxy-u.mon.glb` | ✅ |  | cap7 |
| `atmos.ta.tavg-p19-hxy-air.day.glb` | ✅ |  | cap7 core |
| `atmos.ta.tavg-p19-hxy-air.mon.glb` | ✅ |  | cap7 core |
| `atmos.ta.tpt-al-hxy-u.6hr.glb` | ✅ |  | cap7 |
| `atmos.ta.tpt-p3-hxy-air.6hr.glb` | ✅ |  | cap7 core |
| `atmos.ta.tpt-p6-hxy-air.3hr.glb` | ✅ |  | veg |
| `atmos.ta.tpt-p7h-hxy-air.6hr.glb` | ✅ |  | cap7 |
| `atmos.tas.tavg-h2m-hxy-u.day.glb` | ✅ |  | cap7 core |
| `atmos.tas.tavg-h2m-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.tas.tmax-h2m-hxy-crp.day.glb` | ❌ | Only 1hr/3hr/day/mon frequency implemented | extra |
| `atmos.tas.tmax-h2m-hxy-u.day.glb` | ✅ |  | cap7 core |
| `atmos.tas.tmaxavg-h2m-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.tas.tmin-h2m-hxy-crp.day.glb` | ❌ | Only 1hr/3hr/day/mon frequency implemented | extra |
| `atmos.tas.tmin-h2m-hxy-u.day.glb` | ✅ |  | cap7 core |
| `atmos.tas.tminavg-h2m-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.tas.tpt-h2m-hxy-u.3hr.glb` | ✅ |  | cap7 core |
| `atmos.tauu.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.tauv.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.tnhus.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (humidity tendency not exposed) | cap7 |
| `atmos.tnhusa.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (humidity tendency not exposed) | cap7 |
| `atmos.tnhusc.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (humidity tendency not exposed) | cap7 |
| `atmos.tnhusd.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (humidity tendency not exposed) | cap7 |
| `atmos.tnhusmp.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (humidity tendency not exposed) | cap7 |
| `atmos.tnhuspbl.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (humidity tendency not exposed) | cap7 |
| `atmos.tnhusscp.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (humidity tendency not exposed) | cap7 |
| `atmos.tnhusscpbl.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (humidity tendency not exposed) | cap7 |
| `atmos.tnt.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (temperature tendency not exposed) | cap7 |
| `atmos.tnta.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (temperature tendency not exposed) | cap7 |
| `atmos.tntc.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (temperature tendency not exposed) | cap7 |
| `atmos.tntd.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (temperature tendency not exposed) | cap7 |
| `atmos.tntmp.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (temperature tendency not exposed) | cap7 |
| `atmos.tntpbl.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (temperature tendency not exposed) | cap7 |
| `atmos.tntr.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (temperature tendency not exposed) | cap7 |
| `atmos.tntrl.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (temperature tendency not exposed) | cap7 |
| `atmos.tntrlcs.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (temperature tendency not exposed) | cap7 |
| `atmos.tntrs.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (temperature tendency not exposed) | cap7 |
| `atmos.tntrscs.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (temperature tendency not exposed) | cap7 |
| `atmos.tntscp.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (temperature tendency not exposed) | cap7 |
| `atmos.tntscpbl.tavg-al-hxy-u.mon.glb` | ❌ | Needs IFS source changes (temperature tendency not exposed) | cap7 |
| `atmos.ts.tavg-u-hxy-lnd.day.glb` | ❌ | Only 1hr/3hr/6hr/day/mon frequency implemented | veg |
| `atmos.ts.tavg-u-hxy-u.1hr.glb` | ✅ |  | cap7 |
| `atmos.ts.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.ts.tpt-u-hxy-u.3hr.glb` | ❌ | production gap: OIFS XIOS emits ``atmos_1h_ts_ts`` (averaged), no ``atmos_3h_pt_ts`` instant source. CMIP7 asks for ``time: point`` to capture diurnal-cycle peaks; sampling a 1-hour average every 3 hours would smooth the daytime maximum that the variable was meant to record. Production runs already underway; adding the XIOS field would require a model restart. Skipped honestly rather than mislabel. | extra |
| `atmos.ts.tpt-u-hxy-u.6hr.glb` | ✅ |  | cap7 |
| `atmos.ua.tavg-p19-hxy-air.day.glb` | ✅ |  | cap7 core |
| `atmos.ua.tavg-p19-hxy-air.mon.glb` | ✅ |  | cap7 core |
| `atmos.ua.tpt-al-hxy-u.6hr.glb` | ✅ |  | cap7 |
| `atmos.ua.tpt-h100m-hxy-u.1hr.glb` | ❌ | Needs IFS source changes (IFS does not interpolate to 100m height) | cap7 |
| `atmos.ua.tpt-p3-hxy-air.6hr.glb` | ✅ |  | cap7 core |
| `atmos.ua.tpt-p6-hxy-air.3hr.glb` | ✅ |  | veg |
| `atmos.ua.tpt-p7h-hxy-air.6hr.glb` | ✅ |  | cap7 |
| `atmos.uas.tavg-h10m-hxy-u.day.glb` | ✅ |  | cap7 core |
| `atmos.uas.tavg-h10m-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.uas.tpt-h10m-hxy-u.1hr.glb` | ✅ |  | cap7 |
| `atmos.uas.tpt-h10m-hxy-u.3hr.glb` | ✅ |  | cap7 core |
| `atmos.va.tavg-p19-hxy-air.day.glb` | ✅ |  | cap7 core |
| `atmos.va.tavg-p19-hxy-air.mon.glb` | ✅ |  | cap7 core |
| `atmos.va.tpt-al-hxy-u.6hr.glb` | ✅ |  | cap7 |
| `atmos.va.tpt-h100m-hxy-u.1hr.glb` | ❌ | Needs IFS source changes (IFS does not interpolate to 100m height) | cap7 |
| `atmos.va.tpt-p3-hxy-air.6hr.glb` | ✅ |  | cap7 core |
| `atmos.va.tpt-p6-hxy-air.3hr.glb` | ✅ |  | veg |
| `atmos.va.tpt-p7h-hxy-air.6hr.glb` | ✅ |  | cap7 |
| `atmos.vas.tavg-h10m-hxy-u.day.glb` | ✅ |  | cap7 core |
| `atmos.vas.tavg-h10m-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `atmos.vas.tpt-h10m-hxy-u.1hr.glb` | ✅ |  | cap7 |
| `atmos.vas.tpt-h10m-hxy-u.3hr.glb` | ✅ |  | cap7 core |
| `atmos.wap.tavg-500hPa-hxy-air.day.glb` | ✅ |  | cap7 |
| `atmos.wap.tavg-p19-hxy-air.mon.glb` | ✅ |  | cap7 core |
| `atmos.wap.tavg-p19-hxy-u.day.glb` | ✅ |  | cap7 core |
| `atmos.wap.tpt-p6-hxy-air.3hr.glb` | ✅ |  | veg |
| `atmos.wbgt.tavg-h2m-hxy-u.day.glb` | ❌ | No Wet Bulb Globe Temperature scheme | extra |
| `atmos.wbgt.tmax-h2m-hxy-u.day.glb` | ❌ | No Wet Bulb Globe Temperature scheme | extra |
| `atmos.wsg.tmax-h100m-hxy-u.1hr.glb` | ❌ | IFS does not output wind gust at 100m height (10m gust is implemented) | cap7 |
| `atmos.wsg.tmax-h100m-hxy-u.mon.glb` | ❌ | IFS does not output wind gust at 100m height (10m gust is implemented) | extra |
| `atmos.wsg.tmax-h10m-hxy-u.1hr.glb` | ✅ |  | cap7 |
| `atmos.wsg.tmax-h10m-hxy-u.mon.glb` | ✅ |  | extra |
| `atmos.zfull.ti-al-hxy-u.fx.glb` | ❌ | Needs IFS source changes (offline geopotential height computation needed) | cap7 |
| `atmos.zg.tavg-p19-hxy-air.day.glb` | ✅ |  | cap7 core |
| `atmos.zg.tavg-p19-hxy-air.mon.glb` | ✅ |  | cap7 core |
| `atmos.zg.tpt-al-hxy-u.6hr.glb` | ✅ |  | cap7 |
| `atmos.zg.tpt-p7h-hxy-air.6hr.glb` | ✅ |  | cap7 |
| `atmos.ztp.tavg-u-hxy-u.mon.glb` | ❌ | Needs IFS source changes (tropopause not exposed via XIOS) | cap7 |

## Realm: atmos aerosol — 0/4 rules

| Variable | Rule | Reason | Categories |
|---|---|---|---|
| `atmos.co2.tavg-al-hxy-u.mon.glb` | ❌ | No CO2 tracer (concentration-driven) | cap7 |
| `atmos.co2.tavg-p19-hxy-air.mon.glb` | ❌ | No CO2 tracer (concentration-driven) | cap7 |
| `atmos.co2.tclm-p19-hxy-air.mon.glb` | ❌ | No CO2 tracer (concentration-driven) | cap7 |
| `atmos.co2.tclm-u-hm-u.mon.glb` | ❌ | No CO2 tracer (concentration-driven) | cap7 |

## Realm: atmos aerosol land — 2/2 rules

| Variable | Rule | Reason | Categories |
|---|---|---|---|
| `atmos.bldep.tavg-u-hxy-u.1hr.glb` | ✅ |  | extra |
| `atmos.bldep.tpt-u-hxy-u.3hr.glb` | ✅ |  | veg |

## Realm: atmos atmosChem aerosol — 0/4 rules

| Variable | Rule | Reason | Categories |
|---|---|---|---|
| `atmos.reffcclwtop.tavg-u-hxy-ccl.day.glb` | ❌ | No cloud microphysics diagnostics | cap7 |
| `atmos.reffsclwtop.tavg-u-hxy-scl.day.glb` | ❌ | No cloud microphysics diagnostics | cap7 |
| `atmos.reffsclwtop.tavg-u-hxy-scl.mon.glb` | ❌ | No cloud microphysics diagnostics | veg |
| `atmos.scldncl.tavg-u-hxy-scl.day.glb` | ❌ | No cloud microphysics diagnostics (no column droplet number diagnostic) | cap7 |

## Realm: atmos land — 3/3 rules

| Variable | Rule | Reason | Categories |
|---|---|---|---|
| `atmos.areacella.ti-u-hxy-u.fx.glb` | ✅ |  | core |
| `atmos.evspsbl.tavg-u-hxy-lnd.day.glb` | ✅ |  | extra |
| `atmos.evspsbl.tavg-u-hxy-u.mon.glb` | ✅ |  | core |

## Realm: atmosChem — 4/18 rules

| Variable | Rule | Reason | Categories |
|---|---|---|---|
| `atmosChem.cfc11.tavg-u-hm-air.mon.glb` | ✅ | input4MIPs CR-CMIP-1-0-0 annual global-mean, ffill→monthly, ×1e-12 ppt→mol mol-1 | cap7 |
| `atmosChem.cfc113.tavg-u-hm-air.mon.glb` | ❌ | Prescribed scalar — GHG input4MIPs file not confirmed available at *ghg path | cap7 |
| `atmosChem.cfc12.tavg-u-hm-air.mon.glb` | ✅ | input4MIPs CR-CMIP-1-0-0 annual global-mean, ffill→monthly, ×1e-12 ppt→mol mol-1 | cap7 |
| `atmosChem.ch4.tavg-p19-hxy-air.mon.glb` | ❌ | Prescribed well-mixed scalar — CMIP7: omit 3D field, report global mean (ch4.tavg-u-hm-air) instead | cap7 |
| `atmosChem.ch4.tavg-u-hm-air.mon.glb` | ✅ | input4MIPs CR-CMIP-1-0-0 annual global-mean, ffill→monthly, ×1e-9 ppb→mol mol-1 (well-mixed prescribed; CMIP7: report global mean instead of 3D) | cap7 |
| `atmosChem.ch4.tclm-p19-hxy-air.mon.glb` | ❌ | Prescribed well-mixed scalar — CMIP7: omit 3D field, report global mean (ch4.tavg-u-hm-air) instead | cap7 |
| `atmosChem.ch4.tclm-u-hm-air.mon.glb` | ❌ | Prescribed well-mixed scalar — CMIP7: omit 3D field, report global mean (ch4.tavg-u-hm-air) instead | cap7 |
| `atmosChem.flashrate.tavg-u-hxy-u.day.glb` | ❌ | No lightning parameterization output in IFS configuration | extra |
| `atmosChem.flashrate.tavg-u-hxy-u.mon.glb` | ❌ | No lightning parameterization output in IFS configuration | extra |
| `atmosChem.hcfc22.tavg-u-hm-air.mon.glb` | ❌ | HCFC22 on model levels: no atmospheric chemistry in IFS | cap7 |
| `atmosChem.n2o.tavg-al-hxy-u.mon.glb` | ❌ | Prescribed well-mixed scalar — CMIP7: omit 3D field, report global mean (n2o.tavg-u-hm-air) instead | cap7 |
| `atmosChem.n2o.tavg-p19-hxy-air.mon.glb` | ❌ | Prescribed well-mixed scalar — CMIP7: omit 3D field, report global mean (n2o.tavg-u-hm-air) instead | cap7 |
| `atmosChem.n2o.tavg-u-hm-air.mon.glb` | ✅ | input4MIPs CR-CMIP-1-0-0 annual global-mean, ffill→monthly, ×1e-9 ppb→mol mol-1 (well-mixed prescribed; CMIP7: report global mean instead of 3D) | cap7 |
| `atmosChem.n2o.tclm-p19-hxy-air.mon.glb` | ❌ | Prescribed well-mixed scalar — CMIP7: omit 3D field, report global mean (n2o.tavg-u-hm-air) instead | cap7 |
| `atmosChem.n2o.tclm-u-hm-air.mon.glb` | ❌ | Prescribed well-mixed scalar — CMIP7: omit 3D field, report global mean (n2o.tavg-u-hm-air) instead | cap7 |
| `atmosChem.o3.tavg-al-hxy-u.mon.glb` | ❌ | IFS does not send o3 to XIOS (not in context_ifs.xml.j2) — needs IFS source change | cap7 |
| `atmosChem.o3.tavg-p19-hxy-air.mon.glb` | ❌ | IFS does not send o3 to XIOS (not in context_ifs.xml.j2) — needs IFS source change | cap7 |
| `atmosChem.o3.tclm-p19-hxy-air.mon.glb` | ❌ | IFS does not send o3 to XIOS (not in context_ifs.xml.j2) — needs IFS source change | cap7 |

## Realm: atmosChem aerosol — 0/5 rules

| Variable | Rule | Reason | Categories |
|---|---|---|---|
| `atmosChem.ch4.tavg-al-hxy-u.mon.glb` | ❌ | Prescribed well-mixed scalar — CMIP7: omit 3D field, report global mean (ch4.tavg-u-hm-air) instead | cap7 |
| `atmosChem.dms.tavg-al-hxy-u.mon.glb` | ❌ | No atmospheric chemistry in IFS | cap7 |
| `atmosChem.drynoy.tavg-u-hxy-u.mon.glb` | ❌ | No deposition scheme (no atmospheric chemistry) | cap7 |
| `atmosChem.emich4.tavg-u-hxy-u.mon.glb` | ❌ | No methane emission scheme (ifmethane=0) | extra |
| `atmosChem.wetnoy.tavg-u-hxy-u.mon.glb` | ❌ | No deposition scheme (no atmospheric chemistry) | cap7 |

## Realm: land — 126/190 rules

| Variable | Rule | Reason | Categories |
|---|---|---|---|
| `land.areacellr.ti-u-hxy-u.fx.glb` | ✅ |  | extra |
| `land.baresoilFrac.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 |
| `land.baresoilFrac.tavg-u-hxy-u.yr.glb` | ✅ |  | veg |
| `land.burntFractionAll.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 |
| `land.c3PftFrac.tavg-u-hxy-u.mon.glb` | ✅ |  | extra |
| `land.c4PftFrac.tavg-u-hxy-u.mon.glb` | ✅ |  | extra |
| `land.cGeologicStorage.tavg-u-hxy-u.mon.glb` | ❌ | No geologic carbon storage model | cap7 |
| `land.cLand.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.cLeaf.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.cLitter.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.cLitterCwd.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.cLitterLut.tpt-u-hxy-multi.yr.glb` | ✅ |  | veg |
| `land.cLitterSubSurf.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.cLitterSurf.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.cOther.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.cProduct.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.cProductLut.tpt-u-hxy-multi.yr.glb` | ✅ |  | veg |
| `land.cRoot.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.cSoil.tavg-d100cm-hxy-lnd.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.cSoil.tavg-sl-hxy-lnd.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.cSoil.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.cSoilLut.tpt-u-hxy-multi.yr.glb` | ✅ |  | veg |
| `land.cSoilPools.tavg-u-hxy-lnd.mon.glb` | ❌ | Requires per-pool disaggregation not in LPJ-GUESS standard output | cap7 |
| `land.cStem.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.cVeg.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.cVeg.tavg-u-hxy-ng.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.cVeg.tavg-u-hxy-shb.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.cVeg.tavg-u-hxy-tree.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.cVegLut.tpt-u-hxy-multi.yr.glb` | ✅ |  | veg |
| `land.cropFrac.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 |
| `land.cropFrac.tavg-u-hxy-u.yr.glb` | ✅ |  | veg |
| `land.cropFracC3.tavg-u-hxy-u.mon.glb` | ✅ |  | extra |
| `land.cropFracC4.tavg-u-hxy-u.mon.glb` | ✅ |  | extra |
| `land.dcw.tavg-u-hxy-lnd.day.glb` | ✅ |  | extra |
| `land.dgw.tavg-u-hxy-lnd.day.glb` | ✅ |  | veg |
| `land.drivw.tavg-u-hxy-lnd.day.glb` | ❌ | No river routing model | veg |
| `land.dslw.tavg-u-hxy-lnd.day.glb` | ✅ |  | extra |
| `land.dsn.tavg-u-hxy-lnd.day.glb` | ✅ |  | veg |
| `land.dsw.tavg-u-hxy-lnd.day.glb` | ✅ |  | veg |
| `land.esn.tavg-u-hxy-lnd.day.glb` | ❌ | Only day frequency implemented | veg |
| `land.evspsblpot.tavg-u-hxy-lnd.day.glb` | ❌ | Only mon frequency implemented | veg |
| `land.evspsblpot.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `land.evspsblsoi.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 core |
| `land.evspsblsoi.tavg-u-hxy-u.3hr.glb` | ❌ | Only mon frequency implemented | veg |
| `land.evspsblveg.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 core |
| `land.evspsblveg.tavg-u-hxy-u.3hr.glb` | ❌ | Only mon frequency implemented | veg |
| `land.fAnthDisturb.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.fBNF.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `land.fCLandToOcean.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.fDeforestToAtmos.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.fDeforestToProduct.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.fFire.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.fFireAll.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.fFireNat.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.fHarvestToAtmos.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.fHarvestToGeologicStorage.tavg-u-hxy-lnd.mon.glb` | ❌ | No geologic carbon storage model | cap7 |
| `land.fHarvestToProduct.tavg-u-hxy-lnd.mon.glb` | ❌ | No fHarvestToProduct_monthly.out in LPJ-GUESS | cap7 |
| `land.fLitterFire.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.fLitterSoil.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.fLuc.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `land.fLulccAtmLut.tavg-u-hxy-multi.mon.glb` | ✅ |  | veg |
| `land.fNLandToOcean.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `land.fNLitterSoil.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `land.fNVegSoil.tavg-u-hxy-lnd.mon.glb` | ❌ | No fNVegSoil output in LPJ-GUESS (only fNVegLitter) | veg |
| `land.fNgas.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `land.fNgasFire.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `land.fNleach.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `land.fNloss.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `land.fNup.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `land.fProductDecomp.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.fVegFire.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.fVegLitter.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.fVegLitterMortality.tavg-u-hxy-lnd.mon.glb` | ❌ | Not in LPJ-GUESS output files | cap7 |
| `land.fVegLitterSenescence.tavg-u-hxy-lnd.mon.glb` | ❌ | Not in LPJ-GUESS output files | cap7 |
| `land.fVegSoil.tavg-u-hxy-lnd.mon.glb` | ❌ | Not in LPJ-GUESS output files | cap7 |
| `land.fVegSoilMortality.tavg-u-hxy-lnd.mon.glb` | ❌ | Not in LPJ-GUESS output files | cap7 |
| `land.fVegSoilSenescence.tavg-u-hxy-lnd.mon.glb` | ❌ | Not in LPJ-GUESS output files | cap7 |
| `land.fracInLut.tsum-u-hxy-lnd.yr.glb` | ✅ |  | veg |
| `land.fracLut.tpt-u-hxy-u.mon.glb` | ✅ |  | veg |
| `land.fracLut.tpt-u-hxy-u.yr.glb` | ✅ |  | veg |
| `land.fracOutLut.tsum-u-hxy-lnd.yr.glb` | ✅ |  | veg |
| `land.gpp.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.gpp.tavg-u-hxy-ng.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.gpp.tavg-u-hxy-shb.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.gpp.tavg-u-hxy-tree.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.gppLut.tavg-u-hxy-multi.mon.glb` | ✅ |  | veg |
| `land.gppVgt.tavg-u-hxy-multi.day.glb` | ❌ | No daily per-PFT output from LPJ-GUESS | veg |
| `land.grassFrac.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 |
| `land.grassFrac.tavg-u-hxy-u.yr.glb` | ✅ |  | veg |
| `land.hfdsl.tavg-u-hxy-lnd.3hr.glb` | ✅ |  | veg |
| `land.hflsLut.tavg-u-hxy-multi.mon.glb` | ❌ | Surface energy balance variable — not output by LPJ-GUESS | veg |
| `land.hfssLut.tavg-u-hxy-multi.mon.glb` | ❌ | Surface energy balance variable — not output by LPJ-GUESS | veg |
| `land.irrDem.tavg-u-hxy-u.day.glb` | ❌ | No irrigation scheme | extra |
| `land.irrGw.tavg-u-hxy-u.day.glb` | ❌ | No irrigation scheme | extra |
| `land.irrLut.tavg-u-hxy-multi.mon.glb` | ✅ |  | veg |
| `land.irrLut.tavg-u-hxy-u.day.glb` | ❌ | Only mon frequency implemented | extra |
| `land.irrSurf.tavg-u-hxy-u.day.glb` | ❌ | No irrigation scheme | extra |
| `land.lai.tavg-u-hxy-lnd.day.glb` | ✅ |  | extra |
| `land.lai.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 core |
| `land.laiLut.tavg-u-hxy-multi.mon.glb` | ✅ |  | veg |
| `land.laiVgt.tavg-u-hxy-multi.day.glb` | ❌ | No daily per-PFT output from LPJ-GUESS | veg |
| `land.landCoverFrac.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 |
| `land.mrro.tavg-u-hxy-lnd.3hr.glb` | ✅ |  | veg |
| `land.mrro.tavg-u-hxy-lnd.day.glb` | ✅ |  | cap7 |
| `land.mrro.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 core |
| `land.mrrob.tavg-u-hxy-lnd.day.glb` | ✅ |  | veg |
| `land.mrros.tavg-u-hxy-lnd.3hr.glb` | ✅ |  | veg |
| `land.mrros.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 core |
| `land.mrso.tavg-u-hxy-lnd.day.glb` | ✅ |  | cap7 |
| `land.mrso.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 core |
| `land.mrsofc.ti-u-hxy-lnd.fx.glb` | ✅ |  | cap7 core |
| `land.mrsol.tavg-d100cm-hxy-lnd.3hr.glb` | ✅ |  | veg |
| `land.mrsol.tavg-d10cm-hxy-lnd.day.glb` | ✅ |  | cap7 |
| `land.mrsol.tavg-d10cm-hxy-lnd.mon.glb` | ✅ |  | cap7 core |
| `land.mrsol.tavg-sl-hxy-lnd.mon.glb` | ❌ | Only 3hr/day/mon frequency implemented | cap7 |
| `land.mrsol.tpt-d10cm-hxy-lnd.3hr.glb` | ✅ |  | veg |
| `land.mrsolLut.tavg-d10cm-hxy-multi.mon.glb` | ✅ |  | veg |
| `land.mrsow.tavg-u-hxy-lnd.day.glb` | ✅ |  | extra |
| `land.mrtws.tavg-u-hxy-lnd.day.glb` | ✅ |  | veg |
| `land.nLand.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `land.nLitter.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `land.nMineral.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `land.nProduct.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `land.nSoil.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `land.nVeg.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `land.nbp.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.nbpLut.tavg-u-hxy-multi.mon.glb` | ❌ | No per-tile variant in LPJ-GUESS (only gridcell total nbp_monthly.out) | veg |
| `land.nep.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.npp.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.npp.tavg-u-hxy-ng.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.npp.tavg-u-hxy-shb.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.npp.tavg-u-hxy-tree.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.nppLeaf.tavg-u-hxy-lnd.mon.glb` | ❌ | Not in LPJ-GUESS standard output files | cap7 |
| `land.nppLut.tavg-u-hxy-multi.mon.glb` | ✅ |  | veg |
| `land.nppOther.tavg-u-hxy-lnd.mon.glb` | ❌ | Not in LPJ-GUESS standard output files | cap7 |
| `land.nppRoot.tavg-u-hxy-lnd.mon.glb` | ❌ | Not in LPJ-GUESS standard output files | cap7 |
| `land.nppStem.tavg-u-hxy-lnd.mon.glb` | ❌ | Not in LPJ-GUESS standard output files | cap7 |
| `land.nppVgt.tavg-u-hxy-multi.day.glb` | ❌ | No daily per-PFT output from LPJ-GUESS | veg |
| `land.orog.ti-u-hxy-u.fx.30S-90S` | ✅ |  | extra |
| `land.orog.ti-u-hxy-u.fx.glb` | ✅ |  | cap7 core |
| `land.pastureFrac.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 |
| `land.pastureFracC3.tavg-u-hxy-u.mon.glb` | ✅ |  | extra |
| `land.pastureFracC4.tavg-u-hxy-u.mon.glb` | ✅ |  | extra |
| `land.prveg.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.qgwr.tavg-u-hxy-lnd.day.glb` | ❌ | No groundwater scheme in HTESSEL | veg |
| `land.ra.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.ra.tavg-u-hxy-ng.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.ra.tavg-u-hxy-shb.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.ra.tavg-u-hxy-tree.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.raLeaf.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.raLut.tavg-u-hxy-multi.mon.glb` | ✅ |  | veg |
| `land.raOther.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.raRoot.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.raStem.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.raVgt.tavg-u-hxy-multi.day.glb` | ❌ | No daily per-PFT output from LPJ-GUESS | veg |
| `land.residualFrac.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 |
| `land.rh.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.rh.tavg-u-hxy-ng.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.rh.tavg-u-hxy-shb.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.rh.tavg-u-hxy-tree.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `land.rhLitter.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.rhLut.tavg-u-hxy-multi.mon.glb` | ✅ |  | veg |
| `land.rhSoil.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.rhVgt.tavg-u-hxy-multi.day.glb` | ❌ | No daily per-PFT output from LPJ-GUESS | veg |
| `land.rivi.tavg-u-hxy-lnd.day.glb` | ❌ | No river routing model | extra |
| `land.rivo.tavg-u-hxy-lnd.day.glb` | ❌ | No river routing model | veg |
| `land.rootd.ti-u-hxy-lnd.fx.glb` | ✅ |  | cap7 core |
| `land.rzwc.tavg-u-hxy-lnd.day.glb` | ❌ | HTESSEL has fixed soil layers not defined by root depth — cannot derive root-zone moisture | extra |
| `land.sftgif.ti-u-hxy-u.fx.glb` | ✅ |  | cap7 core |
| `land.shrubFrac.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 |
| `land.shrubFrac.tavg-u-hxy-u.yr.glb` | ✅ |  | veg |
| `land.slthick.ti-sl-hxy-lnd.fx.glb` | ✅ |  | cap7 core |
| `land.srfrad.tavg-u-hxy-u.3hr.glb` | ✅ |  | veg |
| `land.sw.tavg-u-hxy-lnd.day.glb` | ❌ | No surface water scheme in HTESSEL | veg |
| `land.sweLut.tavg-u-hxy-multi.mon.glb` | ❌ | No per-tile variant in LPJ-GUESS (only gridcell total snw_monthly.out) | veg |
| `land.tas.tavg-h2m-hxy-u.1hr.30S-90S` | ❌ | Only 1hr/3hr/day/mon frequency implemented | extra |
| `land.tas.tavg-h2m-hxy-u.1hr.glb` | ❌ | Only 1hr/3hr/day/mon frequency implemented | cap7 |
| `land.tasLut.tavg-h2m-hxy-multi.mon.glb` | ❌ | Atmospheric variable — not per-tile from LPJ-GUESS | veg |
| `land.tran.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `land.tran.tavg-u-hxy-u.3hr.glb` | ❌ | Only mon frequency implemented | veg |
| `land.treeFrac.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 |
| `land.treeFrac.tavg-u-hxy-u.yr.glb` | ✅ |  | veg |
| `land.treeFracBdlDcd.tavg-u-hxy-u.mon.glb` | ✅ |  | veg |
| `land.tsLut.tavg-u-hxy-multi.mon.glb` | ❌ | Soil temperature exists gridcell-only — no per-tile LUT variant | veg |
| `land.tsl.tavg-sl-hxy-lnd.mon.glb` | ❌ | Feasible from tsl_monthly.out (15 depth levels) — pipeline not yet written | cap7 |
| `land.tslsi.tavg-u-hxy-lsi.day.glb` | ❌ | Only 3hr/day frequency implemented | cap7 |
| `land.tslsi.tpt-u-hxy-lsi.3hr.glb` | ✅ |  | veg |
| `land.vegFrac.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 |
| `land.vegHeight.tavg-u-hxy-tree.mon.glb` | ❌ | Only vegHeightTree_monthly.out (tree-only) — no grid-cell mean vegetation height | veg |
| `land.wtd.tavg-u-hxy-lnd.day.glb` | ❌ | No groundwater scheme in HTESSEL | veg |

## Realm: landIce — 3/3 rules

| Variable | Rule | Reason | Categories |
|---|---|---|---|
| `landIce.sbl.tavg-u-hxy-lnd.mon.glb` | ✅ |  | veg |
| `landIce.sbl.tavg-u-hxy-u.day.glb` | ✅ |  | veg |
| `landIce.sbl.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 |

## Realm: landIce land — 9/19 rules

| Variable | Rule | Reason | Categories |
|---|---|---|---|
| `landIce.agesno.tavg-u-hxy-lnd.mon.glb` | ❌ | No interactive ice sheet model | veg |
| `landIce.hfdsn.tavg-u-hxy-lnd.day.glb` | ❌ | Feasible from snow energy balance (lambda*(Tsn-Tsoil)/dz) — pipeline not yet written | veg |
| `landIce.hfdsn.tavg-u-hxy-lnd.mon.glb` | ❌ | Feasible from snow energy balance (lambda*(Tsn-Tsoil)/dz) — pipeline not yet written | veg |
| `landIce.lwsnl.tavg-u-hxy-lnd.day.glb` | ❌ | Single-layer snow scheme in HTESSEL — no liquid water tracking | veg |
| `landIce.lwsnl.tavg-u-hxy-lnd.mon.glb` | ❌ | Single-layer snow scheme in HTESSEL — no liquid water tracking | veg |
| `landIce.mrfso.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 core |
| `landIce.pflw.tavg-u-hxy-lnd.day.glb` | ❌ | No permafrost scheme in HTESSEL | veg |
| `landIce.pflw.tavg-u-hxy-lnd.mon.glb` | ❌ | No permafrost scheme | veg |
| `landIce.snc.tavg-u-hxy-lnd.day.glb` | ✅ |  | cap7 |
| `landIce.snc.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 core |
| `landIce.snd.tavg-u-hxy-lnd.day.glb` | ✅ |  | veg |
| `landIce.snd.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 |
| `landIce.snm.tavg-u-hxy-lnd.day.glb` | ✅ |  | veg |
| `landIce.snw.tavg-u-hxy-lnd.day.glb` | ✅ |  | cap7 |
| `landIce.snw.tavg-u-hxy-lnd.mon.glb` | ✅ |  | cap7 core |
| `landIce.sootsn.tavg-u-hxy-lnd.mon.glb` | ❌ | Requires CAMS aerosol deposition (not available in this config) | veg |
| `landIce.tpf.tavg-u-hxy-lnd.day.glb` | ❌ | No permafrost scheme in HTESSEL | veg |
| `landIce.tpf.tavg-u-hxy-lnd.mon.glb` | ❌ | No permafrost scheme | veg |
| `landIce.tsn.tavg-u-hxy-lnd.day.glb` | ✅ |  | veg |

## Realm: ocean — 93/175 rules

| Variable | Rule | Reason | Categories |
|---|---|---|---|
| `ocean.absscint.tavg-op4-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.agessc.tavg-ol-hxy-sea.mon.glb` | ❌ | Age tracer not enabled in current config (use_age_tracer=.false.) | lrcs |
| `ocean.areacello.ti-u-hxy-u.fx.glb` | ✅ |  | cap7 core |
| `ocean.basin.ti-u-hxy-u.fx.glb` | ✅ |  | cap7 core |
| `ocean.bigthetao.tavg-u-hm-sea.mon.glb` | ❌ | FESOM uses potential temperature, not conservative temperature | lrcs |
| `ocean.bigthetao.tavg-ol-hxy-sea.dec.glb` | ❌ | FESOM uses potential temperature, not conservative temperature | lrcs |
| `ocean.bigthetao.tavg-ol-hxy-sea.mon.glb` | ❌ | FESOM uses potential temperature, not conservative temperature | cap7 core |
| `ocean.chcint.tavg-op4-hxy-sea.mon.glb` | ❌ | FESOM uses potential temperature, not conservative temperature | lrcs |
| `ocean.deptho.ti-u-hxy-sea.fx.glb` | ✅ |  | cap7 core |
| `ocean.difmxybo.tavg-ol-hxy-sea.yr.glb` | ❌ | Biharmonic diffusivity coefficient not output separately in FESOM | lrcs |
| `ocean.difmxylo.tavg-ol-hxy-sea.yr.glb` | ✅ |  | lrcs |
| `ocean.diftrblo.tavg-ol-hxy-sea.yr.glb` | ❌ | Requires fer_gm=.true. (GM diffusivity fer_K not output in current config) | lrcs |
| `ocean.diftrelo.tavg-ol-hxy-sea.yr.glb` | ❌ | Requires fer_gm=.true. (GM diffusivity fer_K not output in current config) | lrcs |
| `ocean.difvho.tavg-ol-hxy-sea.yr.glb` | ✅ |  | lrcs |
| `ocean.difvso.tavg-ol-hxy-sea.yr.glb` | ✅ |  | lrcs |
| `ocean.dispkexyfo.tavg-u-hxy-sea.yr.glb` | ❌ | No KE dissipation diagnostic in FESOM | lrcs |
| `ocean.dxto.ti-u-hxy-u.fx.glb` | ❌ | Unstructured mesh — no dx/dy concept (would need Voronoi edge lengths) | lrcs |
| `ocean.dxuo.ti-u-hxy-u.fx.glb` | ❌ | Unstructured mesh — no staggered u-point grid | lrcs |
| `ocean.dxvo.ti-u-hxy-u.fx.glb` | ❌ | Unstructured mesh — no staggered v-point grid | lrcs |
| `ocean.dyto.ti-u-hxy-u.fx.glb` | ❌ | Unstructured mesh — no dx/dy concept | lrcs |
| `ocean.dyuo.ti-u-hxy-u.fx.glb` | ❌ | Unstructured mesh — no staggered u-point grid | lrcs |
| `ocean.dyvo.ti-u-hxy-u.fx.glb` | ❌ | Unstructured mesh — no staggered v-point grid | lrcs |
| `ocean.evspsbl.tavg-u-hxy-ifs.mon.glb` | ✅ |  | lrcs |
| `ocean.ficeberg.tavg-ol-hxy-sea.mon.glb` | ❌ | No icebergs | cap7 |
| `ocean.ficeberg.tavg-u-hxy-sea.mon.glb` | ❌ | No icebergs | lrcs |
| `ocean.flandice.tavg-u-hxy-sea.mon.glb` | ❌ | Requires use_landice_water=.true. — not confirmed enabled in current config | lrcs |
| `ocean.friver.tavg-u-hxy-sea.mon.glb` | ✅ |  | cap7 |
| `ocean.hfacrossline.tavg-u-ht-sea.mon.glb` | ❌ | No strait diagnostic in FESOM — requires offline line-integral with transect mask | lrcs |
| `ocean.hfbasin.tavg-u-hyb-sea.mon.glb` | ❌ | Requires basin masks — not available for DARS unstructured mesh | cap7 |
| `ocean.hfbasinpadv.tavg-u-hyb-sea.mon.glb` | ❌ | Requires basin masks + GM eddy decomposition | lrcs |
| `ocean.hfbasinpmadv.tavg-u-hyb-sea.mon.glb` | ❌ | Requires basin masks | lrcs |
| `ocean.hfbasinpmdiff.tavg-u-hyb-sea.mon.glb` | ❌ | Requires basin masks | lrcs |
| `ocean.hfbasinpsmadv.tavg-u-hyb-sea.mon.glb` | ❌ | No submesoscale parameterization; requires basin masks | lrcs |
| `ocean.hfds.tavg-u-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.hfevapds.tavg-u-hxy-ifs.mon.glb` | ❌ | Requires atmosphere-side heat flux decomposition (IFS output, not FESOM) | lrcs |
| `ocean.hfgeou.tavg-u-hxy-sea.mon.glb` | ❌ | Not implemented in FESOM 2.7 | lrcs |
| `ocean.hfgeou.ti-u-hxy-sea.fx.glb` | ❌ | FESOM does not include geothermal heating | cap7 core |
| `ocean.hfibthermds.tavg-ol-hxy-sea.mon.glb` | ❌ | Requires use_icebergs=.true. (not enabled) | lrcs |
| `ocean.hfibthermds.tavg-u-hxy-sea.mon.glb` | ❌ | Requires use_icebergs=.true. (not enabled) | lrcs |
| `ocean.hfrainds.tavg-u-hxy-ifs.mon.glb` | ❌ | Requires atmosphere-side heat flux decomposition (IFS output, not FESOM) | lrcs |
| `ocean.hfrunoffds.tavg-ol-hxy-sea.mon.glb` | ❌ | Requires runoff temperature diagnostic, not output by FESOM | lrcs |
| `ocean.hfrunoffds.tavg-u-hxy-sea.mon.glb` | ❌ | Requires runoff temperature diagnostic, not output by FESOM | lrcs |
| `ocean.hfsnthermds.tavg-ol-hxy-sea.mon.glb` | ❌ | No snow thermodynamic heat flux diagnostic in FESOM | lrcs |
| `ocean.hfsnthermds.tavg-u-hxy-sea.mon.glb` | ❌ | No snow thermodynamic heat flux diagnostic in FESOM | lrcs |
| `ocean.hfx.tavg-ol-hxy-sea.mon.glb` | ✅ |  | cap7 |
| `ocean.hfx.tavg-u-hxy-sea.day.glb` | ✅ |  | lrcs |
| `ocean.hfx.tavg-u-hxy-sea.mon.glb` | ✅ |  | cap7 |
| `ocean.hfy.tavg-ol-hxy-sea.mon.glb` | ✅ |  | cap7 |
| `ocean.hfy.tavg-u-hxy-sea.day.glb` | ✅ |  | lrcs |
| `ocean.hfy.tavg-u-hxy-sea.mon.glb` | ✅ |  | cap7 |
| `ocean.htovgyre.tavg-u-hyb-sea.mon.glb` | ❌ | Requires basin masks + gyre/overturning decomposition | lrcs |
| `ocean.htovovrt.tavg-u-hyb-sea.mon.glb` | ❌ | Requires basin masks + gyre/overturning decomposition | lrcs |
| `ocean.masscello.tavg-ol-hxy-sea.dec.glb` | ✅ |  | lrcs |
| `ocean.masscello.tavg-ol-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.masscello.ti-ol-hxy-sea.fx.glb` | ✅ |  | cap7 core |
| `ocean.masso.tavg-u-hm-sea.dec.glb` | ✅ |  | lrcs |
| `ocean.masso.tavg-u-hm-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.mfo.tavg-u-ht-sea.mon.glb` | ❌ | No strait diagnostic in FESOM — requires predefined ocean transect lines (oline dimension) | lrcs |
| `ocean.mlotst.tavg-u-hxy-sea.day.glb` | ✅ |  | lrcs |
| `ocean.mlotst.tavg-u-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.mlotst.tmax-u-hxy-sea.mon.glb` | ❌ | Only day/mon frequency implemented | lrcs |
| `ocean.mlotst.tmin-u-hxy-sea.mon.glb` | ❌ | Only day/mon frequency implemented | lrcs |
| `ocean.mlotstsq.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.msftbarot.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.msftm.tavg-ol-hyb-sea.mon.glb` | ❌ | Only mon frequency implemented | cap7 |
| `ocean.msftm.tavg-rho-hyb-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.msftmmpa.tavg-ol-hyb-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.msftmmpa.tavg-rho-hyb-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.msftmsmpa.tavg-ol-hyb-sea.mon.glb` | ❌ | No submesoscale parameterization output in FESOM | lrcs |
| `ocean.msfty.tavg-ol-ht-sea.mon.glb` | ❌ | Requires structured grid or regridding + basin masks | cap7 |
| `ocean.msfty.tavg-rho-ht-sea.mon.glb` | ❌ | Requires structured grid or regridding + basin masks | lrcs |
| `ocean.msftypa.tavg-ol-ht-sea.mon.glb` | ❌ | Requires structured grid or regridding + basin masks | lrcs |
| `ocean.msftypa.tavg-rho-ht-sea.mon.glb` | ❌ | Requires structured grid or regridding + basin masks | lrcs |
| `ocean.obvfsq.tavg-ol-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.ocontempdiff.tavg-ol-hxy-sea.yr.glb` | ❌ | FESOM uses potential temperature, not conservative temperature | lrcs |
| `ocean.ocontempmint.tavg-u-hxy-sea.yr.glb` | ❌ | FESOM uses potential temperature, not conservative temperature | lrcs |
| `ocean.ocontemppadvect.tavg-ol-hxy-sea.yr.glb` | ❌ | FESOM uses potential temperature, not conservative temperature | lrcs |
| `ocean.ocontemppmdiff.tavg-ol-hxy-sea.yr.glb` | ❌ | FESOM uses potential temperature, not conservative temperature | lrcs |
| `ocean.ocontemppsmadvect.tavg-ol-hxy-sea.yr.glb` | ❌ | FESOM uses potential temperature, not conservative temperature | lrcs |
| `ocean.ocontemprmadvect.tavg-ol-hxy-sea.yr.glb` | ❌ | FESOM uses potential temperature, not conservative temperature | lrcs |
| `ocean.ocontemptend.tavg-ol-hxy-sea.yr.glb` | ❌ | FESOM uses potential temperature, not conservative temperature | lrcs |
| `ocean.opottempdiff.tavg-ol-hxy-sea.yr.glb` | ✅ |  | lrcs |
| `ocean.opottempmint.tavg-u-hxy-sea.yr.glb` | ✅ |  | lrcs |
| `ocean.opottemppadvect.tavg-ol-hxy-sea.yr.glb` | ❌ | Zero field — fer_gm=.false., no GM parameterization active | lrcs |
| `ocean.opottemppmdiff.tavg-ol-hxy-sea.yr.glb` | ❌ | Zero field — fer_gm=.false., no mesoscale diffusion | lrcs |
| `ocean.opottemppsmadvect.tavg-ol-hxy-sea.yr.glb` | ❌ | Zero field — no submesoscale parameterization | lrcs |
| `ocean.opottemprmadvect.tavg-ol-hxy-sea.yr.glb` | ✅ |  | lrcs |
| `ocean.opottemptend.tavg-ol-hxy-sea.dec.glb` | ❌ | Only yr frequency implemented | lrcs |
| `ocean.opottemptend.tavg-ol-hxy-sea.yr.glb` | ✅ |  | lrcs |
| `ocean.osaltdiff.tavg-ol-hxy-sea.yr.glb` | ✅ |  | lrcs |
| `ocean.osaltpadvect.tavg-ol-hxy-sea.yr.glb` | ❌ | Zero field — fer_gm=.false., no GM parameterization active | lrcs |
| `ocean.osaltpmdiff.tavg-ol-hxy-sea.yr.glb` | ❌ | Zero field — fer_gm=.false., no mesoscale diffusion | lrcs |
| `ocean.osaltpsmadvect.tavg-ol-hxy-sea.mon.glb` | ❌ | Zero field — no submesoscale parameterization | lrcs |
| `ocean.osaltpsmadvect.tavg-ol-hxy-sea.yr.glb` | ❌ | Zero field — no submesoscale parameterization | lrcs |
| `ocean.osaltrmadvect.tavg-ol-hxy-sea.yr.glb` | ✅ |  | lrcs |
| `ocean.osalttend.tavg-ol-hxy-sea.yr.glb` | ✅ |  | lrcs |
| `ocean.pbo.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.pfscint.tavg-op4-hxy-sea.mon.glb` | ❌ | No preformed salinity tracer in FESOM | lrcs |
| `ocean.phcint.tavg-op4-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.pso.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.rsdoabsorb.tavg-ol-hxy-sea.yr.glb` | ✅ |  | lrcs |
| `ocean.rsds.tavg-u-hxy-ifs.mon.glb` | ❌ | FESOM swr.fesom is total shortwave (not ice-free only); ice-free masking not yet implemented | lrcs |
| `ocean.rsus.tavg-u-hxy-ifs.mon.glb` | ❌ | FESOM does not output reflected shortwave (rsus) separately | lrcs |
| `ocean.scint.tavg-op4-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.sf6.tavg-ol-hxy-sea.mon.glb` | ❌ | No SF6 tracer in FESOM | cap7 |
| `ocean.sfacrossline.tavg-u-ht-sea.mon.glb` | ❌ | No strait diagnostic in FESOM — requires offline line-integral with transect mask | lrcs |
| `ocean.sfriver.tavg-u-hxy-sea.mon.glb` | ❌ | No river salt flux diagnostic in FESOM2 | lrcs |
| `ocean.sftof.ti-u-hxy-u.fx.glb` | ✅ |  | cap7 core |
| `ocean.sfx.tavg-ol-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.sfx.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.sfy.tavg-ol-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.sfy.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.sltbasin.tavg-u-hyb-sea.mon.glb` | ❌ | Requires basin masks | lrcs |
| `ocean.sltovgyre.tavg-u-hyb-sea.mon.glb` | ❌ | Requires basin masks + gyre/overturning decomposition | lrcs |
| `ocean.sltovovrt.tavg-u-hyb-sea.mon.glb` | ❌ | Requires basin masks + gyre/overturning decomposition | lrcs |
| `ocean.so.tavg-u-hm-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.so.tavg-ol-hxy-sea.dec.glb` | ✅ |  | lrcs |
| `ocean.so.tavg-ol-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.sob.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.somint.tavg-u-hxy-sea.yr.glb` | ✅ |  | lrcs |
| `ocean.sos.tavg-u-hm-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.sos.tavg-u-hxy-sea.day.glb` | ✅ |  | cap7 core |
| `ocean.sos.tavg-u-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.sossq.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.sw17O.tavg-ol-hxy-sea.mon.glb` | ❌ | Water isotopes not enabled (lwiso=.false.) | lrcs |
| `ocean.sw18O.tavg-ol-hxy-sea.mon.glb` | ❌ | Water isotopes not enabled (lwiso=.false.) | lrcs |
| `ocean.sw2H.tavg-ol-hxy-sea.mon.glb` | ❌ | Water isotopes not enabled (lwiso=.false.) | lrcs |
| `ocean.tauuo.tavg-u-hxy-sea.3hr.glb` | ✅ |  | cap7 |
| `ocean.tauuo.tavg-u-hxy-sea.dec.glb` | ✅ |  | lrcs |
| `ocean.tauuo.tavg-u-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.tauvo.tavg-u-hxy-sea.3hr.glb` | ✅ |  | cap7 |
| `ocean.tauvo.tavg-u-hxy-sea.dec.glb` | ✅ |  | lrcs |
| `ocean.tauvo.tavg-u-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.thetao.tavg-u-hm-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.thetao.tavg-ol-hxy-sea.dec.glb` | ✅ |  | lrcs |
| `ocean.thetao.tavg-ol-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.thetao.tavg-op20bar-hxy-sea.day.glb` | ❌ | Only dec/mon frequency implemented | lrcs |
| `ocean.thkcello.tavg-ol-hxy-sea.dec.glb` | ✅ |  | lrcs |
| `ocean.thkcello.tavg-ol-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.thkcello.ti-ol-hxy-sea.fx.glb` | ✅ |  | cap7 core |
| `ocean.thkcelluo.tavg-ol-hxy-sea.mon.glb` | ❌ | Unstructured mesh — no u/v grid distinction in FESOM | lrcs |
| `ocean.thkcellvo.tavg-ol-hxy-sea.mon.glb` | ❌ | Unstructured mesh — no u/v grid distinction in FESOM | lrcs |
| `ocean.tnkebto.tavg-u-hxy-sea.yr.glb` | ❌ | No KE tendency diagnostic in FESOM | lrcs |
| `ocean.tnpeo.tavg-u-hxy-sea.yr.glb` | ❌ | No PE tendency diagnostic in FESOM | lrcs |
| `ocean.tob.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.tos.tavg-u-hm-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.tos.tavg-u-hxy-sea.day.glb` | ✅ |  | cap7 core |
| `ocean.tos.tavg-u-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.tossq.tavg-u-hxy-sea.day.glb` | ✅ |  | cap7 |
| `ocean.tossq.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.umo.tavg-ol-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.uo.tavg-ol-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.uos.tavg-u-hxy-sea.day.glb` | ✅ |  | lrcs |
| `ocean.vmo.tavg-ol-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.vo.tavg-ol-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.volcello.tavg-ol-hxy-sea.dec.glb` | ✅ |  | lrcs |
| `ocean.volcello.tavg-ol-hxy-sea.mon.glb` | ✅ |  | cap7 |
| `ocean.volcello.tavg-ol-hxy-sea.yr.glb` | ❌ | Only dec/fx/mon frequency implemented | lrcs |
| `ocean.volcello.ti-ol-hxy-sea.fx.glb` | ✅ |  | lrcs |
| `ocean.volo.tavg-u-hm-sea.dec.glb` | ✅ |  | lrcs |
| `ocean.volo.tavg-u-hm-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.vos.tavg-u-hxy-sea.day.glb` | ✅ |  | lrcs |
| `ocean.vsf.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.vsfcorr.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.vsfevap.tavg-u-hxy-sea.mon.glb` | ❌ | FESOM does not split virtual salt flux by component | lrcs |
| `ocean.vsfpr.tavg-u-hxy-sea.mon.glb` | ❌ | FESOM does not split virtual salt flux by component | lrcs |
| `ocean.vsfriver.tavg-u-hxy-sea.mon.glb` | ❌ | FESOM does not split virtual salt flux by component | lrcs |
| `ocean.wfcorr.tavg-u-hxy-sea.mon.glb` | ❌ | No water flux correction in standard FESOM2 config | lrcs |
| `ocean.wfo.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.wmo.tavg-ol-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.wo.tavg-ol-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.zos.tavg-u-hxy-sea.day.glb` | ✅ |  | cap7 core |
| `ocean.zos.tavg-u-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `ocean.zossq.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.zostoga.tavg-u-hm-sea.mon.glb` | ✅ |  | cap7 core |

## Realm: ocean seaIce — 3/3 rules

| Variable | Rule | Reason | Categories |
|---|---|---|---|
| `ocean.sfdsi.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.siflfwbot.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `ocean.vsfsit.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |

## Realm: seaIce — 78/114 rules

| Variable | Rule | Reason | Categories |
|---|---|---|---|
| `seaIce.evspsbl.tavg-u-hxy-si.mon.glb` | ✅ |  | cap7 |
| `seaIce.prra.tavg-u-hxy-si.mon.glb` | ✅ |  | cap7 |
| `seaIce.prsn.tavg-u-hxy-si.mon.glb` | ✅ |  | cap7 |
| `seaIce.rlds.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.rlus.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.rsds.tavg-u-hxy-si.day.glb` | ✅ |  | lrcs |
| `seaIce.rsds.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.rsus.tavg-u-hxy-si.day.glb` | ✅ |  | lrcs |
| `seaIce.rsus.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sbl.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sfdsi.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.siage.tavg-u-hxy-si.day.glb` | ❌ | tr_iage=.false., not enabled | lrcs |
| `seaIce.siage.tavg-u-hxy-si.mon.glb` | ❌ | tr_iage=.false., not enabled | lrcs |
| `seaIce.siarea.tavg-u-hm-u.day.nh` | ✅ |  | lrcs |
| `seaIce.siarea.tavg-u-hm-u.day.sh` | ✅ |  | lrcs |
| `seaIce.siarea.tavg-u-hm-u.mon.nh` | ✅ |  | lrcs |
| `seaIce.siarea.tavg-u-hm-u.mon.sh` | ✅ |  | lrcs |
| `seaIce.siareaacrossline.tavg-u-ht-u.mon.glb` | ❌ | No strait diagnostic in FESOM — requires offline line-integral with transect mask | lrcs |
| `seaIce.sicompstren.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.siconc.tavg-u-hxy-u.day.glb` | ✅ |  | cap7 core |
| `seaIce.siconc.tavg-u-hxy-u.mon.glb` | ✅ |  | cap7 core |
| `seaIce.siconca.tavg-u-hxy-u.day.glb` | ✅ |  | lrcs |
| `seaIce.siconca.tavg-u-hxy-u.mon.glb` | ✅ |  | lrcs |
| `seaIce.sidconcdyn.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `seaIce.sidconcth.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `seaIce.sidivvel.tpt-u-hxy-si.mon.glb` | ❌ | Requires divergence computation on unstructured FESOM mesh | lrcs |
| `seaIce.sidmassdyn.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sidmassgrowthbot.tavg-u-hxy-si.mon.glb` | ❌ | No split thermodynamic growth terms in FESOM (only total thermo/dynamic) | lrcs |
| `seaIce.sidmassgrowthsi.tavg-u-hxy-si.mon.glb` | ❌ | No split thermodynamic growth terms in FESOM | lrcs |
| `seaIce.sidmassgrowthwat.tavg-u-hxy-si.mon.glb` | ❌ | No split thermodynamic growth terms in FESOM (no frazil output) | lrcs |
| `seaIce.sidmassmeltbot.tavg-u-hxy-si.mon.glb` | ❌ | No split thermodynamic melt terms in FESOM (only total thermo/dynamic) | lrcs |
| `seaIce.sidmassmeltlat.tavg-u-hxy-si.mon.glb` | ❌ | No split thermodynamic melt terms in FESOM | lrcs |
| `seaIce.sidmassmelttop.tavg-u-hxy-si.mon.glb` | ❌ | No split thermodynamic melt terms in FESOM | lrcs |
| `seaIce.sidmassth.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sidmasstranx.tavg-u-hxy-u.mon.glb` | ✅ |  | lrcs |
| `seaIce.sidmasstrany.tavg-u-hxy-u.mon.glb` | ✅ |  | lrcs |
| `seaIce.sidragbot.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sidragtop.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sieqthick.tavg-u-hxy-si.mon.glb` | ✅ |  | cap7 |
| `seaIce.siextent.tavg-u-hm-u.day.nh` | ✅ |  | lrcs |
| `seaIce.siextent.tavg-u-hm-u.day.sh` | ✅ |  | lrcs |
| `seaIce.siextent.tavg-u-hm-u.mon.nh` | ✅ |  | lrcs |
| `seaIce.siextent.tavg-u-hm-u.mon.sh` | ✅ |  | lrcs |
| `seaIce.sifb.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.siflcondbot.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.siflcondtop.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.siflfwbot.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.siflfwdrain.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sifllattop.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.siflsensbot.tavg-u-hxy-si.mon.glb` | ❌ | Ocean-ice interface sensible heat flux not output by FESOM | lrcs |
| `seaIce.siflsenstop.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.siflswdbot.tavg-u-hxy-si.mon.glb` | ❌ | Shortwave flux transmitted through ice not output by FESOM | lrcs |
| `seaIce.siforcecoriolx.tavg-u-hxy-si.mon.glb` | ❌ | Coriolis force not output by FESOM sea ice dynamics | lrcs |
| `seaIce.siforcecorioly.tavg-u-hxy-si.mon.glb` | ❌ | Coriolis force not output by FESOM sea ice dynamics | lrcs |
| `seaIce.siforceintstrx.tavg-u-hxy-si.mon.glb` | ❌ | Internal stress not output by FESOM sea ice dynamics | lrcs |
| `seaIce.siforceintstry.tavg-u-hxy-si.mon.glb` | ❌ | Internal stress not output by FESOM sea ice dynamics | lrcs |
| `seaIce.siforcetiltx.tavg-u-hxy-si.mon.glb` | ❌ | Sea-surface tilt force not output by FESOM sea ice dynamics | lrcs |
| `seaIce.siforcetilty.tavg-u-hxy-si.mon.glb` | ❌ | Sea-surface tilt force not output by FESOM sea ice dynamics | lrcs |
| `seaIce.sihc.tavg-u-hxy-sea.mon.glb` | ✅ |  | lrcs |
| `seaIce.siitdconc.tavg-u-hxy-si.mon.glb` | ❌ | Single-category ice — no ITD | lrcs |
| `seaIce.siitdsnconc.tavg-u-hxy-si.day.glb` | ❌ | Requires ITD (ice thickness distribution) — FESOM is single-category | veg |
| `seaIce.siitdsnconc.tavg-u-hxy-si.mon.glb` | ❌ | Requires ITD (ice thickness distribution) — FESOM is single-category | lrcs |
| `seaIce.siitdsnthick.tavg-u-hxy-si.day.glb` | ❌ | Requires ITD (ice thickness distribution) — FESOM is single-category | veg |
| `seaIce.siitdsnthick.tavg-u-hxy-si.mon.glb` | ❌ | Single-category ice — no ITD | lrcs |
| `seaIce.siitdthick.tavg-u-hxy-si.mon.glb` | ❌ | Single-category ice — no ITD | lrcs |
| `seaIce.simass.tavg-u-hxy-si.mon.glb` | ✅ |  | cap7 core |
| `seaIce.simassacrossline.tavg-u-ht-u.mon.glb` | ❌ | No strait diagnostic in FESOM — requires offline line-integral with transect mask | lrcs |
| `seaIce.simpconc.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.simpeffconc.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.simprefrozen.tavg-u-hxy-simp.mon.glb` | ✅ |  | lrcs |
| `seaIce.simpthick.tavg-u-hxy-simp.mon.glb` | ✅ |  | lrcs |
| `seaIce.sirdgconc.tavg-u-hxy-si.mon.glb` | ❌ | tr_lvl=.false. — ridging tracer not enabled in FESOM | lrcs |
| `seaIce.sisali.tavg-u-hxy-si.mon.glb` | ❌ | Constant salinity — not a prognostic variable | cap7 |
| `seaIce.sisaltmass.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sishearvel.tpt-u-hxy-si.mon.glb` | ❌ | Requires shear computation on unstructured FESOM mesh | lrcs |
| `seaIce.sisndmassdyn.tavg-u-hxy-si.mon.glb` | ❌ | Snow mass budget terms not output separately by FESOM | lrcs |
| `seaIce.sisndmasssi.tavg-u-hxy-si.mon.glb` | ❌ | Snow mass budget terms not output separately by FESOM | lrcs |
| `seaIce.sisndmasswind.tavg-u-hxy-si.mon.glb` | ❌ | No wind redistribution of snow in FESOM (sisndmasswind not output) | lrcs |
| `seaIce.sisnhc.tavg-u-hxy-si.day.glb` | ✅ |  | veg |
| `seaIce.sisnhc.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sisnmass.tavg-u-hm-si.day.nh` | ✅ |  | lrcs |
| `seaIce.sisnmass.tavg-u-hm-si.day.sh` | ✅ |  | lrcs |
| `seaIce.sisnmass.tavg-u-hm-si.mon.nh` | ✅ |  | lrcs |
| `seaIce.sisnmass.tavg-u-hm-si.mon.sh` | ✅ |  | lrcs |
| `seaIce.sisnmassacrossline.tavg-u-ht-u.mon.glb` | ❌ | No strait diagnostic in FESOM — requires offline line-integral with transect mask | lrcs |
| `seaIce.sispeed.tavg-u-hxy-si.day.glb` | ✅ |  | lrcs |
| `seaIce.sispeed.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sistressave.tpt-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sistressmax.tpt-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sistrxdtop.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sistrxubot.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sistrydtop.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sistryubot.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sitempbot.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.sitempsnic.tavg-u-hxy-si.day.glb` | ❌ | Snow-ice interface temperature: computed internally in FESOM thermodynamic solver, not exposed as output (Tsnice only in icepack driver, which is not active) | veg |
| `seaIce.sitempsnic.tavg-u-hxy-si.mon.glb` | ❌ | Snow-ice interface temperature: computed internally in FESOM thermodynamic solver, not exposed as output (Tsnice only in icepack driver, which is not active) | cap7 |
| `seaIce.sithick.tavg-u-hxy-sir.mon.glb` | ❌ | Ridged ice variant (tavg-u-hxy-sir): tr_lvl=.false. — ridging tracer not enabled in FESOM | lrcs |
| `seaIce.sitimefrac.tavg-u-hxy-sea.day.glb` | ✅ |  | lrcs |
| `seaIce.sitimefrac.tavg-u-hxy-sea.mon.glb` | ✅ |  | cap7 core |
| `seaIce.siu.tavg-u-hxy-si.day.glb` | ✅ |  | cap7 |
| `seaIce.siu.tavg-u-hxy-si.mon.glb` | ✅ |  | cap7 core |
| `seaIce.siv.tavg-u-hxy-si.day.glb` | ✅ |  | cap7 |
| `seaIce.siv.tavg-u-hxy-si.mon.glb` | ✅ |  | cap7 core |
| `seaIce.sivol.tavg-u-hm-u.day.nh` | ✅ |  | lrcs |
| `seaIce.sivol.tavg-u-hm-u.day.sh` | ✅ |  | lrcs |
| `seaIce.sivol.tavg-u-hm-u.mon.nh` | ✅ |  | lrcs |
| `seaIce.sivol.tavg-u-hm-u.mon.sh` | ✅ |  | lrcs |
| `seaIce.snc.tavg-u-hxy-si.mon.glb` | ❌ | Single-category ice — no snow cover fraction | cap7 |
| `seaIce.snd.tavg-u-hxy-sn.day.glb` | ✅ |  | cap7 |
| `seaIce.snd.tavg-u-hxy-sn.mon.glb` | ✅ |  | cap7 core |
| `seaIce.snm.tavg-u-hxy-si.mon.glb` | ✅ |  | lrcs |
| `seaIce.snw.tavg-u-hxy-si.mon.glb` | ✅ |  | cap7 |
| `seaIce.ts.tavg-u-hxy-si.day.glb` | ✅ |  | lrcs |
| `seaIce.ts.tavg-u-hxy-si.mon.glb` | ✅ |  | cap7 core |

## Realm: seaIce ocean — 2/2 rules

| Variable | Rule | Reason | Categories |
|---|---|---|---|
| `seaIce.sithick.tavg-u-hxy-si.day.glb` | ✅ |  | cap7 |
| `seaIce.sithick.tavg-u-hxy-si.mon.glb` | ✅ |  | cap7 core |

