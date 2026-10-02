# CAP7 Aerosol / AtmosChem — Implementation Status

Source: 5 CSVs in `cap7_aerosol/` (52 variable-frequency entries, unfiltered)

AWI-ESM3-VEG-HR has **no prognostic aerosol** and **no interactive chemistry**.
Tropospheric aerosol forcing is prescribed via **MACv2-SP** (simple plumes),
and ozone is prescribed from climatology. Most variables in this tier are
therefore not producible.

## Summary

| Status | Count |
|--------|-------|
| Already in veg_atm (fire emissions + lwp) | 0 |
| Implemented (new cap7 rules) | 5 |
| Blocked — no prognostic aerosol (MACv2-SP total only) | 20 |
| Blocked — no atmospheric chemistry | 15 |
| Blocked — no CO2 tracer in current config | 4 |
| Blocked — no cloud microphysics diagnostics | 3 |
| Blocked — no deposition scheme | 4 |
| **Total** | **51** |

_Note: od550aer (1 entry) is excluded from total — dropped because MACv2-SP provides only anthropogenic AOD perturbation, not total AOD. Implemented: toz, cfc11, cfc12, ch4 (global mean), n2o (global mean)._

---

## Implemented — new cap7 rules (1)

### MACv2-SP aerosol optical depth

- [ ] **od550aer** (mon) — `aerosol.od550aer.tavg-u-hxy-u.mon.glb` — **Dropped**: `macv2sp_taod550` represents only the anthropogenic simple-plume AOD perturbation, not total AOD as required by CMIP7. Without a natural background aerosol AOD from a prognostic scheme, outputting `macv2sp_taod550` as `od550aer` would be physically incorrect.

### Prescribed ozone (1)

- [x] **toz** (mon) — `aerosol.toz.tavg-u-hxy-u.mon.glb` — Total column ozone. From IFS `tco3` field (ECMWF param 206). Requires adding `tco3` to XIOS monthly output. Unit conversion: `tco3` (kg m-2) → `toz` (m) via scale_factor = 1/2.1415 (dividing by density of O3 at STP). IFS computes tco3 from prescribed ozone climatology — valid CMIP output.

---

## Blocked — no prognostic aerosol (20)

MACv2-SP is a simple-plume parametrization that provides ONLY total column AOD
at 550nm. It does NOT decompose by aerosol species, absorption/scattering, or
size mode. All species-specific and property-specific AOD variables are blocked.

### Species-specific AOD (7)

- [ ] **od550bc** (mon) — `aerosol.od550bc.tavg-u-hxy-u.mon.glb` — Black carbon AOD. No species decomposition in MACv2-SP.
- [ ] **od550dust** (mon) — `aerosol.od550dust.tavg-u-hxy-u.mon.glb` — Dust AOD. No species decomposition.
- [ ] **od550no3** (mon) — `aerosol.od550no3.tavg-u-hxy-u.mon.glb` — Nitrate AOD. No species decomposition.
- [ ] **od550oa** (mon) — `aerosol.od550oa.tavg-u-hxy-u.mon.glb` — Organic aerosol AOD. No species decomposition.
- [ ] **od550so4** (mon) — `aerosol.od550so4.tavg-u-hxy-u.mon.glb` — Sulfate AOD. No species decomposition.
- [ ] **od550ss** (mon) — `aerosol.od550ss.tavg-u-hxy-u.mon.glb` — Sea salt AOD. No species decomposition.
- [ ] **od550bb** (mon) — `aerosol.od550bb.tavg-u-hxy-u.mon.glb` — Biomass burning AOD. No species decomposition.

### AOD property decomposition (2)

- [ ] **abs550aer** (mon) — `aerosol.abs550aer.tavg-u-hxy-u.mon.glb` — Absorption AOD. MACv2-SP provides total only, no absorption/scattering split.
- [ ] **od550lt1aer** (mon) — `aerosol.od550lt1aer.tavg-u-hxy-u.mon.glb` — Fine mode AOD. No size-resolved AOD from MACv2-SP.

### Aerosol concentrations/mixing ratios (11)

These require prognostic aerosol (e.g., CAMS, M7, GOCART):

- [ ] **cdnc** (mon, model levels) — `aerosol.cdnc.tavg-al-hxy-u.mon.glb` — Cloud droplet number concentration. MACv2-SP affects CDNC via Twomey parametrization but IFS doesn't output it as a standard diagnostic.
- [ ] **so2** (mon, model levels) — `aerosol.so2.tavg-al-hxy-u.mon.glb` — SO2 volume mixing ratio. No chemistry.
- [ ] **oh** (mon, model levels) — `aerosol.oh.tavg-al-hxy-u.mon.glb` — OH volume mixing ratio. No chemistry.
- [ ] **hcl** (mon, model levels) — `aerosol.hcl.tavg-al-hxy-u.mon.glb` — HCl volume mixing ratio. No chemistry.
- [ ] **hno3** (mon, model levels) — `aerosol.hno3.tavg-al-hxy-u.mon.glb` — HNO3 volume mixing ratio. No chemistry.
- [ ] **cfc114** (mon, model levels) — `aerosol.cfc114.tavg-al-hxy-u.mon.glb` — CFC114 mole fraction. No chemistry.
- [ ] **hcfc22** (mon, model levels) — `aerosol.hcfc22.tavg-al-hxy-u.mon.glb` — HCFC22 mole fraction on levels. No chemistry.
- [ ] **hfc125** (mon, model levels) — `aerosol.hfc125.tavg-al-hxy-u.mon.glb` — HFC125 mole fraction. No chemistry.
- [ ] **hfc134a** (mon, model levels) — `aerosol.hfc134a.tavg-al-hxy-u.mon.glb` — HFC134a mole fraction. No chemistry.
- [ ] **bry** (mon, plev39) — `aerosol.bry.tavg-p39-hy-air.mon.glb` — Total inorganic bromine. No chemistry.
- [ ] **cly** (mon, plev39) — `aerosol.cly.tavg-p39-hy-air.mon.glb` — Total inorganic chlorine. No chemistry.

---

## Blocked — no atmospheric chemistry (19)

AWI-ESM3-VEG-HR has no interactive chemistry module. Trace gases (CH4, N2O, O3,
CFCs) are prescribed as well-mixed or climatological forcing, not prognostic.
While prescribed values exist, they are forcing inputs, not model output.

### Ozone on levels (3)

- [ ] **o3** (mon, model levels) — `atmosChem.o3.tavg-al-hxy-u.mon.glb` — Ozone mole fraction on model levels. Field `o3` is defined in `field_def_cmip7.xml.j2` but IFS does not send it to XIOS (not in `context_ifs.xml.j2`). Requires IFS source change to expose 3D prescribed ozone array via XIOS. Unit conversion: kg kg-1 → mol mol-1 via ×(M_air/M_O3) = ×0.60354.
- [ ] **o3** (mon, plev19) — `atmosChem.o3.tavg-p19-hxy-air.mon.glb` — Ozone on pressure levels. Same blocker as model-level variant.
- [ ] **o3** (mon, plev19, clim) — `atmosChem.o3.tclm-p19-hxy-air.mon.glb` — Ozone climatology on plev19. Same blocker.

### Methane (5)

- [ ] **ch4** (mon, model levels) — `atmosChem.ch4.tavg-al-hxy-u.mon.glb` — CH4 mole fraction on levels. Prescribed WMGHG.
- [ ] **ch4** (mon, plev19) — `atmosChem.ch4.tavg-p19-hxy-air.mon.glb` — CH4 on plev19. Prescribed WMGHG.
- [x] **ch4** (mon, global mean) — `atmosChem.ch4.tavg-u-hm-air.mon.glb` — Global mean CH4. From input4MIPs `ch4_*_gm_1750-2022.nc` (ppb → mol/mol via ×1e-9, annual → monthly by ffill). CMIP7 guidance for prescribed-concentration runs: report global mean instead of 3D field.
- [ ] **ch4** (mon, plev19, clim) — `atmosChem.ch4.tclm-p19-hxy-air.mon.glb` — CH4 climatology.
- [ ] **ch4** (mon, global mean, clim) — `atmosChem.ch4.tclm-u-hm-air.mon.glb` — CH4 climatological global mean. Implementable from input4MIPs GHG file (12 constant monthly values). CMIP7 processing note: "When calling CMOR, identify this variable as `ch4globalClim`, not `ch4global`" — pycmor handles this automatically via `compound_name` lookup in data request metadata.

### Nitrous oxide (5)

- [ ] **n2o** (mon, model levels) — `atmosChem.n2o.tavg-al-hxy-u.mon.glb` — N2O on model levels. Prescribed WMGHG.
- [ ] **n2o** (mon, plev19) — `atmosChem.n2o.tavg-p19-hxy-air.mon.glb` — N2O on plev19. Prescribed WMGHG.
- [x] **n2o** (mon, global mean) — `atmosChem.n2o.tavg-u-hm-air.mon.glb` — Global mean N2O. From input4MIPs `n2o_*_gm_1750-2022.nc` (ppb → mol/mol via ×1e-9, annual → monthly by ffill). CMIP7 guidance for prescribed-concentration runs: report global mean instead of 3D field.
- [ ] **n2o** (mon, plev19, clim) — `atmosChem.n2o.tclm-p19-hxy-air.mon.glb` — N2O climatology.
- [ ] **n2o** (mon, global mean, clim) — `atmosChem.n2o.tclm-u-hm-air.mon.glb` — N2O climatological global mean. Implementable from input4MIPs GHG file (12 constant monthly values). CMIP7 processing note: "When calling CMOR, identify this variable as `n2oglobalClim`" — pycmor handles this automatically via `compound_name` lookup in data request metadata.

### Other trace gases (6)

- [ ] **dms** (mon, model levels) — `atmosChem.dms.tavg-al-hxy-u.mon.glb` — DMS mole fraction. No chemistry.
- [ ] **noy** (mon, plev39) — `aerosol.noy.tavg-p39-hy-air.mon.glb` — Total reactive nitrogen. No chemistry.
- [x] **cfc11** (mon, global mean) — `atmosChem.cfc11.tavg-u-hm-air.mon.glb` — Global mean CFC11. From input4MIPs `cfc11_*_gm_1750-2022.nc` (ppt → mol/mol via ×1e-12, annual → monthly by ffill).
- [x] **cfc12** (mon, global mean) — `atmosChem.cfc12.tavg-u-hm-air.mon.glb` — Global mean CFC12. From input4MIPs `cfc12_*_gm_1750-2022.nc` (ppt → mol/mol via ×1e-12, annual → monthly by ffill).
- [ ] **cfc113** (mon, global mean) — `atmosChem.cfc113.tavg-u-hm-air.mon.glb` — Global mean CFC113. Prescribed scalar.
- [ ] **hcfc22** (mon, global mean) — `atmosChem.hcfc22.tavg-u-hm-air.mon.glb` — Global mean HCFC22. Prescribed scalar.

---

## Blocked — no CO2 tracer in current config (4)

CO2 output is behind Jinja2 flags (`with_co2_tracer`, `with_co2_oce_coupling`,
`with_co2_veg_coupling`) that default to false. If CO2 coupling is enabled for
production runs, these could become available.

- [ ] **co2** (mon, model levels) — `atmos.co2.tavg-al-hxy-u.mon.glb` — CO2 mole fraction on levels.
- [ ] **co2** (mon, plev19) — `atmos.co2.tavg-p19-hxy-air.mon.glb` — CO2 on plev19.
- [ ] **co2** (mon, plev19, clim) — `atmos.co2.tclm-p19-hxy-air.mon.glb` — CO2 climatology.
- [ ] **co2** (mon, global mean, clim) — `atmos.co2.tclm-u-hm-u.mon.glb` — CO2 climatological global mean.

---

## Blocked — no cloud microphysics diagnostics (4)

Cloud-top effective radius and droplet number require IFS microphysics diagnostics
that are not available as standard XIOS output fields.

- [ ] **reffcclwtop** (day) — `atmos.reffcclwtop.tavg-u-hxy-ccl.day.glb` — Cloud-top effective droplet radius, convective. Not an IFS diagnostic output.
- [ ] **reffsclwtop** (day) — `atmos.reffsclwtop.tavg-u-hxy-scl.day.glb` — Cloud-top effective droplet radius, stratiform. Not an IFS diagnostic output.
- [ ] **scldncl** (day) — `atmos.scldncl.tavg-u-hxy-scl.day.glb` — Cloud droplet number concentration at cloud top. Not an IFS diagnostic output.

---

## Blocked — no deposition scheme (4)

Deposition fluxes require a prognostic aerosol/chemistry scheme with
interactive removal processes. MACv2-SP is diagnostic-only.

- [ ] **drydust** (mon) — `aerosol.drydust.tavg-u-hxy-u.mon.glb` — Dry deposition rate of dust. No prognostic dust.
- [ ] **wetdust** (mon) — `aerosol.wetdust.tavg-u-hxy-u.mon.glb` — Wet deposition rate of dust. No prognostic dust.
- [ ] **drynoy** (mon) — `atmosChem.drynoy.tavg-u-hxy-u.mon.glb` — Dry deposition rate of NOy. No chemistry.
- [ ] **wetnoy** (mon) — `atmosChem.wetnoy.tavg-u-hxy-u.mon.glb` — Wet deposition rate of NOy. No chemistry.

---

## XIOS XML changes required

To enable `toz` output, add `tco3` to the `_1m` monthly surface output file
in `file_def_oifs_cmip7_spinup.xml.j2`. The field definition already exists
in `field_def_cmip7.xml`.
