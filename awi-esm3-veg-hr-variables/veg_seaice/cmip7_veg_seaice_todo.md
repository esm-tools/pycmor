# CMIP7 VEG Sea Ice Variables -- Rule Implementation TODO

Variables from 1 CSV in `veg_seaice/`: 4 total rows (all SIday).

Model constraints:
- Sea ice: FESOM 2.6 built-in single-category (no ITD / ice thickness distribution)
- No iceband dimension available (single category = 1 "band")
- Snow on ice: single-layer, no snow-ice interface temperature tracked
- Output via FESOM namelist.io on unstructured mesh

---

## Daily sea ice variables

### NOT producible (require ice thickness distribution)

- ~~**siitdsnconc** (SIday)~~ -- Snow Area Fraction by Ice Thickness Category (`%`) -- requires ITD / iceband dimension
- ~~**siitdsnthick** (SIday)~~ -- Snow Thickness by Ice Thickness Category (`m`) -- requires ITD / iceband dimension

### Producible

- [x] **sisnhc** (SIday) -- Snow Heat Content over Sea Ice (`J m-2`) -- derived from daily `m_snow` and `a_ice`: `h_snow = m_snow / a_ice`, then `sisnhc = -rho_snow * L_f * h_snow` (same formula as monthly lrcs_seaice, but from daily fields)

### NOT producible (missing physics)

- ~~**sitempsnic** (SIday)~~ -- Temperature at Snow-Ice Interface (`K`) -- FESOM single-category ice only tracks surface temperature (`ice_temp`/`ist`), not the snow-ice boundary. `Tsnice` exists only in the icepack driver which is not active.

---

## Summary

| Category | Count | Done | Blocked |
|----------|-------|------|---------|
| ITD variables | 2 | 0 | 2 (no ITD) |
| Snow heat content | 1 | 1 | 0 |
| Snow-ice interface temp | 1 | 0 | 1 (no physics) |
| **Total** | **4** | **1** | **3** |

## Implementation status

1 producible variable implemented:
- pycmor YAML rule in `cmip7_awiesm3-veg-hr_seaice.yaml`
- Custom step `compute_sisnhc_from_msnow` derives h_snow from daily m_snow/a_ice
- Reuses existing `compute_sisnhc` for the heat content calculation
