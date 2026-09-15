# Computing CMIP `hfbasin` from FESOM 2.7 native output — implementation guide

Status: research note, no code written. Audience: whoever picks up the
`compute_hfbasin` rewrite next.

## Why this document exists

The current `examples/custom_steps.py:compute_hfbasin` produces ±60 PW per
(basin, lat) band on AWI-ESM-3-HR Test_06_cli_y1587_v7 output — ~30× the
established Trenberth ±2 PW reference. Diagnostic showed that FESOM's
element-centered velocity integrated as `Σ v_e · dz · elem_dx_e` does not
satisfy mass conservation across a latitude line (~ ±992 Sv per Pacific
equator band vs the physical ~5 Sv). Option D (per-band mass-balance
correction by subtracting band-mean T) only knocked peaks down by 20–50%;
still leaves Pacific equator at +29 PW.

The root cause is a discretisation bug, not a unit / sign / cache issue.
The fix is non-trivial. This document is the prep work to do it right.

## 1. CMIP6 `hfbasin` specification

From `cmip6-cmor-tables/Tables/CMIP6_Omon.json`:

- `standard_name`: `northward_ocean_heat_transport`
- `units`: `W` (not W m⁻², not PW)
- `dimensions`: `latitude basin time`
- `cell_methods`: `longitude: sum (comment: basin sum [along zig-zag grid path]) depth: sum time: mean`
- `comment`: contains contributions from all physical processes affecting
  northward heat transport, including resolved advection, parameterized
  advection, lateral diffusion. Use **Celsius** for temperature scale.

`basin` is a 3-value coord enumerated in `CMIP6_coordinate.json`:
`atlantic_arctic_ocean`, `indian_pacific_ocean`, `global_ocean`.
`latitude` has no `requested` list — AWI-CM-1-1-MR published 1°-centered
bins; that's the de-facto standard.

**Critical**: this is a **cross-section integral**, not a "zonal-mean ×
depth integral". Griffies et al. 2016 (OMIP/CMIP6 protocol,
https://doi.org/10.5194/gmd-9-3231-2016) is explicit: compute on the
native grid along a snapped/zig-zag path approximating the latitude
circle, then sum. **No interpolation to a regular lat-lon grid first.**

## 2. Use `tripyview`, not roll your own

The canonical AWI diagnostic stack is `FESOM/tripyview`
(https://github.com/FESOM/tripyview, MIT-licensed, actively maintained,
last commit 2026-01-15, maintainer Patrick Scholz). It supersedes
`pyfesom2` for hfbasin / MOC. **The work is essentially already done
there** — porting it is far cheaper than reimplementing from scratch.

Relevant functions (verified via GitHub):

- `tripyview/sub_transp.py:calc_mhflx_box_fast(mesh, data, box_list, dlat=1.0, ...)`
  (lines ~238-456) — meridional heat flux from `utemp`/`vtemp` (your
  existing FESOM inputs), basin masking via shapefile `box_list`.
- `tripyview/sub_transp.py:calc_mhflx_box_fast_lessmem(mesh, data, datat, mdiag, ...)`
  (lines ~458-710) — same but lower memory, takes `temp` separately and
  averages to edges. **This is the one to port.**
  - Core constants: `rho0=1030; cp=3850; inPW=1.0e-15`.
  - Integrates along the **edges crossed by each latitude bin** — this
    is exactly Griffies' "zig-zag" path. **The missing piece in the
    current rule, which uses `elem_dx = elem_area/dy_elem` and either
    double-counts or skips edges.**
- `tripyview/sub_transp.py:calc_gmhflx_box(...)` (lines ~1013-1160) —
  alternative path from surface flux convergence (cumulative sum from
  south). Use as a cross-check, not the primary deliverable.
- `tripyview/sub_zmoc.py:calc_zmoc(mesh, data, dlat=1.0, which_moc=...)` —
  bins `w` at nodes per the Sidorenko et al. 2020 conservative algorithm
  (https://doi.org/10.5194/gmd-13-3337-2020).
- `tripyview/sub_dmoc.py:load_dmoc_data + calc_dmoc` — consume
  `U_rho_x_DZ`, `V_rho_x_DZ`, `density_dMOC`, `std_dens_H`. **Does NOT
  compute heat transport.** `density_dMOC` is just the density field
  used to define σ classes; the σ→z remap goes via `std_dens_H`
  cumulative sum. **You cannot shortcut hfbasin from the dMOC
  diagnostics.** Stick with `utemp`/`vtemp` on the native mesh.

### Root cause of the ±60 PW (confirmed from this research)

`elem_dx = elem_area / dy_elem` is an isotropic-element approximation
that fails near coasts and where elements straddle a latitude line.
tripyview finds the actual edges intersected by each lat line and uses
their projected `dx`/`dy`. Mass conservation comes from the
*edge-crossing topology*, not from per-element averaging. Option D
(per-band mean subtraction) papered over the discretisation bug; it
cannot fix it.

## 3. Implementation effort — port-from-tripyview path

| Component | Approx LoC | Notes |
|---|---|---|
| pycmor step `compute_hfbasin_tripyview` calling `calc_mhflx_box_fast_lessmem` over 3 CMIP basins | ~150 | Strip `inPW=1e-15` to keep raw W |
| Basin shapefile fixture/discovery | ~50 | Use `tripyview/shapefiles/` — atl+arctic, indo-pac, global. Don't redraw. |
| Mesh-diagnostics file (`fesom.mesh.diag.nc`) — TCo319/DARS-2 must have edges, edge_dx_lr, nlevels available | ~50 | Verify path in `inherit:` block |
| CMOR attribute writing (basin coord, lat bounds, cell_methods) | ~80 | Follow existing `compute_msftmz` pattern |
| Validation harness vs RAPID + Trenberth + ESGF AWI-CM-1-1-MR | ~100 | One-shot test, not in production pipeline |
| **Total** | **~430** | ~1 week impl, ~1 week validation |

The roll-your-own Bryan/Hall decomposition (no tripyview dependency) is
roughly 700-1000 LoC and 3 weeks. **Not recommended** unless tripyview's
basin definitions don't match CMIP6 — they do (AWI uses them for
AWI-CM-1-1-MR CMIP6 already).

## 4. Bryan/Hall decomposition — for validation, not the deliverable

If something looks off after porting, decompose the answer to diagnose:

- `HT_total = HT_overturning + HT_azonal_gyre`
- `HT_OV(y) = ρ·cp · ∫_z v̄(y,z) T̄(y,z) dz`, where `v̄`, `T̄` are zonal
  means at fixed (y, z) within basin
- `HT_AZ(y) = ρ·cp · ∫_x ∫_z (v − v̄)(T − T̄) dz dx`

CDFTOOLS' `cdfmhst.f90`
(https://github.com/meom-group/CDFTOOLS/blob/master/src/cdfmhst.f90)
does NOT decompose — it integrates `v·T` directly. Useful structural
reference (`cn_fbasins`, ρ=1000, cp=4000) but won't explain why the
FESOM result blows up.

## 5. Validation targets

| Source | Expected basin peak |
|---|---|
| RAPID 26.5°N (2004–2020 mean, https://royalsocietypublishing.org/doi/10.1098/rsta.2022.0188) | Atlantic 1.20 ± 0.12 PW |
| Trenberth & Caron 2001 (https://doi.org/10.1175/1520-0442(2001)014<3433:EOMAAO>2.0.CO;2) | Global peak ~2 PW at 15–20°N; Atlantic ~1.2 PW at 25°N |
| Ganachaud & Wunsch 2000 | WOCE-derived basin OHT estimates |
| ESGF AWI-CM-1-1-MR `Omon.hfbasin` piControl (https://www.wdc-climate.de/ui/cmip6?input=CMIP6.CMIP.AWI.AWI-CM-1-1-MR) | Use as a reference shape / sanity check |

Acceptance tolerance: **±20% on basin peaks** is reasonable for a first
delivery. HR FESOM eddy term will differ from CMIP6 LR.

## 6. Open questions / risks

1. **Sub-monthly eddy term.** `utemp`/`vtemp` are FESOM's online product
   `UV·T̄_3nodes/3` averaged over the output window (verified in
   `fesom-2.7/src/gen_modules_diag.F90:383-391`). Monthly `utemp` already
   contains sub-monthly correlation; you do NOT need higher-frequency
   `u`, `T`. Good.
2. **Basin boundaries.** `tripyview/shapefiles/` follow CMIP convention
   (Bering closed for Atlantic, Indonesian throughflow attributed to
   Indo-Pacific, Arctic in Atlantic basin). Spot-check against the
   AWI-CM CMIP6 publication before shipping.
3. **GM/Redi.** FESOM 2.7 with `Fer_GM=.true.` writes `tuv` *including*
   `fer_UV` bolus (lines 383-384). Confirm whether the Test_06 run had
   `Fer_GM` on; if so, the diffusive contribution to `hfbasin` is
   already in `vtemp` — no extra term needed.

## 7. Same fix applies to siblings

The current `compute_sltbasin`, `compute_hfx`, `compute_hfy`,
`compute_sltx`, `compute_slty` use the same per-element approximation
and have the same discretisation bug. Magnitudes for unintegrated `hfx`
/ `hfy` are less catastrophic (no cancellation expected) but the
diagnostics are not CMIP-conformant. Port them in the same pass.

## Sources

- [CMIP6 Omon table — `hfbasin` entry](https://github.com/PCMDI/cmip6-cmor-tables/blob/main/Tables/CMIP6_Omon.json)
  (local: `cmip6-cmor-tables/Tables/CMIP6_Omon.json`)
- [Griffies et al. 2016, OMIP/CMIP6 protocol — "zig-zag" definition](https://doi.org/10.5194/gmd-9-3231-2016)
- [Sidorenko et al. 2020 — conservative MOC on unstructured FESOM mesh](https://doi.org/10.5194/gmd-13-3337-2020)
- [FESOM/tripyview](https://github.com/FESOM/tripyview) — active, MIT, last commit 2026-01-15
- [FESOM/pyfesom2 `xmoc_data`](https://github.com/FESOM/pyfesom2/blob/main/pyfesom2/diagnostics.py#L376) — MOC only, no hfbasin
- [CDFTOOLS `cdfmhst.f90`](https://github.com/meom-group/CDFTOOLS/blob/master/src/cdfmhst.f90) — NEMO reference
- [RAPID 26.5°N, 1.20±0.12 PW (2004–2020)](https://royalsocietypublishing.org/doi/10.1098/rsta.2022.0188)
- [Trenberth & Caron 2001](https://doi.org/10.1175/1520-0442(2001)014%3C3433:EOMAAO%3E2.0.CO;2)
- FESOM 2.7 `gen_modules_diag.F90:383-391` (verified locally):
  `tuv(1,nz,elem) = (UV(1,nz,elem) + fer_UV(1,nz,elem)) * sum(temp(nz,elnodes))/3`
- AWI-CM-1-1-MR CMIP6 archive: https://www.wdc-climate.de/ui/cmip6?input=CMIP6.CMIP.AWI.AWI-CM-1-1-MR

## Bottom line

The 30× error in the current rule is a **discretisation bug** in
`examples/custom_steps.py:compute_hfbasin` (lines 3864+) — `elem_dx =
elem_area / dy_elem` is not the path-integral discretisation Griffies /
CMIP6 require. `tripyview.sub_transp.calc_mhflx_box_fast_lessmem` already
implements the correct edge-crossing integration on FESOM 2.x and is in
active use at AWI for CMIP6 / CMIP7 production. **Port that, don't
rewrite from scratch.** ~430 LoC, ~1 week + 1 week validation.

Until ported, treat current `hfbasin` / `hfx` / `hfy` / `sltbasin` /
`sltx` / `slty` outputs as **known-broken WIP** in the sanity-check
walker — don't loosen the bounds, the bounds are right, the data is
wrong.
