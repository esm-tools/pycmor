# CMIP7 Core Ocean Variables — Rule Implementation TODO

Variables from `cmip7_all_core_variables_ocean.csv` that need pycmor rules for AWI-ESM3.
Rules file: `cmip7_awiesm3-veg-hr_ocean.yaml`

## Monthly 2D (Omon, surface/integrated)

- [x] **tos** — Sea Surface Temperature (`degC`, Omon) *(implemented + tested)*
- [x] **sos** — Sea Surface Salinity (`1E-03`, Omon) *(rule written, uses sss.fesom)*
- [x] **zos** — Sea Surface Height Above Geoid (`m`, Omon) *(rule written, uses ssh.fesom)*
- [x] **hfds** — Downward Heat Flux at Sea Water Surface (`W m-2`, Omon) *(rule written, uses fh.fesom)*
- [x] **mlotst** — Ocean Mixed Layer Thickness by Sigma T (`m`, Omon) *(rule written, uses MLD3.fesom)*
- [x] **absscint** — Depth-integrated salinity (`kg m-2`, Omon) *(implemented + tested)*
- [x] **tauuo** — Sea Water Surface Downward X Stress (`N m-2`, Omon) *(rule written, uses tx_sur.fesom, elem grid)*
- [x] **tauvo** — Sea Water Surface Downward Y Stress (`N m-2`, Omon) *(rule written, uses ty_sur.fesom, elem grid)*
- [x] **zostoga** — Global Average Thermosteric Sea Level Change (`m`, Omon) *(rule + zostoga_pipeline, gsw/TEOS-10)*

## Monthly 3D (Omon, with olevel)

- [x] **thetao** — Sea Water Potential Temperature (`degC`, Omon, 3D) *(rule written, uses temp.fesom)*
- [x] **so** — Sea Water Salinity (`1E-03`, Omon, 3D) *(rule written, uses salt.fesom)*
- [x] **wo** — Sea Water Vertical Velocity (`m s-1`, Omon, 3D) *(rule written, uses w.fesom, nz=57 interfaces)*
- [~] **bigthetao** — SKIPPED: FESOM2 uses potential temp, not conservative
- [x] **uo** — Sea Water X Velocity (`m s-1`, Omon, 3D) *(rule written, uses unod.fesom, needs vec_autorotate)*
- [x] **vo** — Sea Water Y Velocity (`m s-1`, Omon, 3D) *(rule written, uses vnod.fesom, needs vec_autorotate)*
- [x] **thkcello** — Ocean Model Cell Thickness (`m`, Omon, time-varying) *(rule written, uses hnode.fesom, needs model re-run)*
- [x] **masscello** — Ocean Grid-Cell Mass per Area (`kg m-2`, Omon, time-varying) — rho_0 × hnode via scale_pipeline
- [x] **umo** — Ocean Mass X Transport (`kg s-1`, Omon, 3D) *(rule + mass_transport_pipeline, Boussinesq approx)*
- [x] **vmo** — Ocean Mass Y Transport (`kg s-1`, Omon, 3D) *(rule + mass_transport_pipeline)*
- [x] **wmo** — Upward Ocean Mass Transport (`kg s-1`, Omon, 3D) *(rule + mass_transport_pipeline)*

## Fixed frequency (Ofx) — fx pipelines built

- [x] **areacello** — Grid-Cell Area (`m2`, Ofx) *(rule + fx_extract_pipeline, reads mesh.nc cell_area)*
- [x] **deptho** — Sea Floor Depth Below Geoid (`m`, Ofx) *(rule + fx_deptho_pipeline)*
- [x] **sftof** — Sea Area Percentage (`%`, Ofx) *(rule + fx_sftof_pipeline)*
- [x] **thkcello** — Ocean Model Cell Thickness (`m`, Ofx, static) *(rule + fx_thkcello_pipeline)*
- [x] **masscello** — Ocean Grid-Cell Mass per Area (`kg m-2`, Ofx, static) *(rule + fx_masscello_pipeline)*
- [x] **basin** — Region Selection Index (`1`, Ofx) — Implemented via `basin_pipeline` using external basin mask file
- [~] **hfgeou** — Upward Geothermal Heat Flux (`W m-2`, Ofx) — NOT POSSIBLE: FESOM does not include geothermal heating

## Daily (Oday) — namelist.io updated to daily frequency

- [x] **tos** — Sea Surface Temperature (`degC`, Oday) — rule written (tos_day, DefaultPipeline)
- [x] **sos** — Sea Surface Salinity (`1E-03`, Oday) — rule written (sos_day, DefaultPipeline)
- [x] **zos** — Sea Surface Height Above Geoid (`m`, Oday) — rule written (zos_day, DefaultPipeline)

## Blockers (namelist.io updated, model re-run needed)

1. ~~**vec_autorotate=.false.**~~ → FIXED in namelist.io, set to .true.
2. **elem vs nod2 grid** → tauuo/tauvo still on elem grid, may need interpolation step
3. ~~**No daily output**~~ → FIXED: daily sst/sss/ssh added to namelist.io
4. ~~**hnode not enabled**~~ → FIXED: hnode added to namelist.io
5. **No mass transport** → umo/vmo/wmo not output by FESOM, need post-processing pipeline

## Research findings

- FESOM2 uses **potential temperature** (not conservative) → bigthetao not applicable
- **MLD3** (Griffies 2016, sigma_t=0.03) is the CMIP-compliant mlotst definition
- mesh.nc contains: cell_area, depth[57], depth_bnds[58], depth_lev per cell
- Vertical dims: nz1=56 (midpoints, tracers+horiz vel), nz=57 (interfaces, w only)
- u/v on elem (6.2M), tracers/unod/vnod on nod2 (3.1M)
