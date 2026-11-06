# CMIP7 LRCS Sea Ice Variables — AWI-ESM3-VEG-HR

Status of LRCS (extra priority) sea ice variables for FESOM 2.7 / AWI-ESM3.
These are lower priority than core variables but still important.

**Key model constraints:**
- FESOM's own sea ice (NOT icepack) — single-category, no ITD
- mEVP rheology (whichevp=1)
- Melt ponds enabled (use_meltponds=.true.)
- No ice age tracer (tr_iage=.false.)
- No ridged ice tracer (tr_lvl=.false.)
- vec_autorotate=.true. for velocity/stress rotation
- ldiag_cmor=.true. for hemisphere-integrated scalars

Sources: cmip7_LRCSextra_variables_seaIce.csv, cmip7_LRCSextra_variables_ocean_seaIce.csv

## SImon — Monthly variables

### Radiation fluxes over sea ice (from atmosphere coupling)
- [x] **rlds** — Downwelling Longwave Flux over Sea Ice (`W m-2`, high) — OpenIFS rlds (atmos_mon_rlds) regridded to FESOM nodes then masked by a_ice (regrid_atm_to_fesom_seaice_mask_pipeline); total LW over all surfaces, not ice-tile-specific
- [x] **rlus** — Upwelling Longwave Flux over Sea Ice (`W m-2`, high) — OpenIFS rlus (atmos_mon_rlus) regridded to FESOM nodes then masked by a_ice (regrid_atm_to_fesom_seaice_mask_pipeline); total LW over all surfaces, not ice-tile-specific
- [x] **rsds** — Downwelling Shortwave Flux over Sea Ice (`W m-2`, high) — OpenIFS rsds (atmos_mon_rsds / atmos_day_cap7_rsds) regridded to FESOM nodes then masked by a_ice (regrid_atm_to_fesom_seaice_mask_pipeline); rsds added to daily _day_cap7 file group in file_def
- [x] **rsus** — Upwelling Shortwave Flux over Sea Ice (`W m-2`, high) — OpenIFS rsus (atmos_mon_rsus / atmos_day_cap7_rsus) regridded to FESOM nodes then masked by a_ice (regrid_atm_to_fesom_seaice_mask_pipeline)

### Heat fluxes
- [x] **siflcondbot** — Net Conductive Heat Flux at Ice Base (`W m-2`, high) — qcon (DefaultPipeline)
- [x] **siflcondtop** — Net Conductive Heat Flux at Ice Surface (`W m-2`, high) — k_ice*(T_base-ist)/h_ice (siflcondtop_pipeline)
- [x] **sifllattop** — Net Latent Heat Flux over Sea Ice (`W m-2`, high) — OpenIFS total slhf (atmos_mon_land_slhf) regridded to FESOM nodes then masked by a_ice (regrid_atm_to_fesom_seaice_mask_pipeline); note: total flux over all surfaces, not ice-tile-specific; verify units (IFS slhf is accumulated J/m²)
- [ ] **siflsensbot** — Net Upward Sensible Heat Flux under Sea Ice (`W m-2`, high) — ocean-ice interface
- [x] **siflsenstop** — Net Downward Sensible Heat Flux over Sea Ice (`W m-2`, high) — OpenIFS total sshf (atmos_mon_land_sshf) regridded to FESOM nodes then masked by a_ice (regrid_atm_to_fesom_seaice_mask_pipeline); note: total flux over all surfaces, not ice-tile-specific; verify units (IFS sshf is accumulated J/m²)
- [ ] **siflswdbot** — Downwelling Shortwave Flux under Sea Ice (`W m-2`, high) — transmitted through ice
- [x] **sihc** — Sea-Ice Heat Content (`J m-2`, high) — rho_ice*h_ice*(c_ice*(T_mean-T_melt)-L_f) (sihc_pipeline)

### Ice/snow heat content
- [x] **sisnhc** — Snow Heat Content (`J m-2`, high) — -rho_snow*L_f*h_snow (sisnhc_pipeline)

### Thermodynamic/dynamic tendencies
- [x] **sidconcdyn** — Area Fraction Tendency from Dynamics (`s-1`, high) — dyngrarea (DefaultPipeline)
- [x] **sidconcth** — Area Fraction Tendency from Thermodynamics (`s-1`, high) — thdgrarea (DefaultPipeline)
- [x] **sidmassdyn** — Mass Change from Dynamics (`kg m-2 s-1`, high) — dyngrice × rho_ice=910 (scale_pipeline)
- [x] **sidmassth** — Mass Change from Thermodynamics (`kg m-2 s-1`, high) — thdgrice × rho_ice=910 (scale_pipeline)
- [ ] **sidmassgrowthbot** — Mass Change Through Basal Growth (`kg m-2 s-1`, high) — NOT AVAILABLE: no split growth terms
- [ ] **sidmassgrowthsi** — Mass Change Through Snow-to-Ice Conversion (`kg m-2 s-1`, high) — NOT AVAILABLE
- [ ] **sidmassgrowthwat** — Mass Change Through Frazil Growth (`kg m-2 s-1`, high) — NOT AVAILABLE
- [ ] **sidmassmeltbot** — Mass Change Through Bottom Melting (`kg m-2 s-1`, high) — NOT AVAILABLE: no split melt terms
- [ ] **sidmassmeltlat** — Mass Change Through Lateral Melting (`kg m-2 s-1`, high) — NOT AVAILABLE
- [ ] **sidmassmelttop** — Mass Change Through Surface Melting (`kg m-2 s-1`, high) — NOT AVAILABLE

### Freshwater and salt fluxes
- [x] **sbl** — Snow Sublimation Rate (`kg m-2 s-1`, high) — OpenIFS total sbl (atmos_mon_land_sbl) regridded to FESOM nodes then masked by a_ice (regrid_atm_to_fesom_seaice_mask_pipeline); note: total sublimation from all surfaces, not ice-tile-specific
- [x] **snm** — Snow Melt Rate (`kg m-2 s-1`, high) — thdgrsn × rho_snow=330 (scale_pipeline)
- [x] **sfdsi** — Salt Flux from Sea Ice (`kg m-2 s-1`, medium) — realsalt (sfdsi_seaice rule, scale_pipeline; same data as ocean.sfdsi, no masking needed as realsalt is zero where no sea ice)
- [x] **siflfwbot** — Freshwater Flux from Sea Ice (`kg m-2 s-1`, medium) — fw_ice × rho_water=1000 (scale_pipeline)
- [x] **siflfwdrain** — Freshwater Flux from Sea-Ice Surface (`kg m-2 s-1`, medium) — fw_snw × rho_water=1000 (scale_pipeline)
- [x] **sisaltmass** — Mass of Salt in Sea Ice (`kg m-2`, high) — m_ice × 0.004 (sice=4 psu, scale_pipeline)

### Stress and force balance
- [x] **sistrxdtop** — X-Component Atmospheric Stress on Ice (`N m-2`, high) — atmice_x (DefaultPipeline)
- [x] **sistrydtop** — Y-Component Atmospheric Stress on Ice (`N m-2`, high) — atmice_y (DefaultPipeline)
- [x] **sistrxubot** — X-Component Ocean Stress on Ice (`N m-2`, high) — iceoce_x (DefaultPipeline)
- [x] **sistryubot** — Y-Component Ocean Stress on Ice (`N m-2`, high) — iceoce_y (DefaultPipeline)
- [ ] **siforcecoriolx** — Coriolis Force X (`N m-2`, medium) — NOT AVAILABLE: not output by FESOM
- [ ] **siforcecorioly** — Coriolis Force Y (`N m-2`, medium) — NOT AVAILABLE
- [ ] **siforceintstrx** — Internal Stress X (`N m-2`, medium) — NOT AVAILABLE
- [ ] **siforceintstry** — Internal Stress Y (`N m-2`, medium) — NOT AVAILABLE
- [ ] **siforcetiltx** — Sea-Surface Tilt X (`N m-2`, medium) — NOT AVAILABLE
- [ ] **siforcetilty** — Sea-Surface Tilt Y (`N m-2`, medium) — NOT AVAILABLE

### Ice dynamics derived
- [x] **sispeed** — Sea-Ice Speed (`m s-1`, high) — sqrt(uice² + vice²) (sispeed_pipeline)
- [ ] **sidivvel** — Divergence of Ice Velocity Field (`s-1`, medium) — NOT AVAILABLE: needs spatial derivatives on unstructured grid
- [ ] **sishearvel** — Maximum Shear of Ice Velocity Field (`s-1`, medium) — NOT AVAILABLE: needs spatial derivatives
- [x] **sidmasstranx** — X-Component Ice Mass Transport (`kg s-1`, medium) — uice × m_ice (ice_mass_transport_pipeline)
- [x] **sidmasstrany** — Y-Component Ice Mass Transport (`kg s-1`, medium) — vice × m_ice (ice_mass_transport_pipeline)

### Mechanical properties
- [x] **sicompstren** — Compressive Sea Ice Strength (`N m-1`, medium) — strength_ice (DefaultPipeline)
- [x] **sistressave** — Average Normal Stress (`N m-1`, high) — (sgm11+sgm22)/2 (sistressave_pipeline)
- [x] **sistressmax** — Maximum Shear Stress (`N m-1`, high) — from sgm11/12/22 (sistressmax_pipeline)

### Other
- [x] **sitempbot** — Temperature at Ice-Ocean Interface (`K`, medium) — T_freeze from SSS (sitempbot_pipeline)
- [x] **sifb** — Sea-Ice Freeboard (`m`, medium) — from h_ice, h_snow, densities (sifb_pipeline)
- [x] **sidragbot** — Ocean Drag Coefficient (`1`, medium) — constant 0.0055 (constant_field_pipeline)
- [x] **sidragtop** — Atmospheric Drag Coefficient (`1`, medium) — constant 0.0012 (cd_atm_ice from namelist.ice — verify against runtime namelist) (constant_field_pipeline)

### Hemisphere-integrated scalars (ldiag_cmor=.true.)
- [x] **siarea (N)** — Sea-Ice Area North (`1e6 km2`, high) — siarean (DefaultPipeline)
- [x] **siarea (S)** — Sea-Ice Area South (`1e6 km2`, high) — siareas (DefaultPipeline)
- [x] **siextent (N)** — Sea-Ice Extent North (`1e6 km2`, high) — siextentn (DefaultPipeline)
- [x] **siextent (S)** — Sea-Ice Extent South (`1e6 km2`, high) — siextents (DefaultPipeline)
- [x] **sivol (N)** — Sea-Ice Volume North (`1e3 km3`, high) — sivoln (DefaultPipeline)
- [x] **sivol (S)** — Sea-Ice Volume South (`1e3 km3`, high) — sivols (DefaultPipeline)
- [x] **sisnmass (N)** — Snow Mass on Sea Ice North (`kg`, high) — m_snow × cell_area, lat≥0 (hemisphere_integral_pipeline); hm-u (sisnmass_north) and hm-si (sisnmass_north_si) variants
- [x] **sisnmass (S)** — Snow Mass on Sea Ice South (`kg`, high) — m_snow × cell_area, lat<0 (hemisphere_integral_pipeline); hm-u (sisnmass_south) and hm-si (sisnmass_south_si) variants

### Melt ponds (use_meltponds=.true.)
- [x] **simpconc** — Melt Pond Fraction (`%`, high) — apnd × 100 (fraction_to_percent_pipeline)
- [x] **simpeffconc** — Effective Melt Pond Fraction (`%`, medium) — apnd*(1-ipnd/hpnd)*100 (simpeffconc_pipeline)
- [x] **simpthick** — Melt Pond Depth (`m`, medium) — hpnd (DefaultPipeline)
- [x] **simprefrozen** — Refrozen Ice on Melt Pond (`m`, medium) — ipnd (DefaultPipeline)

### Strait fluxes
- [ ] **siareaacrossline** — Ice Area Flux Through Straits (`m2 s-1`, high) — NOT AVAILABLE: no strait diagnostics
- [ ] **simassacrossline** — Ice Mass Flux Through Straits (`kg s-1`, high) — NOT AVAILABLE
- [ ] **sisnmassacrossline** — Snow Mass Flux Through Straits (`kg s-1`, high) — NOT AVAILABLE

## NOT AVAILABLE — requires physics not in this configuration

- [ ] **siage** (day/mon) — Ice Age — tr_iage=.false., not enabled
- [ ] **sirdgconc** — Ridged Ice Fraction — tr_lvl=.false., no ridging tracer
- [ ] **sithick (ridged)** — Ridged Ice Thickness — tr_lvl=.false.
- [ ] **siitdconc** — Ice Area by Thickness Category — no ITD (single-category FESOM ice, not icepack)
- [ ] **siitdthick** — Ice Thickness by Category — no ITD
- [ ] **siitdsnconc** — Snow Area by Thickness Category — no ITD
- [ ] **siitdsnthick** — Snow Thickness by Category — no ITD
- [ ] **sisndmassdyn** — Snow Mass Change from Dynamics — not output separately
- [ ] **sisndmasssi** — Snow Mass Change from Snow-to-Ice Conversion — not output separately
- [ ] **sisndmasswind** — Snow Mass Change from Wind Drift — not output (no wind redistribution)

## SIday — Daily variables
- [x] **rsds** — Downwelling Shortwave (`W m-2`, high) — atmos_day_cap7_rsds (rsds added to daily _day_cap7 file group); regrid_atm_to_fesom_seaice_mask_pipeline
- [x] **rsus** — Upwelling Shortwave (`W m-2`, high) — atmos_day_cap7_rsus; regrid_atm_to_fesom_seaice_mask_pipeline
- [ ] **siage** — Ice Age (`s`, high) — NOT AVAILABLE: tr_iage=.false.
- [x] **siconca** (mon+day) — Ice Area on Atm Grid (`%`, high) — ci from OpenIFS remapped by OASIS; monthly: atm_remapped_1m_ci, daily: atm_remapped_1d_cmip7_ci (added to file_def) (fraction_to_percent_pipeline)
- [x] **sispeed** — Ice Speed (`m s-1`, high) — sqrt(uice²+vice²) (sispeed_pipeline, daily uice/vice added to namelist)
- [x] **sitimefrac** — Fraction of Time with Ice (`1`, high) — daily a_ice>0 (more accurate than monthly)
- [x] **ts** — Surface Temperature (`K`, high) — daily ist (added to namelist)
- [x] **siarea (N/S)** — daily hemisphere areas — computed from daily a_ice via hemisphere_integral_pipeline (ldiag_cmor scalars are monthly-only)
- [x] **siextent (N/S)** — daily hemisphere extents — computed from daily a_ice with 15% threshold via hemisphere_integral_pipeline (extent_threshold: 0.15; ldiag_cmor scalars are monthly-only)
- [x] **sivol (N/S)** — daily hemisphere volumes — sivoln/sivols ldiag_cmor scalar files (sivol_north/south_day); if daily scalar output not available, enable daily m_ice and switch to hemisphere_integral_pipeline
- [x] **sisnmass (N/S)** — daily hemisphere snow mass — hemisphere_integral_pipeline (daily m_snow added to namelist); hm-u (sisnmass_north/south_day) and hm-si (sisnmass_north/south_day_si) variants

## SImon — cross-realm (ocean seaIce)
- [x] **sfdsi** — Downward Sea Ice Basal Salt Flux (`kg m-2 s-1`, medium) — realsalt (scale_pipeline, factor needs verification)
- [x] **siflfwbot** — Water Flux into Ocean from Ice Thermodynamics (`kg m-2 s-1`, medium) — fw_ice × 1000 (scale_pipeline)
- [x] **vsfsit** — Virtual Salt Flux from Ice Thermodynamics (`kg m-2 s-1`, medium) — virtsalt (scale_pipeline, factor needs verification)

## Blockers

1. **Radiation fluxes** (rlds, rlus, rsds, rsus): These come from the atmosphere model (OpenIFS), not FESOM. Need separate atmosphere CMORization or coupling interface output.
2. **Split thermodynamic budget** (sidmassgrowthbot/si/wat, sidmassmeltbot/lat/top): FESOM outputs total thermo/dynamic tendency but not individual budget terms.
3. **Force balance terms** (siforcecoriol/intstr/tilt x/y): Not output by FESOM.
4. **ITD variables** (siitdconc/thick/snconc/snthick): No ice thickness distribution — FESOM uses single-category ice (icepack not active).
5. **Ice age** (siage): Tracer not enabled (tr_iage=.false.).
6. **Ridged ice** (sirdgconc, ridged sithick): Tracer not enabled (tr_lvl=.false.).
7. **Strait fluxes** (siareaacrossline, simassacrossline, sisnmassacrossline): No strait diagnostic in FESOM.
8. **Snow budget split** (sisndmassdyn/si/wind): Not output separately.
9. ~~**Melt ponds**~~: RESOLVED — FESOM outputs apnd, hpnd, ipnd when use_meltponds=.true. Added to namelist.io.
10. **Spatial derivatives** (sidivvel, sishearvel): Require computing divergence/shear on unstructured mesh — non-trivial post-processing.
