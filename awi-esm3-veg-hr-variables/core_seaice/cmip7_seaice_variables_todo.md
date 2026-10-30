# CMIP7 Core Sea Ice Variables — Rule Implementation TODO

Variables from `cmip7_all_core_variables_seaIce.csv` and `cmip7_all_core_variables_seaIce_ocean.csv`.

Two CSVs because CMIP7 data request splits by modeling_realm:
- `seaIce.csv` — realm=seaIce (pure sea-ice diagnostics)
- `seaIce_ocean.csv` — realm="seaIce ocean" (cross-realm, just sithick)
Both come from FESOM sea-ice output and are handled the same way.

## Monthly (SImon)

- [x] **siconc** — Sea-Ice Area Percentage (`%`, SImon) — from a_ice.fesom via siconc_pipeline (fraction_to_percent)
- [x] **simass** — Sea-Ice Mass (`kg m-2`, SImon) — from m_ice.fesom (DefaultPipeline)
- [x] **sithick** — Sea-Ice Thickness (`m`, SImon, cross-realm seaIce+ocean) — from h_ice.fesom (added to namelist.io, DefaultPipeline)
- [x] **sitimefrac** — Fraction of Time with Sea Ice (`1`, SImon) — from a_ice.fesom via sitimefrac_pipeline (binary siconc>0)
- [x] **siu** — Sea-Ice X Velocity (`m s-1`, SImon) — from uice.fesom (vec_autorotate=.true., DefaultPipeline)
- [x] **siv** — Sea-Ice Y Velocity (`m s-1`, SImon) — from vice.fesom (vec_autorotate=.true., DefaultPipeline)
- [x] **snd** — Snow Thickness on Sea Ice (`m`, SImon) — from h_snow.fesom (added to namelist.io, DefaultPipeline)
- [x] **ts** — Surface Temperature of Sea Ice (`K`, SImon) — from ist.fesom (available with __oifs, added to namelist.io, DefaultPipeline)

## Daily (SIday)

- [x] **siconc** — Sea-Ice Area Percentage (`%`, SIday) — daily a_ice in namelist.io, rule written (siconc_pipeline)

## Available FESOM output (monthly)

| File | Variable | Description |
|------|----------|-------------|
| a_ice.fesom.1350.nc | a_ice | ice concentration (fraction, 0-1) |
| m_ice.fesom.1350.nc | m_ice | ice mass per unit area (kg/m2) |
| m_snow.fesom.1350.nc | m_snow | snow mass per unit area (kg/m2) |
| uice.fesom.1350.nc | uice | ice velocity x (m/s) |
| vice.fesom.1350.nc | vice | ice velocity y (m/s) |

## Blockers

1. ~~**siconc units**~~: RESOLVED — fraction_to_percent step in siconc_pipeline
2. ~~**siu/siv rotation**~~: RESOLVED — vec_autorotate=.true. set in namelist.io
3. ~~**sithick**~~: RESOLVED — h_ice added to namelist.io (direct output, no computation needed)
4. ~~**snd**~~: RESOLVED — h_snow added to namelist.io (direct output, no computation needed)
5. ~~**ts**~~: RESOLVED — ist available via __oifs flag, added to namelist.io
6. ~~**sitimefrac**~~: RESOLVED — compute_sitimefrac step (binary siconc>0 from monthly data, approximation)
7. **Daily siconc**: Needs daily `a_ice` entry in namelist.io (added, but needs model re-run to produce output)

## Research findings

- a_ice is ice concentration as fraction (0-1), not percentage
- m_ice is ice mass per unit area (kg/m2), equivalent to simass
- FESOM namelist catalog has `h_ice` and `h_snow` available but not enabled
- `ist` (ice surface temp in K) available under OIFS interface (needs __oifs flag)
- uice/vice subject to same vec_autorotate as ocean velocities
