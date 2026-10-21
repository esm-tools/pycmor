# CAP7 Sea Ice — Implementation Status

Source CSVs (unfiltered): `cmip7_CAP7_variables_seaIce.csv` (19), `cmip7_CAP7_variables_seaIce_ocean.csv` (2).

Total: 21 compound_name entries — 9 already in core/lrcs/veg, 9 new cap7 rules, 3 blocked

## Namelist changes required

The following changes to `namelist.io` enable CAP7 sea ice output:
- `h_ice`: changed from monthly (`'m'`) to daily (`'d'`) — enables daily sithick
- `h_snow`: changed from monthly (`'m'`) to daily (`'d'`) — enables daily snd
- `prec`: uncommented — enables rainfall rate (prra)
- `snow`: uncommented — enables snowfall rate (prsn)

Note: monthly sithick/snd in core_seaice will now receive daily data; the default pipeline's timeavg step resamples to monthly per the compound_name frequency.

---

## Already in core/lrcs/veg (9)

- [x] **siconc** (day) — `seaIce.siconc.tavg-u-hxy-u.day.glb` — core
- [x] **siconc** (mon) — `seaIce.siconc.tavg-u-hxy-u.mon.glb` — core
- [x] **simass** (mon) — `seaIce.simass.tavg-u-hxy-si.mon.glb` — lrcs
- [x] **sithick** (mon) — `seaIce.sithick.tavg-u-hxy-si.mon.glb` — core
- [x] **sitimefrac** (mon) — `seaIce.sitimefrac.tavg-u-hxy-sea.mon.glb` — core
- [x] **siu** (mon) — `seaIce.siu.tavg-u-hxy-si.mon.glb` — core
- [x] **siv** (mon) — `seaIce.siv.tavg-u-hxy-si.mon.glb` — core
- [x] **snd** (mon) — `seaIce.snd.tavg-u-hxy-sn.mon.glb` — core
- [x] **ts** (mon) — `seaIce.ts.tavg-u-hxy-si.mon.glb` — core

---

## Implemented — new cap7 rules (9)

### Daily (4)

- [x] **sithick** (day) — `seaIce.sithick.tavg-u-hxy-si.day.glb` — Sea-ice thickness from `h_ice` (now daily output). Direct passthrough.
- [x] **snd** (day) — `seaIce.snd.tavg-u-hxy-sn.day.glb` — Snow depth on ice from `h_snow` (now daily output). Direct passthrough.
- [x] **siu** (day) — `seaIce.siu.tavg-u-hxy-si.day.glb` — Sea-ice x-velocity from `uice` (already daily).
- [x] **siv** (day) — `seaIce.siv.tavg-u-hxy-si.day.glb` — Sea-ice y-velocity from `vice` (already daily).

### Monthly (5)

- [x] **sieqthick** (mon) — `seaIce.sieqthick.tavg-u-hxy-si.mon.glb` — Sea-ice equivalent thickness (= m_ice, effective ice thickness = volume per area). Direct passthrough from `m_ice` monthly output.
- [x] **snw** (mon) — `seaIce.snw.tavg-u-hxy-si.mon.glb` — Surface snow amount on ice (= m_snow). Input is daily m_snow; timeavg resamples to monthly.
- [x] **evspsbl** (mon) — `seaIce.evspsbl.tavg-u-hxy-si.mon.glb` — Evaporation over sea ice from `evap` (total evap, monthly). SeaIce realm convention: "area: mean where sea_ice" handled by cell_methods metadata. Units: m/s -> kg m-2 s-1 (x 1000).
- [x] **prra** (mon) — `seaIce.prra.tavg-u-hxy-si.mon.glb` — Rainfall rate over sea ice from `prec` (newly enabled). Units: m/s -> kg m-2 s-1 (x 1000).
- [x] **prsn** (mon) — `seaIce.prsn.tavg-u-hxy-si.mon.glb` — Snowfall rate over sea ice from `snow` (newly enabled). Units: m/s -> kg m-2 s-1 (x 1000).

---

## Blocked (3)

- [ ] **sisali** (mon) — `seaIce.sisali.tavg-u-hxy-si.mon.glb` — Sea-Ice Bulk Salinity. FESOM uses **constant ice salinity** (~4 ppt), not a prognostic variable. Not meaningful to output as a field.
- [ ] **sitempsnic** (mon) — `seaIce.sitempsnic.tavg-u-hxy-si.mon.glb` — Temperature at Snow-Ice Interface. FESOM computes this internally in the thermodynamic solver but **does not expose it** as output.
- [ ] **snc** (mon) — `seaIce.snc.tavg-u-hxy-si.mon.glb` — Snow Area Fraction on Sea Ice. **Single-category sea ice** does not resolve partial snow cover on ice — all ice is either fully snow-covered or not. Output would be a binary 0/1 field.
