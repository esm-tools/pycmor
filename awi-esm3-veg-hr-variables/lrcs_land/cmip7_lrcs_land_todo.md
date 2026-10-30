# CMIP7 LRCS Land Variables — TODO

Variables deferred from core_land that cannot be produced from IFS/OIFS output alone.
These require LPJ-GUESS dynamic vegetation output or external datasets.

## From core_land CSVs (deferred)

### From LPJ-GUESS output (now implemented)

- [x] **evspsblsoi** — Water Evaporation from Soil (`kg m-2 s-1`, Lmon)
  - compound_name: `land.evspsblsoi.tavg-u-hxy-lnd.mon.glb`
  - From LPJ-GUESS `evspsblsoi_monthly.out` (Jan..Dec format)
  - IFS `e` is total evaporation (not partitioned), but LPJ-GUESS provides soil evaporation separately

- [x] **evspsblveg** — Evaporation from Canopy (`kg m-2 s-1`, Lmon)
  - compound_name: `land.evspsblveg.tavg-u-hxy-lnd.mon.glb`
  - From LPJ-GUESS `evspsblveg_monthly.out` (Jan..Dec format)
  - LPJ-GUESS provides canopy evaporation (interception loss)

- [x] **rootd** — Maximum Root Depth (`m`, fx)
  - compound_name: `land.rootd.ti-u-hxy-lnd.fx.glb`
  - Derived from IFS `tvl`/`tvh` vegetation type fields + HTESSEL Zeng et al. (1998) root depth lookup
  - Vegetation-type-weighted root depth: `rootd = cvl * rootd(tvl) + cvh * rootd(tvh)`

### Derived from IFS static fields (now implemented)

- [x] **mrsofc** — Capacity of Soil to Store Water / Field Capacity (`kg m-2`, fx)
  - compound_name: `land.mrsofc.ti-u-hxy-lnd.fx.glb`
  - Derived from IFS `slt` (soil type) + HTESSEL Van Genuchten field capacity lookup
  - `mrsofc = theta_fc(slt) * 2.89m * 1000 kg/m3`

- [x] **sftgif** — Land Ice Area Percentage (`%`, fx)
  - compound_name: `land.sftgif.ti-u-hxy-u.fx.glb`
  - Derived from IFS vegetation type 12 = "Ice Caps and Glaciers"
  - `sftgif = (cvl * (tvl==12) + cvh * (tvh==12)) * 100`

- [x] **mrfso** — Soil Frozen Water Content (`kg m-2`, LImon)
  - compound_name: `landIce.mrfso.tavg-u-hxy-lnd.mon.glb`
  - From LPJ-GUESS `mrfso_monthly.out` (Jan..Dec format)
  - LPJ-GUESS tracks frozen soil water content directly

## Additional LRCS-specific land variables

(To be populated when LRCS land CSV is available)
