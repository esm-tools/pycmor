# CMIP7 AWI-ESM3-VEG-HR — Output Sanity-Check Reference Table

Expected value ranges for every variable produced across the 17 rule yamls in
`awi-esm3-veg-hr-variables/`, for pre-industrial control (piControl, ~1850)
conditions.

**Values are reference estimates from published CMIP6/CMIP5 literature,
standard climatologies (ERA5, WOA, GPCP, CERES-EBAF, HadCRUT, HadISST, LUH2,
NSIDC, PIOMAS, MODIS, Friedlingstein Global Carbon Budget, IPCC AR6), and
physical reasoning.** They were derived without reading any of this system's
output files, so they can be used as an independent sanity check.

- **Expected Min / Max**: plausible minimum/maximum grid-cell value globally.
- **Expected Mean**: area-weighted global annual mean.
- **Units**: exactly as produced by the pipeline (no conversions applied).
- piControl-specific zeros applied to all anthropogenic LUC/harvest/product
  variables and to drift-sensitive quantities.

| Variable | Realm | Units | Expected Min | Expected Mean | Expected Max | Source/Rationale |
|---|---|---|---|---|---|---|
| absscint | ocean | kg m-2 | 0 | ~1.4e5 | ~1.6e5 | rho*S*H ≈ 1025*35e-3*4000m; Locarnini 2018 (WOA18) |
| areacella | atmos | m2 | ~4e6 | ~1e10 | ~1.5e10 | Cell edge 2-120 km (HR to LR); 5.1e14 m2 / Ngrid |
| areacello | ocean | m2 | ~1e7 | ~4e10 | ~6e10 | Ocean grid cell; FESOM unstructured varies with resolution |
| areacellr | land | m2 | ~1e7 | ~5e10 | ~6e10 | River grid cell, order 1deg |
| baresoilFrac | land | % | 0 | ~10 | 100 | Sahara/Antarctica ~100%; global land ~10-15% bare (CMIP6 LUH2) |
| basin | ocean | 1 | 0 | - | ~10 | Integer region index; basin masks IPCC AR6 |
| bldep | atmos | m | ~50 | ~600 | ~3000 | PBL height; ERA5 climatology, deepest over subtropical deserts |
| burntFractionAll | land | % | 0 | ~1 | ~30 | GFED4 climatology, savanna fire belt |
| c3PftFrac | land | % | 0 | ~25 | 100 | LUH2/CMIP6 PFT distribution |
| c4PftFrac | land | % | 0 | ~5 | 100 | LUH2 C4 grasses tropical |
| cfc11 | atmosChem | 1E-12 | 0 | ~0 | ~0 | piControl 1850: zero anthropogenic CFC; CMIP6 forcing dataset (Meinshausen 2017) |
| cfc12 | atmosChem | 1E-12 | 0 | ~0 | ~0 | piControl 1850: zero anthropogenic CFC; CMIP6 forcing dataset (Meinshausen 2017) |
| ch4 | atmosChem | mol mol-1 | ~1e-7 | ~7.22e-7 | ~9e-7 | Pre-industrial CH4 ~722 ppb; ice cores Etheridge 1998; strat depleted to ~150 ppb |
| ci | atmos | 1 | 0 | ~0.1 | 1 | Convection fraction; ITCZ higher |
| cl | atmos | % | 0 | ~5 | 100 | Cloud area fraction PER atm layer; volume-averaged over all model levels is small (~5%) because most levels are cloud-free. Column-total ~65% is clt, not cl. |
| cLand | land | kg m-2 | 0 | ~25 | ~80 | Total land C ~2000 PgC / land area; IPCC AR6 carbon cycle |
| cLeaf | land | kg m-2 | 0 | ~0.3 | ~2 | Leaf C; tropical forest LAI; TRENDY |
| cli | atmos | kg kg-1 | 0 | ~1e-6 | ~1e-3 | Cloud ice mixing ratio; ERA5/CMIP |
| cLitter | land | kg m-2 | 0 | ~2 | ~80 | Total litter (surface + sub-surface + CWD). 32 km cells in W. Siberia / Hudson Bay / Indonesia peatlands approach Lavoie 2021 organic-horizon mean (22-66 kg C/m2); add CWD fraction -> ~80 ceiling |
| cLitterCwd | land | kg m-2 | 0 | ~1 | ~15 | CWD; tropical old-growth plot stocks 4-10 kg C/m2 (Pfeifer 2015); Pan 2011 global ~1.8 |
| cLitterLut | land | kg m-2 | 0 | ~2 | ~80 | Per-tile litter; tracks cLitter |
| cLitterSubSurf | land | kg m-2 | 0 | ~1 | ~30 | Belowground litter; fine-root + buried duff in cold/saturated cells |
| cLitterSurf | land | kg m-2 | 0 | ~1 | ~70 | Aboveground/surface litter. 32 km peatland-dominant cells (Hudson Bay Lowlands, W. Siberia) approach Lavoie 2021 organic-horizon range (22-66 kg C/m2) |
| clivi | atmos | kg m-2 | 0 | ~0.02 | ~1 | Ice water path; CloudSat/CERES (mon-cadence default) |
| clivi_day | atmos | kg m-2 | 0 | ~0.02 | ~10 | Deep-convective anvil IWP (Tian 2018 JGR Atmos) |
| clt | atmos | % | 0 | ~66 | 100 | ISCCP global mean cloud cover ~66% |
| clw | atmos | kg kg-1 | 0 | ~1e-5 | ~2e-3 | Cloud liquid mixing ratio; ERA5 |
| clwvi | atmos | kg m-2 | 0 | ~0.1 | ~2 | Condensed water path; CERES/CloudSat (mon-cadence default) |
| clwvi_day | atmos | kg m-2 | 0 | ~0.1 | ~5 | RSS microwave LWP climatology (daily extreme) |
| cnc | land | % | 0 | ~70 | 100 | Canopy covered area fraction; LUH2/MODIS vegetated cover, ~70% of land |
| cOther | land | kg m-2 | 0 | ~0.2 | ~3 | Reproductive/other tissues small fraction |
| cProduct | land | kg m-2 | 0 | ~0 | ~0 | piControl has no land-use products; ~0 |
| cProductLut | land | kg m-2 | 0 | ~0 | ~0 | piControl LUC products ~0 |
| cRoot | land | kg m-2 | 0 | ~1 | ~15 | Root C; Jackson 1997 global root distributions; tropical 32 km cells approach 10-15 |
| cropFrac | land | % | 0 | ~5 | 100 | 1850 cropland ~5% global land; LUH2 |
| cropFracC3 | land | % | 0 | ~4 | 100 | LUH2 1850: most cropland is C3 (wheat/oats/rice/barley); ~80% of cropFrac globally |
| cropFracC4 | land | % | 0 | ~1 | 100 | LUH2 1850: minor C4 share (maize/sorghum/millet) ~20% of cropFrac globally |
| cSoil | land | kg m-2 | 0 | ~15 | ~200 | Soil C; HWSD mineral soils 5-20; Lavoie 2021 peat columns 62-172; Hugelius 2014 permafrost circumpolar peaks |
| cSoilLut | land | kg m-2 | 0 | ~15 | ~200 | Per-tile soil C; tracks cSoil |
| cSoilPools | land | kg m-2 | 0 | ~5 | ~100 | Per-pool C (active/slow/passive); sum equals cSoil; passive pool dominates in deep peat |
| cStem | land | kg m-2 | 0 | ~3 | ~25 | Stem C; tropical forests |
| cVeg | land | kg m-2 | 0 | ~5 | ~30 | Vegetation C (AGB+roots). Saatchi 2011 pan-tropical AGB max ~25 kg DM/m2 ≈ 11 kg C/m2 at plot scale; 32 km cells smooth to ~20-25. DGVMs commonly overshoot in tropics (WARN expected) |
| cVegLut | land | kg m-2 | 0 | ~5 | ~30 | Per-tile vegetation C; tracks cVeg |
| dcw | land | kg m-2 | -5 | ~0 | 5 | Change in interception; near zero annual mean |
| deptho | ocean | m | 0 | ~3700 | ~11000 | Mean ocean depth; Mariana trench max |
| dgw | land | kg m-2 | -50 | ~0 | 50 | Annual groundwater change ~0 in steady state |
| difmxylo | ocean | m2 s-1 | 0 | ~0.01 | ~5 | HR-FESOM (DARS ~10km) Laplacian eddy mixing; higher-res models have much lower values than coarse-res defaults (Christian, cli37 review). cli37 observed mean ~0.01, max ~1.6 |
| difvho | ocean | m2 s-1 | ~1e-7 | ~1e-3 | ~10 | CMIP convention: includes convective Kv from KPP/TKE/EVD schemes. Interior background is Munk-Ledwell (median ~1e-5, Whalen 2012; Waterhouse 2014 JPO), but convective columns in winter high latitudes hit the scheme cap (FESOM CVMix 1-10 m2/s; NEMO `rn_avevd` 100 m2/s default). Global volumetric mean is dominated by the convective tail (~1e-4 to ~1e-3 m2/s typical). |
| difvso | ocean | m2 s-1 | ~1e-7 | ~1e-3 | ~10 | Same Kv as difvho in FESOM CVMix (one diffusivity for heat + salt); same convective-tail behaviour |
| dslw | land | kg m-2 | -100 | ~0 | 100 | Soil moisture change; steady state ~0 |
| dsn | land | kg m-2 | -2000 | ~0 | 2000 | SWE change; HR maritime mountain cells reach 1500-2000 (Mortimer 2020 GlobSnow v3); ~0 mean in steady state |
| dsw | land | kg m-2 | -500 | ~0 | 500 | Surface water storage change; Pantanal/Amazon flood pulse ±300-500 (Tapley 2019 GRACE); ~0 mean steady state |
| emibbbc | aerosol | kg m-2 s-1 | 0 | ~1e-13 | ~1e-9 | 1850 BB BC; van Marle 2017 (BB4CMIP/input4MIPs); per-cell savanna fire peaks ~10x global mean |
| emibbch4 | aerosol | kg m-2 s-1 | 0 | ~1e-12 | ~1e-8 | 1850 BB CH4; van Marle 2017 (BB4CMIP) |
| emibbco | aerosol | kg m-2 s-1 | 0 | ~1e-11 | ~1e-7 | 1850 BB CO; van Marle 2017 (BB4CMIP); active savanna pixels Andreae 2019 |
| emibbdms | aerosol | kg m-2 s-1 | 0 | ~0 | ~1e-12 | DMS from BB negligible (Andreae 2019) |
| emibboa | aerosol | kg m-2 s-1 | 0 | ~1e-12 | ~1e-8 | 1850 BB OA; van Marle 2017 (BB4CMIP) |
| emibbso2 | aerosol | kg m-2 s-1 | 0 | ~1e-13 | ~1e-9 | 1850 BB SO2; van Marle 2017 (BB4CMIP) |
| emibbvoc | aerosol | kg m-2 s-1 | 0 | ~1e-12 | ~1e-8 | 1850 BB NMVOC; van Marle 2017 (BB4CMIP) |
| esn | land | kg m-2 s-1 | 0 | ~1e-6 | ~5e-5 | Snow sublimation; polar climatology (mon default) |
| esn_day | land | kg m-2 s-1 | -1e-5 | ~1e-6 | ~2e-4 | Daily extreme — dry-cold high-wind sublimation |
| evspsbl | atmos | kg m-2 s-1 | 0 | ~3.2e-5 | ~2e-4 | Global mean E ~2.8 mm/day; GPCP/ERA5 (mon default) |
| evspsbl_day | atmos | kg m-2 s-1 | -1e-4 | ~3.2e-5 | ~6e-4 | Daily extreme; allow small negative (dew/condensation) |
| evspsblpot | land | kg m-2 s-1 | 0 | ~5e-5 | ~3e-4 | PET highest subtropics |
| evspsblsoi | land | kg m-2 s-1 | 0 | ~1e-5 | ~1e-4 | Soil evap component |
| evspsblveg | land | kg m-2 s-1 | 0 | ~1e-5 | ~1.5e-4 | Canopy evap/transpiration |
| fAnthDisturb | land | kg m-2 s-1 | 0 | ~1e-10 | ~1e-8 | piControl: small but non-zero — LUH3 1850 transitions file carries wood-harvest rates (`primf_harv` ~1.7e-4/yr on 21% of cells, max 1.6e-2/yr; `secmf_harv`, `secnf_harv` similar) which drive `acflux_wood_harvest` even when macro LU state is frozen (Laszlo round 3); cli37 cmor mean 4.3e-10, max 1.14e-8 |
| fBNF | land | kg m-2 s-1 | 0 | ~3e-12 | ~3e-10 | Natural BNF ~100 TgN/yr global (Vitousek 2013); tropical legume stands reach ~1e-9 (Davies-Barnard & Friedlingstein 2020) |
| fCLandToOcean | land | kg m-2 s-1 | 0 | ~1e-11 | ~1e-9 | Riverine C ~0.9 PgC/yr; IPCC AR6 |
| fco2antt | atmos | kg m-2 s-1 | 0 | ~0 | ~0 | piControl 1850: no anthropogenic CO2 emissions; LUH2/CMIP6 |
| fco2nat | atmos | kg m-2 s-1 | -3e-7 | ~0 | ~3e-7 | Natural land+ocean CO2 flux; piControl globally balanced (Friedlingstein 2022 GCB); HR upwelling/forest cells reach ±3e-7 (Hoffman 2014 C4MIP) |
| fDeforestToAtmos | land | kg m-2 s-1 | 0 | ~0 | ~0 | piControl: no deforestation |
| fDeforestToProduct | land | kg m-2 s-1 | 0 | ~0 | ~0 | piControl: no deforestation |
| fFire | land | kg m-2 s-1 | 0 | ~5e-10 | ~3e-6 | piControl: PI fire higher than PD (Hamilton 2018, SIMFIRE-BLAZE PI 2-5x CMIP6 PI). Global 1.5-6 PgC/yr -> land-mean ~3e-10 to 1.3e-9. Per-pixel monthly peak: GFED5 grid-cell peaks ~1-3 kg C/m2/month at 0.25 deg; DGVMs (LPJ-GUESS BLAZE) typically overshoot 2-3x; TCo319 smaller cells concentrate further -> ceiling ~3e-6 kg/m2/s (~7.9 kg C/m2/month) |
| fFireAll | land | kg m-2 s-1 | 0 | ~5e-10 | ~3e-6 | Same as fFire (incl. fLuc, zero in piControl). FireMIP PD 1.7-3.0 PgC/yr (Li 2019 ACP); GFED5 3.4 PgC/yr; PI plausibly elevated. Per-pixel monthly peak ceiling tracks fFire |
| fFireNat | land | kg m-2 s-1 | 0 | ~5e-10 | ~3e-6 | Natural-only fire; in piControl ~equals fFire. SIMFIRE-BLAZE PI range; per-pixel monthly peak ceiling tracks fFire |
| fHarvestToAtmos | land | kg m-2 s-1 | 0 | ~1e-10 | ~1e-8 | piControl: small but non-zero — 1850 LUH3 state has ~10% cropland + ~20% pasture that keeps being harvested every year (Laszlo round 2); cli37 cmor mean 4.3e-10, max 1.14e-8 |
| fHarvestToProduct | land | kg m-2 s-1 | 0 | ~0 | ~0 | piControl negligible harvest; LUH2 1850 |
| fLitterFire | land | kg m-2 s-1 | 0 | ~2e-11 | ~2e-9 | Litter burning component |
| fLitterSoil | land | kg m-2 s-1 | 0 | ~2e-9 | ~5e-8 | Litter->soil turnover |
| fLuc | land | kg m-2 s-1 | -1e-9 | ~0 | ~1e-9 | piControl LUC ~0; CMIP6 spec |
| fLulccAtmLut | land | kg m-2 s-1 | 0 | ~0 | ~0 | piControl LUC ~0 |
| fN2O | land | kg m-2 s-1 | 0 | ~5e-13 | ~3e-10 | Pre-industrial land N2O ~7 TgN-N2O/yr (Tian 2020); tropical wet-forest hotspots ~order higher (Davidson & Kanter 2014) |
| fNAnthDisturb | land | kg m-2 s-1 | 0 | ~2e-11 | ~5e-10 | piControl: N analog of fAnthDisturb — N in biomass removed by LUH3 1850 wood harvest (Laszlo round 3); cli37 cmor mean 1.47e-11, max 4.85e-10 |
| fNdep | land | kg m-2 s-1 | 0 | ~3e-13 | ~5e-11 | 1850 N deposition ~5 TgN/yr (Galloway 2004; Lamarque 2013 input4MIPs); lightning-active tropics reach ~3e-11 |
| fNfert | land | kg m-2 s-1 | 0 | ~3e-11 | ~5e-9 | piControl: LUH3 `fertl_*` is 0 in 1850, but LPJ-GUESS management still emits a small implicit baseline (manure-N / residue redistribution) on the ~10% cropland + ~20% pasture inherited from 1850 LUH3 state (Laszlo round 3); cli37 cmor mean 2.26e-11, max 3.14e-9. Magnitude ~0.07 g N m-2 yr-1, ~100x below modern application — origin worth a future LPJ-GUESS code audit |
| fNgas | land | kg m-2 s-1 | 0 | ~3e-12 | ~3e-9 | Gaseous N loss; tropical denitrification/NH3 hotspots ~1e-9 (Davidson 2009; Bouwman 2013) |
| fNgasFire | land | kg m-2 s-1 | 0 | ~1e-12 | ~1e-9 | N from fires; active fire cells ~1e-9 (Andreae 2019) |
| fNgasNonFire | land | kg m-2 s-1 | 0 | ~3e-12 | ~3e-9 | Non-fire gaseous N loss ~50 TgN/yr (IPCC AR6); per-cell hotspots Davidson 2009 |
| fNLandToOcean | land | kg m-2 s-1 | 0 | ~1e-12 | ~1e-8 | Riverine N flux ~40 TgN/yr globally; concentrated in major river-mouth cells (Beusen 2016 GlobalNEWS; Seitzinger 2010) |
| fNleach | land | kg m-2 s-1 | 0 | ~1e-12 | ~1e-9 | N leaching; tropical wet-forest peaks ~1e-9 (Boyer 2006; Galloway 2004) |
| fNLitterSoil | land | kg m-2 s-1 | 0 | ~3e-11 | ~1e-8 | N litter-to-soil; tropical-forest peaks ~1e-8 (Cleveland 2013; Schmidt 2011) |
| fNloss | land | kg m-2 s-1 | 0 | ~5e-12 | ~5e-9 | Total N loss; sum of leach+gas hotspots in tropical wet forests (Galloway 2004; Cleveland 2013) |
| fNnetmin | land | kg m-2 s-1 | 0 | ~5e-12 | ~3e-10 | Net N mineralisation ~80 TgN/yr; Cleveland 2013 |
| fNOx | land | kg m-2 s-1 | 0 | ~3e-13 | ~3e-10 | Soil NOx ~5-10 TgN/yr global (Yienger & Levy 1995); semi-arid pulse cells ~5e-11, agricultural peaks ~1e-10 (Hudman 2012; Vinken 2014) |
| fNProduct | land | kg m-2 s-1 | 0 | ~0 | ~0 | piControl: no LU products; LUH2 1850 |
| fNup | land | kg m-2 s-1 | 0 | ~3e-10 | ~3e-8 | Plant N uptake; Cleveland |
| fNVegLitter | land | kg m-2 s-1 | 0 | ~5e-11 | ~3e-9 | N litterfall; ratio to fVegLitter via canopy C:N~30 (TRENDY) |
| fProductDecomp | land | kg m-2 s-1 | 0 | ~0 | ~0 | piControl products ~0 |
| fracInLut | land | % | 0 | ~0 | ~0 | piControl no LU transitions |
| fracLut | land | % | 0 | ~25 | 100 | Per-tile fraction; LUH2 |
| fracOutLut | land | % | 0 | ~0 | ~0 | piControl no LU transitions |
| friver | ocean | kg m-2 s-1 | 0 | ~1e-5 | ~1e-2 | River discharge, Amazon mouth high |
| fVegFire | land | kg m-2 s-1 | 0 | ~3e-11 | ~3e-8 | Vegetation fire C flux; active savanna pixels ~3e-8 (van der Werf 2017 GFED4s) |
| fVegLitter | land | kg m-2 s-1 | 0 | ~2e-9 | ~5e-8 | Litterfall ~60 PgC/yr |
| fVegLitterMortality | land | kg m-2 s-1 | 0 | ~5e-10 | ~3e-8 | Mortality litter flux ~10-30% of total litterfall (Pugh et al. 2019) |
| fVegLitterSenescence | land | kg m-2 s-1 | 0 | ~1.5e-9 | ~5e-8 | Senescence dominates litterfall; ~70-90% of total ~60 PgC/yr |
| gpp | land | kg m-2 s-1 | 0 | ~3.5e-8 | ~1e-7 | GPP ~120 PgC/yr; Beer 2010 |
| gppLut | land | kg m-2 s-1 | 0 | ~3.5e-8 | ~1e-7 | Per-tile GPP |
| grassFrac | land | % | 0 | ~20 | 100 | Natural grass coverage; LUH2 |
| grassFracC3 | land | % | 0 | ~15 | 100 | C3 natural grass fraction; temperate dominant; LUH2/CMIP6 |
| grassFracC4 | land | % | 0 | ~5 | 100 | C4 natural grass fraction; tropical/subtropical; LUH2/CMIP6 |
| hfbasin | ocean | W | -1e16 | ~0 | 1e16 | Northward heat transport per basin (monthly). Annual climatology: Trenberth & Caron 2001 global peak ~2 PW @ 35°N, Atlantic ~1.3 PW. Monthly observations: RAPID 26.5°N Atlantic ranges 0.2-2.5 PW (Johns et al. 2011); Pacific tropical cell NHT 1.75±0.30 PW, SHT -1.69±0.55 PW. Global = basin-sum at peak latitude can reach 5-8 PW monthly in HR with mesoscale eddies. Bounds set to ±10 PW (5× Trenberth annual) to admit HR monthly extremes; 2× regression WARNs, 5× FAILs |
| hfds | ocean | W m-2 | -500 | ~0 | 500 | Net heat into ocean (positive=down). piControl steady state should give global mean near 0; small drift either sign is acceptable. Per-cell monthly peaks in deep-convection / strong air-sea contrast regions can reach ±500 W/m² (Labrador/Greenland Sea; subtropical evaporation maxima). Values beyond ±1000 are a sentinel for outlier cells worth investigating |
| hfdsl | land | W m-2 | -300 | ~0 | 300 | Ground heat flux; subtropical desert mon extremes ±200 (mon default) |
| hfdsl_3hr | land | W m-2 | -1200 | ~0 | 1200 | 3-hourly net surface flux extremes |
| hfls | atmos | W m-2 | 0 | ~80 | 250 | LH flux; CERES/ERA5 (mon default) |
| hfls_day | atmos | W m-2 | -250 | ~80 | 700 | Daily LH extreme — tropical convection / cyclones |
| hfls_3hr | atmos | W m-2 | -500 | ~80 | 1100 | 3-hourly LH extreme |
| hfls_1hr | atmos | W m-2 | -500 | ~80 | 1500 | Hourly LH extreme; ERA5 TC peaks ~1500 (Hersbach 2020) |
| hfss | atmos | W m-2 | -150 | ~20 | 300 | SH flux; cold-air outbreaks (mon default) |
| hfss_day | atmos | W m-2 | -500 | ~20 | 500 | Daily SH extreme |
| hfss_3hr | atmos | W m-2 | -2500 | ~20 | 700 | 3-hourly SH extreme — extreme cold-air outbreaks |
| hfss_1hr | atmos | W m-2 | -3000 | ~20 | 900 | Hourly SH extreme — Sahara/Arabia summer noon |
| hfx | ocean | W | -5e14 | ~0 | 5e14 | Per-CELL zonal heat transport (not basin-integrated like hfbasin); HR FESOM WBC cells reach ±0.5 PW; the ±2 PW Trenberth-scale bound is the basin-integrated benchmark, not appropriate per-cell |
| hfy | ocean | W | -5e14 | ~0 | 5e14 | Per-CELL meridional heat transport; see hfx |
| hur | atmos | % | 0 | ~60 | 100 | RH profile; ERA5 |
| hurs | atmos | % | 10 | ~75 | 100 | Near-surface RH; ERA5 |
| hus | atmos | 1 | ~1e-6 | ~3e-3 | ~0.025 | Specific humidity; tropics saturated ~25 g/kg |
| huss | atmos | 1 | ~0.00001 | ~0.008 | ~0.025 | ERA5 near-surface q, polar dry to tropical moist (mon default) |
| huss_3hr | atmos | 1 | ~0.00001 | ~0.008 | ~0.028 | 3-hourly extreme tropical moist |
| huss_1hr | atmos | 1 | ~0.00001 | ~0.008 | ~0.030 | Hourly tropical peak (~30 g/kg) |
| irrLut | land | kg m-2 s-1 | 0 | ~0 (piControl) | ~1e-5 | No anthropogenic irrigation in 1850 piControl |
| lai | land | 1 | 0 | ~1.2 | ~7 | MODIS LAI climatology, tropical forests peak |
| laiLut | land | 1 | 0 | ~1.2 | ~7 | MODIS per-tile LAI |
| landCoverFrac | land | % | 0 | varies by PFT | 100 | Fraction bounded 0-100 |
| lwp | aerosol | kg m-2 | 0 | ~0.05-0.1 | ~0.5 | CMIP6/ISCCP cloud LWP climatology |
| masscello | ocean | kg m-2 | 5125 | ~1e5 | 358750 | rho*dz per layer: AWI-ESM vertical discretization has min(dz)=5m, max(dz)=350m; rho~1025 kg/m3 |
| masso | ocean | kg | 1.3e21 | 1.35e21 | 1.4e21 | Global ocean mass ~1.35e21 kg |
| mlotst | ocean | m | ~10 | ~60 | ~2000 | de Boyer Montegut climatology; deep Labrador/Weddell |
| mlotstsq | ocean | m2 | 100 | ~1e4 | ~4e6 | Square of mlotst |
| mrfso | landIce | kg m-2 | 0 | ~200 | ~5000 | Frozen soil water, permafrost regions |
| mrro | land | kg m-2 s-1 | 0 | ~1e-5 (30 mm/yr land avg) | ~2e-4 | GRDC/CMIP6 land runoff (mon default) |
| mrro_day | land | kg m-2 s-1 | 0 | ~1e-5 | ~3e-3 | Daily extreme — saturated land + heavy rain |
| mrro_3hr | land | kg m-2 s-1 | 0 | ~1e-5 | ~1e-2 | 3-hourly runoff burst |
| mrrob | land | kg m-2 s-1 | 0 | ~1e-5 | ~5e-3 | Subsurface runoff, wettest tropics — singular grid-cell spikes are real model output, global field is fine (Laszlo + Christian round 2); cli37 cmor max 2.34e-3 |
| mrros | land | kg m-2 s-1 | 0 | ~5e-6 | ~1e-4 | Surface runoff fraction of total (mon default) |
| mrros_3hr | land | kg m-2 s-1 | 0 | ~5e-6 | ~1e-2 | 3-hourly surface runoff burst |
| mrsll | land | kg m-2 | 0 | ~30 | ~300 | Per-layer liquid soil water; thicker layers larger; CMIP6 Land |
| mrso | land | kg m-2 | 0 | ~500 | ~2000 | Total soil moisture column |
| mrsofc | land | kg m-2 | 0 | ~300 | ~1500 | Soil field capacity, typical 300mm |
| mrsol | land | kg m-2 | 0 | ~100 | ~500 | Upper soil layer water |
| mrsolLut | land | kg m-2 | 0 | ~100 | ~500 | Per-tile upper soil moisture |
| mrsow | land | 1 | 0 | ~0.5 | 1 | Soil wetness fraction |
| mrtws | land | kg m-2 | 0 | ~1500 | ~1e5 | GRACE TWS incl. groundwater/ice |
| msftbarot | ocean | kg s-1 | -5e11 | 0 | 5e11 | ACC ~173 Sv (Donohue 2016); HR (1/12 deg) gyre interiors reach 250-300 Sv (Treguier 2014) |
| msftm | ocean | kg s-1 | -1e11 | 0 | 1e11 | Global MOC streamfunction extremes ~80 Sv (Deacon Cell+AABW; Talley 2013, Lumpkin & Speer 2007); AMOC alone ~17 Sv (Smeed 2018 RAPID) |
| msftmmpa | ocean | kg s-1 | -5e10 | 0 | 5e10 | Mesoscale MOC component smaller than resolved |
| n2o | atmosChem | mol mol-1 | ~1e-7 | ~2.72e-7 | ~3.5e-7 | Pre-industrial N2O ~272 ppb; Flueckiger 2002 ice cores; strat depleted |
| nbp | land | kg m-2 s-1 | -1e-7 | ~0 (piControl balanced) | 1e-7 | piControl NBP near zero, Friedlingstein 2022 |
| nbpLut | land | kg m-2 s-1 | -1e-7 | ~0 | 1e-7 | Per-tile NBP; same scale as nbp; piControl ~0 mean |
| nep | land | kg m-2 s-1 | -5e-6 | ~0 | 2e-7 | NEP near zero annual mean in piControl; Central America wet-tropics drainage spikes are real model output, raw .out matches cmor (Laszlo round 2); cli37 cmor min -1.93e-6, max 1.01e-7 |
| nLand | land | kg m-2 | 0 | ~1.5 | ~20 | Total N in soil+veg, ~200 PgN / land |
| nLeaf | land | kg m-2 | 0 | ~0.01 | ~0.05 | Leaf N from cLeaf~0.3 with C:N~30 (TRENDY canopy) |
| nLitter | land | kg m-2 | 0 | ~0.05 | ~1 | Litter N, small pool |
| nLitterCwd | land | kg m-2 | 0 | ~0.01 | ~0.1 | CWD N from cLitterCwd~1 with woody C:N~80; Pan 2011 |
| nLitterSubSurf | land | kg m-2 | 0 | ~0.03 | ~0.3 | Belowground litter N; cLitterSubSurf~1, C:N~30 |
| nLitterSurf | land | kg m-2 | 0 | ~0.03 | ~0.3 | Aboveground litter N; cLitterSurf~1, C:N~30 |
| nMineral | land | kg m-2 | 0 | ~0.05 | ~1 | Mineral/inorganic soil N |
| nMineralNH4 | land | kg m-2 | 0 | ~0.02 | ~0.5 | Soil mineral NH4; CMIP6/JSBACH typical column totals |
| nMineralNO3 | land | kg m-2 | 0 | ~0.02 | ~0.5 | Soil mineral NO3; CMIP6/JSBACH typical column totals |
| nOther | land | kg m-2 | 0 | ~0.008 | ~0.1 | Reproductive/other N; cOther~0.2 with C:N~25 |
| npp | land | kg m-2 s-1 | 0 | ~1.9e-8 (~60 PgC/yr/land) | ~5e-7 | CMIP6 NPP, tropical forests |
| nppLeaf | land | kg m-2 s-1 | 0 | ~6e-9 | ~1.5e-7 | ~30% of npp; tropical forests peak (Malhi 2011) |
| nppLut | land | kg m-2 s-1 | 0 | ~1.9e-8 | ~5e-7 | Per-tile NPP |
| nppOther | land | kg m-2 s-1 | 0 | ~3e-9 | ~1e-7 | Reproductive NPP small fraction |
| nppRoot | land | kg m-2 s-1 | 0 | ~6e-9 | ~1.5e-7 | ~30% of npp; Jackson 1997 belowground allocation |
| nppStem | land | kg m-2 s-1 | 0 | ~6e-9 | ~1.5e-7 | ~30% of npp; woody allocation |
| nProduct | land | kg m-2 | 0 | ~0 (piControl) | ~0.01 | No land-use products in piControl |
| nRoot | land | kg m-2 | 0 | ~0.02 | ~0.2 | Root N; cRoot~1 with C:N~50; Jackson 1997 |
| nSoil | land | kg m-2 | 0 | ~1 | ~15 | Soil N dominates total |
| nStem | land | kg m-2 | 0 | ~0.02 | ~0.2 | Stem N; cStem~3 with sapwood C:N~150 |
| nVeg | land | kg m-2 | 0 | ~0.1 | ~2 | Vegetation N pool |
| obvfsq | ocean | s-2 | 0 | ~1e-5 | ~1e-3 | N^2 pycnocline values |
| od550aer | aerosol | 1 | ~0.02 | ~0.12 | ~1 | MODIS/AERONET AOD climatology |
| opottempdiff | ocean | W m-2 | -500 | ~0 | 500 | Diapycnal/isopycnal mixing tendency; convective adjustment + overflows (Denmark Strait, Faroe Bank) reach ~500 (Kuhlbrodt 2007; Griffies 2015 OMIP) |
| opottempmint | ocean | degC kg m-2 | -1e7 | ~1e7 | 1e8 | rho*theta*depth, ~1025*10*4000 tropics |
| opottemprmadvect | ocean | W m-2 | -5000 | ~0 | 5000 | Residual-mean advective heat tendency; HR eddy-active regions reach few thousand W m-2 (Griffies 2015 OMIP; Treguier 2017) |
| opottemptend | ocean | W m-2 | -500 | ~0 | 500 | Total theta tendency, piControl near 0 global |
| orog | land | m | 0 | ~800 | ~8848 | ETOPO topography |
| osaltdiff | ocean | kg m-2 s-1 | -1e-4 | ~0 | 1e-4 | Diffusive salt tendency; convective regions and overflow plumes ~1e-4 (Griffies 2015 OMIP) |
| osaltrmadvect | ocean | kg m-2 s-1 | -1e-4 | ~0 | 1e-4 | Advective salt tendency |
| osalttend | ocean | kg m-2 s-1 | -1e-4 | ~0 | 1e-4 | Total salt tendency ~0 in piControl |
| pastureFrac | land | % | 0 | ~3 | 100 | LUH2 1850 pasture ~3% global land, locally up to ~100% rangeland |
| pastureFracC3 | land | % | 0 | ~2 | 100 | LUH2 1850 C3 pasture in temperate (most of pastureFrac); ~60% of global pasture |
| pastureFracC4 | land | % | 0 | ~1 | 100 | LUH2 1850 C4 pasture in tropical savanna; ~40% of global pasture |
| pbo | ocean | Pa | 0 | ~4e7 | ~1.1e8 | rho*g*H; 4000m ocean |
| pfull | atmos | Pa | ~1 | ~5e4 | ~101325 | Model level pressures |
| phcint | ocean | J m-2 | -1e10 | ~1e10 | ~5e10 | rho*cp*T*H with T in degC (ref 0 degC): high-lat columns with T<0 give phcint<0 |
| pr | atmos | kg m-2 s-1 | 0 | ~3e-5 (~2.7 mm/day) | ~3e-4 | GPCP global mean precip (mon-cadence default) |
| pr_day | atmos | kg m-2 s-1 | 0 | ~3e-5 | ~1e-2 | Daily extreme — tropical convergence zones |
| pr_3hr | atmos | kg m-2 s-1 | 0 | ~3e-5 | ~2e-2 | 3-hourly extreme — convective storm cores |
| pr_1hr | atmos | kg m-2 s-1 | 0 | ~3e-5 | ~3e-2 | Hourly extreme — single-cell convective burst |
| prc | atmos | kg m-2 s-1 | 0 | ~1.5e-5 | ~2e-4 | Convective fraction ~50% (mon default) |
| prc_day | atmos | kg m-2 s-1 | 0 | ~1.5e-5 | ~3e-3 | Daily convective extreme |
| prra | seaIce | kg m-2 s-1 | 0 | ~3e-5 | ~1e-3 | pycmor writes prra over the full FESOM domain (not masked to ice), so walker sees global rain ~ pr magnitudes; not "rain over ice only" |
| prsn | atmos | kg m-2 s-1 | 0 | ~5e-6 | ~1e-4 | Snowfall ~15% of precip (mon default) |
| prsn_day | atmos | kg m-2 s-1 | 0 | ~5e-6 | ~2e-3 | Daily extreme snowstorm (SWE rate) |
| prsn_6hr | atmos | kg m-2 s-1 | 0 | ~5e-6 | ~3e-3 | 6-hourly extreme snowstorm |
| prsn_3hr | atmos | kg m-2 s-1 | 0 | ~5e-6 | ~5e-3 | 3-hourly extreme — lake effect / orographic |
| prveg | land | kg m-2 s-1 | 0 | ~1e-5 | ~3e-4 | Canopy-intercepted precip |
| prw | atmos | kg m-2 | 0.5 | ~25 | ~70 | ERA5 TCWV climatology |
| ps | atmos | Pa | 50000 | ~98500 | 105000 | Surface pressure range incl. Tibet |
| psl | atmos | Pa | 95000 | ~101325 | 105000 | MSLP ERA5 |
| pso | ocean | Pa | ~0 | ~101325 | ~102000 | Sea surface pressure ~atmospheric |
| ra | land | kg m-2 s-1 | 0 | ~2e-8 | ~5e-7 | Autotrophic resp ~60 PgC/yr |
| raLeaf | land | kg m-2 s-1 | 0 | ~5e-9 | ~1e-7 | Leaf resp fraction of ra |
| raLut | land | kg m-2 s-1 | 0 | ~2e-8 | ~5e-7 | Per-tile ra |
| raOther | land | kg m-2 s-1 | 0 | ~5e-9 | ~1e-7 | Small ra fraction |
| raRoot | land | kg m-2 s-1 | 0 | ~5e-9 | ~1e-7 | Root resp fraction |
| raStem | land | kg m-2 s-1 | 0 | ~5e-9 | ~1e-7 | Stem resp fraction |
| residualFrac | land | % | 0 | ~5 | 100 | Bare/urban/other residual |
| rh | land | kg m-2 s-1 | 0 | ~1.8e-8 (~55 PgC/yr) | ~3e-7 | Heterotrophic resp, CMIP6 |
| rhLitter | land | kg m-2 s-1 | 0 | ~9e-9 | ~1.5e-7 | Litter decomp fraction |
| rhLut | land | kg m-2 s-1 | 0 | ~1.8e-8 | ~3e-7 | Per-tile rh |
| rhSoil | land | kg m-2 s-1 | 0 | ~9e-9 | ~1.5e-7 | Soil rh component |
| rlds | atmos | W m-2 | 100 | ~345 | 450 | CERES-EBAF surface LW down |
| rldscs | atmos | W m-2 | 80 | ~315 | 430 | Clear-sky LW down |
| rls | atmos | W m-2 | -200 | ~-55 | 50 | Net LW surface (down-up) |
| rlus | atmos | W m-2 | 150 | ~398 | 520 | sigma*T^4, CERES (mon default) |
| rlus_1hr | atmos | W m-2 | 150 | ~398 | 750 | Hourly extreme — desert skin σT⁴ at 340 K |
| rlus_3hr | atmos | W m-2 | 150 | ~398 | 700 | 3-hourly extreme |
| rluscs | atmos | W m-2 | 150 | ~398 | 520 | Same as rlus (clear-sky same surface T) |
| rlut | atmos | W m-2 | 120 | ~239 | 320 | CERES OLR |
| rlutcs | atmos | W m-2 | 150 | ~266 | 330 | Clear-sky OLR |
| rootd | land | m | 0 | ~2 | ~10 | Schenk & Jackson root depths |
| rsdoabsorb | ocean | W m-2 | 0 | varies by layer | ~300 | SW penetration, surface layer |
| rsds | atmos | W m-2 | 0 | ~185 | 400 | CERES surface SW down annual (mon default) |
| rsds_day | atmos | W m-2 | 0 | ~185 | 500 | Daily extreme — clear-sky high-latitude summer |
| rsds_3hr | atmos | W m-2 | 0 | ~185 | 1200 | 3-hourly extreme — clear-sky tropical noon |
| rsds_1hr | atmos | W m-2 | 0 | ~185 | 1400 | Hourly extreme — TOA ~1361, surface clear-sky tropical noon |
| rsdscs | atmos | W m-2 | 0 | ~245 | 450 | Clear-sky surface SW down |
| rsdt | atmos | W m-2 | 0 | ~340 | ~550 | TOA incident SW, S0/4 |
| rss | atmos | W m-2 | 0 | ~160 | 350 | Net SW surface |
| rsus | atmos | W m-2 | 0 | ~24 | 300 | Surface upward SW (albedo*rsds) (mon default) |
| rsus_day | atmos | W m-2 | 0 | ~24 | 450 | Daily extreme — high-albedo snow/ice noon |
| rsus_3hr | atmos | W m-2 | 0 | ~24 | 1100 | 3-hourly extreme |
| rsus_1hr | atmos | W m-2 | 0 | ~24 | 1300 | Hourly extreme — bright surface × tropical-noon rsds |
| rsuscs | atmos | W m-2 | 0 | ~30 | 350 | Clear-sky upwelling SW surface |
| rsut | atmos | W m-2 | 0 | ~100 | 400 | CERES TOA reflected SW |
| rsutcs | atmos | W m-2 | 0 | ~53 | 300 | Clear-sky TOA reflected (mon default) |
| rsutcs_day | atmos | W m-2 | 0 | ~53 | 400 | Daily extreme — bright deserts / ice |
| rtmt | atmos | W m-2 | -200 | ~0 (piControl balanced) | 200 | Net TOA ~0 in piControl |
| sbl | landIce | kg m-2 s-1 | -1e-4 | ~1e-7 | 1e-4 | Snow/ice sublimation; Antarctic Plateau katabatic events reach ~1e-4 (Lenaerts 2012 RACMO; Box & Steffen 2001) |
| scint | ocean | kg m-2 | 0 | ~1.4e5 | ~1.5e5 | S*rho*H, ~35 PSU*1025*4000m |
| sfcWind | atmos | m s-1 | 0 | ~6.5 | ~40 | ERA5 10m wind daily max |
| sfdsi | ocean | kg m-2 s-1 | -5e-4 | ~0 | 5e-4 | Sea-ice salt flux; strong Arctic freezing bursts can hit ~1e-4, mean cancels to ~0 (cli37 observed min -1.6e-4, max 6e-5, pattern PASS per review) |
| sftgif | land | % | 0 | ~3 | 100 | Glacier/ice fraction (Greenland/Antarctica=100) |
| sftlf | atmos | % | 0 | ~29 | 100 | Land fraction, ~29% globe |
| sftof | ocean | % | 0 | ~71 | 100 | Ocean fraction complement |
| sfx | ocean | kg s-1 | -1e10 | ~0 | 1e10 | 3D salt transport per cell edge. compute_salt_transport multiplies by sqrt(cell_area) as effective edge width on FESOM Voronoi cells. cli37 values are pre-fix (kg/(s*m)) and FAIL; first run with the fix should PASS. |
| sfy | ocean | kg s-1 | -1e10 | ~0 | 1e10 | Same as sfx (y-component) |
| shrubFrac | land | % | 0 | 5-10 | 100 | LUH2/CMIP6 land cover |
| siarea | seaIce | 1e6 km2 | 4 (Sep) | 11 | 16 (Mar) | NSIDC NH climatology |
| sicompstren | seaIce | N m-1 | 0 | 5e3 | 5e4 | Hibler rheology P* typical |
| siconc | seaIce | % | 0 | ~60 | 100 | Walker averages over non-NaN cells; pycmor's si-mask keeps ice-capable nodes, so mean is ice-zone (~60%), not full-ocean ~5% |
| siconca | seaIce | % | 0 | ~5 | 100 | Sea-ice concentration on the global atm grid (lat-lon, includes ice-free tropics): mean is global ~5% |
| sidconcdyn | seaIce | s-1 | -1e-5 | ~0 | 1e-5 | CMIP6 sea-ice tendencies |
| sidconcth | seaIce | s-1 | -1e-5 | ~0 | 1e-5 | CMIP6 sea-ice tendencies |
| sidmassdyn | seaIce | kg m-2 s-1 | -1e-3 | ~0 | 1e-3 | CMIP6 order-of-magnitude |
| sidmassth | seaIce | kg m-2 s-1 | -1e-3 | ~0 | 1e-3 | CMIP6 order-of-magnitude |
| sidmasstranx | seaIce | kg s-1 | -1e8 | ~0 | 1e8 | Fram Strait export ~1e8 kg/s |
| sidmasstrany | seaIce | kg s-1 | -1e8 | ~0 | 1e8 | Fram Strait export ~1e8 kg/s |
| sidragbot | seaIce | 1 | 1e-3 | 5e-3 | 2e-2 | McPhee ice-ocean drag |
| sidragtop | seaIce | 1 | 1e-3 | 5e-3 | 2e-2 | Atmospheric drag coefficient; CCSM/CICE Cd; same scale as sidragbot (McPhee 1980) |
| sieqthick | seaIce | m | 0 | 0.3 (ice zone ~1.5) | 8 | PIOMAS climatology |
| siextent | seaIce | 1e6 km2 | 6 (Sep) | 12 | 16 (Mar) | NSIDC NH |
| sifb | seaIce | m | 0 | 0.2 | 1.5 | ICESat freeboard |
| siflcondbot | seaIce | W m-2 | -100 | ~-10 | 50 | Maykut/Perovich conductive flux; CMIP6 model archive — annual mean NH/SH typically -5 to -20 W m-2 (winter heat loss upward dominates over summer downward); positive=down convention |
| siflcondtop | seaIce | W m-2 | -1000 | ~-20 | 200 | Maykut/Perovich conductive flux; positive=down. Annual NH/SH mean -5 to -20 W/m2 (winter loss upward dominates). HR captures sub-cm ice cells in extreme cold: q = k*dT/h with k~2 W/m/K, dT~30 K, h~1 cm gives -6 kW/m2 -- thin-ice tail can hit -1 kW/m2 at 32 km; <0.01% of cells will WARN beyond this |
| siflfwbot | seaIce | kg m-2 s-1 | -5e-3 | ~0 | 5e-3 | CMIP6 ice FW flux; annual mean ~0 (mass balance). HR per-cell extremes: marginal ice zone melt of ~30 cm ice/day gives ~4e-3 kg/m2/s; freeze-up at thin pack gives similar magnitude with opposite sign |
| siflfwdrain | seaIce | kg m-2 s-1 | 0 | ~1e-6 | 1e-4 | Melt pond drainage |
| sifllattop | seaIce | W m-2 | -100 | ~-10 | 50 | Latent heat flux over sea ice (downward positive); ERA5 polar climatology |
| siflsenstop | seaIce | W m-2 | -100 | ~-10 | 100 | Sensible heat flux over sea ice (downward positive); ERA5 polar climatology |
| sihc | seaIce | J m-2 | -1e9 | -1e8 | 0 | c*rho*h*dT; negative=cold |
| simass | seaIce | kg m-2 | 0 | 30 | 8000 | h*rho_ice; Lincoln Sea / north-Greenland multi-year ridges 8-10 m (Schweiger 2011 PIOMAS; Kwok 2018 ICESat-2/CryoSat-2) |
| simpconc | seaIce | % | 0 | 5 | 50 | CICE melt-pond frac |
| simpeffconc | seaIce | % | 0 | 3 | 40 | CICE effective pond frac |
| simprefrozen | seaIce | m | 0 | 0.02 | 0.3 | CICE topo melt-pond |
| simpthick | seaIce | m | 0 | 0.05 | 0.5 | CICE melt-pond depth |
| sisaltmass | seaIce | kg m-2 | 0 | 0.15 | 25 | ~5 psu * simass/1000 |
| sisnhc | seaIce | J m-2 | -5e7 | -2e6 | 0 | c_snow*rho*h*dT; deeper drifted snow on ridges (h~1.5-2 m) extends magnitude to ~5e7 (Sturm 2002; Massom 2001) |
| sisnmass | seaIce | kg | 0 | 2e15 | 1e16 | NH snow-on-ice total |
| sispeed | seaIce | m s-1 | 0 | 0.05 | 1.0 | IABP drift buoys |
| sistressave | seaIce | N m-1 | -5e4 | 0 | 5e4 | CICE stress tensor |
| sistressmax | seaIce | N m-1 | 0 | 1e3 | 5e4 | CICE stress tensor |
| sistrxdtop | seaIce | N m-2 | -1 | 0 | 1 | Atm stress on ice |
| sistrxubot | seaIce | N m-2 | -1 | 0 | 1 | Ocean stress on ice |
| sistrydtop | seaIce | N m-2 | -1 | 0 | 1 | Atm stress on ice |
| sistryubot | seaIce | N m-2 | -1 | 0 | 1 | Ocean stress on ice |
| sitempbot | seaIce | K | 271 | 271.35 | 273.15 | Freezing point sea water |
| sithick | seaIce | m | 0 | 0.3 (ice zone 1-3) | 12 | Multi-year ridged ice grid cells reach 8-10 m monthly mean (Laxon 2013 CryoSat-2; Petty 2020 ICESat-2; Belter 2020 AWI atlas); piControl can support thicker (Kay 2015 CESM-LE) |
| sitimefrac | seaIce | 1 | 0 | ~0.3 | 1 | Fraction of period with ice, walker averages over ice-capable cells; obs day~0.8 (winter-heavy) / mon~0.3 |
| siu | seaIce | m s-1 | -1 | 0 | 1 | IABP drift buoys |
| siv | seaIce | m s-1 | -1 | 0 | 1 | IABP drift buoys |
| sivol | seaIce | 1e3 km3 | 10 (Sep) | 20 | 30 (Apr) | PIOMAS NH volume |
| sltbasin | ocean | kg s-1 | -1e9 | ~0 | 1e9 | Northward salt transport per basin; ~10^8-10^9 kg/s peaks (Talley 2008) |
| slthick | land | m | 0.01 | 0.3 | 5 | JSBACH/CLM soil layers |
| snc | landIce | % | 0 | 15 | 100 | Rutgers NH snow cover |
| snd | landIce | m | 0 | 0.05 | 10 | GlobSnow/ERA5 snow depth |
| snm | landIce | kg m-2 s-1 | 0 | 1e-6 | 1e-3 | Seasonal melt rate |
| snmsl | atmos | kg m-2 s-1 | 0 | 1e-6 | 1e-3 | Snowpack runoff |
| snw | landIce | kg m-2 | 0 | 20 | 3000 | ERA5 SWE, glaciers large |
| so | ocean | 1E-03 | 0 | 34.7 | 42 | WOA salinity; min 0 covers Baltic / Black Sea / Hudson Bay surface freshwater; max ~42 Red Sea / Persian Gulf / Med deep |
| sob | ocean | 1E-03 | 5 | 34.7 | 42 | WOA bottom salinity; lower min for shallow brackish shelves (Baltic ~7 at bottom); Red Sea / Med deep ~40-42 |
| somint | ocean | g m-2 | 1e5 | 1.4e8 | 2e8 | rho*S*H for H~4000m |
| sos | ocean | 1E-03 | 0 | 34.7 | 42 | WOA surface salinity; Baltic ~3-7 PSU, Black Sea ~17-18, large estuaries near 0; max ~42 Persian Gulf |
| sossq | ocean | 1E-06 | 0 | 1205 | 1764 | sos squared (max=42^2) |
| srfrad | land | W m-2 | -100 | 60 | 250 | CERES land net radiation |
| sweLut | land | m | 0 | ~0.02 | ~3 | Per-tile snow water equivalent; lwe thickness; ERA5/GlobSnow |
| ta | atmos | K | 180 | 255 | 310 | ERA5 free-atm mean ~255K |
| tas | atmos | K | 220 | 287 | 320 | HadCRUT/ERA5 1850 ~286.7K |
| tauu | atmos | Pa | -0.5 | ~0 | 0.5 | ERA5 wind stress |
| tauuo | ocean | N m-2 | -0.5 | ~0 | 0.5 | ERA5 ocean stress |
| tauv | atmos | Pa | -0.5 | ~0 | 0.5 | ERA5 wind stress |
| tauvo | ocean | N m-2 | -0.5 | ~0 | 0.5 | ERA5 ocean stress |
| thetao | ocean | degC | -2 | 3.5 | 32 | WOA global ocean mean |
| thkcello | ocean | m | 1 | 50 | 500 | z-coord cell thickness |
| tob | ocean | degC | -2 | 1 | 10 | WOA bottom temperature |
| tos | ocean | degC | -2 | 18 | 32 | HadISST/WOA SST |
| tossq | ocean | degC2 | 0 | ~350 | 1024 | tos squared |
| toz | aerosol | m | 2.2e-3 | 3e-3 | 5e-3 | ~300 DU = 3e-3 m (TOMS) |
| tran | land | kg m-2 s-1 | 0 | 1.5e-5 | 1e-4 | GLEAM/FLUXNET transpiration |
| treeFrac | land | % | 0 | 30 | 100 | LUH2 preindustrial ~30% |
| treeFracBdlDcd | land | % | 0 | 5 | 100 | LUH2 land cover |
| treeFracBdlEvg | land | % | 0 | ~10 | 100 | Tropical/temperate broadleaf evergreen; LUH2/MODIS |
| treeFracNdlDcd | land | % | 0 | ~3 | 100 | Boreal larch (Siberian taiga); LUH2/MODIS |
| treeFracNdlEvg | land | % | 0 | ~5 | 100 | Boreal pine/spruce; LUH2/MODIS |
| ts | atmos | K | 220 | 288 | 330 | ERA5 skin temperature |
| tsl | land | K | 150 | ~285 | 325 | Soil temperature per layer; ERA5/CMIP6 Land. Floor relaxed from 220 to 150 K: LPJ-GUESS shallow-layer Tsoil tracks OpenIFS forcing when uninsulated, 1.5% of values <220 K in NH winter (Laszlo round 2). Anything below 150 K is physically impossible and must remain a FAIL. |
| tslsi | land | K | 220 | 285 | 330 | Land/sea-ice skin temp |
| tsn | landIce | K | 220 | 260 | 273.15 | Snow temperature ≤ 0°C |
| ua | atmos | m s-1 | -80 | ~0 | 100 | ERA5 zonal wind (mon default; jet-stream-aware) |
| ua_day | atmos | m s-1 | -100 | ~0 | 120 | Daily extreme — 500 hPa subtropical jet |
| uas | atmos | m s-1 | -30 | ~0 | 30 | ERA5 10m wind (mon default) |
| uas_day | atmos | m s-1 | -50 | ~0 | 50 | Daily extreme — ERA5 storm/TC 10m |
| uas_3hr | atmos | m s-1 | -65 | ~0 | 65 | 3-hourly extreme |
| uas_1hr | atmos | m s-1 | -75 | ~0 | 75 | Hourly extreme — Cat-5 TC (Saffir-Simpson) |
| umo | ocean | kg s-1 | -1e12 | 0 | 1e12 | Mass transport per cell edge. compute_mass_transport multiplies by sqrt(cell_area) as effective edge width on FESOM Voronoi cells. cli37 values are pre-fix (kg/(s*m)) and FAIL; first run with the fix should PASS. |
| uo | ocean | m s-1 | -2 | ~0 | 2 | WOCE/Argo currents |
| uos | ocean | m s-1 | -2 | ~0 | 2.5 | OSCAR surface currents |
| va | atmos | m s-1 | -60 | ~0 | 60 | ERA5 meridional wind (mon default) |
| va_day | atmos | m s-1 | -80 | ~0 | 80 | Daily extreme — 500 hPa peak |
| va_6hr | atmos | m s-1 | -90 | ~0 | 90 | 6-hourly extreme — jet-core / wave |
| vas | atmos | m s-1 | -30 | ~0 | 30 | ERA5 10m wind (mon default) |
| vas_day | atmos | m s-1 | -50 | ~0 | 50 | Daily extreme — ERA5 storm/TC 10m |
| vas_3hr | atmos | m s-1 | -65 | ~0 | 65 | 3-hourly extreme |
| vas_1hr | atmos | m s-1 | -75 | ~0 | 75 | Hourly extreme — Cat-5 TC (Saffir-Simpson) |
| vegFrac | land | % | 0 | 70 | 100 | LUH2 vegetated fraction |
| vegHeight | land | m | 0 | ~5 | ~50 | Tree canopy height; Simard 2011. NB: LPJ-GUESS does not emit a grass-only height (vegHeightGrass), so cmor falls back to the tree-dominated field (Laszlo round 2). Bounds revisit pending model-side fix. |
| vmo | ocean | kg s-1 | -1e12 | 0 | 1e12 | Same as umo (y-component) |
| vo | ocean | m s-1 | -2 | ~0 | 2 | WOCE/Argo currents |
| volcello | ocean | m3 | 1e6 | 1e10 | 1e12 | Grid cell volume |
| volo | ocean | m3 | 1.33e18 | 1.335e18 | 1.34e18 | Global ocean volume ~1.335e18 m3 |
| vos | ocean | m s-1 | -2 | ~0 | 2.5 | OSCAR surface currents |
| vsf | ocean | kg m-2 s-1 | -1e-4 | ~0 | 1e-4 | Virtual salt flux |
| vsfcorr | ocean | kg m-2 s-1 | -1e-4 | ~0 | 1e-4 | Salt flux correction; exactly 0 in coupled runs (no SSS restoring) |
| vsfsit | ocean | kg m-2 s-1 | -1e-4 | ~0 | 1e-4 | Ice-related virtual salt flux |
| wap | atmos | Pa s-1 | -5 | ~0 | 5 | ERA5 omega |
| wetlandCH4 | land | kg m-2 s-1 | -1e-9 | ~5e-12 | ~5e-9 | Net wetland CH4 ~150-200 TgCH4/yr global; Saunois 2020 |
| wetlandCH4cons | land | kg m-2 s-1 | 0 | ~1e-12 | ~5e-10 | Methanotrophy ~30 TgCH4/yr; smaller than production |
| wetlandCH4prod | land | kg m-2 s-1 | 0 | ~6e-12 | ~5e-9 | Methanogenesis ~250 TgCH4/yr; Saunois 2020 |
| wetlandFrac | land | % | 0 | ~6 | 100 | Global wetland ~1.5e7 km^2 (Lehner & Doll 2004); ~6% of land |
| wfo | ocean | kg m-2 s-1 | -1e-4 (evap) | ~0 | 1e-4 (precip) | GPCP/CMIP6 E-P |
| wmo | ocean | kg s-1 | -1e10 | ~0 | 1e10 | Cell vertical mass transport |
| wo | ocean | m s-1 | -1e-3 | ~0 | 1e-3 | Ocean vertical velocity |
| wsg | atmos | m s-1 | 0 | 8 | 60 | ERA5 wind gust |
| zg | atmos | m | -500 | ~1e4 (mid-trop) | 35000 | Geopotential height profile |
| zos | ocean | m | -2 | 0 | 1.5 | AVISO SSH anomaly |
| zossq | ocean | m2 | 0 | 0.1 | 4 | zos squared |
| zostoga | ocean | m | -0.01 | 0 | 0.01 | piControl thermosteric ~0 |
