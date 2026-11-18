# Output-vs-Expected Discrepancies

Auto-flagged 142 variables (out of 408 files) whose actual stats fall outside expected physical ranges.

Flag criteria: actual mean ≪/≫ expected mean by ≥ 100×, sign flip, mean outside expected min–max by >20%, or max/min extending beyond expected by ≥10×.

Sorted by severity (sign flips first).

| Test set | Variable | Units | Actual min | Actual mean | Actual max | Exp min | Exp mean | Exp max | Issue | Expected source |
|---|---|---|---|---|---|---|---|---|---|---|
| cap7_seaice_core2_test | evspsbl | kg m-2 s-1 | -0.000145 | -2.54e-05 | 1.12e-05 | 0.0 | 3.2e-05 | 0.0002 | sign flip: actual -2.54e-05 vs expected 3.2e-05 | Global mean E ~2.8 mm/day; GPCP/ERA5 |
| lrcs_ocean_core2_test | evspsbl | kg m-2 s-1 | -0.000145 | -2.54e-05 | 1.12e-05 | 0.0 | 3.2e-05 | 0.0002 | sign flip: actual -2.54e-05 vs expected 3.2e-05 | Global mean E ~2.8 mm/day; GPCP/ERA5 |
| lrcs_ocean_core2_test | mlotst | m | -3.28e+03 | -48.7 | -7.5 | 10.0 | 60.0 | 2000.0 | sign flip: actual -48.7 vs expected 60; mean -48.7 < expected min 10; min -3.28e+03 ≪ expected min 10 | de Boyer Montegut climatology; deep Labrador/Weddell |
| lrcs_seaice_core2_test | siflcondbot | W m-2 | -185 | -12.1 | 36.1 | -50.0 | 5.0 | 100.0 | sign flip: actual -12.1 vs expected 5 | Maykut conductive flux |
| cap7_aerosol_tco95_test | od550aer | 1 | 0 | 5.33e-08 | 1.57e-05 | 0.02 | 0.12 | 1.0 | actual mean 5.33e-08 ≪ expected 0.12; mean 5.33e-08 < expected min 0.02 | MODIS/AERONET AOD climatology |
| cap7_atm_tco95_test | hfls | W m-2 | -970 | 364 | 5.1e+03 | 0.0 | 80.0 | 250.0 | mean 364 > expected max 250; max 5.1e+03 ≫ expected max 250 | LH flux; CERES/ERA5 |
| cap7_atm_tco95_test | hfss | W m-2 | -1.19e+03 | 73.9 | 3.81e+03 | -50.0 | 20.0 | 150.0 | max 3.81e+03 ≫ expected max 150; min -1.19e+03 ≪ expected min -50 | SH flux; CERES/ERA5 |
| cap7_atm_tco95_test | hur | % | 0 | 0 | 0 | 0.0 | 60.0 | 100.0 | actual mean 0 ≪ expected 60 | RH profile; ERA5 |
| cap7_atm_tco95_test | prc | kg m-2 s-1 | 0 | 8.83e-05 | 0.00886 | 0.0 | 1.5e-05 | 0.0002 | max 0.00886 ≫ expected max 0.0002 | Convective fraction ~50% |
| cap7_atm_tco95_test | prsn | kg m-2 s-1 | 0 | 2.47e-05 | 0.00687 | 0.0 | 5e-06 | 0.0001 | max 0.00687 ≫ expected max 0.0001 | Snowfall ~15% of precip |
| cap7_atm_tco95_test | rsds | W m-2 | 0 | 994 | 6.68e+03 | 0.0 | 185.0 | 400.0 | mean 994 > expected max 400; max 6.68e+03 ≫ expected max 400 | CERES surface SW down annual |
| cap7_atm_tco95_test | rtmt | W m-2 | -619 | 826 | 2.2e+03 | -200.0 | 0.0 | 200.0 | mean 826 > expected max 200; max 2.2e+03 ≫ expected max 200 | Net TOA ~0 in piControl |
| cap7_land_tco95_test | burntFractionAll | % | 0 | 0 | 0 | 0.0 | 1.0 | 30.0 | actual mean 0 ≪ expected 1 | GFED4 climatology, savanna fire belt |
| cap7_land_tco95_test | cLand | kg m-2 | 0 | 0 | 0 | 0.0 | 25.0 | 80.0 | actual mean 0 ≪ expected 25 | Total land C ~2000 PgC / land area; IPCC AR6 carbon cycle |
| cap7_land_tco95_test | cLeaf | kg m-2 | 0 | 0 | 0 | 0.0 | 0.3 | 2.0 | actual mean 0 ≪ expected 0.3 | Leaf C; tropical forest LAI; TRENDY |
| cap7_land_tco95_test | cLitter | kg m-2 | 0 | 0 | 0 | 0.0 | 2.0 | 15.0 | actual mean 0 ≪ expected 2 | Litter pool; IPCC AR6 |
| cap7_land_tco95_test | cLitterCwd | kg m-2 | 0 | 0 | 0 | 0.0 | 1.0 | 10.0 | actual mean 0 ≪ expected 1 | CWD stocks; Pan et al. 2011 |
| cap7_land_tco95_test | cLitterSubSurf | kg m-2 | 0 | 0 | 0 | 0.0 | 1.0 | 8.0 | actual mean 0 ≪ expected 1 | Belowground litter subset |
| cap7_land_tco95_test | cLitterSurf | kg m-2 | 0 | 0 | 0 | 0.0 | 1.0 | 8.0 | actual mean 0 ≪ expected 1 | Aboveground litter subset |
| cap7_land_tco95_test | cOther | kg m-2 | 0 | 0 | 0 | 0.0 | 0.2 | 3.0 | actual mean 0 ≪ expected 0.2 | Reproductive/other tissues small fraction |
| cap7_land_tco95_test | cRoot | kg m-2 | 0 | 0 | 0 | 0.0 | 1.0 | 10.0 | actual mean 0 ≪ expected 1 | Root C; Jackson 1997 |
| cap7_land_tco95_test | cSoil | kg m-2 | 0 | 0 | 0 | 0.0 | 15.0 | 100.0 | actual mean 0 ≪ expected 15 | Soil C HWSD; peatlands high |
| cap7_land_tco95_test | cStem | kg m-2 | 0 | 0 | 0 | 0.0 | 3.0 | 25.0 | actual mean 0 ≪ expected 3 | Stem C; tropical forests |
| cap7_land_tco95_test | cVeg | kg m-2 | 0 | 0 | 0 | 0.0 | 5.0 | 35.0 | actual mean 0 ≪ expected 5 | Vegetation C; IPCC AR6 ~450 PgC |
| cap7_land_tco95_test | cropFrac | % | 0 | 0 | 0 | 0.0 | 5.0 | 100.0 | actual mean 0 ≪ expected 5 | 1850 cropland ~5% global land; LUH2 |
| cap7_land_tco95_test | fCLandToOcean | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 1e-11 | 1e-09 | actual mean 0 ≪ expected 1e-11 | Riverine C ~0.9 PgC/yr; IPCC AR6 |
| cap7_land_tco95_test | fFire | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 5e-11 | 5e-09 | actual mean 0 ≪ expected 5e-11 | Natural fire dominant in piControl |
| cap7_land_tco95_test | fFireAll | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 5e-11 | 5e-09 | actual mean 0 ≪ expected 5e-11 | GFED ~2 PgC/yr global |
| cap7_land_tco95_test | fFireNat | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 5e-11 | 5e-09 | actual mean 0 ≪ expected 5e-11 | GFED natural |
| cap7_land_tco95_test | fLitterFire | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 2e-11 | 2e-09 | actual mean 0 ≪ expected 2e-11 | Litter burning component |
| cap7_land_tco95_test | fLitterSoil | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 2e-09 | 5e-08 | actual mean 0 ≪ expected 2e-09 | Litter->soil turnover |
| cap7_land_tco95_test | fVegFire | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 3e-11 | 3e-09 | actual mean 0 ≪ expected 3e-11 | Vegetation fire C flux |
| cap7_land_tco95_test | fVegLitter | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 2e-09 | 5e-08 | actual mean 0 ≪ expected 2e-09 | Litterfall ~60 PgC/yr |
| cap7_land_tco95_test | gpp | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 3.5e-08 | 1e-07 | actual mean 0 ≪ expected 3.5e-08 | GPP ~120 PgC/yr; Beer 2010 |
| cap7_land_tco95_test | grassFrac | % | 0 | 0 | 0 | 0.0 | 20.0 | 100.0 | actual mean 0 ≪ expected 20 | Natural grass coverage; LUH2 |
| cap7_land_tco95_test | mrro | kg m-2 s-1 | 0 | 1.31e-05 | 0.0103 | 0.0 | 1e-05 | 0.0002 | max 0.0103 ≫ expected max 0.0002 | GRDC/CMIP6 land runoff |
| cap7_land_tco95_test | npp | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 1.9e-08 | 5e-07 | actual mean 0 ≪ expected 1.9e-08 | CMIP6 NPP, tropical forests |
| cap7_land_tco95_test | pastureFrac | % | 0 | 0 | 0 | 0.0 | 3.0 | 50.0 | actual mean 0 ≪ expected 3 | LUH2 1850 pasture mostly low |
| cap7_land_tco95_test | prveg | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 1e-05 | 0.0003 | actual mean 0 ≪ expected 1e-05 | Canopy-intercepted precip |
| cap7_land_tco95_test | ra | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 2e-08 | 5e-07 | actual mean 0 ≪ expected 2e-08 | Autotrophic resp ~60 PgC/yr |
| cap7_land_tco95_test | raLeaf | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 5e-09 | 1e-07 | actual mean 0 ≪ expected 5e-09 | Leaf resp fraction of ra |
| cap7_land_tco95_test | raOther | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 5e-09 | 1e-07 | actual mean 0 ≪ expected 5e-09 | Small ra fraction |
| cap7_land_tco95_test | raRoot | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 5e-09 | 1e-07 | actual mean 0 ≪ expected 5e-09 | Root resp fraction |
| cap7_land_tco95_test | raStem | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 5e-09 | 1e-07 | actual mean 0 ≪ expected 5e-09 | Stem resp fraction |
| cap7_land_tco95_test | residualFrac | % | 0 | 0 | 0 | 0.0 | 5.0 | 100.0 | actual mean 0 ≪ expected 5 | Bare/urban/other residual |
| cap7_land_tco95_test | rh | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 1.8e-08 | 3e-07 | actual mean 0 ≪ expected 1.8e-08 | Heterotrophic resp, CMIP6 |
| cap7_land_tco95_test | rhLitter | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 9e-09 | 1.5e-07 | actual mean 0 ≪ expected 9e-09 | Litter decomp fraction |
| cap7_land_tco95_test | rhSoil | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 9e-09 | 1.5e-07 | actual mean 0 ≪ expected 9e-09 | Soil rh component |
| cap7_land_tco95_test | shrubFrac | % | 0 | 0 | 0 | 0.0 | 5.0 | 100.0 | actual mean 0 ≪ expected 5 | LUH2/CMIP6 land cover |
| cap7_land_tco95_test | tran | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 1.5e-05 | 0.0001 | actual mean 0 ≪ expected 1.5e-05 | GLEAM/FLUXNET transpiration |
| cap7_land_tco95_test | treeFrac | % | 0 | 0 | 0 | 0.0 | 30.0 | 100.0 | actual mean 0 ≪ expected 30 | LUH2 preindustrial ~30% |
| cap7_land_tco95_test | vegFrac | % | 0 | 0 | 0 | 0.0 | 70.0 | 100.0 | actual mean 0 ≪ expected 70 | LUH2 vegetated fraction |
| cap7_seaice_core2_test | sithick | m | 1.74e-16 | 1.25 | 164 | 0.0 | 0.3 | 8.0 | max 164 ≫ expected max 8 | PIOMAS/ICESat |
| core_atm_tco95_test | hfss | W m-2 | -375 | 73.9 | 1.62e+03 | -50.0 | 20.0 | 150.0 | max 1.62e+03 ≫ expected max 150 | SH flux; CERES/ERA5 |
| core_atm_tco95_test | hur | % | 0 | 0 | 0 | 0.0 | 60.0 | 100.0 | actual mean 0 ≪ expected 60 | RH profile; ERA5 |
| core_atm_tco95_test | pr | kg m-2 s-1 | 0 | 0.00016 | 0.0157 | 0.0 | 3e-05 | 0.0003 | max 0.0157 ≫ expected max 0.0003 | GPCP global mean precip |
| core_atm_tco95_test | prc | kg m-2 s-1 | 0 | 8.82e-05 | 0.00232 | 0.0 | 1.5e-05 | 0.0002 | max 0.00232 ≫ expected max 0.0002 | Convective fraction ~50% |
| core_atm_tco95_test | tauu | Pa | -4.53 | 0.0824 | 9.78 | -0.5 | 0.0 | 0.5 | max 9.78 ≫ expected max 0.5 | ERA5 wind stress |
| core_atm_tco95_test | tauv | Pa | -5.1 | 0.00301 | 7.04 | -0.5 | 0.0 | 0.5 | max 7.04 ≫ expected max 0.5; min -5.1 ≪ expected min -0.5 | ERA5 wind stress |
| core_land_tco95_test | mrros | kg m-2 s-1 | 0 | 5.69e-06 | 0.00113 | 0.0 | 5e-06 | 0.0001 | max 0.00113 ≫ expected max 0.0001 | Surface runoff fraction of total |
| extra_land_tco95_test | c3PftFrac | % | 0 | 0 | 0 | 0.0 | 25.0 | 100.0 | actual mean 0 ≪ expected 25 | LUH2/CMIP6 PFT distribution |
| extra_land_tco95_test | c4PftFrac | % | 0 | 0 | 0 | 0.0 | 5.0 | 100.0 | actual mean 0 ≪ expected 5 | LUH2 C4 grasses tropical |
| extra_land_tco95_test | lai | 1 | 0 | 0 | 0 | 0.0 | 1.2 | 7.0 | actual mean 0 ≪ expected 1.2 | MODIS LAI climatology, tropical forests peak |
| lrcs_land_tco95_test | evspsblveg | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 1e-05 | 0.00015 | actual mean 0 ≪ expected 1e-05 | Canopy evap/transpiration |
| lrcs_land_tco95_test | mrfso | kg m-2 | 0 | 0 | 0 | 0.0 | 200.0 | 5000.0 | actual mean 0 ≪ expected 200 | Frozen soil water, permafrost regions |
| lrcs_land_tco95_test | sftgif | % | 0 | 0 | 0 | 0.0 | 3.0 | 100.0 | actual mean 0 ≪ expected 3 | Glacier/ice fraction (Greenland/Antarctica=100) |
| lrcs_ocean_core2_test | difmxylo | m2 s-1 | 0.0001 | 0.0119 | 0.736 | 0.0 | 1000.0 | 10000.0 | actual mean 0.0119 ≪ expected 1e+03 | Laplacian horizontal viscosity typical |
| lrcs_ocean_core2_test | difvho | m2 s-1 | 1e-05 | 0.014 | 1.67 | 1e-06 | 0.0001 | 0.01 | actual mean 0.014 ≫ expected 0.0001; mean 0.014 > expected max 0.01; max 1.67 ≫ expected max 0.01 | Vertical diffusivity; Munk/Ledwell |
| lrcs_ocean_core2_test | difvso | m2 s-1 | 1e-05 | 0.014 | 1.67 | 1e-06 | 0.0001 | 0.01 | actual mean 0.014 ≫ expected 0.0001; mean 0.014 > expected max 0.01; max 1.67 ≫ expected max 0.01 | Vertical salt diffusivity |
| lrcs_ocean_core2_test | msftbarot | kg s-1 | -4.03e+12 | 2.82e+10 | 3.93e+12 | -200000000000.0 | 0.0 | 200000000000.0 | max 3.93e+12 ≫ expected max 2e+11; min -4.03e+12 ≪ expected min -2e+11 | ACC ~150 Sv *1025 kg/m3 |
| lrcs_ocean_core2_test | obvfsq | s-2 | -7.01e-05 | 5.19e-05 | 0.0222 | 0.0 | 1e-05 | 0.001 | max 0.0222 ≫ expected max 0.001 | N^2 pycnocline values |
| lrcs_ocean_core2_test | opottempdiff | W m-2 | -2.87e+03 | -1.18 | 2.97e+03 | -50.0 | 0.0 | 50.0 | max 2.97e+03 ≫ expected max 50; min -2.87e+03 ≪ expected min -50 | Diapycnal mixing tendency small |
| lrcs_ocean_core2_test | opottempmint | degC kg m-2 | -3.68e+03 | 8.38e+03 | 5.14e+04 | -10000000.0 | 10000000.0 | 100000000.0 | actual mean 8.38e+03 ≪ expected 1e+07 | rho*theta*depth, ~1025*10*4000 tropics |
| lrcs_ocean_core2_test | phcint | J m-2 | -3.92e+03 | 8.38e+03 | 5.16e+04 | 0.0 | 10000000000.0 | 50000000000.0 | actual mean 8.38e+03 ≪ expected 1e+10 | Ocean heat content rho*cp*T*H |
| lrcs_ocean_core2_test | somint | g m-2 | 184 | 9.43e+04 | 2.15e+05 | 100000.0 | 140000000.0 | 200000000.0 | actual mean 9.43e+04 ≪ expected 1.4e+08 | rho*S*H for H~4000m |
| lrcs_ocean_core2_test | wfo | kg m-2 s-1 | -0.00422 | -9.67e-06 | 0.00124 | -0.0001 | 0.0 | 0.0001 | max 0.00124 ≫ expected max 0.0001; min -0.00422 ≪ expected min -0.0001 | GPCP/CMIP6 E-P |
| lrcs_seaice_core2_test | siflcondtop | W m-2 | -169 | 24.1 | 1.88e+03 | -50.0 | 5.0 | 100.0 | max 1.88e+03 ≫ expected max 100 | Maykut conductive flux |
| lrcs_seaice_core2_test | siflfwbot | kg m-2 s-1 | -0.00122 | -9.05e-06 | 0.00189 | -0.0001 | 0.0 | 0.0001 | max 0.00189 ≫ expected max 0.0001; min -0.00122 ≪ expected min -0.0001 | CMIP6 ice FW flux |
| lrcs_seaice_core2_test | simprefrozen | m | 2.75e-11 | 0.239 | 3.35 | 0.0 | 0.02 | 0.3 | max 3.35 ≫ expected max 0.3 | CICE topo melt-pond |
| lrcs_seaice_core2_test | sisnhc | J m-2 | -3.93e+08 | -2.28e+07 | -7.74e-07 | -20000000.0 | -2000000.0 | 0.0 | min -3.93e+08 ≪ expected min -2e+07 | c_snow*rho*h*dT |
| veg_atm_tco95_test | emibbbc | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 1e-13 | 1e-10 | actual mean 0 ≪ expected 1e-13 | 1850 BB BC emissions; CMIP6 input4MIPs |
| veg_atm_tco95_test | emibbch4 | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 1e-12 | 1e-09 | actual mean 0 ≪ expected 1e-12 | 1850 BB CH4; CMIP6 |
| veg_atm_tco95_test | emibbco | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 1e-11 | 1e-08 | actual mean 0 ≪ expected 1e-11 | 1850 BB CO; CMIP6 |
| veg_atm_tco95_test | emibboa | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 1e-12 | 1e-09 | actual mean 0 ≪ expected 1e-12 | 1850 BB OA; CMIP6 |
| veg_atm_tco95_test | emibbso2 | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 1e-13 | 1e-10 | actual mean 0 ≪ expected 1e-13 | 1850 BB SO2; CMIP6 |
| veg_atm_tco95_test | emibbvoc | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 1e-12 | 1e-09 | actual mean 0 ≪ expected 1e-12 | 1850 BB NMVOC; CMIP6 |
| veg_atm_tco95_test | hfls | W m-2 | -1.41e+03 | 364 | 6.64e+03 | 0.0 | 80.0 | 250.0 | mean 364 > expected max 250; max 6.64e+03 ≫ expected max 250 | LH flux; CERES/ERA5 |
| veg_atm_tco95_test | hfss | W m-2 | -2.66e+03 | 73.9 | 4.96e+03 | -50.0 | 20.0 | 150.0 | max 4.96e+03 ≫ expected max 150; min -2.66e+03 ≪ expected min -50 | SH flux; CERES/ERA5 |
| veg_atm_tco95_test | prsn | kg m-2 s-1 | 0 | 2.47e-05 | 0.00687 | 0.0 | 5e-06 | 0.0001 | max 0.00687 ≫ expected max 0.0001 | Snowfall ~15% of precip |
| veg_atm_tco95_test | rsds | W m-2 | 0 | 994 | 6.68e+03 | 0.0 | 185.0 | 400.0 | mean 994 > expected max 400; max 6.68e+03 ≫ expected max 400 | CERES surface SW down annual |
| veg_atm_tco95_test | rsus | W m-2 | -0.000556 | 226 | 4.37e+03 | 0.0 | 24.0 | 300.0 | max 4.37e+03 ≫ expected max 300 | Surface upward SW (albedo*rsds) |
| veg_land_tco95_test | cLitterLut | kg m-2 | 0 | 0 | 0 | 0.0 | 2.0 | 15.0 | actual mean 0 ≪ expected 2 | Per-tile litter; LUH2 |
| veg_land_tco95_test | cSoilLut | kg m-2 | 0 | 0 | 0 | 0.0 | 15.0 | 100.0 | actual mean 0 ≪ expected 15 | Per-tile soil C |
| veg_land_tco95_test | cVegLut | kg m-2 | 0 | 0 | 0 | 0.0 | 5.0 | 35.0 | actual mean 0 ≪ expected 5 | Per-tile vegetation C |
| veg_land_tco95_test | cropFrac | % | 0 | 0 | 0 | 0.0 | 5.0 | 100.0 | actual mean 0 ≪ expected 5 | 1850 cropland ~5% global land; LUH2 |
| veg_land_tco95_test | dsn | kg m-2 | -5.49e+04 | 1.4 | 5.97e+04 | -500.0 | 0.0 | 500.0 | max 5.97e+04 ≫ expected max 500; min -5.49e+04 ≪ expected min -500 | SWE change annual; ~0 in steady state |
| veg_land_tco95_test | fBNF | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 3e-12 | 3e-11 | actual mean 0 ≪ expected 3e-12 | Biological N fixation ~100 TgN/yr; Vitousek |
| veg_land_tco95_test | fNLitterSoil | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 3e-11 | 1e-09 | actual mean 0 ≪ expected 3e-11 | N litter-to-soil |
| veg_land_tco95_test | fNgasFire | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 1e-12 | 1e-10 | actual mean 0 ≪ expected 1e-12 | N from fires small fraction |
| veg_land_tco95_test | fNup | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 3e-10 | 3e-08 | actual mean 0 ≪ expected 3e-10 | Plant N uptake; Cleveland |
| veg_land_tco95_test | gppLut | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 3.5e-08 | 1e-07 | actual mean 0 ≪ expected 3.5e-08 | Per-tile GPP |
| veg_land_tco95_test | grassFrac | % | 0 | 0 | 0 | 0.0 | 20.0 | 100.0 | actual mean 0 ≪ expected 20 | Natural grass coverage; LUH2 |
| veg_land_tco95_test | hfdsl | W m-2 | -9.78e+03 | -10.5 | 6.13e+03 | -100.0 | 0.0 | 100.0 | max 6.13e+03 ≫ expected max 100; min -9.78e+03 ≪ expected min -100 | Ground heat flux annual ~0 |
| veg_land_tco95_test | laiLut | 1 | 0 | 0 | 0 | 0.0 | 1.2 | 7.0 | actual mean 0 ≪ expected 1.2 | MODIS per-tile LAI |
| veg_land_tco95_test | mrro | kg m-2 s-1 | 0 | 1.31e-05 | 0.015 | 0.0 | 1e-05 | 0.0002 | max 0.015 ≫ expected max 0.0002 | GRDC/CMIP6 land runoff |
| veg_land_tco95_test | mrrob | kg m-2 s-1 | 0 | 7.45e-06 | 0.0104 | 0.0 | 1e-05 | 0.0001 | max 0.0104 ≫ expected max 0.0001 | Subsurface runoff, wettest tropics |
| veg_land_tco95_test | mrros | kg m-2 s-1 | 0 | 5.69e-06 | 0.00774 | 0.0 | 5e-06 | 0.0001 | max 0.00774 ≫ expected max 0.0001 | Surface runoff fraction of total |
| veg_land_tco95_test | nLand | kg m-2 | 9.9e-05 | 0.000101 | 0.00339 | 0.0 | 1.5 | 20.0 | actual mean 0.000101 ≪ expected 1.5 | Total N in soil+veg, ~200 PgN / land |
| veg_land_tco95_test | nLitter | kg m-2 | 0 | 0 | 0 | 0.0 | 0.05 | 1.0 | actual mean 0 ≪ expected 0.05 | Litter N, small pool |
| veg_land_tco95_test | nSoil | kg m-2 | 0 | 0 | 0 | 0.0 | 1.0 | 15.0 | actual mean 0 ≪ expected 1 | Soil N dominates total |
| veg_land_tco95_test | nVeg | kg m-2 | 0 | 0 | 0 | 0.0 | 0.1 | 2.0 | actual mean 0 ≪ expected 0.1 | Vegetation N pool |
| veg_land_tco95_test | nppLut | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 1.9e-08 | 5e-07 | actual mean 0 ≪ expected 1.9e-08 | Per-tile NPP |
| veg_land_tco95_test | raLut | kg m-2 s-1 | 0 | 0 | 0 | 0.0 | 2e-08 | 5e-07 | actual mean 0 ≪ expected 2e-08 | Per-tile ra |
| veg_land_tco95_test | rhLut | kg m-2 s-1 | 0 | 6.78e-16 | 8e-14 | 0.0 | 1.8e-08 | 3e-07 | actual mean 6.78e-16 ≪ expected 1.8e-08 | Per-tile rh |
| veg_land_tco95_test | sbl | kg m-2 s-1 | -0.000183 | 5.55e-07 | 0.000198 | -1e-05 | 1e-07 | 1e-05 | max 0.000198 ≫ expected max 1e-05; min -0.000183 ≪ expected min -1e-05 | Snow/ice sublimation small |
| veg_land_tco95_test | shrubFrac | % | 0 | 0 | 0 | 0.0 | 5.0 | 100.0 | actual mean 0 ≪ expected 5 | LUH2/CMIP6 land cover |
| veg_land_tco95_test | srfrad | W m-2 | -1.14e+03 | 427 | 5.52e+03 | -100.0 | 60.0 | 250.0 | mean 427 > expected max 250; max 5.52e+03 ≫ expected max 250; min -1.14e+03 ≪ expected min -100 | CERES land net radiation |
| veg_land_tco95_test | treeFrac | % | 0 | 0 | 0 | 0.0 | 30.0 | 100.0 | actual mean 0 ≪ expected 30 | LUH2 preindustrial ~30% |
| veg_land_tco95_test | treeFracBdlDcd | % | 0 | 0 | 0 | 0.0 | 5.0 | 100.0 | actual mean 0 ≪ expected 5 | LUH2 land cover |
| veg_seaice_core2_test | sisnhc | J m-2 | -1.43e+09 | -2.43e+04 | 0 | -20000000.0 | -2000000.0 | 0.0 | min -1.43e+09 ≪ expected min -2e+07 | c_snow*rho*h*dT |
| cap7_atm_tco95_test | rlds | W m-2 | 229 | 1.77e+03 | 3.02e+03 | 100.0 | 345.0 | 450.0 | mean 1.77e+03 > expected max 450 | CERES-EBAF surface LW down |
| cap7_atm_tco95_test | rldscs | W m-2 | 228 | 1.61e+03 | 2.88e+03 | 80.0 | 315.0 | 430.0 | mean 1.61e+03 > expected max 430 | Clear-sky LW down |
| cap7_atm_tco95_test | rlus | W m-2 | 351 | 2.12e+03 | 3.79e+03 | 150.0 | 398.0 | 520.0 | mean 2.12e+03 > expected max 520 | sigma*T^4, CERES |
| cap7_atm_tco95_test | rluscs | W m-2 | 378 | 2.11e+03 | 3.73e+03 | 150.0 | 398.0 | 520.0 | mean 2.11e+03 > expected max 520 | Same as rlus (clear-sky same surface T) |
| cap7_atm_tco95_test | rlut | W m-2 | 445 | 1.35e+03 | 2.29e+03 | 120.0 | 239.0 | 320.0 | mean 1.35e+03 > expected max 320 | CERES OLR |
| cap7_atm_tco95_test | rlutcs | W m-2 | 455 | 1.47e+03 | 2.28e+03 | 150.0 | 266.0 | 330.0 | mean 1.47e+03 > expected max 330 | Clear-sky OLR |
| cap7_atm_tco95_test | rsdscs | W m-2 | 0 | 1.29e+03 | 2.9e+03 | 0.0 | 245.0 | 450.0 | mean 1.29e+03 > expected max 450 | Clear-sky surface SW down |
| cap7_atm_tco95_test | rsdt | W m-2 | 0 | 1.79e+03 | 3.36e+03 | 0.0 | 340.0 | 550.0 | mean 1.79e+03 > expected max 550 | TOA incident SW, S0/4 |
| cap7_atm_tco95_test | rsut | W m-2 | 0 | 624 | 2.42e+03 | 0.0 | 100.0 | 400.0 | mean 624 > expected max 400 | CERES TOA reflected SW |
| cap7_atm_tco95_test | rsutcs | W m-2 | 0 | 397 | 2.41e+03 | 0.0 | 53.0 | 300.0 | mean 397 > expected max 300 | Clear-sky TOA reflected |
| core_atm_tco95_test | hfls | W m-2 | -194 | 364 | 2.47e+03 | 0.0 | 80.0 | 250.0 | mean 364 > expected max 250 | LH flux; CERES/ERA5 |
| core_atm_tco95_test | rlds | W m-2 | 348 | 1.77e+03 | 2.78e+03 | 100.0 | 345.0 | 450.0 | mean 1.77e+03 > expected max 450 | CERES-EBAF surface LW down |
| core_atm_tco95_test | rlus | W m-2 | 528 | 2.12e+03 | 3.63e+03 | 150.0 | 398.0 | 520.0 | mean 2.12e+03 > expected max 520 | sigma*T^4, CERES |
| core_atm_tco95_test | rlut | W m-2 | 576 | 1.34e+03 | 2.2e+03 | 120.0 | 239.0 | 320.0 | mean 1.34e+03 > expected max 320 | CERES OLR |
| core_atm_tco95_test | rlutcs | W m-2 | 576 | 1.47e+03 | 2.21e+03 | 150.0 | 266.0 | 330.0 | mean 1.47e+03 > expected max 330 | Clear-sky OLR |
| core_atm_tco95_test | rsds | W m-2 | 0 | 994 | 2.79e+03 | 0.0 | 185.0 | 400.0 | mean 994 > expected max 400 | CERES surface SW down annual |
| core_atm_tco95_test | rsdt | W m-2 | 0 | 1.79e+03 | 3.29e+03 | 0.0 | 340.0 | 550.0 | mean 1.79e+03 > expected max 550 | TOA incident SW, S0/4 |
| core_atm_tco95_test | rsut | W m-2 | 0 | 624 | 2.36e+03 | 0.0 | 100.0 | 400.0 | mean 624 > expected max 400 | CERES TOA reflected SW |
| core_atm_tco95_test | rsutcs | W m-2 | 0 | 397 | 2.36e+03 | 0.0 | 53.0 | 300.0 | mean 397 > expected max 300 | Clear-sky TOA reflected |
| veg_atm_tco95_test | rlds | W m-2 | 229 | 1.77e+03 | 3.02e+03 | 100.0 | 345.0 | 450.0 | mean 1.77e+03 > expected max 450 | CERES-EBAF surface LW down |
| veg_atm_tco95_test | rlus | W m-2 | 348 | 2.12e+03 | 4.6e+03 | 150.0 | 398.0 | 520.0 | mean 2.12e+03 > expected max 520 | sigma*T^4, CERES |
| veg_atm_tco95_test | rss | W m-2 | 7.15e-05 | 768 | 2.19e+03 | 0.0 | 160.0 | 350.0 | mean 768 > expected max 350 | Net SW surface |
