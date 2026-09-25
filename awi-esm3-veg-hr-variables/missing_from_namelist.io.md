# CMIP7 Ocean Variables — Missing from FESOM2 Output

Variables that FESOM2 currently cannot write or that need external data.

## Cannot be derived from FESOM output

### basin (Ofx)
Ocean basin classification index (Atlantic, Pacific, Indian, Arctic, Southern, etc.).
Not stored in mesh.nc or any FESOM output. Requires an external basin mask dataset
mapped onto the FESOM unstructured grid. Could potentially use regionmask Python
package to generate from coordinates, but this is external post-processing.

### hfgeou (Ofx)
Upward geothermal heat flux at sea floor. FESOM2 does not include geothermal
heating in its standard configuration. No output variable or forcing field found
in the source code. Would require adding a geothermal forcing module to FESOM2
and is not a small effort.

## Could be added with namelist/config changes

### zostoga (Omon)
Global average thermosteric sea level change. Not computed directly by FESOM2.
The CMOR diagnostics module (`gen_modules_cmor_diag.F90`) computes `pbo` (bottom
pressure) which includes a steric contribution, but deriving zostoga from it
requires non-trivial post-processing (global volume-weighted thermal expansion
integral). Alternatively, could be computed offline from thetao + so + depth
using the TEOS-10 equation of state, but this needs a dedicated pipeline step.

### umo / vmo / wmo (Omon)
Ocean mass transport in x/y/z directions. FESOM2 outputs only velocity fields
(u, v, w), not mass transports. Computing these requires:
- velocity × water density × cell cross-section area
- Density from equation of state (temp, salt, pressure)
- Cell areas from mesh
This is feasible in post-processing but needs a dedicated pipeline with
multiple input variables (velocity + temp + salt + mesh).

### masscello time-varying (Omon)
Time-varying grid-cell mass per area. Requires density × hnode (ALE layer
thickness). hnode is available in FESOM2 but not currently enabled in namelist.io.
Density must be computed from temperature and salinity via equation of state.
Once hnode is enabled, this is feasible in post-processing.
