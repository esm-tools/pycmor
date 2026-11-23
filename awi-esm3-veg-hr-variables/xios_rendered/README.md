# XIOS XML as the HR production run used it

Copied unchanged from the work directory of the newest AWI-ESM3-VEG-HR
piControl leg (2141, `awiesm3-v3.4.2/AWI-ESM3-VEG-HR-CMIP7-piControl`), so
there is no jinja left to render. These are the files the pycmor rules in this
directory are written against.

- OpenIFS 48r1: `iodef.xml`, `context_ifs.xml`, `field_def.xml`,
  `file_def.xml`, `axis_def.xml`, `domain_def.xml`, `grid_def.xml`
- FESOM 2: `context_fesom.xml`, `field_def_fesom.xml`, `file_def_fesom.xml`,
  `axis_def_fesom.xml`, `domain_def_fesom.xml`, `grid_def_fesom.xml`

`iodef.xml` loads both contexts; for an OpenIFS-only setup drop the `fesom`
context line.

`field_def.xml` asks OpenIFS for some fields that come from AWI's 48r1 XIOS
interface rather than stock 48r1, for example the CMIP7 relative humidity
(`send_cmip7_rh.F90`). A model that does not send a field id leaves that
field empty or makes XIOS abort, so drop those fields from `file_def.xml`.

The templates these were rendered from are one level up
(`*_def*.xml(.j2)`, from esm_tools `namelists/oifs/48r1/xios/cmip7`) and in
esm_tools `namelists/fesom2/xios_xml_cmip7`.

The `pressure_levels_7h` axis here is the CMIP7 set (925 850 700 600 500 250
50 hPa). HR years written before 2026-10-21 used the CMIP6 set, which is why
the HR plev7h rules in `cap7_atm` stay commented out.
