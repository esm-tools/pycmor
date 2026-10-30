# HR: fixing the plev3 and plev7h output in running experiments

State on 2026-10-19. This fixes the output from the next chunk on. Nothing is rerun.

## The problem

The 6-hourly pressure-level streams `atmos_6h_pl3_*` and `atmos_6h_pl7h_*` contain data from the wrong levels. The labels in the files are right; the data under them are not.

| Stream | Labels in the file (hPa) | Data actually from (hPa) |
|---|---|---|
| `atmos_6h_pl3_{t,u,v}` | 850, 500, 250 | 1000, 925, 850 |
| `atmos_6h_pl7h_{ta,ua,va,hus,zg}` | 1000, 925, 850, 700, 500, 250, 100 | 1000, 925, 850, 700, 600, 500, 400 |

Measured on piControl 1852: the January mean of `atmos_6h_pl3_t` matches `atmos_mon_pl_ta` at 1000/925/850 hPa to 0.03 K, and is 8 to 55 K off at the labelled levels.

## Why

FullPos does all vertical interpolation, and OpenIFS takes its pressure levels from one XIOS axis, `pressure_levels`, the 19 standard levels (`suxios.F90`, it reads that axis into `RFP3P`). XIOS itself does no vertical interpolation here.

In `cmip7/axis_def.xml`, `pressure_levels_3` and `pressure_levels_7h` are separate axes that only list their own values. They are not linked to `pressure_levels`, so XIOS gets a 19-level field for a 3- or 7-level axis and writes the first 3 or 7 levels under the new labels.

All plev3 and plev7h levels are part of the 19 standard levels. So the fix is to derive both axes from `pressure_levels` by a linear interpolation, which at these levels is an exact copy. FullPos, the 19-level output and every other stream stay exactly as they are.

An index subset (`zoom_axis index=...`) does not work: XIOS 2.5 turns the index list into its min-max range and writes every level in between (seen in abrupt-4xCO2 2002: 7 instead of 3 and 13 instead of 7 levels).

## What to change

One file per experiment:

```
<experiment>/.venv_esmtools/lib/python3.9/site-packages/esm_tools/namelists/oifs/48r1/xios/cmip7/axis_def.xml
```

Each experiment has its own copy there, and it is what gets copied into every new chunk. It is identical to `<experiment>/config/xios/axis_def.xml`.

Replace these two lines:

```xml
    <axis id="pressure_levels_3" long_name="vertical pressure levels plev3" n_glo="3" value="(0,2)[85000.0 50000.0 25000.0]" />
    <axis id="pressure_levels_7h" long_name="vertical pressure levels plev7h" n_glo="7" value="(0,6)[100000.0 92500.0 85000.0 70000.0 50000.0 25000.0 10000.0]" />
```

with (Nadine's version, in use since abrupt-4xCO2 2003):

```xml
    <axis id="pressure_levels_3" long_name="vertical pressure levels plev3" n_glo="3" value="(0,2)[85000.0 50000.0 25000.0]" >
      <interpolate_axis type="polynomial" order="1" />
    </axis>
    <axis id="pressure_levels_7h" long_name="vertical pressure levels plev7h" n_glo="7" value="(0,6)[92500.0 85000.0 70000.0 60000.0 50000.0 25000.0 5000.0]" >
      <interpolate_axis type="polynomial" order="1" />
    </axis>
```

plev7h thereby also gets the levels CMIP asks for. The old axis listed 1000 and 100 hPa, which are not in the CMIP plev7h.

Checked on abrupt-4xCO2 2003: every plev3 and plev7h level matches `atmos_mon_pl_ta` at the same pressure to 0.03 K (January means).

Leave `pressure_levels_6` alone. It has the same fault, but `atmos_3h_pl6` is switched off, and its levels (950, 900, 800, 750 hPa) are not among the 19, so it cannot be fixed this way.

Do not touch the work directory of a chunk that is already running. The change takes effect with the next chunk that esm_runscripts prepares.

## Which experiments

| Experiment | Year running on 2026-10-19 | Runs until | Action |
|---|---|---|---|
| `AWI-ESM3-VEG-HR-CMIP7-piControl` | 2082 | 2249 | edit |
| `AWI-ESM3-VEG-HR-CMIP7-historical_1949` | 1934 | 2021 | edit |
| `AWI-ESM3-VEG-HR-CMIP7-abrupt4xCO2_1949` | 2001 | 2149 | edit |
| `AWI-ESM3-VEG-HR-CMIP7-1pctCO2_1949` | 1999 | 1999 | finished, nothing to do |

Also edit any other HR experiment that is still running with the CMIP7 XML, for example `piControl-fullout` or `piControl-minout` if they are production runs.

Write down, per experiment, the first model year written with the fix. The pycmor side needs it (see below).

## Checking the first fixed chunk

1. In the new chunk, `config/xios/axis_def.xml` shows the `zoom_axis` lines.
2. If XIOS does not accept the change, the chunk stops during initialisation. Then put the old two lines back and resubmit.
3. When the chunk has finished, `ncdump -h atmos_6h_pl3_t_<YEAR>-<YEAR>.nc` should show 3 levels, values 85000, 50000 and 25000, and the dimension name `pressure_levels_3` as before.
4. Compare the data against the daily 19-level stream:

```python
import xarray as xr, numpy as np
D = "<experiment>/outdata/oifs"
Y = "<YEAR>"
day = xr.open_dataset(f"{D}/atmos_day_pl_ta_{Y}-{Y}.nc")
lev = [d for d in day.dims if "pressure" in d][0]
ref = day.ta.isel(time_counter=0)
for stream, var in [("pl3", "t"), ("pl7h", "ta")]:
    f = xr.open_dataset(f"{D}/atmos_6h_{stream}_{var}_{Y}-{Y}.nc")
    plev = [d for d in f.dims if "pressure" in d][0]
    first_day = f[var].isel(time_counter=slice(0, 4)).mean("time_counter")
    for p in f[plev].values:
        diff = float(abs(first_day.sel({plev: p}) - ref.sel({lev: p})).mean())
        print(stream, int(p), f"{diff:.3f} K")
```

Every level should come out at a few hundredths of a kelvin. Before the fix, the same check gives 8 to 55 K for plev3.

## What happens to the years before the fix

- The plev3 and plev7h files from years before the fix are wrong and cannot be repaired. HR has no other 6-hourly stream on all 19 levels or on model levels to rebuild them from.
- They must not be cmorized. Jan switches off `ta_6hr`, `ua_6hr` and `va_6hr` in `core_atm` for those years, and the five plev7h rules stay off until the fixed years are reached. For that he needs the first fixed year per experiment.
- If your 1pctCO2 cmorization has already written `ta`, `ua` or `va` at 6hr on `p3`, delete those files. Every year of 1pctCO2 has the wrong data.
- The raw plev3 and plev7h files of the pre-fix years are about 16 GB and 60 GB per year in piControl. Whether to delete them is up to you and Jan.

## Not covered here

- esm_tools itself. The `.venv_esmtools` copies are what the running experiments use. New HR experiments installed from esm_tools need the same change in the repository; Jan takes care of that.
- LR (v3.5.1). It gets a different fix that also adds the plev6 levels.
