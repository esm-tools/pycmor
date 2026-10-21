"""One block per netCDF file: data variables with shape, units and statistics,
and the global attributes that identify the dataset. Used by run.sh to keep
results in the log instead of tracking output files."""
import sys

import netCDF4 as nc
import numpy as np

GLOBALS = ("variable_id", "branded_variable", "frequency", "grid_label",
           "source_id", "experiment_id", "variant_label", "activity_id")

for path in sys.argv[1:]:
    print(f"file {path}")
    with nc.Dataset(path) as ds:
        print("  " + " | ".join(f"{k}={ds.getncattr(k)}" for k in GLOBALS if k in ds.ncattrs()))
        for name, var in ds.variables.items():
            if name in ds.dimensions or "bnds" in name or var.ndim < 2:
                continue
            x = var[:].astype("f8").filled(np.nan)
            print(f"  {name}{tuple(var.dimensions)} {x.shape} {getattr(var, 'units', '')} "
                  f"min={np.nanmin(x):.6g} max={np.nanmax(x):.6g} mean={np.nanmean(x):.6g} "
                  f"nan={int(np.isnan(x).sum())}")
        if "time" in ds.variables:
            t = ds["time"]
            print(f"  time {t[0]:g}..{t[-1]:g} {t.units} {getattr(t, 'calendar', '')} n={t.size}")
