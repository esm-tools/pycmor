"""EC-Earth4 pipeline steps for pycmor (script://).

Numerics live in cmip7-cmor-common; this file only adapts
them to xarray, which pycmor passes between steps.
"""
from datetime import date

import numpy as np
import pandas as pd
import xarray as xr

from cmip7_cmor_common import lpjg
from cmip7_cmor_common.gaussian import ReducedGaussian


def reduced_to_regular(data, rule):
    """OIFS reduced Gaussian (..., cell) to regular (..., lat, lon).

    No-op when there is no cell dimension (output already regular, e.g.
    oifs.output.regular_grid: true). Rule option regrid_nearest: true
    keeps land/sea edges (nearest along rows); default linear.
    """
    if "cell" not in data.dims:
        return data
    grid = ReducedGaussian(data["lat"].values, data["lon"].values)
    nearest = bool(rule.get("regrid_nearest", False))
    nlat, nlon = grid.shape
    out = xr.apply_ufunc(
        grid.to_regular,
        data.drop_vars(["lat", "lon"]),
        kwargs={"nearest": nearest},
        input_core_dims=[["cell"]],
        output_core_dims=[["lat", "lon"]],
        dask="parallelized",
        output_dtypes=[data.dtype],
        dask_gufunc_kwargs={"output_sizes": {"lat": nlat, "lon": nlon}},
        keep_attrs=True,
    )
    out = out.assign_coords(
        lat=("lat", grid.lat, {"standard_name": "latitude", "units": "degrees_north"}),
        lon=("lon", grid.lon, {"standard_name": "longitude", "units": "degrees_east"}),
    )
    out.attrs.pop("coordinates", None)
    out.name = data.name
    return out


def load_lpjg(data, rule):
    """LPJ-GUESS .out files to a Dataset (time, lat, lon); replaces load_mfdataset.

    Files: rule.inputs[0].path globbed with its pattern (gzip read as is).
    Land points go to the regular Gaussian grid, nearest along rows, ocean
    NaN (cmip7_cmor_common.lpjg). Values are passed through: no sign
    flip, no clipping. The variable is named rule.model_variable.
    rule.year_start / year_end restrict years when set.
    """
    inputs = rule.inputs[0]
    files = sorted(inputs.path.glob(inputs.pattern_str))
    if not files:
        raise FileNotFoundError(f"no LPJG files: {inputs.path}/{inputs.pattern_str}")
    ref = date(1850, 1, 1)
    y0, y1 = rule.get("year_start"), rule.get("year_end")
    times, bnds, fields = [], [], []
    lat = lon = units = None
    for path in files:
        freq = lpjg.detect_frequency(path)
        first, last = lpjg.get_timespan(path)
        for year in range(first, last + 1):
            if (y0 and year < int(y0)) or (y1 and year > int(y1)):
                continue
            tab = lpjg.read_year(path, year, freq)
            tv, tb, units, lat, lon, field = lpjg.to_regular_arrays(tab, freq, year, ref)
            times.append(tv)
            bnds.append(tb)
            fields.append(field)
    if not fields:
        raise ValueError(f"no years selected in {files}")
    name = rule.get("model_variable") or "total"
    ds = xr.Dataset(
        {name: (("time", "lat", "lon"), np.concatenate(fields).astype("f4"))},
        coords={
            "time": ("time", np.concatenate(times),
                     {"units": units, "calendar": "proleptic_gregorian"}),
            "lat": ("lat", lat, {"standard_name": "latitude", "units": "degrees_north"}),
            "lon": ("lon", lon, {"standard_name": "longitude", "units": "degrees_east"}),
        },
    )
    return xr.decode_cf(ds)


def daily_statistic_time(data, rule):
    """Daily OIFS statistic written as XIOS instant: move stamps to mid-day.

    OIFS computes the daily maximum/minimum itself; XIOS writes it once a
    day, stamped at the end of that day (00:00 next day, zero-width bounds).
    Resampling by calendar day would put it one day late, so the stamp moves
    back 12 h, into the day it describes. Only with rule option
    daily_statistic: true and when stamps sit at 00:00.
    """
    if not rule.get("daily_statistic", False):
        return data
    dim = next(d for d in data.dims if d.startswith("time"))
    t = data[dim]
    if not bool((t.dt.hour == 0).all()):
        return data
    # pd.Timedelta works with cftime and datetime64 alike.
    return data.assign_coords({dim: t - pd.Timedelta(hours=12)})
