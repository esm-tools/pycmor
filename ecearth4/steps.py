"""EC-Earth4 pipeline steps for pycmor (script://).

Numerics live in cmip7-cmor-common; this file only adapts
them to xarray, which pycmor passes between steps.
"""
import xarray as xr

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
