"""Pipeline steps for CF cell-measure (fx) variables.

CMIP7 variables that reference ``cell_measures`` (e.g. ``area: areacello``)
need the measure itself shipped as a companion fx-frequency file.
These steps load the measure from the model's grid/mesh and make it
available to pycmor's standard save path.

The three steps (``load_gridfile``, ``compute_areacello``,
``compute_areacella``) together with the :class:`AreacelloFxPipeline`
and :class:`AreacellaFxPipeline` frozen pipelines in
``pycmor.core.pipeline`` cover the common cases. They are generic over
model; any config with ``grid_file`` pointing at a mesh containing
``cell_area`` (ocean) or with loadable lat/lon coords (atmosphere)
can reuse them.
"""

from __future__ import annotations

import numpy as np
import xarray as xr

from ..core.logging import logger


def load_gridfile(data, rule):
    """Load ``rule.grid_file`` as an xarray Dataset.

    Drop-in replacement for ``pycmor.core.gather_inputs.load_mfdataset``
    at the head of an fx pipeline: instead of reading time-series model
    output, it reads the time-invariant grid/mesh file.

    Works for any model that stores grid info in a NetCDF file
    (FESOM ``mesh.nc``, ICON grid file, atmospheric grid descriptor).
    """
    grid_file = rule.get("grid_file")
    if grid_file is None:
        raise ValueError("Rule must specify 'grid_file' for load_gridfile step")
    logger.info(f"Loading grid file: {grid_file}")
    return xr.open_dataset(grid_file)


def compute_areacello(data, rule):
    """Ocean grid-cell area as read from an unstructured mesh.

    Reads ``cell_area`` (or ``cluster_area`` as a fallback) from the
    mesh Dataset produced by :func:`load_gridfile`. No computation —
    the mesh already stores the per-node surface area in m².
    """
    for name in ("cell_area", "cluster_area"):
        if name in data:
            area = data[name]
            break
    else:
        raise ValueError("Mesh must contain 'cell_area' or 'cluster_area' for areacello")

    result = area.copy()
    result.attrs = {
        "units": "m2",
        "standard_name": "cell_area",
        "long_name": "Ocean Grid-Cell Area",
        "cell_methods": "area: sum",
    }
    result.name = rule.model_variable
    return result


def compute_areacella(data, rule):
    """Atmospheric grid-cell area from lat/lon coordinates.

    Supports two grid layouts:

    * Regular grid — ``lat`` and ``lon`` are 1D along distinct dimensions.
      Uses ``area = R² · Δλ · |sin(φ+Δφ/2) − sin(φ−Δφ/2)|`` with mean Δλ, Δφ.
    * Unstructured / reduced Gaussian — ``lat`` and ``lon`` are auxiliary
      coordinates along a single dim (e.g. ``cell``), with ``bounds_lat``
      ``(cell, nvertex)`` and ``bounds_lon`` ``(cell, nvertex)`` providing
      the corner coordinates. Per-cell area uses the same spherical-strip
      formula with each cell's lat/lon bounds.

    Earth radius R = 6 371 000 m.
    """
    R = 6371000.0

    lat = None
    lon = None
    for coord_name in data.coords:
        cname = str(coord_name).lower()
        if cname in ("lat", "latitude") or cname.endswith("_lat"):
            lat = data.coords[coord_name]
        if cname in ("lon", "longitude") or cname.endswith("_lon"):
            lon = data.coords[coord_name]
    if lat is None or lon is None:
        for coord_name in data.coords:
            cname = str(coord_name).lower()
            if lat is None and "lat" in cname and "bound" not in cname:
                lat = data.coords[coord_name]
            if lon is None and "lon" in cname and "bound" not in cname:
                lon = data.coords[coord_name]
    if lat is None or lon is None:
        raise ValueError("Cannot find lat/lon coordinates in input data")

    unstructured = (lat.ndim == 1 and lon.ndim == 1 and lat.dims == lon.dims)

    if unstructured:
        # Use bounds_lat / bounds_lon (or equivalent) for per-cell area.
        ds_src = data if isinstance(data, xr.Dataset) else data._coords.get("__parent__", None)
        # Try common bound-variable names on the source Dataset/DataArray.
        candidates_lat = [lat.attrs.get("bounds"), "bounds_lat", "lat_bnds", "lat_bounds"]
        candidates_lon = [lon.attrs.get("bounds"), "bounds_lon", "lon_bnds", "lon_bounds"]
        lat_bnds = lon_bnds = None
        search_objs = []
        if isinstance(data, xr.Dataset):
            search_objs.append(data)
        search_objs.append(data.coords)
        for obj in search_objs:
            for k in candidates_lat:
                if k and k in obj:
                    lat_bnds = obj[k]
                    break
            for k in candidates_lon:
                if k and k in obj:
                    lon_bnds = obj[k]
                    break
            if lat_bnds is not None and lon_bnds is not None:
                break
        if lat_bnds is None or lon_bnds is None:
            raise ValueError(
                "Unstructured grid detected but lat/lon bounds not found "
                "(expected e.g. 'bounds_lat', 'bounds_lon')"
            )

        # open_mfdataset may broadcast bounds along the time dim; drop anything
        # that isn't the cell dim or the nvertex/vertices dim.
        cell_dim_name = lat.dims[0]
        def _reduce_to_cell_nvertex(bnds):
            for d in list(bnds.dims):
                if d == cell_dim_name:
                    continue
                if bnds.sizes[d] <= 32:  # nvertex-like: keep
                    continue
                bnds = bnds.isel({d: 0})
            return bnds
        lat_bnds = _reduce_to_cell_nvertex(lat_bnds)
        lon_bnds = _reduce_to_cell_nvertex(lon_bnds)

        lat_b = np.deg2rad(np.asarray(lat_bnds.values))
        lon_b = np.deg2rad(np.asarray(lon_bnds.values))
        lat_max = lat_b.max(axis=-1)
        lat_min = lat_b.min(axis=-1)
        # Handle longitude wrap-around: width is the smaller of forward/backward span.
        lon_span = lon_b.max(axis=-1) - lon_b.min(axis=-1)
        lon_span = np.where(lon_span > np.pi, 2 * np.pi - lon_span, lon_span)
        area_1d = R**2 * lon_span * np.abs(np.sin(lat_max) - np.sin(lat_min))

        cell_dim = lat.dims[0]
        result = xr.DataArray(
            area_1d,
            dims=[cell_dim],
            coords={lat.name: lat, lon.name: lon},
        )
    else:
        lat_vals = np.deg2rad(lat.values)
        lon_vals = np.deg2rad(lon.values)
        dlat = float(np.abs(np.diff(lat_vals).mean()))
        dlon = float(np.abs(np.diff(lon_vals).mean()))
        lat_upper = lat_vals + dlat / 2
        lat_lower = lat_vals - dlat / 2
        area_1d = R**2 * dlon * np.abs(np.sin(lat_upper) - np.sin(lat_lower))
        area_2d = np.broadcast_to(area_1d[:, np.newaxis], (len(lat_vals), len(lon_vals)))
        result = xr.DataArray(
            area_2d,
            dims=[lat.dims[0], lon.dims[0]],
            coords={lat.name: lat, lon.name: lon},
        )

    result.attrs = {
        "units": "m2",
        "standard_name": "cell_area",
        "long_name": "Grid-Cell Area for Atmospheric Grid Variables",
        "cell_methods": "area: sum",
    }
    result.name = rule.model_variable
    return result
