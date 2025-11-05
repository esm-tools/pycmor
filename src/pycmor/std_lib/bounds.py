"""
Calculate coordinate bounds for lat/lon and other coordinates.

This module provides functionality to automatically calculate coordinate bounds
when they are not present in the dataset. Bounds are required for CMIP compliance
and represent the edges of grid cells.

The main function is :func:`add_bounds_from_coords` which uses cf_xarray to
infer bounds from coordinate values.
"""

import cf_xarray as cfxr  # noqa: F401
import numpy as np
import xarray as xr

from ..core.logging import logger


def calculate_bounds_1d(coord: xr.DataArray) -> xr.DataArray:
    """
    Calculate bounds for a 1D coordinate array.

    This function calculates the bounds for a 1D coordinate by computing
    midpoints between adjacent coordinate values. For the first and last
    points, it extrapolates using the same spacing.

    Parameters
    ----------
    coord : xr.DataArray
        1D coordinate array (e.g., lat or lon values)

    Returns
    -------
    xr.DataArray
        Bounds array with shape (n, 2) where n is the length of coord.
        bounds[i, 0] is the lower bound and bounds[i, 1] is the upper bound.

    Examples
    --------
    >>> lat = xr.DataArray([10, 20, 30], dims=['lat'])
    >>> bounds = calculate_bounds_1d(lat)
    >>> print(bounds.values)
    [[ 5. 15.]
     [15. 25.]
     [25. 35.]]
    """
    values = coord.values
    n = len(values)

    # Create bounds array
    bounds = np.zeros((n, 2))

    if n == 1:
        # Special case: single point
        # Assume a cell width equal to 1 unit (arbitrary but reasonable)
        bounds[0, 0] = values[0] - 0.5
        bounds[0, 1] = values[0] + 0.5
    elif n == 2:
        # Special case: two points
        # Use the spacing between them
        spacing = values[1] - values[0]
        bounds[0, 0] = values[0] - spacing / 2
        bounds[0, 1] = values[0] + spacing / 2
        bounds[1, 0] = values[1] - spacing / 2
        bounds[1, 1] = values[1] + spacing / 2
    else:
        # General case: three or more points
        # Calculate midpoints between adjacent values
        midpoints = (values[:-1] + values[1:]) / 2.0

        # Interior points: use midpoints
        bounds[1:, 0] = midpoints  # Lower bounds
        bounds[:-1, 1] = midpoints  # Upper bounds

        # Extrapolate for first point using spacing to next midpoint
        bounds[0, 0] = values[0] - (midpoints[0] - values[0])

        # Extrapolate for last point using spacing from previous midpoint
        bounds[-1, 1] = values[-1] + (values[-1] - midpoints[-1])

    # Create DataArray with appropriate dimensions
    dim_name = coord.dims[0]
    bounds_da = xr.DataArray(
        bounds,
        dims=[dim_name, "bnds"],
        attrs={
            "long_name": f"{coord.attrs.get('long_name', coord.name)} bounds",
        },
    )

    return bounds_da


def calculate_bounds_2d(coord: xr.DataArray, vertices_dim: str = "vertices") -> xr.DataArray:
    """
    Calculate bounds for a 2D coordinate array (unstructured grids).

    For unstructured grids, bounds calculation is more complex and typically
    requires additional grid topology information. This function provides a
    simple estimation that may not be accurate for all cases.

    Parameters
    ----------
    coord : xr.DataArray
        2D coordinate array
    vertices_dim : str, optional
        Name for the vertices dimension. Default is "vertices".

    Returns
    -------
    xr.DataArray
        Bounds array with an additional vertices dimension.

    Notes
    -----
    This is a simplified implementation. For accurate bounds on unstructured
    grids, it's recommended to provide pre-computed bounds in the grid file.
    """
    logger.warning(
        f"2D bounds calculation for {coord.name} is simplified. "
        "For accurate results, provide pre-computed bounds in the grid file."
    )

    # For now, return None to indicate bounds cannot be reliably calculated
    # In practice, unstructured grids should have bounds pre-computed
    return None


def add_bounds_from_coords(
    ds: xr.Dataset,
    coord_names: list[str] = None,
    bounds_dim: str = "bnds",
) -> xr.Dataset:
    """
    Add coordinate bounds to a dataset by calculating them from coordinate values.

    This function automatically calculates and adds bounds for specified coordinates
    (or lat/lon by default) if they don't already exist. It uses cf_xarray to
    identify coordinates and calculates bounds based on coordinate values.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset
    coord_names : list of str, optional
        List of coordinate names to add bounds for. If None, defaults to
        ['lat', 'lon', 'latitude', 'longitude'].
    bounds_dim : str, optional
        Name for the bounds dimension. Default is "bnds".

    Returns
    -------
    xr.Dataset
        Dataset with bounds variables added (e.g., lat_bnds, lon_bnds)

    Examples
    --------
    >>> ds = xr.Dataset({
    ...     'temp': (['time', 'lat', 'lon'], np.random.rand(10, 5, 6)),
    ... }, coords={
    ...     'lat': np.linspace(-90, 90, 5),
    ...     'lon': np.linspace(0, 360, 6),
    ... })
    >>> ds_with_bounds = add_bounds_from_coords(ds)
    >>> print('lat_bnds' in ds_with_bounds)
    True
    """
    if coord_names is None:
        coord_names = ["lat", "lon", "latitude", "longitude"]

    ds_out = ds.copy()

    for coord_name in coord_names:
        # Check if coordinate exists in dataset
        if coord_name not in ds.coords and coord_name not in ds.data_vars:
            continue

        coord = ds[coord_name]
        bounds_name = f"{coord_name}_bnds"

        # Skip if bounds already exist
        if bounds_name in ds.data_vars or bounds_name in ds.coords:
            logger.debug(f"  → Bounds '{bounds_name}' already exist, skipping calculation")
            continue

        # Calculate bounds based on dimensionality
        if coord.ndim == 1:
            logger.info(f"  → Calculating 1D bounds for '{coord_name}'")
            bounds = calculate_bounds_1d(coord)

            if bounds is not None:
                ds_out[bounds_name] = bounds
                # Add bounds attribute to coordinate
                ds_out[coord_name].attrs["bounds"] = bounds_name
                logger.info(f"  → Added bounds variable '{bounds_name}'")
        elif coord.ndim == 2:
            logger.info(f"  → Attempting 2D bounds calculation for '{coord_name}'")
            bounds = calculate_bounds_2d(coord)

            if bounds is not None:
                ds_out[bounds_name] = bounds
                ds_out[coord_name].attrs["bounds"] = bounds_name
                logger.info(f"  → Added bounds variable '{bounds_name}'")
            else:
                logger.warning(
                    f"  → Could not calculate bounds for 2D coordinate '{coord_name}'. "
                    "Provide pre-computed bounds in grid file."
                )
        else:
            logger.warning(
                f"  → Coordinate '{coord_name}' has {coord.ndim} dimensions. "
                "Bounds calculation only supports 1D and 2D coordinates."
            )

    return ds_out


def add_vertical_bounds(
    ds: xr.Dataset,
    vertical_coord_names: list[str] = None,
    bounds_dim: str = "bnds",
) -> xr.Dataset:
    """
    Add vertical coordinate bounds to a dataset (similar to cdo genlevelbounds).

    This function automatically calculates and adds bounds for vertical coordinates
    such as pressure levels (plev, plev19, etc.) or depth levels if they don't
    already exist. It uses the same algorithm as horizontal bounds calculation.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset
    vertical_coord_names : list of str, optional
        List of vertical coordinate names to add bounds for. If None, automatically
        detects common vertical coordinate names like 'plev', 'lev', 'depth', 'pressure'.
    bounds_dim : str, optional
        Name for the bounds dimension. Default is "bnds".

    Returns
    -------
    xr.Dataset
        Dataset with vertical bounds variables added (e.g., plev_bnds, depth_bnds)

    Examples
    --------
    >>> ds = xr.Dataset({
    ...     'ta': (['time', 'plev', 'lat', 'lon'], np.random.rand(10, 8, 5, 6)),
    ... }, coords={
    ...     'plev': [100000, 92500, 85000, 70000, 60000, 50000, 40000, 30000],
    ...     'lat': np.linspace(-90, 90, 5),
    ...     'lon': np.linspace(0, 360, 6),
    ... })
    >>> ds_with_bounds = add_vertical_bounds(ds)
    >>> print('plev_bnds' in ds_with_bounds)
    True

    Notes
    -----
    This function is similar to CDO's genlevelbounds operator, which generates
    bounds for vertical coordinates in climate model output.
    """
    if vertical_coord_names is None:
        # Common vertical coordinate names in climate data
        vertical_coord_names = [
            "plev",
            "lev",
            "level",
            "pressure",
            "depth",
            "plev19",
            "plev8",
            "plev7",
            "plev4",
            "plev3",
            "height",
            "alt",
            "altitude",
        ]

    ds_out = ds.copy()

    for coord_name in vertical_coord_names:
        # Check if coordinate exists in dataset
        if coord_name not in ds.coords and coord_name not in ds.data_vars:
            continue

        coord = ds[coord_name]
        bounds_name = f"{coord_name}_bnds"

        # Skip if bounds already exist
        if bounds_name in ds.data_vars or bounds_name in ds.coords:
            logger.debug(f"  → Vertical bounds '{bounds_name}' already exist, skipping calculation")
            continue

        # Only handle 1D vertical coordinates
        if coord.ndim == 1:
            logger.info(f"  → Calculating vertical bounds for '{coord_name}'")
            bounds = calculate_bounds_1d(coord)

            if bounds is not None:
                ds_out[bounds_name] = bounds
                # Add bounds attribute to coordinate
                ds_out[coord_name].attrs["bounds"] = bounds_name
                logger.info(f"  → Added vertical bounds variable '{bounds_name}'")
        else:
            logger.warning(
                f"  → Vertical coordinate '{coord_name}' has {coord.ndim} dimensions. "
                "Bounds calculation only supports 1D coordinates."
            )

    return ds_out


def add_bounds_to_grid(grid: xr.Dataset) -> xr.Dataset:
    """
    Add lat/lon bounds to a grid dataset if they don't exist.

    This is a convenience function specifically for grid files that may be
    missing bounds variables.

    Parameters
    ----------
    grid : xr.Dataset
        Grid dataset

    Returns
    -------
    xr.Dataset
        Grid dataset with bounds added if they were missing
    """
    logger.info("[Bounds] Checking for coordinate bounds in grid")

    # Check for various lat/lon naming conventions
    lat_names = [name for name in ["lat", "latitude"] if name in grid.coords or name in grid.data_vars]
    lon_names = [name for name in ["lon", "longitude"] if name in grid.coords or name in grid.data_vars]

    coord_names = lat_names + lon_names

    if not coord_names:
        logger.warning("  → No lat/lon coordinates found in grid")
        return grid

    grid_with_bounds = add_bounds_from_coords(grid, coord_names=coord_names)

    return grid_with_bounds
