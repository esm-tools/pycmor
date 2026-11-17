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

    # Create DataArray with appropriate dimensions.
    # CF §7.1: bounds variables MUST NOT carry boundary-related attrs of
    # their own (long_name, units, standard_name, axis, positive, ...).
    # They inherit those from the parent coord at read time. Setting a
    # long_name here trips wcrp/cf §7.1 with a "non matching boundary
    # related attributes: ['long_name']" finding.
    dim_name = coord.dims[0]
    bounds_da = xr.DataArray(
        bounds,
        dims=[dim_name, "bnds"],
        attrs={},
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
    >>> import xarray as xr
    >>> import numpy as np
    >>> ds = xr.Dataset({
    ...     'temp': (['time', 'lat', 'lon'], np.random.rand(10, 5, 6)),
    ... }, coords={
    ...     'lat': np.linspace(-90, 90, 5),
    ...     'lon': np.linspace(0, 360, 6),
    ... })
    >>> print(ds)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    <xarray.Dataset> Size: ...
    Dimensions:  (time: 10, lat: 5, lon: 6)
    Coordinates:
      * lat      (lat) float64 ... -90.0 -45.0 0.0 45.0 90.0
      * lon      (lon) float64 ... 0.0 72.0 144.0 216.0 288.0 360.0
    Dimensions without coordinates: time
    Data variables:
        temp     (time, lat, lon) float64 ...
    >>> logger.disable("pycmor")
    >>> ds_with_bounds = add_bounds_from_coords(ds)
    >>> 'lat_bnds' in ds_with_bounds
    True
    >>> print(ds_with_bounds)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    <xarray.Dataset> Size: ...
    Dimensions:   (time: 10, lat: 5, lon: 6, bnds: 2)
    Coordinates:
      * lat       (lat) float64 ... -90.0 -45.0 0.0 45.0 90.0
      * lon       (lon) float64 ... 0.0 72.0 144.0 216.0 288.0 360.0
    Dimensions without coordinates: time, bnds
    Data variables:
        temp      (time, lat, lon) float64 ...
        lat_bnds  (lat, bnds) float64 ...
        lon_bnds  (lon, bnds) float64 ...
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
    >>> import xarray as xr
    >>> import numpy as np
    >>> ds = xr.Dataset({
    ...     'ta': (['time', 'plev', 'lat', 'lon'], np.random.rand(10, 8, 5, 6)),
    ... }, coords={
    ...     'plev': [100000, 92500, 85000, 70000, 60000, 50000, 40000, 30000],
    ...     'lat': np.linspace(-90, 90, 5),
    ...     'lon': np.linspace(0, 360, 6),
    ... })
    >>> print(ds)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    <xarray.Dataset> Size: ...
    Dimensions:  (time: 10, plev: 8, lat: 5, lon: 6)
    Coordinates:
      * plev     (plev) int... 100000 92500 85000 70000 60000 50000 40000 30000
      * lat      (lat) float64 ... -90.0 -45.0 0.0 45.0 90.0
      * lon      (lon) float64 ... 0.0 72.0 144.0 216.0 288.0 360.0
    Dimensions without coordinates: time
    Data variables:
        ta       (time, plev, lat, lon) float64 ...
    >>> logger.disable("pycmor")
    >>> ds_with_bounds = add_vertical_bounds(ds)
    >>> 'plev_bnds' in ds_with_bounds
    True
    >>> print(ds_with_bounds)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    <xarray.Dataset> Size: ...
    Dimensions:    (time: 10, plev: 8, lat: 5, lon: 6, bnds: 2)
    Coordinates:
      * plev       (plev) int... 100000 92500 85000 70000 60000 50000 40000 30000
      * lat        (lat) float64 ... -90.0 -45.0 0.0 45.0 90.0
      * lon        (lon) float64 ... 0.0 72.0 144.0 216.0 288.0 360.0
    Dimensions without coordinates: time, bnds
    Data variables:
        ta         (time, plev, lat, lon) float64 ...
        plev_bnds  (plev, bnds) float64 ...

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
            # Potential-density vertical coord for msftm-rho-style
            # (meridional overturning streamfunction in density space).
            "rho",
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


def _unit_vectors(lon, lat):
    lo, la = np.radians(lon), np.radians(lat)
    return np.stack([np.cos(la) * np.cos(lo), np.cos(la) * np.sin(lo), np.sin(la)], axis=-1)


def dual_cell_vertices(lon, lat, triangles):
    """Vertices of the median-dual cell around every node of a triangular mesh.

    FESOM2 puts scalars on the nodes, and the cell a node stands for is its
    median-dual polygon: going counter-clockwise round the node, the midpoint
    of each edge leaving it alternates with the centroid of each triangle
    between two such edges. On the open boundary (coast, ice-shelf front) the
    fan of triangles does not close; the polygon then runs from the midpoint
    of the last boundary edge along that edge back to the node itself and out
    along the first boundary edge, so the node is one of its vertices.

    The ``lat_bnds`` / ``lon_bnds`` that pyfesom2 ``writeCDO`` stores in the
    CORE3 and DARS2 ``mesh.nc`` do not follow this: the fan is ordered wrongly,
    every polygon repeats one or two vertices in place of others, and boundary
    nodes are not included, so 1103 CORE3 boundary nodes fall outside their own
    cell (cf §7.1). This rebuilds the cells from the triangles instead.

    Parameters
    ----------
    lon, lat : array-like, shape (n,)
        Node positions in degrees.
    triangles : array-like of int, shape (m, 3)
        Zero-based node indices of every triangle, any orientation.

    Returns
    -------
    (lon_bnds, lat_bnds) : two float64 arrays of shape (n, nvertices)
        Counter-clockwise vertices, longitude in [-180, 180), padded to the
        largest cell by repeating the last vertex, which is the CMOR
        convention. Midpoints and centroids are taken on the sphere.
    """
    lon = np.asarray(lon, dtype=np.float64).ravel()
    lat = np.asarray(lat, dtype=np.float64).ravel()
    tri = np.array(triangles, dtype=np.int64)
    n = lon.size
    p = _unit_vectors(lon, lat)
    a, b, c = p[tri[:, 0]], p[tri[:, 1]], p[tri[:, 2]]
    clockwise = np.einsum("ij,ij->i", np.cross(b - a, c - a), a) < 0
    tri[clockwise] = tri[clockwise, ::-1]
    centroid = p[tri].sum(axis=1)

    # One wedge per triangle corner: node A, then B and C counter-clockwise.
    # The wedge after it round A is the one whose B is this wedge's C.
    node = tri.ravel()
    first = tri[:, [1, 2, 0]].ravel()
    last = tri[:, [2, 0, 1]].ravel()
    owner = np.repeat(np.arange(len(tri)), 3)
    key = node * n + first
    order = np.argsort(key, kind="stable")
    sorted_key = key[order]
    if np.any(sorted_key[1:] == sorted_key[:-1]):
        raise ValueError("mesh has an edge that two triangles traverse in the same direction")
    want = node * n + last
    hit = np.minimum(np.searchsorted(sorted_key, want), key.size - 1)
    following = np.where(sorted_key[hit] == want, order[hit], -1)
    has_previous = np.zeros(key.size, dtype=bool)
    has_previous[following[following >= 0]] = True

    # A boundary fan starts at the wedge nothing precedes; a closed fan
    # starts anywhere. A node where two fans touch gets both, one after the
    # other, each closing through the node.
    open_starts = np.flatnonzero(~has_previous)
    any_wedge = np.full(n, -1)
    any_wedge[node[::-1]] = np.arange(key.size)[::-1]
    closed_nodes = np.setdiff1d(np.unique(node), node[open_starts])
    starts = np.concatenate([open_starts, any_wedge[closed_nodes]])
    degree = np.bincount(node, minlength=n)
    count = 2 * degree + 2 * np.bincount(node[open_starts], minlength=n)
    nvertices = max(int(count.max()), 1)

    # A node in no triangle keeps itself as its only vertex.
    lon_bnds = np.repeat(lon[:, None], nvertices, axis=1)
    lat_bnds = np.repeat(lat[:, None], nvertices, axis=1)
    filled = np.zeros(n, dtype=np.int64)

    def put(rows, vectors):
        vectors = vectors / np.linalg.norm(vectors, axis=-1, keepdims=True)
        lon_bnds[rows, filled[rows]] = np.degrees(np.arctan2(vectors[:, 1], vectors[:, 0]))
        lat_bnds[rows, filled[rows]] = np.degrees(np.arcsin(np.clip(vectors[:, 2], -1.0, 1.0)))
        filled[rows] += 1

    # Walk the fans of each node one at a time so ``filled`` stays per node.
    fan_node = node[starts]
    by_node = np.argsort(fan_node, kind="stable")
    rank = np.empty(starts.size, dtype=np.int64)
    rank[by_node] = np.arange(starts.size) - np.searchsorted(fan_node[by_node], fan_node[by_node])
    for r in range(int(rank.max()) + 1 if starts.size else 0):
        wedge = start = starts[rank == r]
        while wedge.size:
            rows = node[wedge]
            put(rows, p[rows] + p[first[wedge]])
            put(rows, centroid[owner[wedge]])
            after = following[wedge]
            ends = after < 0
            closing = rows[ends]
            put(closing, p[closing] + p[last[wedge[ends]]])
            # The node itself, copied exactly so it lies on its own cell.
            lon_bnds[closing, filled[closing]] = lon[closing]
            lat_bnds[closing, filled[closing]] = lat[closing]
            filled[closing] += 1
            go_on = ~ends & (after != start)
            wedge, start = after[go_on], start[go_on]
    # A cell round a pole can have its node further poleward than any vertex,
    # and then no latitude range of its vertices holds the node (cf §7.1).
    # Route the ring through the pole along the two meridians of the
    # shortest edge that does not pass the node's own meridian. That cuts
    # the sliver between edge and pole off the cell (10 % of the one polar
    # CORE3 cell) but keeps the node inside it.
    turn = np.zeros(n)
    for j in range(nvertices):
        step = np.where(j + 1 < filled, lon_bnds[:, (j + 1) % nvertices], lon_bnds[:, 0]) - lon_bnds[:, j]
        turn += np.where(j < filled, (step + 180.0) % 360.0 - 180.0, 0.0)
    polar = np.flatnonzero(np.abs(turn) > 180.0)
    if polar.size and int(filled[polar].max()) >= nvertices:
        nvertices += 1
        lon_bnds = np.concatenate([lon_bnds, lon_bnds[:, -1:]], axis=1)
        lat_bnds = np.concatenate([lat_bnds, lat_bnds[:, -1:]], axis=1)
    for row in polar:
        k = int(filled[row])
        ring = lon_bnds[row, :k]
        step = (np.roll(ring, -1) - ring + 180.0) % 360.0 - 180.0
        to_node = (lon[row] - ring + 180.0) % 360.0 - 180.0
        passes_node = (to_node * step > 0) & (np.abs(to_node) < np.abs(step))
        at = int(np.argmin(np.where(passes_node, np.inf, np.abs(step)))) + 1
        lon_bnds[row, at : k + 1] = np.concatenate([[lon_bnds[row, at - 1]], lon_bnds[row, at:k]])
        lat_bnds[row, at : k + 1] = np.concatenate([[np.copysign(90.0, lat[row])], lat_bnds[row, at:k]])
        filled[row] += 1
    for j in range(1, nvertices):
        short = filled <= j
        short &= filled > 0
        lon_bnds[short, j] = lon_bnds[short, j - 1]
        lat_bnds[short, j] = lat_bnds[short, j - 1]
    lon_bnds[lon_bnds >= 180.0] -= 360.0
    return lon_bnds, lat_bnds
