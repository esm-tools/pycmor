#!/usr/bin/env python3
"""Ocean fx fields for the regridded FESOM output (0.5 deg, g131).

The XIOS-regridded fields (``<var>.fesom.gr.<year>.nc``) name ``areacello``
and ``volcello`` in ``cell_measures``, but the fx files only existed for the
native mesh. This writes the g131 ones, once per mesh, into a static file
the gr fx rules read (see generate_gr_yaml.py):

    areacello  full area of each 0.5 deg cell
    sftof      percentage of it covered by the FESOM mesh
    deptho     sea floor depth, area-weighted over that part
    volcello   ocean volume per level, from the native layer thicknesses

Each mesh node's polygon (``lon_bnds``/``lat_bnds`` of mesh.nc) is
intersected with the cells in a cylindrical equal-area projection. The
pieces of a node are then scaled to add up to its ``cell_area``, so the
totals equal the native ones and the projection only moves area between
neighbouring cells. Depth and thickness come from the same helpers as the
native fx rules.

The XIOS weights cannot be used for this: they are renormalised, and for
coastal nodes the polygons XIOS used differ from mesh.nc (cli127: overlaps
adding up to more than a cell).

    make_gr_fx.py <mesh.nc> <out.nc> [--nlon 720 --nlat 360]
"""

import argparse
import sys
from pathlib import Path

import netCDF4
import numpy as np
import scipy.sparse as sp
import shapely
import xarray as xr

sys.path.insert(0, str(Path(__file__).resolve().parent))
import custom_steps as cs  # noqa: E402

R_EARTH = 6371000.0  # what the DARS2 cell_area values imply


class _Rule(dict):
    model_variable = "deptho"


def _node_polygons(lon_b, lat_b, lon_c):
    """Node polygons in (lon, sin(lat)), lon unwrapped around the node."""
    lon = lon_b - 360.0 * np.round((lon_b - lon_c[:, None]) / 360.0)
    coords = np.stack([lon, np.sin(np.deg2rad(lat_b))], -1)
    coords = np.concatenate([coords, coords[:, :1]], 1)
    polys = shapely.polygons(coords)
    bad = ~shapely.is_valid(polys)
    if bad.any():
        polys[bad] = shapely.make_valid(polys[bad])
    return polys, int(bad.sum())


def overlaps(mesh, nlon, nlat, chunk=200_000):
    """Sparse (cell, node) overlap areas in m2, each node summing to its cell_area."""
    lon_b = np.asarray(mesh["lon_bnds"][:], float)
    lat_b = np.asarray(mesh["lat_bnds"][:], float)
    lon_c = np.asarray(mesh["lon"][:], float)
    area = np.asarray(mesh["cell_area"][:], float)
    dlon, dlat = 360.0 / nlon, 180.0 / nlat
    lat_edges = np.sin(np.deg2rad(-90.0 + dlat * np.arange(nlat + 1)))
    scale = R_EARTH**2 * np.deg2rad(1.0)
    rows, cols, vals = [], [], []
    n_invalid, polar = 0, 0
    for start in range(0, area.size, chunk):
        sl = slice(start, min(start + chunk, area.size))
        polys, nb = _node_polygons(lon_b[sl], lat_b[sl], lon_c[sl])
        n_invalid += nb
        x0, y0, x1, y1 = shapely.bounds(polys).T
        polar += int(((x1 - x0) > 180).sum())
        i0 = np.floor(x0 / dlon).astype(int)
        i1 = np.floor(x1 / dlon).astype(int)
        j0 = np.clip(np.searchsorted(lat_edges, y0, side="right") - 1, 0, nlat - 1)
        j1 = np.clip(np.searchsorted(lat_edges, y1, side="right") - 1, 0, nlat - 1)
        ni, nj = i1 - i0 + 1, j1 - j0 + 1
        npair = ni * nj
        node = np.repeat(np.arange(sl.start, sl.stop), npair)
        local = np.concatenate([np.arange(n) for n in npair])
        rep = np.repeat(np.arange(npair.size), npair)
        ii = i0[rep] + local % ni[rep]
        jj = j0[rep] + local // ni[rep]
        boxes = shapely.box(ii * dlon, lat_edges[jj], (ii + 1) * dlon, lat_edges[jj + 1])
        a = shapely.area(shapely.intersection(polys[rep], boxes)) * scale
        keep = a > 0
        rows.append(jj[keep] * nlon + np.mod(ii[keep], nlon))
        cols.append(node[keep])
        vals.append(a[keep])
        print(f"  nodes {sl.stop}/{area.size}", flush=True)
    A = sp.csr_matrix(
        (np.concatenate(vals), (np.concatenate(rows), np.concatenate(cols))), shape=(nlon * nlat, area.size)
    )
    proj = np.asarray(A.sum(0)).ravel()
    ratio = proj / area
    print(
        f"projected/cell_area per node: p0.1={np.percentile(ratio, 0.1):.6f} median={np.median(ratio):.6f} "
        f"p99.9={np.percentile(ratio, 99.9):.6f}; invalid polygons repaired: {n_invalid}; "
        f"spanning >180 deg of longitude: {polar}",
        flush=True,
    )
    A = A @ sp.diags(np.where(proj > 0, area / np.where(proj > 0, proj, 1), 0.0))
    return A, area


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mesh")
    ap.add_argument("out")
    ap.add_argument("--nlon", type=int, default=720)
    ap.add_argument("--nlat", type=int, default=360)
    ap.add_argument(
        "--footprint",
        help="XIOS weight file of the node-to-regular interpolation; cells it does not fill count as land",
    )
    args = ap.parse_args(argv)

    mesh = netCDF4.Dataset(args.mesh)
    A, node_area = overlaps(mesh, args.nlon, args.nlat)
    mesh_ds = xr.open_dataset(args.mesh)
    rule = _Rule()
    node_depth = np.asarray(cs.compute_deptho(mesh_ds, rule).values, float)
    depth, thickness = cs._mesh_vertical_geometry(mesh_ds)
    wet = cs._profile_over_cells(thickness, mesh_ds, {}, "thk", rule).values  # (lev, node), NaN below floor
    node_thk = np.nan_to_num(wet).T  # (node, lev)

    nlon, nlat = args.nlon, args.nlat
    dlon, dlat = 360.0 / nlon, 180.0 / nlat
    lat = -90.0 + dlat * (np.arange(nlat) + 0.5)
    lon = dlon * (np.arange(nlon) + 0.5)
    band = R_EARTH**2 * np.deg2rad(dlon) * (np.sin(np.deg2rad(lat + dlat / 2)) - np.sin(np.deg2rad(lat - dlat / 2)))
    cell_area = np.repeat(band, nlon)

    ocean = np.asarray(A.sum(1)).ravel()
    if args.footprint:
        # The regridded node fields have data only where XIOS has weights. A
        # few coastal cells hold a sliver of mesh that XIOS leaves out; they
        # are land here too, so every g131 field agrees on one coastline.
        filled = np.zeros(ocean.size, bool)
        filled[np.unique(netCDF4.Dataset(args.footprint)["dst_idx"][:] - 1)] = True
        dropped = (ocean > 0) & ~filled
        print(
            f"footprint: {filled.sum()} cells; {dropped.sum()} cells with mesh but no XIOS data "
            f"({ocean[dropped].sum() / ocean.sum():.2e} of the ocean area) set to land"
        )
        A = sp.diags(filled.astype(float)) @ A
        ocean = np.where(filled, ocean, 0.0)
    frac = ocean / cell_area
    print(f"sftof: max before clipping {100 * frac.max():.4f} %, cells > 100.01 %: {(frac > 1.0001).sum()}")
    with np.errstate(invalid="ignore", divide="ignore"):
        deptho = np.where(ocean > 0, (A @ node_depth) / ocean, np.nan)
    vol = np.asarray(A @ node_thk)  # (cell, lev)
    vol[vol == 0] = np.nan

    ds = xr.Dataset(
        {
            "areacello": (("lat", "lon"), cell_area.reshape(nlat, nlon)),
            "sftof": (("lat", "lon"), np.minimum(100.0 * frac, 100.0).reshape(nlat, nlon)),
            "deptho": (("lat", "lon"), deptho.reshape(nlat, nlon)),
            "volcello": (("lev", "lat", "lon"), vol.T.reshape(-1, nlat, nlon)),
        },
        coords={"lat": lat, "lon": lon, "lev": depth},
    )
    ds["lat"].attrs = {"standard_name": "latitude", "units": "degrees_north", "axis": "Y"}
    ds["lon"].attrs = {"standard_name": "longitude", "units": "degrees_east", "axis": "X"}
    # as cs._attach_level_coord gives the native fx files
    ds["lev"].attrs = {
        "standard_name": "depth",
        "long_name": "ocean depth coordinate",
        "units": "m",
        "axis": "Z",
        "positive": "down",
    }
    ds["areacello"].attrs = {"standard_name": "cell_area", "units": "m2"}
    ds["sftof"].attrs = {"standard_name": "sea_area_fraction", "units": "%"}
    ds["deptho"].attrs = {"standard_name": "sea_floor_depth_below_geoid", "units": "m"}
    ds["volcello"].attrs = {"standard_name": "ocean_volume", "units": "m3"}
    ds.attrs = {
        "title": f"FESOM ocean fx fields on the regular {dlon} deg grid of the XIOS-regridded output",
        "source_mesh": str(Path(args.mesh).resolve()),
        "history": "examples/make_gr_fx.py",
    }
    if args.footprint:
        ds.attrs["footprint"] = str(Path(args.footprint).resolve())
    ds.to_netcdf(args.out, encoding={v: {"zlib": True, "complevel": 4} for v in ds.data_vars})

    native_vol = float(np.nansum(wet * node_area[None, :]))
    print(
        f"ocean area: g131 {ocean.sum():.6e} m2, native {node_area.sum():.6e} m2; "
        f"volume: g131 {np.nansum(vol):.6e} m3, native {native_vol:.6e} m3"
    )


if __name__ == "__main__":
    sys.exit(main())
