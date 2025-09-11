import logging
import os

import joblib
import numpy as np
import scipy
import scipy.spatial.qhull as qhull
import xarray as xr
from pyfesom2.load_mesh_data import load_mesh
from scipy.interpolate import CloughTocher2DInterpolator, LinearNDInterpolator
from scipy.spatial import cKDTree

from ..core.pipeline import FrozenPipeline


def lon_lat_to_cartesian(lon, lat, R=6371000):
    """
    calculates lon, lat coordinates of a point on a sphere with
    radius R. Taken from http://earthpy.org/interpolation_between_grids_with_ckdtree.html
    """
    lon_r = np.radians(lon)
    lat_r = np.radians(lat)

    x = R * np.cos(lat_r) * np.cos(lon_r)
    y = R * np.cos(lat_r) * np.sin(lon_r)
    z = R * np.sin(lat_r)
    return x, y, z


def create_indexes_and_distances(mesh, lons, lats, k=1, n_jobs=2):
    """
    Creates KDTree object and query it for indexes of points in FESOM mesh that are close to the
    points of the target grid. Also return distances of the original points to target points.

    Parameters
    ----------
    mesh : fesom_mesh object
        pyfesom mesh representation
    lons/lats : array
        2d arrays with target grid values.
    k : int
        k-th nearest neighbors to return.
    n_jobs : int, optional
        Number of jobs to schedule for parallel processing. If -1 is given
        all processors are used. Default: 1.

    Returns
    -------
    distances : array of floats
        The distances to the nearest neighbors.
    inds : ndarray of ints
        The locations of the neighbors in data.

    """
    xs, ys, zs = lon_lat_to_cartesian(mesh.x2, mesh.y2)
    xt, yt, zt = lon_lat_to_cartesian(lons.flatten(), lats.flatten())

    tree = cKDTree(list(zip(xs, ys, zs)))

    border_version = "1.6.0"
    current_version = scipy.__version__
    v1_parts = list(map(int, border_version.split(".")))
    v2_parts = list(map(int, current_version.split(".")))

    if v2_parts > v1_parts:
        distances, inds = tree.query(list(zip(xt, yt, zt)), k=k, workers=n_jobs)
    else:
        distances, inds = tree.query(list(zip(xt, yt, zt)), k=k, n_jobs=n_jobs)

    return distances, inds


def fesom2regular(
    data,
    mesh,
    lons,
    lats,
    distances_path=None,
    inds_path=None,
    qhull_path=None,
    how="nn",
    k=5,
    radius_of_influence=100000,
    n_jobs=2,
    dumpfile=True,
    basepath=None,
):
    """
    Interpolates data from FESOM mesh to target (usually regular) mesh.

    Parameters
    ----------
    data : array
        1d array that represents FESOM data at one
    mesh : fesom_mesh object
        pyfesom mesh representation
    lons/lats : array
        2d arrays with target grid values.
    distances_path : string
        Path to the file with distances. If not provided and dumpfile=True, it will be created.
    inds_path : string
        Path to the file with inds. If not provided and dumpfile=True, it will be created.
    qhull_path : str
         Path to the file with qhull (needed for linear and cubic interpolations). If not provided
         and dumpfile=True, it will be created.
    how : str
       Interpolation method. Options are 'nn' (nearest neighbor), 'idist' (inverce distance), "linear" and "cubic".
    k : int
        k-th nearest neighbors to use. Only used when how==idist
    radius_of_influence : int
        Cut off distance in meters, only used in nn and idist.
    n_jobs : int, optional
        Number of jobs to schedule for parallel processing. If -1 is given
        all processors are used. Default: 1. Only used for nn and idist.
    dumpfile: bool
        wether to dump resulted distances and inds to the file.
    basepath: str
        path where to store additional interpolation files. If None (default),
        the path of the mesh will be used.

    Returns
    -------
    data_interpolated : 2d array
        array with data interpolated to the target grid.

    """
    if isinstance(data, xr.DataArray):
        data = data.data
    data = data.squeeze()

    left, right, down, up = np.min(lons), np.max(lons), np.min(lats), np.max(lats)
    lonNumber, latNumber = lons.shape[1], lats.shape[0]

    if how == "nn":
        kk = 1
    else:
        kk = k

    distances_paths = []
    inds_paths = []
    qhull_paths = []

    MESH_BASE = os.path.basename(mesh.path)
    # MESH_DIR = mesh.path
    CACHE_DIR = os.environ.get("PYFESOM_CACHE", os.path.join(os.getcwd(), "MESH_cache"))
    CACHE_DIR = os.path.join(CACHE_DIR, MESH_BASE)

    if not os.path.isdir(CACHE_DIR):
        os.makedirs(CACHE_DIR)

    distances_file = "distances_{}_{}_{}_{}_{}_{}_{}_{}".format(
        mesh.n2d, left, right, down, up, lonNumber, latNumber, kk
    )
    inds_file = "inds_{}_{}_{}_{}_{}_{}_{}_{}".format(
        mesh.n2d, left, right, down, up, lonNumber, latNumber, kk
    )
    qhull_file = "qhull_{}".format(mesh.n2d)

    distances_paths.append(os.path.join(mesh.path, distances_file))
    distances_paths.append(os.path.join(CACHE_DIR, distances_file))

    inds_paths.append(os.path.join(mesh.path, inds_file))
    inds_paths.append(os.path.join(CACHE_DIR, inds_file))

    qhull_paths.append(os.path.join(mesh.path, qhull_file))
    qhull_paths.append(os.path.join(CACHE_DIR, qhull_file))

    # if distances_path is provided, use it first
    if distances_path is not None:
        distances_paths.insert(0, distances_path)

    if inds_path is not None:
        inds_paths.insert(0, inds_path)

    if qhull_path is not None:
        qhull_paths.insert(0, qhull_path)

    loaded_distances = False
    loaded_inds = False
    loaded_qhull = False
    if how == "nn":
        for distances_path in distances_paths:
            if os.path.isfile(distances_path):
                logging.info(
                    "Note: using precalculated file from {}".format(distances_path)
                )
                try:
                    distances = joblib.load(distances_path)
                    loaded_distances = True
                    break
                except PermissionError:
                    # who knows, something didn't work. Try the next path:
                    continue
        for inds_path in inds_paths:
            if os.path.isfile(inds_path):
                logging.info("Note: using precalculated file from {}".format(inds_path))
                try:
                    inds = joblib.load(inds_path)
                    loaded_inds = True
                    break
                except PermissionError:
                    # Same as above...something is wrong
                    continue
        if not (loaded_distances and loaded_inds):
            distances, inds = create_indexes_and_distances(
                mesh, lons, lats, k=kk, n_jobs=n_jobs
            )
            if dumpfile:
                for distances_path in distances_paths:
                    try:
                        joblib.dump(distances, distances_path)
                        break
                    except PermissionError:
                        # Couldn't dump the file, try next path
                        continue
                for inds_path in inds_paths:
                    try:
                        joblib.dump(inds, inds_path)
                        break
                    except PermissionError:
                        # Couldn't dump inds file, try next
                        continue

        data_interpolated = data[inds]
        data_interpolated[distances >= radius_of_influence] = np.nan
        data_interpolated = data_interpolated.reshape(lons.shape)
        data_interpolated = np.ma.masked_invalid(data_interpolated)
        return data_interpolated

    elif how == "idist":
        for distances_path in distances_paths:
            if os.path.isfile(distances_path):
                logging.info(
                    "Note: using precalculated file from {}".format(distances_path)
                )
                try:
                    distances = joblib.load(distances_path)
                    loaded_distances = True
                    break
                except PermissionError:
                    # who knows, something didn't work. Try the next path:
                    continue
        for inds_path in inds_paths:
            if os.path.isfile(inds_path):
                logging.info("Note: using precalculated file from {}".format(inds_path))
                try:
                    inds = joblib.load(inds_path)
                    loaded_inds = True
                    break
                except PermissionError:
                    # Same as above...something is wrong
                    continue
        if not (loaded_distances and loaded_inds):
            distances, inds = create_indexes_and_distances(
                mesh, lons, lats, k=kk, n_jobs=n_jobs
            )
            if dumpfile:
                for distances_path in distances_paths:
                    try:
                        joblib.dump(distances, distances_path)
                        break
                    except PermissionError:
                        # Couldn't dump the file, try next path
                        continue
                for inds_path in inds_paths:
                    try:
                        joblib.dump(inds, inds_path)
                        break
                    except PermissionError:
                        # Couldn't dump inds file, try next
                        continue

        distances_ma = np.ma.masked_greater(distances, radius_of_influence)

        w = 1.0 / distances_ma**2
        data_interpolated = np.ma.sum(w * data[inds], axis=1) / np.ma.sum(w, axis=1)
        data_interpolated.shape = lons.shape
        data_interpolated = np.ma.masked_invalid(data_interpolated)
        return data_interpolated

    elif how == "linear":
        for qhull_path in qhull_paths:
            if os.path.isfile(qhull_path):
                logging.info(
                    "Note: using precalculated file from {}".format(qhull_path)
                )
                try:
                    qh = joblib.load(qhull_path)
                    loaded_qhull = True
                    break
                except PermissionError:
                    # who knows, something didn't work. Try the next path:
                    continue
        if not loaded_qhull:
            points = np.vstack((mesh.x2, mesh.y2)).T
            qh = qhull.Delaunay(points)
            if dumpfile:
                for qhull_path in qhull_paths:
                    try:
                        joblib.dump(qh, qhull_path)
                        break
                    except PermissionError:
                        continue
        data_interpolated = LinearNDInterpolator(qh, data)((lons, lats))
        data_interpolated = np.ma.masked_invalid(data_interpolated)
        return data_interpolated

    elif how == "cubic":
        for qhull_path in qhull_paths:
            if os.path.isfile(qhull_path):
                logging.info(
                    "Note: using precalculated file from {}".format(qhull_path)
                )
                logging.info(
                    "Note: using precalculated file from {}".format(qhull_path)
                )
                try:
                    qh = joblib.load(qhull_path)
                    loaded_qhull = True
                    break
                except PermissionError:
                    # who knows, something didn't work. Try the next path:
                    continue
        if not loaded_qhull:
            points = np.vstack((mesh.x2, mesh.y2)).T
            qh = qhull.Delaunay(points)
            if dumpfile:
                for qhull_path in qhull_paths:
                    try:
                        joblib.dump(qh, qhull_path)
                        break
                    except PermissionError:
                        continue
        data_interpolated = CloughTocher2DInterpolator(qh, data)((lons, lats))
    else:
        raise ValueError("Interpolation method is not supported")


##############################################################################


def attach_mesh_to_rule(data, rule):
    rule.mesh = load_mesh(rule.mesh_path)
    return data


def regrid_to_regular(data, rule):
    mesh = load_mesh(rule.mesh_path)
    box = rule.get("box", "-180, 180, -90, 90")
    x_min, x_max, y_min, y_max = map(float, box.split(","))
    x = np.linspace(x_min, x_max, int(x_max - x_min))
    y = np.linspace(y_min, y_max, int(y_max - y_min))
    lon, lat = np.meshgrid(x, y)
    # This works on a timestep-by-timestep basis, so we need to
    # run an apply here...
    # Apply `fesom2regular` function to each time step
    # breakpoint()
    interpolated = data.chunk({"time": 1}).map_blocks(
        fesom2regular,
        kwargs={"mesh": mesh, "lons": lon, "lats": lat},
        template=xr.DataArray(
            np.empty((len(data["time"]), 360, 180)), dims=["time", "lon", "lat"]
        ).chunk({"time": 1}),
    )
    return interpolated


class FESOMRegridPipeline(FrozenPipeline):
    STEPS = ("pymor.fesom.regrid_to_regular",)
    NAME = "pymor.fesom.FESOMRegridPipeline"
