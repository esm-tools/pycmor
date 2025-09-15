# This file is part of pyfesom
#
################################################################################
#
# Original code by Dmitry Sidorenko, 2013
# https://github.com/FESOM/pyfesom2
#
# Modifications:
#   Nikolay Koldunov, 2016
#          - optimisation of reading ASCII fies (add pandas dependency)
#          - move loading and processing of the mesh to the mesh class itself
#
#   Paul Gierz, 2024
#          - extracted relevant functions from original code only for pymor
#          - general cleanup of booleans (usepickle, usejoblib)
#
################################################################################

import logging
import math as mt
import os
import pickle
from datetime import datetime

import joblib
import numpy as np
import pandas as pd


def scalar_r2g(al, be, ga, rlon, rlat):
    """
    Converts rotated coordinates to geographical coordinates.

    Parameters
    ----------
    al : float
        alpha Euler angle
    be : float
        beta Euler angle
    ga : float
        gamma Euler angle
    rlon : array
        1d array of longitudes in rotated coordinates
    rlat : array
        1d araay of latitudes in rotated coordinates

    Returns
    -------
    lon : array
        1d array of longitudes in geographical coordinates
    lat : array
        1d array of latitudes in geographical coordinates

    """

    rad = mt.pi / 180
    al = al * rad
    be = be * rad
    ga = ga * rad
    rotate_matrix = np.zeros(shape=(3, 3))
    rotate_matrix[0, 0] = np.cos(ga) * np.cos(al) - np.sin(ga) * np.cos(be) * np.sin(al)
    rotate_matrix[0, 1] = np.cos(ga) * np.sin(al) + np.sin(ga) * np.cos(be) * np.cos(al)
    rotate_matrix[0, 2] = np.sin(ga) * np.sin(be)
    rotate_matrix[1, 0] = -np.sin(ga) * np.cos(al) - np.cos(ga) * np.cos(be) * np.sin(
        al
    )
    rotate_matrix[1, 1] = -np.sin(ga) * np.sin(al) + np.cos(ga) * np.cos(be) * np.cos(
        al
    )
    rotate_matrix[1, 2] = np.cos(ga) * np.sin(be)
    rotate_matrix[2, 0] = np.sin(be) * np.sin(al)
    rotate_matrix[2, 1] = -np.sin(be) * np.cos(al)
    rotate_matrix[2, 2] = np.cos(be)

    rotate_matrix = np.linalg.pinv(rotate_matrix)

    rlat = rlat * rad
    rlon = rlon * rad

    # Rotated Cartesian coordinates:
    xr = np.cos(rlat) * np.cos(rlon)
    yr = np.cos(rlat) * np.sin(rlon)
    zr = np.sin(rlat)

    # Geographical Cartesian coordinates:
    xg = rotate_matrix[0, 0] * xr + rotate_matrix[0, 1] * yr + rotate_matrix[0, 2] * zr
    yg = rotate_matrix[1, 0] * xr + rotate_matrix[1, 1] * yr + rotate_matrix[1, 2] * zr
    zg = rotate_matrix[2, 0] * xr + rotate_matrix[2, 1] * yr + rotate_matrix[2, 2] * zr

    # Geographical coordinates:
    lat = np.arcsin(zg)
    lon = np.arctan2(yg, xg)

    a = np.where((np.abs(xg) + np.abs(yg)) == 0)
    if a:
        lon[a] = 0

    lat = lat / rad
    lon = lon / rad

    return (lon, lat)


def load_mesh(path, abg=[50, 15, -90], get3d=True, usepickle=True, usejoblib=False):
    """Loads FESOM mesh

    Parameters
    ----------
    path : str
        Path to the directory with mesh files
    abg : list
        alpha, beta and gamma Euler angles. Default [50, 15, -90]
    get3d : bool
        do we load complete 3d mesh or only 2d nodes.

    Returns
    -------
    mesh : object
        fesom_mesh object
    """
    python_version = "3"
    path = os.path.abspath(path)
    if usepickle and usejoblib:
        raise ValueError(
            "Both `usepickle` and `usejoblib` set to True, select only one"
        )

    if usepickle:
        pickle_file = os.path.join(path, "pickle_mesh_py3")

    if usejoblib:
        joblib_file = os.path.join(path, "joblib_mesh")

    if usepickle and (os.path.isfile(pickle_file)):
        print("The usepickle == True)")
        print("The pickle file for python 3 exists.")
        print("The mesh will be loaded from {}".format(pickle_file))

        ifile = open(pickle_file, "rb")
        mesh = pickle.load(ifile)
        ifile.close()
        return mesh

    elif usepickle and not os.path.isfile(pickle_file):
        print("The usepickle == True")
        print("The pickle file for python 3 DO NOT exists")
        print("The mesh will be saved to {}".format(pickle_file))

        mesh = fesom_mesh(path=path, abg=abg, get3d=get3d)
        try:
            logging.info("Use pickle to save the mesh information")
            print("Save mesh to binary format")
            outfile = open(pickle_file, "wb")
            pickle.dump(mesh, outfile)
            outfile.close()
        except OSError:
            logging.warning("Unable to save pickle as mesh cache, sorry...")
        return mesh

    elif not usepickle and not usejoblib:
        mesh = fesom_mesh(path=path, abg=abg, get3d=get3d)
        return mesh

    if usejoblib and os.path.isfile(joblib_file):
        print("The usejoblib == True)")
        print("The joblib file for python {} exists.".format(str(python_version)))
        print("The mesh will be loaded from {}".format(joblib_file))

        mesh = joblib.load(joblib_file)
        return mesh

    elif usejoblib and not os.path.isfile(joblib_file):
        print("The usejoblib == True")
        print("The joblib file for python {} DO NOT exists".format(str(python_version)))
        print("The mesh will be saved to {}".format(joblib_file))

        mesh = fesom_mesh(path=path, abg=abg, get3d=get3d)
        logging.info("Use joblib to save the mesh information")
        print("Save mesh to binary format")
        joblib.dump(mesh, joblib_file)

        return mesh


class fesom_mesh(object):
    """Creates instance of the FESOM mesh.

    This class creates instance that contain information
    about FESOM mesh. At present the class works with
    ASCII representation of the FESOM grid, but should be extended
    to be able to read also netCDF version (probably UGRID convention).

    Minimum requirement is to provide the path to the directory,
    where following files should be located (not nessesarely all of them will
    be used):

    - nod2d.out
    - nod3d.out
    - elem2d.out
    - aux3d.out

    Parameters
    ----------
    path : str
        Path to the directory with mesh files

    abg : list
        alpha, beta and gamma Euler angles. Default [50, 15, -90]

    get3d : bool
        do we load complete 3d mesh or only 2d nodes.

    Attributes
    ----------
    path : str
        Path to the directory with mesh files
    x2 : array
        x position (lon) of the surface node
    y2 : array
        y position (lat) of the surface node
    n2d : int
        number of 2d nodes
    e2d : int
        number of 2d elements (triangles)
    n3d : int
        number of 3d nodes
    nlev : int
        number of vertical levels
    zlevs : array
        array of vertical level depths
    voltri : array
        array with 2d volume of triangles
    alpha : float
        Euler angle alpha
    beta : float
        Euler angle beta
    gamma : float
        Euler angle gamma

    Returns
    -------
    mesh : object
        fesom_mesh object
    """

    def __init__(self, path, abg=[50, 15, -90], get3d=True):
        self.path = os.path.abspath(path)

        if not os.path.exists(self.path):
            raise IOError('The path "{}" does not exists'.format(self.path))

        self.alpha = abg[0]
        self.beta = abg[1]
        self.gamma = abg[2]

        self.nod2dfile = os.path.join(self.path, "nod2d.out")
        self.elm2dfile = os.path.join(self.path, "elem2d.out")
        self.aux3dfile = os.path.join(self.path, "aux3d.out")
        self.nod3dfile = os.path.join(self.path, "nod3d.out")

        self.n3d = 0
        self.e2d = 0
        self.nlev = 0
        self.zlevs = []
        # self.x2= []
        # self.y2= []
        # self.elem= []
        self.n32 = []
        # self.no_cyclic_elem=[]
        self.topo = []
        self.voltri = []

        logging.info("load 2d part of the grid")
        start = datetime.now()
        self.read2d()
        end = datetime.now()
        print("Load 2d part of the grid in {} second(s)".format(end - start))

        if get3d:
            logging.info("load 3d part of the grid")
            start = datetime.now()
            self.read3d()
            end = datetime.now()
            print("Load 3d part of the grid in {} seconds".format(end - start))

    def read2d(self):
        """Reads only surface part of the mesh.
        Useful if your mesh is large and you want to visualize only surface.
        """
        file_content = pd.read_csv(
            self.nod2dfile,
            delim_whitespace=True,
            skiprows=1,
            names=["node_number", "x", "y", "flag"],
        )
        self.x2 = file_content.x.values
        self.y2 = file_content.y.values
        self.ind2d = file_content.flag.values
        self.n2d = len(self.x2)

        file_content = pd.read_csv(
            self.elm2dfile,
            delim_whitespace=True,
            skiprows=1,
            names=["first_elem", "second_elem", "third_elem"],
        )
        self.elem = file_content.values - 1
        self.e2d = np.shape(self.elem)[0]

        ###########################################
        # here we compute the volumes of the triangles
        # this should be moved into fesom generan mesh output netcdf file
        #
        r_earth = 6371000.0
        rad = np.pi / 180
        edx = self.x2[self.elem]
        edy = self.y2[self.elem]
        ed = np.array([edx, edy])

        jacobian2D = ed[:, :, 1] - ed[:, :, 0]
        jacobian2D = np.array([jacobian2D, ed[:, :, 2] - ed[:, :, 0]])
        for j in range(2):
            mind = [i for (i, val) in enumerate(jacobian2D[j, 0, :]) if val > 355]
            pind = [i for (i, val) in enumerate(jacobian2D[j, 0, :]) if val < -355]
            jacobian2D[j, 0, mind] = jacobian2D[j, 0, mind] - 360
            jacobian2D[j, 0, pind] = jacobian2D[j, 0, pind] + 360

        jacobian2D = jacobian2D * r_earth * rad

        for k in range(2):
            jacobian2D[k, 0, :] = jacobian2D[k, 0, :] * np.cos(edy * rad).mean(axis=1)

        self.voltri = abs(np.linalg.det(np.rollaxis(jacobian2D, 2))) / 2.0

        # compute the 2D lump operator
        # cnt = np.array((0,) * self.n2d)
        self.lump2 = np.array((0.0,) * self.n2d)
        for i in range(3):
            for j in range(self.e2d):
                n = self.elem[j, i]
                # cnt[n]=cnt[n]+1
                self.lump2[n] = self.lump2[n] + self.voltri[j]
        self.lump2 = self.lump2 / 3.0

        self.x2, self.y2 = scalar_r2g(
            self.alpha, self.beta, self.gamma, self.x2, self.y2
        )
        d = self.x2[self.elem].max(axis=1) - self.x2[self.elem].min(axis=1)
        self.no_cyclic_elem = [i for (i, val) in enumerate(d) if val < 100]

        return self

    def read3d(self):
        """
        Reads 3d part of the mesh.
        """
        self.n3d = int(open(self.nod3dfile).readline().rstrip())
        df = pd.read_csv(
            self.nod3dfile,
            delim_whitespace=True,
            skiprows=1,
            names=["node_number", "x", "y", "z", "flag"],
        )
        zcoord = -df.z.values
        self.zlevs = np.unique(zcoord)

        with open(self.aux3dfile) as f:
            self.nlev = int(next(f))
            self.n32 = np.fromiter(
                f, dtype=np.int32, count=self.n2d * self.nlev
            ).reshape(self.n2d, self.nlev)

        self.topo = np.zeros(shape=(self.n2d))
        for prof in self.n32:
            ind_nan = prof[prof > 0]
            ind_nan = ind_nan[-1]
            self.topo[prof[0] - 1] = zcoord[ind_nan - 1]

        return self

    def __repr__(self):
        meshinfo = """
FESOM mesh:
path                  = {}
alpha, beta, gamma    = {}, {}, {}
number of 2d nodes    = {}
number of 2d elements = {}
number of 3d nodes    = {}

        """.format(
            self.path,
            str(self.alpha),
            str(self.beta),
            str(self.gamma),
            str(self.n2d),
            str(self.e2d),
            str(self.n3d),
        )
        return meshinfo

    def __str__(self):
        return self.__repr__()


def ind_for_depth(depth, mesh):
    """
    Return indexes that belong to certain depth.

    Parameters
    ----------
    depth : float
        desired depth. Note there will be no interpolation, the model level
        that is closest to desired depth will be selected.
    mesh : object
        FESOM mesh object

    Returns
    -------
    ind_depth : 1d array
        vector with the size equal to the size of the surface nodes with index values where
        we have data values and missing values where we don't have data values.
    ind_noempty : 1d array
        vector with indexes of the `ind_depth` that have data values.
    ind_empty : 1d array
        vector with indexes of the `ind_depth` that do not have data values.
    """

    # Find indexes of the model depth that are closest to the required depth.
    dind = (abs(mesh.zlevs - depth)).argmin()
    # select data from the level and find indexes with values and with nans
    ind_depth = mesh.n32[:, dind] - 1
    ind_noempty = np.where(ind_depth >= 0)[0]
    ind_empty = np.where(ind_depth < 0)[0]
    return ind_depth, ind_noempty, ind_empty
