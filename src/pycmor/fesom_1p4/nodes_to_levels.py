#!/usr/bin/env python3
"""
This module contains a function to convert FESOM 1.4 output data stored in
the dimensions (nodes, time) to the dimensions (nodes, levels, time).

You can include it in your Pipeline by using the step::

    pymor.fesom1.nodes_to_levels

This script can also be used stand-alone::

    $ pymor scripts fesom1 nodes_to_levels mesh in_path out_path [variable]

The argument ``[variable]`` defaults to ``"temp"``.
"""
import os

import numpy as np
import rich_click as click
import xarray as xr
from dask.diagnostics import ProgressBar

from .load_mesh_data import ind_for_depth, load_mesh


def open_dataset(filepath):
    """Open a dataset with Xarray."""
    return xr.open_dataset(filepath, engine="netcdf4", decode_times=False)


def save_dataset(ds, filepath, compute=True):
    """Save an Xarray dataset to a NetCDF file."""
    print(f"Saving {filepath}")
    with ProgressBar():
        ds.to_netcdf(filepath, mode="w", format="NETCDF4", compute=compute)


def process_dataset(input_path, output_path, processor, skip=False):
    """
    General framework for loading, processing, and saving datasets.

    Parameters:
        input_path (str): Path to the input file.
        output_path (str): Path to the output file.
        processor (function): Function that takes an Xarray dataset and returns a processed one.
        skip (bool): Whether to skip processing if the output file exists.
    """
    if skip and os.path.isfile(output_path):
        print(f"File {output_path} exists. Skipping.")
        return

    ds_in = open_dataset(input_path)
    ds_out = processor(ds_in)
    save_dataset(ds_out, output_path)
    ds_in.close()


def interpolate_to_levels(data, mesh, indices):
    """
    Interpolates unstructured data onto depth levels for FESOM.

    Parameters:
        data (np.ndarray): Input data for a single time step.
        mesh (object): FESOM mesh object.
        indices (dict): Precomputed depth and mask indices.

    Returns:
        np.ndarray: Interpolated data (depth, ncells).
    """
    level_data = np.full((len(mesh.zlevs), mesh.n2d), np.nan)
    for i, (ind_depth, ind_noempty, ind_empty) in enumerate(
        zip(
            indices["ind_depth_all"],
            indices["ind_noempty_all"],
            indices["ind_empty_all"],
        )
    ):
        level_data[i, ind_noempty] = data[ind_depth[ind_noempty]]
        level_data[i, ind_empty] = np.nan  # Fill missing values
    return level_data


def interpolate_dataarray(ds_in, mesh, indices):
    """
    Applies depth-level interpolation across an entire dataset.

    Parameters:
        ds_in (xarray.DataArray): Input dataset.
        mesh (object): FESOM mesh object.
        variable (str): Variable name to interpolate.
        indices (dict): Precomputed depth and mask indices.

    Returns:
        xarray.Dataset: Dataset with interpolated values.
    """
    variable = ds_in.name

    # Apply interpolation per time step
    level_data = xr.apply_ufunc(
        interpolate_to_levels,
        # data_var,
        ds_in,
        input_core_dims=[["nodes_3d"]],
        output_core_dims=[["depth", "ncells"]],
        vectorize=True,
        dask="parallelized",
        kwargs={"mesh": mesh, "indices": indices},
        output_dtypes=[np.float32],
        output_sizes={"depth": len(mesh.zlevs), "ncells": mesh.n2d},
    )

    # Build the output dataset
    coords = {
        "time": ds_in["time"],
        "depth": ("depth", mesh.zlevs),
        "ncells": ("ncells", np.arange(mesh.n2d)),
    }
    attrs = ds_in.attrs.copy()
    attrs.update(
        {
            "grid_type": "unstructured",
            "description": f"{variable} interpolated to FESOM levels",
        }
    )

    # FIXME(PG): This works but is hard to read. Level_data is already an xr.DataArray, so
    # we need to get out the "data" attribute again...
    ds_out = xr.Dataset(
        {variable: (["time", "depth", "ncells"], level_data.data, attrs)},
        coords=coords,
    )

    # Add metadata for time and depth
    ds_out["time"].attrs = ds_in["time"].attrs
    ds_out["depth"].attrs = {
        "units": "m",
        "long_name": "depth",
        "positive": "down",
        "axis": "Z",
    }
    return ds_out


def interpolate_dataset(ds_in, variable, mesh, indices):
    """Interpolate Dataset -> Interpolate DataArray converter function"""
    da_in = ds_in[variable]
    return interpolate_dataarray(da_in, mesh, indices)


def nodes_to_levels(data, rule):
    mesh = rule.mesh_path
    mesh = load_mesh(mesh)
    indices = indicies_from_mesh(mesh)
    data = interpolate_dataarray(data, mesh, indices)
    return data


# FIXME(PG): This should be... a method of the mesh object??
def indicies_from_mesh(mesh):
    # Precompute depth indices
    indices = {
        "ind_depth_all": [],
        "ind_noempty_all": [],
        "ind_empty_all": [],
    }
    for zlev in mesh.zlevs:
        ind_depth, ind_noempty, ind_empty = ind_for_depth(zlev, mesh)
        indices["ind_depth_all"].append(ind_depth)
        indices["ind_noempty_all"].append(ind_noempty)
        indices["ind_empty_all"].append(ind_empty)

    return indices


def _convert(meshpath, ipath, opath, variable, ncore, skip):
    """Main CLI for FESOM unstructured-to-structured conversion."""
    mesh = load_mesh(meshpath, usepickle=False, usejoblib=True)
    indices = indicies_from_mesh(mesh)

    # Define a reusable processor function
    def processor(ds_in):
        return interpolate_dataset(ds_in, variable, mesh, indices)

    # Process all input files
    for ifile in ipath:
        ofile = os.path.join(opath, f"{os.path.basename(ifile)[:-3]}_levels.nc")
        process_dataset(ifile, ofile, processor, skip=skip)


@click.command()
@click.argument("meshpath", type=click.Path(exists=True), required=True)
@click.argument("ipath", nargs=-1, type=click.Path(exists=True), required=True)
@click.argument("opath", nargs=1, required=False, default="./")
@click.argument("variable", nargs=1, required=False, default="temp")
@click.option(
    "--ncore",
    "-n",
    default=2,
    help="Number of cores (parallel processes)",
    show_default=True,
)
@click.option(
    "--skip",
    "-s",
    is_flag=True,
    help="Skip the calculation if the output file already exists.",
)
def convert(meshpath, ipath, opath, variable, ncore, skip):
    """Main CLI for FESOM unstructured-to-structured conversion."""
    _convert(meshpath, ipath, opath, variable, ncore, skip)


if __name__ == "__main__":
    convert()
