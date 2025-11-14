"""
Runtime library for generating NetCDF files from YAML stub manifests.

This module provides functions to create xarray Datasets and NetCDF files
from YAML manifests, filling them with random data that matches the
metadata specifications.
"""

from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd
import xarray as xr
import yaml


def parse_dtype(dtype_str: str) -> np.dtype:
    """
    Parse a dtype string into a numpy dtype.

    Parameters
    ----------
    dtype_str : str
        Dtype string (e.g., "float32", "datetime64[ns]")

    Returns
    -------
    np.dtype
        Numpy dtype object
    """
    return np.dtype(dtype_str)


def generate_random_data(shape: tuple, dtype: np.dtype, fill_value: Any = None) -> np.ndarray:
    """
    Generate random data with the specified shape and dtype.

    Parameters
    ----------
    shape : tuple
        Shape of the array
    dtype : np.dtype
        Data type
    fill_value : Any, optional
        Fill value to use for masked/missing data

    Returns
    -------
    np.ndarray
        Random data array
    """
    if dtype.kind in ("U", "S"):  # String types
        return np.array(["stub_data"] * np.prod(shape)).reshape(shape)
    elif dtype.kind == "M":  # Datetime
        # Generate datetime range
        start = pd.Timestamp("2000-01-01")
        return pd.date_range(start, periods=np.prod(shape), freq="D").values.reshape(shape)
    elif dtype.kind == "m":  # Timedelta
        return np.arange(np.prod(shape), dtype=dtype).reshape(shape)
    elif dtype.kind in ("f", "c"):  # Float or complex
        data = np.random.randn(*shape).astype(dtype)
        if fill_value is not None:
            # Randomly mask some values
            mask = np.random.rand(*shape) < 0.01  # 1% missing
            data[mask] = fill_value
        return data
    elif dtype.kind in ("i", "u"):  # Integer
        return np.random.randint(0, 100, size=shape, dtype=dtype)
    elif dtype.kind == "b":  # Boolean
        return np.random.rand(*shape) > 0.5
    else:
        # Default: zeros
        return np.zeros(shape, dtype=dtype)


def create_coordinate(coord_meta: Dict[str, Any], file_index: int = 0) -> xr.DataArray:
    """
    Create a coordinate DataArray from metadata.

    Parameters
    ----------
    coord_meta : Dict[str, Any]
        Coordinate metadata (dtype, dims, shape, attrs)
    file_index : int, optional
        Index of the file being generated (for varying time coordinates)

    Returns
    -------
    xr.DataArray
        Coordinate DataArray
    """
    dtype = parse_dtype(coord_meta["dtype"])
    shape = tuple(coord_meta["shape"])
    dims = coord_meta["dims"]

    # Special handling for time coordinates
    if "sample_value" in coord_meta:
        # Use sample value to infer time range
        # Handle out-of-range dates by using a default range with file_index offset
        try:
            sample = pd.Timestamp(coord_meta["sample_value"])
            # For out-of-range dates, this will fail and we'll use fallback
            data = pd.date_range(sample, periods=shape[0], freq="D").values
        except (ValueError, pd.errors.OutOfBoundsDatetime):
            # Fallback to a default date range, but offset by file_index to ensure uniqueness
            # Parse the sample value to extract day offset if possible
            import re

            sample_str = coord_meta["sample_value"]
            # Try to extract day from date string like "2686-01-02 00:00:00"
            match = re.search(r"\d{4}-\d{2}-(\d{2})", sample_str)
            if match:
                day_offset = int(match.group(1)) - 1  # Day 1 -> offset 0, Day 2 -> offset 1
            else:
                day_offset = file_index

            # Create time coordinate with unique offset
            base = pd.Timestamp("2000-01-01")
            start = base + pd.Timedelta(days=day_offset)
            data = pd.date_range(start, periods=shape[0], freq="D").values
    else:
        # Generate random data
        data = generate_random_data(shape, dtype)

    coord = xr.DataArray(
        data,
        dims=dims,
        attrs=coord_meta.get("attrs", {}),
    )

    return coord


def create_variable(var_meta: Dict[str, Any], coords: Dict[str, xr.DataArray]) -> xr.DataArray:
    """
    Create a variable DataArray from metadata.

    Parameters
    ----------
    var_meta : Dict[str, Any]
        Variable metadata (dtype, dims, shape, attrs, fill_value)
    coords : Dict[str, xr.DataArray]
        Coordinate arrays

    Returns
    -------
    xr.DataArray
        Variable DataArray
    """
    dtype = parse_dtype(var_meta["dtype"])
    shape = tuple(var_meta["shape"])
    dims = var_meta["dims"]
    fill_value = var_meta.get("fill_value")

    # Generate random data
    data = generate_random_data(shape, dtype, fill_value)

    # Create variable
    var = xr.DataArray(
        data,
        dims=dims,
        coords={dim: coords[dim] for dim in dims if dim in coords},
        attrs=var_meta.get("attrs", {}),
    )

    # Set fill value if present
    if fill_value is not None:
        var.attrs["_FillValue"] = fill_value

    return var


def create_dataset_from_metadata(metadata: Dict[str, Any], file_index: int = 0) -> xr.Dataset:
    """
    Create an xarray Dataset from metadata dictionary.

    Parameters
    ----------
    metadata : Dict[str, Any]
        Dataset metadata (dimensions, coordinates, variables, attrs)
    file_index : int, optional
        Index of the file being generated (for varying time coordinates)

    Returns
    -------
    xr.Dataset
        Generated Dataset with random data
    """
    # Create coordinates
    coords = {}
    for coord_name, coord_meta in metadata.get("coordinates", {}).items():
        coords[coord_name] = create_coordinate(coord_meta, file_index)

    # Create variables
    data_vars = {}
    for var_name, var_meta in metadata.get("variables", {}).items():
        data_vars[var_name] = create_variable(var_meta, coords)

    # Create dataset
    ds = xr.Dataset(
        data_vars=data_vars,
        coords=coords,
        attrs=metadata.get("attrs", {}),
    )

    return ds


def load_manifest(manifest_file: Path) -> Dict[str, Any]:
    """
    Load a YAML stub manifest.

    Parameters
    ----------
    manifest_file : Path
        Path to YAML manifest file

    Returns
    -------
    Dict[str, Any]
        Manifest dictionary
    """
    with open(manifest_file, "r") as f:
        manifest = yaml.safe_load(f)
    return manifest


def generate_stub_files(manifest_file: Path, output_dir: Path) -> Path:
    """
    Generate stub NetCDF files from a YAML manifest.

    Parameters
    ----------
    manifest_file : Path
        Path to YAML manifest file
    output_dir : Path
        Output directory for generated NetCDF files

    Returns
    -------
    Path
        Output directory containing generated files
    """
    # Load manifest
    manifest = load_manifest(manifest_file)

    print(f"Generating stub data from {manifest_file}")
    print(f"Output directory: {output_dir}")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate each file
    for file_index, file_meta in enumerate(manifest.get("files", [])):
        file_path = Path(file_meta["path"])
        output_path = output_dir / file_path

        print(f"  Creating {file_path}...")

        # Create output subdirectories
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Generate dataset with file index for unique time coordinates
        ds = create_dataset_from_metadata(file_meta["dataset"], file_index)

        # Write NetCDF
        ds.to_netcdf(output_path)
        ds.close()

    print(f"✓ Generated {len(manifest.get('files', []))} stub files")

    # Generate minimal mesh files if needed
    _generate_mesh_stubs(output_dir, manifest)

    return output_dir


def _generate_mesh_stubs(output_dir: Path, manifest: dict):
    """
    Generate minimal FESOM mesh files for stub testing.

    Creates minimal versions of FESOM mesh files (nod2d.out, elem2d.out, etc.)
    that are sufficient for tests that check for mesh file existence and basic
    structure, without requiring full mesh data.

    Parameters
    ----------
    output_dir : Path
        Output directory where mesh files should be created
    manifest : dict
        Manifest dictionary that may contain mesh_paths key
    """
    # Check if manifest specifies mesh paths
    mesh_paths = manifest.get("mesh_paths", [])
    if not mesh_paths:
        # Infer mesh paths from common patterns in file paths
        mesh_paths = _infer_mesh_paths(output_dir, manifest)

    for mesh_path_str in mesh_paths:
        mesh_path = output_dir / mesh_path_str
        mesh_path.mkdir(parents=True, exist_ok=True)

        # Create minimal nod2d.out (node coordinates)
        # Format: num_nodes \n node_id lon lat flag
        nod2d_file = mesh_path / "nod2d.out"
        with open(nod2d_file, "w") as f:
            f.write("10\n")  # 10 nodes for minimal mesh
            for i in range(1, 11):
                lon = 300.0 + i * 0.1
                lat = 74.0 + i * 0.05
                f.write(f"{i:8d} {lon:14.7f}  {lat:14.7f}        0\n")

        # Create minimal elem2d.out (element connectivity)
        # Format: num_elements \n elem_id node1 node2 \n node2 node3 node4
        elem2d_file = mesh_path / "elem2d.out"
        with open(elem2d_file, "w") as f:
            f.write("5\n")  # 5 elements
            for i in range(1, 6):
                n1, n2, n3 = i, i + 1, i + 2
                f.write(f"{i:8d} {n1:8d} {n2:8d}\n")
                f.write(f"{n2:8d} {n3:8d} {(i % 8) + 1:8d}\n")

        print(f"  Created mesh files in {mesh_path_str}")


def _infer_mesh_paths(output_dir: Path, manifest: dict) -> list:
    """
    Infer mesh directory paths from file paths in manifest.

    Looks for common FESOM mesh path patterns like:
    - awi-esm-1-1-lr_kh800/piControl/input/fesom/mesh
    - input/fesom/mesh/pi
    - fesom_2p6_pimesh/input/fesom/mesh/pi
    - . (root directory for pi_uxarray mesh files)

    Parameters
    ----------
    output_dir : Path
        Output directory
    manifest : dict
        Manifest containing file paths

    Returns
    -------
    list
        List of mesh directory paths (relative to output_dir)
    """
    mesh_paths = set()

    # Check if any files suggest a mesh directory structure
    for file_meta in manifest.get("files", []):
        file_path = file_meta["path"]

        # Pattern 1: awi-esm-1-1-lr_kh800/piControl/input/fesom/mesh
        if "/piControl/outdata/fesom/" in file_path or "/piControl/input/" in file_path:
            # Extract base path and add mesh directory
            parts = file_path.split("/")
            if "piControl" in parts:
                idx = parts.index("piControl")
                mesh_path = "/".join(parts[: idx + 1]) + "/input/fesom/mesh"
                mesh_paths.add(mesh_path)

        # Pattern 2: fesom_2p6_pimesh structure - input/fesom/mesh/pi
        if "fesom_2p6_pimesh" in file_path or "/input/fesom" in file_path:
            mesh_paths.add("input/fesom/mesh/pi")

        # Pattern 3: pi_uxarray - files directly in pi/ directory, mesh also in root
        # Check if file is in a simple "pi/" structure with fesom.mesh.diag.nc
        if file_path.startswith("pi/") and "fesom.mesh.diag.nc" in [f["path"] for f in manifest.get("files", [])]:
            mesh_paths.add(".")  # Mesh files go in root of output_dir

    return list(mesh_paths)
