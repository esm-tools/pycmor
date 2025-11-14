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

    return output_dir
