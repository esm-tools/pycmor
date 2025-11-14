#!/usr/bin/env python
"""
Generate YAML stub manifests from real NetCDF test data.

This script scans NetCDF files and extracts their metadata (dimensions,
coordinates, variables, attributes) to create lightweight YAML manifests
that can be used to generate stub data for testing.

Usage:
    python generate_test_stubs.py <input_dir> --output <output_yaml>

Example:
    python generate_test_stubs.py \
        ~/.cache/pycmor/test_data/awicm_1p0_recom \
        --output tests/fixtures/stub_data/awicm_1p0_recom.yaml
"""

import argparse
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import xarray as xr
import yaml


def serialize_value(value: Any) -> Any:
    """
    Convert numpy/pandas types to JSON/YAML-serializable types.

    Parameters
    ----------
    value : Any
        Value to serialize

    Returns
    -------
    Any
        Serializable value
    """
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    elif isinstance(value, np.ndarray):
        return value.tolist()
    elif isinstance(value, np.bool_):
        return bool(value)
    elif hasattr(value, "dtype"):  # numpy scalar
        return value.item()
    return value


def extract_dataset_metadata(ds: xr.Dataset) -> Dict[str, Any]:
    """
    Extract metadata from an xarray Dataset.

    Parameters
    ----------
    ds : xr.Dataset
        Dataset to extract metadata from

    Returns
    -------
    Dict[str, Any]
        Metadata dictionary with dimensions, coordinates, variables, and attributes
    """
    metadata = {
        "dimensions": dict(ds.sizes),
        "coordinates": {},
        "variables": {},
        "attrs": {},
    }

    # Extract coordinate metadata
    for coord_name, coord in ds.coords.items():
        metadata["coordinates"][coord_name] = {
            "dtype": str(coord.dtype),
            "dims": list(coord.dims),
            "shape": list(coord.shape),
            "attrs": {k: serialize_value(v) for k, v in coord.attrs.items()},
        }

        # For time coordinates, store sample values for reconstruction
        if "time" in coord_name.lower() and coord.size > 0:
            metadata["coordinates"][coord_name]["sample_value"] = str(coord.values[0])

    # Extract data variable metadata
    for var_name, var in ds.data_vars.items():
        var_meta = {
            "dtype": str(var.dtype),
            "dims": list(var.dims),
            "shape": list(var.shape),
            "attrs": {k: serialize_value(v) for k, v in var.attrs.items()},
        }

        # Store fill value if present
        if hasattr(var, "_FillValue"):
            var_meta["fill_value"] = serialize_value(var._FillValue)
        elif "_FillValue" in var.attrs:
            var_meta["fill_value"] = serialize_value(var.attrs["_FillValue"])

        metadata["variables"][var_name] = var_meta

    # Extract global attributes
    metadata["attrs"] = {k: serialize_value(v) for k, v in ds.attrs.items()}

    return metadata


def scan_netcdf_directory(input_dir: Path, relative_to: Path = None) -> List[Dict[str, Any]]:
    """
    Scan a directory for NetCDF files and extract metadata.

    Parameters
    ----------
    input_dir : Path
        Directory to scan
    relative_to : Path, optional
        Base path for relative file paths

    Returns
    -------
    List[Dict[str, Any]]
        List of file metadata dictionaries
    """
    if relative_to is None:
        relative_to = input_dir

    files_metadata = []

    # Find all NetCDF files
    for nc_file in sorted(input_dir.rglob("*.nc")):
        print(f"Processing {nc_file.relative_to(input_dir)}...")

        try:
            # Open dataset
            ds = xr.open_dataset(nc_file)

            # Extract metadata
            file_meta = {
                "path": str(nc_file.relative_to(relative_to)),
                "dataset": extract_dataset_metadata(ds),
            }

            files_metadata.append(file_meta)

            # Close dataset
            ds.close()

        except Exception as e:
            print(f"  ERROR: Failed to process {nc_file}: {e}")
            continue

    return files_metadata


def generate_stub_manifest(input_dir: Path, output_file: Path) -> None:
    """
    Generate a YAML stub manifest from a directory of NetCDF files.

    Parameters
    ----------
    input_dir : Path
        Directory containing NetCDF files
    output_file : Path
        Output YAML file path
    """
    print(f"\nScanning directory: {input_dir}")
    print(f"Output file: {output_file}\n")

    # Scan directory
    files_metadata = scan_netcdf_directory(input_dir, relative_to=input_dir)

    # Create manifest
    manifest = {
        "source_directory": str(input_dir),
        "files": files_metadata,
        "total_files": len(files_metadata),
    }

    # Write YAML
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        yaml.dump(
            manifest,
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )

    print(f"\n✓ Generated manifest with {len(files_metadata)} files")
    print(f"  Output: {output_file}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Generate YAML stub manifests from NetCDF test data")
    parser.add_argument(
        "input_dir",
        type=Path,
        help="Directory containing NetCDF files",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        required=True,
        help="Output YAML file path",
    )

    args = parser.parse_args()

    # Validate input
    if not args.input_dir.exists():
        print(f"ERROR: Input directory does not exist: {args.input_dir}")
        return 1

    # Generate manifest
    generate_stub_manifest(args.input_dir, args.output)

    return 0


if __name__ == "__main__":
    exit(main())
