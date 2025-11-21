"""
Pipeline step to set CF-compliant metadata attributes on coordinate variables.

This module handles setting standard_name, axis, units, and other CF attributes
for coordinate variables (latitude, longitude, vertical coordinates, etc.) to
ensure proper interpretation by xarray and other CF-aware tools.

The time coordinate is handled separately in files.py during the save operation.
"""

from pathlib import Path
from typing import Dict, Optional, Union

import xarray as xr
import yaml

from ..core.logging import logger
from ..core.rule import Rule


def _load_coordinate_metadata() -> Dict[str, Dict[str, str]]:
    """
    Load coordinate metadata from YAML file.

    Returns
    -------
    dict
        Dictionary mapping coordinate names to their CF metadata attributes.

    Notes
    -----
    The metadata is loaded from src/pycmor/data/coordinate_metadata.yaml.
    This allows users to add or modify coordinate definitions without
    changing Python code.
    """
    metadata_file = Path(__file__).parent.parent / "data" / "coordinate_metadata.yaml"

    if not metadata_file.exists():
        logger.warning(
            f"Coordinate metadata file not found: {metadata_file}. "
            "Using empty metadata dictionary."
        )
        return {}

    try:
        with open(metadata_file, "r") as f:
            metadata = yaml.safe_load(f)
        logger.debug(f"Loaded coordinate metadata for {len(metadata)} coordinates")
        return metadata
    except Exception as e:
        logger.error(
            f"Failed to load coordinate metadata from {metadata_file}: {e}. "
            "Using empty metadata dictionary."
        )
        return {}


# Load coordinate metadata from YAML file
# This is loaded once at module import time for performance
COORDINATE_METADATA = _load_coordinate_metadata()


def _get_coordinate_metadata(coord_name: str) -> Optional[Dict[str, str]]:
    """
    Get CF metadata for a coordinate variable.

    Parameters
    ----------
    coord_name : str
        Name of the coordinate variable

    Returns
    -------
    dict or None
        Dictionary of CF attributes, or None if not recognized
    """
    # Direct lookup
    if coord_name in COORDINATE_METADATA:
        return COORDINATE_METADATA[coord_name].copy()

    # Try lowercase match
    coord_lower = coord_name.lower()
    if coord_lower in COORDINATE_METADATA:
        return COORDINATE_METADATA[coord_lower].copy()

    return None


def _should_skip_coordinate(coord_name: str, rule: Rule) -> bool:
    """
    Check if a coordinate should be skipped from metadata setting.

    Parameters
    ----------
    coord_name : str
        Name of the coordinate
    rule : Rule
        Processing rule

    Returns
    -------
    bool
        True if coordinate should be skipped
    """
    # Skip time coordinates (handled separately in files.py)
    if coord_name in ["time", "time1", "time2", "time3", "time4"]:
        return True

    # Skip time-related CMIP7 dimensions (handled separately)
    if coord_name in [
        "time-intv",
        "time-point",
        "time-fxc",
        "climatology",
        "diurnal-cycle",
    ]:
        return True

    # Skip bounds variables
    if coord_name.endswith("_bnds") or coord_name.endswith("_bounds"):
        return True

    return False


def set_coordinate_attributes(
    ds: Union[xr.Dataset, xr.DataArray], rule: Rule
) -> Union[xr.Dataset, xr.DataArray]:
    """
    Set CF-compliant metadata attributes on coordinate variables.

    This function sets standard_name, axis, units, and positive attributes
    on coordinate variables to ensure proper interpretation by xarray and
    other CF-aware tools.

    Time coordinates are handled separately in files.py during save operation.

    Parameters
    ----------
    ds : xr.Dataset or xr.DataArray
        The dataset or data array to process
    rule : Rule
        Processing rule containing configuration

    Returns
    -------
    xr.Dataset or xr.DataArray
        Dataset/DataArray with coordinate attributes set

    Notes
    -----
    This function:
    - Sets CF standard_name, axis, units for recognized coordinates
    - Sets positive attribute for vertical coordinates
    - Skips time coordinates (handled in files.py)
    - Skips bounds variables
    - Validates existing metadata and handles conflicts based on configuration
    - Logs all attribute changes

    Configuration Options
    ---------------------
    xarray_set_coordinate_attributes : bool
        Enable/disable coordinate attribute setting (default: True)
    xarray_set_coordinates_attribute : bool
        Enable/disable 'coordinates' attribute on data variables (default: True)
    xarray_validate_coordinate_attributes : str
        How to handle conflicting metadata in source data:
        - 'ignore': Silent, keep existing values
        - 'warn': Log warning, keep existing values (default)
        - 'error': Raise ValueError
        - 'fix': Overwrite with correct values

    Examples
    --------
    >>> ds = xr.Dataset({
    ...     'tas': (['time', 'lat', 'lon'], data),
    ... }, coords={
    ...     'lat': np.arange(-90, 90, 1),
    ...     'lon': np.arange(0, 360, 1),
    ... })
    >>> ds = set_coordinate_attributes(ds, rule)
    >>> print(ds['lat'].attrs)
    {'standard_name': 'latitude', 'units': 'degrees_north', 'axis': 'Y'}
    """
    # Convert DataArray to Dataset for uniform processing
    input_was_dataarray = isinstance(ds, xr.DataArray)
    if input_was_dataarray:
        ds = ds.to_dataset()

    # Check if coordinate attribute setting is enabled
    if not rule._pycmor_cfg("xarray_set_coordinate_attributes"):
        logger.info("Coordinate attribute setting is disabled in configuration")
        return ds if not input_was_dataarray else ds[ds.data_vars.__iter__().__next__()]

    logger.info("[Coordinate Attributes] Setting CF-compliant metadata")

    coords_processed = 0
    coords_skipped = 0

    # Process each coordinate
    for coord_name in ds.coords:
        # Skip coordinates that should not be processed
        if _should_skip_coordinate(coord_name, rule):
            logger.debug(
                f"  → Skipping '{coord_name}' (handled elsewhere or bounds variable)"
            )
            coords_skipped += 1
            continue

        # Get metadata for this coordinate
        metadata = _get_coordinate_metadata(coord_name)

        if metadata is None:
            logger.debug(f"  → No metadata defined for '{coord_name}'")
            coords_skipped += 1
            continue

        # Set attributes with validation
        logger.info(f"  → Setting attributes for '{coord_name}':")
        validation_mode = rule._pycmor_cfg("xarray_validate_coordinate_attributes")

        for attr_name, attr_value in metadata.items():
            if attr_name not in ds[coord_name].attrs:
                # Attribute not present, set it
                ds[coord_name].attrs[attr_name] = attr_value
                logger.info(f"      • {attr_name} = {attr_value}")
            else:
                # Attribute already exists, validate it
                existing_value = ds[coord_name].attrs[attr_name]

                if existing_value == attr_value:
                    # Values match, all good
                    logger.debug(f"      • {attr_name} already correct ({attr_value})")
                else:
                    # Values don't match, handle according to validation mode
                    if validation_mode == "ignore":
                        logger.debug(
                            f"      • {attr_name} mismatch: got '{existing_value}', "
                            f"expected '{attr_value}' (ignoring)"
                        )
                    elif validation_mode == "warn":
                        logger.warning(
                            f"Coordinate '{coord_name}' has {attr_name}='{existing_value}' "
                            f"but expected '{attr_value}' (keeping existing value)"
                        )
                    elif validation_mode == "error":
                        raise ValueError(
                            f"Invalid {attr_name} for coordinate '{coord_name}': "
                            f"got '{existing_value}', expected '{attr_value}'"
                        )
                    elif validation_mode == "fix":
                        logger.info(
                            f"      • {attr_name} corrected: '{existing_value}' → '{attr_value}'"
                        )
                        ds[coord_name].attrs[attr_name] = attr_value
                    else:
                        logger.warning(
                            f"Unknown validation mode '{validation_mode}', defaulting to 'warn'"
                        )
                        logger.warning(
                            f"Coordinate '{coord_name}' has {attr_name}='{existing_value}' "
                            f"but expected '{attr_value}'"
                        )

        coords_processed += 1

    logger.info(
        f"  → Processed {coords_processed} coordinates, skipped {coords_skipped}"
    )

    # Set 'coordinates' attribute on data variables
    if rule._pycmor_cfg("xarray_set_coordinates_attribute"):
        _set_coordinates_attribute(ds, rule)

    # Return in original format
    if input_was_dataarray:
        return ds[list(ds.data_vars)[0]]
    return ds


def _set_coordinates_attribute(ds: xr.Dataset, rule: Rule) -> None:
    """
    Set the 'coordinates' attribute on data variables.

    This attribute lists all coordinate variables associated with the data
    variable, which is required for CF compliance especially for auxiliary
    coordinates.

    Parameters
    ----------
    ds : xr.Dataset
        Dataset to process (modified in place)
    rule : Rule
        Processing rule
    """
    logger.info(
        "[Coordinate Attributes] Setting 'coordinates' attribute on data variables"
    )

    for var_name in ds.data_vars:
        # Get all coordinates used by this variable
        var_coords = []

        # Get dimension coordinates
        for dim in ds[var_name].dims:
            if dim in ds.coords:
                var_coords.append(dim)

        # Get non-dimension coordinates (auxiliary coordinates)
        for coord_name in ds.coords:
            if coord_name not in var_coords and coord_name in ds[var_name].coords:
                var_coords.append(coord_name)

        if var_coords:
            # Create coordinates attribute string
            coords_str = " ".join(var_coords)
            ds[var_name].attrs["coordinates"] = coords_str
            logger.info(f"  → {var_name}: coordinates = '{coords_str}'")


# Alias for consistency with other modules
set_coordinate_attrs = set_coordinate_attributes
