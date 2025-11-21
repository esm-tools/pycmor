"""
Dimension Mapping for CMORization

This module handles dimension mapping from source data to CMIP table requirements:
1. Semantic dimension detection (identify what dimensions represent)
2. Dimension name mapping (source names → CMIP names)
3. Dimension value validation (check against CMIP standards)
4. Automatic dimension renaming

Key Concepts:
- Source dimensions: Names in the input dataset (e.g., 'latitude', 'lev')
- CMIP dimensions: Names required by CMIP tables (e.g., 'lat', 'plev19')
- Semantic matching: Identify dimensions by metadata, values, or patterns
"""

import logging
import re
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import xarray as xr

from ..data_request.variable import DataRequestVariable

logger = logging.getLogger(__name__)


class DimensionMapper:
    """
    Maps dimensions from source data to CMIP table requirements

    This class handles the "input side" of dimension handling:
    - Identifies what source dimensions represent
    - Maps source dimension names to CMIP dimension names
    - Validates dimension values against CMIP standards
    - Renames dimensions to match CMIP requirements

    Examples
    --------
    >>> mapper = DimensionMapper()
    >>> # Map source dimensions to CMIP dimensions
    >>> mapping = mapper.create_mapping(
    ...     ds=source_dataset,
    ...     data_request_variable=cmip_variable,
    ...     user_mapping={'lev': 'plev19'}
    ... )
    >>> # Apply mapping to dataset
    >>> ds_mapped = mapper.apply_mapping(source_dataset, mapping)
    """

    # Semantic patterns for dimension detection
    DIMENSION_PATTERNS = {
        # Horizontal coordinates
        "latitude": [
            r"^lat(itude)?(_\w+)?$",
            r"^y(lat)?$",
            r"^rlat$",
            r"^nav_lat$",
        ],
        "longitude": [
            r"^lon(gitude)?(_\w+)?$",
            r"^x(lon)?$",
            r"^rlon$",
            r"^nav_lon$",
        ],
        # Vertical coordinates - pressure
        "pressure": [
            r"^(p)?lev(el)?s?$",
            r"^plev\d*$",
            r"^pressure(_\w+)?$",
            r"^pres$",
        ],
        # Vertical coordinates - ocean
        "depth": [
            r"^(o)?lev(el)?s?$",
            r"^depth(_\w+)?$",
            r"^olevel\d*$",
            r"^z(_\w+)?$",
        ],
        # Vertical coordinates - atmosphere
        "model_level": [
            r"^alev(el)?s?$",
            r"^(model_)?level(_\w+)?$",
            r"^lev$",
        ],
        # Vertical coordinates - height
        "height": [
            r"^(alt|height)(_?\d+m?)?$",
            r"^z$",
        ],
        # Time
        "time": [
            r"^time\d*$",
            r"^t$",
        ],
    }

    # Standard names for semantic matching
    STANDARD_NAME_MAP = {
        "latitude": ["latitude", "grid_latitude"],
        "longitude": ["longitude", "grid_longitude"],
        "pressure": ["air_pressure"],
        "depth": ["depth", "ocean_depth"],
        "height": ["height", "altitude"],
        "time": ["time"],
    }

    # Axis attribute for semantic matching
    AXIS_MAP = {
        "latitude": "Y",
        "longitude": "X",
        "pressure": "Z",
        "depth": "Z",
        "height": "Z",
        "model_level": "Z",
        "time": "T",
    }

    def __init__(self):
        """Initialize dimension mapper"""
        self._compile_patterns()

    def _compile_patterns(self):
        """Compile regex patterns for efficiency"""
        self._compiled_patterns = {}
        for dim_type, patterns in self.DIMENSION_PATTERNS.items():
            self._compiled_patterns[dim_type] = [
                re.compile(p, re.IGNORECASE) for p in patterns
            ]

    def detect_dimension_type(self, ds: xr.Dataset, dim_name: str) -> Optional[str]:
        """
        Detect what type of dimension this is (latitude, longitude, pressure, etc.)

        Uses multiple strategies:
        1. Name pattern matching
        2. Standard name attribute
        3. Axis attribute
        4. Value range analysis

        Parameters
        ----------
        ds : xr.Dataset
            Dataset containing the dimension
        dim_name : str
            Name of dimension to detect

        Returns
        -------
        Optional[str]
            Dimension type (e.g., 'latitude', 'longitude', 'pressure')
            or None if cannot be determined
        """
        # Strategy 1: Check name patterns
        for dim_type, patterns in self._compiled_patterns.items():
            for pattern in patterns:
                if pattern.match(dim_name):
                    logger.debug(
                        f"Dimension '{dim_name}' matched pattern for '{dim_type}'"
                    )
                    return dim_type

        # Strategy 2: Check standard_name attribute
        if dim_name in ds.coords:
            coord = ds.coords[dim_name]
            standard_name = coord.attrs.get("standard_name", "").lower()
            for dim_type, std_names in self.STANDARD_NAME_MAP.items():
                if standard_name in std_names:
                    logger.debug(
                        f"Dimension '{dim_name}' matched standard_name for '{dim_type}'"
                    )
                    return dim_type

            # Strategy 3: Check axis attribute
            axis = coord.attrs.get("axis", "").upper()
            for dim_type, expected_axis in self.AXIS_MAP.items():
                if axis == expected_axis:
                    logger.debug(
                        f"Dimension '{dim_name}' matched axis for '{dim_type}'"
                    )
                    return dim_type

            # Strategy 4: Analyze values
            dim_type = self._detect_from_values(coord)
            if dim_type:
                logger.debug(
                    f"Dimension '{dim_name}' detected from values as '{dim_type}'"
                )
                return dim_type

        logger.debug(f"Could not detect type for dimension '{dim_name}'")
        return None

    def _detect_from_values(self, coord: xr.DataArray) -> Optional[str]:
        """
        Detect dimension type from coordinate values

        Parameters
        ----------
        coord : xr.DataArray
            Coordinate variable

        Returns
        -------
        Optional[str]
            Dimension type or None
        """
        try:
            values = coord.values
            if len(values) == 0:
                return None

            # Check for latitude (-90 to 90)
            if np.all(values >= -90) and np.all(values <= 90):
                if len(values) > 10:  # Likely a grid
                    return "latitude"

            # Check for longitude (0 to 360 or -180 to 180)
            if (np.all(values >= 0) and np.all(values <= 360)) or (
                np.all(values >= -180) and np.all(values <= 180)
            ):
                if len(values) > 10:  # Likely a grid
                    return "longitude"

            # Check for pressure (typically in Pa or hPa)
            if np.all(values > 0):
                # Pressure in Pa: typically 100 to 100000
                if np.all(values >= 100) and np.all(values <= 110000):
                    return "pressure"
                # Pressure in hPa: typically 1 to 1100
                if np.all(values >= 1) and np.all(values <= 1100):
                    return "pressure"

            # Check for depth (negative or positive, typically meters)
            if np.all(values >= -10000) and np.all(values <= 10000):
                # Could be depth, but need more context
                pass

        except (ValueError, TypeError):
            pass

        return None

    def map_to_cmip_dimension(
        self,
        dim_type: str,
        cmip_dimensions: List[str],
        coord_size: Optional[int] = None,
    ) -> Optional[str]:
        """
        Map a detected dimension type to a specific CMIP dimension name

        Parameters
        ----------
        dim_type : str
            Detected dimension type (e.g., 'latitude', 'pressure')
        cmip_dimensions : List[str]
            List of dimension names from CMIP table
        coord_size : Optional[int]
            Size of the coordinate (helps distinguish plev19 vs plev8, etc.)

        Returns
        -------
        Optional[str]
            CMIP dimension name or None if no match
        """
        # Map dimension types to CMIP dimension patterns
        type_to_cmip = {
            "latitude": ["latitude", "lat", "gridlatitude"],
            "longitude": ["longitude", "lon", "gridlongitude"],
            "time": ["time", "time1", "time2", "time3"],
            "pressure": [
                "plev",
                "plev3",
                "plev4",
                "plev7",
                "plev8",
                "plev19",
                "plev23",
                "plev27",
                "plev39",
            ],
            "depth": ["olevel", "olevhalf", "oline", "depth"],
            "height": [
                "height",
                "height2m",
                "height10m",
                "height100m",
                "alt16",
                "alt40",
            ],
            "model_level": ["alevel", "alevhalf"],
        }

        possible_names = type_to_cmip.get(dim_type, [])

        # Find matching CMIP dimension
        for cmip_dim in cmip_dimensions:
            cmip_lower = cmip_dim.lower()
            for possible in possible_names:
                if cmip_lower == possible.lower():
                    # If size is provided, check if it matches (for plevN dimensions)
                    if coord_size is not None and dim_type == "pressure":
                        # Extract number from dimension name (e.g., plev19 -> 19)
                        match = re.search(r"plev(\d+)", cmip_dim, re.IGNORECASE)
                        if match:
                            expected_size = int(match.group(1))
                            if coord_size == expected_size:
                                return cmip_dim
                        else:
                            # Generic 'plev' without number
                            return cmip_dim
                    else:
                        return cmip_dim

        return None

    def create_mapping(
        self,
        ds: xr.Dataset,
        data_request_variable: DataRequestVariable,
        user_mapping: Optional[Dict[str, str]] = None,
        allow_override: bool = True,
    ) -> Dict[str, str]:
        """
        Create dimension mapping from source dataset to CMIP requirements

        Parameters
        ----------
        ds : xr.Dataset
            Source dataset
        data_request_variable : DataRequestVariable
            CMIP variable specification with required dimensions
        user_mapping : Optional[Dict[str, str]]
            User-specified mapping {source_dim: output_dim}.
            Can override CMIP table dimension names if allow_override=True.
        allow_override : bool
            If True, allows user_mapping to override CMIP table dimension names.
            If False, validates that user mappings match CMIP requirements.
            Default: True

        Returns
        -------
        Dict[str, str]
            Mapping from source dimension names to CMIP dimension names

        Examples
        --------
        >>> mapping = mapper.create_mapping(
        ...     ds=source_ds,
        ...     data_request_variable=cmip_var,
        ...     user_mapping={'lev': 'plev19'}
        ... )
        >>> # mapping = {'time': 'time', 'lev': 'plev19', 'latitude': 'lat', 'longitude': 'lon'}
        """
        cmip_dims = list(data_request_variable.dimensions)
        source_dims = list(ds.sizes.keys())

        logger.info("Creating dimension mapping")
        logger.info(f"  Source dimensions: {source_dims}")
        logger.info(f"  CMIP dimensions: {cmip_dims}")

        mapping = {}
        mapped_cmip = set()
        mapped_source = set()

        # Step 1: Apply user-specified mappings
        if user_mapping:
            for source_dim, output_dim in user_mapping.items():
                if source_dim not in source_dims:
                    logger.warning(
                        f"User mapping specifies source dimension '{source_dim}' "
                        f"which doesn't exist in dataset"
                    )
                    continue

                # In flexible mode, allow any output dimension name
                # In strict mode, warn if output dimension not in CMIP table
                if not allow_override and output_dim not in cmip_dims:
                    logger.warning(
                        f"User mapping specifies output dimension '{output_dim}' "
                        f"which is not in CMIP table (strict mode)"
                    )

                mapping[source_dim] = output_dim
                mapped_source.add(source_dim)
                if output_dim in cmip_dims:
                    mapped_cmip.add(output_dim)
                logger.info(f"  User mapping: {source_dim} → {output_dim}")

        # Step 2: Auto-detect and map remaining dimensions
        unmapped_source = [d for d in source_dims if d not in mapped_source]
        unmapped_cmip = [d for d in cmip_dims if d not in mapped_cmip]

        for source_dim in unmapped_source:
            # Detect dimension type
            dim_type = self.detect_dimension_type(ds, source_dim)
            if not dim_type:
                logger.debug(f"  Could not detect type for '{source_dim}'")
                continue

            # Get coordinate size
            coord_size = ds.sizes[source_dim] if source_dim in ds.sizes else None

            # Map to CMIP dimension
            cmip_dim = self.map_to_cmip_dimension(dim_type, unmapped_cmip, coord_size)
            if cmip_dim:
                mapping[source_dim] = cmip_dim
                mapped_source.add(source_dim)
                mapped_cmip.add(cmip_dim)
                unmapped_cmip.remove(cmip_dim)
                logger.info(
                    f"  Auto-mapped: {source_dim} → {cmip_dim} (type: {dim_type})"
                )

        # Report unmapped dimensions
        final_unmapped_source = [d for d in source_dims if d not in mapped_source]
        final_unmapped_cmip = [d for d in cmip_dims if d not in mapped_cmip]

        if final_unmapped_source:
            logger.warning(f"Unmapped source dimensions: {final_unmapped_source}")
        if final_unmapped_cmip:
            logger.warning(f"Unmapped CMIP dimensions: {final_unmapped_cmip}")

        return mapping

    def apply_mapping(self, ds: xr.Dataset, mapping: Dict[str, str]) -> xr.Dataset:
        """
        Apply dimension mapping to dataset (rename dimensions)

        Parameters
        ----------
        ds : xr.Dataset
            Source dataset
        mapping : Dict[str, str]
            Mapping from source dimension names to CMIP dimension names

        Returns
        -------
        xr.Dataset
            Dataset with renamed dimensions

        Examples
        --------
        >>> ds_mapped = mapper.apply_mapping(ds, {'latitude': 'lat', 'longitude': 'lon'})
        """
        logger.info("Applying dimension mapping")
        rename_dict = {}

        for source_dim, cmip_dim in mapping.items():
            if source_dim != cmip_dim:
                rename_dict[source_dim] = cmip_dim
                logger.info(f"  Renaming: {source_dim} → {cmip_dim}")

        if rename_dict:
            ds = ds.rename(rename_dict)
            logger.info(f"Renamed {len(rename_dict)} dimensions")
        else:
            logger.info("No dimension renaming needed")

        return ds

    def validate_mapping(
        self,
        ds: xr.Dataset,
        mapping: Dict[str, str],
        data_request_variable: DataRequestVariable,
        allow_override: bool = True,
    ) -> Tuple[bool, List[str]]:
        """
        Validate that dimension mapping is complete and correct

        Parameters
        ----------
        ds : xr.Dataset
            Source dataset
        mapping : Dict[str, str]
            Dimension mapping
        data_request_variable : DataRequestVariable
            CMIP variable specification
        allow_override : bool
            If True, allows output dimensions to differ from CMIP table.
            If False, validates that output matches CMIP requirements.
            Default: True

        Returns
        -------
        Tuple[bool, List[str]]
            (is_valid, list of error messages)
        """
        errors = []
        cmip_dims = set(data_request_variable.dimensions)
        mapped_output = set(mapping.values())

        if not allow_override:
            # Strict mode: output dimensions must match CMIP table
            missing_cmip = cmip_dims - mapped_output
            if missing_cmip:
                errors.append(
                    f"Missing CMIP dimensions in mapping: {sorted(missing_cmip)}"
                )

            # Check for non-CMIP dimensions in output
            extra_dims = mapped_output - cmip_dims
            if extra_dims:
                errors.append(
                    f"Output dimensions not in CMIP table: {sorted(extra_dims)}"
                )
        else:
            # Flexible mode: just check that we have the right number of dimensions
            if len(mapped_output) != len(cmip_dims):
                logger.warning(
                    f"Dimension count mismatch: "
                    f"CMIP table expects {len(cmip_dims)} dimensions, "
                    f"mapping provides {len(mapped_output)}"
                )

        # Check if all source dimensions exist
        for source_dim in mapping.keys():
            if source_dim not in ds.sizes:
                errors.append(f"Source dimension '{source_dim}' not found in dataset")

        # Check for duplicate mappings
        if len(mapping.values()) != len(set(mapping.values())):
            errors.append("Duplicate output dimensions in mapping")

        is_valid = len(errors) == 0
        return is_valid, errors


def map_dimensions(
    ds: Union[xr.Dataset, xr.DataArray], rule
) -> Union[xr.Dataset, xr.DataArray]:
    """
    Pipeline function to map dimensions from source to CMIP requirements

    This function:
    1. Detects dimension types in source data
    2. Maps source dimension names to CMIP dimension names
    3. Renames dimensions to match CMIP requirements
    4. Validates the mapping

    Parameters
    ----------
    ds : Union[xr.Dataset, xr.DataArray]
        Input dataset or data array
    rule : Rule
        Rule object containing data request variable and configuration

    Returns
    -------
    Union[xr.Dataset, xr.DataArray]
        Dataset with renamed dimensions

    Examples
    --------
    >>> # In pipeline
    >>> ds = map_dimensions(ds, rule)
    """
    # Convert DataArray to Dataset if needed
    if isinstance(ds, xr.DataArray):
        was_dataarray = True
        da_name = ds.name
        ds = ds.to_dataset()
    else:
        was_dataarray = False

    # Check if dimension mapping is enabled
    if not rule._pycmor_cfg("xarray_enable_dimension_mapping"):
        logger.debug("Dimension mapping is disabled")
        return ds if not was_dataarray else ds[da_name]

    # Get user-specified mapping from rule
    user_mapping = rule._pycmor_cfg("dimension_mapping", default={})

    # Get allow_override setting
    allow_override = rule._pycmor_cfg("dimension_mapping_allow_override", default=True)

    # Create mapper
    mapper = DimensionMapper()

    # Create mapping
    try:
        mapping = mapper.create_mapping(
            ds=ds,
            data_request_variable=rule.data_request_variable,
            user_mapping=user_mapping,
            allow_override=allow_override,
        )

        # Validate mapping
        is_valid, errors = mapper.validate_mapping(
            ds, mapping, rule.data_request_variable, allow_override=allow_override
        )

        if not is_valid:
            validation_mode = rule._pycmor_cfg(
                "dimension_mapping_validation", default="warn"
            )
            error_msg = "Dimension mapping validation failed:\n" + "\n".join(
                f"  - {e}" for e in errors
            )

            if validation_mode == "error":
                raise ValueError(error_msg)
            elif validation_mode == "warn":
                logger.warning(error_msg)
            # ignore mode: do nothing

        # Apply mapping
        ds = mapper.apply_mapping(ds, mapping)

    except Exception as e:
        logger.error(f"Error in dimension mapping: {e}")
        raise

    # Convert back to DataArray if needed
    if was_dataarray:
        return ds[da_name]
    return ds
