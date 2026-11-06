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
from .coordinate_attributes import AXIS_ENTRIES

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
    .. note::
       These examples are illustrative and not verified by doctests.

    .. code-block:: python

        mapper = DimensionMapper()
        # Map source dimensions to CMIP dimensions
        mapping = mapper.create_mapping(
            ds=source_dataset,
            data_request_variable=cmip_variable,
            user_mapping={'lev': 'plev19'}
        )
        # Apply mapping to dataset
        ds_mapped = mapper.apply_mapping(source_dataset, mapping)
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
            r"^(model_)?levels?(_\w+)?$",
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
            r"^time_counter$",
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
            self._compiled_patterns[dim_type] = [re.compile(p, re.IGNORECASE) for p in patterns]

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
                    logger.debug(f"Dimension '{dim_name}' matched pattern for '{dim_type}'")
                    return dim_type

        # Strategy 2: Check standard_name attribute
        if dim_name in ds.coords:
            coord = ds.coords[dim_name]
            standard_name = coord.attrs.get("standard_name", "").lower()
            for dim_type, std_names in self.STANDARD_NAME_MAP.items():
                if standard_name in std_names:
                    logger.debug(f"Dimension '{dim_name}' matched standard_name for '{dim_type}'")
                    return dim_type

            # Strategy 3: Check axis attribute
            axis = coord.attrs.get("axis", "").upper()
            for dim_type, expected_axis in self.AXIS_MAP.items():
                if axis == expected_axis:
                    logger.debug(f"Dimension '{dim_name}' matched axis for '{dim_type}'")
                    return dim_type

            # Strategy 4: Analyze values
            dim_type = self._detect_from_values(coord)
            if dim_type:
                logger.debug(f"Dimension '{dim_name}' detected from values as '{dim_type}'")
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

            # Integer index sequence (e.g. OIFS ``model_levels`` = 1..137,
            # or any 0..N-1 model-level numbering): treat as model_level
            # before the lat/lon range checks pick it up because the small
            # integers fit inside the longitude (0..360) window. Without
            # this guard the OIFS atmospheric model-level dim ends up
            # renamed to ``longitude`` in the output, which trips cf §2.4
            # dim-order on every variable on alevel.
            if (np.issubdtype(values.dtype, np.integer) or np.all(np.equal(np.mod(values, 1), 0))) and len(values) > 1:
                diffs = np.diff(values.astype(float))
                if np.all(diffs == 1) and float(values[0]) in (0.0, 1.0):
                    return "model_level"

            # Check for latitude (-90 to 90)
            if np.all(values >= -90) and np.all(values <= 90):
                if len(values) > 10:  # Likely a grid
                    return "latitude"

            # Check for longitude (0 to 360 or -180 to 180)
            if (np.all(values >= 0) and np.all(values <= 360)) or (np.all(values >= -180) and np.all(values <= 180)):
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
        possible_names = _type_to_cmip().get(dim_type, [])

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
        .. note::
           These examples are illustrative and not verified by doctests.

        .. code-block:: python

            mapping = mapper.create_mapping(
                ds=source_ds,
                data_request_variable=cmip_var,
                user_mapping={'lev': 'plev19'}
            )
            # mapping = {'time': 'time', 'lev': 'plev19', 'latitude': 'lat', 'longitude': 'lon'}
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
                        f"User mapping specifies source dimension '{source_dim}' " f"which doesn't exist in dataset"
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
                logger.info(f"  Auto-mapped: {source_dim} → {cmip_dim} (type: {dim_type})")

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
        .. note::
           These examples are illustrative and not verified by doctests.

        .. code-block:: python

            ds_mapped = mapper.apply_mapping(ds, {'latitude': 'lat', 'longitude': 'lon'})
        """
        logger.info("Applying dimension mapping")
        # CMIP convention: the on-disk time-axis variable is ALWAYS named
        # "time", regardless of which CMOR axis ID (time / time1 / time2 /
        # time3) the data request points at. The numeric suffix only drives
        # the cell_methods string ("time: point" vs "time: mean") inside
        # CMOR; it is NOT a separate dim name. wcrp_cmip7 TIME003 and
        # ATTR001 hardcode "time", so a tpt-style file shipped with dim
        # name "time1" trips them ("Missing 'time' variable") even though
        # the data request asks for axis time1. Collapse here so the file
        # writes "time".
        mapping = {src: ("time" if tgt in ("time1", "time2", "time3") else tgt) for src, tgt in mapping.items()}
        rename_dict = {}

        for source_dim, cmip_dim in mapping.items():
            if source_dim != cmip_dim:
                rename_dict[source_dim] = cmip_dim
                logger.info(f"  Renaming: {source_dim} → {cmip_dim}")
                # Also rename the matching `{source}_bnds` aux variable if
                # present. xr.Dataset.rename({src: tgt}) only renames the
                # dim and its index coord; a separately-named bounds aux
                # (e.g. `time_counter_bnds` from LPJ-GUESS / NEMO-style
                # output) is left under its old name, which then trips
                # cf §7.1 (orphan bnds) and wcrp TIME003 (no `time` var).
                src_bnds = f"{source_dim}_bnds"
                tgt_bnds = f"{cmip_dim}_bnds"
                if src_bnds in ds.variables and tgt_bnds not in ds.variables:
                    rename_dict[src_bnds] = tgt_bnds
                    logger.info(f"  Renaming bnds: {src_bnds} → {tgt_bnds}")

        if rename_dict:
            ds = ds.rename(rename_dict)
            # Fix up the `bounds` attr pointer on the renamed coord so it
            # references the renamed bnds aux, not the old name.
            for source_dim, cmip_dim in mapping.items():
                if source_dim == cmip_dim or cmip_dim not in ds.variables:
                    continue
                coord_attrs = ds[cmip_dim].attrs
                old_bnds = f"{source_dim}_bnds"
                new_bnds = f"{cmip_dim}_bnds"
                if coord_attrs.get("bounds") == old_bnds and new_bnds in ds.variables:
                    coord_attrs["bounds"] = new_bnds
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
                errors.append(f"Missing CMIP dimensions in mapping: {sorted(missing_cmip)}")

            # Check for non-CMIP dimensions in output
            extra_dims = mapped_output - cmip_dims
            if extra_dims:
                errors.append(f"Output dimensions not in CMIP table: {sorted(extra_dims)}")
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

    def detect_all_types(self, ds: xr.Dataset) -> Dict[str, Optional[str]]:
        """
        Detect dimension types for all dimensions in dataset.

        Parameters
        ----------
        ds : xr.Dataset
            Dataset to analyze

        Returns
        -------
        Dict[str, Optional[str]]
            Mapping of {dim_name: dim_type} for all dimensions

        Examples
        --------
        .. note::
           These examples are illustrative and not verified by doctests.

        .. code-block:: python

            mapper = DimensionMapper()
            types = mapper.detect_all_types(ds)
            print(types)
            # {'time': 'time', 'lev': 'pressure', 'latitude': 'latitude', 'longitude': 'longitude'}
        """
        dim_types = {}
        for dim_name in ds.sizes.keys():
            dim_type = self.detect_dimension_type(ds, dim_name)
            dim_types[dim_name] = dim_type
        return dim_types

    def create_mapping_flexible(
        self,
        ds: xr.Dataset,
        data_request_variable: Optional[DataRequestVariable] = None,
        target_dimensions: Optional[List[str]] = None,
        user_mapping: Optional[Dict[str, str]] = None,
        allow_override: bool = True,
    ) -> Dict[str, str]:
        """
        Create dimension mapping with flexible targeting.

        This method works with or without DataRequestVariable:
        - If data_request_variable provided: use its dimensions as target
        - If target_dimensions provided: use manual dimension list
        - If neither: perform smart type-based mapping with common CMIP names

        Parameters
        ----------
        ds : xr.Dataset
            Source dataset
        data_request_variable : DataRequestVariable, optional
            CMIP variable specification with required dimensions
        target_dimensions : List[str], optional
            Manual list of target dimension names
        user_mapping : Dict[str, str], optional
            User-specified mapping {source_dim: output_dim}
        allow_override : bool
            Allow user_mapping to override computed mappings (default: True)

        Returns
        -------
        Dict[str, str]
            Mapping from source dimension names to target dimension names

        Examples
        --------
        .. note::
           These examples are illustrative and not verified by doctests.

        .. code-block:: python

            # With DataRequestVariable
            mapping = mapper.create_mapping_flexible(
                ds=ds, data_request_variable=drv
            )

            # With manual target dimensions
            mapping = mapper.create_mapping_flexible(
                ds=ds, target_dimensions=['time', 'plev19', 'lat', 'lon']
            )

            # Standalone smart mapping
            mapping = mapper.create_mapping_flexible(ds=ds)
        """
        # If DataRequestVariable provided, delegate to existing method
        if data_request_variable is not None:
            return self.create_mapping(
                ds=ds,
                data_request_variable=data_request_variable,
                user_mapping=user_mapping,
                allow_override=allow_override,
            )

        # Determine target dimensions
        if target_dimensions is not None:
            cmip_dims = target_dimensions
            logger.info("Using manual target dimensions")
        else:
            # Standalone mode: use smart defaults based on detected types
            cmip_dims = []
            logger.info("Using smart dimension mapping (no CMIP table)")

        source_dims = list(ds.sizes.keys())
        logger.info(f"  Source dimensions: {source_dims}")
        if cmip_dims:
            logger.info(f"  Target dimensions: {cmip_dims}")

        mapping = {}
        mapped_source = set()
        mapped_target = set()

        # Step 1: Apply user-specified mappings
        if user_mapping:
            for source_dim, output_dim in user_mapping.items():
                if source_dim not in source_dims:
                    logger.warning(
                        f"User mapping specifies source dimension '{source_dim}' " f"which doesn't exist in dataset"
                    )
                    continue

                mapping[source_dim] = output_dim
                mapped_source.add(source_dim)
                if output_dim in cmip_dims:
                    mapped_target.add(output_dim)
                logger.info(f"  User mapping: {source_dim} → {output_dim}")

        # Step 2: Auto-detect and map remaining dimensions
        unmapped_source = [d for d in source_dims if d not in mapped_source]
        unmapped_target = [d for d in cmip_dims if d not in mapped_target] if cmip_dims else []

        # Standard mapping for common types (used when no target specified)
        standard_type_to_cmip = {
            "latitude": "lat",
            "longitude": "lon",
            "time": "time",
            "pressure": "plev",
            "depth": "olevel",
            "height": "height",
            "model_level": "alevel",
        }

        for source_dim in unmapped_source:
            # Detect dimension type
            dim_type = self.detect_dimension_type(ds, source_dim)
            if not dim_type:
                logger.debug(f"  Could not detect type for '{source_dim}'")
                # If no type detected, keep original name
                mapping[source_dim] = source_dim
                continue

            coord_size = ds.sizes[source_dim] if source_dim in ds.sizes else None

            if unmapped_target:
                # Have target dimensions - map to them
                cmip_dim = self.map_to_cmip_dimension(dim_type, unmapped_target, coord_size)
                if cmip_dim:
                    mapping[source_dim] = cmip_dim
                    mapped_source.add(source_dim)
                    mapped_target.add(cmip_dim)
                    unmapped_target.remove(cmip_dim)
                    logger.info(f"  Auto-mapped: {source_dim} → {cmip_dim} (type: {dim_type})")
                else:
                    # No matching target, keep original
                    mapping[source_dim] = source_dim
                    logger.debug(f"  No target match for '{source_dim}', keeping original name")
            else:
                # No target dimensions - use standard CMIP names
                standard_name = standard_type_to_cmip.get(dim_type, source_dim)

                # For pressure, try to get specific level count
                if dim_type == "pressure" and coord_size:
                    # Common CMIP pressure level counts
                    if coord_size in [3, 4, 7, 8, 19, 23, 27, 39]:
                        standard_name = f"plev{coord_size}"

                mapping[source_dim] = standard_name
                mapped_source.add(source_dim)
                logger.info(f"  Smart mapping: {source_dim} → {standard_name} (type: {dim_type})")

        # Report unmapped
        final_unmapped_source = [d for d in source_dims if d not in mapped_source]
        if final_unmapped_source:
            logger.warning(f"Unmapped source dimensions: {final_unmapped_source}")

        if unmapped_target:
            logger.warning(f"Unmapped target dimensions: {unmapped_target}")

        return mapping


# Data request dimension names that are *not* output names. Resolved after the
# DReq dimension match so the file carries the real coordinate name.
#
# ``olevel``/``olevhalf`` have no entry in CMIP7_coordinate.json at all (the
# concrete options are listed individually, e.g. ``depth_coord``), so they are
# mapped by hand. ``alevel``/``alevhalf`` are deliberately absent: the
# atmospheric options are parametric coordinates and the hybrid coordinate step
# names that axis itself. Renaming them here would hand them the ocean
# depth_coord metadata that coordinate_metadata.yaml attaches to "lev"
# (standard_name depth, units m), i.e. 137 model levels labelled as ocean depth
# in metres.
_GENERIC_LEVEL_OUT_NAME = {
    "olevel": "lev",
    "olevhalf": "lev",
}


# The hand-maintained half of the dimension-type map. Kept as the base so the
# candidate order (and with it the existing disambiguation behaviour) does not
# change; names derived from the coordinate table are appended, never inserted.
_STATIC_TYPE_TO_CMIP = {
    "latitude": ["latitude", "lat", "gridlatitude"],
    "longitude": ["longitude", "lon", "gridlongitude"],
    "time": ["time", "time1", "time2", "time3"],
    "pressure": ["plev", "plev3", "plev4", "plev7", "plev8", "plev19", "plev23", "plev27", "plev39"],
    "depth": ["olevel", "olevhalf", "oline", "depth"],
    "height": ["height", "height2m", "height10m", "height100m", "alt16", "alt40"],
    "model_level": ["alevel", "alevhalf"],
}

# Z-axis standard_names, grouped into the dimension types this module uses.
_Z_STANDARD_NAMES = {
    "air_pressure": "pressure",
    "depth": "depth",
    "height": "height",
    "altitude": "height",
    "model_level_number": "model_level",
}

_TYPE_TO_CMIP_CACHE: Optional[Dict[str, List[str]]] = None


def _type_to_cmip() -> Dict[str, List[str]]:
    """Dimension type -> the data request names that denote it.

    The static table above only listed the axes we had happened to meet, which
    silently broke matching for the rest: a source ``sdepth`` never matched the
    request's ``sdepth`` because "depth" only knew olevel/olevhalf/oline/depth,
    and ``plev7h`` never matched because "pressure" only listed ``plev\\d+``.
    An unmatched dimension is not renamed at all, so it reached the file under
    its model name and no amount of out_name resolution downstream could help.
    That is why cli114 shipped ``sdepth`` and ``pressure_levels_7h``.

    CMIP7_coordinate.json already classifies every axis: ``axis`` gives X/Y/T/Z
    and, within Z, ``standard_name`` separates pressure (22 entries), depth
    (11), height/altitude (5) and the parametric coordinates. Derive from that
    and append to the static lists, so new request tiers work without another
    edit here.
    """
    global _TYPE_TO_CMIP_CACHE
    if _TYPE_TO_CMIP_CACHE is not None:
        return _TYPE_TO_CMIP_CACHE

    derived = {key: list(names) for key, names in _STATIC_TYPE_TO_CMIP.items()}
    axis_to_type = {"Y": "latitude", "X": "longitude", "T": "time"}
    for name, entry in sorted(AXIS_ENTRIES.items()):
        axis = entry.get("axis")
        dim_type = axis_to_type.get(axis)
        if dim_type is None and axis == "Z":
            standard_name = str(entry.get("standard_name") or "")
            dim_type = _Z_STANDARD_NAMES.get(standard_name)
            if dim_type is None and standard_name.startswith(("atmosphere_", "ocean_sigma")):
                dim_type = "model_level"
        if dim_type and name not in derived[dim_type]:
            derived[dim_type].append(name)
    _TYPE_TO_CMIP_CACHE = derived
    return derived


def _out_name_for(dim: str) -> str:
    """Resolve a data request dimension name to its CMOR ``out_name``.

    The data request names a dimension by the *request tier* it belongs to;
    what belongs on disk is the ``out_name`` from CMIP7_coordinate.json. 110 of
    its entries differ from their key, and writing the key through produced a
    long tail of findings that all had the same cause::

        latitude   -> lat        41 files, plus most of the AICC006
        longitude  -> lon        dimension-ordering findings
        plev19     -> plev       the count is a tier marker, not a name
        plev7h     -> plev
        sdepth     -> depth
        oplayer4   -> pdepth

    This used to be two hardcoded special cases (a dict for ``olevel`` and a
    regex for ``plev\\d+``), which fixed the two we had noticed and left the
    rest. Resolving from the table covers all of them, including the ones we
    have not hit yet.

    Falls back to the name itself when the table has no entry, so unknown or
    model-specific dimensions pass through untouched.
    """
    entry = AXIS_ENTRIES.get(dim)
    if entry:
        out_name = entry.get("out_name")
        if out_name:
            return str(out_name)
    return _GENERIC_LEVEL_OUT_NAME.get(dim, dim)


def map_dimensions(ds: Union[xr.Dataset, xr.DataArray], rule) -> Union[xr.Dataset, xr.DataArray]:
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
    .. note::
       These examples are illustrative and not verified by doctests.

    .. code-block:: python

        # In pipeline
        ds = map_dimensions(ds, rule)
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

    # Get user-specified mapping: try rule attribute first, then config
    user_mapping = getattr(rule, "dimension_mapping", None)
    if not isinstance(user_mapping, dict):
        try:
            user_mapping = rule._pycmor_cfg("dimension_mapping", default="")
            if not isinstance(user_mapping, dict):
                user_mapping = {}
        except Exception:
            user_mapping = {}

    # Get allow_override setting
    allow_override = rule._pycmor_cfg("dimension_mapping_allow_override")

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
            validation_mode = rule._pycmor_cfg("dimension_mapping_validation", default="warn")
            error_msg = "Dimension mapping validation failed:\n" + "\n".join(f"  - {e}" for e in errors)

            if validation_mode == "error":
                raise ValueError(error_msg)
            elif validation_mode == "warn":
                logger.warning(error_msg)
            # ignore mode: do nothing

        # The data request names a dimension by the request tier it belongs to;
        # what belongs on disk is the CMOR out_name. "olevel ist nur ein
        # Platzhalter" (Schupfner, Teil 2) was the first instance we hit, and
        # the same is true of latitude/longitude, every plevN, sdepth and
        # oplayer4. Resolve all of them from CMIP7_coordinate.json rather than
        # patching them one at a time; see _out_name_for.
        mapping = {src: _out_name_for(dst) for src, dst in mapping.items()}

        # Apply mapping
        ds = mapper.apply_mapping(ds, mapping)

    except Exception as e:
        logger.error(f"Error in dimension mapping: {e}")
        raise

    # Convert back to DataArray if needed
    if was_dataarray:
        return ds[da_name]
    return ds
