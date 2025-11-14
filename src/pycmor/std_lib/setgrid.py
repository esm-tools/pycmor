"""
Set grid information on the data file.

xarray does not have a built-in `setgrid` operator unlike `cdo`.  Using
`xarray.merge` directly to merge grid with data may or may not produce the
desired result all the time.

Some guiding rules to set the grid information:

1. At least one dimension size in both data file and grid file should match.
2. If the dimension size match but not the dimension name, then the dimension
   name in data file is renamed to match the dimension name in grid file.
3. The matching dimension size must be one of the coordinate variables in both data
   file and grid file.
4. If all above conditions are met, then the data file is merged with the grid file.
5. The coordinate variables and boundary variables (lat_bnds, lon_bnds) from the grid file
   are kept, while other data variables in grid file are dropped.
6. The result of the merge is always a xarray.Dataset
7. If coordinate bounds (lat_bnds, lon_bnds) are not present in the grid file,
   they will be automatically calculated from the coordinate values.

Note: Rule 5 is not strict and may go away if it is not desired.

Automatic Bounds Calculation
-----------------------------
As of the latest version, this module automatically calculates coordinate bounds
(lat_bnds, lon_bnds) if they are not present in the grid file. This ensures
CMIP compliance, as coordinate bounds are required for proper data interpretation.

The bounds calculation:
- Uses midpoints between adjacent coordinate values for interior cells
- Extrapolates for edge cells using the same spacing
- Ensures continuity (no gaps between cells)
- Works for both regular and irregular grids
"""

from typing import Union

import xarray as xr

from ..core.logging import logger
from ..core.rule import Rule
from .bounds import add_bounds_to_grid


def setgrid(da: Union[xr.Dataset, xr.DataArray], rule: Rule) -> Union[xr.Dataset, xr.DataArray]:
    """
    Appends grid information to data file if necessary coordinate dimensions exits in data file.
    Renames dimensions in data file to match the dimension names in grid file if necessary.

    Parameters
    ----------
    da : xr.Dataset or xr.DataArray
        The input dataarray or dataset.
    rule: Rule object containing gridfile attribute

    Returns
    -------
    xr.Dataset
        The output dataarray or dataset with the grid information.
    """
    logger.info("[SetGrid] Starting grid merge operation")
    gridfile = rule.get("grid_file")
    logger.info(f"  → Grid File          : {gridfile}")
    if gridfile is None:
        raise ValueError("Missing grid file. Please set 'grid_file' in the rule.")
    grid = xr.open_dataset(gridfile)

    # Add bounds if they don't exist
    grid = add_bounds_to_grid(grid)

    required_dims = set(sum([gc.dims for _, gc in grid.coords.items()], ()))
    logger.info(f"  → Required Dimensions: {sorted(required_dims)}")
    to_rename = {}
    can_merge = False
    for dim in required_dims:
        dimsize = grid.sizes[dim]
        if dim in da.sizes:
            can_merge = True
            if da.sizes[dim] != dimsize:
                raise ValueError(f"Mismatch dimension sizes {dim} {dimsize} (grid) {da.sizes[dim]} (data)")
            logger.info(f"  → Dimension '{dim}' : ✅ Found (size={dimsize})")
        else:
            logger.info(f"  → Dimension '{dim}' : ❌ Not found, checking for size matches...")
            for name, _size in da.sizes.items():
                if dimsize == _size:
                    can_merge = True
                    to_rename[name] = dim
                    logger.info(f"    • Found size match  : '{name}' ({_size}) → '{dim}' ({dimsize})")
    logger.info(f"  → Merge Status       : {'✅ Possible' if can_merge else '❌ Not possible'}")

    if can_merge:
        if to_rename:
            logger.info(f"  → Renaming Dims      : {dict(to_rename)}")
            da = da.rename(to_rename)

        # Keep coordinate variables and boundary variables (lat_bnds, lon_bnds)
        required_vars = list(grid.coords.keys())  # Always include coordinate variables
        logger.info(f"  → Coordinate Vars    : {sorted(required_vars)}")

        # Add boundary variables if they exist
        boundary_vars = ["lat_bnds", "lon_bnds"]
        boundary_found = []
        for var in boundary_vars:
            if var in grid.variables:
                required_vars.append(var)
                boundary_found.append(var)

        if boundary_found:
            logger.info(f"  → Boundary Vars      : {sorted(boundary_found)}")
        else:
            logger.info("  → Boundary Vars      : None found")

        new_grid = grid[required_vars]
        da = new_grid.merge(da)
        logger.info("  → Grid Merge         : ✅ Completed")
    else:
        logger.warning("  → Warning            : ❌ No compatible dimensions found!")
        logger.warning("    Check grid and data dimension compatibility.")

    logger.info("-" * 50)
    return da
