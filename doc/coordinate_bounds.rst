============================
Coordinate Bounds Calculation
============================

Overview
========

Coordinate bounds (e.g., ``lat_bnds``, ``lon_bnds``) are required for CMIP compliance. They define the edges of grid cells and are essential for proper data interpretation, especially for:

- Calculating accurate spatial averages
- Determining grid cell areas
- Ensuring proper regridding and interpolation
- Meeting CF conventions and CMIP standards

Automatic Bounds Calculation
=============================

As of the latest version, pymorize automatically calculates coordinate bounds if they are not present in your grid file. This feature is integrated into the ``setgrid`` function and works seamlessly with your existing workflows.

How It Works
------------

When you use the ``setgrid`` function with a grid file that doesn't contain bounds variables:

1. The function detects missing ``lat_bnds`` and ``lon_bnds``
2. Automatically calculates bounds from coordinate values
3. Adds the bounds to your dataset
4. Sets the appropriate ``bounds`` attribute on coordinates

Algorithm
---------

The bounds calculation uses a robust algorithm that:

- **For interior cells**: Uses midpoints between adjacent coordinate values
- **For edge cells**: Extrapolates using the same spacing as adjacent cells
- **Ensures continuity**: No gaps between cells (upper bound of cell i equals lower bound of cell i+1)
- **Works for both**: Regular and irregular grids

Example Usage
-------------

.. code-block:: python

   import xarray as xr
   from pycmor.std_lib.setgrid import setgrid

   # Your data
   data = xr.DataArray(
       np.random.rand(10, 100),
       dims=["time", "ncells"],
       coords={"time": np.arange(10)},
       name="temperature"
   )

   # Rule with grid file (even if it doesn't have bounds)
   rule = {"grid_file": "path/to/grid.nc"}

   # Apply setgrid - bounds will be calculated automatically if missing
   result = setgrid(data, rule)

   # Result now has lat_bnds and lon_bnds
   print("lat_bnds" in result.data_vars)  # True
   print("lon_bnds" in result.data_vars)  # True

Manual Bounds Calculation
--------------------------

You can also calculate bounds manually using the ``bounds`` module:

.. code-block:: python

   from pycmor.std_lib.bounds import add_bounds_to_grid, calculate_bounds_1d
   import xarray as xr
   import numpy as np

   # For a simple 1D coordinate
   lat = xr.DataArray(
       np.linspace(-90, 90, 180),
       dims=["lat"],
       attrs={"long_name": "latitude", "units": "degrees_north"}
   )

   lat_bnds = calculate_bounds_1d(lat)
   print(lat_bnds.shape)  # (180, 2)

   # For a complete grid dataset
   grid = xr.open_dataset("grid.nc")
   grid_with_bounds = add_bounds_to_grid(grid)

CMIP Compliance
===============

According to CMIP6/CMIP7 coordinate specifications:

- Both ``latitude`` and ``longitude`` have ``"must_have_bounds": "yes"``
- Bounds are required for proper data interpretation
- The bounds should follow CF conventions

With automatic bounds calculation, pymorize ensures your data meets these requirements even if your original grid files don't include pre-computed bounds.

Technical Details
=================

Bounds Format
-------------

Bounds are stored as 2D arrays with shape ``(n, 2)`` where:

- ``n`` is the number of coordinate points
- First column (``[:, 0]``) contains lower bounds
- Second column (``[:, 1]``) contains upper bounds

Coordinate Attributes
---------------------

When bounds are added, the coordinate variable gets a ``bounds`` attribute:

.. code-block:: python

   lat.attrs["bounds"] = "lat_bnds"

This follows CF conventions and allows tools to automatically discover the bounds.

Supported Grids
---------------

- **1D regular grids**: Evenly spaced coordinates (e.g., ``lat = [-90, -89, ..., 90]``)
- **1D irregular grids**: Unevenly spaced coordinates (e.g., ``lat = [10, 15, 25, 45, 50]``)
- **2D unstructured grids**: Limited support (pre-computed bounds recommended)

Limitations
===========

For 2D unstructured grids (e.g., FESOM meshes), automatic bounds calculation is simplified and may not be accurate. For these cases, it's recommended to:

1. Pre-compute bounds using your model's grid generation tools
2. Include bounds in your grid files
3. The existing bounds will be preserved and used

See Also
========

- `CF Conventions - Cell Boundaries <http://cfconventions.org/Data/cf-conventions/cf-conventions-1.10/cf-conventions.html#cell-boundaries>`_
- `CMIP6 Coordinate Tables <https://github.com/PCMDI/cmip6-cmor-tables>`_
- :mod:`pycmor.std_lib.bounds` module documentation
- :mod:`pycmor.std_lib.setgrid` module documentation
