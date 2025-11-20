====================================
CF-Compliant Coordinate Attributes
====================================

Overview
========

Coordinate attributes are essential metadata that enable proper interpretation of NetCDF files by xarray, cf-xarray, and other CF-aware tools. The ``coordinate_attributes`` module automatically sets CF-compliant metadata on coordinate variables to ensure your CMIP6/CMIP7 outputs are correctly recognized and processed.

Why Coordinate Attributes Matter
=================================

Without proper coordinate attributes, tools like xarray may not correctly identify:

- Which variables are coordinates vs. data variables
- Spatial dimensions (X, Y, Z axes)
- Temporal dimensions (T axis)
- Physical units and standard names
- Vertical coordinate direction (positive up/down)

This can lead to:

- Incorrect plotting and visualization
- Failed regridding operations
- Misinterpretation of vertical coordinates
- Non-compliance with CF conventions and CMIP standards

Automatic Attribute Setting
============================

.. note::
   **Automatic in Default Pipeline**: As of the latest version, coordinate attribute setting is automatically included in the ``DefaultPipeline``. If you're using the default pipeline, CF-compliant coordinate attributes will be added automatically—no additional configuration needed!

The coordinate attributes feature is integrated into the default processing pipeline and runs after variable attributes are set. It automatically:

- Sets ``standard_name`` for recognized coordinates
- Sets ``axis`` attribute (X, Y, Z, or T)
- Sets ``units`` for physical quantities
- Sets ``positive`` attribute for vertical coordinates
- Sets ``coordinates`` attribute on data variables
- Validates existing metadata (configurable)

Supported Coordinates
=====================

The system recognizes and handles metadata for:

Horizontal Coordinates
----------------------

- **Longitude**: ``longitude``, ``lon``, ``gridlongitude``
- **Latitude**: ``latitude``, ``lat``, ``gridlatitude``

Vertical Coordinates - Pressure Levels
---------------------------------------

- **Standard pressure levels**: ``plev``, ``plev3``, ``plev4``, ``plev7``, ``plev8``, ``plev19``, ``plev23``, ``plev27``, ``plev39``
- **Special pressure levels**: ``plev3u``, ``plev7c``, ``plev7h``

Vertical Coordinates - Ocean Levels
------------------------------------

- **Ocean depth**: ``olevel``, ``olevhalf``, ``oline``
- **Density**: ``rho``

Vertical Coordinates - Atmosphere Model Levels
-----------------------------------------------

- **Model levels**: ``alevel``, ``alevhalf``

Vertical Coordinates - Altitude
--------------------------------

- **Altitude**: ``alt16``, ``alt40``
- **Height**: ``height``, ``height2m``, ``height10m``, ``height100m``
- **Depth**: ``depth0m``, ``depth100m``, ``depth300m``, ``depth700m``, ``depth2000m``
- **Soil depth**: ``sdepth``, ``sdepth1``, ``sdepth10``

Scalar Coordinates
------------------

- **Pressure points**: ``p10``, ``p100``, ``p220``, ``p500``, ``p560``, ``p700``, ``p840``, ``p850``, ``p1000``

Other Coordinates
-----------------

- **Site**: ``site``
- **Basin**: ``basin``

Usage in Default Pipeline
==========================

The coordinate attributes step is automatically included in the ``DefaultPipeline``:

.. code-block:: python

   from pycmor.core.pipeline import DefaultPipeline

   # The default pipeline includes coordinate attributes automatically
   pipeline = DefaultPipeline()
   
   # Process your data - coordinate attributes added automatically
   result = pipeline.run(data, rule_spec)

Usage in Custom Pipelines
==========================

You can explicitly add ``set_coordinate_attributes`` to custom pipelines:

.. code-block:: python

   from pycmor.std_lib import set_coordinate_attributes

   # In your pipeline configuration
   pipeline = [
       "load_data",
       "get_variable",
       "set_variable_attributes",
       "set_coordinate_attributes",  # Add this step
       "convert_units",
       # ... other steps
   ]

Standalone Usage
================

You can also use it directly on datasets:

.. code-block:: python

   from pycmor.std_lib.coordinate_attributes import set_coordinate_attributes
   import xarray as xr
   import numpy as np

   # Dataset with coordinates
   ds = xr.Dataset({
       'tas': (['time', 'lat', 'lon'], np.random.rand(10, 90, 180)),
   }, coords={
       'time': np.arange(10),
       'lat': np.linspace(-89.5, 89.5, 90),
       'lon': np.linspace(0.5, 359.5, 180),
   })

   # Apply coordinate attributes
   ds = set_coordinate_attributes(ds, rule)

   # Now coordinates have CF-compliant metadata
   print(ds['lat'].attrs)
   # {'standard_name': 'latitude', 'units': 'degrees_north', 'axis': 'Y'}

Configuration Options
=====================

The coordinate attributes module provides several configuration options:

Enable/Disable Coordinate Attributes
-------------------------------------

.. code-block:: yaml

   # In .pycmor.yaml or rule configuration
   xarray_set_coordinate_attributes: yes  # Default: yes

Set to ``no`` to disable automatic coordinate attribute setting.

Enable/Disable 'coordinates' Attribute
---------------------------------------

.. code-block:: yaml

   xarray_set_coordinates_attribute: yes  # Default: yes

Controls whether the ``coordinates`` attribute is set on data variables to list their associated coordinates.

Metadata Validation
===================

The system can validate existing coordinate metadata in source data and handle conflicts according to your preference.

Validation Modes
----------------

.. code-block:: yaml

   xarray_validate_coordinate_attributes: warn  # Default: warn

Available modes:

**ignore** (Silent)
  Keep existing values without warnings. Use when you trust source data completely.

  .. code-block:: python

     # Source data has wrong metadata
     ds['lat'].attrs = {'standard_name': 'wrong_name', 'units': 'meters'}
     
     # After processing (ignore mode)
     # - Keeps 'wrong_name' and 'meters' (no warnings)
     # - Adds missing 'axis': 'Y'

**warn** (Default)
  Log warnings for conflicts but keep existing values. Recommended for development and monitoring.

  .. code-block:: python

     # Source data has wrong metadata
     ds['lat'].attrs = {'standard_name': 'wrong_name'}
     
     # After processing (warn mode)
     # WARNING: Coordinate 'lat' has standard_name='wrong_name' 
     #          but expected 'latitude' (keeping existing value)
     # - Keeps 'wrong_name'
     # - Adds 'units': 'degrees_north' and 'axis': 'Y'

**error** (Strict)
  Raise ValueError on conflicts. Use for strict validation in CI/CD pipelines.

  .. code-block:: python

     # Source data has wrong metadata
     ds['lat'].attrs = {'standard_name': 'wrong_name'}
     
     # After processing (error mode)
     # ValueError: Invalid standard_name for coordinate 'lat': 
     #   got 'wrong_name', expected 'latitude'

**fix** (Auto-correct)
  Automatically overwrite wrong values with correct ones. Use to fix known issues.

  .. code-block:: python

     # Source data has wrong metadata
     ds['lat'].attrs = {'standard_name': 'wrong_name', 'units': 'meters'}
     
     # After processing (fix mode)
     # INFO: standard_name corrected: 'wrong_name' → 'latitude'
     # INFO: units corrected: 'meters' → 'degrees_north'
     # - Corrects to 'latitude' and 'degrees_north'
     # - Adds 'axis': 'Y'

Validation Examples
===================

Example 1: Development Mode (Default)
--------------------------------------

.. code-block:: yaml

   # Monitor data quality without breaking pipeline
   xarray_validate_coordinate_attributes: warn

This mode:

- Identifies data quality issues
- Doesn't break existing workflows
- Logs actionable warnings
- Safe for production

Example 2: Production with Trusted Data
----------------------------------------

.. code-block:: yaml

   # Trust source data, no validation overhead
   xarray_validate_coordinate_attributes: ignore

This mode:

- No validation overhead
- Preserves all source metadata
- Suitable for validated datasets

Example 3: Strict Validation
-----------------------------

.. code-block:: yaml

   # Fail fast on bad data
   xarray_validate_coordinate_attributes: error

This mode:

- Ensures data quality
- Catches issues early
- Prevents bad data from propagating
- Good for CI/CD pipelines

Example 4: Auto-correction
---------------------------

.. code-block:: yaml

   # Automatically fix known issues
   xarray_validate_coordinate_attributes: fix

This mode:

- Corrects common metadata errors
- Ensures CF compliance
- Reduces manual intervention
- Logs all corrections

Metadata Definitions
====================

All coordinate metadata is defined in an external YAML file (``src/pycmor/data/coordinate_metadata.yaml``), making it easy to:

- Add new coordinate definitions
- Modify existing metadata
- Maintain coordinate standards
- Version control changes

Adding Custom Coordinates
--------------------------

To add a new coordinate, simply edit the YAML file:

.. code-block:: yaml

   # In src/pycmor/data/coordinate_metadata.yaml
   my_custom_level:
     standard_name: altitude
     units: m
     positive: up
     axis: Z
     long_name: custom altitude level

No Python code changes needed!

Example Output
==============

Before Coordinate Attributes
-----------------------------

.. code-block:: python

   # Original dataset
   ds = xr.Dataset({
       'ta': (['time', 'plev19', 'lat', 'lon'], data),
   }, coords={
       'plev19': [100000, 92500, ..., 1000],  # Pa
       'lat': [-89.5, -88.5, ..., 89.5],
       'lon': [0.5, 1.5, ..., 359.5],
   })
   
   print(ds['plev19'].attrs)
   # {}  # Empty!
   
   print(ds['lat'].attrs)
   # {}  # Empty!

After Coordinate Attributes
----------------------------

.. code-block:: python

   # After applying coordinate attributes
   ds = set_coordinate_attributes(ds, rule)
   
   print(ds['plev19'].attrs)
   # {
   #     'standard_name': 'air_pressure',
   #     'units': 'Pa',
   #     'axis': 'Z',
   #     'positive': 'down'
   # }
   
   print(ds['lat'].attrs)
   # {
   #     'standard_name': 'latitude',
   #     'units': 'degrees_north',
   #     'axis': 'Y'
   # }
   
   print(ds['lon'].attrs)
   # {
   #     'standard_name': 'longitude',
   #     'units': 'degrees_east',
   #     'axis': 'X'
   # }
   
   print(ds['ta'].attrs['coordinates'])
   # 'plev19 lat lon'

CMIP Compliance
===============

The coordinate attributes module ensures compliance with:

- **CF Conventions**: All attributes follow CF standard names and conventions
- **CMIP6 Standards**: Compatible with CMIP6 coordinate specifications
- **CMIP7 Standards**: Compatible with CMIP7 coordinate specifications
- **xarray Requirements**: Ensures proper coordinate recognition by xarray

Benefits for xarray
-------------------

With proper coordinate attributes, xarray can:

- Automatically identify coordinate variables
- Enable ``.sel()`` and ``.isel()`` operations
- Support cf-xarray accessors
- Enable proper plotting with correct axis labels
- Support coordinate-based operations

Technical Details
=================

Attribute Priority
------------------

The system follows this priority:

1. **Existing correct metadata**: Preserved without changes
2. **Missing metadata**: Added from definitions
3. **Conflicting metadata**: Handled according to validation mode

Time Coordinates
----------------

Time coordinates are handled separately in ``files.py`` during the save operation, not by this module.

Bounds Variables
----------------

Bounds variables (e.g., ``lat_bnds``, ``plev_bnds``) are automatically skipped and not processed.

Case Sensitivity
----------------

Coordinate name matching is case-insensitive, so ``LAT``, ``Lat``, and ``lat`` all match the ``latitude`` definition.

Performance
-----------

- Metadata is loaded once at module import time
- Minimal overhead per coordinate (< 1ms)
- No additional I/O operations
- Efficient for large datasets

Logging
=======

The module provides detailed logging at different levels:

INFO Level
----------

.. code-block:: text

   [Coordinate Attributes] Setting CF-compliant metadata
     → Setting attributes for 'lat':
         • standard_name = latitude
         • units = degrees_north
         • axis = Y
     → Setting attributes for 'lon':
         • standard_name = longitude
         • units = degrees_east
         • axis = X
     → Processed 3 coordinates, skipped 1

DEBUG Level
-----------

.. code-block:: text

   → Skipping 'time' (handled elsewhere or bounds variable)
   → No metadata defined for 'custom_coord'
     • standard_name already correct (latitude)

WARNING Level
-------------

.. code-block:: text

   Coordinate 'lat' has standard_name='wrong_name' but expected 'latitude'
   Coordinate 'plev19' has units='hPa' but expected 'Pa'

Troubleshooting
===============

Coordinates Not Recognized
---------------------------

If a coordinate is not getting attributes:

1. Check if it's in the YAML definitions
2. Check for typos in coordinate names
3. Verify it's not a bounds variable (e.g., ``lat_bnds``)
4. Check if it's being skipped (time coordinates)

Validation Warnings
-------------------

If you see validation warnings:

1. Review source data metadata
2. Decide if warnings are valid concerns
3. Choose appropriate validation mode:
   
   - ``ignore``: Trust source data
   - ``warn``: Monitor issues (default)
   - ``error``: Enforce strict compliance
   - ``fix``: Auto-correct issues

Attributes Not Applied
-----------------------

If attributes aren't being set:

1. Check configuration: ``xarray_set_coordinate_attributes: yes``
2. Verify coordinate names match definitions
3. Check logs for skipped coordinates
4. Ensure you're using the correct pipeline

See Also
========

- `CF Conventions - Coordinate Types <http://cfconventions.org/Data/cf-conventions/cf-conventions-1.10/cf-conventions.html#coordinate-types>`_
- `CF Standard Names <https://cfconventions.org/standard-names.html>`_
- `CMIP6 Coordinate Tables <https://github.com/PCMDI/cmip6-cmor-tables>`_
- :doc:`coordinate_bounds` - Coordinate bounds calculation
- :doc:`pycmor_configuration` - Configuration options
- :mod:`pycmor.std_lib.coordinate_attributes` - Module API documentation
