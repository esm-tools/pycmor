====================================
Dimension Mapping to CMIP Standards
====================================

Overview
========

Dimension mapping automatically translates dimension names from source data to CMIP table requirements. This is essential because model output often uses different dimension names than what CMIP tables specify (e.g., ``latitude`` vs ``lat``, ``lev`` vs ``plev19``).

Why Dimension Mapping Matters
==============================

CMIP tables specify exact dimension names that must appear in the output. For example:

- CMIP table requires: ``time plev19 lat lon``
- Your model output has: ``time lev latitude longitude``

Without dimension mapping:

- Manual renaming is tedious and error-prone
- Dimension names don't match CMIP requirements
- Coordinate attributes may be set on wrong dimension names
- Output files fail CMIP validation

With dimension mapping:

- Automatic detection of dimension types
- Intelligent mapping to CMIP dimension names
- Seamless integration with coordinate attributes
- CMIP-compliant output

Automatic in Default Pipeline
==============================

.. note::
   **Automatic in Default Pipeline**: Dimension mapping is automatically included in the ``DefaultPipeline``. If you're using the default pipeline, dimensions will be mapped automatically—no additional configuration needed!

The dimension mapping feature is integrated into the default processing pipeline and runs before coordinate attributes are set. It automatically:

- Detects what each dimension represents (latitude, longitude, pressure, etc.)
- Maps source dimension names to CMIP dimension names
- Renames dimensions to match CMIP requirements
- Validates the mapping (configurable)

How It Works
============

The dimension mapper uses **four detection strategies** to identify what each dimension represents:

1. **Name Pattern Matching**
   Recognizes common dimension name patterns using regular expressions.

2. **Standard Name Attribute**
   Checks the CF ``standard_name`` attribute on coordinates.

3. **Axis Attribute**
   Checks the CF ``axis`` attribute (X, Y, Z, T).

4. **Value Range Analysis**
   Analyzes coordinate values to detect latitude, longitude, or pressure.

Detection Strategies
====================

Strategy 1: Name Pattern Matching
----------------------------------

The mapper recognizes common dimension name patterns:

**Latitude patterns:**

- ``latitude``, ``lat``, ``y``, ``ylat``
- ``rlat``, ``nav_lat``, ``gridlatitude``

**Longitude patterns:**

- ``longitude``, ``lon``, ``x``, ``xlon``
- ``rlon``, ``nav_lon``, ``gridlongitude``

**Pressure patterns:**

- ``lev``, ``level``, ``levels``, ``plev``
- ``plev19``, ``plev8``, ``pressure``, ``pres``

**Depth patterns:**

- ``depth``, ``olevel``, ``olevhalf``
- ``z``, ``oline``

**Time patterns:**

- ``time``, ``time1``, ``time2``, ``t``

Strategy 2: Standard Name Attribute
------------------------------------

If a coordinate has a CF ``standard_name`` attribute, the mapper uses it:

.. code-block:: python

   # Coordinate with standard_name
   ds.coords['y'].attrs = {'standard_name': 'latitude'}
   
   # Mapper detects: y → latitude type

Strategy 3: Axis Attribute
---------------------------

If a coordinate has a CF ``axis`` attribute, the mapper uses it:

.. code-block:: python

   # Coordinate with axis attribute
   ds.coords['y'].attrs = {'axis': 'Y'}
   
   # Mapper detects: y → latitude type (Y axis)

Strategy 4: Value Range Analysis
---------------------------------

The mapper can detect dimension types from coordinate values:

.. code-block:: python

   # Latitude detection (values in -90 to 90 range)
   ds.coords['y'] = np.linspace(-89.5, 89.5, 180)
   # Mapper detects: y → latitude type
   
   # Longitude detection (values in 0 to 360 range)
   ds.coords['x'] = np.linspace(0.5, 359.5, 360)
   # Mapper detects: x → longitude type
   
   # Pressure detection (values in Pa or hPa range)
   ds.coords['level'] = [100000, 92500, 85000, 70000]
   # Mapper detects: level → pressure type

CMIP Dimension Mapping
=======================

Once a dimension type is detected, the mapper finds the matching CMIP dimension name:

Horizontal Coordinates
----------------------

- ``latitude`` type → ``lat`` or ``latitude``
- ``longitude`` type → ``lon`` or ``longitude``

Vertical Coordinates - Pressure
--------------------------------

- ``pressure`` type → ``plev``, ``plev3``, ``plev4``, ``plev7``, ``plev8``, ``plev19``, ``plev23``, ``plev27``, ``plev39``

The mapper uses coordinate size to select the correct ``plevN`` dimension:

.. code-block:: python

   # Source has 19 pressure levels
   ds.coords['lev'] = np.arange(19)
   
   # CMIP table requires plev19
   # Mapper selects: lev → plev19 (size matches)

Vertical Coordinates - Ocean
-----------------------------

- ``depth`` type → ``olevel``, ``olevhalf``, ``oline``

Vertical Coordinates - Altitude/Height
---------------------------------------

- ``height`` type → ``height``, ``height2m``, ``height10m``, ``height100m``
- ``height`` type → ``alt16``, ``alt40``

Vertical Coordinates - Model Levels
------------------------------------

- ``model_level`` type → ``alevel``, ``alevhalf``

Time Coordinates
----------------

- ``time`` type → ``time``, ``time1``, ``time2``, ``time3``

Usage in Default Pipeline
==========================

The dimension mapping step is automatically included in the ``DefaultPipeline``:

.. code-block:: python

   from pycmor.core.pipeline import DefaultPipeline

   # The default pipeline includes dimension mapping automatically
   pipeline = DefaultPipeline()
   
   # Process your data - dimensions mapped automatically
   result = pipeline.run(data, rule_spec)

Pipeline Order
--------------

Dimension mapping runs **before** coordinate attributes:

1. Load data
2. Get variable
3. Add vertical bounds
4. Time averaging
5. Unit conversion
6. Set global attributes
7. Set variable attributes
8. **Map dimensions** ← Renames dimensions to CMIP names
9. **Set coordinate attributes** ← Sets metadata on renamed dimensions
10. Checkpoint
11. Trigger compute
12. Show data
13. Save dataset

This order ensures coordinates have the correct CMIP names before metadata is set.

Usage in Custom Pipelines
==========================

You can explicitly add ``map_dimensions`` to custom pipelines:

.. code-block:: python

   from pycmor.std_lib import map_dimensions

   # In your pipeline configuration
   pipeline = [
       "load_data",
       "get_variable",
       "map_dimensions",              # Add dimension mapping
       "set_coordinate_attributes",   # Then set metadata
       "convert_units",
       # ... other steps
   ]

Standalone Usage
================

You can also use dimension mapping directly:

.. code-block:: python

   from pycmor.std_lib.dimension_mapping import DimensionMapper
   import xarray as xr
   import numpy as np

   # Create dataset with non-CMIP dimension names
   ds = xr.Dataset({
       'temp': (['time', 'lev', 'latitude', 'longitude'], data),
   }, coords={
       'time': np.arange(10),
       'lev': np.arange(19),
       'latitude': np.linspace(-90, 90, 180),
       'longitude': np.linspace(0, 360, 360),
   })

   # Create mapper and mapping
   mapper = DimensionMapper()
   mapping = mapper.create_mapping(ds, data_request_variable)
   
   # Apply mapping
   ds_mapped = mapper.apply_mapping(ds, mapping)
   
   # Now dimensions have CMIP names
   print(ds_mapped.dims)
   # Frozen({'time': 10, 'plev19': 19, 'lat': 180, 'lon': 360})

Configuration Options
=====================

Enable/Disable Dimension Mapping
---------------------------------

.. code-block:: yaml

   # In .pycmor.yaml or rule configuration
   xarray_enable_dimension_mapping: yes  # Default: yes

Set to ``no`` to disable automatic dimension mapping.

Validation Mode
---------------

.. code-block:: yaml

   dimension_mapping_validation: warn  # Default: warn

Available modes:

**ignore** (Silent)
  No validation, silent operation. Use when you trust the mapping completely.

  .. code-block:: python

     # Mapping may be incomplete, but no warnings
     # Use with caution

**warn** (Default)
  Log warnings for mapping issues but continue. Recommended for development.

  .. code-block:: python

     # Logs warnings for unmapped dimensions
     # WARNING: Unmapped CMIP dimensions: ['plev19']
     # WARNING: Unmapped source dimensions: ['unknown_dim']

**error** (Strict)
  Raise ValueError on mapping validation failures. Use for strict validation.

  .. code-block:: python

     # Raises exception if mapping is incomplete
     # ValueError: Dimension mapping validation failed:
     #   - Missing CMIP dimensions in mapping: ['plev19']

User-Specified Mapping
-----------------------

.. code-block:: yaml

   dimension_mapping:
     lev: plev19
     latitude: lat
     longitude: lon

User-specified mappings override automatic detection.

Allow Override Mode
-------------------

.. code-block:: yaml

   dimension_mapping_allow_override: yes  # Default: yes

Controls whether users can override CMIP table dimension names in output.

**yes** (Flexible Mode - Default)
  Allows output dimension names to differ from CMIP table requirements.
  Useful for custom output formats, legacy compatibility, or experimental variables.

  .. code-block:: yaml

     dimension_mapping_allow_override: yes
     dimension_mapping:
       lev: my_custom_level    # Override: plev19 → my_custom_level
       latitude: my_lat        # Override: lat → my_lat
       longitude: my_lon       # Override: lon → my_lon

**no** (Strict Mode)
  Enforces that output dimension names match CMIP table requirements exactly.
  Use when preparing data for CMIP submission.

  .. code-block:: yaml

     dimension_mapping_allow_override: no
     # Output dimensions must match CMIP table
     # Custom dimension names will cause validation errors

Examples
========

Example 1: Simple Latitude/Longitude Mapping
---------------------------------------------

**Source Data:**

.. code-block:: python

   ds = xr.Dataset({
       'tas': (['time', 'latitude', 'longitude'], data),
   }, coords={
       'time': np.arange(10),
       'latitude': np.linspace(-90, 90, 180),
       'longitude': np.linspace(0, 360, 360),
   })

**CMIP Table Requires:**

.. code-block:: text

   dimensions = "time lat lon"

**After Mapping:**

.. code-block:: python

   ds_mapped = map_dimensions(ds, rule)
   
   print(ds_mapped.dims)
   # Frozen({'time': 10, 'lat': 180, 'lon': 360})
   
   print(list(ds_mapped['tas'].dims))
   # ['time', 'lat', 'lon']

Example 2: Pressure Level Mapping
----------------------------------

**Source Data:**

.. code-block:: python

   ds = xr.Dataset({
       'ta': (['time', 'lev', 'lat', 'lon'], data),
   }, coords={
       'time': np.arange(10),
       'lev': np.arange(19),  # 19 pressure levels
       'lat': np.linspace(-90, 90, 180),
       'lon': np.linspace(0, 360, 360),
   })

**CMIP Table Requires:**

.. code-block:: text

   dimensions = "time plev19 lat lon"

**After Mapping:**

.. code-block:: python

   ds_mapped = map_dimensions(ds, rule)
   
   print(ds_mapped.dims)
   # Frozen({'time': 10, 'plev19': 19, 'lat': 180, 'lon': 360})
   
   # 'lev' was automatically mapped to 'plev19' based on size

Example 3: Ocean Data Mapping
------------------------------

**Source Data:**

.. code-block:: python

   ds = xr.Dataset({
       'thetao': (['time', 'depth', 'lat', 'lon'], data),
   }, coords={
       'time': np.arange(10),
       'depth': np.array([5, 15, 25, 50, 100, 200]),
       'lat': np.linspace(-90, 90, 180),
       'lon': np.linspace(0, 360, 360),
   })

**CMIP Table Requires:**

.. code-block:: text

   dimensions = "time olevel lat lon"

**After Mapping:**

.. code-block:: python

   ds_mapped = map_dimensions(ds, rule)
   
   print(ds_mapped.dims)
   # Frozen({'time': 10, 'olevel': 6, 'lat': 180, 'lon': 360})
   
   # 'depth' was automatically mapped to 'olevel'

Example 4: User-Specified Mapping
----------------------------------

**Configuration:**

.. code-block:: yaml

   dimension_mapping:
     level: plev19
     y: lat
     x: lon

**Source Data:**

.. code-block:: python

   ds = xr.Dataset({
       'ta': (['time', 'level', 'y', 'x'], data),
   })

**After Mapping:**

.. code-block:: python

   ds_mapped = map_dimensions(ds, rule)
   
   print(ds_mapped.dims)
   # Frozen({'time': 10, 'plev19': 19, 'lat': 180, 'lon': 360})
   
   # User mappings were applied:
   # level → plev19
   # y → lat
   # x → lon

Example 5: Detection by Attributes
-----------------------------------

**Source Data with Attributes:**

.. code-block:: python

   ds = xr.Dataset({
       'tas': (['time', 'y', 'x'], data),
   }, coords={
       'time': np.arange(10),
       'y': (['y'], np.linspace(-90, 90, 180), {
           'standard_name': 'latitude',
           'axis': 'Y'
       }),
       'x': (['x'], np.linspace(0, 360, 360), {
           'standard_name': 'longitude',
           'axis': 'X'
       }),
   })

**After Mapping:**

.. code-block:: python

   ds_mapped = map_dimensions(ds, rule)
   
   print(ds_mapped.dims)
   # Frozen({'time': 10, 'lat': 180, 'lon': 360})
   
   # Detected from standard_name and axis attributes:
   # y → lat
   # x → lon

Example 6: Overriding CMIP Dimension Names
-------------------------------------------

**Scenario**: CMIP table requires ``time plev19 lat lon``, but you want custom names

**Configuration:**

.. code-block:: yaml

   rules:
     - model_variable: temp
       cmor_variable: ta
       dimension_mapping_allow_override: yes
       dimension_mapping:
         lev: pressure_level      # Override: plev19 → pressure_level
         latitude: grid_lat       # Override: lat → grid_lat
         longitude: grid_lon      # Override: lon → grid_lon

**Source Data:**

.. code-block:: python

   ds = xr.Dataset({
       'temp': (['time', 'lev', 'latitude', 'longitude'], data),
   }, coords={
       'time': np.arange(10),
       'lev': np.arange(19),
       'latitude': np.linspace(-90, 90, 180),
       'longitude': np.linspace(0, 360, 360),
   })

**After Mapping:**

.. code-block:: python

   ds_mapped = map_dimensions(ds, rule)
   
   print(ds_mapped.dims)
   # Frozen({'time': 10, 'pressure_level': 19, 'grid_lat': 180, 'grid_lon': 360})
   
   # Custom dimension names instead of CMIP names:
   # lev → pressure_level (not plev19)
   # latitude → grid_lat (not lat)
   # longitude → grid_lon (not lon)

**Use Cases:**

- Legacy compatibility with existing analysis tools
- Custom output format requirements
- Alternative naming conventions
- Experimental or non-CMIP variables

Example 7: Per-Rule Override Configuration
-------------------------------------------

**Scenario**: Different variables need different dimension naming strategies

**Configuration:**

.. code-block:: yaml

   # Global default: flexible mode
   dimension_mapping_allow_override: yes
   
   rules:
     # Rule 1: CMIP-compliant output (strict mode)
     - model_variable: tas
       cmor_variable: tas
       dimension_mapping_allow_override: no
       # Output: time lat lon (CMIP standard)
     
     # Rule 2: Custom output (flexible mode)
     - model_variable: temp_3d
       cmor_variable: ta
       dimension_mapping_allow_override: yes
       dimension_mapping:
         lev: my_level
         latitude: y
         longitude: x
       # Output: time my_level y x (custom names)
     
     # Rule 3: Partial override
     - model_variable: wind_u
       cmor_variable: ua
       dimension_mapping:
         lev: height  # Only override vertical dimension
       # Output: time height lat lon (mixed)

**Result:**

.. code-block:: python

   # Variable 1: CMIP standard names
   ds_tas.dims
   # Frozen({'time': 10, 'lat': 180, 'lon': 360})
   
   # Variable 2: Custom names
   ds_ta.dims
   # Frozen({'time': 10, 'my_level': 19, 'y': 180, 'x': 360})
   
   # Variable 3: Mixed (partial override)
   ds_ua.dims
   # Frozen({'time': 10, 'height': 19, 'lat': 180, 'lon': 360})

Integration with Coordinate Attributes
=======================================

Dimension mapping and coordinate attributes work together:

**Step 1: Dimension Mapping**

.. code-block:: python

   # Before: source dimension names
   ds.dims
   # Frozen({'time': 10, 'lev': 19, 'latitude': 180, 'longitude': 360})
   
   # After dimension mapping
   ds_mapped = map_dimensions(ds, rule)
   ds_mapped.dims
   # Frozen({'time': 10, 'plev19': 19, 'lat': 180, 'lon': 360})

**Step 2: Coordinate Attributes**

.. code-block:: python

   # After coordinate attributes
   ds_final = set_coordinate_attributes(ds_mapped, rule)
   
   # Now coordinates have correct names AND metadata
   print(ds_final['plev19'].attrs)
   # {
   #     'standard_name': 'air_pressure',
   #     'units': 'Pa',
   #     'axis': 'Z',
   #     'positive': 'down'
   # }
   
   print(ds_final['lat'].attrs)
   # {
   #     'standard_name': 'latitude',
   #     'units': 'degrees_north',
   #     'axis': 'Y'
   # }

Complete Transformation
-----------------------

.. code-block:: python

   # 1. Start with source data
   ds_source = xr.Dataset({
       'temp': (['time', 'lev', 'latitude', 'longitude'], data),
   })
   
   # 2. Map dimensions (Part 2)
   ds_mapped = map_dimensions(ds_source, rule)
   # Result: Dimensions renamed to CMIP names
   
   # 3. Set coordinate attributes (Part 1)
   ds_final = set_coordinate_attributes(ds_mapped, rule)
   # Result: CF-compliant metadata on all coordinates
   
   # 4. Final output is fully CMIP-compliant
   # - Correct dimension names ✓
   # - Correct coordinate metadata ✓
   # - Ready for CMIP submission ✓

Logging
=======

The dimension mapper provides detailed logging:

INFO Level
----------

.. code-block:: text

   [Dimension Mapping] Creating dimension mapping
     Source dimensions: ['time', 'lev', 'latitude', 'longitude']
     CMIP dimensions: ['time', 'plev19', 'lat', 'lon']
   [Dimension Mapping] Auto-mapped: lev → plev19 (type: pressure)
   [Dimension Mapping] Auto-mapped: latitude → lat (type: latitude)
   [Dimension Mapping] Auto-mapped: longitude → lon (type: longitude)
   [Dimension Mapping] Applying dimension mapping
     Renaming: lev → plev19
     Renaming: latitude → lat
     Renaming: longitude → lon
   [Dimension Mapping] Renamed 3 dimensions

DEBUG Level
-----------

.. code-block:: text

   Dimension 'lev' matched pattern for 'pressure'
   Dimension 'latitude' matched pattern for 'latitude'
   Could not detect type for 'unknown_dim'

WARNING Level
-------------

.. code-block:: text

   Unmapped source dimensions: ['unknown_dim']
   Unmapped CMIP dimensions: ['plev19']
   User mapping specifies source dimension 'level' which doesn't exist in dataset

Troubleshooting
===============

Dimensions Not Detected
------------------------

If a dimension is not being detected:

1. **Check dimension name**
   
   - Does it match common patterns?
   - Try adding to user mapping

2. **Add attributes**
   
   .. code-block:: python
   
      ds.coords['y'].attrs = {
          'standard_name': 'latitude',
          'axis': 'Y'
      }

3. **Use user mapping**
   
   .. code-block:: yaml
   
      dimension_mapping:
        y: lat
        x: lon

Wrong CMIP Dimension Selected
------------------------------

If the wrong CMIP dimension is selected:

1. **Check coordinate size**
   
   - For pressure: size must match (19 levels → plev19)
   - Verify your data has the correct number of levels

2. **Use user mapping to override**
   
   .. code-block:: yaml
   
      dimension_mapping:
        lev: plev8  # Force plev8 instead of auto-detection

Validation Warnings
-------------------

If you see validation warnings:

1. **Review unmapped dimensions**
   
   - Are they needed by CMIP table?
   - Should they be mapped?

2. **Adjust validation mode**
   
   - ``ignore``: Suppress warnings
   - ``warn``: See warnings (default)
   - ``error``: Fail on issues

Mapping Not Applied
-------------------

If dimensions aren't being renamed:

1. **Check configuration**
   
   .. code-block:: yaml
   
      xarray_enable_dimension_mapping: yes

2. **Verify pipeline order**
   
   - Dimension mapping should run before coordinate attributes

3. **Check logs**
   
   - Look for mapping messages
   - Check for errors or warnings

Performance
===========

- Dimension detection is fast (< 1ms per dimension)
- No additional I/O operations
- Minimal memory overhead
- Efficient for large datasets

Technical Details
=================

Dimension vs Coordinate
-----------------------

- **Dimension**: Size of an axis (e.g., ``lat: 180``)
- **Coordinate**: Values along that axis (e.g., ``lat = [-89.5, -88.5, ...]``)

The mapper renames both the dimension and its associated coordinate variable.

Case Sensitivity
----------------

Dimension name matching is case-insensitive:

- ``LAT``, ``Lat``, ``lat`` all match ``latitude`` pattern

Coordinate Variables
--------------------

When a dimension is renamed, its coordinate variable is also renamed:

.. code-block:: python

   # Before
   ds.coords['latitude']  # Coordinate variable
   ds.dims['latitude']    # Dimension
   
   # After mapping
   ds.coords['lat']       # Renamed coordinate
   ds.dims['lat']         # Renamed dimension

Multiple Mappings
-----------------

If multiple source dimensions could map to the same CMIP dimension, the mapper uses:

1. User-specified mapping (highest priority)
2. Exact name match
3. Size-based selection (for pressure levels)
4. First detected match

See Also
========

- :doc:`coordinate_attributes` - Setting CF-compliant coordinate metadata
- :doc:`pycmor_configuration` - Configuration options
- `CF Conventions - Coordinate Types <http://cfconventions.org/Data/cf-conventions/cf-conventions-1.10/cf-conventions.html#coordinate-types>`_
- `CMIP6 Coordinate Tables <https://github.com/PCMDI/cmip6-cmor-tables>`_
- :mod:`pycmor.std_lib.dimension_mapping` - Module API documentation
