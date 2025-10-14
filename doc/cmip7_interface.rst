========================================
CMIP7 Data Request Interface Usage Guide
========================================

This document explains how to use the CMIP7 data request interface in pycmor.

Overview
========

The CMIP7 data request interface provides:

1. **CMIP7Interface** - High-level interface to work with CMIP7 metadata
2. **CMIP7DataRequestVariable** - Variable class with CMIP7 compound name support
3. **CMIP7DataRequestTable** - Table class for organizing variables
4. **Backward compatibility** with CMIP6 table-based lookups

Installation
============

First, install the official CMIP7 Data Request API:

.. code-block:: bash

   pip install CMIP7-data-request-api

Generating Metadata Files
==========================

Before using the interface, generate the metadata files using the official API:

.. code-block:: bash

   # Export all variables with metadata for version v1.2.2.2
   export_dreq_lists_json -a -m dreq_v1.2.2.2_metadata.json v1.2.2.2 dreq_v1.2.2.2.json

This creates two files:

- ``dreq_v1.2.2.2.json`` - Experiment-to-variable mappings
- ``dreq_v1.2.2.2_metadata.json`` - Variable metadata

Basic Usage
===========

1. Initialize the Interface
----------------------------

.. code-block:: python

   from pycmor.data_request import CMIP7Interface

   # Create interface
   interface = CMIP7Interface()

   # Load metadata from file
   interface.load_metadata(metadata_file='dreq_v1.2.2.2_metadata.json')

   # Optionally load experiments data
   interface.load_experiments_data('dreq_v1.2.2.2.json')

2. Get Variable Metadata by CMIP7 Compound Name
------------------------------------------------

.. code-block:: python

   # CMIP7 compound name format: realm.variable.branding.frequency.region
   var_metadata = interface.get_variable_metadata('atmos.tas.tavg-h2m-hxy-u.mon.GLB')

   print(var_metadata['standard_name'])  # 'air_temperature'
   print(var_metadata['units'])           # 'K'
   print(var_metadata['frequency'])       # 'mon'

3. Get Variable by CMIP6 Name (Backward Compatibility)
-------------------------------------------------------

.. code-block:: python

   # Use CMIP6 compound name: table.variable
   var_metadata = interface.get_variable_by_cmip6_name('Amon.tas')

   print(var_metadata['cmip7_compound_name'])  # 'atmos.tas.tavg-h2m-hxy-u.mon.GLB'

4. Find All Variants of a Variable
-----------------------------------

.. code-block:: python

   # Find all variants of 'clt' (total cloud fraction)
   variants = interface.find_variable_variants('clt')

   print(f'Found {len(variants)} variants')
   for var in variants:
       print(f"  {var['cmip7_compound_name']}")

Output:

.. code-block:: text

   Found 8 variants
     atmos.clt.tavg-u-hxy-u.mon.GLB
     atmos.clt.tavg-u-hxy-u.day.GLB
     atmos.clt.tavg-u-hxy-lnd.day.GLB
     atmos.clt.tavg-u-hxy-u.3hr.GLB
     atmos.clt.tpt-u-hxy-u.3hr.GLB
     atmos.clt.tavg-u-hxy-u.1hr.30S-90S
     atmos.clt.tavg-u-hxy-u.mon.30S-90S
     atmos.clt.tpt-u-hs-u.subhr.GLB

5. Filter Variants by Criteria
-------------------------------

.. code-block:: python

   # Find monthly global variants of 'tas'
   variants = interface.find_variable_variants(
       'tas',
       frequency='mon',
       region='GLB'
   )

   # Find ocean variables at daily frequency
   variants = interface.find_variable_variants(
       'tos',
       realm='ocean',
       frequency='day'
   )

6. Get Variables for an Experiment
-----------------------------------

.. code-block:: python

   # Get all variables for historical experiment
   hist_vars = interface.get_variables_for_experiment('historical')

   print(f"Core priority: {len(hist_vars['Core'])} variables")
   print(f"High priority: {len(hist_vars['High'])} variables")

   # Get only Core priority variables
   core_vars = interface.get_variables_for_experiment('historical', priority='Core')
   print(f"Core variables: {core_vars[:5]}")

7. Parse and Build Compound Names
----------------------------------

.. code-block:: python

   # Parse a CMIP7 compound name
   parsed = interface.parse_compound_name('atmos.tas.tavg-h2m-hxy-u.mon.GLB')
   print(parsed)
   # {'realm': 'atmos', 'variable': 'tas', 'branding': 'tavg-h2m-hxy-u', 
   #  'frequency': 'mon', 'region': 'GLB'}

   # Build a compound name from components
   compound_name = interface.build_compound_name(
       realm='ocean',
       variable='tos',
       branding='tavg-u-hxy-sea',
       frequency='mon',
       region='GLB'
   )
   print(compound_name)  # 'ocean.tos.tavg-u-hxy-sea.mon.GLB'

Working with CMIP7DataRequestVariable
======================================

Create Variable from Metadata
------------------------------

.. code-block:: python

   from pycmor.data_request import CMIP7DataRequestVariable
   import json

   # Load metadata
   with open('dreq_v1.2.2.2_metadata.json', 'r') as f:
       metadata = json.load(f)

   # Get variable data
   var_data = metadata['Compound Name']['atmos.tas.tavg-h2m-hxy-u.mon.GLB']

   # Create variable instance
   var = CMIP7DataRequestVariable.from_dict(var_data)

Access Variable Properties
---------------------------

.. code-block:: python

   # Basic properties
   print(var.name)              # 'tas'
   print(var.out_name)          # 'tas'
   print(var.standard_name)     # 'air_temperature'
   print(var.units)             # 'K'
   print(var.frequency)         # 'mon'
   print(var.modeling_realm)    # 'atmos'

   # CMIP7-specific properties
   print(var.cmip7_compound_name)  # 'atmos.tas.tavg-h2m-hxy-u.mon.GLB'
   print(var.branding_label)       # 'tavg-h2m-hxy-u'
   print(var.region)               # 'GLB'

   # CMIP6 backward compatibility
   print(var.cmip6_compound_name)  # 'Amon.tas'
   print(var.table_name)           # 'Amon'

Get Attributes for NetCDF
--------------------------

.. code-block:: python

   # Get attributes for xarray DataArray
   attrs = var.attrs
   print(attrs)
   # {'standard_name': 'air_temperature',
   #  'long_name': 'Near-Surface Air Temperature',
   #  'units': 'K',
   #  'cell_methods': 'area: time: mean',
   #  'comment': '...'}

   # Get global attributes for xarray Dataset
   global_attrs = var.global_attrs()
   print(global_attrs)
   # {'Conventions': 'CF-1.7 CMIP-7.0',
   #  'mip_era': 'CMIP7',
   #  'frequency': 'mon',
   #  'realm': 'atmos',
   #  'variable_id': 'tas',
   #  'table_id': 'Amon',
   #  'cmip7_compound_name': 'atmos.tas.tavg-h2m-hxy-u.mon.GLB',
   #  'branding_label': 'tavg-h2m-hxy-u',
   #  'region': 'GLB'}

Understanding CMIP7 Compound Names
===================================

Structure
---------

CMIP7 compound names have 5 components::

   realm.variable.branding.frequency.region

**Example:** ``atmos.tas.tavg-h2m-hxy-u.mon.GLB``

Components Explained
--------------------

1. **Realm** (``atmos``): Modeling realm

   - ``atmos`` - Atmosphere
   - ``ocean`` - Ocean
   - ``land`` - Land
   - ``seaIce`` - Sea ice
   - ``landIce`` - Land ice
   - ``aerosol`` - Aerosol

2. **Variable** (``tas``): Physical parameter name

   - Same as CMIP6 variable names

3. **Branding Label** (``tavg-h2m-hxy-u``): Processing descriptor

   - **Temporal sampling**: ``tavg`` (time average), ``tpt`` (time point), ``tmax``, ``tmin``
   - **Vertical level**: ``h2m`` (2m height), ``p19`` (19 pressure levels), ``u`` (unspecified)
   - **Spatial grid**: ``hxy`` (horizontal grid), ``hs`` (site)
   - **Domain**: ``u`` (unspecified), ``sea`` (ocean), ``lnd`` (land), ``air`` (atmosphere)

4. **Frequency** (``mon``): Output frequency

   - ``mon`` - Monthly
   - ``day`` - Daily
   - ``3hr`` - 3-hourly
   - ``1hr`` - Hourly
   - ``6hr`` - 6-hourly
   - ``subhr`` - Sub-hourly
   - ``fx`` - Fixed (time-invariant)

5. **Region** (``GLB``): Spatial domain

   - ``GLB`` - Global
   - ``30S-90S`` - Southern Hemisphere
   - ``ATA`` - Antarctica
   - Custom regional definitions

Comparison with CMIP6
----------------------

.. list-table::
   :header-rows: 1
   :widths: 20 30 50

   * - Aspect
     - CMIP6
     - CMIP7
   * - Format
     - ``table.variable``
     - ``realm.variable.branding.frequency.region``
   * - Example
     - ``Amon.tas``
     - ``atmos.tas.tavg-h2m-hxy-u.mon.GLB``
   * - Components
     - 2
     - 5
   * - Uniqueness
     - Table name
     - Frequency + Branding + Region

Common Use Cases
================

Use Case 1: CMORization Workflow
---------------------------------

.. code-block:: python

   from pycmor.data_request import CMIP7Interface, CMIP7DataRequestVariable
   import xarray as xr

   # Initialize interface
   interface = CMIP7Interface()
   interface.load_metadata(metadata_file='dreq_v1.2.2.2_metadata.json')

   # Get variable metadata
   var_metadata = interface.get_variable_metadata('atmos.tas.tavg-h2m-hxy-u.mon.GLB')

   # Create variable instance
   var = CMIP7DataRequestVariable.from_dict(var_metadata)

   # Load your model data
   ds = xr.open_dataset('model_output.nc')

   # Apply CMIP7 metadata
   ds['tas'].attrs.update(var.attrs)
   ds.attrs.update(var.global_attrs({
       'source_id': 'MY-MODEL',
       'experiment_id': 'historical',
       # ... other required attributes
   }))

   # Save CMORized output
   ds.to_netcdf('cmor_output.nc')

Use Case 2: Finding Variables for Your Model
---------------------------------------------

.. code-block:: python

   # Find all monthly atmospheric variables
   interface = CMIP7Interface()
   interface.load_metadata(metadata_file='dreq_v1.2.2.2_metadata.json')
   interface.load_experiments_data('dreq_v1.2.2.2.json')

   # Get Core priority variables for historical experiment
   core_vars = interface.get_variables_for_experiment('historical', priority='Core')

   # Filter for monthly atmospheric variables
   monthly_atmos = [
       v for v in core_vars 
       if v.startswith('atmos.') and '.mon.' in v
   ]

   print(f"Found {len(monthly_atmos)} monthly atmospheric Core variables")
   for var in monthly_atmos[:10]:
       metadata = interface.get_variable_metadata(var)
       print(f"  {var}: {metadata['long_name']}")

Use Case 3: Backward Compatibility with CMIP6 Code
---------------------------------------------------

.. code-block:: python

   # If you have existing CMIP6 code that uses table.variable format
   cmip6_var_name = 'Amon.tas'

   # Get the CMIP7 metadata
   interface = CMIP7Interface()
   interface.load_metadata(metadata_file='dreq_v1.2.2.2_metadata.json')

   var_metadata = interface.get_variable_by_cmip6_name(cmip6_var_name)

   # Now you have both CMIP6 and CMIP7 information
   print(f"CMIP6: {var_metadata['cmip6_compound_name']}")
   print(f"CMIP7: {var_metadata['cmip7_compound_name']}")
   print(f"Table: {var_metadata['cmip6_table']}")

Use Case 4: Integration with CMORizer
--------------------------------------

The CMIP7 interface can be automatically initialized within the CMORizer for
runtime queries and metadata lookups.

**Configuration:**

Add the metadata file path to your pycmor configuration:

.. code-block:: yaml

   general:
     cmor_version: CMIP7
     CMIP_Tables_Dir: /path/to/cmip7/tables
     cmip7_metadata_file: /path/to/dreq_v1.2.2.2_metadata.json
     cmip7_experiments_file: /path/to/dreq_v1.2.2.2.json  # optional

   # ... rest of your configuration

**Usage:**

.. code-block:: python

   from pycmor import CMORizer

   # Load configuration
   cmorizer = CMORizer.from_dict(config)

   # Access the CMIP7 interface if available
   if cmorizer.cmip7_interface:
       # Query variables during runtime
       variants = cmorizer.cmip7_interface.find_variable_variants(
           'tas', 
           frequency='mon',
           region='GLB'
       )
       
       # Get detailed metadata
       metadata = cmorizer.cmip7_interface.get_variable_metadata(
           'atmos.tas.tavg-h2m-hxy-u.mon.GLB'
       )
       
       # Check which experiments require a variable
       experiments = cmorizer.cmip7_interface.get_all_experiments()
       print(f"Available experiments: {experiments}")
   else:
       print("CMIP7 interface not available")

   # Continue with normal CMORization workflow
   cmorizer.process()

**Notes:**

- The interface is **optional** - CMORizer works without it
- Only initialized if ``cmor_version: CMIP7`` and metadata file is configured
- Gracefully degrades if CMIP7 Data Request API is not installed
- Does not affect the core CMORization workflow
- Useful for runtime queries and validation

API Reference
=============

CMIP7Interface
--------------

Methods
^^^^^^^

- ``load_metadata(version, metadata_file, force_reload)`` - Load variable metadata
- ``load_experiments_data(experiments_file)`` - Load experiment mappings
- ``get_variable_metadata(cmip7_compound_name)`` - Get metadata by CMIP7 name
- ``get_variable_by_cmip6_name(cmip6_compound_name)`` - Get metadata by CMIP6 name
- ``find_variable_variants(variable_name, realm, frequency, region)`` - Find all variants
- ``get_variables_for_experiment(experiment, priority)`` - Get variables for experiment
- ``get_all_experiments()`` - List all experiments
- ``get_all_compound_names()`` - List all CMIP7 compound names
- ``parse_compound_name(cmip7_compound_name)`` - Parse into components
- ``build_compound_name(realm, variable, branding, frequency, region)`` - Build from components

Properties
^^^^^^^^^^

- ``version`` - Currently loaded version
- ``metadata`` - Loaded metadata dictionary
- ``experiments_data`` - Loaded experiments data

CMIP7DataRequestVariable
-------------------------

Key Properties
^^^^^^^^^^^^^^

- ``name`` - Variable name
- ``out_name`` - Output name
- ``standard_name`` - CF standard name
- ``units`` - Units
- ``frequency`` - Output frequency
- ``modeling_realm`` - Modeling realm
- ``cmip7_compound_name`` - Full CMIP7 compound name
- ``cmip6_compound_name`` - CMIP6 compound name (backward compatibility)
- ``branding_label`` - CMIP7 branding label
- ``region`` - CMIP7 region code
- ``table_name`` - CMIP6 table name (backward compatibility)

Methods
^^^^^^^

- ``from_dict(data)`` - Create from dictionary
- ``from_all_var_info_json(compound_name, use_cmip6_name)`` - Load from vendored file
- ``attrs`` - Get attributes for xarray DataArray
- ``global_attrs(override_dict)`` - Get global attributes for xarray Dataset
- ``clone()`` - Create a copy

Troubleshooting
===============

ImportError: CMIP7 Data Request API not available
--------------------------------------------------

**Solution:** Install the official API:

.. code-block:: bash

   pip install CMIP7-data-request-api

ValueError: Metadata not loaded
--------------------------------

**Solution:** Call ``load_metadata()`` before using query methods:

.. code-block:: python

   interface.load_metadata(metadata_file='dreq_v1.2.2.2_metadata.json')

Variable not found
------------------

**Solution:** Check the compound name format:

- CMIP7: ``realm.variable.branding.frequency.region``
- CMIP6: ``table.variable``

Use ``get_all_compound_names()`` to see available variables.

Additional Resources
====================

- `CMIP6 to CMIP7 Transition Guide <../CMIP6_to_CMIP7_transition.md>`_
- `CMIP7 Data Request Website <https://wcrp-cmip.org/cmip7/cmip7-data-request/>`_
- `CMIP7 Data Request Software <https://github.com/CMIP-Data-Request/CMIP7_DReq_Software>`_
- `Official Documentation <https://cmip-data-request.github.io/CMIP7_DReq_Software/data_request_api/>`_
