===============================
CMIP7 Configuration Guide
===============================

This guide explains how to configure pycmor for CMIP7 data CMORization using YAML configuration files.

Overview
========

CMIP7 introduces several changes from CMIP6:

1. **Compound Names**: Variables use 5-component names (e.g., ``atmos.tas.tavg-h2m-hxy-u.mon.GLB``)
2. **Data Request API**: Metadata comes from the CMIP7 Data Request instead of CMOR tables
3. **Controlled Vocabularies**: Updated CV structure in CMIP7-CVs repository
4. **New Required Fields**: ``institution_id`` is now required

Key Differences from CMIP6
===========================

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Aspect
     - CMIP6
     - CMIP7
   * - Variable specification
     - ``cmor_variable: tas``
     - ``compound_name: atmos.tas.tavg-h2m-hxy-u.mon.GLB``
   * - Metadata source
     - CMIP6 CMOR Tables
     - CMIP7 Data Request API
   * - Frequency
     - Inferred from table
     - From compound name or explicit
   * - Realm
     - ``model_component: atmos``
     - ``realm: atmos`` (or from compound name)
   * - Institution
     - Optional
     - ``institution_id`` required
   * - Tables directory
     - ``CMIP_Tables_Dir`` required
     - Optional for CMIP7
   * - Metadata file
     - Not needed
     - ``CMIP7_DReq_metadata`` recommended

Minimal CMIP7 Configuration
============================

Here's the minimum configuration needed for CMIP7:

.. code-block:: yaml

   general:
     name: "my-cmip7-project"
     cmor_version: "CMIP7"
     CV_Dir: "/path/to/CMIP7-CVs"
     CMIP7_DReq_metadata: "/path/to/dreq_metadata.json"

   pycmor:
     warn_on_no_rule: False
     dask_cluster: "local"

   rules:
     - name: tas
       compound_name: atmos.tas.tavg-h2m-hxy-u.mon.GLB
       model_variable: temp2
       inputs:
         - path: /path/to/model/output
           pattern: "temp2_*.nc"

       # Required identifiers (5 minimum)
       source_id: AWI-CM-1-1-HR
       institution_id: AWI
       experiment_id: historical
       variant_label: r1i1p1f1
       grid_label: gn

       output_directory: /path/to/output

Required Fields Explained
==========================

General Section
---------------

**cmor_version** (required)
  Must be ``"CMIP7"`` for CMIP7 CMORization.

**CV_Dir** (required)
  Path to CMIP7 Controlled Vocabularies directory. Clone from:

  .. code-block:: bash

     git clone https://github.com/WCRP-CMIP/CMIP7-CVs.git

**CMIP7_DReq_metadata** (recommended)
  Path to CMIP7 Data Request metadata JSON file. Generate using:

  .. code-block:: bash

     export_dreq_lists_json -a -m dreq_metadata.json v1.2.2.2 dreq.json

Rules Section
-------------

Each rule must specify:

**compound_name** (recommended) OR **cmor_variable** (required)
  - With compound_name: ``atmos.tas.tavg-h2m-hxy-u.mon.GLB``
  - Without: ``cmor_variable: tas`` (must also specify ``frequency``, ``realm``, ``table_id``)

**model_variable** (required)
  Variable name in your model output files.

**inputs** (required)
  List of input file specifications:

  .. code-block:: yaml

     inputs:
       - path: /path/to/data
         pattern: "*.nc"

**source_id** (required)
  Model identifier (e.g., ``AWI-CM-1-1-HR``).

**institution_id** (required)
  Institution identifier (e.g., ``AWI``). **New in CMIP7!**

**experiment_id** (required)
  Experiment identifier (e.g., ``historical``, ``piControl``).

**variant_label** (required)
  Ensemble member in format ``r<N>i<N>p<N>f<N>`` (e.g., ``r1i1p1f1``).

**grid_label** (required)
  Grid identifier (e.g., ``gn`` for native grid, ``gr`` for regridded).

**output_directory** (required)
  Where to write CMORized output files.

Optional but Recommended Fields
================================

**grid** (recommended)
  Human-readable grid description:

  .. code-block:: yaml

     grid: "T63 Gaussian grid (192x96)"

**nominal_resolution** (recommended)
  Model resolution:

  .. code-block:: yaml

     nominal_resolution: "250 km"

**frequency** (optional)
  Output frequency. Automatically provided by compound_name, but can override:

  .. code-block:: yaml

     frequency: mon

**realm** (optional)
  Modeling realm. Automatically provided by compound_name:

  .. code-block:: yaml

     realm: atmos

**table_id** (optional)
  CMOR table ID. Automatically provided by compound_name:

  .. code-block:: yaml

     table_id: Amon

Complete Example
================

Atmospheric Variable with Compound Name
----------------------------------------

.. code-block:: yaml

   general:
     name: "cmip7-historical"
     cmor_version: "CMIP7"
     mip: "CMIP"
     CV_Dir: "/work/ab0995/CMIP7/CMIP7-CVs"
     CMIP7_DReq_metadata: "/work/ab0995/CMIP7/dreq_v1.2.2.2_metadata.json"

   pycmor:
     warn_on_no_rule: False
     use_flox: True
     dask_cluster: "local"

   rules:
     - name: near_surface_temperature
       # Compound name provides: cmor_variable, frequency, realm, table_id
       compound_name: atmos.tas.tavg-h2m-hxy-u.mon.GLB

       # Your model's variable name
       model_variable: temp2

       # Input files
       inputs:
         - path: /work/ab0995/model_runs/historical/outdata/echam
           pattern: "temp2_echam_mon_*.nc"

       # Required identifiers
       source_id: AWI-CM-1-1-HR
       institution_id: AWI
       experiment_id: historical
       variant_label: r1i1p1f1
       grid_label: gn

       # Recommended metadata
       grid: "T63 Gaussian grid (192x96)"
       nominal_resolution: "250 km"

       # Output
       output_directory: /work/ab0995/cmip7_output

Ocean Variable on Unstructured Grid
------------------------------------

.. code-block:: yaml

   rules:
     - name: sea_surface_temperature
       compound_name: ocean.tos.tavg-u-hxy-u.mon.GLB
       model_variable: sst

       inputs:
         - path: /work/ab0995/model_runs/historical/outdata/fesom
           pattern: "sst_fesom_mon_*.nc"

       # Required identifiers
       source_id: AWI-CM-1-1-HR
       institution_id: AWI
       experiment_id: historical
       variant_label: r1i1p1f1
       grid_label: gn

       # Unstructured grid information
       grid_file: /pool/data/AWICM/FESOM1/MESHES/core/griddes.nc
       mesh_path: /pool/data/AWICM/FESOM1/MESHES/core
       grid: "FESOM 1.4 unstructured grid (1306775 wet nodes)"
       nominal_resolution: "25 km"

       output_directory: /work/ab0995/cmip7_output

Without Compound Name (Manual Specification)
---------------------------------------------

If you don't use compound names, you must specify metadata manually:

.. code-block:: yaml

   rules:
     - name: ocean_co2_flux
       # Manual specification (no compound name)
       cmor_variable: fgco2
       model_variable: CO2f

       # Must specify these manually
       frequency: mon
       realm: ocnBgchem
       table_id: Omon

       inputs:
         - path: /work/ab0995/model_runs/piControl/outdata/recom
           pattern: "CO2f_fesom_mon_*.nc"

       # Required identifiers
       source_id: AWI-CM-1-1-HR
       institution_id: AWI
       experiment_id: piControl
       variant_label: r1i1p1f1
       grid_label: gn

       grid_file: /pool/data/AWICM/FESOM1/MESHES/core/griddes.nc
       mesh_path: /pool/data/AWICM/FESOM1/MESHES/core
       grid: "FESOM 1.4 unstructured grid"
       nominal_resolution: "25 km"

       output_directory: /work/ab0995/cmip7_output

Multiple Variables
------------------

.. code-block:: yaml

   rules:
     # Atmospheric temperature
     - name: tas
       compound_name: atmos.tas.tavg-h2m-hxy-u.mon.GLB
       model_variable: temp2
       inputs:
         - path: /path/to/echam/output
           pattern: "temp2_*.nc"
       source_id: AWI-CM-1-1-HR
       institution_id: AWI
       experiment_id: historical
       variant_label: r1i1p1f1
       grid_label: gn
       grid: "T63 Gaussian grid"
       nominal_resolution: "250 km"
       output_directory: /path/to/output

     # Ocean temperature
     - name: tos
       compound_name: ocean.tos.tavg-u-hxy-u.mon.GLB
       model_variable: sst
       inputs:
         - path: /path/to/fesom/output
           pattern: "sst_*.nc"
       source_id: AWI-CM-1-1-HR
       institution_id: AWI
       experiment_id: historical
       variant_label: r1i1p1f1
       grid_label: gn
       grid_file: /path/to/griddes.nc
       mesh_path: /path/to/mesh
       grid: "FESOM unstructured grid"
       nominal_resolution: "25 km"
       output_directory: /path/to/output

     # Precipitation
     - name: pr
       compound_name: atmos.pr.tavg-u-hxy-u.mon.GLB
       model_variable: aprl
       inputs:
         - path: /path/to/echam/output
           pattern: "aprl_*.nc"
       source_id: AWI-CM-1-1-HR
       institution_id: AWI
       experiment_id: historical
       variant_label: r1i1p1f1
       grid_label: gn
       grid: "T63 Gaussian grid"
       nominal_resolution: "250 km"
       output_directory: /path/to/output

Understanding CMIP7 Compound Names
===================================

Structure
---------

CMIP7 compound names have 5 components::

   realm.variable.branding.frequency.region

Example: ``atmos.tas.tavg-h2m-hxy-u.mon.GLB``

Components
----------

1. **realm**: ``atmos`` (atmosphere, ocean, land, seaIce, landIce, aerosol)
2. **variable**: ``tas`` (physical parameter name)
3. **branding**: ``tavg-h2m-hxy-u`` (processing descriptor)

   - ``tavg`` = time average
   - ``h2m`` = 2-meter height
   - ``hxy`` = horizontal grid
   - ``u`` = unspecified domain

4. **frequency**: ``mon`` (monthly, day, 3hr, 1hr, 6hr, subhr, fx)
5. **region**: ``GLB`` (global, 30S-90S, ATA, etc.)

Benefits of Using Compound Names
---------------------------------

✅ **Less configuration**: No need to specify ``cmor_variable``, ``frequency``, ``realm``, ``table_id``

✅ **Consistency**: Metadata comes directly from CMIP7 Data Request

✅ **Validation**: Ensures official CMIP7 variable definitions

✅ **Future-proof**: Automatically updated with Data Request

Validation
==========

Before running CMORization, validate your configuration:

.. code-block:: bash

   pycmor validate config my_config.yaml

This checks:

- Required fields are present
- Field formats are correct (e.g., ``variant_label`` format)
- Paths exist
- CMIP7-specific fields are valid

Running CMORization
===================

.. code-block:: bash

   pycmor process my_config.yaml

Monitor progress:

.. code-block:: bash

   # View logs
   tail -f logs/pycmor-process-*.log

   # Check Dask dashboard
   grep Dashboard logs/pycmor-process-*.log

Migration from CMIP6
=====================

To migrate a CMIP6 configuration to CMIP7:

1. **Update general section**:

   .. code-block:: yaml

      # Before (CMIP6)
      general:
        cmor_version: "CMIP6"
        CMIP_Tables_Dir: "/path/to/cmip6-cmor-tables/Tables"
        CV_Dir: "/path/to/CMIP6_CVs"

      # After (CMIP7)
      general:
        cmor_version: "CMIP7"
        CV_Dir: "/path/to/CMIP7-CVs"
        CMIP7_DReq_metadata: "/path/to/dreq_metadata.json"

2. **Update each rule**:

   .. code-block:: yaml

      # Before (CMIP6)
      rules:
        - name: tas
          cmor_variable: tas
          model_variable: temp2
          model_component: atmos
          # ... other fields

      # After (CMIP7)
      rules:
        - name: tas
          compound_name: atmos.tas.tavg-h2m-hxy-u.mon.GLB
          model_variable: temp2
          institution_id: AWI  # NEW: required in CMIP7
          grid: "T63 grid"     # NEW: recommended
          nominal_resolution: "250 km"  # NEW: recommended
          # ... other fields (source_id, experiment_id, etc. unchanged)

3. **Keep unchanged**:

   - ``source_id``
   - ``experiment_id``
   - ``variant_label``
   - ``grid_label``
   - ``model_variable``
   - Input/output paths

Common Issues and Solutions
============================

Missing institution_id
----------------------

**Error**: ``KeyError: 'institution_id'``

**Solution**: Add ``institution_id`` to your rule (required in CMIP7):

.. code-block:: yaml

   institution_id: AWI

Missing compound_name or cmor_variable
---------------------------------------

**Error**: Validation fails

**Solution**: Provide either ``compound_name`` OR all of:

- ``cmor_variable``
- ``frequency``
- ``realm``
- ``table_id``

Invalid variant_label format
-----------------------------

**Error**: ``variant_label`` validation fails

**Solution**: Use format ``r<N>i<N>p<N>f<N>``:

.. code-block:: yaml

   variant_label: r1i1p1f1  # Correct
   variant_label: r1i1p1    # Wrong (CMIP6 format)

CMIP7 Data Request not found
-----------------------------

**Error**: Cannot load metadata

**Solution**: Generate metadata file:

.. code-block:: bash

   pip install CMIP7-data-request-api
   export_dreq_lists_json -a -m dreq_metadata.json v1.2.2.2 dreq.json

Then add to config:

.. code-block:: yaml

   general:
     CMIP7_DReq_metadata: "/path/to/dreq_metadata.json"

Additional Resources
====================

- :doc:`cmip7_interface` - CMIP7 Data Request API usage
- :doc:`cmip7_controlled_vocabularies` - CMIP7 CVs documentation
- :doc:`quickstart` - General pycmor quickstart
- :doc:`pycmor_building_blocks` - Configuration file structure
- `CMIP7 Data Request <https://wcrp-cmip.org/cmip7/cmip7-data-request/>`_
- `CMIP7-CVs Repository <https://github.com/WCRP-CMIP/CMIP7-CVs>`_

Summary Checklist
=================

Before running CMIP7 CMORization, ensure:

☑ **General section**:

  - ``cmor_version: "CMIP7"``
  - ``CV_Dir`` points to CMIP7-CVs
  - ``CMIP7_DReq_metadata`` points to metadata JSON (recommended)

☑ **Each rule has**:

  - ``compound_name`` (recommended) OR ``cmor_variable`` + ``frequency`` + ``realm`` + ``table_id``
  - ``model_variable``
  - ``inputs`` with path and pattern
  - ``source_id``
  - ``institution_id`` ← **Required in CMIP7!**
  - ``experiment_id``
  - ``variant_label`` (format: ``r<N>i<N>p<N>f<N>``)
  - ``grid_label``
  - ``output_directory``

☑ **Recommended fields**:

  - ``grid`` (grid description)
  - ``nominal_resolution`` (model resolution)

☑ **Validation**:

  - Run ``pycmor validate config your_config.yaml``
  - Check all paths exist
  - Verify CMIP7-CVs is up to date
