========================
NetCDF Chunking Guide
========================

Overview
========

`pycmor` supports internal NetCDF chunking to optimize I/O performance when reading and writing climate data files. Chunking divides the data into smaller blocks that can be read and written more efficiently, especially for datasets with specific access patterns.

Why Chunking Matters
=====================

NetCDF-4 files can be internally "chunked" to improve I/O performance:

- **Better read performance**: Reading subsets of data becomes faster when chunks align with access patterns
- **Compression efficiency**: Chunked data can be compressed more effectively
- **Parallel I/O**: Chunked files enable better parallel read/write operations
- **Optimal for time-series**: Climate data is often accessed along the time dimension, so chunking along time improves performance

Configuration Options
======================

Global Configuration via Inherit Block
---------------------------------------

The recommended way to configure chunking is through the ``inherit`` block in your pycmor configuration file. 
Settings in the ``inherit`` block are automatically passed down to all rules, making them available as rule attributes:

.. code-block:: yaml

   general:
     cmor_version: "CMIP6"
     CMIP_Tables_Dir: ./cmip6-cmor-tables/Tables/
   
   pycmor:
     warn_on_no_rule: False
   
   # Chunking configuration that applies to all rules
   inherit:
     # Enable/disable chunking
     netcdf_enable_chunking: yes
     
     # Chunking algorithm: simple, even_divisor, or iterative
     netcdf_chunk_algorithm: simple
     
     # Target chunk size (can be specified as bytes or string like '100MB')
     netcdf_chunk_size: 100MB
     
     # Tolerance for chunk size matching (0.0-1.0, used by even_divisor and iterative)
     netcdf_chunk_tolerance: 0.5
     
     # Prefer chunking along time dimension
     netcdf_chunk_prefer_time: yes
     
     # Compression level (1-9, higher = better compression but slower)
     netcdf_compression_level: 4
     
     # Enable zlib compression
     netcdf_enable_compression: yes
   
   rules:
     - model_variable: temp
       cmor_variable: tas
       # ... other rule settings ...
       # This rule inherits all chunking settings from the inherit block

Alternative: Global pycmor Configuration
-----------------------------------------

You can also configure chunking defaults in the global ``pycmor`` configuration block (e.g., ``~/.config/pycmor/pycmor.yaml``).
However, using the ``inherit`` block is preferred as it makes the settings explicit and easier to override per-rule:

.. code-block:: yaml

   pycmor:
     netcdf_enable_chunking: yes
     netcdf_chunk_algorithm: simple
     netcdf_chunk_size: 100MB
     # ... other settings ...

Per-Rule Configuration
----------------------

You can override chunking settings for specific variables in your YAML rule configuration:

.. code-block:: yaml

   rules:
     - model_variable: temperature
       cmor_variable: tas
       cmor_table: CMIP6_Amon.json
       model_component: atmosphere
       input_patterns:
         - /path/to/data/*_tas.nc
       # Override chunking for this variable
       netcdf_chunk_algorithm: even_divisor
       netcdf_chunk_size: 50MB
       netcdf_chunk_prefer_time: yes

Chunking Algorithms
===================

1. Simple Algorithm (Default)
------------------------------

The ``simple`` algorithm is fast and works well for most use cases:

- Preferentially chunks along the time dimension (if ``netcdf_chunk_prefer_time: yes``)
- Keeps spatial dimensions unchunked for better spatial access
- Calculates chunk size based on target memory size

**Best for**: Standard climate data with time-series access patterns

**Example configuration**:

.. code-block:: yaml

   netcdf_chunk_algorithm: simple
   netcdf_chunk_size: 100MB
   netcdf_chunk_prefer_time: yes

2. Even Divisor Algorithm
--------------------------

The ``even_divisor`` algorithm finds chunk sizes that evenly divide dimension lengths:

- Ensures chunks align perfectly with dimension boundaries
- Considers aspect ratio preferences across dimensions
- May take longer to compute but produces optimal chunks

**Best for**: Data that will be accessed in regular patterns, or when you need precise control over chunk alignment

**Example configuration**:

.. code-block:: yaml

   netcdf_chunk_algorithm: even_divisor
   netcdf_chunk_size: 100MB
   netcdf_chunk_tolerance: 0.5

3. Iterative Algorithm
-----------------------

The ``iterative`` algorithm scales chunks iteratively to match the target size:

- Starts with maximum chunk size and scales down
- Respects aspect ratio preferences
- Good balance between speed and optimization

**Best for**: Complex datasets where simple chunking doesn't work well

**Example configuration**:

.. code-block:: yaml

   netcdf_chunk_algorithm: iterative
   netcdf_chunk_size: 100MB
   netcdf_chunk_tolerance: 0.5

Choosing Chunk Size
====================

The optimal chunk size depends on your use case:

For Time-Series Analysis
-------------------------

.. code-block:: yaml

   netcdf_chunk_size: 50MB
   netcdf_chunk_prefer_time: yes

- Smaller chunks along time dimension
- Full spatial dimensions
- Optimizes reading time slices

For Spatial Analysis
--------------------

.. code-block:: yaml

   netcdf_chunk_size: 100MB
   netcdf_chunk_prefer_time: no

- Chunks distributed across all dimensions
- Better for reading spatial slices

For Large Datasets
-------------------

.. code-block:: yaml

   netcdf_chunk_size: 200MB
   netcdf_compression_level: 6

- Larger chunks reduce metadata overhead
- Higher compression saves disk space

For Small Datasets
-------------------

.. code-block:: yaml

   netcdf_chunk_size: 10MB
   netcdf_compression_level: 4

- Smaller chunks for finer-grained access
- Moderate compression for speed

Compression Settings
====================

Chunking works together with compression:

.. code-block:: yaml

   # Enable compression (recommended)
   netcdf_enable_compression: yes

   # Compression level 1-9
   # 1 = fastest, less compression
   # 9 = slowest, best compression
   # 4 = good balance (default)
   netcdf_compression_level: 4

**Compression level guidelines**:

- **Level 1-3**: Fast compression, use for temporary files or when speed is critical
- **Level 4-6**: Balanced compression (recommended for most use cases)
- **Level 7-9**: Maximum compression, use for archival or when disk space is limited

Examples
========

Example 1: Default Configuration
---------------------------------

.. code-block:: yaml

   # Use defaults - simple chunking with 100MB chunks
   netcdf_enable_chunking: yes
   netcdf_chunk_algorithm: simple
   netcdf_chunk_size: 100MB

This will:

- Chunk along time dimension
- Keep spatial dimensions full
- Apply moderate compression (level 4)

Example 2: High-Resolution Ocean Data
--------------------------------------

.. code-block:: yaml

   # Optimize for large ocean datasets
   netcdf_enable_chunking: yes
   netcdf_chunk_algorithm: even_divisor
   netcdf_chunk_size: 200MB
   netcdf_chunk_tolerance: 0.6
   netcdf_compression_level: 6

Example 3: Atmospheric 3D Fields
---------------------------------

.. code-block:: yaml

   # Optimize for 3D atmospheric data
   netcdf_enable_chunking: yes
   netcdf_chunk_algorithm: iterative
   netcdf_chunk_size: 150MB
   netcdf_chunk_prefer_time: yes
   netcdf_compression_level: 5

Example 4: Disable Chunking
----------------------------

.. code-block:: yaml

   # Disable chunking (use contiguous storage)
   netcdf_enable_chunking: no

Performance Tips
================

1. **Match access patterns**: If you primarily read time series, use ``netcdf_chunk_prefer_time: yes``

2. **Test different sizes**: Start with 100MB and adjust based on your data access patterns

3. **Consider compression**: Higher compression levels reduce file size but increase I/O time

4. **Monitor performance**: Use tools like ``ncdump -sh`` to inspect chunk sizes in output files

5. **Balance chunk size**:

   - Too small: High metadata overhead
   - Too large: Inefficient partial reads
   - Sweet spot: Usually 10-200MB depending on data size

Checking Chunk Information
===========================

After generating files, you can inspect the chunking with:

.. code-block:: bash

   # View chunk information
   ncdump -sh output_file.nc

   # Example output:
   # float temperature(time, lat, lon) ;
   #     temperature:_ChunkSizes = 10, 180, 360 ;
   #     temperature:_DeflateLevel = 4 ;

Troubleshooting
===============

Chunking fails with "NoMatchingChunks" error
---------------------------------------------

**Solution**: Increase ``netcdf_chunk_tolerance``:

.. code-block:: yaml

   netcdf_chunk_tolerance: 0.8  # Increase from default 0.5

Files are too large
-------------------

**Solution**: Increase compression level:

.. code-block:: yaml

   netcdf_compression_level: 7  # Increase from default 4

I/O is slow
-----------

**Solution**: Try different chunk sizes or algorithms:

.. code-block:: yaml

   netcdf_chunk_size: 50MB  # Reduce from 100MB
   netcdf_chunk_algorithm: simple  # Use faster algorithm

Chunks don't align with dimensions
-----------------------------------

**Solution**: Use the ``even_divisor`` algorithm:

.. code-block:: yaml

   netcdf_chunk_algorithm: even_divisor
   netcdf_chunk_tolerance: 0.7

References
==========

- `NetCDF-4 Chunking Guide <https://www.unidata.ucar.edu/software/netcdf/docs/netcdf_perf_chunking.html>`_
- `Xarray Chunking Documentation <https://docs.xarray.dev/en/stable/user-guide/io.html#chunking-and-performance>`_
- `Dynamic Chunks Library <https://github.com/jbusecke/dynamic_chunks>`_ (inspiration for this implementation)

Advanced: Custom Chunking in Python
====================================

If you need more control, you can programmatically set chunking:

.. code-block:: python

   from pycmor.std_lib.chunking import calculate_chunks_simple, get_encoding_with_chunks
   import xarray as xr

   # Load your dataset
   ds = xr.open_dataset('input.nc')

   # Calculate optimal chunks
   chunks = calculate_chunks_simple(
       ds,
       target_chunk_size='100MB',
       prefer_time_chunking=True
   )

   # Get encoding with chunks and compression
   encoding = get_encoding_with_chunks(
       ds,
       chunks=chunks,
       compression_level=4,
       enable_compression=True
   )

   # Save with custom encoding
   ds.to_netcdf('output.nc', encoding=encoding)
