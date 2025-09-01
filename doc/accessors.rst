Unified Pymor Xarray Accessors
===============================

The pymor package provides unified xarray accessors that offer a consistent interface
to all pymor functionality through the ``data.pymor`` and ``dataset.pymor`` namespaces.
This unified approach simplifies the user experience while maintaining full backward
compatibility with specialized accessors.

Overview
--------

Pymor registers two main types of xarray accessors:

- **Specialized Accessors**: Domain-specific functionality (e.g., ``data.timefreq`` for time frequency operations)
- **Unified Accessor**: Single namespace ``data.pymor`` that provides access to all pymor functionality

The unified accessor delegates to specialized accessors while providing a consistent,
discoverable interface for users.

Features
--------

- 🎯 **Single Namespace**: All pymor functionality accessible via ``data.pymor`` and ``dataset.pymor``
- 🔄 **Delegation Pattern**: Unified accessor delegates to specialized accessors for actual implementation
- 🔙 **Backward Compatibility**: Existing specialized accessors (``data.timefreq``) continue to work
- 🚀 **Future-Ready**: Easy to extend with new pymor functionality
- 📚 **Consistent API**: Same method signatures and behavior across all access patterns

Quick Start
-----------

**Basic Usage:**

.. code-block:: python

   import pymor  # Registers all accessors
   import xarray as xr
   import cftime

   # Create sample data
   times = [cftime.Datetime360Day(2000, m, 15) for m in range(1, 13)]
   data = xr.DataArray(
       range(12), 
       coords={"time": times}, 
       dims="time",
       name="temperature"
   )

   # Use unified pymor accessor
   freq_info = data.pymor.infer_frequency()
   print(f"Frequency: {freq_info['frequency']}")  # 'M'

   # Check temporal resolution
   resolution = data.pymor.check_resolution(target_approx_interval=30.0)
   print(f"Valid for resampling: {resolution['is_valid_for_resampling']}")

   # Safe resampling with validation
   resampled = data.pymor.resample_safe(
       target_approx_interval=30.0,
       calendar="360_day"
   )

**Dataset Usage:**

.. code-block:: python

   # Create dataset
   dataset = xr.Dataset({
       "temperature": data,
       "precipitation": data * 2
   })

   # Use unified accessor on datasets
   freq_info = dataset.pymor.infer_frequency()
   
   # Resample entire dataset
   resampled_ds = dataset.pymor.resample_safe(
       freq_str="3M",  # Quarterly
       calendar="360_day"
   )

Accessor Comparison
-------------------

The unified accessor provides the same functionality as specialized accessors
but through a consistent namespace:

**Specialized Accessor (still available):**

.. code-block:: python

   # Time frequency operations via specialized accessor
   data.timefreq.infer_frequency()
   data.timefreq.check_resolution(target_approx_interval=30.0)
   data.timefreq.resample_safe(freq_str="M")

**Unified Accessor (recommended):**

.. code-block:: python

   # Same operations via unified accessor
   data.pymor.infer_frequency()
   data.pymor.check_resolution(target_approx_interval=30.0)
   data.pymor.resample_safe(freq_str="M")

Both approaches produce identical results, but the unified accessor provides
a single, discoverable entry point for all pymor functionality.

Available Methods
-----------------

The unified pymor accessor currently provides the following methods:

Time Frequency Operations
~~~~~~~~~~~~~~~~~~~~~~~~~

All time frequency methods are available through the unified accessor:

.. code-block:: python

   # Infer temporal frequency from data
   result = data.pymor.infer_frequency(
       strict=True,
       calendar="360_day",
       log=True
   )

   # Check if resolution is sufficient for resampling
   check = data.pymor.check_resolution(
       target_approx_interval=30.0,
       tolerance=0.01,
       strict=False
   )

   # Safe resampling with automatic validation
   resampled = data.pymor.resample_safe(
       target_approx_interval=30.0,  # ~monthly
       freq_str="M",                 # pandas frequency string
       calendar="360_day",
       method="mean"
   )

Parameter Flexibility
~~~~~~~~~~~~~~~~~~~~~

The ``resample_safe`` method accepts flexible parameter combinations:

.. code-block:: python

   # Option 1: Provide target interval (will be converted to frequency string)
   data.pymor.resample_safe(target_approx_interval=30.0)

   # Option 2: Provide frequency string directly
   data.pymor.resample_safe(freq_str="M")

   # Option 3: Provide both (freq_str takes precedence)
   data.pymor.resample_safe(
       target_approx_interval=30.0,
       freq_str="M"
   )

Dataset Operations
~~~~~~~~~~~~~~~~~~

For datasets, the unified accessor operates on the time dimension:

.. code-block:: python

   # Automatic time dimension detection
   dataset.pymor.infer_frequency()

   # Explicit time dimension specification
   dataset.pymor.check_resolution(
       target_approx_interval=1.0,
       time_dim="time"
   )

   # Resample all variables in the dataset
   resampled_ds = dataset.pymor.resample_safe(
       freq_str="D",
       time_dim="time"
   )

Error Handling
--------------

The unified accessor provides consistent error handling:

.. code-block:: python

   # Data without time dimension
   spatial_data = xr.DataArray([[1, 2], [3, 4]], dims=["x", "y"])

   try:
       spatial_data.pymor.infer_frequency()
   except (ValueError, KeyError) as e:
       print(f"Error: {e}")  # No time dimension found

   # Dataset with missing time dimension
   try:
       dataset_no_time.pymor.resample_safe(freq_str="M")
   except ValueError as e:
       print(f"Error: {e}")  # Time dimension not found

Architecture
------------

The unified accessor uses a delegation pattern for clean separation of concerns:

**Implementation Structure:**

.. code-block:: python

   @register_dataarray_accessor("pymor")
   class PymorDataArrayAccessor:
       def __init__(self, xarray_obj):
           self._obj = xarray_obj
           # Initialize specialized accessors
           self._timefreq = TimeFrequencyAccessor(xarray_obj)
       
       def resample_safe(self, *args, **kwargs):
           # Delegate to specialized accessor
           return self._timefreq.resample_safe(*args, **kwargs)

**Benefits:**

- **Modularity**: Core functionality remains in specialized modules
- **Maintainability**: Changes to specialized accessors automatically propagate
- **Extensibility**: Easy to add new specialized accessors to the unified interface
- **Testing**: Can test delegation and specialized functionality independently

Future Extensions
-----------------

The unified accessor is designed to accommodate future pymor functionality:

.. code-block:: python

   # Future pymor features will be accessible via the unified accessor
   # data.pymor.quality_control()      # Future QC functionality
   # data.pymor.metadata_validation()  # Future metadata tools
   # data.pymor.cmip_compliance()      # Future CMIP validation

   # While maintaining access to specialized functionality
   # data.pymor.timefreq.resample_safe()  # Direct access if needed

Registration and Import
-----------------------

Accessors are automatically registered when importing pymor:

.. code-block:: python

   import pymor  # Registers all accessors

   # Both specialized and unified accessors are now available
   assert hasattr(data, 'timefreq')  # Specialized accessor
   assert hasattr(data, 'pymor')     # Unified accessor

**Internal Registration:**

The accessor registration is centralized in ``pymor.accessors`` module:

.. code-block:: python

   # In pymor/accessors.py
   from xarray import register_dataarray_accessor, register_dataset_accessor
   from .core.infer_freq import TimeFrequencyAccessor, DatasetFrequencyAccessor

   @register_dataarray_accessor("pymor")
   class PymorDataArrayAccessor:
       # Unified accessor implementation
       pass

Best Practices
--------------

**Recommended Usage:**

1. **Use the unified accessor** (``data.pymor``) for new code
2. **Maintain existing code** using specialized accessors (``data.timefreq``)
3. **Import pymor once** at the top of your script to register all accessors
4. **Use consistent parameter names** across different methods

**Example Workflow:**

.. code-block:: python

   import pymor
   import xarray as xr

   def process_climate_data(dataset):
       """Process climate dataset with unified pymor accessor."""
       
       # Check temporal resolution
       resolution = dataset.pymor.check_resolution(
           target_approx_interval=30.0  # Monthly
       )
       
       if not resolution['is_valid_for_resampling']:
           raise ValueError("Data resolution too coarse for monthly analysis")
       
       # Resample to monthly means
       monthly_data = dataset.pymor.resample_safe(
           freq_str="M",
           method="mean"
       )
       
       return monthly_data

API Reference
-------------

For detailed API documentation of individual methods, see:

- :doc:`infer_freq` - Core time frequency functionality
- :doc:`API` - Complete API reference

The unified accessor methods have identical signatures and behavior to their
specialized counterparts, ensuring consistent behavior across access patterns.

Migration Guide
---------------

**From Specialized to Unified Accessor:**

.. code-block:: python

   # Old approach (still works)
   freq = data.timefreq.infer_frequency()
   check = data.timefreq.check_resolution(target_approx_interval=30.0)
   result = data.timefreq.resample_safe(freq_str="M")

   # New unified approach (recommended)
   freq = data.pymor.infer_frequency()
   check = data.pymor.check_resolution(target_approx_interval=30.0)
   result = data.pymor.resample_safe(freq_str="M")

**No Breaking Changes:**

- All existing code continues to work unchanged
- Specialized accessors remain available
- Method signatures and behavior are identical
- Only the namespace changes (``timefreq`` → ``pymor``)

The unified accessor provides a path forward for consistent pymor usage while
maintaining full backward compatibility with existing code.
