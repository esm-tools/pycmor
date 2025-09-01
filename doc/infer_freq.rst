Temporal Frequency Inference and Resolution Validation
=======================================================

This module provides tools to infer the temporal frequency of time coordinates
in an xarray `Dataset` or `DataArray`, with support for non-standard calendars
(e.g. 360_day, noleap), and to validate whether the data has a sufficiently fine
temporal resolution for operations like resampling or aggregation, in line with
CMIP6 compliance.

Features
--------

- 📅 Calendar-aware frequency inference (standard, noleap, 360_day)
- 🧠 Intelligent fallback if `xarray.infer_freq()` fails
- 🛠 `xarray` accessors for `infer_frequency()` and `check_resolution()`
- ✅ Comparison to CMIP6 `approx_interval` (e.g. 30.4375 days for monthly)
- 🔍 Strict mode to detect missing or irregular time steps
- 🧾 Human-readable logging of inference results


Quick Start
-----------

.. code-block:: python

   import xarray as xr
   import cftime
   from pymor.core.infer_freq import infer_frequency

   # Create a DataArray with 360_day calendar
   times = [cftime.Datetime360Day(2000, m, 15) for m in range(1, 5)]
   da = xr.DataArray([1, 2, 3, 4], coords={"time": times}, dims="time")

   # Simple frequency inference (returns FrequencyResult object)
   result = da.timefreq.infer_frequency(log=False)
   print(f"Frequency: {result.frequency}")  # Output: "M"

   # Detailed frequency inference with metadata (returns FrequencyResult)
   result = infer_frequency(times, return_metadata=True, calendar="360_day")
   print(f"Frequency: {result.frequency}")      # "M"
   print(f"Delta: {result.delta_days} days")   # 30.0 days
   print(f"Exact: {result.is_exact}")          # True
   print(f"Status: {result.status}")           # "valid"

   # Validate resolution against CMIP6 monthly approx_interval
   da.timefreq.check_resolution(target_approx_interval=30.4375)

**DataArray Accessor (``da.timefreq``):**

.. code-block:: python

   # Infer frequency with metadata (returns FrequencyResult object)
   result = da.timefreq.infer_frequency(strict=True, calendar="360_day", log=False)
   print(result.frequency)     # 'M'
   print(result.is_exact)      # True
   print(result.status)        # 'valid'
   
   # Check if resolution is fine enough for resampling
   check = da.timefreq.check_resolution(target_approx_interval=30.4375)
   print(check['is_valid_for_resampling'])  # True
   
   # Safe resampling with automatic resolution validation
   resampled = da.timefreq.resample_safe(
       freq_str="M", 
       target_approx_interval=30.4375,
       calendar="360_day",
       method="mean"
   )

**Dataset Accessor (``ds.timefreq``):**

.. code-block:: python

   # Infer frequency from dataset's time dimension
   info = ds.timefreq.infer_frequency(time_dim="time", log=False)
   
   # Check resolution for entire dataset
   check = ds.timefreq.check_resolution(
       target_approx_interval=30.4375, 
       time_dim="time"
   )
   
   # Safe dataset resampling
   resampled_ds = ds.timefreq.resample_safe(
       freq_str="M",
       target_approx_interval=30.4375,
       time_dim="time",
       calendar="360_day",
       method="mean"
   )

API Reference
-------------

FrequencyResult
~~~~~~~~~~~~~~~

When ``return_metadata=True``, frequency inference functions return a ``FrequencyResult`` namedtuple with the following fields:

.. code-block:: python

   FrequencyResult = namedtuple('FrequencyResult', [
       'frequency',      # str or None: inferred frequency string (e.g., 'M', '2D')
       'delta_days',     # float or None: median time delta in days
       'step',           # int or None: step multiplier for the frequency
       'is_exact',       # bool: whether the time series has exact regular spacing
       'status'          # str: status message ('valid', 'irregular', 'no_match', etc.)
   ])

**Example Usage:**

.. code-block:: python

   # Get detailed metadata
   result = infer_frequency(times, return_metadata=True)
   
   # Access fields by name (much cleaner than tuple unpacking!)
   if result.frequency:
       print(f"Found {result.frequency} frequency")
       print(f"Time delta: {result.delta_days:.2f} days")
       print(f"Regular spacing: {result.is_exact}")
       print(f"Status: {result.status}")

Status Values
~~~~~~~~~~~~~

The ``status`` field in ``FrequencyResult`` indicates the quality and characteristics of the inferred frequency:

- **"valid"**: Regular time series with exact spacing
- **"irregular"**: Time intervals vary but no clear pattern of missing steps
- **"missing_steps"**: Regular pattern detected but with gaps in the expected sequence
- **"no_match"**: No recognizable frequency pattern found
- **"too_short"**: Time series has fewer than 2 time points
- **"invalid_input"**: Error processing the time values

**Examples of Different Status Values:**

.. code-block:: python

   import cftime
   from pymor.core.infer_freq import infer_frequency

   # Valid: Perfect monthly spacing
   times_valid = [
       cftime.Datetime360Day(2000, 1, 15),
       cftime.Datetime360Day(2000, 2, 15),
       cftime.Datetime360Day(2000, 3, 15)
   ]
   result = infer_frequency(times_valid, return_metadata=True, log=True)
   # Status: "valid", Frequency: "M"

   # Irregular: Varying intervals but no clear gaps
   times_irregular = [
       cftime.Datetime360Day(2000, 1, 1),
       cftime.Datetime360Day(2000, 1, 20),  # 19 days
       cftime.Datetime360Day(2000, 2, 15),  # 26 days
       cftime.Datetime360Day(2000, 3, 10)   # 24 days
   ]
   result = infer_frequency(times_irregular, return_metadata=True, log=True)
   # Status: "irregular", Frequency: detected pattern with ❌ spacing

   # Missing Steps: Regular pattern with gaps (requires strict=True)
   times_missing = [
       cftime.Datetime360Day(2000, 1, 1),   # Day 1
       cftime.Datetime360Day(2000, 1, 2),   # Day 2
       cftime.Datetime360Day(2000, 1, 3),   # Day 3
       # Missing days 4, 5, 6 (3-day gap!)
       cftime.Datetime360Day(2000, 1, 7),   # Day 7
       cftime.Datetime360Day(2000, 1, 8)    # Day 8
   ]
   result = infer_frequency(times_missing, return_metadata=True, strict=True, log=True)
   # Status: "missing_steps", Frequency: "D" (daily pattern with gaps)

   # Too Short: Insufficient data
   times_short = [cftime.Datetime360Day(2000, 1, 1)]
   result = infer_frequency(times_short, return_metadata=True, log=True)
   # Status: "too_short", Frequency: None

Core Functions
~~~~~~~~~~~~~~

.. autofunction:: infer_frequency
.. autofunction:: infer_frequency_core
.. autofunction:: is_resolution_fine_enough
.. autofunction:: log_frequency_check

Accessor Methods
~~~~~~~~~~~~~~~~

Time frequency functionality is available through xarray accessors. For comprehensive
documentation of all accessor methods, including both specialized (``timefreq``) and 
unified (``pymor``) accessors, see:

.. seealso::
   :doc:`accessors` - Complete guide to pymor xarray accessors

**Quick Reference:**

.. code-block:: python

   # Specialized accessor (domain-specific)
   result = data.timefreq.infer_frequency()
   check = data.timefreq.check_resolution(target_approx_interval=30.0)
   
   # Unified accessor (recommended for new code)
   result = data.pymor.infer_frequency()
   check = data.pymor.check_resolution(target_approx_interval=30.0)

Calendar Support
----------------

The following calendars are supported:

- ``standard`` or ``gregorian``: 365.25 days/year
- ``noleap``: 365 days/year
- ``360_day``: 360 days/year
