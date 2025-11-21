.. _time_bounds:

Time Bounds
===========

The time bounds feature in PyMorize provides functionality to handle and generate time bounds for time series data. This is particularly useful for climate and weather data where variables are often associated with time intervals rather than single time points.

Overview
--------

The ``time_bounds`` function creates time bounds for a given xarray Dataset with a time dimension. It automatically detects the time coordinate, checks for existing bounds, and creates appropriate bounds if they don't exist.

Features
--------

- Automatic detection of time coordinate in the dataset
- Support for various time frequencies (daily, monthly, etc.)
- Handling of both standard and offset time points (e.g., mid-month dates)
- Preservation of existing time bounds
- Comprehensive logging of operations

Usage
-----

Basic usage:

.. code-block:: python

    from pymor.std_lib.time_bounds import time_bounds
    import xarray as xr
    import numpy as np
    import pandas as pd

    # Create a sample dataset with time dimension
    times = pd.date_range("2000-01-01", periods=12, freq="MS")  # Monthly start
    data = np.random.rand(12)
    ds = xr.Dataset({"temperature": (["time"], data)}, coords={"time": times})

    # Apply time bounds
    result = time_bounds(ds, rule)

Supported Time Frequencies
-------------------------

The function handles various time frequencies, including:

- Daily ("D")
- Monthly ("MS" for month start, or custom offsets)
- Custom frequencies using pandas offset aliases

For example, with offset monthly data (e.g., 15th of each month):

.. code-block:: python

    # Create dataset with offset monthly data (15th of each month)
    base_dates = pd.date_range("2000-01-15", periods=12, freq="MS")
    times = base_dates + pd.DateOffset(days=14)
    data = np.random.rand(12)
    ds = xr.Dataset({"temperature": (["time"], data)}, coords={"time": times})
    
    # Apply time bounds
    result = time_bounds(ds, rule)

Logging
-------

The function provides detailed logging of its operations, including:

- Dataset validation
- Time coordinate detection
- Bounds creation/verification
- Warnings and errors

Example output:

.. code-block:: text

    [Time bounds] dataset_name
      → is dataset:   ✅
      → time label  : time
      → bounds label: bnds
      → has time bounds: ❌
      → time values  : 12 points from 2000-01-01 to 2000-12-01
      → time step    : 30 days 00:00:00
      → set time bounds: time_bnds(12, 2)
      → bounds range   : 2000-01-01 to 2001-01-01

Error Handling
--------------

The function raises appropriate exceptions for invalid inputs:

- ``ValueError`` if the input is not an xarray Dataset
- ``ValueError`` if no valid time coordinate is found
- ``ValueError`` if there are fewer than 2 time points

API Reference
-------------

.. automodule:: pymor.std_lib.time_bounds
   :members:
   :undoc-members:
   :show-inheritance:
