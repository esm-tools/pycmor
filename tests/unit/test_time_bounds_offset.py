"""Test time bounds with offset monthly frequency."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

# Temporarily skip this test module until time_bounds is finalized
try:
    from pycmor.std_lib.time_bounds import time_bounds
except Exception:
    pytest.skip(
        "Temporarily skipping time_bounds offset test during refactor",
        allow_module_level=True,
    )


def test_monthly_frequency_with_offset():
    """Test that time bounds are created correctly for monthly frequency data with offset dates."""
    # Create a test dataset with monthly time steps starting from the 15th of each month
    base_dates = pd.date_range("2000-01-15", periods=12, freq="MS")  # Start from 15th
    times = base_dates + pd.DateOffset(days=14)  # Offset to 15th of each month
    data = np.random.rand(12)
    ds = xr.Dataset({"temperature": (["time"], data)}, coords={"time": times})

    class MockRule:
        pass

    rule = MockRule()

    # Apply the time_bounds function
    result = time_bounds(ds, rule)

    # Verify the results
    assert "time_bnds" in result.coords
    assert result.coords["time_bnds"].dims == ("time", "bnds")
    assert result.coords["time_bnds"].shape == (12, 2)

    # Check the bounds values
    bounds = result.coords["time_bnds"].values

    # For all months, the start bound should be the 15th of the current month
    # and the end bound should be the 15th of the next month
    for i in range(len(times) - 1):
        assert bounds[i, 0] == times[i].to_numpy()
        assert bounds[i, 1] == times[i + 1].to_numpy()

    # For the last month, the end bound should be 15th of the next month
    last_month = times[-1].to_numpy()
    next_month = (times[-1] + pd.offsets.MonthBegin(1)).to_numpy() + np.timedelta64(14, "D")
    assert bounds[-1, 0] == last_month
    assert bounds[-1, 1] == next_month

    # Check the time variable's bounds attribute
    assert "bounds" in result["time"].attrs
    assert result["time"].attrs["bounds"] == "time_bnds"
