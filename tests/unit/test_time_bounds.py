import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pymor.std_lib.time_bounds import time_bounds


def test_time_bounds_creation():
    """Test that time bounds are created correctly when they don't exist."""
    # Create a test dataset with a time dimension
    times = pd.date_range("2000-01-01", periods=5, freq="D")
    data = np.random.rand(5)
    ds = xr.Dataset({"temperature": (["time"], data)}, coords={"time": times})

    # Create a mock Rule object (simplified for testing)
    class MockRule:
        pass

    rule = MockRule()

    # Apply the time_bounds function
    result = time_bounds(ds, rule)

    # Verify the results
    assert "time_bnds" in result.coords
    assert result.coords["time_bnds"].dims == ("time", "bnds")
    assert result.coords["time_bnds"].shape == (5, 2)  # same length as time dimension

    # Check the bounds values
    bounds = result.coords["time_bnds"].values
    time_diff = np.median(np.diff(times))  # get the time frequency

    # For all but the last time point, the end bound should be the next time point
    for i in range(len(times) - 1):
        assert bounds[i, 0] == times[i].to_numpy()
        assert bounds[i, 1] == times[i + 1].to_numpy()

    # For the last time point, the end bound should be time + time_diff
    assert bounds[-1, 0] == times[-1].to_numpy()
    assert bounds[-1, 1] == (times[-1] + time_diff).to_numpy()

    # Check the time variable's bounds attribute
    assert "bounds" in result["time"].attrs
    assert result["time"].attrs["bounds"] == "time_bnds"


def test_existing_time_bounds():
    """Test that existing time bounds are not modified."""
    # Create a dataset with existing time bounds
    times = pd.date_range("2000-01-01", periods=3, freq="D")
    time_bnds = pd.date_range("2000-01-01", periods=4, freq="D")
    time_bnds = np.array([time_bnds[:-1], time_bnds[1:]]).T
    ds = xr.Dataset(
        {"temperature": (["time"], np.random.rand(3))},
        coords={"time": times, "time_bnds": (["time", "bnds"], time_bnds)},
    )

    # Add bounds attribute to time variable
    ds["time"].attrs["bounds"] = "time_bnds"

    class MockRule:
        pass

    rule = MockRule()

    # Apply the time_bounds function
    result = time_bounds(ds, rule)

    # The function should return the dataset unchanged
    xr.testing.assert_identical(result, ds)


def test_single_time_point():
    """Test behavior with a single time point (should not create bounds)."""
    ds = xr.Dataset(
        {"temperature": (["time"], [25.0])},
        coords={"time": pd.date_range("2000-01-01", periods=1)},
    )

    class MockRule:
        pass

    rule = MockRule()

    # Should raise ValueError because we can't create bounds with a single time point
    with pytest.raises(ValueError):
        time_bounds(ds, rule)


def test_dataarray_instead_of_dataset():
    """Test that passing a DataArray instead of a Dataset raises an error."""
    # Create a test DataArray with a time dimension
    times = pd.date_range("2000-01-01", periods=3, freq="D")
    da = xr.DataArray(
        np.random.rand(3), dims=["time"], coords={"time": times}, name="temperature"
    )

    class MockRule:
        pass

    rule = MockRule()

    # Should raise ValueError because we need a Dataset, not a DataArray
    with pytest.raises(ValueError, match="The input is not a dataset"):
        time_bounds(da, rule)


def test_monthly_frequency():
    """Test that time bounds are created correctly for monthly frequency data."""
    # Create a test dataset with monthly time steps
    times = pd.date_range("2000-01-01", periods=12, freq="MS")  # MS = month start
    data = np.random.rand(12)
    ds = xr.Dataset({"precipitation": (["time"], data)}, coords={"time": times})

    class MockRule:
        pass

    rule = MockRule()

    # Apply the time_bounds function
    result = time_bounds(ds, rule)

    # Verify the results
    assert "time_bnds" in result.coords
    assert result.coords["time_bnds"].dims == ("time", "bnds")
    assert result.coords["time_bnds"].shape == (12, 2)  # 12 months, 2 bounds each

    # Check the bounds values
    bounds = result.coords["time_bnds"].values

    # For all but the last month, the end bound should be the start of the next month
    for i in range(len(times) - 1):
        assert bounds[i, 0] == times[i].to_numpy()
        assert bounds[i, 1] == times[i + 1].to_numpy()

    # For the last month, the end bound should be the start of the next month
    assert bounds[-1, 0] == times[-1].to_numpy()
    assert (
        bounds[-1, 1]
        == (times[-1] + pd.offsets.MonthEnd(1) + pd.offsets.Day(1)).to_numpy()
    )

    # Check the time variable's bounds attribute
    assert "bounds" in result["time"].attrs
    assert result["time"].attrs["bounds"] == "time_bnds"
