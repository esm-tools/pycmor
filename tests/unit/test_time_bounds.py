import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pycmor.std_lib.time_bounds import time_bounds


class MockRule:
    """Minimal mock for Rule with optional attributes."""

    def __init__(self, approx_interval=None, time_method=None):
        if approx_interval is not None:
            self.approx_interval = approx_interval
        if time_method is not None:
            self.time_method = time_method


def test_time_bounds_creation():
    """Time bounds are created correctly for daily data."""
    times = pd.date_range("2000-01-01", periods=5, freq="D")
    ds = xr.Dataset({"temperature": (["time"], np.random.rand(5))}, coords={"time": times})

    result = time_bounds(ds, MockRule())

    assert "time_bnds" in result.coords
    assert result.coords["time_bnds"].dims == ("time", "bnds")
    assert result.coords["time_bnds"].shape == (5, 2)

    bounds = result.coords["time_bnds"].values
    time_diff = np.median(np.diff(times))

    for i in range(len(times) - 1):
        assert bounds[i, 0] == times[i].to_numpy()
        assert bounds[i, 1] == times[i + 1].to_numpy()

    assert bounds[-1, 0] == times[-1].to_numpy()
    assert bounds[-1, 1] == (times[-1] + time_diff).to_numpy()
    assert result["time"].attrs["bounds"] == "time_bnds"


def test_existing_time_bounds_preserved():
    """Existing time bounds are not modified."""
    times = pd.date_range("2000-01-01", periods=3, freq="D")
    time_bnds = pd.date_range("2000-01-01", periods=4, freq="D")
    time_bnds = np.array([time_bnds[:-1], time_bnds[1:]]).T
    ds = xr.Dataset(
        {"temperature": (["time"], np.random.rand(3))},
        coords={"time": times, "time_bnds": (["time", "bnds"], time_bnds)},
    )
    ds["time"].attrs["bounds"] = "time_bnds"

    result = time_bounds(ds, MockRule())
    xr.testing.assert_identical(result, ds)


def test_single_time_point_raises():
    """Single time point with mean method raises ValueError."""
    ds = xr.Dataset(
        {"temperature": (["time"], [25.0])},
        coords={"time": pd.date_range("2000-01-01", periods=1)},
    )
    with pytest.raises(ValueError, match="at least 2 time points"):
        time_bounds(ds, MockRule())


def test_single_monthly_time_point_gets_month_bounds():
    """A one-record monthly file (one-month run) gets (month_start, next_month_start)."""
    ds = xr.Dataset(
        {"temperature": (["time"], [25.0])},
        coords={"time": pd.DatetimeIndex(["1850-01-16T12:00"])},
    )
    result = time_bounds(ds, MockRule(approx_interval=30.0))
    bounds = result["time_bnds"].values
    assert bounds.shape == (1, 2)
    assert pd.Timestamp(bounds[0, 0]) == pd.Timestamp("1850-01-01")
    assert pd.Timestamp(bounds[0, 1]) == pd.Timestamp("1850-02-01")
    assert result["time"].attrs["bounds"] == "time_bnds"


def test_single_monthly_tpt_point_keeps_its_stamp():
    """A one-record tpt monthly file gets no month bounds and keeps its sample time."""
    from types import SimpleNamespace

    stamp = pd.Timestamp("1850-02-01")
    ds = xr.Dataset({"sistressave": (["time"], [1.0])}, coords={"time": pd.DatetimeIndex([stamp])})
    rule = MockRule(time_method="INSTANTANEOUS")
    rule.data_request_variable = SimpleNamespace(
        frequency="mon", cell_methods="area: mean where sea_ice (mask=siconc) time: point"
    )
    result = time_bounds(ds, rule)
    assert "time_bnds" not in result.variables
    assert pd.Timestamp(result["time"].values[0]) == stamp


def test_dataarray_raises():
    """Passing a DataArray instead of Dataset raises ValueError."""
    times = pd.date_range("2000-01-01", periods=3, freq="D")
    da = xr.DataArray(np.random.rand(3), dims=["time"], coords={"time": times}, name="temperature")
    with pytest.raises(ValueError, match="not a dataset"):
        time_bounds(da, MockRule())


def test_monthly_frequency():
    """Time bounds are created correctly for monthly data."""
    times = pd.date_range("2000-01-01", periods=12, freq="MS")
    ds = xr.Dataset({"precipitation": (["time"], np.random.rand(12))}, coords={"time": times})

    result = time_bounds(ds, MockRule())

    assert "time_bnds" in result.coords
    assert result.coords["time_bnds"].shape == (12, 2)

    bounds = result.coords["time_bnds"].values
    for i in range(len(times) - 1):
        assert bounds[i, 0] == times[i].to_numpy()
        assert bounds[i, 1] == times[i + 1].to_numpy()

    assert bounds[-1, 0] == times[-1].to_numpy()
    assert bounds[-1, 1] == (times[-1] + pd.offsets.MonthEnd(1) + pd.offsets.Day(1)).to_numpy()
    assert result["time"].attrs["bounds"] == "time_bnds"


def test_monthly_with_approx_interval():
    """Monthly data with approx_interval ~30 uses month-start bounds."""
    times = pd.date_range("2000-01-15", periods=12, freq="MS") + pd.Timedelta("14D")
    ds = xr.Dataset({"temperature": (["time"], np.random.rand(12))}, coords={"time": times})

    result = time_bounds(ds, MockRule(approx_interval=30.0))

    bounds = result.coords["time_bnds"].values
    # Bounds should be month starts regardless of mid-month time points
    for i in range(12):
        ts = pd.Timestamp(times[i])
        month_start = ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        assert pd.Timestamp(bounds[i, 0]) == month_start


def test_instantaneous_time_method():
    """Instantaneous time method produces zero-width bounds."""
    times = pd.date_range("2000-01-01", periods=5, freq="D")
    ds = xr.Dataset({"temperature": (["time"], np.random.rand(5))}, coords={"time": times})

    result = time_bounds(ds, MockRule(time_method="instantaneous"))

    bounds = result.coords["time_bnds"].values
    for i in range(5):
        assert bounds[i, 0] == bounds[i, 1]


def test_climatology_skipped():
    """Climatology time method returns dataset unchanged (no bounds added)."""
    times = pd.date_range("2000-01-01", periods=5, freq="D")
    ds = xr.Dataset({"temperature": (["time"], np.random.rand(5))}, coords={"time": times})

    result = time_bounds(ds, MockRule(time_method="climatology"))
    assert "time_bnds" not in result.coords
