"""Tests for time averaging functionality."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

import pycmor.std_lib.timeaverage
from pycmor.std_lib.timeaverage import timeavg


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    # Create a year of daily data
    dates = pd.date_range("2023-01-01", "2023-12-31", freq="D")
    values = np.random.rand(len(dates))
    # Create chunked data array
    return xr.DataArray(values, coords={"time": dates}, dims=["time"]).chunk(
        {"time": 30}
    )  # Chunk by month


@pytest.fixture
def sample_rule():
    """Create a sample rule for testing."""

    class MockTable:
        def __init__(self, table_id, approx_interval, frequency=None):
            self.table_id = table_id if table_id is not None else "Unknown"
            self.approx_interval = approx_interval
            # If frequency is not provided, derive it from table_id
            self.frequency = frequency if frequency is not None else table_id

    class MockDataRequestVariable:
        def __init__(self, table):
            self.table = table
            self.table_header = table  # Add this line to provide table_header
            self.frequency = table.frequency

    class MockRule(dict):
        def __init__(self, table_id="Amon", approx_interval="30", frequency=None):
            super().__init__()
            self.data_request_variable = MockDataRequestVariable(
                MockTable(table_id, approx_interval, frequency)
            )
            self.adjust_timestamp = None

    return MockRule


def test_instantaneous_sampling(sample_data, sample_rule):
    """Test instantaneous sampling (first value of each period)."""
    rule = sample_rule("AmonPt", "30")  # Monthly instantaneous
    result = timeavg(sample_data, rule)

    # Should have 12 monthly values
    assert len(result) == 12
    # Each value should be the first value of the month
    assert pd.Timestamp("2023-01-01") in result.time.values
    assert pd.Timestamp("2023-02-01") in result.time.values


def test_mean_default_offset(sample_data, sample_rule):
    """Test mean with default offset (None).
    This used to be mid-values as default. Now it is changed to None
    """
    rule = sample_rule("Amon", "30")  # Monthly mean
    result = timeavg(sample_data, rule)

    # Should have 12 monthly values
    assert len(result) == 12
    # month starting dates
    times = pd.DatetimeIndex(result.time.values)
    assert times[0].strftime("%Y-%m-%d") == "2023-01-01"
    assert times[1].strftime("%Y-%m-%d") == "2023-02-01"


@pytest.mark.parametrize(
    "offset,expected_date",
    [
        (0.0, "2023-01-01"),  # Start of month
        (0.5, "2023-01-15 12:00:00"),  # Middle of month
        (1.0, "2023-01-31"),  # End of month
        ("first", "2023-01-01"),  # Start of month
        ("mid", "2023-01-15 12:00:00"),  # Middle of month
        ("last", "2023-01-31"),  # End of month
        ("14d", "2023-01-15"),  # Fixed 14 days offset
    ],
)
def test_mean_with_different_offsets(sample_data, sample_rule, offset, expected_date):
    """Test mean with various offset specifications."""
    rule = sample_rule("Amon", "30")  # Monthly mean
    rule["adjust_timestamp"] = offset
    result = timeavg(sample_data, rule)

    # Check January timestamp
    jan_time = pd.Timestamp(result.time.values[0])
    if offset in (0.5, "mid"):
        assert jan_time.strftime("%Y-%m-%d %H:%M:%S") == expected_date
    else:
        assert jan_time.strftime("%Y-%m-%d") == expected_date


def test_climatology_monthly(sample_data, sample_rule):
    """Test monthly climatology."""
    rule = sample_rule("AmonC", "30", frequency="monC")
    result = timeavg(sample_data, rule)

    # Should have 12 values (one per month)
    assert len(result) == 12
    # Should have month coordinate
    assert "month" in result.coords


def test_climatology_hourly(sample_data, sample_rule):
    """Test hourly climatology."""
    # Create hourly data first
    hourly_dates = pd.date_range("2023-01-01", "2023-01-07", freq="h")
    hourly_values = np.random.rand(len(hourly_dates))
    hourly_data = xr.DataArray(
        hourly_values, coords={"time": hourly_dates}, dims=["time"]
    ).chunk(
        {"time": 24}
    )  # Chunk by day

    rule = sample_rule("AmonCM", "30", frequency="1hrCM")
    result = timeavg(hourly_data, rule)

    # Should have 24 values (one per hour)
    assert len(result) == 24
    # Should have hour coordinate
    assert "hour" in result.coords


FREQUENCY_TIME_METHOD = {
    "fx": "MEAN",
    "1hr": "MEAN",
    "3hr": "MEAN",
    "6hr": "MEAN",
    "day": "MEAN",
    "mon": "MEAN",
    "yr": "MEAN",
    "dec": "MEAN",
    "1hrPt": "INSTANTANEOUS",
    "subhrPt": "INSTANTANEOUS",
    "6hrPt": "INSTANTANEOUS",
    "3hrPt": "INSTANTANEOUS",
    "monPt": "INSTANTANEOUS",
    "yrPt": "INSTANTANEOUS",
    "1hrCM": "CLIMATOLOGY",
    "monC": "CLIMATOLOGY",
}


@pytest.mark.parametrize("frequency_name, expected", FREQUENCY_TIME_METHOD.items())
def test__get_time_method(frequency_name, expected):
    answer = pycmor.std_lib.timeaverage._get_time_method(frequency_name)
    assert answer == expected


def test__frequency_from_approx_interval_decade():
    assert (
        pycmor.std_lib.timeaverage._frequency_from_approx_interval("3650") == "10YS"
    )  # Decade conversion


def test__frequency_from_approx_interval_year():
    # Test that 365 days is interpreted as 1 year
    result = pycmor.std_lib.timeaverage._frequency_from_approx_interval("365")
    assert result in ("YS", "1YS")  # Both formats are acceptable
    
    # Test that 365 days is interpreted as 1 year (explicit check)
    assert pycmor.std_lib.timeaverage._frequency_from_approx_interval("365") in ("YS", "1YS")
    
    # Test that 1095 days (3 years) is interpreted as 3 years
    assert pycmor.std_lib.timeaverage._frequency_from_approx_interval("1095") == "3YS"


def test__frequency_from_approx_interval_month():
    # Test that 30 days is interpreted as 1 month
    result = pycmor.std_lib.timeaverage._frequency_from_approx_interval("30")
    assert result in ("MS", "1MS")  # Both formats are acceptable
    
    # Test that 60 days is interpreted as 2 months
    assert pycmor.std_lib.timeaverage._frequency_from_approx_interval("60") == "2MS"


def test__frequency_from_approx_interval_day():
    assert pycmor.std_lib.timeaverage._frequency_from_approx_interval("1") in {
        "D",
        "1D",
    }  # One day


def test__frequency_from_approx_interval_hour():
    assert pycmor.std_lib.timeaverage._frequency_from_approx_interval("0.04167") in {
        "h",
        "1h",
    }  # Approximately one hour in days
    assert (
        pycmor.std_lib.timeaverage._frequency_from_approx_interval("0.08333") == "2h"
    )  # Approximately two hours in days
    assert (
        pycmor.std_lib.timeaverage._frequency_from_approx_interval("0.5") == "12h"
    )  # Half a day in hours


def test__frequency_from_approx_interval_minute():
    assert pycmor.std_lib.timeaverage._frequency_from_approx_interval("0.000694") in {
        "m",
        "1m",
    }  # Approximately one minute in days
    assert (
        pycmor.std_lib.timeaverage._frequency_from_approx_interval("0.001388") == "2m"
    )  # Approximately two minutes in days
    assert (
        pycmor.std_lib.timeaverage._frequency_from_approx_interval("0.020833") == "30m"
    )  # Approximately half an hour in minutes


def test__frequency_from_approx_interval_second():
    assert pycmor.std_lib.timeaverage._frequency_from_approx_interval("0.000011574") in {
        "s",
        "1s",
    }  # Approximately one second in days
    assert (
        pycmor.std_lib.timeaverage._frequency_from_approx_interval("0.00002314") == "2s"
    )  # Approximately two seconds in days
    assert not (
        pycmor.std_lib.timeaverage._frequency_from_approx_interval("0.000694") == "60s"
    )  # Approximately one minute in seconds, should give back min, since it can round up.


@pytest.mark.skip(reason="not supported.")
def test__frequency_from_approx_interval_millisecond():
    assert (
        pycmor.std_lib.timeaverage._frequency_from_approx_interval("1.1574e-8") == "ms"
    )  # Approximately one millisecond in days
    assert (
        pycmor.std_lib.timeaverage._frequency_from_approx_interval("2.3148e-8") == "2ms"
    )  # Approximately two milliseconds in days


def test__invalid_interval():
    with pytest.raises(ValueError):
        pycmor.std_lib.timeaverage._frequency_from_approx_interval("not_a_number")
