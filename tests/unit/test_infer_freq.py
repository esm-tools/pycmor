import cftime
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pymor.core.infer_freq import (
    infer_frequency,
    is_resolution_fine_enough,
    log_frequency_check,
)
from pymor.core.time_utils import get_time_label, is_datetime_type


@pytest.fixture
def regular_monthly_time():
    return [cftime.Datetime360Day(2000, m, 15) for m in range(1, 5)]


@pytest.fixture
def irregular_time():
    return [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 1, 20),
        cftime.Datetime360Day(2000, 2, 15),
        cftime.Datetime360Day(2000, 3, 10),
    ]


@pytest.fixture
def short_time():
    return [cftime.Datetime360Day(2000, 1, 1)]


def test_infer_monthly_frequency(regular_monthly_time):
    freq = infer_frequency(regular_monthly_time)
    assert freq == "M"


def test_infer_irregular_time(irregular_time):
    freq, delta, _, exact, status = infer_frequency(
        irregular_time, return_metadata=True
    )
    assert freq is not None
    assert not exact
    assert status in ("irregular", "missing_steps")


def test_short_time_series(short_time):
    freq = infer_frequency(short_time)
    assert freq is None


def test_resolution_check_finer_than_month(regular_monthly_time):
    result = is_resolution_fine_enough(
        regular_monthly_time, target_approx_interval=30.5, calendar="360_day"
    )
    assert result["comparison_status"] == "finer"
    assert result["is_valid_for_resampling"]


def test_resolution_check_equal_to_month(regular_monthly_time):
    result = is_resolution_fine_enough(
        regular_monthly_time, target_approx_interval=30.0, calendar="360_day"
    )
    assert result["comparison_status"] in ("equal", "finer")
    assert result["is_valid_for_resampling"]


def test_resolution_check_too_sparse():
    times = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 4, 1),
        cftime.Datetime360Day(2000, 7, 1),
    ]
    result = is_resolution_fine_enough(
        times, target_approx_interval=30.4375, calendar="360_day"
    )
    assert result["comparison_status"] == "coarser"
    assert not result["is_valid_for_resampling"]


def test_accessor_on_dataarray(regular_monthly_time):
    da = xr.DataArray([1, 2, 3, 4], coords={"time": regular_monthly_time}, dims="time")
    result = da.timefreq.infer_frequency(log=False)
    assert result.frequency == "M"


def test_accessor_on_dataset(regular_monthly_time):
    da = xr.DataArray([1, 2, 3, 4], coords={"time": regular_monthly_time}, dims="time")
    ds = xr.Dataset({"tas": da})
    result = ds.timefreq.infer_frequency(log=False)
    assert result.frequency == "M"


def test_strict_mode_detection():
    # Intentionally skip one time step
    times = [cftime.Datetime360Day(2000, m, 15) for m in (1, 2, 4, 5)]
    result = is_resolution_fine_enough(
        times, target_approx_interval=30.0, calendar="360_day", strict=True
    )
    assert result["comparison_status"] == "missing_steps"
    assert not result["is_valid_for_resampling"]


def test_dataarray_resample_safe_pass(regular_monthly_time):
    da = xr.DataArray([1, 2, 3, 4], coords={"time": regular_monthly_time}, dims="time")

    # Should pass and return resampled array
    resampled = da.timefreq.resample_safe(
        freq_str="M", target_approx_interval=30.4375, calendar="360_day"
    )

    assert isinstance(resampled, xr.DataArray)
    assert "time" in resampled.dims


def test_dataset_resample_safe_pass(regular_monthly_time):
    da = xr.DataArray([1, 2, 3, 4], coords={"time": regular_monthly_time}, dims="time")
    ds = xr.Dataset({"pr": da})

    # Should pass and return resampled dataset
    resampled_ds = ds.timefreq.resample_safe(
        freq_str="M", target_approx_interval=30.4375, calendar="360_day"
    )

    assert isinstance(resampled_ds, xr.Dataset)
    assert "time" in resampled_ds.dims
    assert "pr" in resampled_ds.data_vars


def test_resample_safe_fails_on_coarse_resolution():
    # Coarser than monthly (e.g. quarterly)
    times = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 4, 1),
        cftime.Datetime360Day(2000, 7, 1),
    ]
    da = xr.DataArray([1, 2, 3], coords={"time": times}, dims="time")

    with pytest.raises(ValueError, match="time resolution too coarse"):
        da.timefreq.resample_safe(
            freq_str="M", target_approx_interval=30.4375, calendar="360_day"
        )


def test_resample_safe_with_mean(regular_monthly_time):
    da = xr.DataArray(
        [1.0, 2.0, 3.0, 4.0], coords={"time": regular_monthly_time}, dims="time"
    )

    # Should apply 'mean' over each monthly bin
    resampled = da.timefreq.resample_safe(
        freq_str="M", target_approx_interval=30.0, calendar="360_day", method="mean"
    )

    assert np.allclose(resampled.values, [1.0, 2.0, 3.0, 4.0])


def test_missing_steps_daily_gaps():
    """Test missing_steps detection for daily time series with gaps."""
    # Daily series with missing days 4, 5, 6
    times_with_gaps = [
        cftime.Datetime360Day(2000, 1, 1),  # Day 1
        cftime.Datetime360Day(2000, 1, 2),  # Day 2
        cftime.Datetime360Day(2000, 1, 3),  # Day 3
        # Missing days 4, 5, 6 (3-day gap!)
        cftime.Datetime360Day(2000, 1, 7),  # Day 7
        cftime.Datetime360Day(2000, 1, 8),  # Day 8
    ]

    result = infer_frequency(
        times_with_gaps, return_metadata=True, strict=True, calendar="360_day"
    )

    assert result.frequency == "D"
    assert result.status == "missing_steps"
    assert not result.is_exact
    assert result.step == 1


def test_missing_steps_weekly_gaps():
    """Test missing_steps detection for weekly time series with gaps."""
    # Weekly series with missing week 3
    times_weekly_gaps = [
        cftime.Datetime360Day(2000, 1, 1),  # Week 1
        cftime.Datetime360Day(2000, 1, 8),  # Week 2
        # Missing week 3 (Jan 15)
        cftime.Datetime360Day(2000, 1, 22),  # Week 4
        cftime.Datetime360Day(2000, 1, 29),  # Week 5
    ]

    result = infer_frequency(
        times_weekly_gaps, return_metadata=True, strict=True, calendar="360_day"
    )

    assert result.frequency == "7D"
    assert result.status == "missing_steps"
    assert not result.is_exact
    assert result.step == 7


def test_missing_steps_vs_irregular():
    """Test difference between missing_steps and irregular status."""
    # Irregular: varying intervals but no clear pattern
    times_irregular = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 1, 20),  # 19 days
        cftime.Datetime360Day(2000, 2, 15),  # 26 days
        cftime.Datetime360Day(2000, 3, 10),  # 24 days
    ]

    result_irregular = infer_frequency(
        times_irregular, return_metadata=True, strict=True, calendar="360_day"
    )

    # Should be irregular, not missing_steps
    assert result_irregular.status == "irregular"
    assert not result_irregular.is_exact

    # Missing steps: clear daily pattern with gaps
    times_missing = [
        cftime.Datetime360Day(2000, 1, 1),  # Day 1
        cftime.Datetime360Day(2000, 1, 2),  # Day 2
        cftime.Datetime360Day(2000, 1, 5),  # Day 5 (missing 3,4)
        cftime.Datetime360Day(2000, 1, 6),  # Day 6
    ]

    result_missing = infer_frequency(
        times_missing, return_metadata=True, strict=True, calendar="360_day"
    )

    # Should be missing_steps
    assert result_missing.status == "missing_steps"
    assert result_missing.frequency == "D"


def test_missing_steps_requires_strict_mode():
    """Test that missing_steps detection requires strict=True."""
    times_with_gaps = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 1, 2),
        cftime.Datetime360Day(2000, 1, 5),  # Gap: missing days 3,4
        cftime.Datetime360Day(2000, 1, 6),
    ]

    # Without strict mode: should be "irregular"
    result_non_strict = infer_frequency(
        times_with_gaps, return_metadata=True, strict=False, calendar="360_day"
    )

    assert result_non_strict.status == "irregular"

    # With strict mode: should be "missing_steps"
    result_strict = infer_frequency(
        times_with_gaps, return_metadata=True, strict=True, calendar="360_day"
    )

    assert result_strict.status == "missing_steps"


def test_consistent_is_exact_and_status():
    """Test that is_exact and status are consistent when strict=True detects irregularities."""
    # Time series with small offsets that pass std_delta test but fail strict individual delta test
    import numpy as np

    times_with_offsets = np.array(
        [
            "3007-02-01T00:00:00",  # Feb 1
            "3007-03-01T00:00:00",  # Mar 1 (28 days)
            "3007-04-02T00:00:00",  # Apr 2 (32 days) <- 1-day offset
            "3007-05-01T00:00:00",  # May 1 (29 days)
            "3007-06-01T00:00:00",  # Jun 1 (31 days)
            "3007-07-02T00:00:00",  # Jul 2 (31 days) <- 1-day offset
            "3007-08-01T00:00:00",  # Aug 1 (30 days)
        ],
        dtype="datetime64[s]",
    )

    # With strict=True: should detect irregularity and set is_exact=False
    result_strict = infer_frequency(
        times_with_offsets, return_metadata=True, strict=True
    )

    # Both status and is_exact should indicate irregularity
    assert result_strict.status == "irregular"
    assert not result_strict.is_exact  # Should be consistent with status
    assert result_strict.frequency == "M"

    # With strict=False: should be valid (less strict tolerance)
    result_non_strict = infer_frequency(
        times_with_offsets, return_metadata=True, strict=False
    )

    # Should be valid with non-strict mode
    assert result_non_strict.status == "valid"
    assert result_non_strict.is_exact
    assert result_non_strict.frequency == "M"


def test_is_datetime_type_numpy_datetime64():
    """Test is_datetime_type with numpy datetime64 arrays."""
    # Test various numpy datetime64 types
    dt_array_ns = np.array(["2000-01-01", "2000-01-02"], dtype="datetime64[ns]")
    dt_array_s = np.array(["2000-01-01", "2000-01-02"], dtype="datetime64[s]")
    dt_array_D = np.array(["2000-01-01", "2000-01-02"], dtype="datetime64[D]")

    assert is_datetime_type(dt_array_ns)
    assert is_datetime_type(dt_array_s)
    assert is_datetime_type(dt_array_D)


def test_is_datetime_type_cftime_objects():
    """Test is_datetime_type with cftime datetime objects."""
    # Test different cftime calendar types
    cftime_360day = np.array(
        [cftime.Datetime360Day(2000, 1, 1), cftime.Datetime360Day(2000, 1, 2)]
    )

    cftime_noleap = np.array(
        [cftime.DatetimeNoLeap(2000, 1, 1), cftime.DatetimeNoLeap(2000, 1, 2)]
    )

    cftime_gregorian = np.array(
        [cftime.DatetimeGregorian(2000, 1, 1), cftime.DatetimeGregorian(2000, 1, 2)]
    )

    assert is_datetime_type(cftime_360day)
    assert is_datetime_type(cftime_noleap)
    assert is_datetime_type(cftime_gregorian)


def test_is_datetime_type_non_datetime_arrays():
    """Test is_datetime_type with non-datetime arrays."""
    # Test various non-datetime array types
    int_array = np.array([1, 2, 3, 4])
    float_array = np.array([1.0, 2.0, 3.0, 4.0])
    string_array = np.array(["a", "b", "c"])
    bool_array = np.array([True, False, True])

    assert not is_datetime_type(int_array)
    assert not is_datetime_type(float_array)
    assert not is_datetime_type(string_array)
    assert not is_datetime_type(bool_array)


def test_is_datetime_type_empty_arrays():
    """Test is_datetime_type with empty arrays."""
    # Empty datetime64 array
    empty_dt = np.array([], dtype="datetime64[ns]")
    # Empty regular array
    empty_int = np.array([], dtype=int)

    # After fixing is_cftime_type, empty arrays should be handled gracefully
    # Empty datetime64 array should return True (dtype is datetime64)
    assert is_datetime_type(empty_dt)

    # Empty integer array should return False (dtype is not datetime)
    assert not is_datetime_type(empty_int)


def test_is_datetime_type_single_element_arrays():
    """Test is_datetime_type with single-element arrays."""
    # Single datetime64 element
    single_dt = np.array(["2000-01-01"], dtype="datetime64[ns]")
    single_cftime = np.array([cftime.Datetime360Day(2000, 1, 1)])
    single_int = np.array([42])

    assert is_datetime_type(single_dt)
    assert is_datetime_type(single_cftime)
    assert not is_datetime_type(single_int)


# ===== COVERAGE IMPROVEMENT TESTS =====


def test_short_time_series_with_logging(capsys):
    """Test logging behavior for short time series."""
    short_times = [cftime.Datetime360Day(2000, 1, 1)]
    result = infer_frequency(short_times, log=True, return_metadata=True)
    assert result.status == "too_short"
    assert result.frequency is None

    # Check that logging occurred
    captured = capsys.readouterr()
    assert "too_short" in captured.out or "Time Series" in captured.out


def test_empty_time_series():
    """Test handling of empty time series."""
    empty_times = []
    result = infer_frequency(empty_times, return_metadata=True)
    assert result.status == "too_short"  # Empty array is treated as too short
    assert result.frequency is None


def test_single_time_point():
    """Test handling of single time point."""
    single_time = [cftime.Datetime360Day(2000, 1, 1)]
    result = infer_frequency(single_time, return_metadata=True)
    assert result.status == "too_short"
    assert result.frequency is None


def test_cftime_ordinal_fallback():
    """Test fallback ordinal calculation for cftime objects."""
    # Create very early dates that might cause issues with toordinal
    times = [
        cftime.DatetimeNoLeap(1, 1, 1),
        cftime.DatetimeNoLeap(1, 1, 2),
        cftime.DatetimeNoLeap(1, 1, 3),
    ]
    # Should handle any ordinal conversion issues gracefully
    result = infer_frequency(times, return_metadata=True)
    assert result.frequency is not None  # Should still detect daily frequency


def test_mixed_calendar_types():
    """Test frequency inference with different calendar types."""
    # Test 360-day calendar
    times_360 = [cftime.Datetime360Day(2000, m, 15) for m in range(1, 5)]
    result_360 = infer_frequency(times_360, calendar="360_day", return_metadata=True)
    assert result_360.frequency == "M"

    # Test no-leap calendar
    times_noleap = [cftime.DatetimeNoLeap(2000, m, 15) for m in range(1, 5)]
    result_noleap = infer_frequency(
        times_noleap, calendar="noleap", return_metadata=True
    )
    assert result_noleap.frequency == "M"


def test_accessor_no_datetime_coord_error():
    """Test error when no datetime coordinate is found in accessor."""
    # DataArray with no datetime coordinates
    da = xr.DataArray([1, 2, 3], coords={"x": [1, 2, 3]}, dims=["x"])

    with pytest.raises(ValueError, match="No datetime coordinate found"):
        da.timefreq.infer_frequency()


def test_accessor_invalid_manual_time_dim():
    """Test behavior when manually specified time_dim doesn't exist."""
    da = xr.DataArray([1, 2, 3], coords={"x": [1, 2, 3]}, dims=["x"])

    # When time_dim doesn't exist, it uses the coordinate values [1, 2, 3] which
    # get converted to ordinals with zero deltas, hence "all_duplicates"
    result = da.timefreq.infer_frequency(time_dim="nonexistent")
    assert result.status == "all_duplicates"


def test_dataset_accessor_no_datetime_coord_error():
    """Test error when no datetime coordinate is found in dataset accessor."""
    ds = xr.Dataset({"data": (["x"], [1, 2, 3])}, coords={"x": [1, 2, 3]})

    with pytest.raises(ValueError, match="No datetime coordinate found"):
        ds.timefreq.infer_frequency()


def test_dataset_accessor_invalid_manual_time_dim():
    """Test error when manually specified time_dim doesn't exist in dataset."""
    ds = xr.Dataset({"data": (["x"], [1, 2, 3])}, coords={"x": [1, 2, 3]})

    with pytest.raises(ValueError, match="Time dimension 'nonexistent' not found"):
        ds.timefreq.infer_frequency(time_dim="nonexistent")


def test_irregular_time_series_logging(capsys):
    """Test logging for irregular time series."""
    irregular_times = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 1, 20),  # 19 days
        cftime.Datetime360Day(2000, 2, 15),  # 26 days
        cftime.Datetime360Day(2000, 3, 10),  # 24 days
    ]

    result = infer_frequency(
        irregular_times, log=True, strict=True, return_metadata=True
    )
    assert result.status == "irregular"

    # Check that logging occurred
    captured = capsys.readouterr()
    assert "irregular" in captured.out.lower() or "Freq Check" in captured.out


def test_very_short_time_series_edge_cases():
    """Test edge cases with very short time series."""
    # Test with exactly 2 points
    two_points = [cftime.Datetime360Day(2000, 1, 1), cftime.Datetime360Day(2000, 1, 2)]
    result = infer_frequency(two_points, return_metadata=True)
    assert result.frequency == "D"
    assert result.status == "valid"


def test_numpy_datetime64_with_different_units():
    """Test numpy datetime64 arrays with different time units."""
    # Test with nanosecond precision
    times_ns = np.array(
        ["2000-01-01", "2000-01-02", "2000-01-03"], dtype="datetime64[ns]"
    )
    result_ns = infer_frequency(times_ns, return_metadata=True)
    assert result_ns.frequency == "D"

    # Test with second precision
    times_s = np.array(
        ["2000-01-01", "2000-01-02", "2000-01-03"], dtype="datetime64[s]"
    )
    result_s = infer_frequency(times_s, return_metadata=True)
    assert result_s.frequency == "D"


def test_resample_safe_error_paths():
    """Test error paths in resample_safe methods."""
    # Create a coarse time series (quarterly)
    coarse_times = pd.date_range("2000-01-01", periods=4, freq="QS")
    da = xr.DataArray([1, 2, 3, 4], coords={"time": coarse_times}, dims=["time"])

    # Should raise error when trying to resample to finer resolution
    with pytest.raises(ValueError, match="time resolution too coarse"):
        da.timefreq.resample_safe(
            freq_str="M", target_approx_interval=30.4375  # Monthly interval
        )


def test_different_strict_mode_behaviors():
    """Test different behaviors with strict mode on/off."""
    # Time series with larger irregularities to ensure detection
    times_with_offsets = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 1, 20),  # 19 days
        cftime.Datetime360Day(2000, 2, 15),  # 26 days
        cftime.Datetime360Day(2000, 3, 10),  # 24 days
    ]

    # Non-strict mode might still detect irregularity for very irregular data
    result_non_strict = infer_frequency(
        times_with_offsets, strict=False, return_metadata=True
    )
    # Just check that we get a result
    assert result_non_strict.status in ["valid", "irregular"]

    # Strict mode should detect irregularity
    result_strict = infer_frequency(
        times_with_offsets, strict=True, return_metadata=True
    )
    assert result_strict.status in ["irregular", "missing_steps"]


def test_log_frequency_check_function():
    """Test the log_frequency_check function directly."""

    # Test different scenarios
    log_frequency_check("Test Series", "D", 1.0, 1, True, "valid", False)
    log_frequency_check("Test Series", None, None, None, False, "too_short", True)
    log_frequency_check("Test Series", "M", 30.0, 1, False, "irregular", True)


def test_pandas_datetime_index_input():
    """Test with pandas DatetimeIndex input."""
    # Test with pandas DatetimeIndex
    times_index = pd.date_range("2000-01-01", periods=5, freq="D")
    result = infer_frequency(times_index, return_metadata=True)
    assert result.frequency == "D"
    assert result.status == "valid"


def test_get_time_label_dataset_with_time_coord():
    """Test get_time_label with Dataset containing 'time' coordinate."""
    # Create dataset with time coordinate
    time_coord = pd.date_range("2000-01-01", periods=10)
    ds = xr.Dataset(
        {"temperature": (["time"], np.random.rand(10))}, coords={"time": time_coord}
    )

    result = get_time_label(ds)
    assert result == "time"


def test_get_time_label_dataset_with_custom_time_coord():
    """Test get_time_label with Dataset containing custom time coordinate name."""
    # Create dataset with 'T' as time coordinate
    time_coord = pd.date_range("2000-01-01", periods=5)
    ds = xr.Dataset({"data": (["T"], np.random.rand(5))}, coords={"T": time_coord})

    result = get_time_label(ds)
    assert result == "T"


def test_get_time_label_dataarray_with_time_coord():
    """Test get_time_label with DataArray containing time coordinate."""
    # Create DataArray with time coordinate
    time_coord = pd.date_range("2000-01-01", periods=8)
    da = xr.DataArray(np.random.rand(8), coords={"time": time_coord}, dims=["time"])

    result = get_time_label(da)
    assert result == "time"


def test_get_time_label_dataarray_with_custom_time_coord():
    """Test get_time_label with DataArray containing custom time coordinate name."""
    # Create DataArray with 'T' as time coordinate
    time_coord = pd.date_range("2000-01-01", periods=6)
    da = xr.DataArray(np.random.rand(6), coords={"T": time_coord}, dims=["T"])

    result = get_time_label(da)
    assert result == "T"


def test_get_time_label_cftime_coordinates():
    """Test get_time_label with cftime datetime coordinates."""
    # Create dataset with cftime coordinates
    cftime_coords = [cftime.Datetime360Day(2000, m, 15) for m in range(1, 6)]
    ds = xr.Dataset(
        {"temperature": (["time"], np.random.rand(5))}, coords={"time": cftime_coords}
    )

    result = get_time_label(ds)
    assert result == "time"


def test_get_time_label_no_datetime_coords():
    """Test get_time_label with Dataset containing no datetime coordinates."""
    # Create dataset with only non-datetime coordinates
    ds = xr.Dataset(
        {"data": (["x", "y"], np.random.rand(3, 4))},
        coords={"x": [1, 2, 3], "y": [10, 20, 30, 40]},
    )

    result = get_time_label(ds)
    assert result is None


def test_get_time_label_dataset_with_non_datetime_time_coord():
    """Test get_time_label with Dataset where 'time' coord is not datetime."""
    # Create dataset with 'time' coordinate that's not datetime
    ds = xr.Dataset(
        {"data": (["time"], np.random.rand(5))}, coords={"time": [1, 2, 3, 4, 5]}
    )

    result = get_time_label(ds)
    assert result is None


def test_get_time_label_multiple_datetime_coords():
    """Test get_time_label with multiple datetime coordinates."""
    # Create dataset with multiple datetime coordinates
    # The function uses appendleft(), so the last processed coord gets priority
    time1 = pd.date_range("2000-01-01", periods=3)
    time2 = pd.date_range("2001-01-01", periods=4)

    ds = xr.Dataset(
        {"data": (["time1"], np.random.rand(3))},
        coords={
            "time1": time1,
            "time2": time2,  # This coordinate is not used by any data variable
        },
    )

    result = get_time_label(ds)
    # The refined implementation correctly prioritizes coordinates used by data variables
    # time1 is used by the 'data' variable, time2 is not used by any data variable
    assert result == "time1"


def test_get_time_label_datetime_coord_not_used_by_datavar():
    """Test get_time_label when datetime coord exists but not used by data variables."""
    # Create dataset where datetime coord exists but no data variable uses it
    time_coord = pd.date_range("2000-01-01", periods=5)
    ds = xr.Dataset(
        {"data": (["x"], np.random.rand(3))},
        coords={"time": time_coord, "x": [1, 2, 3]},  # Exists but not used by 'data'
    )

    result = get_time_label(ds)
    assert result is None


def test_get_time_label_scalar_datetime_coord():
    """Test get_time_label with scalar datetime coordinate (no dimensions)."""
    # Create dataset with scalar datetime coordinate
    ds = xr.Dataset(
        {"data": (["x"], np.random.rand(3))},
        coords={
            "time": pd.Timestamp("2000-01-01"),  # Scalar coordinate
            "x": [1, 2, 3],
        },
    )

    result = get_time_label(ds)
    assert result is None


# Tests for TimeFrequencyAccessor.check_resolution
def test_dataarray_check_resolution_with_auto_detection():
    """Test DataArray check_resolution with automatic time dimension detection."""
    # Create monthly time series
    times = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 2, 1),
        cftime.Datetime360Day(2000, 3, 1),
    ]
    da = xr.DataArray([1, 2, 3], coords={"time": times}, dims="time")

    # Test with automatic detection (should work)
    result = da.timefreq.check_resolution(target_approx_interval=30.0, log=False)

    assert "inferred_interval" in result
    assert "comparison_status" in result
    assert "is_valid_for_resampling" in result
    assert result["comparison_status"] in ["equal", "finer"]
    assert result["is_valid_for_resampling"]


def test_dataarray_check_resolution_with_manual_time_dim():
    """Test DataArray check_resolution with manual time dimension specification."""
    # Create monthly time series with custom time dimension name
    times = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 2, 1),
        cftime.Datetime360Day(2000, 3, 1),
    ]
    da = xr.DataArray([1, 2, 3], coords={"T": times}, dims="T")

    # Test with manual specification
    result = da.timefreq.check_resolution(
        target_approx_interval=30.0, time_dim="T", log=False
    )

    assert "inferred_interval" in result
    assert "comparison_status" in result
    assert "is_valid_for_resampling" in result
    assert result["comparison_status"] in ["equal", "finer"]
    assert result["is_valid_for_resampling"]


def test_dataarray_check_resolution_coarse_data():
    """Test DataArray check_resolution with coarse resolution data."""
    # Create quarterly time series (coarser than monthly)
    times = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 4, 1),
        cftime.Datetime360Day(2000, 7, 1),
    ]
    da = xr.DataArray([1, 2, 3], coords={"time": times}, dims="time")

    # Test against monthly target (should be too coarse)
    result = da.timefreq.check_resolution(target_approx_interval=30.0, log=False)

    assert result["comparison_status"] == "coarser"
    assert not result["is_valid_for_resampling"]


def test_dataarray_check_resolution_no_datetime_coord_error():
    """Test DataArray check_resolution error when no datetime coordinate found."""
    # Create DataArray without datetime coordinates
    da = xr.DataArray([1, 2, 3], coords={"x": [1, 2, 3]}, dims="x")

    # Should raise error with automatic detection
    with pytest.raises(ValueError, match="No datetime coordinate found"):
        da.timefreq.check_resolution(target_approx_interval=30.0)


# Tests for DatasetFrequencyAccessor.check_resolution
def test_dataset_check_resolution_with_auto_detection():
    """Test Dataset check_resolution with automatic time dimension detection."""
    # Create monthly time series
    times = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 2, 1),
        cftime.Datetime360Day(2000, 3, 1),
    ]
    ds = xr.Dataset({"temp": (["time"], [20, 21, 22])}, coords={"time": times})

    # Test with automatic detection (should work)
    result = ds.timefreq.check_resolution(target_approx_interval=30.0, log=False)

    assert "inferred_interval" in result
    assert "comparison_status" in result
    assert "is_valid_for_resampling" in result
    assert result["comparison_status"] in ["equal", "finer"]
    assert result["is_valid_for_resampling"]


def test_dataset_check_resolution_with_manual_time_dim():
    """Test Dataset check_resolution with manual time dimension specification."""
    # Create monthly time series with custom time dimension name
    times = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 2, 1),
        cftime.Datetime360Day(2000, 3, 1),
    ]
    ds = xr.Dataset({"temp": (["T"], [20, 21, 22])}, coords={"T": times})

    # Test with manual specification
    result = ds.timefreq.check_resolution(
        target_approx_interval=30.0, time_dim="T", log=False
    )

    assert "inferred_interval" in result
    assert "comparison_status" in result
    assert "is_valid_for_resampling" in result
    assert result["comparison_status"] in ["equal", "finer"]
    assert result["is_valid_for_resampling"]


def test_dataset_check_resolution_coarse_data():
    """Test Dataset check_resolution with coarse resolution data."""
    # Create quarterly time series (coarser than monthly)
    times = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 4, 1),
        cftime.Datetime360Day(2000, 7, 1),
    ]
    ds = xr.Dataset({"temp": (["time"], [20, 21, 22])}, coords={"time": times})

    # Test against monthly target (should be too coarse)
    result = ds.timefreq.check_resolution(target_approx_interval=30.0, log=False)

    assert result["comparison_status"] == "coarser"
    assert not result["is_valid_for_resampling"]


def test_dataset_check_resolution_no_datetime_coord_error():
    """Test Dataset check_resolution error when no datetime coordinate found."""
    # Create Dataset without datetime coordinates
    ds = xr.Dataset({"temp": (["x"], [20, 21, 22])}, coords={"x": [1, 2, 3]})

    # Should raise error with automatic detection
    with pytest.raises(ValueError, match="No datetime coordinate found"):
        ds.timefreq.check_resolution(target_approx_interval=30.0)


def test_dataset_check_resolution_invalid_time_dim_error():
    """Test Dataset check_resolution error when specified time_dim not found."""
    times = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 2, 1),
        cftime.Datetime360Day(2000, 3, 1),
    ]
    ds = xr.Dataset({"temp": (["time"], [20, 21, 22])}, coords={"time": times})

    # Should raise error when time_dim doesn't exist
    with pytest.raises(ValueError, match="Time dimension 'nonexistent' not found"):
        ds.timefreq.check_resolution(
            target_approx_interval=30.0, time_dim="nonexistent"
        )


# Tests for different calendar types and modes
def test_check_resolution_with_different_calendars():
    """Test check_resolution with different calendar types."""
    # Test with noleap calendar
    times_noleap = [
        cftime.DatetimeNoLeap(2000, 1, 1),
        cftime.DatetimeNoLeap(2000, 2, 1),
        cftime.DatetimeNoLeap(2000, 3, 1),
    ]
    da_noleap = xr.DataArray([1, 2, 3], coords={"time": times_noleap}, dims="time")

    result_noleap = da_noleap.timefreq.check_resolution(
        target_approx_interval=31.0, calendar="noleap", log=False
    )

    assert "inferred_interval" in result_noleap
    # Just check that we get a result - the exact validity depends on the inferred interval
    assert "is_valid_for_resampling" in result_noleap

    # Test with 360_day calendar
    times_360 = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 2, 1),
        cftime.Datetime360Day(2000, 3, 1),
    ]
    da_360 = xr.DataArray([1, 2, 3], coords={"time": times_360}, dims="time")

    result_360 = da_360.timefreq.check_resolution(
        target_approx_interval=30.0, calendar="360_day", log=False
    )

    assert "inferred_interval" in result_360
    assert result_360["is_valid_for_resampling"]


def test_check_resolution_with_strict_mode():
    """Test check_resolution with strict mode enabled."""
    # Create slightly irregular monthly data
    times = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 2, 2),  # One day off
        cftime.Datetime360Day(2000, 3, 1),
    ]
    da = xr.DataArray([1, 2, 3], coords={"time": times}, dims="time")

    # Test with strict=True
    result_strict = da.timefreq.check_resolution(
        target_approx_interval=30.0, strict=True, log=False
    )

    # Test with strict=False
    result_non_strict = da.timefreq.check_resolution(
        target_approx_interval=30.0, strict=False, log=False
    )

    # Both should have results, but strict mode might be more restrictive
    assert "inferred_interval" in result_strict
    assert "inferred_interval" in result_non_strict
    assert "status" in result_strict
    assert "status" in result_non_strict


def test_infer_freq_with_missing_month():
    """Test that infer_frequency can handle a single missing month."""
    # Monthly data, but March is missing
    times = pd.to_datetime(["2000-01-31", "2000-02-29", "2000-04-30"])
    freq = infer_frequency(times)
    assert freq == "M", f"Expected 'M', but got {freq}"


def test_check_resolution_with_strict_mode():
    """Test check_resolution with strict mode enabled."""
    # Create slightly irregular monthly data
    times = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 2, 2),  # One day off
        cftime.Datetime360Day(2000, 3, 1),
    ]
    da = xr.DataArray([1, 2, 3], coords={"time": times}, dims="time")

    # Test with strict=True
    result = da.timefreq.check_resolution(
        target_approx_interval=30.0, calendar="360_day", strict=True
    )

    assert result["status"] == "irregular"
    assert not result["is_valid_for_resampling"]


def test_check_resolution_with_pandas_datetime():
    """Test check_resolution with pandas datetime objects."""
    # Create monthly time series with pandas datetime
    times = pd.date_range("2000-01-01", periods=3, freq="MS")
    da = xr.DataArray([1, 2, 3], coords={"time": times}, dims="time")

    # Test with automatic detection
    result = da.timefreq.check_resolution(target_approx_interval=31.0, log=False)

    assert "inferred_interval" in result
    assert "comparison_status" in result
    assert "is_valid_for_resampling" in result
    # Just check that we get a result - the exact validity depends on the inferred interval
    assert isinstance(result["is_valid_for_resampling"], bool)


def test_check_resolution_tolerance_parameter():
    """Test check_resolution with different tolerance values."""
    times = [
        cftime.Datetime360Day(2000, 1, 1),
        cftime.Datetime360Day(2000, 2, 1),
        cftime.Datetime360Day(2000, 3, 1),
    ]
    da = xr.DataArray([1, 2, 3], coords={"time": times}, dims="time")

    # Test with tight tolerance
    result_tight = da.timefreq.check_resolution(
        target_approx_interval=30.0, tolerance=0.001, log=False
    )

    # Test with loose tolerance
    result_loose = da.timefreq.check_resolution(
        target_approx_interval=30.0, tolerance=1.0, log=False
    )

    # Both should have results
    assert "inferred_interval" in result_tight
    assert "inferred_interval" in result_loose
    assert result_tight["is_valid_for_resampling"]
    assert result_loose["is_valid_for_resampling"]


def test_check_resolution_return_format():
    """Test that check_resolution returns the expected dictionary format."""
    times = pd.date_range("2000-01-01", periods=12, freq="M")
    
    result = is_resolution_fine_enough(times, target_approx_interval=30.0, log=False)
    
    # Check that all expected keys are present
    expected_keys = {"inferred_interval", "comparison_status", "is_valid_for_resampling", "status"}
    assert set(result.keys()) >= expected_keys
    
    # Check types
    assert isinstance(result["inferred_interval"], (float, type(None)))
    assert isinstance(result["comparison_status"], str)
    assert isinstance(result["is_valid_for_resampling"], bool)
    assert isinstance(result["status"], str)


def test_infer_frequency_with_duplicates():
    """Test that infer_frequency correctly handles duplicate timestamps."""
    import cftime
    
    # Test case 1: Monthly data with duplicates should return 'M'
    monthly_with_duplicates = [
        cftime.Datetime360Day(2000, 1, 16, 0, 0, 0, 0, has_year_zero=True),
        cftime.Datetime360Day(2000, 1, 16, 0, 0, 0, 0, has_year_zero=True),  # duplicate
        cftime.Datetime360Day(2000, 2, 16, 0, 0, 0, 0, has_year_zero=True),
        cftime.Datetime360Day(2000, 2, 16, 0, 0, 0, 0, has_year_zero=True),  # duplicate
        cftime.Datetime360Day(2000, 3, 16, 0, 0, 0, 0, has_year_zero=True)
    ]
    
    result = infer_frequency(monthly_with_duplicates, return_metadata=True)
    assert result.frequency == "M"
    assert result.delta_days == 30.0
    assert result.status == "valid"
    
    # Test case 2: Daily data with duplicates should return 'D'
    daily_with_duplicates = [
        cftime.Datetime360Day(2000, 1, 1, 0, 0, 0, 0, has_year_zero=True),
        cftime.Datetime360Day(2000, 1, 1, 0, 0, 0, 0, has_year_zero=True),  # duplicate
        cftime.Datetime360Day(2000, 1, 2, 0, 0, 0, 0, has_year_zero=True),
        cftime.Datetime360Day(2000, 1, 3, 0, 0, 0, 0, has_year_zero=True),
        cftime.Datetime360Day(2000, 1, 3, 0, 0, 0, 0, has_year_zero=True),  # duplicate
        cftime.Datetime360Day(2000, 1, 4, 0, 0, 0, 0, has_year_zero=True)
    ]
    
    result = infer_frequency(daily_with_duplicates, return_metadata=True)
    assert result.frequency == "D"
    assert result.delta_days == 1.0
    assert result.status == "valid"
    
    # Test case 3: All duplicates should return 'all_duplicates'
    all_duplicates = [
        cftime.Datetime360Day(2000, 1, 16, 0, 0, 0, 0, has_year_zero=True),
        cftime.Datetime360Day(2000, 1, 16, 0, 0, 0, 0, has_year_zero=True),
        cftime.Datetime360Day(2000, 1, 16, 0, 0, 0, 0, has_year_zero=True)
    ]
    
    result = infer_frequency(all_duplicates, return_metadata=True)
    assert result.frequency is None
    assert result.delta_days == 0.0
    assert result.status == "all_duplicates"
