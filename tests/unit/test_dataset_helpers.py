import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pycmor.std_lib.dataset_helpers import (
    freq_is_coarser_than_data,
    get_time_label,
    has_time_axis,
    is_datetime_type,
    needs_resampling,
)


def test_no_resampling_required_when_data_timespan_is_less_than_target_timespan():
    t = pd.date_range("2020-01-01 1:00:00", "2020-02-28 1:00:00", freq="D")
    da = xr.DataArray(np.ones(t.size), coords={"time": t})
    timespan = "6ME"
    assert needs_resampling(da, timespan) is False


def test_needs_resampling_when_target_timespan_is_lower_than_data_timespan():
    t = pd.date_range("2020-01-01 1:00:00", "2020-02-28 1:00:00", freq="D")
    da = xr.DataArray(np.ones(t.size), coords={"time": t})
    timespan = "ME"
    assert needs_resampling(da, timespan) is True


def test_no_resampling_required_with_single_timestamp_data():
    da = xr.DataArray(10, coords={"time": pd.Timestamp.now()}, name="t")
    timespan = "1MS"
    assert needs_resampling(da, timespan) is False


def test_no_resampling_required_when_target_timespan_is_None():
    t = pd.date_range("2020-01-01 1:00:00", "2020-02-28 1:00:00", freq="D")
    da = xr.DataArray(np.ones(t.size), coords={"time": t})
    timespan = None
    assert needs_resampling(da, timespan) is False


def test_is_datetime_type_is_true_for_cftime():
    dates = xr.cftime_range(start="2001", periods=24, freq="MS", calendar="noleap")
    da_nl = xr.DataArray(np.arange(24), coords=[dates], dims=["time"], name="foo")
    assert is_datetime_type(da_nl.time.data) is True


def test_is_datetime_type_is_true_for_numpy_datetime64():
    t = pd.date_range("2020-01-01 1:00:00", periods=50, freq="6h")
    da = xr.DataArray(np.ones(t.size), coords={"time": t}, name="foo")
    assert is_datetime_type(da.time) is True


def test_has_time_axis_not_true_when_no_valid_time_dim_exists():
    da = xr.DataArray(
        10,
        coords={"time": 1},
        dims=[
            "time",
        ],
        name="notime",
    )
    assert has_time_axis(da) is False


def test_has_time_axis_is_true_with_time_as_scalar_coordinate():
    da = xr.DataArray(
        [10],
        coords={
            "time": (
                [
                    "time",
                ],
                [
                    pd.Timestamp.now(),
                ],
            )
        },
        name="t",
    )
    assert has_time_axis(da) is True


def test_has_time_axis_recognizes_T_as_time_dimension():
    t = pd.date_range("2020-01-01 1:00:00", periods=50, freq="6h")
    da = xr.DataArray(
        np.ones(t.size),
        coords={"T": t},
        dims=[
            "T",
        ],
        name="foo",
    )
    assert has_time_axis(da) is True


def test_get_time_label_can_recognize_time_dimension_named_time():
    np.random.seed(0)
    temperature = 15 + 8 * np.random.randn(2, 2, 3)
    lon = [[-99.83, -99.32], [-99.79, -99.23]]
    lat = [[42.25, 42.21], [42.63, 42.59]]
    time = pd.date_range("2014-09-06", periods=3)
    reference_time = pd.Timestamp("2014-09-05")
    da = xr.DataArray(
        data=temperature,
        dims=["x", "y", "time"],
        coords=dict(
            lon=(["x", "y"], lon),
            lat=(["x", "y"], lat),
            reference_time=reference_time,
            time=time,
        ),
        attrs=dict(
            description="Ambient temperature.",
            units="degC",
        ),
    )
    assert get_time_label(da) == "time"


def test_get_time_label_can_recognize_scalar_time_dimension():
    s = xr.DataArray(
        1,
        coords=dict(
            T=(
                [
                    "time",
                ],
                [pd.Timestamp.now()],
            )
        ),
        dims=[
            "time",
        ],
    )
    assert get_time_label(s) == "T"


def test_get_time_label_is_None_for_missing_time_dimension():
    np.random.seed(0)
    temperature = 15 + 8 * np.random.randn(2, 2, 3)
    lon = [[-99.83, -99.32], [-99.79, -99.23]]
    lat = [[42.25, 42.21], [42.63, 42.59]]
    # time = pd.date_range("2014-09-06", periods=3)
    reference_time = pd.Timestamp("2014-09-05")
    da = xr.DataArray(
        data=temperature,
        dims=["x", "y", "time"],
        coords=dict(
            lon=(["x", "y"], lon),
            lat=(["x", "y"], lat),
            reference_time=reference_time,
            # Removing time dimension on purpose.
            # get_time_label should not consider reference_time.
            # time=time,
        ),
        attrs=dict(
            description="Ambient temperature.",
            units="degC",
        ),
    )
    assert get_time_label(da) is None


def test_get_time_label_can_recognize_time_label_even_if_dimension_is_named_differently():
    b = xr.DataArray(
        [1, 2, 3],
        coords={
            "time": (
                [
                    "ncells",
                ],
                pd.date_range(pd.Timestamp.now(), periods=3, freq="h"),
            )
        },
        dims=[
            "ncells",
        ],
    )
    assert get_time_label(b) == "time"


@pytest.fixture
def daily_dataset():
    time = pd.date_range("2000-01-01", periods=10, freq="D")
    return xr.Dataset({"temp": ("time", np.random.rand(10))}, coords={"time": time})


def test_month_is_coarser_than_day(daily_dataset):
    assert freq_is_coarser_than_data("M", daily_dataset) is True


def test_hour_is_not_coarser_than_day(daily_dataset):
    assert freq_is_coarser_than_data("6H", daily_dataset) is False


def test_same_freq_is_not_coarser(daily_dataset):
    assert freq_is_coarser_than_data("D", daily_dataset) is False


def test_year_is_coarser_than_day(daily_dataset):
    assert freq_is_coarser_than_data("A", daily_dataset) is True


def test_unknown_frequency_raises():
    # Irregular time steps → infer_freq returns None
    time = pd.to_datetime(["2000-01-01", "2000-01-02", "2000-01-04"])
    ds = xr.Dataset({"temp": ("time", np.random.rand(3))}, coords={"time": time})
    with pytest.raises(ValueError, match="Could not infer frequency"):
        freq_is_coarser_than_data("D", ds)
