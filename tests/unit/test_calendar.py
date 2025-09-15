import numpy as np
import pytest
import xarray as xr

import pycmor.core.calendar
from pycmor.core.calendar import assign_time_axis, date_ranges_from_year_bounds


@pytest.fixture
def fake_multidim_data():
    np.random.seed(0)
    data = np.random.rand(10, 5, 5)
    da = xr.DataArray(data, dims=("time", "x", "y"))
    return da


def test_assign_time_axis_matching_length_multidim(fake_multidim_data):
    year_bounds = [[2000, 2009]]
    time_axis = date_ranges_from_year_bounds(year_bounds, freq="YE")
    result = assign_time_axis(fake_multidim_data, time_axis)
    assert "time" in result.coords
    assert len(result.time) == 10
    assert result.time[0].dt.year == 2000
    assert result.time[-1].dt.year == 2009


def test_assign_time_axis_mismatching_length_multidim(fake_multidim_data):
    year_bounds = [[2000, 2011]]
    time_axis = date_ranges_from_year_bounds(year_bounds, freq="YE")
    with pytest.raises(ValueError):
        assign_time_axis(fake_multidim_data, time_axis)


@pytest.fixture
def fake_multidim_data_diff_dims():
    np.random.seed(0)
    data = np.random.rand(10, 4, 6)
    da = xr.DataArray(data, dims=("time", "x", "y"))
    return da


def test_assign_time_axis_matching_length_multidim_diff_dims(
    fake_multidim_data_diff_dims,
):
    year_bounds = [[2000, 2009]]
    time_axis = date_ranges_from_year_bounds(year_bounds, freq="YE")
    result = assign_time_axis(fake_multidim_data_diff_dims, time_axis)
    assert "time" in result.coords
    assert len(result.time) == 10
    assert result.time[0].dt.year == 2000
    assert result.time[-1].dt.year == 2009


def test_assign_time_axis_mismatching_length_multidim_diff_dims(
    fake_multidim_data_diff_dims,
):
    year_bounds = [[2000, 2011]]
    time_axis = date_ranges_from_year_bounds(year_bounds, freq="YE")
    with pytest.raises(ValueError):
        assign_time_axis(fake_multidim_data_diff_dims, time_axis)


@pytest.fixture
def fake_data_three():
    np.random.seed(0)
    data1 = np.random.rand(10)
    data2 = np.random.rand(20)
    data3 = np.random.rand(30)
    da1 = xr.DataArray(data1, dims="time")
    da2 = xr.DataArray(data2, dims="time")
    da3 = xr.DataArray(data3, dims="time")
    return da1, da2, da3


def test_assign_time_axis_matching_length_three(fake_data_three):
    year_bounds1 = [[2000, 2009]]
    year_bounds2 = [[2000, 2019]]
    year_bounds3 = [[2000, 2029]]
    time_axis1 = date_ranges_from_year_bounds(year_bounds1, freq="YE")
    time_axis2 = date_ranges_from_year_bounds(year_bounds2, freq="YE")
    time_axis3 = date_ranges_from_year_bounds(year_bounds3, freq="YE")
    result1 = assign_time_axis(fake_data_three[0], time_axis1)
    result2 = assign_time_axis(fake_data_three[1], time_axis2)
    result3 = assign_time_axis(fake_data_three[2], time_axis3)
    assert "time" in result1.coords
    assert "time" in result2.coords
    assert "time" in result3.coords
    assert len(result1.time) == 10
    assert len(result2.time) == 20
    assert len(result3.time) == 30
    assert result1.time[0].dt.year == 2000
    assert result1.time[-1].dt.year == 2009
    assert result2.time[0].dt.year == 2000
    assert result2.time[-1].dt.year == 2019
    assert result3.time[0].dt.year == 2000
    assert result3.time[-1].dt.year == 2029


def test_assign_time_axis_mismatching_length_three(fake_data_three):
    year_bounds1 = [[2000, 2011]]
    year_bounds2 = [[2000, 2021]]
    year_bounds3 = [[2000, 2031]]
    time_axis1 = date_ranges_from_year_bounds(year_bounds1, freq="YE")
    time_axis2 = date_ranges_from_year_bounds(year_bounds2, freq="YE")
    time_axis3 = date_ranges_from_year_bounds(year_bounds3, freq="YE")
    with pytest.raises(ValueError):
        assign_time_axis(fake_data_three[0], time_axis1)
    with pytest.raises(ValueError):
        assign_time_axis(fake_data_three[1], time_axis2)
    with pytest.raises(ValueError):
        assign_time_axis(fake_data_three[2], time_axis3)


def test_simple_ranges_from_bounds():
    bounds = [(1, 5), (10, 15)]
    result = list(pycmor.core.calendar.simple_ranges_from_bounds(bounds))
    expected = [range(1, 6), range(10, 16)]
    assert result == expected


def test_single_range():
    bounds = [(1, 5)]
    result = pycmor.core.calendar.simple_ranges_from_bounds(bounds)
    expected = range(1, 6)
    assert result == expected


def test_single_range_single_element():
    bounds = [(3, 3)]
    result = pycmor.core.calendar.simple_ranges_from_bounds(bounds)
    expected = range(3, 4)
    assert result == expected


def test_single_range_negative():
    bounds = [(-5, -1)]
    result = pycmor.core.calendar.simple_ranges_from_bounds(bounds)
    expected = range(-5, 0)
    assert result == expected


def test_date_ranges_from_bounds():
    bounds = [("2020-01-01", "2020-01-31"), ("2020-02-01", "2020-02-29")]
    result = pycmor.core.calendar.date_ranges_from_bounds(bounds)
    expected = (
        xr.date_range(start="2020-01-01", end="2020-01-31", freq="ME"),
        xr.date_range(start="2020-02-01", end="2020-02-29", freq="ME"),
    )
    assert result == expected


def test_date_ranges_from_bounds_single_range():
    bounds = [("2020-01-01", "2020-12-31")]
    result = pycmor.core.calendar.date_ranges_from_bounds(bounds)
    expected = xr.date_range(start="2020-01-01", end="2020-12-31", freq="ME")
    assert (result == expected).all()


def test_year_bounds_major_digits_first_can_end_with_binning_digit():
    bounds = pycmor.core.calendar.year_bounds_major_digits(
        first=2700, last=2720, step=10, binning_digit=1
    )
    assert [[2700, 2700], [2701, 2710], [2711, 2720]] == bounds


def test_year_bounds_major_digits_can_start_1before_major_digit1():
    bounds = pycmor.core.calendar.year_bounds_major_digits(
        first=2050, last=2070, step=10, binning_digit=1
    )
    assert [[2050, 2050], [2051, 2060], [2061, 2070]] == bounds


def test_year_bounds_major_digits_can_have_no_complete_range():
    bounds = pycmor.core.calendar.year_bounds_major_digits(
        first=2050, last=2055, step=10, binning_digit=1
    )
    assert [[2050, 2050], [2051, 2055]] == bounds


def test_year_bounds_major_digits_can_start_3before_major_digit3():
    bounds = pycmor.core.calendar.year_bounds_major_digits(
        first=2050, last=2070, step=10, binning_digit=3
    )
    assert [[2050, 2052], [2053, 2062], [2063, 2070]] == bounds


def test_year_bounds_major_digits_can_start_9before_major_digit1():
    bounds = pycmor.core.calendar.year_bounds_major_digits(
        first=2042, last=2070, step=10, binning_digit=1
    )
    assert [[2042, 2050], [2051, 2060], [2061, 2070]] == bounds


def test_year_bounds_major_digits_can_start_1before_major_digit1_with_step20():
    bounds = pycmor.core.calendar.year_bounds_major_digits(
        first=2050, last=2080, step=20, binning_digit=1
    )
    assert [[2050, 2050], [2051, 2070], [2071, 2080]] == bounds


def test_year_bounds_major_digits_can_start_3before_major_digit3_with_step5():
    bounds = pycmor.core.calendar.year_bounds_major_digits(
        first=2050, last=2070, step=5, binning_digit=3
    )
    assert [
        [2050, 2052],
        [2053, 2057],
        [2058, 2062],
        [2063, 2067],
        [2068, 2070],
    ] == bounds


def test_year_bounds_major_digits_can_start_1before_major_digit1_with_step1():
    bounds = pycmor.core.calendar.year_bounds_major_digits(
        first=2050, last=2055, step=1, binning_digit=1
    )
    assert [
        [2050, 2050],
        [2051, 2051],
        [2052, 2052],
        [2053, 2053],
        [2054, 2054],
        [2055, 2055],
    ] == bounds
