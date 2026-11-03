"""reduced_to_regular and daily_statistic_time (EC-Earth4 OIFS output)."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "examples"))
import custom_steps as cs  # noqa: E402


class _Rule:
    def __init__(self, **kw):
        self._d = kw

    def get(self, key, default=None):
        return self._d.get(key, default)


ROWS = (4, 8, 8, 4)  # cells per row, north to south
LATS = (60.0, 20.0, -20.0, -60.0)


def _reduced(values=None, ntime=2):
    lat = np.concatenate([[la] * n for la, n in zip(LATS, ROWS)])
    lon = np.concatenate([np.arange(n) * 360.0 / n for n in ROWS])
    if values is None:
        values = np.arange(lat.size, dtype="f8")
    x = np.broadcast_to(values, (ntime, lat.size)).copy()
    return xr.DataArray(
        x,
        dims=("time", "cell"),
        coords={"lat": ("cell", lat), "lon": ("cell", lon)},
        name="tas",
        attrs={"units": "K", "coordinates": "lat lon"},
    )


def test_shape_and_coords():
    out = cs.reduced_to_regular(_reduced(), _Rule())
    assert out.dims == ("time", "lat", "lon")
    assert out.shape == (2, 4, 8)
    np.testing.assert_allclose(out["lat"], LATS)
    np.testing.assert_allclose(out["lon"], np.arange(8) * 45.0)
    assert out.name == "tas" and out.attrs["units"] == "K"
    assert "coordinates" not in out.attrs


def test_full_rows_unchanged_short_rows_linear():
    out = cs.reduced_to_regular(_reduced(), _Rule()).isel(time=0).values
    np.testing.assert_array_equal(out[1], np.arange(4, 12))
    # 4-cell row at 45 deg spacing: halfway between neighbours, wraps at 360
    np.testing.assert_allclose(out[0], [0, 0.5, 1, 1.5, 2, 2.5, 3, 1.5])


def test_nearest_takes_existing_values():
    out = cs.reduced_to_regular(_reduced(), _Rule(regrid_nearest=True)).isel(time=0).values
    assert set(np.unique(out[0])) <= {0.0, 1.0, 2.0, 3.0}


def test_constant_field_stays_constant():
    out = cs.reduced_to_regular(_reduced(np.full(24, 7.0)), _Rule())
    assert np.all(out.values == 7.0)


def test_no_cell_dim_is_noop():
    da = xr.DataArray(np.zeros((2, 3)), dims=("lat", "lon"))
    assert cs.reduced_to_regular(da, _Rule()) is da


def test_rows_south_to_north_rejected():
    da = _reduced().isel(cell=slice(None, None, -1))
    with pytest.raises(ValueError):
        cs.reduced_to_regular(da, _Rule())


def _daily(hour):
    t = pd.date_range(f"1990-01-02 {hour:02d}:00", periods=3, freq="D")
    return xr.DataArray(np.arange(3.0), dims="time", coords={"time": t})


def test_daily_statistic_moves_into_day():
    out = cs.daily_statistic_time(_daily(0), _Rule(daily_statistic=True))
    assert list(out["time"].dt.day.values) == [1, 2, 3]
    assert bool((out["time"].dt.hour == 12).all())


def test_daily_statistic_off_or_not_midnight_is_noop():
    da = _daily(0)
    assert cs.daily_statistic_time(da, _Rule()) is da
    da = _daily(12)
    assert cs.daily_statistic_time(da, _Rule(daily_statistic=True)) is da
