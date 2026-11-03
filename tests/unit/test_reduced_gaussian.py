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


def test_bounds_like_ece2cmor3():
    da = _reduced()
    lat_b, lon_b = cs._reduced_gaussian_bounds(da["lat"].values, da["lon"].values)
    assert lat_b.shape == lon_b.shape == (24, 4)
    np.testing.assert_allclose(lat_b[0], [90, 90, 40, 40])  # first row: north edge at the pole
    np.testing.assert_allclose(lat_b[4], [40, 40, 0, 0])  # midpoints between rows
    np.testing.assert_allclose(lat_b[-1], [-40, -40, -90, -90])
    np.testing.assert_allclose(lon_b[0], [-45, 45, 45, -45])  # 4-cell row: 90 deg wide
    np.testing.assert_allclose(lon_b[4], [-22.5, 22.5, 22.5, -22.5])


def _needs_cdo():
    import shutil

    pytest.importorskip("pycdo")
    if shutil.which("cdo") is None:
        pytest.skip("cdo not on PATH")


def test_remap_constant_subset_with_fill(tmp_path):
    _needs_cdo()
    full = _reduced(np.full(24, 7.0), ntime=1)
    full.to_dataset().to_netcdf(tmp_path / "ref.nc")
    land = full.isel(cell=[4, 5, 6, 7, 8, 9])
    out = cs.remap_conservative(
        land, _Rule(remap_source_grid=str(tmp_path / "ref.nc"), remap_fill=7.0, remap_target="r8x4")
    )
    assert out.dims == ("time", "lat", "lon") and out.shape == (1, 4, 8)
    assert bool(np.all(out["lat"].diff("lat") < 0))
    np.testing.assert_allclose(out.values, 7.0, rtol=1e-6)


def test_remap_not_a_subset_rejected(tmp_path):
    _needs_cdo()
    full = _reduced(ntime=1)
    full.to_dataset().to_netcdf(tmp_path / "ref.nc")
    other = full.isel(cell=[4, 5]).assign_coords(lat=("cell", [10.0, 10.0]))
    with pytest.raises(ValueError):
        cs.remap_conservative(other, _Rule(remap_source_grid=str(tmp_path / "ref.nc"), remap_target="r8x4"))


def test_remap_dataset_keeps_time_encoding(tmp_path):
    _needs_cdo()
    full = _reduced(np.full(24, 3.0), ntime=2)
    full.to_dataset().to_netcdf(tmp_path / "ref.nc")
    ds = full.isel(cell=[4, 5, 6]).assign_coords(time=[0.0, 31.0]).to_dataset()
    ds["time"].encoding["units"] = "days since 1850-01-01"
    out = cs.remap_conservative(
        ds, _Rule(remap_source_grid=str(tmp_path / "ref.nc"), remap_fill=3.0, remap_target="r8x4")
    )
    assert isinstance(out, xr.Dataset) and out["tas"].dims == ("time", "lat", "lon")
    assert out["time"].encoding["units"] == "days since 1850-01-01"
    np.testing.assert_allclose(out["tas"].values, 3.0, rtol=1e-6)
