"""orca_grid, orca_vertices, orca_cell_area (EC-Earth4 NEMO and SI3 output)."""

import sys
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "examples"))
import custom_steps as cs  # noqa: E402

NY, NX, DX, DY = 6, 8, 45.0, 25.0


class _Rule:
    def __init__(self, **kw):
        self._d = kw
        self.model_variable = kw.get("model_variable")

    def get(self, key, default=None):
        return self._d.get(key, default)


def _points(ox, oy):
    i = np.arange(NX)[None, :]
    j = np.arange(NY)[:, None]
    lon = np.broadcast_to((i + ox) * DX - 180.0, (NY, NX)).astype("f8")
    lat = np.broadcast_to(-75.0 + (j + oy) * DY, (NY, NX)).astype("f8")
    return lon, lat


@pytest.fixture
def domain(tmp_path):
    v = {}
    for p, (ox, oy) in {"t": (0.5, 0.5), "u": (1, 0.5), "v": (0.5, 1), "f": (1, 1)}.items():
        lon, lat = _points(ox, oy)
        v[f"glam{p}"] = (("time_counter", "y", "x"), lon[None])
        v[f"gphi{p}"] = (("time_counter", "y", "x"), lat[None])
        v[f"e1{p}"] = (("time_counter", "y", "x"), np.full((1, NY, NX), 1.0e5))
        v[f"e2{p}"] = (("time_counter", "y", "x"), np.full((1, NY, NX), 2.0e5))
    path = tmp_path / "domain_cfg.nc"
    xr.Dataset(v).to_netcdf(path)
    return str(path)


def _field(point="t"):
    lon, lat = _points(*{"t": (0.5, 0.5), "u": (1, 0.5)}[point])
    return xr.DataArray(
        np.ones((2, NY, NX), "f4"),
        dims=("time_counter", "y", "x"),
        coords={"nav_lat": (("y", "x"), lat.astype("f4")), "nav_lon": (("y", "x"), lon.astype("f4"))},
        name="tos",
        attrs={"units": "degC", "coordinates": "time_centered nav_lat nav_lon"},
    )


def test_orca_grid_t_point(domain):
    out = cs.orca_grid(_field(), _Rule(orca_domain=domain))
    assert out.dims == ("time_counter", "j", "i")
    assert "nav_lat" not in out.coords and "coordinates" not in out.attrs
    assert out["latitude"].dtype == np.float64
    assert out["latitude"].attrs["bounds"] == "vertices_latitude"
    np.testing.assert_allclose(out["latitude"].values, _points(0.5, 0.5)[1])


def test_orca_vertices_t_point(domain):
    rule = _Rule(orca_domain=domain)
    ds = cs.orca_vertices(cs.orca_grid(_field(), rule), rule)
    lat_b, lon_b = ds["vertices_latitude"].values, ds["vertices_longitude"].values
    assert lat_b.shape == (NY, NX, 4)
    # cell (2, 3): T centre at lon -22.5, lat -12.5; corners at the F points
    np.testing.assert_allclose(lon_b[2, 3], [-45.0, 0.0, 0.0, -45.0])
    np.testing.assert_allclose(lat_b[2, 3], [-25.0, -25.0, 0.0, 0.0])
    # first row: south corners extrapolated, first column: west corners wrap
    np.testing.assert_allclose(lat_b[0, 0], [-75.0, -75.0, -50.0, -50.0])
    np.testing.assert_allclose(lon_b[0, 0], [-180.0, -135.0, -135.0, -180.0])


def test_orca_vertices_u_point_wraps_east(domain):
    rule = _Rule(orca_domain=domain, orca_point="U")
    da = _field("u").rename("tauuo")
    ds = cs.orca_vertices(cs.orca_grid(da, rule), rule)
    lon_b = ds["vertices_longitude"].values
    # last column: U at lon 180, corners at V of columns NX-1 and 0 (wrapped)
    np.testing.assert_allclose(lon_b[1, NX - 1], [157.5, 202.5, 202.5, 157.5])
    np.testing.assert_allclose(ds["vertices_latitude"].values[1, 0], [-50.0, -50.0, -25.0, -25.0])


def test_orca_grid_shape_mismatch(domain):
    da = _field().isel(x=slice(0, NX - 1))
    with pytest.raises(ValueError, match="does not match"):
        cs.orca_grid(da, _Rule(orca_domain=domain))


def test_orca_grid_needs_domain_and_point():
    with pytest.raises(ValueError, match="orca_domain"):
        cs.orca_grid(_field(), _Rule())
    with pytest.raises(ValueError, match="orca_point"):
        cs.orca_grid(_field(), _Rule(orca_domain="x.nc", orca_point="W"))


def test_orca_grid_noop_without_xy():
    da = xr.DataArray(np.ones(3), dims=("time",))
    assert cs.orca_grid(da, _Rule()) is da


def test_orca_cell_area(domain):
    area = cs.orca_cell_area(xr.open_dataset(domain), _Rule(model_variable="areacello"))
    assert area.dims == ("y", "x") and area.name == "areacello"
    np.testing.assert_allclose(area.values, 2.0e10)
    assert area.attrs["units"] == "m2"
