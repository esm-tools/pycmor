"""A ``glb`` file must cover the grid its grid_label names (DKRZ review round 7)."""

from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from pycmor.std_lib.files import _ensure_lat_lon_bounds_impl, _expand_global_subset_to_reference_grid

N = 12
LAND = [1, 4, 5, 9]


@pytest.fixture
def reference(tmp_path):
    """A 12-cell stand-in for the OIFS grid file, with 4-vertex bounds."""
    lat = np.linspace(80, -80, N)
    lon = (np.arange(N) * 30.0) % 360
    ds = xr.Dataset(
        {
            "bounds_lat": (("cell", "nvertex"), np.stack([lat - 1, lat - 1, lat + 1, lat + 1], axis=1)),
            "bounds_lon": (("cell", "nvertex"), np.stack([lon - 1, lon + 1, lon + 1, lon - 1], axis=1)),
        },
        coords={"lat": ("cell", lat), "lon": ("cell", lon)},
    )
    path = tmp_path / "atmos_mon_land_orog_1850.nc"
    ds.to_netcdf(path)
    return path, lat, lon


def _land_dataset(lat, lon, with_type=False):
    """What an LPJ-GUESS loader returns: land cells only, longitudes on -180..180."""
    cells = np.array(LAND)
    lon_pm = np.where(lon[cells] > 180, lon[cells] - 360, lon[cells])
    dims = ("time", "type", "ncells") if with_type else ("time", "ncells")
    shape = (2, 3, len(cells)) if with_type else (2, len(cells))
    data = np.arange(np.prod(shape), dtype=np.float64).reshape(shape) + 1
    ds = xr.Dataset(
        {"treeFrac": (dims, data, {"units": "%"})},
        coords={"lat": ("ncells", lat[cells], {"units": "degrees_north"}), "lon": ("ncells", lon_pm)},
        attrs={"title": "x"},
    )
    ds["treeFrac"].encoding["_FillValue"] = 1.0e20
    return ds


def _rule(path, compound="land.treeFrac.tavg-u-hxy-u.mon.glb", **extra):
    return SimpleNamespace(bounds_reference_file=str(path), compound_name=compound, inputs=[], **extra)


@pytest.mark.parametrize("with_type", [False, True])
def test_land_cells_are_placed_on_the_full_grid(reference, with_type):
    path, lat, lon = reference
    ds = _land_dataset(lat, lon, with_type)
    out = _expand_global_subset_to_reference_grid(ds, _rule(path))

    assert out.sizes["ncells"] == N
    np.testing.assert_allclose(out["lat"].values, lat)
    np.testing.assert_allclose(out["lon"].values, lon)
    np.testing.assert_array_equal(out["treeFrac"].isel(ncells=LAND).values, ds["treeFrac"].values)
    ocean = [i for i in range(N) if i not in LAND]
    assert np.isnan(out["treeFrac"].isel(ncells=ocean).values).all()
    assert out["treeFrac"].attrs == {"units": "%"} and out["treeFrac"].encoding["_FillValue"] == 1.0e20
    assert out["lat"].attrs == {"units": "degrees_north"} and out.attrs == {"title": "x"}
    assert "ncells" not in out.variables, "no index variable left behind"


def test_every_cell_then_gets_the_reference_polygon(reference):
    path, lat, lon = reference
    out = _ensure_lat_lon_bounds_impl(_land_dataset(lat, lon), _rule(path))
    assert out["lat_bnds"].shape == (N, 4) and out["lon_bnds"].shape == (N, 4)
    assert not np.isnan(out["lat_bnds"].values).any()
    np.testing.assert_allclose(out["lat_bnds"].values[:, 0], lat - 1)


def test_a_region_file_keeps_its_subset(reference):
    """30S-90S is a subset because the data request asks for one."""
    path, lat, lon = reference
    ds = _land_dataset(lat, lon)
    out = _expand_global_subset_to_reference_grid(ds, _rule(path, "atmos.tas.tavg-h2m-hxy-u.1hr.30S-90S"))
    assert out.sizes["ncells"] == len(LAND)


def test_cells_that_are_not_on_the_grid_are_left_alone(reference):
    path, lat, lon = reference
    ds = _land_dataset(lat, lon)
    ds = ds.assign_coords(lat=("ncells", ds["lat"].values + 3.0))
    out = _expand_global_subset_to_reference_grid(ds, _rule(path))
    assert out.sizes["ncells"] == len(LAND), "a wrong placement is worse than a subset"


def test_opt_out_and_no_reference(reference):
    path, lat, lon = reference
    ds = _land_dataset(lat, lon)
    assert _expand_global_subset_to_reference_grid(ds, _rule(path, keep_subset_grid=True)).sizes["ncells"] == len(LAND)
    rule = SimpleNamespace(compound_name="land.treeFrac.tavg-u-hxy-u.mon.glb", inputs=[])
    assert _expand_global_subset_to_reference_grid(ds, rule).sizes["ncells"] == len(LAND)


def test_a_complete_file_is_untouched(reference):
    path, lat, lon = reference
    full = xr.Dataset({"tas": (("time", "cell"), np.ones((2, N)))}, coords={"lat": ("cell", lat), "lon": ("cell", lon)})
    assert _expand_global_subset_to_reference_grid(full, _rule(path)) is full
