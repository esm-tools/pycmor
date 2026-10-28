"""apply_ocean_mask: g131 fields lose the 0 XIOS writes over land."""

from unittest.mock import MagicMock

import numpy as np
import pytest
import xarray as xr

from pycmor.std_lib import files


class _Rule(dict):
    pass


@pytest.fixture
def mask_file(tmp_path):
    path = tmp_path / "gr_fx.nc"
    sftof = np.array([[0.0, 40.0, 100.0], [100.0, 0.0, 100.0]])
    xr.Dataset(
        {"sftof": (("lat", "lon"), sftof)},
        coords={"lat": [-0.25, 0.25], "lon": [0.25, 0.75, 1.25]},
    ).to_netcdf(path)
    files._OCEAN_MASKS.clear()
    return str(path)


def _field(dims, shape):
    da = xr.DataArray(np.ones(shape, dtype="f4"), dims=dims, name="tos", attrs={"units": "degC"})
    # coordinates from another file need not match the mask's to the last bit
    da = da.assign_coords(lat=np.array([-0.25, 0.25]) + 1e-9, lon=[0.25, 0.75, 1.25])
    da.encoding["_FillValue"] = np.float32(1e20)
    return da


def test_land_becomes_missing_and_metadata_stays(mask_file):
    out = files.apply_ocean_mask(_field(("time", "lat", "lon"), (2, 2, 3)), _Rule(ocean_mask_file=mask_file))
    assert np.isnan(out.values[:, 0, 0]).all() and np.isnan(out.values[:, 1, 1]).all()
    assert np.isfinite(out.values[:, 0, 1]).all() and out.values[0, 1, 2] == 1.0
    assert out.dims == ("time", "lat", "lon")
    assert out.attrs == {"units": "degC"} and out.encoding["_FillValue"] == np.float32(1e20)


def test_3d_field_is_masked_at_every_level(mask_file):
    out = files.apply_ocean_mask(_field(("time", "lev", "lat", "lon"), (1, 3, 2, 3)), _Rule(ocean_mask_file=mask_file))
    assert np.isnan(out.values[0, :, 0, 0]).all()
    assert out.dims == ("time", "lev", "lat", "lon")


def test_dataset_masks_its_float_variables(mask_file):
    ds = xr.Dataset({"tos": _field(("lat", "lon"), (2, 3)), "time_bnds": xr.DataArray(np.zeros(2), dims="bnds")})
    out = files.apply_ocean_mask(ds, _Rule(ocean_mask_file=mask_file))
    assert np.isnan(out["tos"].values[0, 0])


def test_no_mask_file_leaves_data_alone():
    da = _field(("lat", "lon"), (2, 3))
    assert files.apply_ocean_mask(da, _Rule(ocean_mask_file="")) is da
    assert files.apply_ocean_mask(da, _Rule()) is da
    # stub rules (MagicMock) answer every get() with a mock, not a path
    assert files.apply_ocean_mask(da, MagicMock()) is da


def test_other_grid_is_an_error(mask_file):
    da = xr.DataArray(np.ones((4, 5), dtype="f4"), dims=("lat", "lon"), name="tos")
    with pytest.raises(ValueError, match="tos"):
        files.apply_ocean_mask(da, _Rule(ocean_mask_file=mask_file))
