# Just import dask for parallelisms...
import os

import dask  # noqa
import pytest
import xarray as xr

# Meta tests validate environment setup (NetCDF libraries, engines)
# These tests should use real data when validating the environment,
# but can be skipped when using stub data in regular CI
pytestmark = pytest.mark.skipif(
    not os.getenv("PYCMOR_USE_REAL_TEST_DATA"),
    reason="Meta tests require real data for environment validation (set PYCMOR_USE_REAL_TEST_DATA=1)",
)


@pytest.mark.parametrize(
    "engine",
    [
        "netcdf4",
    ],
)
def test_open_awicm_1p0_recom(awicm_1p0_recom_data, engine):
    ds = xr.open_mfdataset(
        f"{awicm_1p0_recom_data}/awi-esm-1-1-lr_kh800/piControl/outdata/fesom/*.nc",
        engine=engine,
    )
    assert isinstance(ds, xr.Dataset)


@pytest.mark.parametrize(
    "engine",
    [
        "h5netcdf",
    ],
)
def test_open_fesom_2p6_pimesh_esm_tools(fesom_2p6_pimesh_esm_tools_data, engine):
    matching_files = [
        f for f in (fesom_2p6_pimesh_esm_tools_data / "outdata/fesom/").iterdir() if f.name.startswith("temp.fesom")
    ]
    assert len(matching_files) > 0
    ds = xr.open_mfdataset(
        matching_files,
        engine=engine,
    )
    assert isinstance(ds, xr.Dataset)


@pytest.mark.parametrize(
    "engine",
    [
        "h5netcdf",
    ],
)
def test_open_fesom_2p6_pimesh_esm_tools_cftime(fesom_2p6_pimesh_esm_tools_data, engine):
    ds = xr.open_mfdataset(
        (f for f in (fesom_2p6_pimesh_esm_tools_data / "outdata/fesom/").iterdir() if f.name.startswith("temp")),
        use_cftime=True,
        engine=engine,
    )
    assert isinstance(ds, xr.Dataset)


@pytest.mark.parametrize(
    "engine",
    [
        "h5netcdf",
    ],
)
def test_open_fesom_2p6_pimesh_esm_tools_parallel(fesom_2p6_pimesh_esm_tools_data, engine):
    ds = xr.open_mfdataset(
        (f for f in (fesom_2p6_pimesh_esm_tools_data / "outdata/fesom/").iterdir() if f.name.startswith("temp")),
        parallel=True,
        engine=engine,
    )
    assert isinstance(ds, xr.Dataset)


@pytest.mark.parametrize(
    "engine",
    [
        "h5netcdf",
    ],
)
def test_open_fesom_2p6_pimesh_esm_tools_full(fesom_2p6_pimesh_esm_tools_data, engine):
    ds = xr.open_mfdataset(
        (f for f in (fesom_2p6_pimesh_esm_tools_data / "outdata/fesom/").iterdir() if f.name.startswith("temp")),
        use_cftime=True,
        parallel=True,
        engine=engine,
    )
    assert isinstance(ds, xr.Dataset)
