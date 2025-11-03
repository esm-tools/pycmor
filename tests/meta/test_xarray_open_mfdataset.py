# Just import dask for parallelisms...
import dask  # noqa
import pytest
import xarray as xr


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
        parallel=False,
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
        f
        for f in (fesom_2p6_pimesh_esm_tools_data / "outdata/fesom/").iterdir()
        if f.name.startswith("temp.fesom")
    ]
    assert len(matching_files) > 0
    ds = xr.open_mfdataset(
        matching_files,
        engine=engine,
        parallel=False,
        phony_dims="sort",  # Workaround for h5netcdf dimension scale issues
    )
    assert isinstance(ds, xr.Dataset)


@pytest.mark.parametrize(
    "engine",
    [
        "h5netcdf",
    ],
)
def test_open_fesom_2p6_pimesh_esm_tools_cftime(
    fesom_2p6_pimesh_esm_tools_data, engine
):
    ds = xr.open_mfdataset(
        (
            f
            for f in (fesom_2p6_pimesh_esm_tools_data / "outdata/fesom/").iterdir()
            if f.name.startswith("temp")
        ),
        use_cftime=True,
        engine=engine,
        parallel=False,
        phony_dims="sort",  # Workaround for h5netcdf dimension scale issues
    )
    assert isinstance(ds, xr.Dataset)


@pytest.mark.parametrize(
    "engine",
    [
        "h5netcdf",
    ],
)
def test_open_fesom_2p6_pimesh_esm_tools_parallel(
    fesom_2p6_pimesh_esm_tools_data, engine
):
    ds = xr.open_mfdataset(
        (
            f
            for f in (fesom_2p6_pimesh_esm_tools_data / "outdata/fesom/").iterdir()
            if f.name.startswith("temp")
        ),
        parallel=False,
        engine=engine,
        phony_dims="sort",  # Workaround for h5netcdf dimension scale issues
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
        (
            f
            for f in (fesom_2p6_pimesh_esm_tools_data / "outdata/fesom/").iterdir()
            if f.name.startswith("temp")
        ),
        use_cftime=True,
        parallel=False,
        engine=engine,
        phony_dims="sort",  # Workaround for h5netcdf dimension scale issues
    )
    assert isinstance(ds, xr.Dataset)
