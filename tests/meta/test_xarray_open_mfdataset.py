# Just import dask for parallelisms...
import dask  # noqa
import pytest
import xarray as xr


@pytest.fixture(scope="module")
def fesom_2p6_temp_files(fesom_2p6_pimesh_esm_tools_data):
    """Cache the list of FESOM 2.6 temperature files to avoid repeated directory scans."""
    return sorted(
        [
            f
            for f in (fesom_2p6_pimesh_esm_tools_data / "outdata/fesom/").iterdir()
            if f.name.startswith("temp")
        ]
    )


@pytest.mark.parametrize(
    "engine",
    [
        "netcdf4",
        "h5netcdf",
    ],
)
def test_open_awicm_1p0_recom(awicm_1p0_recom_data, engine):
    with xr.open_mfdataset(
        f"{awicm_1p0_recom_data}/awi-esm-1-1-lr_kh800/piControl/outdata/fesom/*.nc",
        engine=engine,
    ) as ds:
        assert len(ds.dims) > 0
        assert len(ds.data_vars) > 0


@pytest.mark.parametrize(
    "engine",
    [
        "netcdf4",
        "h5netcdf",
    ],
)
def test_open_fesom_2p6_pimesh_esm_tools(fesom_2p6_temp_files, engine):
    matching_files = [
        f for f in fesom_2p6_temp_files if f.name.startswith("temp.fesom")
    ]
    assert len(matching_files) > 0
    with xr.open_mfdataset(
        matching_files,
        engine=engine,
    ) as ds:
        assert len(ds.dims) > 0
        assert len(ds.data_vars) > 0


@pytest.mark.parametrize(
    "engine",
    [
        "netcdf4",
        "h5netcdf",
    ],
)
def test_open_fesom_2p6_pimesh_esm_tools_cftime(fesom_2p6_temp_files, engine):
    with xr.open_mfdataset(
        fesom_2p6_temp_files,
        use_cftime=True,
        engine=engine,
    ) as ds:
        assert len(ds.dims) > 0
        assert len(ds.data_vars) > 0


@pytest.mark.parametrize(
    "engine",
    [
        "netcdf4",
        "h5netcdf",
    ],
)
def test_open_fesom_2p6_pimesh_esm_tools_parallel(fesom_2p6_temp_files, engine):
    with xr.open_mfdataset(
        fesom_2p6_temp_files,
        parallel=True,
        engine=engine,
    ) as ds:
        assert len(ds.dims) > 0
        assert len(ds.data_vars) > 0


@pytest.mark.parametrize(
    "engine",
    [
        "netcdf4",
        "h5netcdf",
    ],
)
def test_open_fesom_2p6_pimesh_esm_tools_full(fesom_2p6_temp_files, engine):
    with xr.open_mfdataset(
        fesom_2p6_temp_files,
        use_cftime=True,
        parallel=True,
        engine=engine,
    ) as ds:
        assert len(ds.dims) > 0
        assert len(ds.data_vars) > 0
