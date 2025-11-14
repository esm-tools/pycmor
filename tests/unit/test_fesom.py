import os

import pytest
import xarray as xr

import pycmor
import pycmor.fesom_2p1.regridding


@pytest.mark.skipif(
    not os.getenv("PYCMOR_USE_REAL_TEST_DATA"),
    reason="FESOM regridding requires real mesh data (set PYCMOR_USE_REAL_TEST_DATA=1)",
)
def test_regridding(fesom_pi_mesh_config, fesom_2p6_pimesh_esm_tools_data, pi_uxarray_mesh):
    config = fesom_pi_mesh_config
    rule = pycmor.core.rule.Rule.from_dict(config["rules"][0])
    rule.mesh_path = pi_uxarray_mesh
    ds = xr.open_mfdataset(str(fesom_2p6_pimesh_esm_tools_data / "outdata/fesom") + "/temp.fesom.*.nc")
    da = ds.temp.load()
    da = pycmor.fesom_2p1.regridding.regrid_to_regular(da, rule)
    assert da.shape == (3, 360, 180)


@pytest.mark.skipif(
    not os.getenv("PYCMOR_USE_REAL_TEST_DATA"),
    reason="FESOM mesh attachment requires real mesh data (set PYCMOR_USE_REAL_TEST_DATA=1)",
)
def test_attach_mesh_to_rule(fesom_pi_mesh_config, pi_uxarray_mesh):
    config = fesom_pi_mesh_config
    rule = pycmor.core.rule.Rule.from_dict(config["rules"][0])
    mesh_path = pi_uxarray_mesh
    rule.mesh_path = mesh_path
    data = None  # Not important for this test
    assert not hasattr(rule, "mesh")
    # _ symbolizes just any return value, which we never use
    _ = pycmor.fesom_2p1.regridding.attach_mesh_to_rule(data, rule)
    assert hasattr(rule, "mesh")
