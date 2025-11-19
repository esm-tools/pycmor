import pytest

from tests.utils.constants import TEST_ROOT


@pytest.fixture
def test_config():
    return TEST_ROOT / "configs" / "test_config.yaml"


@pytest.fixture
def fesom_pi_mesh_config_file():
    return TEST_ROOT / "configs/fesom_pi_mesh_run.yaml"


@pytest.fixture
def test_config_cmip6():
    return TEST_ROOT / "configs" / "test_config_cmip6.yaml"


@pytest.fixture
def test_config_cmip7():
    return TEST_ROOT / "configs" / "test_config_cmip7.yaml"


@pytest.fixture
def pi_uxarray_config():
    return TEST_ROOT / "configs" / "test_config_pi_uxarray.yaml"


@pytest.fixture
def pi_uxarray_config_cmip7():
    return TEST_ROOT / "configs" / "test_config_pi_uxarray_cmip7.yaml"


@pytest.fixture
def fesom_2p6_pimesh_esm_tools_config():
    return TEST_ROOT / "configs" / "test_config_fesom_2p6_pimesh.yaml"


@pytest.fixture
def awicm_1p0_recom_config():
    return TEST_ROOT / "configs" / "test_config_awicm_1p0_recom.yaml"


@pytest.fixture
def awicm_1p0_recom_config_cmip7():
    return TEST_ROOT / "configs" / "test_config_awicm_1p0_recom_cmip7.yaml"


@pytest.fixture
def fesom_2p6_pimesh_esm_tools_config_cmip7():
    return TEST_ROOT / "configs" / "test_config_fesom_2p6_pimesh_cmip7.yaml"
