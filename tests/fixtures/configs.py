import pytest
import ruamel.yaml


@pytest.fixture
def config(request):
    return request.getfixturevalue(request.param)


@pytest.fixture
def config_empty():
    return {"pycmor": {}}


@pytest.fixture
def config_pattern_env_var_name():
    return {
        "pycmor": {
            "pattern_env_var_name": "CMOR_PATTERN",
        }
    }


@pytest.fixture
def config_pattern_env_var_value():
    return {
        "pycmor": {
            "pattern_env_var_value": "test.*nc",
        }
    }


@pytest.fixture
def config_pattern_env_var_name_and_value():
    return {
        "pycmor": {
            "pattern_env_var_name": "CMOR_PATTERN",
            "pattern_env_var_value": "other_test.*nc",
        }
    }


@pytest.fixture
def fesom_pi_mesh_config(fesom_pi_mesh_config_file):
    yaml = ruamel.yaml.YAML()
    return yaml.load(fesom_pi_mesh_config_file.open())
