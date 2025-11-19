import pytest
import yaml

from pycmor.core.cmorizer import CMORizer
from pycmor.core.logging import logger
from pycmor.core.pipeline import DefaultPipeline

STEPS = DefaultPipeline.STEPS
PROGRESSIVE_STEPS = [STEPS[: i + 1] for i in range(len(STEPS))]


@pytest.mark.parametrize(
    "config_fixture",
    [
        pytest.param("fesom_2p6_pimesh_esm_tools_config", id="CMIP6"),
        pytest.param("fesom_2p6_pimesh_esm_tools_config_cmip7", id="CMIP7"),
    ],
)
def test_init(config_fixture, fesom_2p6_pimesh_esm_tools_data, request):
    config = request.getfixturevalue(config_fixture)
    logger.info(f"Processing {config}")
    with open(config, "r") as f:
        cfg = yaml.safe_load(f)
    for rule in cfg["rules"]:
        for input in rule["inputs"]:
            input["path"] = input["path"].replace("REPLACE_ME", str(fesom_2p6_pimesh_esm_tools_data))
    CMORizer.from_dict(cfg)
    # If we get this far, it was possible to construct
    # the object, so this test passes:
    assert True


@pytest.mark.parametrize(
    "config_fixture",
    [
        pytest.param("fesom_2p6_pimesh_esm_tools_config", id="CMIP6"),
        pytest.param("fesom_2p6_pimesh_esm_tools_config_cmip7", id="CMIP7"),
    ],
)
def test_process(config_fixture, fesom_2p6_pimesh_esm_tools_data, request):
    config = request.getfixturevalue(config_fixture)
    logger.info(f"Processing {config}")
    with open(config, "r") as f:
        cfg = yaml.safe_load(f)
    for rule in cfg["rules"]:
        for input in rule["inputs"]:
            input["path"] = input["path"].replace("REPLACE_ME", str(fesom_2p6_pimesh_esm_tools_data))
    cmorizer = CMORizer.from_dict(cfg)
    cmorizer.process()
