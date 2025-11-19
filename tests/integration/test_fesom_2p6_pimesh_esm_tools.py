import pytest
import yaml

from pycmor.core.cmorizer import CMORizer
from pycmor.core.logging import logger
from pycmor.core.pipeline import DefaultPipeline

STEPS = DefaultPipeline.STEPS
PROGRESSIVE_STEPS = [STEPS[: i + 1] for i in range(len(STEPS))]


def test_init(fesom_2p6_pimesh_esm_tools_config, fesom_2p6_pimesh_esm_tools_data):
    logger.info(f"Processing {fesom_2p6_pimesh_esm_tools_config}")
    with open(fesom_2p6_pimesh_esm_tools_config, "r") as f:
        cfg = yaml.safe_load(f)
    for rule in cfg["rules"]:
        for input in rule["inputs"]:
            input["path"] = input["path"].replace("REPLACE_ME", str(fesom_2p6_pimesh_esm_tools_data))
    CMORizer.from_dict(cfg)
    # If we get this far, it was possible to construct
    # the object, so this test passes:
    assert True


def test_process(fesom_2p6_pimesh_esm_tools_config, fesom_2p6_pimesh_esm_tools_data):
    logger.info(f"Processing {fesom_2p6_pimesh_esm_tools_config}")
    with open(fesom_2p6_pimesh_esm_tools_config, "r") as f:
        cfg = yaml.safe_load(f)
    for rule in cfg["rules"]:
        for input in rule["inputs"]:
            input["path"] = input["path"].replace("REPLACE_ME", str(fesom_2p6_pimesh_esm_tools_data))
    cmorizer = CMORizer.from_dict(cfg)
    cmorizer.process()
