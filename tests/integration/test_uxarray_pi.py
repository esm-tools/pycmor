import pytest
import yaml

from pycmor.core.cmorizer import CMORizer
from pycmor.core.logging import logger
from pycmor.core.pipeline import DefaultPipeline

STEPS = DefaultPipeline.STEPS
PROGRESSIVE_STEPS = [STEPS[: i + 1] for i in range(len(STEPS))]


# There is a segfault somewhere in the code, so I'd like to find out where it is...
@pytest.mark.skip
@pytest.mark.parametrize("steps", PROGRESSIVE_STEPS)
def test_process_progressive_pipeline(pi_uxarray_config, pi_uxarray_data, steps):
    logger.info(f"Processing {pi_uxarray_config} with {steps}")
    with open(pi_uxarray_config, "r") as f:
        cfg = yaml.safe_load(f)
    if "pipelines" not in cfg:
        cfg["pipelines"] = []
    for rule in cfg["rules"]:
        for input in rule["inputs"]:
            input["path"] = input["path"].replace("REPLACE_ME", str(pi_uxarray_data))
        rule["pipelines"] = ["default"]
    cfg["pipelines"].append({"name": "default", "steps": []})
    pipeline = cfg["pipelines"][0]
    pipeline["steps"][:] = steps
    cmorizer = CMORizer.from_dict(cfg)
    cmorizer.process()


def test_process(pi_uxarray_config, pi_uxarray_data):
    logger.info(f"Processing {pi_uxarray_config}")
    with open(pi_uxarray_config, "r") as f:
        cfg = yaml.safe_load(f)
    for rule in cfg["rules"]:
        for input in rule["inputs"]:
            input["path"] = input["path"].replace("REPLACE_ME", str(pi_uxarray_data))
    cmorizer = CMORizer.from_dict(cfg)
    cmorizer.process()


def test_process_native(pi_uxarray_config, pi_uxarray_data):
    logger.info(f"Processing {pi_uxarray_config}")
    with open(pi_uxarray_config, "r") as f:
        cfg = yaml.safe_load(f)
    cfg["pycmor"]["pipeline_workflow_orchestrator"] = "native"
    cfg["pycmor"]["dask_cluster"] = "local"
    for rule in cfg["rules"]:
        for input in rule["inputs"]:
            input["path"] = input["path"].replace("REPLACE_ME", str(pi_uxarray_data))
    cmorizer = CMORizer.from_dict(cfg)
    cmorizer.process()


def test_process_cmip7(pi_uxarray_config_cmip7, pi_uxarray_data):
    logger.info(f"Processing {pi_uxarray_config_cmip7}")
    with open(pi_uxarray_config_cmip7, "r") as f:
        cfg = yaml.safe_load(f)
    for rule in cfg["rules"]:
        for input in rule["inputs"]:
            input["path"] = input["path"].replace("REPLACE_ME", str(pi_uxarray_data))
    cmorizer = CMORizer.from_dict(cfg)
    cmorizer.process()
