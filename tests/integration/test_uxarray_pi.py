import yaml

from pycmor.core.cmorizer import CMORizer
from pycmor.core.logging import logger
from pycmor.core.pipeline import DefaultPipeline

STEPS = DefaultPipeline.STEPS
PROGRESSIVE_STEPS = [STEPS[: i + 1] for i in range(len(STEPS))]


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

    # CMIP7 uses packaged data - no CMIP_Tables_Dir needed

    for rule in cfg["rules"]:
        for input in rule["inputs"]:
            input["path"] = input["path"].replace("REPLACE_ME", str(pi_uxarray_data))
    cmorizer = CMORizer.from_dict(cfg)
    cmorizer.process()
