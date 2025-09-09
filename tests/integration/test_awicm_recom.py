import yaml

from pycmor.core.cmorizer import CMORizer
from pycmor.core.logging import logger


def test_process(awicm_1p0_recom_config, awicm_1p0_recom_data):
    logger.info(f"Processing {awicm_1p0_recom_config}")
    with open(awicm_1p0_recom_config, "r") as f:
        cfg = yaml.safe_load(f)
    for rule in cfg["rules"]:
        for input in rule["inputs"]:
            input["path"] = input["path"].replace(
                "REPLACE_ME",
                str(f"{awicm_1p0_recom_data}/awi-esm-1-1-lr_kh800/piControl/"),
            )
        if "mesh_path" in rule:
            rule["mesh_path"] = rule["mesh_path"].replace(
                "REPLACE_ME",
                str(f"{awicm_1p0_recom_data}/awi-esm-1-1-lr_kh800/piControl/"),
            )
    cmorizer = CMORizer.from_dict(cfg)
    cmorizer.process()
