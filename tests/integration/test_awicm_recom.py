import pytest
import yaml

from pycmor.core.cmorizer import CMORizer
from pycmor.core.logging import logger


@pytest.mark.parametrize(
    "config_fixture",
    [
        pytest.param("awicm_1p0_recom_config", id="CMIP6"),
        pytest.param("awicm_1p0_recom_config_cmip7", id="CMIP7"),
    ],
)
def test_process(config_fixture, awicm_1p0_recom_data, request):
    config = request.getfixturevalue(config_fixture)
    logger.info(f"Processing {config}")
    with open(config, "r") as f:
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
