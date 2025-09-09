"""
Integration test that mimics the CLI command for a minimal config
"""

import shutil

import pytest
import yaml
from prefect.logging import disable_run_logger

from pycmor.core.cmorizer import CMORizer
from pycmor.core.logging import logger


@pytest.mark.parametrize(
    "config",
    [
        pytest.param("test_config_cmip6", id="CMIP6"),
        pytest.param(
            "test_config_cmip7",
            id="CMIP7",
            marks=pytest.mark.xfail(reason="NotImplementedError"),
        ),
    ],
    indirect=True,
)
def test_init(config):
    disable_run_logger()  # Turns off Prefect's extra logging layer, for testing
    logger.info(f"Processing {config}")
    with open(config, "r") as f:
        cfg = yaml.safe_load(f)
    cmorizer = CMORizer.from_dict(cfg)
    # If we get this far, it was possible to construct
    # the object, so this test passes. Meaningless test,
    # but we know that the object is at least constructable:
    assert isinstance(cmorizer, CMORizer)
    # breakpoint()


@pytest.mark.skipif(
    shutil.which("sbatch") is None, reason="sbatch is not available on this host"
)
@pytest.mark.parametrize(
    "config",
    [
        pytest.param("test_config_cmip6", id="CMIP6"),
        pytest.param(
            "test_config_cmip7",
            id="CMIP7",
            marks=pytest.mark.xfail(reason="NotImplementedError"),
        ),
    ],
    indirect=True,
)
def test_process(config):
    logger.info(f"Processing {config}")
    with open(config, "r") as f:
        cfg = yaml.safe_load(f)
    cmorizer = CMORizer.from_dict(cfg)
    cmorizer.process()
