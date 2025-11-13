import logging

import pytest

from tests.utils.constants import TEST_ROOT  # noqa: F401


@pytest.fixture(scope="function", autouse=True)
def suppress_third_party_logs():
    """Suppress noisy INFO logs from distributed/dask/prefect during tests.

    This runs before every test function to ensure logs are suppressed even
    when distributed.Client creates new workers.
    """
    # Set WARNING level for all noisy distributed/dask/prefect loggers
    loggers_to_suppress = [
        "distributed",
        "distributed.core",
        "distributed.scheduler",
        "distributed.nanny",
        "distributed.worker",
        "distributed.http.proxy",
        "distributed.worker.memory",
        "distributed.comm",
        "prefect",
    ]

    for logger_name in loggers_to_suppress:
        logging.getLogger(logger_name).setLevel(logging.WARNING)


pytest_plugins = [
    "tests.fixtures.CMIP_Tables_Dir",
    "tests.fixtures.CV_Dir",
    "tests.fixtures.cmip7_test_data",
    "tests.fixtures.config_files",
    "tests.fixtures.configs",
    "tests.fixtures.datasets",
    "tests.fixtures.environment",
    "tests.fixtures.example_data.awicm_recom",
    "tests.fixtures.example_data.fesom_2p6_pimesh",
    "tests.fixtures.example_data.pi_uxarray",
    "tests.fixtures.fake_data.fesom_mesh",
    "tests.fixtures.fake_filesystem",
    "tests.fixtures.sample_rules",
    "tests.fixtures.config_files",
    "tests.fixtures.CV_Dir",
    "tests.fixtures.CMIP_Tables_Dir",
    "tests.fixtures.config_files",
    "tests.fixtures.CV_Dir",
    "tests.fixtures.data_requests",
]
