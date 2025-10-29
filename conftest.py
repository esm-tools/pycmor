from tests.utils.constants import TEST_ROOT  # noqa: F401

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
