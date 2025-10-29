"""
Tests for DataRequestVariable
"""

from pycmor.data_request.variable import CMIP6JSONDataRequestVariable


def test_cmip6_init_from_json_file():
    drv = CMIP6JSONDataRequestVariable.from_json_file(
        "cmip6-cmor-tables/Tables/CMIP6_Omon.json",
        "thetao",
    )
    assert drv.name == "thetao"
    assert drv.frequency == "mon"
    assert drv.table_name == "Omon"


def test_cmip7_from_vendored_json():
    # Skip this test - vendored JSON is limited, full testing done in test_cmip7_interface.py
    import pytest
    pytest.skip("Vendored all_var_info.json has limited data. Full CMIP7 testing in test_cmip7_interface.py")
