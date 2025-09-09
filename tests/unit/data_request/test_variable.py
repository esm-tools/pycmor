"""
Tests for DataRequestVariable
"""

from pycmor.data_request.variable import (
    CMIP6JSONDataRequestVariable,
    CMIP7DataRequestVariable,
)


def test_cmip6_init_from_json_file():
    drv = CMIP6JSONDataRequestVariable.from_json_file(
        "cmip6-cmor-tables/Tables/CMIP6_Omon.json",
        "thetao",
    )
    assert drv.name == "thetao"
    assert drv.frequency == "mon"
    assert drv.table_name == "Omon"


def test_cmip7_from_vendored_json():
    drv = CMIP7DataRequestVariable.from_all_var_info_json("thetao", "Omon")
    assert drv.name == "thetao"
    assert drv.frequency == "mon"
    assert drv.table_name == "Omon"
