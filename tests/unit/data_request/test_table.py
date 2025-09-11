from pycmor.data_request.table import CMIP7DataRequestTable


def test_cmip7_from_vendored_json():
    drt = CMIP7DataRequestTable.from_all_var_info_json("Omon")
    # For right now, just check if the object is creatable
    assert drt is not None
