import xarray as xr

from pycmor.std_lib.variable_attributes import set_variable_attrs


def test_variable_attrs_dataarray(rule_after_cmip6_cmorizer_init, mocker):
    """Pseudo-integration test for the variable attributes of a DataArray"""
    # Set the fixture as the rule
    rule = rule_after_cmip6_cmorizer_init

    # Mock the _pycmor_cfg to return the required values
    mock_cfg = mocker.Mock()
    mock_cfg.return_value = 1.0e30  # Default missing value
    rule._pycmor_cfg = mock_cfg

    # Set the DataArray with a name that matches the rule's model_variable
    da = xr.DataArray(name=rule.model_variable)

    # Set the variable attributes
    da = set_variable_attrs(da, rule)

    # Get the variable attributes
    d = da.attrs
    e = da.encoding

    # Check that _FillValue is set correctly in encoding
    assert e["_FillValue"] == 1.0e30

    # Check that required attributes are set
    for attr in ["standard_name", "long_name"]:
        assert attr in d


def test_rule_variable_attributes_override_the_data_request(rule_after_cmip6_cmorizer_init, mocker):
    rule = rule_after_cmip6_cmorizer_init
    rule._pycmor_cfg = mocker.Mock(return_value=1.0e30)
    standard_name = rule.data_request_variable.attrs["standard_name"]
    rule.variable_attributes = {"comment": "model-specific note"}

    da = set_variable_attrs(xr.DataArray(name=rule.model_variable), rule)

    assert da.attrs["comment"] == "model-specific note"
    assert da.attrs["standard_name"] == standard_name
