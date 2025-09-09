import xarray as xr

from pycmor.core.config import PyCMORConfigManager
from pycmor.std_lib.variable_attributes import set_variable_attrs


def test_variable_attrs_dataarray(rule_after_cmip6_cmorizer_init):
    """Pseudo-integration test for the variable attributes of a DataArray"""
    # Set the fixture as the rule
    rule = rule_after_cmip6_cmorizer_init
    rule._pycmor_cfg = PyCMORConfigManager.from_pycmor_cfg({})
    # Set the DataArray
    da = xr.DataArray()
    # Set the variable attributes
    da = set_variable_attrs(da, rule)
    # Get the variable attributes
    d = da.attrs
    e = da.encoding
    assert e["_FillValue"] == 1.0e30
    for attr in [
        "standard_name",
        "long_name",
    ]:
        assert attr in d
