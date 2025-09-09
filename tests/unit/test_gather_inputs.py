import xarray as xr

from pycmor.core.gather_inputs import load_mfdataset


def test_load_mfdataset_pi_uxarray(pi_uxarray_temp_rule):
    data = load_mfdataset(None, pi_uxarray_temp_rule)
    # Check if load worked correctly and we got back a Dataset
    assert isinstance(data, xr.Dataset)


def test_load_mfdataset_fesom_2p6_esmtools(fesom_2p6_esmtools_temp_rule):
    data = load_mfdataset(None, fesom_2p6_esmtools_temp_rule)
    # Check if load worked correctly and we got back a Dataset
    assert isinstance(data, xr.Dataset)
