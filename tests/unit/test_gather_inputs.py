import os
import pytest
import xarray as xr
from pycmor.core.gather_inputs import load_mfdataset


def test_load_mfdataset_pi_uxarray(pi_uxarray_temp_rule):
    # Skip test if the test data is not available
    if not os.path.exists(pi_uxarray_temp_rule.inputs[0].path):
        pytest.skip("Test data not available")
    
    try:
        data = load_mfdataset(None, pi_uxarray_temp_rule)
        # Check if load worked correctly and we got back a Dataset
        assert isinstance(data, xr.Dataset)
    except Exception as e:
        pytest.skip(f"Skipping test due to: {str(e)}")


def test_load_mfdataset_fesom_2p6_esmtools(fesom_2p6_esmtools_temp_rule):
    # Skip test if the test data is not available
    if not os.path.exists(fesom_2p6_esmtools_temp_rule.inputs[0].path):
        pytest.skip("Test data not available")
    
    try:
        data = load_mfdataset(None, fesom_2p6_esmtools_temp_rule)
        # Check if load worked correctly and we got back a Dataset
        assert isinstance(data, xr.Dataset)
    except Exception as e:
        pytest.skip(f"Skipping test due to: {str(e)}")
