import pytest
import requests

from pycmor.data_request.collection import CMIP6DataRequest, CMIP7DataRequest


def test_cmip6_from_git():
    try:
        request = CMIP6DataRequest.from_git()
    except requests.exceptions.HTTPError as e:
        # If we get 429, it's because we're being rate limited.
        # In that case, mark this test as skipped:
        pytest.skip(f"Rate limited: {e}")
    # If the function worked, we should get tables:
    assert request.tables


def test_cmip7_from_vendored_json():
    request = CMIP7DataRequest.from_vendored_json()
    # If the function worked, we should get tables:
    assert request.tables
    # And we should get variables:
    assert request.variables
