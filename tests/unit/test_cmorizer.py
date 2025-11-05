from unittest.mock import Mock, patch

import pytest

from pycmor.core.cmorizer import CMORizer
from pycmor.core.pipeline import TestingPipeline


@pytest.mark.skip
def test_parallel_process(CMIP_Tables_Dir):
    # Create a mock client
    mock_client = Mock()

    # Mock the submit method to return a known value
    mock_client.submit.return_value = "known_value"

    # Mock the gather method to return a list of known values
    mock_client.gather.return_value = ["known_value" for _ in range(5)]  # assuming there are 5 rules

    # Use patch to replace Client with our mock_client in the context of this test
    with patch("pycmor.cmorizer.Client", return_value=mock_client):
        pycmor_cfg = {"parallel": True}
        general_cfg = {"CMIP_Tables_Dir": CMIP_Tables_Dir}
        pipelines_cfg = [TestingPipeline()]
        rules_cfg = [{"name": f"rule_{i}", "cmor_variable": ["tas"], "input_patterns": [".*"]} for i in range(5)]
        cmorizer = CMORizer(pycmor_cfg, general_cfg, pipelines_cfg, rules_cfg)
        results = cmorizer.parallel_process()

    # Check that submit was called once for each rule
    assert mock_client.submit.call_count == len(cmorizer.rules)

    # Check that the results are as expected
    assert results == ["known_value" for _ in range(len(cmorizer.rules))]
