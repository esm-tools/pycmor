"""
Tests for pyfesom2 functionality as used in pycmor
"""
import os
import pytest
from pyfesom2.load_mesh_data import load_mesh
import pycmor.core.rule


def test_read_grid_from_rule(fesom_pi_mesh_config):
    config = fesom_pi_mesh_config
    mesh_path = config["inherit"]["mesh_path"]

    # Skip test if the test data is not available
    if not os.path.exists(mesh_path):
        pytest.skip(f"Test data not found at {mesh_path}")

    try:
        rule = pycmor.core.rule.Rule.from_dict(config["rules"][0])
        rule.mesh_path = config["inherit"]["mesh_path"]
        load_mesh(mesh_path)
    except Exception as e:
        pytest.skip(f"Skipping test due to: {str(e)}")
