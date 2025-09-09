"""
Tests for pyfesom2 functionality as used in pycmor
"""

from pyfesom2.load_mesh_data import load_mesh

import pycmor.core.rule


def test_read_grid_from_rule(fesom_pi_mesh_config):
    config = fesom_pi_mesh_config
    mesh_path = config["inherit"]["mesh_file"]
    rule = pycmor.core.rule.Rule.from_dict(config["rules"][0])
    rule.mesh_file = config["inherit"]["mesh_file"]

    load_mesh(mesh_path)
