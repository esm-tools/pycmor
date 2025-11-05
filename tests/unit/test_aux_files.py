# import pytest
from pyfesom2.load_mesh_data import fesom_mesh

from pycmor.core.aux_files import attach_files_to_rule


def test_aux_files_attach_without_aux(pi_uxarray_temp_rule):
    rule = pi_uxarray_temp_rule
    attach_files_to_rule(rule)
    assert rule.aux == {}


def test_aux_files_attach_simple_file(pi_uxarray_temp_rule, tmp_path):
    # Create a temporary file
    temp_file = tmp_path / "temp_file.txt"
    temp_file.write_text("Hello, pytest!")

    rule = pi_uxarray_temp_rule
    rule.aux = [
        {
            "name": "aux1",
            "path": str(temp_file),
        },
    ]
    attach_files_to_rule(rule)
    assert rule.aux == {"aux1": "Hello, pytest!"}


def test_aux_files_attach_fesom_mesh(fesom_2p6_esmtools_temp_rule, fesom_2p6_pimesh_esm_tools_data):
    mesh = fesom_2p6_pimesh_esm_tools_data / "input/fesom/mesh/pi"
    rule = fesom_2p6_esmtools_temp_rule
    rule.aux = [
        {
            "name": "mesh",
            "path": str(mesh),
            "loader": "pyfesom2.load_mesh_data.load_mesh",
        },
    ]
    attach_files_to_rule(rule)
    print(f'PG DEBUG >>> {rule.aux["mesh"]}')
    assert isinstance(rule.aux["mesh"], fesom_mesh)
