"""CMIP7DataRequest.from_directory finds the data request among other JSON files."""

from unittest.mock import patch

from pycmor.data_request.collection import CMIP7DataRequest


def test_all_var_info_is_read_whatever_lies_next_to_it(tmp_path):
    for name in ("CMIP7_coordinate.json", "CMIP7_formula_terms.json", "CMIP7_grids.json", "all_var_info.json"):
        (tmp_path / name).write_text("{}")
    with patch.object(CMIP7DataRequest, "from_json_file", return_value="request") as read:
        assert CMIP7DataRequest.from_directory(tmp_path) == "request"
    read.assert_called_once_with(tmp_path / "all_var_info.json")


def test_a_lone_file_of_another_name_still_works(tmp_path):
    (tmp_path / "metadata.json").write_text("{}")
    with patch.object(CMIP7DataRequest, "from_json_file", return_value="request") as read:
        assert CMIP7DataRequest.from_directory(tmp_path) == "request"
    read.assert_called_once_with(tmp_path / "metadata.json")
