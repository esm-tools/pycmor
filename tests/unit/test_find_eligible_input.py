import os
import pathlib
import re

import pytest

from pycmor.core.gather_inputs import (
    _files_to_string,
    _filter_by_year,
    _input_files_in_path,
    _input_pattern_from_env,
    _resolve_symlinks,
    _sort_by_year,
    _validate_rule_has_marked_regex,
)


@pytest.mark.parametrize(
    "config, expected_pattern, expected_output",
    [
        (
            "config_empty",  # Matches any input
            re.compile(".*"),
            [
                pathlib.Path("/path/to/test1.txt"),
                pathlib.Path("/path/to/test2.txt"),
                pathlib.Path("/path/to/fesom_test.nc"),
                pathlib.Path("/path/to/other_test123.nc"),
                pathlib.Path("/path/to/test.nc"),
            ],
        ),
        (
            "config_pattern_env_var_name",
            re.compile(".*"),
            [
                pathlib.Path("/path/to/test1.txt"),
                pathlib.Path("/path/to/test2.txt"),
                pathlib.Path("/path/to/fesom_test.nc"),
                pathlib.Path("/path/to/other_test123.nc"),
                pathlib.Path("/path/to/test.nc"),
            ],
        ),
        (
            "config_pattern_env_var_value",
            re.compile("test.*nc"),
            [pathlib.Path("/path/to/test.nc")],
        ),
        (
            "config_pattern_env_var_name_and_value",
            re.compile("other_test.*nc"),
            [pathlib.Path("/path/to/other_test123.nc")],
        ),
    ],
    indirect=[  # This tells pytest to treat 'config' as a fixture
        "config",
    ],
)
def test_listing_function(config, expected_pattern, expected_output, fs_basic):
    pattern = _input_pattern_from_env(config)
    assert pattern == expected_pattern
    output = _input_files_in_path("/path/to", pattern)
    assert set(expected_output) == set(output)


@pytest.mark.parametrize(
    "config", ["config_empty", "config_pattern_env_var_name"], indirect=True
)
@pytest.mark.parametrize("env", ["env_empty"], indirect=True)
def test_default_pattern(config, env):
    pattern = _input_pattern_from_env(config)
    assert isinstance(pattern, re.Pattern)
    assert pattern.match("test")


@pytest.mark.parametrize(
    "config", ["config_empty", "config_pattern_env_var_name"], indirect=True
)
@pytest.mark.parametrize("env", ["env_empty"], indirect=True)
def test_custom_pattern_name(config, env):
    os.environ["CMOR_PATTERN"] = "test.*"
    os.environ["PYCMOR_INPUT_PATTERN"] = "test.*"
    pattern = _input_pattern_from_env(config)
    assert isinstance(pattern, re.Pattern)
    assert pattern.match("test123")
    assert not pattern.match("123test")


@pytest.mark.parametrize("env", ["env_empty"], indirect=True)
def test_custom_pattern_value(config_pattern_env_var_value, env):
    pattern = _input_pattern_from_env(config_pattern_env_var_value)
    assert isinstance(pattern, re.Pattern)
    assert pattern.match("test1.nc")
    assert pattern.match("test.nc")
    assert not pattern.match("something_test.nc")


@pytest.mark.parametrize("env", ["env_empty"], indirect=True)
def test_custom_both(config_pattern_env_var_name_and_value, env):
    config = config_pattern_env_var_name_and_value
    pattern = _input_pattern_from_env(config)
    assert isinstance(pattern, re.Pattern)
    assert pattern.match("other_test123.nc")
    os.environ["CMOR_PATTERN"] = "test.*"
    os.environ["PYCMOR_INPUT_PATTERN"] = "test.*"
    pattern = _input_pattern_from_env(config)
    assert isinstance(pattern, re.Pattern)
    assert pattern.match("test123")
    assert not pattern.match("123test")


@pytest.mark.parametrize(
    "config",
    [
        "config_empty",
        "config_pattern_env_var_name",
        "config_pattern_env_var_value",
        "config_pattern_env_var_name_and_value",
    ],
    indirect=True,
)
@pytest.mark.parametrize("fs", ["fs_basic", "fs_with_symlinks"], indirect=True)
@pytest.mark.parametrize("env", ["env_empty"], indirect=True)
def test_env_var_no_match(config, fs, env):
    os.environ["CMOR_PATTERN"] = "no_match*"
    os.environ["PYCMOR_INPUT_PATTERN"] = "no_match*"
    pattern = _input_pattern_from_env(config)
    output = _input_files_in_path("/path/to", pattern)
    assert output == []


# def test_env_var_partial_match(
#     config.pattern_env_var_name, fake_filesystem.basic, environment
# ):
#     os.environ["CMOR_PATTERN"] = "test1.*"
#     pattern = _input_pattern_from_env(config.pattern_env_var_name)
#     output = _input_files_in_path("/path/to", pattern)
#     assert output == [pathlib.Path("/path/to/test1.txt")]


# def test_nonexistent_path(config.empty, fake_filesystem.basic, environment):
#     pattern = _input_pattern_from_env(config.empty)
#     with pytest.raises(FileNotFoundError):
#         _input_files_in_path("/nonexistent/path", pattern)


# def test_empty_directory(config.empty, fake_filesystem.basic, environment):
#     fake_filesystem.fs.create_dir("/empty/path")
#     pattern = _input_pattern_from_env(config.empty)
#     output = _input_files_in_path("/empty/path", pattern)
#     assert output == []


@pytest.mark.parametrize(
    "config", ["config_empty", "config_pattern_env_var_name"], indirect=True
)
@pytest.mark.xfail(reason="subdirectories are not supported")
def test_subdirectories_should_fail(config, fs_with_subdirs):
    pattern = _input_pattern_from_env(config)
    output = _input_files_in_path("/path/to", pattern)
    assert output == [
        pathlib.Path("/path/to/test1.txt"),
        pathlib.Path("/path/to/test2.txt"),
        pathlib.Path("/path/to/fesom_test.nc"),
        pathlib.Path("/path/to/other_test123.nc"),
        pathlib.Path("/path/to/subdir/test3.txt"),
    ]


def test__resolve_symlinks(fs_with_symlinks):
    files = [pathlib.Path("/path/to/file1"), pathlib.Path("/path/to/symlink")]
    resolved_files = _resolve_symlinks(files)
    assert resolved_files == [
        pathlib.Path("/path/to/file1"),
        pathlib.Path("/path/to/actual/file"),
    ]


def test__resolve_symlinks_raises_type_error():
    with pytest.raises(TypeError):
        _resolve_symlinks(["not", "paths"])


def test__sort_by_year(fs_with_datestamps_years):
    # Arrange
    files = [pathlib.Path(f"/path/to/file_{year}.txt") for year in range(2000, 2010)]
    # Shuffle the list of files
    import random

    random.shuffle(files)
    fpattern = re.compile(r".*(?P<year>\d{4}).*")  # noqa: W605

    # Act
    sorted_files = _sort_by_year(files, fpattern)

    # Assert
    assert sorted_files == [
        pathlib.Path(f"/path/to/file_{year}.txt") for year in range(2000, 2010)
    ]


def test__files_to_string():
    # Arrange
    files = [pathlib.Path("path/to/file1"), pathlib.Path("path/to/file2")]
    expected_output = "path/to/file1,path/to/file2"

    # Act
    output = _files_to_string(files)

    # Assert
    assert output == expected_output


def test__files_to_string_with_custom_separator():
    # Arrange
    files = [pathlib.Path("path/to/file1"), pathlib.Path("path/to/file2")]
    expected_output = "path/to/file1 - path/to/file2"
    separator = " - "

    # Act
    output = _files_to_string(files, separator)

    # Assert
    assert output == expected_output


def test__validate_rule_has_marked_regex_with_required_mark():
    rule = {"pattern": "test(?P<year>[0-9]{4})"}
    assert _validate_rule_has_marked_regex(rule) is True


def test__validate_rule_has_marked_regex_without_required_mark():
    rule = {"pattern": "test"}
    assert _validate_rule_has_marked_regex(rule) is False


def test__validate_rule_has_marked_regex_with_none_pattern():
    rule = {"pattern": None}
    assert _validate_rule_has_marked_regex(rule) is False


def test__validate_rule_has_marked_regex_with_multiple_required_marks():
    rule = {"pattern": "test(?P<year>[0-9]{4})(?P<month>[0-9]{2})"}
    assert _validate_rule_has_marked_regex(rule, ["year", "month"]) is True


def test__validate_rule_has_marked_regex_without_all_required_marks():
    rule = {"pattern": "test(?P<year>[0-9]{4})"}
    assert _validate_rule_has_marked_regex(rule, ["year", "month"]) is False


def test__filter_by_year(fs_with_datestamps_years):
    """Test the _filter_by_year function."""
    fake_files = [
        pathlib.Path(f"/path/to/file_{year}.txt") for year in range(2000, 2010)
    ]
    fpattern = re.compile(r"file_(?P<year>\d{4})\.txt")  # noqa: W605

    # Test filtering files from 2010 to 2015
    filtered_files = _filter_by_year(fake_files, fpattern, 2000, 2005)
    assert len(filtered_files) == 6
    assert all(
        2000 <= int(fpattern.match(f.name).group("year")) <= 2005
        for f in filtered_files
    )

    # Test filtering files from 2005 to 2005 (only one year)
    filtered_files = _filter_by_year(fake_files, fpattern, 2005, 2005)
    assert len(filtered_files) == 1
    assert int(fpattern.match(filtered_files[0].name).group("year")) == 2005

    # Test filtering with no matching files
    filtered_files = _filter_by_year(fake_files, fpattern, 2025, 2030)
    assert len(filtered_files) == 0
