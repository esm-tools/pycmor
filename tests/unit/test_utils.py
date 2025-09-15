import os

import pytest

from pycmor.core.utils import generate_partial_function, get_callable_by_name


def test_get_callable_by_name_with_function():
    assert get_callable_by_name("os.path.join") == os.path.join


def test_get_callable_by_name_with_class_method():
    assert get_callable_by_name("os.path.abspath") == os.path.abspath


def test_get_callable_by_name_with_nested_callable():
    assert (
        get_callable_by_name("os.path.supports_unicode_filenames")
        == os.path.supports_unicode_filenames
    )


def test_get_callable_with_from_import():
    assert (
        get_callable_by_name("pycmor.core.utils.get_callable_by_name")
        == get_callable_by_name
    )


def test_get_callable_with_mini_from_import():
    with pytest.raises(ValueError):
        get_callable_by_name("get_callable_by_name") == get_callable_by_name


def test_get_callable_by_name_with_non_existent_callable():
    with pytest.raises(AttributeError):
        get_callable_by_name("os.path.non_existent")


def test_get_callable_by_name_with_non_existent_module():
    with pytest.raises(ImportError):
        get_callable_by_name("non_existent.join")


def test_get_callable_by_name_with_empty_string():
    with pytest.raises(ValueError):
        get_callable_by_name("")


def test_generate_partial_function_with_positional_args():
    def add(x, y, z):
        return x + y + z

    # This is not allowed:
    # partial_func = generate_partial_function(add, "y", 1, 2)
    with pytest.raises(ValueError):
        _ = generate_partial_function(add, "y", 1, 2)


def test_generate_partial_function_with_keyword_args():
    def add(x, y, z):
        return x + y + z

    partial_func = generate_partial_function(add, "y", z=1, x=2)
    assert partial_func(3) == 6


def test_generate_partial_function_with_missing_args():
    def add(x, y, z):
        return x + y + z

    with pytest.raises(ValueError):
        _ = generate_partial_function(add, "y", z=1)


def test_generate_partial_function_with_extra_args():
    def add(x, y, z):
        return x + y + z

    with pytest.raises(ValueError):
        _ = generate_partial_function(add, "y", 1, 2, 3, 4)
