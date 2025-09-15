from pycmor.std_lib.generic import sort_dimensions


def test_sort_dimensions(dummy_array, rule_with_unsorted_data):
    """Test to check that dimensions are sorted correctly"""

    dummy_array = sort_dimensions(dummy_array, rule_with_unsorted_data)

    assert dummy_array.dims == tuple(rule_with_unsorted_data.array_order)


def test_sort_dimensions_without_array_order_attr(dummy_array, rule_with_unsorted_data):
    """Test to check that dimensions are sorted correctly"""

    array_order = rule_with_unsorted_data.array_order
    del rule_with_unsorted_data.array_order

    dummy_array = sort_dimensions(dummy_array, rule_with_unsorted_data)

    assert dummy_array.dims == tuple(array_order)
