"""
Tests for the unified pymor accessor functionality in accessors.py.

This module tests the PymorDataArrayAccessor and PymorDatasetAccessor classes
that provide unified access to all pymor functionality under the data.pymor
and dataset.pymor namespaces.
"""

import cftime
import pytest
import xarray as xr

# Import pymor to register all accessors
import pymor  # noqa: F401


@pytest.fixture
def regular_monthly_time():
    """Regular monthly time series for testing."""
    return [cftime.Datetime360Day(2000, m, 15) for m in range(1, 5)]


@pytest.fixture
def sample_dataarray(regular_monthly_time):
    """Sample DataArray with time dimension for testing."""
    return xr.DataArray(
        [1, 2, 3, 4],
        coords={"time": regular_monthly_time},
        dims="time",
        name="temperature",
    )


@pytest.fixture
def sample_dataset(sample_dataarray):
    """Sample Dataset with time dimension for testing."""
    return xr.Dataset({"tas": sample_dataarray, "pr": sample_dataarray * 2})


class TestPymorDataArrayAccessor:
    """Test the unified pymor accessor for DataArrays."""

    def test_pymor_accessor_registration(self, sample_dataarray):
        """Test that the pymor accessor is properly registered."""
        assert hasattr(sample_dataarray, "pymor")
        assert hasattr(
            sample_dataarray, "timefreq"
        )  # Specialized accessor still available

    def test_pymor_accessor_methods_available(self, sample_dataarray):
        """Test that all expected methods are available on the pymor accessor."""
        expected_methods = ["resample_safe", "check_resolution", "infer_frequency"]

        for method in expected_methods:
            assert hasattr(sample_dataarray.pymor, method)
            assert callable(getattr(sample_dataarray.pymor, method))

    def test_pymor_infer_frequency_delegation(self, sample_dataarray):
        """Test that pymor.infer_frequency delegates correctly to timefreq."""
        # Test via pymor accessor
        pymor_result = sample_dataarray.pymor.infer_frequency(log=False)

        # Test via specialized accessor
        timefreq_result = sample_dataarray.timefreq.infer_frequency(log=False)

        # Results should be identical
        assert pymor_result == timefreq_result
        assert pymor_result.frequency == "M"
        assert pymor_result.status == "valid"

    def test_pymor_check_resolution_delegation(self, sample_dataarray):
        """Test that pymor.check_resolution delegates correctly to timefreq."""
        target_interval = 30.0

        # Test via pymor accessor
        pymor_result = sample_dataarray.pymor.check_resolution(
            target_approx_interval=target_interval, calendar="360_day", log=False
        )

        # Test via specialized accessor
        timefreq_result = sample_dataarray.timefreq.check_resolution(
            target_approx_interval=target_interval, calendar="360_day", log=False
        )

        # Results should be identical
        assert pymor_result == timefreq_result
        assert "is_valid_for_resampling" in pymor_result
        assert "comparison_status" in pymor_result

    def test_pymor_resample_safe_delegation(self, sample_dataarray):
        """Test that pymor.resample_safe delegates correctly to timefreq."""
        # Test via pymor accessor
        pymor_result = sample_dataarray.pymor.resample_safe(
            target_approx_interval=30.0, calendar="360_day"
        )

        # Test via specialized accessor
        timefreq_result = sample_dataarray.timefreq.resample_safe(
            target_approx_interval=30.0, calendar="360_day"
        )

        # Results should be equivalent DataArrays
        assert isinstance(pymor_result, xr.DataArray)
        assert isinstance(timefreq_result, xr.DataArray)
        assert pymor_result.dims == timefreq_result.dims
        assert pymor_result.name == timefreq_result.name

    def test_pymor_resample_safe_with_freq_str(self, sample_dataarray):
        """Test pymor.resample_safe with frequency string parameter."""
        result = sample_dataarray.pymor.resample_safe(freq_str="M", calendar="360_day")

        assert isinstance(result, xr.DataArray)
        assert "time" in result.dims
        assert result.name == sample_dataarray.name

    def test_pymor_resample_safe_parameter_flexibility(self, sample_dataarray):
        """Test that pymor.resample_safe accepts flexible parameter combinations."""
        # Test with target_approx_interval only
        result1 = sample_dataarray.pymor.resample_safe(
            target_approx_interval=30.0, calendar="360_day"
        )

        # Test with freq_str only
        result2 = sample_dataarray.pymor.resample_safe(freq_str="M", calendar="360_day")

        # Test with both parameters
        result3 = sample_dataarray.pymor.resample_safe(
            target_approx_interval=30.0, freq_str="M", calendar="360_day"
        )

        # All should produce valid results
        for result in [result1, result2, result3]:
            assert isinstance(result, xr.DataArray)
            assert "time" in result.dims

    def test_pymor_accessor_error_handling(self):
        """Test that pymor accessor handles errors appropriately."""
        # Create DataArray without time dimension
        da_no_time = xr.DataArray([1, 2, 3], dims=["x"])

        # Should raise appropriate error when trying to use time-based methods
        with pytest.raises((ValueError, KeyError)):
            da_no_time.pymor.infer_frequency()

    def test_pymor_accessor_docstrings(self, sample_dataarray):
        """Test that pymor accessor methods have proper docstrings."""
        methods = ["resample_safe", "check_resolution", "infer_frequency"]

        for method_name in methods:
            method = getattr(sample_dataarray.pymor, method_name)
            assert method.__doc__ is not None
            assert len(method.__doc__.strip()) > 0
            # Should reference the specialized accessor documentation
            assert "TimeFrequencyAccessor" in method.__doc__


class TestPymorDatasetAccessor:
    """Test the unified pymor accessor for Datasets."""

    def test_pymor_accessor_registration(self, sample_dataset):
        """Test that the pymor accessor is properly registered for datasets."""
        assert hasattr(sample_dataset, "pymor")
        assert hasattr(
            sample_dataset, "timefreq"
        )  # Specialized accessor still available

    def test_pymor_accessor_methods_available(self, sample_dataset):
        """Test that all expected methods are available on the dataset pymor accessor."""
        expected_methods = ["resample_safe", "check_resolution", "infer_frequency"]

        for method in expected_methods:
            assert hasattr(sample_dataset.pymor, method)
            assert callable(getattr(sample_dataset.pymor, method))

    def test_pymor_infer_frequency_delegation(self, sample_dataset):
        """Test that dataset pymor.infer_frequency delegates correctly."""
        # Test via pymor accessor
        pymor_result = sample_dataset.pymor.infer_frequency(log=False)

        # Test via specialized accessor
        timefreq_result = sample_dataset.timefreq.infer_frequency(log=False)

        # Results should be identical
        assert pymor_result == timefreq_result
        assert pymor_result.frequency == "M"
        assert pymor_result.status == "valid"

    def test_pymor_check_resolution_delegation(self, sample_dataset):
        """Test that dataset pymor.check_resolution delegates correctly."""
        target_interval = 30.0

        # Test via pymor accessor
        pymor_result = sample_dataset.pymor.check_resolution(
            target_approx_interval=target_interval, calendar="360_day", log=False
        )

        # Test via specialized accessor
        timefreq_result = sample_dataset.timefreq.check_resolution(
            target_approx_interval=target_interval, calendar="360_day", log=False
        )

        # Results should be identical
        assert pymor_result == timefreq_result
        assert "is_valid_for_resampling" in pymor_result

    def test_pymor_resample_safe_delegation(self, sample_dataset):
        """Test that dataset pymor.resample_safe delegates correctly."""
        # Test via pymor accessor
        pymor_result = sample_dataset.pymor.resample_safe(
            target_approx_interval=30.0, calendar="360_day"
        )

        # Test via specialized accessor
        timefreq_result = sample_dataset.timefreq.resample_safe(
            target_approx_interval=30.0, calendar="360_day"
        )

        # Results should be equivalent Datasets
        assert isinstance(pymor_result, xr.Dataset)
        assert isinstance(timefreq_result, xr.Dataset)
        assert set(pymor_result.data_vars) == set(timefreq_result.data_vars)
        assert pymor_result.dims == timefreq_result.dims

    def test_pymor_resample_safe_preserves_variables(self, sample_dataset):
        """Test that dataset pymor.resample_safe preserves all data variables."""
        result = sample_dataset.pymor.resample_safe(freq_str="M", calendar="360_day")

        assert isinstance(result, xr.Dataset)
        assert set(result.data_vars) == set(sample_dataset.data_vars)
        assert "tas" in result.data_vars
        assert "pr" in result.data_vars

    def test_pymor_dataset_error_handling(self):
        """Test that dataset pymor accessor handles errors appropriately."""
        # Create Dataset without time dimension
        ds_no_time = xr.Dataset(
            {
                "var1": xr.DataArray([1, 2, 3], dims=["x"]),
                "var2": xr.DataArray([4, 5, 6], dims=["x"]),
            }
        )

        # Should raise appropriate error when trying to use time-based methods
        with pytest.raises((ValueError, KeyError)):
            ds_no_time.pymor.infer_frequency()

    def test_pymor_dataset_docstrings(self, sample_dataset):
        """Test that dataset pymor accessor methods have proper docstrings."""
        methods = ["resample_safe", "check_resolution", "infer_frequency"]

        for method_name in methods:
            method = getattr(sample_dataset.pymor, method_name)
            assert method.__doc__ is not None
            assert len(method.__doc__.strip()) > 0
            # Should reference the specialized accessor documentation
            assert "DatasetFrequencyAccessor" in method.__doc__


class TestAccessorInteroperability:
    """Test interoperability between specialized and unified accessors."""

    def test_both_accessors_coexist(self, sample_dataarray, sample_dataset):
        """Test that both specialized and unified accessors work together."""
        # DataArray
        assert hasattr(sample_dataarray, "timefreq")
        assert hasattr(sample_dataarray, "pymor")

        # Dataset
        assert hasattr(sample_dataset, "timefreq")
        assert hasattr(sample_dataset, "pymor")

    def test_consistent_results_across_accessors(self, sample_dataarray):
        """Test that specialized and unified accessors give consistent results."""
        # Test infer_frequency
        timefreq_freq = sample_dataarray.timefreq.infer_frequency(log=False)
        pymor_freq = sample_dataarray.pymor.infer_frequency(log=False)
        assert timefreq_freq == pymor_freq

        # Test check_resolution
        timefreq_check = sample_dataarray.timefreq.check_resolution(
            target_approx_interval=30.0, calendar="360_day", log=False
        )
        pymor_check = sample_dataarray.pymor.check_resolution(
            target_approx_interval=30.0, calendar="360_day", log=False
        )
        assert timefreq_check == pymor_check

    def test_unified_accessor_initialization(self, sample_dataarray, sample_dataset):
        """Test that unified accessors initialize their internal specialized accessors."""
        # Check that internal _timefreq accessor is properly initialized
        da_pymor = sample_dataarray.pymor
        assert hasattr(da_pymor, "_timefreq")
        assert da_pymor._timefreq is not None

        ds_pymor = sample_dataset.pymor
        assert hasattr(ds_pymor, "_timefreq")
        assert ds_pymor._timefreq is not None

    def test_accessor_independence(self, sample_dataarray):
        """Test that accessors operate independently without interference."""
        # Modify data through one accessor
        result1 = sample_dataarray.timefreq.resample_safe(
            target_approx_interval=30.0, calendar="360_day"
        )

        # Use the other accessor - should not be affected
        result2 = sample_dataarray.pymor.resample_safe(
            target_approx_interval=30.0, calendar="360_day"
        )

        # Original data should be unchanged
        assert len(sample_dataarray) == 4

        # Results should be equivalent
        assert isinstance(result1, xr.DataArray)
        assert isinstance(result2, xr.DataArray)


class TestAccessorRegistration:
    """Test that accessors are properly registered through the accessors.py module."""

    def test_import_registers_accessors(self):
        """Test that importing pymor registers all accessors."""
        # Create test data
        times = [cftime.Datetime360Day(2000, m, 15) for m in range(1, 4)]
        da = xr.DataArray([1, 2, 3], coords={"time": times}, dims="time")
        ds = xr.Dataset({"var": da})

        # Both specialized and unified accessors should be available
        assert hasattr(da, "timefreq")
        assert hasattr(da, "pymor")
        assert hasattr(ds, "timefreq")
        assert hasattr(ds, "pymor")

    def test_accessor_namespace_separation(self, sample_dataarray):
        """Test that accessor namespaces are properly separated."""
        # timefreq and pymor should be different objects
        assert sample_dataarray.timefreq is not sample_dataarray.pymor

        # But pymor should delegate to timefreq functionality
        assert hasattr(sample_dataarray.pymor, "_timefreq")

    def test_future_extensibility(self, sample_dataarray):
        """Test that the unified accessor is designed for future extensibility."""
        # The unified accessor should have a clear structure for adding new features
        pymor_accessor = sample_dataarray.pymor

        # Should have the current timefreq methods
        assert hasattr(pymor_accessor, "resample_safe")
        assert hasattr(pymor_accessor, "check_resolution")
        assert hasattr(pymor_accessor, "infer_frequency")

        # Should have internal structure that supports adding more specialized accessors
        assert hasattr(pymor_accessor, "_timefreq")
        # Future: assert hasattr(pymor_accessor, '_other_accessor')
