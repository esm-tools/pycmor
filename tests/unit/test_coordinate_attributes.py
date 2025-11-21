"""
Unit tests for coordinate attributes module.

Tests the setting of CF-compliant metadata on coordinate variables.
"""

from unittest.mock import Mock

import numpy as np
import xarray as xr

from pycmor.std_lib.coordinate_attributes import (
    _get_coordinate_metadata,
    _should_skip_coordinate,
    set_coordinate_attributes,
)


class TestGetCoordinateMetadata:
    """Test the _get_coordinate_metadata function."""

    def test_latitude_metadata(self):
        """Test latitude coordinate metadata."""
        metadata = _get_coordinate_metadata("latitude")
        assert metadata is not None
        assert metadata["standard_name"] == "latitude"
        assert metadata["units"] == "degrees_north"
        assert metadata["axis"] == "Y"

    def test_longitude_metadata(self):
        """Test longitude coordinate metadata."""
        metadata = _get_coordinate_metadata("longitude")
        assert metadata is not None
        assert metadata["standard_name"] == "longitude"
        assert metadata["units"] == "degrees_east"
        assert metadata["axis"] == "X"

    def test_lat_short_name(self):
        """Test 'lat' short name maps to latitude."""
        metadata = _get_coordinate_metadata("lat")
        assert metadata is not None
        assert metadata["standard_name"] == "latitude"

    def test_lon_short_name(self):
        """Test 'lon' short name maps to longitude."""
        metadata = _get_coordinate_metadata("lon")
        assert metadata is not None
        assert metadata["standard_name"] == "longitude"

    def test_plev19_metadata(self):
        """Test plev19 pressure level metadata."""
        metadata = _get_coordinate_metadata("plev19")
        assert metadata is not None
        assert metadata["standard_name"] == "air_pressure"
        assert metadata["units"] == "Pa"
        assert metadata["positive"] == "down"
        assert metadata["axis"] == "Z"

    def test_olevel_metadata(self):
        """Test olevel ocean depth metadata."""
        metadata = _get_coordinate_metadata("olevel")
        assert metadata is not None
        assert metadata["standard_name"] == "depth"
        assert metadata["units"] == "m"
        assert metadata["positive"] == "down"
        assert metadata["axis"] == "Z"

    def test_alevel_metadata(self):
        """Test alevel atmosphere model level metadata."""
        metadata = _get_coordinate_metadata("alevel")
        assert metadata is not None
        assert (
            metadata["standard_name"] == "atmosphere_hybrid_sigma_pressure_coordinate"
        )
        assert metadata["axis"] == "Z"
        assert metadata["positive"] == "down"

    def test_unknown_coordinate(self):
        """Test unknown coordinate returns None."""
        metadata = _get_coordinate_metadata("unknown_coord")
        assert metadata is None

    def test_case_insensitive(self):
        """Test case-insensitive matching."""
        metadata = _get_coordinate_metadata("LATITUDE")
        assert metadata is not None
        assert metadata["standard_name"] == "latitude"


class TestShouldSkipCoordinate:
    """Test the _should_skip_coordinate function."""

    def test_skip_time(self):
        """Test that time coordinates are skipped."""
        rule = Mock()
        assert _should_skip_coordinate("time", rule) is True

    def test_skip_time_variants(self):
        """Test that time variants are skipped."""
        rule = Mock()
        assert _should_skip_coordinate("time1", rule) is True
        assert _should_skip_coordinate("time2", rule) is True
        assert _should_skip_coordinate("time-intv", rule) is True
        assert _should_skip_coordinate("time-point", rule) is True

    def test_skip_bounds(self):
        """Test that bounds variables are skipped."""
        rule = Mock()
        assert _should_skip_coordinate("lat_bnds", rule) is True
        assert _should_skip_coordinate("lon_bounds", rule) is True

    def test_dont_skip_regular_coords(self):
        """Test that regular coordinates are not skipped."""
        rule = Mock()
        assert _should_skip_coordinate("latitude", rule) is False
        assert _should_skip_coordinate("plev19", rule) is False


class TestSetCoordinateAttributes:
    """Test the set_coordinate_attributes function."""

    def test_set_lat_lon_attributes(self):
        """Test setting attributes on lat/lon coordinates."""
        # Create test dataset
        ds = xr.Dataset(
            {"tas": (["time", "lat", "lon"], np.random.rand(10, 90, 180))},
            coords={
                "time": np.arange(10),
                "lat": np.arange(-89.5, 90, 2),
                "lon": np.arange(0, 360, 2),
            },
        )

        # Mock rule
        rule = Mock()
        rule._pycmor_cfg = Mock(return_value=True)

        # Apply coordinate attributes
        ds = set_coordinate_attributes(ds, rule)

        # Check latitude attributes
        assert ds["lat"].attrs["standard_name"] == "latitude"
        assert ds["lat"].attrs["units"] == "degrees_north"
        assert ds["lat"].attrs["axis"] == "Y"

        # Check longitude attributes
        assert ds["lon"].attrs["standard_name"] == "longitude"
        assert ds["lon"].attrs["units"] == "degrees_east"
        assert ds["lon"].attrs["axis"] == "X"

    def test_set_plev_attributes(self):
        """Test setting attributes on pressure level coordinates."""
        # Create test dataset with plev19
        plev_values = np.array(
            [
                100000,
                92500,
                85000,
                70000,
                60000,
                50000,
                40000,
                30000,
                25000,
                20000,
                15000,
                10000,
                7000,
                5000,
                3000,
                2000,
                1000,
                500,
                100,
            ]
        )
        ds = xr.Dataset(
            {"ta": (["time", "plev19", "lat", "lon"], np.random.rand(10, 19, 90, 180))},
            coords={
                "time": np.arange(10),
                "plev19": plev_values,
                "lat": np.arange(-89.5, 90, 2),
                "lon": np.arange(0, 360, 2),
            },
        )

        # Mock rule
        rule = Mock()
        rule._pycmor_cfg = Mock(return_value=True)

        # Apply coordinate attributes
        ds = set_coordinate_attributes(ds, rule)

        # Check plev19 attributes
        assert ds["plev19"].attrs["standard_name"] == "air_pressure"
        assert ds["plev19"].attrs["units"] == "Pa"
        assert ds["plev19"].attrs["positive"] == "down"
        assert ds["plev19"].attrs["axis"] == "Z"

    def test_set_olevel_attributes(self):
        """Test setting attributes on ocean level coordinates."""
        # Create test dataset with olevel
        ds = xr.Dataset(
            {
                "thetao": (
                    ["time", "olevel", "lat", "lon"],
                    np.random.rand(10, 50, 90, 180),
                )
            },
            coords={
                "time": np.arange(10),
                "olevel": np.arange(0, 5000, 100),
                "lat": np.arange(-89.5, 90, 2),
                "lon": np.arange(0, 360, 2),
            },
        )

        # Mock rule
        rule = Mock()
        rule._pycmor_cfg = Mock(return_value=True)

        # Apply coordinate attributes
        ds = set_coordinate_attributes(ds, rule)

        # Check olevel attributes
        assert ds["olevel"].attrs["standard_name"] == "depth"
        assert ds["olevel"].attrs["units"] == "m"
        assert ds["olevel"].attrs["positive"] == "down"
        assert ds["olevel"].attrs["axis"] == "Z"

    def test_skip_time_coordinate(self):
        """Test that time coordinates are not modified."""
        # Create test dataset
        ds = xr.Dataset(
            {"tas": (["time", "lat", "lon"], np.random.rand(10, 90, 180))},
            coords={
                "time": np.arange(10),
                "lat": np.arange(-89.5, 90, 2),
                "lon": np.arange(0, 360, 2),
            },
        )

        # Mock rule
        rule = Mock()
        rule._pycmor_cfg = Mock(return_value=True)

        # Apply coordinate attributes
        ds = set_coordinate_attributes(ds, rule)

        # Time should not have attributes set (handled elsewhere)
        assert "standard_name" not in ds["time"].attrs

    def test_dataarray_input(self):
        """Test that DataArray input works correctly."""
        # Create test DataArray
        da = xr.DataArray(
            np.random.rand(10, 90, 180),
            dims=["time", "lat", "lon"],
            coords={
                "time": np.arange(10),
                "lat": np.arange(-89.5, 90, 2),
                "lon": np.arange(0, 360, 2),
            },
            name="tas",
        )

        # Mock rule
        rule = Mock()
        rule._pycmor_cfg = Mock(return_value=True)

        # Apply coordinate attributes
        result = set_coordinate_attributes(da, rule)

        # Should return DataArray
        assert isinstance(result, xr.DataArray)

        # Check attributes were set
        assert result["lat"].attrs["standard_name"] == "latitude"
        assert result["lon"].attrs["standard_name"] == "longitude"

    def test_disabled_via_config(self):
        """Test that coordinate attributes can be disabled via config."""
        # Create test dataset
        ds = xr.Dataset(
            {"tas": (["time", "lat", "lon"], np.random.rand(10, 90, 180))},
            coords={
                "time": np.arange(10),
                "lat": np.arange(-89.5, 90, 2),
                "lon": np.arange(0, 360, 2),
            },
        )

        # Mock rule with disabled config
        rule = Mock()
        rule._pycmor_cfg = Mock(return_value=False)

        # Apply coordinate attributes
        ds = set_coordinate_attributes(ds, rule)

        # Attributes should not be set
        assert "standard_name" not in ds["lat"].attrs
        assert "standard_name" not in ds["lon"].attrs

    def test_coordinates_attribute_on_data_var(self):
        """Test that 'coordinates' attribute is set on data variables."""
        # Create test dataset
        ds = xr.Dataset(
            {"tas": (["time", "lat", "lon"], np.random.rand(10, 90, 180))},
            coords={
                "time": np.arange(10),
                "lat": np.arange(-89.5, 90, 2),
                "lon": np.arange(0, 360, 2),
            },
        )

        # Mock rule
        rule = Mock()
        rule._pycmor_cfg = Mock(return_value=True)

        # Apply coordinate attributes
        ds = set_coordinate_attributes(ds, rule)

        # Check 'coordinates' attribute on data variable
        assert "coordinates" in ds["tas"].attrs
        coords_str = ds["tas"].attrs["coordinates"]
        assert "lat" in coords_str
        assert "lon" in coords_str
        assert "time" in coords_str

    def test_non_overriding(self):
        """Test that existing attributes are not overridden."""
        # Create test dataset with existing attributes
        ds = xr.Dataset(
            {"tas": (["time", "lat", "lon"], np.random.rand(10, 90, 180))},
            coords={
                "time": np.arange(10),
                "lat": np.arange(-89.5, 90, 2),
                "lon": np.arange(0, 360, 2),
            },
        )

        # Set custom attribute
        ds["lat"].attrs["standard_name"] = "custom_latitude"

        # Mock rule
        rule = Mock()
        rule._pycmor_cfg = Mock(return_value=True)

        # Apply coordinate attributes
        ds = set_coordinate_attributes(ds, rule)

        # Custom attribute should be preserved
        assert ds["lat"].attrs["standard_name"] == "custom_latitude"

    def test_multiple_vertical_coordinates(self):
        """Test dataset with multiple vertical coordinate types."""
        # Create test dataset with both plev and olevel
        ds = xr.Dataset(
            {
                "ta": (["time", "plev8", "lat", "lon"], np.random.rand(10, 8, 90, 180)),
                "thetao": (
                    ["time", "olevel", "lat", "lon"],
                    np.random.rand(10, 50, 90, 180),
                ),
            },
            coords={
                "time": np.arange(10),
                "plev8": np.array(
                    [100000, 85000, 70000, 50000, 25000, 10000, 5000, 1000]
                ),
                "olevel": np.arange(0, 5000, 100),
                "lat": np.arange(-89.5, 90, 2),
                "lon": np.arange(0, 360, 2),
            },
        )

        # Mock rule
        rule = Mock()
        rule._pycmor_cfg = Mock(return_value=True)

        # Apply coordinate attributes
        ds = set_coordinate_attributes(ds, rule)

        # Check plev8 attributes
        assert ds["plev8"].attrs["standard_name"] == "air_pressure"
        assert ds["plev8"].attrs["axis"] == "Z"

        # Check olevel attributes
        assert ds["olevel"].attrs["standard_name"] == "depth"
        assert ds["olevel"].attrs["axis"] == "Z"


class TestIntegrationScenarios:
    """Integration-style tests for realistic scenarios."""

    def test_cmip6_style_dataset(self):
        """Test with a CMIP6-style dataset structure."""
        # Create CMIP6-style dataset
        ds = xr.Dataset(
            {"tas": (["time", "lat", "lon"], np.random.rand(12, 90, 180))},
            coords={
                "time": np.arange(12),
                "lat": np.linspace(-89.5, 89.5, 90),
                "lon": np.linspace(0.5, 359.5, 180),
            },
        )

        # Mock rule
        rule = Mock()
        rule._pycmor_cfg = Mock(return_value=True)

        # Apply coordinate attributes
        ds = set_coordinate_attributes(ds, rule)

        # Verify CF compliance
        assert ds["lat"].attrs["standard_name"] == "latitude"
        assert ds["lat"].attrs["axis"] == "Y"
        assert ds["lon"].attrs["standard_name"] == "longitude"
        assert ds["lon"].attrs["axis"] == "X"
        assert "coordinates" in ds["tas"].attrs

    def test_cmip7_style_dataset_with_plev(self):
        """Test with a CMIP7-style dataset with pressure levels."""
        # Create CMIP7-style dataset
        plev_values = np.array([100000, 92500, 85000, 70000, 60000, 50000, 40000])
        ds = xr.Dataset(
            {"ta": (["time", "plev7", "lat", "lon"], np.random.rand(12, 7, 90, 180))},
            coords={
                "time": np.arange(12),
                "plev7": plev_values,
                "lat": np.linspace(-89.5, 89.5, 90),
                "lon": np.linspace(0.5, 359.5, 180),
            },
        )

        # Mock rule
        rule = Mock()
        rule._pycmor_cfg = Mock(return_value=True)

        # Apply coordinate attributes
        ds = set_coordinate_attributes(ds, rule)

        # Verify all coordinates have proper attributes
        assert ds["plev7"].attrs["standard_name"] == "air_pressure"
        assert ds["plev7"].attrs["units"] == "Pa"
        assert ds["plev7"].attrs["positive"] == "down"
        assert ds["plev7"].attrs["axis"] == "Z"
        assert ds["lat"].attrs["axis"] == "Y"
        assert ds["lon"].attrs["axis"] == "X"


class TestValidationModes:
    """Test validation of existing coordinate metadata."""

    def test_validation_mode_ignore(self):
        """Test 'ignore' mode - silently keeps existing wrong values."""
        # Create dataset with wrong metadata
        ds = xr.Dataset(
            {"tas": (["time", "lat", "lon"], np.random.rand(10, 90, 180))},
            coords={
                "time": np.arange(10),
                "lat": np.arange(-89.5, 90, 2),
                "lon": np.arange(0, 360, 2),
            },
        )
        # Set wrong metadata
        ds["lat"].attrs["standard_name"] = "wrong_name"
        ds["lat"].attrs["units"] = "meters"

        # Mock rule with 'ignore' mode
        rule = Mock()
        rule._pycmor_cfg = Mock(
            side_effect=lambda key: {
                "xarray_set_coordinate_attributes": True,
                "xarray_set_coordinates_attribute": True,
                "xarray_validate_coordinate_attributes": "ignore",
            }.get(key, True)
        )

        # Apply coordinate attributes
        ds = set_coordinate_attributes(ds, rule)

        # Wrong values should be preserved
        assert ds["lat"].attrs["standard_name"] == "wrong_name"
        assert ds["lat"].attrs["units"] == "meters"
        # But axis should be added (wasn't present)
        assert ds["lat"].attrs["axis"] == "Y"

    def test_validation_mode_warn(self):
        """Test 'warn' mode - logs warning and keeps existing values."""
        # Create dataset with wrong metadata
        ds = xr.Dataset(
            {"tas": (["time", "lat", "lon"], np.random.rand(10, 90, 180))},
            coords={
                "time": np.arange(10),
                "lat": np.arange(-89.5, 90, 2),
                "lon": np.arange(0, 360, 2),
            },
        )
        ds["lat"].attrs["standard_name"] = "wrong_name"

        # Mock rule with 'warn' mode (default)
        rule = Mock()
        rule._pycmor_cfg = Mock(
            side_effect=lambda key: {
                "xarray_set_coordinate_attributes": True,
                "xarray_set_coordinates_attribute": True,
                "xarray_validate_coordinate_attributes": "warn",
            }.get(key, True)
        )

        # Apply coordinate attributes (should log warning)
        ds = set_coordinate_attributes(ds, rule)

        # Wrong value should be preserved
        assert ds["lat"].attrs["standard_name"] == "wrong_name"
        # Other attributes should be added
        assert ds["lat"].attrs["units"] == "degrees_north"
        assert ds["lat"].attrs["axis"] == "Y"

    def test_validation_mode_error(self):
        """Test 'error' mode - raises ValueError on mismatch."""
        # Create dataset with wrong metadata
        ds = xr.Dataset(
            {"tas": (["time", "lat", "lon"], np.random.rand(10, 90, 180))},
            coords={
                "time": np.arange(10),
                "lat": np.arange(-89.5, 90, 2),
                "lon": np.arange(0, 360, 2),
            },
        )
        ds["lat"].attrs["standard_name"] = "wrong_name"

        # Mock rule with 'error' mode
        rule = Mock()
        rule._pycmor_cfg = Mock(
            side_effect=lambda key: {
                "xarray_set_coordinate_attributes": True,
                "xarray_set_coordinates_attribute": True,
                "xarray_validate_coordinate_attributes": "error",
            }.get(key, True)
        )

        # Should raise ValueError
        try:
            set_coordinate_attributes(ds, rule)
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "Invalid standard_name" in str(e)
            assert "lat" in str(e)
            assert "wrong_name" in str(e)
            assert "latitude" in str(e)

    def test_validation_mode_fix(self):
        """Test 'fix' mode - overwrites wrong values with correct ones."""
        # Create dataset with wrong metadata
        ds = xr.Dataset(
            {"tas": (["time", "lat", "lon"], np.random.rand(10, 90, 180))},
            coords={
                "time": np.arange(10),
                "lat": np.arange(-89.5, 90, 2),
                "lon": np.arange(0, 360, 2),
            },
        )
        ds["lat"].attrs["standard_name"] = "wrong_name"
        ds["lat"].attrs["units"] = "meters"

        # Mock rule with 'fix' mode
        rule = Mock()
        rule._pycmor_cfg = Mock(
            side_effect=lambda key: {
                "xarray_set_coordinate_attributes": True,
                "xarray_set_coordinates_attribute": True,
                "xarray_validate_coordinate_attributes": "fix",
            }.get(key, True)
        )

        # Apply coordinate attributes
        ds = set_coordinate_attributes(ds, rule)

        # Wrong values should be corrected
        assert ds["lat"].attrs["standard_name"] == "latitude"
        assert ds["lat"].attrs["units"] == "degrees_north"
        assert ds["lat"].attrs["axis"] == "Y"

    def test_validation_correct_existing_metadata(self):
        """Test that correct existing metadata is preserved without warnings."""
        # Create dataset with correct metadata
        ds = xr.Dataset(
            {"tas": (["time", "lat", "lon"], np.random.rand(10, 90, 180))},
            coords={
                "time": np.arange(10),
                "lat": np.arange(-89.5, 90, 2),
                "lon": np.arange(0, 360, 2),
            },
        )
        # Set correct metadata
        ds["lat"].attrs["standard_name"] = "latitude"
        ds["lat"].attrs["units"] = "degrees_north"

        # Mock rule
        rule = Mock()
        rule._pycmor_cfg = Mock(
            side_effect=lambda key: {
                "xarray_set_coordinate_attributes": True,
                "xarray_set_coordinates_attribute": True,
                "xarray_validate_coordinate_attributes": "warn",
            }.get(key, True)
        )

        # Apply coordinate attributes (should not warn)
        ds = set_coordinate_attributes(ds, rule)

        # Correct values should be preserved
        assert ds["lat"].attrs["standard_name"] == "latitude"
        assert ds["lat"].attrs["units"] == "degrees_north"
        # Missing axis should be added
        assert ds["lat"].attrs["axis"] == "Y"

    def test_validation_partial_mismatch(self):
        """Test validation with some correct and some wrong attributes."""
        # Create dataset with mixed metadata
        ds = xr.Dataset(
            {"ta": (["time", "plev19", "lat", "lon"], np.random.rand(10, 19, 90, 180))},
            coords={
                "time": np.arange(10),
                "plev19": np.linspace(100000, 1000, 19),
                "lat": np.arange(-89.5, 90, 2),
                "lon": np.arange(0, 360, 2),
            },
        )
        # plev19: correct standard_name, wrong units
        ds["plev19"].attrs["standard_name"] = "air_pressure"
        ds["plev19"].attrs["units"] = "hPa"  # Should be Pa

        # Mock rule with 'fix' mode
        rule = Mock()
        rule._pycmor_cfg = Mock(
            side_effect=lambda key: {
                "xarray_set_coordinate_attributes": True,
                "xarray_set_coordinates_attribute": True,
                "xarray_validate_coordinate_attributes": "fix",
            }.get(key, True)
        )

        # Apply coordinate attributes
        ds = set_coordinate_attributes(ds, rule)

        # Correct value preserved, wrong value fixed
        assert ds["plev19"].attrs["standard_name"] == "air_pressure"
        assert ds["plev19"].attrs["units"] == "Pa"  # Corrected
        assert ds["plev19"].attrs["axis"] == "Z"  # Added
        assert ds["plev19"].attrs["positive"] == "down"  # Added
        assert ds["lat"].attrs["axis"] == "Y"
        assert ds["lon"].attrs["axis"] == "X"
