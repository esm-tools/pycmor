"""
Unit tests for dimension mapping functionality
"""

from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr

from pycmor.std_lib.dimension_mapping import DimensionMapper, map_dimensions


class TestDimensionDetection:
    """Test dimension type detection"""

    def test_detect_latitude_by_name(self):
        """Test latitude detection by name pattern"""
        ds = xr.Dataset(coords={"latitude": np.linspace(-90, 90, 180)})
        mapper = DimensionMapper()

        dim_type = mapper.detect_dimension_type(ds, "latitude")
        assert dim_type == "latitude"

    def test_detect_latitude_by_short_name(self):
        """Test latitude detection by short name"""
        ds = xr.Dataset(coords={"lat": np.linspace(-90, 90, 180)})
        mapper = DimensionMapper()

        dim_type = mapper.detect_dimension_type(ds, "lat")
        assert dim_type == "latitude"

    def test_detect_longitude_by_name(self):
        """Test longitude detection by name pattern"""
        ds = xr.Dataset(coords={"longitude": np.linspace(0, 360, 360)})
        mapper = DimensionMapper()

        dim_type = mapper.detect_dimension_type(ds, "longitude")
        assert dim_type == "longitude"

    def test_detect_longitude_by_short_name(self):
        """Test longitude detection by short name"""
        ds = xr.Dataset(coords={"lon": np.linspace(0, 360, 360)})
        mapper = DimensionMapper()

        dim_type = mapper.detect_dimension_type(ds, "lon")
        assert dim_type == "longitude"

    def test_detect_pressure_by_name(self):
        """Test pressure detection by name pattern"""
        ds = xr.Dataset(coords={"lev": np.array([100000, 92500, 85000, 70000])})
        mapper = DimensionMapper()

        dim_type = mapper.detect_dimension_type(ds, "lev")
        assert dim_type == "pressure"

    def test_detect_pressure_by_values(self):
        """Test pressure detection by value range"""
        ds = xr.Dataset(coords={"level": np.array([1000, 925, 850, 700, 500])})
        mapper = DimensionMapper()

        dim_type = mapper.detect_dimension_type(ds, "level")
        # Should detect as pressure (hPa range)
        assert dim_type == "pressure"

    def test_detect_time_by_name(self):
        """Test time detection by name pattern"""
        ds = xr.Dataset(coords={"time": np.arange(10)})
        mapper = DimensionMapper()

        dim_type = mapper.detect_dimension_type(ds, "time")
        assert dim_type == "time"

    def test_detect_by_standard_name(self):
        """Test detection using standard_name attribute"""
        ds = xr.Dataset(
            coords={
                "y": (["y"], np.linspace(-90, 90, 180), {"standard_name": "latitude"})
            }
        )
        mapper = DimensionMapper()

        dim_type = mapper.detect_dimension_type(ds, "y")
        assert dim_type == "latitude"

    def test_detect_by_axis_attribute(self):
        """Test detection using axis attribute"""
        ds = xr.Dataset(coords={"y": (["y"], np.linspace(-90, 90, 180), {"axis": "Y"})})
        mapper = DimensionMapper()

        dim_type = mapper.detect_dimension_type(ds, "y")
        assert dim_type == "latitude"

    def test_detect_unknown_dimension(self):
        """Test that unknown dimensions return None"""
        ds = xr.Dataset(coords={"unknown_dim": np.arange(10)})
        mapper = DimensionMapper()

        dim_type = mapper.detect_dimension_type(ds, "unknown_dim")
        assert dim_type is None


class TestCMIPDimensionMapping:
    """Test mapping to CMIP dimension names"""

    def test_map_latitude_to_lat(self):
        """Test mapping latitude to lat"""
        mapper = DimensionMapper()
        cmip_dims = ["time", "lat", "lon"]

        cmip_dim = mapper.map_to_cmip_dimension("latitude", cmip_dims)
        assert cmip_dim == "lat"

    def test_map_longitude_to_lon(self):
        """Test mapping longitude to lon"""
        mapper = DimensionMapper()
        cmip_dims = ["time", "lat", "lon"]

        cmip_dim = mapper.map_to_cmip_dimension("longitude", cmip_dims)
        assert cmip_dim == "lon"

    def test_map_pressure_to_plev19(self):
        """Test mapping pressure to plev19 with size matching"""
        mapper = DimensionMapper()
        cmip_dims = ["time", "plev19", "lat", "lon"]

        cmip_dim = mapper.map_to_cmip_dimension("pressure", cmip_dims, coord_size=19)
        assert cmip_dim == "plev19"

    def test_map_pressure_to_plev8(self):
        """Test mapping pressure to plev8 with size matching"""
        mapper = DimensionMapper()
        cmip_dims = ["time", "plev8", "lat", "lon"]

        cmip_dim = mapper.map_to_cmip_dimension("pressure", cmip_dims, coord_size=8)
        assert cmip_dim == "plev8"

    def test_map_depth_to_olevel(self):
        """Test mapping depth to olevel"""
        mapper = DimensionMapper()
        cmip_dims = ["time", "olevel", "lat", "lon"]

        cmip_dim = mapper.map_to_cmip_dimension("depth", cmip_dims)
        assert cmip_dim == "olevel"

    def test_map_time_to_time(self):
        """Test mapping time to time"""
        mapper = DimensionMapper()
        cmip_dims = ["time", "lat", "lon"]

        cmip_dim = mapper.map_to_cmip_dimension("time", cmip_dims)
        assert cmip_dim == "time"

    def test_map_unknown_type_returns_none(self):
        """Test that unknown dimension types return None"""
        mapper = DimensionMapper()
        cmip_dims = ["time", "lat", "lon"]

        cmip_dim = mapper.map_to_cmip_dimension("unknown", cmip_dims)
        assert cmip_dim is None


class TestCreateMapping:
    """Test complete mapping creation"""

    def test_simple_lat_lon_mapping(self):
        """Test simple latitude/longitude mapping"""
        ds = xr.Dataset(
            coords={
                "time": np.arange(10),
                "latitude": np.linspace(-90, 90, 180),
                "longitude": np.linspace(0, 360, 360),
            }
        )

        # Mock data request variable
        drv = Mock()
        drv.dimensions = ("time", "lat", "lon")

        mapper = DimensionMapper()
        mapping = mapper.create_mapping(ds, drv)

        assert mapping["time"] == "time"
        assert mapping["latitude"] == "lat"
        assert mapping["longitude"] == "lon"

    def test_pressure_level_mapping(self):
        """Test pressure level mapping with size detection"""
        ds = xr.Dataset(
            coords={
                "time": np.arange(10),
                "lev": np.array(
                    [100000, 92500, 85000, 70000, 60000, 50000, 40000, 30000]
                ),
                "lat": np.linspace(-90, 90, 180),
                "lon": np.linspace(0, 360, 360),
            }
        )

        # Mock data request variable expecting plev8
        drv = Mock()
        drv.dimensions = ("time", "plev8", "lat", "lon")

        mapper = DimensionMapper()
        mapping = mapper.create_mapping(ds, drv)

        assert mapping["time"] == "time"
        assert mapping["lev"] == "plev8"
        assert mapping["lat"] == "lat"
        assert mapping["lon"] == "lon"

    def test_user_specified_mapping(self):
        """Test user-specified mapping overrides auto-detection"""
        ds = xr.Dataset(
            coords={
                "time": np.arange(10),
                "level": np.arange(19),
                "lat": np.linspace(-90, 90, 180),
                "lon": np.linspace(0, 360, 360),
            }
        )

        # Mock data request variable
        drv = Mock()
        drv.dimensions = ("time", "plev19", "lat", "lon")

        mapper = DimensionMapper()
        user_mapping = {"level": "plev19"}
        mapping = mapper.create_mapping(ds, drv, user_mapping=user_mapping)

        assert mapping["level"] == "plev19"

    def test_ocean_level_mapping(self):
        """Test ocean level mapping"""
        ds = xr.Dataset(
            coords={
                "time": np.arange(10),
                "depth": np.array([5, 15, 25, 50, 100, 200]),
                "lat": np.linspace(-90, 90, 180),
                "lon": np.linspace(0, 360, 360),
            }
        )

        # Mock data request variable
        drv = Mock()
        drv.dimensions = ("time", "olevel", "lat", "lon")

        mapper = DimensionMapper()
        mapping = mapper.create_mapping(ds, drv)

        assert mapping["time"] == "time"
        assert mapping["depth"] == "olevel"
        assert mapping["lat"] == "lat"
        assert mapping["lon"] == "lon"


class TestApplyMapping:
    """Test applying dimension mapping to datasets"""

    def test_apply_simple_mapping(self):
        """Test applying a simple dimension mapping"""
        ds = xr.Dataset(
            {
                "tas": (
                    ["time", "latitude", "longitude"],
                    np.random.rand(10, 180, 360),
                ),
            },
            coords={
                "time": np.arange(10),
                "latitude": np.linspace(-90, 90, 180),
                "longitude": np.linspace(0, 360, 360),
            },
        )

        mapper = DimensionMapper()
        mapping = {
            "time": "time",
            "latitude": "lat",
            "longitude": "lon",
        }

        ds_mapped = mapper.apply_mapping(ds, mapping)

        assert "lat" in ds_mapped.dims
        assert "lon" in ds_mapped.dims
        assert "latitude" not in ds_mapped.dims
        assert "longitude" not in ds_mapped.dims
        assert list(ds_mapped["tas"].dims) == ["time", "lat", "lon"]

    def test_apply_no_renaming_needed(self):
        """Test when no renaming is needed"""
        ds = xr.Dataset(
            {
                "tas": (["time", "lat", "lon"], np.random.rand(10, 180, 360)),
            },
            coords={
                "time": np.arange(10),
                "lat": np.linspace(-90, 90, 180),
                "lon": np.linspace(0, 360, 360),
            },
        )

        mapper = DimensionMapper()
        mapping = {
            "time": "time",
            "lat": "lat",
            "lon": "lon",
        }

        ds_mapped = mapper.apply_mapping(ds, mapping)

        # Should be unchanged
        assert list(ds_mapped.dims) == ["time", "lat", "lon"]

    def test_apply_pressure_level_mapping(self):
        """Test applying pressure level mapping"""
        ds = xr.Dataset(
            {
                "ta": (["time", "lev", "lat", "lon"], np.random.rand(10, 19, 180, 360)),
            },
            coords={
                "time": np.arange(10),
                "lev": np.arange(19),
                "lat": np.linspace(-90, 90, 180),
                "lon": np.linspace(0, 360, 360),
            },
        )

        mapper = DimensionMapper()
        mapping = {
            "time": "time",
            "lev": "plev19",
            "lat": "lat",
            "lon": "lon",
        }

        ds_mapped = mapper.apply_mapping(ds, mapping)

        assert "plev19" in ds_mapped.dims
        assert "lev" not in ds_mapped.dims
        assert list(ds_mapped["ta"].dims) == ["time", "plev19", "lat", "lon"]


class TestValidateMapping:
    """Test dimension mapping validation"""

    def test_validate_complete_mapping(self):
        """Test validation of complete mapping"""
        ds = xr.Dataset(
            coords={
                "time": np.arange(10),
                "lat": np.linspace(-90, 90, 180),
                "lon": np.linspace(0, 360, 360),
            }
        )

        drv = Mock()
        drv.dimensions = ("time", "lat", "lon")

        mapper = DimensionMapper()
        mapping = {"time": "time", "lat": "lat", "lon": "lon"}

        is_valid, errors = mapper.validate_mapping(ds, mapping, drv)

        assert is_valid
        assert len(errors) == 0

    def test_validate_incomplete_mapping(self):
        """Test validation catches incomplete mapping"""
        ds = xr.Dataset(
            coords={
                "time": np.arange(10),
                "lat": np.linspace(-90, 90, 180),
            }
        )

        drv = Mock()
        drv.dimensions = ("time", "lat", "lon")

        mapper = DimensionMapper()
        mapping = {"time": "time", "lat": "lat"}  # Missing lon

        is_valid, errors = mapper.validate_mapping(ds, mapping, drv)

        assert not is_valid
        assert len(errors) > 0
        assert any("lon" in str(e) for e in errors)

    def test_validate_missing_source_dimension(self):
        """Test validation catches missing source dimension"""
        ds = xr.Dataset(
            coords={
                "time": np.arange(10),
                "lat": np.linspace(-90, 90, 180),
            }
        )

        drv = Mock()
        drv.dimensions = ("time", "lat", "lon")

        mapper = DimensionMapper()
        mapping = {
            "time": "time",
            "lat": "lat",
            "longitude": "lon",
        }  # longitude doesn't exist

        is_valid, errors = mapper.validate_mapping(ds, mapping, drv)

        assert not is_valid
        assert any("longitude" in str(e) for e in errors)


class TestPipelineFunction:
    """Test the pipeline function wrapper"""

    def test_map_dimensions_with_dataset(self):
        """Test map_dimensions function with dataset"""
        ds = xr.Dataset(
            {
                "tas": (["time", "latitude", "longitude"], np.random.rand(10, 90, 180)),
            },
            coords={
                "time": np.arange(10),
                "latitude": np.linspace(-90, 90, 90),
                "longitude": np.linspace(0, 360, 180),
            },
        )

        # Mock rule
        rule = Mock()
        rule.data_request_variable = Mock()
        rule.data_request_variable.dimensions = ("time", "lat", "lon")
        rule._pycmor_cfg = Mock(
            side_effect=lambda key, default=None: {
                "xarray_enable_dimension_mapping": True,
                "dimension_mapping_validation": "warn",
                "dimension_mapping": {},
            }.get(key, default)
        )

        ds_mapped = map_dimensions(ds, rule)

        assert isinstance(ds_mapped, xr.Dataset)
        assert "lat" in ds_mapped.dims
        assert "lon" in ds_mapped.dims

    def test_map_dimensions_with_dataarray(self):
        """Test map_dimensions function with DataArray"""
        da = xr.DataArray(
            np.random.rand(10, 90, 180),
            dims=["time", "latitude", "longitude"],
            coords={
                "time": np.arange(10),
                "latitude": np.linspace(-90, 90, 90),
                "longitude": np.linspace(0, 360, 180),
            },
            name="tas",
        )

        # Mock rule
        rule = Mock()
        rule.data_request_variable = Mock()
        rule.data_request_variable.dimensions = ("time", "lat", "lon")
        rule._pycmor_cfg = Mock(
            side_effect=lambda key, default=None: {
                "xarray_enable_dimension_mapping": True,
                "dimension_mapping_validation": "warn",
                "dimension_mapping": {},
            }.get(key, default)
        )

        da_mapped = map_dimensions(da, rule)

        assert isinstance(da_mapped, xr.DataArray)
        assert "lat" in da_mapped.dims
        assert "lon" in da_mapped.dims

    def test_map_dimensions_disabled(self):
        """Test that mapping can be disabled"""
        ds = xr.Dataset(
            {
                "tas": (["time", "latitude", "longitude"], np.random.rand(10, 90, 180)),
            },
            coords={
                "time": np.arange(10),
                "latitude": np.linspace(-90, 90, 90),
                "longitude": np.linspace(0, 360, 180),
            },
        )

        # Mock rule with mapping disabled
        rule = Mock()
        rule.data_request_variable = Mock()
        rule.data_request_variable.dimensions = ("time", "lat", "lon")
        rule._pycmor_cfg = Mock(
            side_effect=lambda key, default=None: {
                "xarray_enable_dimension_mapping": False,
            }.get(key, default)
        )

        ds_result = map_dimensions(ds, rule)

        # Should be unchanged
        assert "latitude" in ds_result.dims
        assert "longitude" in ds_result.dims
        assert "lat" not in ds_result.dims
        assert "lon" not in ds_result.dims

    def test_map_dimensions_with_user_mapping(self):
        """Test map_dimensions with user-specified mapping"""
        ds = xr.Dataset(
            {
                "ta": (
                    ["time", "level", "lat", "lon"],
                    np.random.rand(10, 19, 90, 180),
                ),
            },
            coords={
                "time": np.arange(10),
                "level": np.arange(19),
                "lat": np.linspace(-90, 90, 90),
                "lon": np.linspace(0, 360, 180),
            },
        )

        # Mock rule with user mapping
        rule = Mock()
        rule.data_request_variable = Mock()
        rule.data_request_variable.dimensions = ("time", "plev19", "lat", "lon")
        rule._pycmor_cfg = Mock(
            side_effect=lambda key, default=None: {
                "xarray_enable_dimension_mapping": True,
                "dimension_mapping_validation": "warn",
                "dimension_mapping": {"level": "plev19"},
            }.get(key, default)
        )

        ds_mapped = map_dimensions(ds, rule)

        assert "plev19" in ds_mapped.dims
        assert "level" not in ds_mapped.dims


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
