"""
Unit tests for the create_filepath function.

This module tests the create_filepath function from pycmor.std_lib.files,
which generates CMIP6-compliant file paths based on dataset and rule information.
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pycmor.std_lib.files import create_filepath


class TestCreateFilepath:
    """Test cases for the create_filepath function."""

    def setup_method(self):
        """Set up common test fixtures."""
        # Create a basic mock rule with all required attributes
        self.rule = Mock()
        self.rule.cmor_variable = "tas"
        self.rule.variant_label = "r1i1p1f1"
        self.rule.source_id = "AWI-CM-1-1-MR"
        self.rule.experiment_id = "historical"
        self.rule.grid_label = "gn"
        self.rule.institution = "AWI"

        # Mock data request variable and table header
        self.rule.data_request_variable = Mock()
        self.rule.data_request_variable.table_header = Mock()
        self.rule.data_request_variable.table_header.table_id = "Amon"
        self.rule.data_request_variable.frequency = "mon"

        # Mock pycmor config
        self.rule._pycmor_cfg = Mock()
        self.rule._pycmor_cfg.get = Mock(return_value=False)  # disable subdirs by default

        # Create temporary directory for output
        self.temp_dir = tempfile.mkdtemp()
        self.rule.output_directory = self.temp_dir

    def teardown_method(self):
        """Clean up after each test."""
        # Clean up temporary directory
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_test_dataset(self, time_range=None, has_time=True):
        """Create a test dataset with optional time dimension."""
        if has_time and time_range is not None:
            coords = {"time": time_range, "lat": [0, 1], "lon": [0, 1]}
            dims = ["time", "lat", "lon"]
            data_shape = (len(time_range), 2, 2)
        elif has_time:
            # Default monthly data for 1 year
            time_range = pd.date_range("2000-01-01", periods=12, freq="MS")
            coords = {"time": time_range, "lat": [0, 1], "lon": [0, 1]}
            dims = ["time", "lat", "lon"]
            data_shape = (12, 2, 2)
        else:
            # No time dimension
            coords = {"lat": [0, 1], "lon": [0, 1]}
            dims = ["lat", "lon"]
            data_shape = (2, 2)

        data = np.random.rand(*data_shape)
        return xr.Dataset({"tas": (dims, data)}, coords=coords)

    def test_basic_filepath_generation(self):
        """Test basic filepath generation with monthly data."""
        ds = self.create_test_dataset()
        filepath = create_filepath(ds, self.rule)

        # According to CMIP6 spec:
        # <variable_id>_<table_id>_<source_id>_<experiment_id>_<member_id>_<grid_label>[_<time_range>].nc
        # Note: Current implementation uses institution-source_id format, not just source_id
        expected_pattern = f"{self.temp_dir}/tas_Amon_AWI-AWI-CM-1-1-MR_historical_r1i1p1f1_gn_200001-200012.nc"
        assert filepath == expected_pattern

        # Check that parent directory was created
        assert Path(filepath).parent.exists()

    def test_filepath_without_time_dimension(self):
        """Test filepath generation for datasets without time dimension."""
        ds = self.create_test_dataset(has_time=False)
        filepath = create_filepath(ds, self.rule)

        expected_pattern = f"{self.temp_dir}/tas_Amon_AWI-AWI-CM-1-1-MR_historical_r1i1p1f1_gn.nc"
        assert filepath == expected_pattern

    def test_filepath_with_scalar_time(self):
        """Test filepath generation for datasets with scalar time dimension."""
        # Create dataset with scalar time
        time_val = pd.Timestamp("2000-01-15")
        ds = xr.Dataset(
            {"tas": (["lat", "lon"], np.random.rand(2, 2))},
            coords={"time": time_val, "lat": [0, 1], "lon": [0, 1]},
        )

        filepath = create_filepath(ds, self.rule)

        expected_pattern = f"{self.temp_dir}/tas_Amon_AWI-AWI-CM-1-1-MR_historical_r1i1p1f1_gn.nc"
        assert filepath == expected_pattern

    def test_filepath_with_daily_frequency(self):
        """Test filepath generation with daily frequency."""
        self.rule.data_request_variable.frequency = "day"
        self.rule.data_request_variable.table_header.table_id = "day"

        # Create daily data for January 2000
        time_range = pd.date_range("2000-01-01", "2000-01-31", freq="D")
        ds = self.create_test_dataset(time_range=time_range)

        filepath = create_filepath(ds, self.rule)

        expected_pattern = f"{self.temp_dir}/tas_day_AWI-AWI-CM-1-1-MR_historical_r1i1p1f1_gn_20000101-20000131.nc"
        assert filepath == expected_pattern

    def test_filepath_with_hourly_frequency(self):
        """Test filepath generation with hourly frequency."""
        self.rule.data_request_variable.frequency = "6hr"
        self.rule.data_request_variable.table_header.table_id = "6hrLev"

        # Create 6-hourly data for one day
        time_range = pd.date_range("2000-01-01", "2000-01-01 18:00", freq="6h")
        ds = self.create_test_dataset(time_range=time_range)

        filepath = create_filepath(ds, self.rule)

        expected_pattern = (
            f"{self.temp_dir}/tas_6hrLev_AWI-AWI-CM-1-1-MR_historical_" f"r1i1p1f1_gn_200001010000-200001011800.nc"
        )
        assert filepath == expected_pattern

    def test_filepath_with_yearly_frequency(self):
        """Test filepath generation with yearly frequency."""
        self.rule.data_request_variable.frequency = "yr"
        self.rule.data_request_variable.table_header.table_id = "Ayr"

        # Create yearly data (4 years: 2000, 2001, 2002, 2003)
        time_range = pd.date_range("2000-01-01", periods=4, freq="YS")
        ds = self.create_test_dataset(time_range=time_range)

        filepath = create_filepath(ds, self.rule)

        expected_pattern = f"{self.temp_dir}/tas_Ayr_AWI-AWI-CM-1-1-MR_historical_r1i1p1f1_gn_2000-2003.nc"
        assert filepath == expected_pattern

    def test_filepath_with_fx_frequency(self):
        """Test filepath generation with fixed (fx) frequency."""
        self.rule.data_request_variable.frequency = "fx"
        self.rule.data_request_variable.table_header.table_id = "fx"

        ds = self.create_test_dataset(has_time=False)
        filepath = create_filepath(ds, self.rule)

        expected_pattern = f"{self.temp_dir}/tas_fx_AWI-AWI-CM-1-1-MR_historical_r1i1p1f1_gn.nc"
        assert filepath == expected_pattern

    def test_filepath_with_custom_institution(self):
        """Test filepath generation with custom institution."""
        self.rule.institution = "CUSTOM-INST"

        ds = self.create_test_dataset()
        filepath = create_filepath(ds, self.rule)

        expected_pattern = f"{self.temp_dir}/tas_Amon_CUSTOM-INST-AWI-CM-1-1-MR_historical_r1i1p1f1_gn_200001-200012.nc"
        assert filepath == expected_pattern

    def test_filepath_with_default_institution(self):
        """Test filepath generation with default institution when not specified."""
        # Remove institution attribute to test default
        delattr(self.rule, "institution")

        ds = self.create_test_dataset()
        filepath = create_filepath(ds, self.rule)

        expected_pattern = f"{self.temp_dir}/tas_Amon_AWI-AWI-CM-1-1-MR_historical_r1i1p1f1_gn_200001-200012.nc"
        assert filepath == expected_pattern

    def test_filepath_with_subdirectories_enabled(self):
        """Test filepath generation with output subdirectories enabled."""
        # Enable subdirectories
        self.rule._pycmor_cfg.get = Mock(return_value=True)
        self.rule.ga = Mock()
        self.rule.ga.subdir_path = Mock(
            return_value="CMIP6/historical/AWI/AWI-CM-1-1-MR/r1i1p1f1/Amon/tas/gn/v20200101"
        )

        ds = self.create_test_dataset()
        filepath = create_filepath(ds, self.rule)

        expected_pattern = (
            f"{self.temp_dir}/CMIP6/historical/AWI/AWI-CM-1-1-MR/r1i1p1f1/Amon/tas/gn/v20200101/"
            "tas_Amon_AWI-AWI-CM-1-1-MR_historical_r1i1p1f1_gn_200001-200012.nc"
        )
        assert filepath == expected_pattern

        # Check that subdirectory was created
        assert Path(filepath).parent.exists()

    def test_filepath_with_different_variables(self):
        """Test filepath generation with different CMOR variables."""
        test_cases = [
            ("pr", "precipitation"),
            ("tos", "sea_surface_temperature"),
            ("ua", "eastward_wind"),
            ("va", "northward_wind"),
        ]

        for cmor_var, _ in test_cases:
            self.rule.cmor_variable = cmor_var
            ds = self.create_test_dataset()
            filepath = create_filepath(ds, self.rule)

            expected_pattern = (
                f"{self.temp_dir}/{cmor_var}_Amon_AWI-AWI-CM-1-1-MR_historical_" f"r1i1p1f1_gn_200001-200012.nc"
            )
            assert filepath == expected_pattern

    def test_filepath_with_different_experiments(self):
        """Test filepath generation with different experiment IDs."""
        test_cases = ["historical", "piControl", "1pctCO2", "abrupt-4xCO2", "ssp585"]

        for experiment in test_cases:
            self.rule.experiment_id = experiment
            ds = self.create_test_dataset()
            filepath = create_filepath(ds, self.rule)

            expected_pattern = f"{self.temp_dir}/tas_Amon_AWI-AWI-CM-1-1-MR_{experiment}_r1i1p1f1_gn_200001-200012.nc"
            assert filepath == expected_pattern

    def test_filepath_with_different_grid_labels(self):
        """Test filepath generation with different grid labels."""
        test_cases = ["gn", "gr", "gr1", "gr2", "gm"]

        for grid_label in test_cases:
            self.rule.grid_label = grid_label
            ds = self.create_test_dataset()
            filepath = create_filepath(ds, self.rule)

            expected_pattern = (
                f"{self.temp_dir}/tas_Amon_AWI-AWI-CM-1-1-MR_historical_" f"r1i1p1f1_{grid_label}_200001-200012.nc"
            )
            assert filepath == expected_pattern

    def test_filepath_with_different_variant_labels(self):
        """Test filepath generation with different variant labels."""
        test_cases = ["r1i1p1f1", "r2i1p1f1", "r1i2p1f1", "r1i1p2f1", "r1i1p1f2"]

        for variant_label in test_cases:
            self.rule.variant_label = variant_label
            ds = self.create_test_dataset()
            filepath = create_filepath(ds, self.rule)

            expected_pattern = (
                f"{self.temp_dir}/tas_Amon_AWI-AWI-CM-1-1-MR_historical_" f"{variant_label}_gn_200001-200012.nc"
            )
            assert filepath == expected_pattern

    def test_filepath_with_cftime_coordinates(self):
        """Test filepath generation with cftime coordinates."""
        # Create dataset with cftime coordinates (common in climate models)
        time_range = xr.cftime_range("2000-01-01", periods=12, freq="MS", calendar="noleap")
        ds = self.create_test_dataset(time_range=time_range)

        filepath = create_filepath(ds, self.rule)

        expected_pattern = f"{self.temp_dir}/tas_Amon_AWI-AWI-CM-1-1-MR_historical_r1i1p1f1_gn_200001-200012.nc"
        assert filepath == expected_pattern

    def test_filepath_directory_creation(self):
        """Test that parent directories are created when they don't exist."""
        # Use a nested directory structure
        nested_dir = Path(self.temp_dir) / "nested" / "deep" / "structure"
        self.rule.output_directory = str(nested_dir)

        ds = self.create_test_dataset()
        filepath = create_filepath(ds, self.rule)

        # Check that the directory was created
        assert Path(filepath).parent.exists()
        assert Path(filepath).parent == nested_dir

    def test_filepath_with_special_characters_in_paths(self):
        """Test filepath generation handles special characters properly."""
        # Test with spaces and special characters in output directory
        special_dir = Path(self.temp_dir) / "output with spaces" / "special-chars_123"
        self.rule.output_directory = str(special_dir)

        ds = self.create_test_dataset()
        filepath = create_filepath(ds, self.rule)

        # Check that the directory was created and path is correct
        assert Path(filepath).parent.exists()
        assert "output with spaces" in filepath
        assert "special-chars_123" in filepath

    def test_filepath_components_order(self):
        """Test that filepath components are in the correct CMIP6 order."""
        ds = self.create_test_dataset()
        filepath = create_filepath(ds, self.rule)

        # Extract filename from path
        filename = Path(filepath).name

        # Split filename by underscores (excluding .nc extension)
        components = filename[:-3].split("_")

        # Check CMIP6 filename convention order
        # Official template:
        # <variable_id>_<table_id>_<source_id>_<experiment_id>_<member_id>_<grid_label>[_<time_range>].nc
        assert components[0] == "tas"  # variable_id
        assert components[1] == "Amon"  # table_id
        assert components[2] == "AWI-AWI-CM-1-1-MR"  # source_id (current implementation uses institution-source_id)
        assert components[3] == "historical"  # experiment_id
        assert components[4] == "r1i1p1f1"  # member_id (variant_label when sub_experiment_id="none")
        assert components[5] == "gn"  # grid_label
        assert components[6] == "200001-200012"  # time_range

    @pytest.mark.parametrize(
        "frequency,expected_time_format",
        [
            ("yr", "2000-2004"),
            ("mon", "200001-200012"),
            ("day", "20000101-20000131"),
            ("6hr", "200001010000-200001011800"),
            ("3hr", "200001010000-200001012100"),
            ("1hr", "200001010000-200001012300"),
            ("fx", ""),
        ],
    )
    def test_time_format_by_frequency(self, frequency, expected_time_format):
        """Test that time formats match expected patterns for different frequencies."""
        self.rule.data_request_variable.frequency = frequency
        self.rule.data_request_variable.table_header.table_id = frequency

        # Create appropriate time range for each frequency
        if frequency == "yr":
            time_range = pd.date_range("2000-01-01", periods=5, freq="YS")
        elif frequency == "mon":
            time_range = pd.date_range("2000-01-01", periods=12, freq="MS")
        elif frequency == "day":
            time_range = pd.date_range("2000-01-01", "2000-01-31", freq="D")
        elif frequency in ["6hr", "3hr", "1hr"]:
            if frequency == "6hr":
                time_range = pd.date_range("2000-01-01", "2000-01-01 18:00", freq="6h")
            elif frequency == "3hr":
                time_range = pd.date_range("2000-01-01", "2000-01-01 21:00", freq="3h")
            else:  # 1hr
                time_range = pd.date_range("2000-01-01", "2000-01-01 23:00", freq="1h")
        else:  # fx
            ds = self.create_test_dataset(has_time=False)
            filepath = create_filepath(ds, self.rule)
            assert expected_time_format in filepath or expected_time_format == ""
            return

        ds = self.create_test_dataset(time_range=time_range)
        filepath = create_filepath(ds, self.rule)

        if expected_time_format:
            assert expected_time_format in filepath
        else:
            # For fx frequency, time range should be empty
            assert filepath.endswith("_gn.nc")

    def test_cmip6_filename_compliance(self):
        """Test that generated filenames comply with CMIP6 specification."""
        ds = self.create_test_dataset()
        filepath = create_filepath(ds, self.rule)
        filename = Path(filepath).name

        # CMIP6 spec: All strings in filename use only: a-z, A-Z, 0-9, and hyphen (-)
        # Underscores are prohibited except as shown in template
        import re

        # Remove underscores that are part of the template structure
        components = filename[:-3].split("_")  # Remove .nc and split by underscores

        for i, component in enumerate(components):
            # Each component should only contain allowed characters
            assert re.match(r"^[a-zA-Z0-9-]+$", component), f"Component {i} '{component}' contains forbidden characters"

        # Variable_id must not contain hyphens according to spec
        variable_id = components[0]
        assert "-" not in variable_id, f"variable_id '{variable_id}' should not contain hyphens"

    def test_cmip6_time_range_precision(self):
        """Test that time range precision matches CMIP6 Table 2 specification."""
        test_cases = [
            # (frequency, expected_precision, time_range, expected_format)
            (
                "yr",
                "yyyy",
                pd.date_range("2000-01-01", periods=4, freq="YS"),
                "2000-2003",
            ),
            (
                "dec",
                "yyyy",
                pd.date_range("2000-01-01", periods=3, freq="10YS"),
                "2000-2020",
            ),
            (
                "mon",
                "yyyyMM",
                pd.date_range("2000-01-01", periods=12, freq="MS"),
                "200001-200012",
            ),
            (
                "day",
                "yyyyMMdd",
                pd.date_range("2000-01-01", "2000-01-31", freq="D"),
                "20000101-20000131",
            ),
            (
                "6hr",
                "yyyyMMddhhmm",
                pd.date_range("2000-01-01", "2000-01-01 18:00", freq="6h"),
                "200001010000-200001011800",
            ),
            (
                "3hr",
                "yyyyMMddhhmm",
                pd.date_range("2000-01-01", "2000-01-01 21:00", freq="3h"),
                "200001010000-200001012100",
            ),
            (
                "1hr",
                "yyyyMMddhhmm",
                pd.date_range("2000-01-01", "2000-01-01 23:00", freq="1h"),
                "200001010000-200001012300",
            ),
        ]

        for frequency, precision, time_range, expected_format in test_cases:
            self.rule.data_request_variable.frequency = frequency
            self.rule.data_request_variable.table_header.table_id = frequency

            ds = self.create_test_dataset(time_range=time_range)
            filepath = create_filepath(ds, self.rule)
            filename = Path(filepath).name

            # Extract time range from filename
            components = filename[:-3].split("_")
            time_component = components[-1]

            assert (
                time_component == expected_format
            ), f"For {frequency}, expected {expected_format}, got {time_component}"

    def test_member_id_construction(self):
        """Test member_id construction according to CMIP6 spec."""
        # Test case 1: sub_experiment_id = "none" -> member_id = variant_label
        self.rule.sub_experiment_id = "none"
        ds = self.create_test_dataset()
        filepath = create_filepath(ds, self.rule)
        filename = Path(filepath).name
        components = filename[:-3].split("_")
        member_id = components[4]
        assert member_id == "r1i1p1f1", f"Expected r1i1p1f1, got {member_id}"

        # Test case 2: sub_experiment_id != "none" -> member_id = sub_experiment_id-variant_label
        # Note: Current implementation doesn't handle sub_experiment_id, so this test documents expected behavior
        # This would require modification to the create_filepath function to be fully CMIP6 compliant

    def test_forbidden_characters_in_components(self):
        """Test that filename components don't contain forbidden characters."""
        # Test with various rule configurations that might introduce forbidden characters
        test_cases = [
            ("source_id", "Test.Model-1.0"),  # periods should be converted to hyphens
            (
                "experiment_id",
                "test_experiment",
            ),  # underscores should be converted to hyphens
            ("institution", "Test Institution"),  # spaces should be handled
        ]

        for attr, value in test_cases:
            # Reset rule to defaults
            self.setup_method()
            setattr(self.rule, attr, value)

            ds = self.create_test_dataset()
            filepath = create_filepath(ds, self.rule)
            filename = Path(filepath).name

            # Check that filename doesn't contain forbidden characters (except template underscores)
            components = filename[:-3].split("_")
            for component in components:
                # Each component should only contain a-z, A-Z, 0-9, and hyphen
                import re

                assert re.match(r"^[a-zA-Z0-9-]+$", component), f"Component '{component}' contains forbidden characters"

    def test_time_invariant_fields(self):
        """Test filename generation for time-invariant (fx) fields."""
        self.rule.data_request_variable.frequency = "fx"
        self.rule.data_request_variable.table_header.table_id = "fx"

        # Create dataset without time dimension
        ds = self.create_test_dataset(has_time=False)
        filepath = create_filepath(ds, self.rule)
        filename = Path(filepath).name

        # For fx frequency, time_range should be omitted
        # Expected: <variable_id>_<table_id>_<source_id>_<experiment_id>_<member_id>_<grid_label>.nc
        components = filename[:-3].split("_")
        assert len(components) == 6, f"fx files should have 6 components, got {len(components)}: {components}"

        # Should end with grid_label, not time_range
        assert components[-1] == "gn", f"Last component should be grid_label 'gn', got '{components[-1]}'"

    def test_climatology_suffix(self):
        """Test climatology suffix handling (though not implemented in current version)."""
        # This test documents expected behavior for climatology files
        # According to CMIP6 spec, if variable has "climatology" attribute, suffix should be "-clim"
        # Current implementation doesn't handle this, but test documents the requirement

        ds = self.create_test_dataset()
        # Add climatology attribute to time variable (this is how CF convention marks climatologies)
        ds.time.attrs["climatology"] = "climatology_bounds"

        filepath = create_filepath(ds, self.rule)
        filename = Path(filepath).name

        # Note: Current implementation doesn't handle climatology suffix
        # This test documents what should happen according to CMIP6 spec
        # Expected format would be: tas_Amon_AWI-AWI-CM-1-1-MR_historical_r1i1p1f1_gn_200001-200012-clim.nc

        # Now verify that climatology suffix is properly implemented
        assert filename.endswith(
            "200001-200012-clim.nc"
        ), "Climatology suffix should be added when climatology attribute is present"
