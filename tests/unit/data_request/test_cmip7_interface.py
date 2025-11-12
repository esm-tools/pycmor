"""
Tests for CMIP7Interface module.

This module tests the CMIP7 data request interface, including:
- Loading metadata from JSON files
- Querying variables by CMIP7 compound names
- Querying variables by CMIP6 compound names (backward compatibility)
- Finding variable variants
- Parsing and building compound names
- Getting variables for experiments
"""

import pytest

from pycmor.data_request.cmip7_interface import CMIP7_API_AVAILABLE, CMIP7Interface, get_cmip7_interface


class TestCMIP7InterfaceInit:
    """Test CMIP7Interface initialization."""

    def test_init_requires_api(self):
        """Test that initialization fails without API."""
        if not CMIP7_API_AVAILABLE:
            with pytest.raises(ImportError, match="CMIP7 Data Request API is not available"):
                CMIP7Interface()
        else:
            interface = CMIP7Interface()
            assert interface._metadata is None
            assert interface._version is None
            assert interface._experiments_data is None

    def test_api_available_flag(self):
        """Test that CMIP7_API_AVAILABLE flag is set correctly."""
        assert isinstance(CMIP7_API_AVAILABLE, bool)


class TestLoadMetadata:
    """Test metadata loading functionality."""

    def test_load_metadata_from_file(self, cmip7_interface_with_metadata):
        """Test loading metadata from a JSON file."""
        assert cmip7_interface_with_metadata._metadata is not None
        assert "Compound Name" in cmip7_interface_with_metadata._metadata
        assert len(cmip7_interface_with_metadata._metadata["Compound Name"]) == 3

    def test_load_metadata_sets_version(self, cmip7_interface_with_metadata):
        """Test that loading metadata sets the version."""
        assert cmip7_interface_with_metadata._version == "v1.2.2.2"

    def test_load_metadata_without_force_reload(self, cmip7_metadata_file):
        """Test that metadata is not reloaded if already loaded."""
        if not CMIP7_API_AVAILABLE:
            pytest.skip("CMIP7 API not available")

        interface = CMIP7Interface()
        interface.load_metadata(metadata_file=cmip7_metadata_file)
        first_metadata = interface._metadata

        # Load again without force_reload
        interface.load_metadata(metadata_file=cmip7_metadata_file)
        assert interface._metadata is first_metadata  # Same object

    def test_load_metadata_with_force_reload(self, cmip7_metadata_file):
        """Test that metadata is reloaded when force_reload=True."""
        if not CMIP7_API_AVAILABLE:
            pytest.skip("CMIP7 API not available")

        interface = CMIP7Interface()
        interface.load_metadata(metadata_file=cmip7_metadata_file)
        first_metadata = interface._metadata

        # Load again with force_reload
        interface.load_metadata(metadata_file=cmip7_metadata_file, force_reload=True)
        assert interface._metadata is not first_metadata  # Different object

    def test_load_metadata_file_not_found(self):
        """Test error handling when metadata file doesn't exist."""
        if not CMIP7_API_AVAILABLE:
            pytest.skip("CMIP7 API not available")

        interface = CMIP7Interface()
        with pytest.raises(FileNotFoundError):
            interface.load_metadata(metadata_file="nonexistent_file.json")


class TestLoadExperimentsData:
    """Test experiments data loading functionality."""

    def test_load_experiments_data(self, cmip7_interface_with_all_data):
        """Test loading experiments data from a JSON file."""
        assert cmip7_interface_with_all_data._experiments_data is not None
        assert "experiment" in cmip7_interface_with_all_data._experiments_data
        assert "historical" in cmip7_interface_with_all_data._experiments_data["experiment"]

    def test_load_experiments_data_file_not_found(self, cmip7_interface_with_metadata):
        """Test error handling when experiments file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            cmip7_interface_with_metadata.load_experiments_data("nonexistent_file.json")


class TestGetVariableMetadata:
    """Test getting variable metadata by CMIP7 compound name."""

    def test_get_variable_metadata_success(self, cmip7_interface_with_metadata):
        """Test getting metadata for an existing variable."""
        metadata = cmip7_interface_with_metadata.get_variable_metadata("atmos.tas.tavg-h2m-hxy-u.mon.GLB")
        assert metadata is not None
        assert metadata["standard_name"] == "air_temperature"
        assert metadata["units"] == "K"
        assert metadata["frequency"] == "mon"

    def test_get_variable_metadata_not_found(self, cmip7_interface_with_metadata):
        """Test getting metadata for a non-existent variable."""
        metadata = cmip7_interface_with_metadata.get_variable_metadata("nonexistent.var.branding.freq.region")
        assert metadata is None

    def test_get_variable_metadata_without_loading(self):
        """Test that error is raised if metadata not loaded."""
        if not CMIP7_API_AVAILABLE:
            pytest.skip("CMIP7 API not available")

        interface = CMIP7Interface()
        with pytest.raises(ValueError, match="Metadata not loaded"):
            interface.get_variable_metadata("atmos.tas.tavg-h2m-hxy-u.mon.GLB")


class TestGetVariableByCMIP6Name:
    """Test getting variable metadata by CMIP6 compound name."""

    def test_get_variable_by_cmip6_name_success(self, cmip7_interface_with_metadata):
        """Test getting metadata using CMIP6 compound name."""
        metadata = cmip7_interface_with_metadata.get_variable_by_cmip6_name("Amon.tas")
        assert metadata is not None
        assert metadata["cmip7_compound_name"] == "atmos.tas.tavg-h2m-hxy-u.mon.GLB"
        assert metadata["standard_name"] == "air_temperature"

    def test_get_variable_by_cmip6_name_not_found(self, cmip7_interface_with_metadata):
        """Test getting metadata for non-existent CMIP6 name."""
        metadata = cmip7_interface_with_metadata.get_variable_by_cmip6_name("Nonexistent.var")
        assert metadata is None

    def test_get_variable_by_cmip6_name_without_loading(self):
        """Test that error is raised if metadata not loaded."""
        if not CMIP7_API_AVAILABLE:
            pytest.skip("CMIP7 API not available")

        interface = CMIP7Interface()
        with pytest.raises(ValueError, match="Metadata not loaded"):
            interface.get_variable_by_cmip6_name("Amon.tas")


class TestFindVariableVariants:
    """Test finding all variants of a variable."""

    def test_find_all_variants(self, cmip7_interface_with_metadata):
        """Test finding all variants of a variable."""
        variants = cmip7_interface_with_metadata.find_variable_variants("clt")
        assert len(variants) == 2  # Monthly and daily

        compound_names = [v["cmip7_compound_name"] for v in variants]
        assert "atmos.clt.tavg-u-hxy-u.mon.GLB" in compound_names
        assert "atmos.clt.tavg-u-hxy-u.day.GLB" in compound_names

    def test_find_variants_with_realm_filter(self, cmip7_interface_with_metadata):
        """Test finding variants filtered by realm."""
        variants = cmip7_interface_with_metadata.find_variable_variants("clt", realm="atmos")
        assert len(variants) == 2

        # Test with non-matching realm
        variants = cmip7_interface_with_metadata.find_variable_variants("clt", realm="ocean")
        assert len(variants) == 0

    def test_find_variants_with_frequency_filter(self, cmip7_interface_with_metadata):
        """Test finding variants filtered by frequency."""
        variants = cmip7_interface_with_metadata.find_variable_variants("clt", frequency="mon")
        assert len(variants) == 1
        assert variants[0]["frequency"] == "mon"

    def test_find_variants_with_region_filter(self, cmip7_interface_with_metadata):
        """Test finding variants filtered by region."""
        variants = cmip7_interface_with_metadata.find_variable_variants("clt", region="GLB")
        assert len(variants) == 2

        # Test with non-matching region
        variants = cmip7_interface_with_metadata.find_variable_variants("clt", region="30S-90S")
        assert len(variants) == 0

    def test_find_variants_with_multiple_filters(self, cmip7_interface_with_metadata):
        """Test finding variants with multiple filters."""
        variants = cmip7_interface_with_metadata.find_variable_variants(
            "clt", realm="atmos", frequency="day", region="GLB"
        )
        assert len(variants) == 1
        assert variants[0]["cmip7_compound_name"] == "atmos.clt.tavg-u-hxy-u.day.GLB"

    def test_find_variants_not_found(self, cmip7_interface_with_metadata):
        """Test finding variants for non-existent variable."""
        variants = cmip7_interface_with_metadata.find_variable_variants("nonexistent")
        assert len(variants) == 0

    def test_find_variants_without_loading(self):
        """Test that error is raised if metadata not loaded."""
        if not CMIP7_API_AVAILABLE:
            pytest.skip("CMIP7 API not available")

        interface = CMIP7Interface()
        with pytest.raises(ValueError, match="Metadata not loaded"):
            interface.find_variable_variants("clt")


class TestGetVariablesForExperiment:
    """Test getting variables for specific experiments."""

    def test_get_all_priorities(self, cmip7_interface_with_all_data):
        """Test getting all priorities for an experiment."""
        vars_dict = cmip7_interface_with_all_data.get_variables_for_experiment("historical")
        assert "Core" in vars_dict
        assert "High" in vars_dict
        assert len(vars_dict["Core"]) == 2
        assert len(vars_dict["High"]) == 1

    def test_get_specific_priority(self, cmip7_interface_with_all_data):
        """Test getting variables for a specific priority."""
        core_vars = cmip7_interface_with_all_data.get_variables_for_experiment("historical", priority="Core")
        assert len(core_vars) == 2
        assert "atmos.tas.tavg-h2m-hxy-u.mon.GLB" in core_vars

    def test_get_experiment_not_found(self, cmip7_interface_with_all_data):
        """Test error when experiment doesn't exist."""
        with pytest.raises(ValueError, match="Experiment 'nonexistent' not found"):
            cmip7_interface_with_all_data.get_variables_for_experiment("nonexistent")

    def test_get_priority_not_found(self, cmip7_interface_with_all_data):
        """Test error when priority doesn't exist for experiment."""
        with pytest.raises(ValueError, match="Priority 'Medium' not found"):
            cmip7_interface_with_all_data.get_variables_for_experiment("historical", priority="Medium")

    def test_get_without_loading_experiments(self, cmip7_interface_with_metadata):
        """Test that error is raised if experiments data not loaded."""
        with pytest.raises(ValueError, match="Experiments data not loaded"):
            cmip7_interface_with_metadata.get_variables_for_experiment("historical")


class TestGetAllExperiments:
    """Test getting list of all experiments."""

    def test_get_all_experiments(self, cmip7_interface_with_all_data):
        """Test getting list of all experiments."""
        experiments = cmip7_interface_with_all_data.get_all_experiments()
        assert len(experiments) == 2
        assert "historical" in experiments
        assert "piControl" in experiments

    def test_get_all_experiments_without_loading(self, cmip7_interface_with_metadata):
        """Test that error is raised if experiments data not loaded."""
        with pytest.raises(ValueError, match="Experiments data not loaded"):
            cmip7_interface_with_metadata.get_all_experiments()


class TestGetAllCompoundNames:
    """Test getting list of all compound names."""

    def test_get_all_compound_names(self, cmip7_interface_with_metadata):
        """Test getting all CMIP7 compound names."""
        compound_names = cmip7_interface_with_metadata.get_all_compound_names()
        assert len(compound_names) == 3
        assert "atmos.tas.tavg-h2m-hxy-u.mon.GLB" in compound_names
        assert "atmos.clt.tavg-u-hxy-u.mon.GLB" in compound_names
        assert "atmos.clt.tavg-u-hxy-u.day.GLB" in compound_names

    def test_get_all_compound_names_without_loading(self):
        """Test that error is raised if metadata not loaded."""
        if not CMIP7_API_AVAILABLE:
            pytest.skip("CMIP7 API not available")

        interface = CMIP7Interface()
        with pytest.raises(ValueError, match="Metadata not loaded"):
            interface.get_all_compound_names()


class TestParseCompoundName:
    """Test parsing CMIP7 compound names."""

    def test_parse_valid_compound_name(self, cmip7_interface_with_metadata):
        """Test parsing a valid CMIP7 compound name."""
        parsed = cmip7_interface_with_metadata.parse_compound_name("atmos.tas.tavg-h2m-hxy-u.mon.GLB")
        assert parsed["realm"] == "atmos"
        assert parsed["variable"] == "tas"
        assert parsed["branding"] == "tavg-h2m-hxy-u"
        assert parsed["frequency"] == "mon"
        assert parsed["region"] == "GLB"

    def test_parse_invalid_compound_name(self, cmip7_interface_with_metadata):
        """Test parsing an invalid compound name."""
        with pytest.raises(ValueError, match="Invalid CMIP7 compound name"):
            cmip7_interface_with_metadata.parse_compound_name("invalid.name")

    def test_parse_compound_name_wrong_parts(self, cmip7_interface_with_metadata):
        """Test parsing compound name with wrong number of parts."""
        with pytest.raises(ValueError, match="Invalid CMIP7 compound name"):
            cmip7_interface_with_metadata.parse_compound_name("realm.var.branding.freq")


class TestBuildCompoundName:
    """Test building CMIP7 compound names."""

    def test_build_compound_name(self, cmip7_interface_with_metadata):
        """Test building a CMIP7 compound name from components."""
        compound_name = cmip7_interface_with_metadata.build_compound_name(
            realm="ocean",
            variable="tos",
            branding="tavg-u-hxy-sea",
            frequency="mon",
            region="GLB",
        )
        assert compound_name == "ocean.tos.tavg-u-hxy-sea.mon.GLB"

    def test_build_and_parse_roundtrip(self, cmip7_interface_with_metadata):
        """Test that building and parsing are inverse operations."""
        original = {
            "realm": "atmos",
            "variable": "tas",
            "branding": "tavg-h2m-hxy-u",
            "frequency": "mon",
            "region": "GLB",
        }
        compound_name = cmip7_interface_with_metadata.build_compound_name(**original)
        parsed = cmip7_interface_with_metadata.parse_compound_name(compound_name)
        assert parsed == original


class TestProperties:
    """Test interface properties."""

    def test_version_property(self, cmip7_interface_with_metadata):
        """Test version property."""
        assert cmip7_interface_with_metadata.version == "v1.2.2.2"

    def test_metadata_property(self, cmip7_interface_with_metadata):
        """Test metadata property."""
        assert cmip7_interface_with_metadata.metadata is not None
        assert "Compound Name" in cmip7_interface_with_metadata.metadata

    def test_experiments_data_property(self, cmip7_interface_with_all_data):
        """Test experiments_data property."""
        assert cmip7_interface_with_all_data.experiments_data is not None
        assert "experiment" in cmip7_interface_with_all_data.experiments_data

    def test_properties_before_loading(self):
        """Test properties before loading data."""
        if not CMIP7_API_AVAILABLE:
            pytest.skip("CMIP7 API not available")

        interface = CMIP7Interface()
        assert interface.version is None
        assert interface.metadata is None
        assert interface.experiments_data is None


class TestConvenienceFunction:
    """Test the get_cmip7_interface convenience function."""

    def test_get_cmip7_interface(self, cmip7_metadata_file):
        """Test the convenience function."""
        if not CMIP7_API_AVAILABLE:
            pytest.skip("CMIP7 API not available")

        interface = get_cmip7_interface(metadata_file=cmip7_metadata_file)
        assert interface is not None
        assert interface.metadata is not None
        assert len(interface.metadata["Compound Name"]) == 3


class TestIntegrationScenarios:
    """Integration tests for common usage scenarios."""

    def test_cmip6_to_cmip7_lookup(self, cmip7_interface_with_metadata):
        """Test looking up CMIP7 name from CMIP6 name."""
        # Start with CMIP6 name
        cmip6_name = "Amon.tas"

        # Get CMIP7 metadata
        metadata = cmip7_interface_with_metadata.get_variable_by_cmip6_name(cmip6_name)
        assert metadata is not None

        # Verify we got the right variable
        assert metadata["cmip6_compound_name"] == cmip6_name
        assert metadata["cmip7_compound_name"] == "atmos.tas.tavg-h2m-hxy-u.mon.GLB"

    def test_find_and_filter_workflow(self, cmip7_interface_with_metadata):
        """Test a typical workflow of finding and filtering variables."""
        # Find all variants of clt
        all_variants = cmip7_interface_with_metadata.find_variable_variants("clt")
        assert len(all_variants) == 2

        # Filter to monthly only
        monthly_variants = cmip7_interface_with_metadata.find_variable_variants("clt", frequency="mon")
        assert len(monthly_variants) == 1

        # Get the metadata
        monthly_clt = monthly_variants[0]
        assert monthly_clt["frequency"] == "mon"
        assert monthly_clt["standard_name"] == "cloud_area_fraction"

    def test_experiment_to_variables_workflow(self, cmip7_interface_with_all_data):
        """Test getting variables for an experiment and accessing metadata."""
        # Get Core variables for historical
        core_vars = cmip7_interface_with_all_data.get_variables_for_experiment("historical", priority="Core")
        assert len(core_vars) == 2

        # Get metadata for each variable
        for var_name in core_vars:
            metadata = cmip7_interface_with_all_data.get_variable_metadata(var_name)
            assert metadata is not None
            assert "standard_name" in metadata
            assert "units" in metadata
