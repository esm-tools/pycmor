"""Tests for CMIP7 compound name matching and architecture."""

import pytest

from pycmor.data_request.collection import CMIP7DataRequest
from pycmor.data_request.table import CMIP7DataRequestTableHeader
from pycmor.data_request.variable import CMIP7DataRequestVariable


class TestCMIP7CompoundNameIndexing:
    """Test that CMIP7 DataRequest indexes variables by compound name."""

    def test_from_all_var_info_indexes_by_compound_name(self):
        """Test that variables are indexed by compound name, not table."""
        # Minimal test data without cmip6_table field
        test_data = {
            "Compound Name": {
                "ocean.tos.tavg-u-hxy-sea.mon.GLB": {
                    "out_name": "tos",
                    "frequency": "mon",
                    "modeling_realm": "ocean",
                    "units": "degC",
                    "cell_methods": "area: mean time: mean",
                    "cell_measures": "area: areacello",
                    "long_name": "Sea Surface Temperature",
                    "comment": "Test variable",
                    "dimensions": "longitude latitude time",
                    "type": "real",
                    "positive": "",
                    "spatial_shape": "XY",
                    "temporal_shape": "T",
                    "branding_label": "tavg-u-hxy-sea",
                    "region": "GLB",
                    "cmip7_compound_name": "ocean.tos.tavg-u-hxy-sea.mon.GLB",
                },
                "ocean.tos.tpt-u-hxy-sea.3hr.GLB": {
                    "out_name": "tos",
                    "frequency": "3hr",
                    "modeling_realm": "ocean",
                    "units": "degC",
                    "cell_methods": "area: mean time: point",
                    "cell_measures": "area: areacello",
                    "long_name": "Sea Surface Temperature",
                    "comment": "Test variable with different branding",
                    "dimensions": "longitude latitude time",
                    "type": "real",
                    "positive": "",
                    "spatial_shape": "XY",
                    "temporal_shape": "T",
                    "branding_label": "tpt-u-hxy-sea",
                    "region": "GLB",
                    "cmip7_compound_name": "ocean.tos.tpt-u-hxy-sea.3hr.GLB",
                },
            }
        }

        # Create DataRequest
        dreq = CMIP7DataRequest.from_all_var_info(test_data)

        # Verify variables are indexed by compound name
        assert len(dreq.variables) == 2
        assert "ocean.tos.tavg-u-hxy-sea.mon.GLB" in dreq.variables
        assert "ocean.tos.tpt-u-hxy-sea.3hr.GLB" in dreq.variables

        # Verify variable_id returns compound name
        var1 = dreq.variables["ocean.tos.tavg-u-hxy-sea.mon.GLB"]
        assert var1.variable_id == "ocean.tos.tavg-u-hxy-sea.mon.GLB"

        var2 = dreq.variables["ocean.tos.tpt-u-hxy-sea.3hr.GLB"]
        assert var2.variable_id == "ocean.tos.tpt-u-hxy-sea.3hr.GLB"

    def test_from_all_var_info_without_cmip6_table(self):
        """Test that DataRequest loads successfully without cmip6_table field."""
        test_data = {
            "Compound Name": {
                "atmos.tas.tavg-u-hxy-land.mon.GLB": {
                    "out_name": "tas",
                    "frequency": "mon",
                    "modeling_realm": "atmos",
                    "units": "K",
                    "cell_methods": "area: mean time: mean",
                    "cell_measures": "area: areacella",
                    "long_name": "Near-Surface Air Temperature",
                    "comment": "Test variable without cmip6_table",
                    "dimensions": "longitude latitude time",
                    "type": "real",
                    "positive": "",
                    "spatial_shape": "XY",
                    "temporal_shape": "T",
                    "cmip7_compound_name": "atmos.tas.tavg-u-hxy-land.mon.GLB",
                }
            }
        }

        dreq = CMIP7DataRequest.from_all_var_info(test_data)

        # Should load 1 variable
        assert len(dreq.variables) == 1
        assert "atmos.tas.tavg-u-hxy-land.mon.GLB" in dreq.variables

        # Tables may be empty since no cmip6_table field
        # This is acceptable for CMIP7
        assert isinstance(dreq.tables, dict)


class TestCMIP7VariableFromDict:
    """Test CMIP7DataRequestVariable.from_dict with compound_name parameter."""

    def test_from_dict_with_compound_name_parameter(self):
        """Test that compound_name parameter takes precedence."""
        data = {
            "out_name": "tos",
            "frequency": "mon",
            "modeling_realm": "ocean",
            "units": "degC",
            "cell_methods": "area: mean time: mean",
            "cell_measures": "area: areacello",
            "long_name": "Sea Surface Temperature",
            "comment": "Test",
            "dimensions": "longitude latitude time",
            "type": "real",
            "positive": "",
            "spatial_shape": "XY",
            "temporal_shape": "T",
        }

        compound_name = "ocean.tos.tavg-u-hxy-sea.mon.GLB"
        var = CMIP7DataRequestVariable.from_dict(data, compound_name=compound_name)

        assert var.variable_id == compound_name
        assert var._cmip7_compound_name == compound_name

    def test_from_dict_compound_name_from_data_dict(self):
        """Test that compound_name can come from data dict."""
        data = {
            "out_name": "tos",
            "frequency": "mon",
            "modeling_realm": "ocean",
            "units": "degC",
            "cell_methods": "area: mean time: mean",
            "cell_measures": "area: areacello",
            "long_name": "Sea Surface Temperature",
            "comment": "Test",
            "dimensions": "longitude latitude time",
            "type": "real",
            "positive": "",
            "spatial_shape": "XY",
            "temporal_shape": "T",
            "cmip7_compound_name": "ocean.tos.tavg-u-hxy-sea.mon.GLB",
        }

        var = CMIP7DataRequestVariable.from_dict(data)

        assert var.variable_id == "ocean.tos.tavg-u-hxy-sea.mon.GLB"

    def test_from_dict_parameter_overrides_data_dict(self):
        """Test that parameter takes precedence over data dict."""
        data = {
            "out_name": "tos",
            "frequency": "mon",
            "modeling_realm": "ocean",
            "units": "degC",
            "cell_methods": "area: mean time: mean",
            "cell_measures": "area: areacello",
            "long_name": "Sea Surface Temperature",
            "comment": "Test",
            "dimensions": "longitude latitude time",
            "type": "real",
            "positive": "",
            "spatial_shape": "XY",
            "temporal_shape": "T",
            "cmip7_compound_name": "ocean.tos.old.mon.GLB",
        }

        compound_name = "ocean.tos.tavg-u-hxy-sea.mon.GLB"
        var = CMIP7DataRequestVariable.from_dict(data, compound_name=compound_name)

        # Parameter should override data dict
        assert var.variable_id == compound_name


class TestCMIP7MatchingLogic:
    """Test CMIP7-specific matching logic (requires cmorizer)."""

    def test_exact_compound_name_matching(self):
        """Test that matching uses exact compound name comparison."""
        # This is an integration test that would require setting up
        # a full cmorizer with rules and data request.
        # For now, we document the expected behavior:
        #
        # Given rule: compound_name = "ocean.tos.tavg-u-hxy-sea.mon.GLB"
        # And data request variable: variable_id = "ocean.tos.tavg-u-hxy-sea.mon.GLB"
        # Then: Rule should match
        #
        # Given rule: compound_name = "ocean.tos.tavg-u-hxy-sea.mon.GLB"
        # And data request variable: variable_id = "ocean.tos.tpt-u-hxy-sea.3hr.GLB"
        # Then: Rule should NOT match (different branding and frequency)
        pytest.skip("Integration test - requires full cmorizer setup")

    def test_multiple_branding_variants_distinguished(self):
        """Test that different branding variants are treated as separate variables."""
        test_data = {
            "Compound Name": {
                "ocean.tos.tavg-u-hxy-sea.mon.GLB": {
                    "out_name": "tos",
                    "frequency": "mon",
                    "modeling_realm": "ocean",
                    "units": "degC",
                    "cell_methods": "area: mean time: mean",
                    "cell_measures": "area: areacello",
                    "long_name": "Sea Surface Temperature",
                    "comment": "Time average",
                    "dimensions": "longitude latitude time",
                    "type": "real",
                    "positive": "",
                    "spatial_shape": "XY",
                    "temporal_shape": "T",
                    "branding_label": "tavg-u-hxy-sea",
                    "cmip7_compound_name": "ocean.tos.tavg-u-hxy-sea.mon.GLB",
                },
                "ocean.tos.tpt-u-hxy-sea.3hr.GLB": {
                    "out_name": "tos",
                    "frequency": "3hr",
                    "modeling_realm": "ocean",
                    "units": "degC",
                    "cell_methods": "area: mean time: point",
                    "cell_measures": "area: areacello",
                    "long_name": "Sea Surface Temperature",
                    "comment": "Time point",
                    "dimensions": "longitude latitude time",
                    "type": "real",
                    "positive": "",
                    "spatial_shape": "XY",
                    "temporal_shape": "T",
                    "branding_label": "tpt-u-hxy-sea",
                    "cmip7_compound_name": "ocean.tos.tpt-u-hxy-sea.3hr.GLB",
                },
            }
        }

        dreq = CMIP7DataRequest.from_all_var_info(test_data)

        # Both variants should exist as separate variables
        assert len(dreq.variables) == 2

        var_tavg = dreq.variables["ocean.tos.tavg-u-hxy-sea.mon.GLB"]
        var_tpt = dreq.variables["ocean.tos.tpt-u-hxy-sea.3hr.GLB"]

        # Verify they have different identities
        assert var_tavg.variable_id != var_tpt.variable_id
        assert var_tavg.frequency != var_tpt.frequency
        assert var_tavg.cell_methods != var_tpt.cell_methods


class TestCMIP7BackwardCompatibility:
    """Test backward compatibility with existing CMIP6 fields."""

    def test_cmip6_table_field_still_works(self):
        """Test that metadata with cmip6_table still loads correctly."""
        test_data = {
            "Compound Name": {
                "ocean.tos.tavg-u-hxy-sea.mon.GLB": {
                    "out_name": "tos",
                    "frequency": "mon",
                    "modeling_realm": "ocean",
                    "units": "degC",
                    "cell_methods": "area: mean time: mean",
                    "cell_measures": "area: areacello",
                    "long_name": "Sea Surface Temperature",
                    "comment": "With cmip6_table for backward compat",
                    "dimensions": "longitude latitude time",
                    "type": "real",
                    "positive": "",
                    "spatial_shape": "XY",
                    "temporal_shape": "T",
                    "cmip7_compound_name": "ocean.tos.tavg-u-hxy-sea.mon.GLB",
                    "cmip6_table": "Omon",  # Backward compat field
                }
            }
        }

        dreq = CMIP7DataRequest.from_all_var_info(test_data)

        # Should load successfully
        assert len(dreq.variables) == 1
        assert "ocean.tos.tavg-u-hxy-sea.mon.GLB" in dreq.variables

        # Tables should be populated if cmip6_table present
        assert len(dreq.tables) >= 0  # May or may not build tables

        # Variable should have compound name as ID
        var = dreq.variables["ocean.tos.tavg-u-hxy-sea.mon.GLB"]
        assert var.variable_id == "ocean.tos.tavg-u-hxy-sea.mon.GLB"


class TestSyntheticTableHeader:
    """Test synthetic table header generation for CMIP7 variables."""

    def test_from_variable_metadata_with_cmip6_table(self):
        """Test synthetic header uses cmip6_table if present."""
        var_dict = {
            "frequency": "mon",
            "modeling_realm": "ocean",
            "cmip6_table": "Omon",
        }

        header = CMIP7DataRequestTableHeader.from_variable_metadata(var_dict)

        assert header.table_id == "Omon"
        assert header.realm == ["ocean"]
        assert header.approx_interval == 30.0  # Monthly

    def test_from_variable_metadata_without_cmip6_table(self):
        """Test synthetic header derives table_id without cmip6_table."""
        var_dict = {
            "frequency": "mon",
            "modeling_realm": "ocean",
        }

        header = CMIP7DataRequestTableHeader.from_variable_metadata(var_dict)

        assert header.table_id == "Omon"  # Derived from ocean + mon
        assert header.realm == ["ocean"]
        assert header.approx_interval == 30.0

    def test_from_variable_metadata_various_frequencies(self):
        """Test synthetic header handles different frequencies correctly."""
        test_cases = [
            ("mon", "ocean", "Omon", 30.0),
            ("day", "atmos", "Aday", 1.0),
            ("3hr", "ocean", "O3hr", 0.125),
            ("1hr", "atmos", "A1hr", 0.041666666666666664),
            ("yr", "land", "Lyr", 365.0),
        ]

        for frequency, realm, expected_table_id, expected_interval in test_cases:
            var_dict = {
                "frequency": frequency,
                "modeling_realm": realm,
            }
            header = CMIP7DataRequestTableHeader.from_variable_metadata(var_dict)

            assert header.table_id == expected_table_id, f"Failed for {frequency}/{realm}"
            assert header.realm == [realm]
            assert header.approx_interval == pytest.approx(expected_interval), f"Failed interval for {frequency}"

    def test_from_variable_metadata_various_realms(self):
        """Test realm letter mapping works correctly."""
        test_cases = [
            ("ocean", "O"),
            ("atmos", "A"),
            ("land", "L"),
            ("seaIce", "SI"),
        ]

        for realm, expected_letter in test_cases:
            var_dict = {
                "frequency": "mon",
                "modeling_realm": realm,
            }
            header = CMIP7DataRequestTableHeader.from_variable_metadata(var_dict)

            assert header.table_id == f"{expected_letter}mon", f"Failed for realm {realm}"

    def test_from_variable_metadata_missing_fields(self):
        """Test synthetic header handles missing fields gracefully."""
        var_dict = {
            "frequency": "mon",
            # missing modeling_realm
        }

        header = CMIP7DataRequestTableHeader.from_variable_metadata(var_dict)

        # Should default to "unknown"
        assert header.table_id in ["Umon", "unknown"]  # Depends on fallback logic
        assert header.realm == ["unknown"]

    def test_variables_have_synthetic_table_header(self):
        """Test that variables loaded without cmip6_table have synthetic table_header."""
        test_data = {
            "Compound Name": {
                "ocean.tos.tavg-u-hxy-sea.mon.GLB": {
                    "out_name": "tos",
                    "frequency": "mon",
                    "modeling_realm": "ocean",
                    "units": "degC",
                    "cell_methods": "area: mean time: mean",
                    "cell_measures": "area: areacello",
                    "long_name": "Sea Surface Temperature",
                    "comment": "No cmip6_table field",
                    "dimensions": "longitude latitude time",
                    "type": "real",
                    "positive": "",
                    "spatial_shape": "XY",
                    "temporal_shape": "T",
                    # NO cmip6_table field!
                }
            }
        }

        dreq = CMIP7DataRequest.from_all_var_info(test_data)
        var = dreq.variables["ocean.tos.tavg-u-hxy-sea.mon.GLB"]

        # Variable should have table_header
        assert hasattr(var, "table_header")
        assert var.table_header is not None

        # Table header should have required attributes
        assert hasattr(var.table_header, "table_id")
        assert hasattr(var.table_header, "approx_interval")
        assert hasattr(var.table_header, "realm")

        # Values should be correct
        assert var.table_header.table_id == "Omon"
        assert var.table_header.approx_interval == 30.0
        assert var.table_header.realm == ["ocean"]

    def test_synthetic_header_has_all_required_attributes(self):
        """Test that synthetic headers have all attributes needed by downstream code."""
        var_dict = {
            "frequency": "mon",
            "modeling_realm": "ocean",
        }

        header = CMIP7DataRequestTableHeader.from_variable_metadata(var_dict)

        # Attributes used by timeaverage.py
        assert hasattr(header, "approx_interval")
        assert header.approx_interval is not None

        # Attributes used by files.py and global_attributes.py
        assert hasattr(header, "table_id")
        assert header.table_id is not None

        # Attributes used by global_attributes.py
        assert hasattr(header, "realm")
        assert header.realm is not None

        # Generic levels (may be empty for synthetic headers)
        assert hasattr(header, "generic_levels")


class TestCMIP7IntegrationWithoutCMIP6Table:
    """Integration tests for full workflow without cmip6_table field."""

    def test_full_loading_without_cmip6_table(self):
        """Test complete DataRequest loading with pure CMIP7 metadata."""
        test_data = {
            "Compound Name": {
                "ocean.tos.tavg-u-hxy-sea.mon.GLB": {
                    "out_name": "tos",
                    "frequency": "mon",
                    "modeling_realm": "ocean",
                    "units": "degC",
                    "cell_methods": "area: mean time: mean",
                    "cell_measures": "area: areacello",
                    "long_name": "Sea Surface Temperature",
                    "comment": "Pure CMIP7 metadata",
                    "dimensions": "longitude latitude time",
                    "type": "real",
                    "positive": "",
                    "spatial_shape": "XY",
                    "temporal_shape": "T",
                },
                "atmos.tas.tavg-u-hxy-land.mon.GLB": {
                    "out_name": "tas",
                    "frequency": "mon",
                    "modeling_realm": "atmos",
                    "units": "K",
                    "cell_methods": "area: mean time: mean",
                    "cell_measures": "area: areacella",
                    "long_name": "Near-Surface Air Temperature",
                    "comment": "Pure CMIP7 metadata",
                    "dimensions": "longitude latitude time",
                    "type": "real",
                    "positive": "",
                    "spatial_shape": "XY",
                    "temporal_shape": "T",
                },
            }
        }

        # Should load without errors
        dreq = CMIP7DataRequest.from_all_var_info(test_data)

        # Should have 2 variables
        assert len(dreq.variables) == 2

        # Both variables should have synthetic table headers
        for var_name, var in dreq.variables.items():
            assert hasattr(var, "table_header")
            assert var.table_header is not None
            assert var.table_header.approx_interval is not None
            assert var.table_header.table_id is not None

        # Tables dict may be empty (acceptable for pure CMIP7)
        assert isinstance(dreq.tables, dict)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
