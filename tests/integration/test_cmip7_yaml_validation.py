"""Integration tests for CMIP7 YAML configuration validation."""

import pytest
import yaml

from pycmor.core.validate import GENERAL_VALIDATOR, RULES_VALIDATOR


@pytest.fixture
def cmip7_minimal_config():
    """Minimal valid CMIP7 configuration."""
    return {
        "general": {
            "name": "test-cmip7",
            "cmor_version": "CMIP7",
            "CV_Dir": "/path/to/CMIP7-CVs",
            "CMIP7_DReq_metadata": "/path/to/dreq_metadata.json",
        },
        "rules": [
            {
                "name": "tas",
                "compound_name": "atmos.tas.tavg-h2m-hxy-u.mon.GLB",
                "model_variable": "temp2",
                "inputs": [{"path": "/path/to/data", "pattern": "*.nc"}],
                "source_id": "AWI-CM-1-1-HR",
                "institution_id": "AWI",
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
                "output_directory": "/path/to/output",
            }
        ],
    }


@pytest.fixture
def cmip7_full_config():
    """Full CMIP7 configuration with all optional fields."""
    return {
        "general": {
            "name": "test-cmip7-full",
            "cmor_version": "CMIP7",
            "mip": "CMIP",
            "CV_Dir": "/path/to/CMIP7-CVs",
            "CMIP7_DReq_metadata": "/path/to/dreq_metadata.json",
        },
        "rules": [
            {
                "name": "tas",
                "compound_name": "atmos.tas.tavg-h2m-hxy-u.mon.GLB",
                "model_variable": "temp2",
                "inputs": [{"path": "/path/to/data", "pattern": "*.nc"}],
                "source_id": "AWI-CM-1-1-HR",
                "institution_id": "AWI",
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
                "output_directory": "/path/to/output",
                # Optional fields
                "grid": "T63 Gaussian grid",
                "nominal_resolution": "250 km",
                "realm": "atmos",
                "frequency": "mon",
                "table_id": "Amon",
            }
        ],
    }


@pytest.fixture
def cmip7_without_compound_name():
    """CMIP7 configuration without compound name (manual specification)."""
    return {
        "general": {
            "name": "test-cmip7-manual",
            "cmor_version": "CMIP7",
            "CV_Dir": "/path/to/CMIP7-CVs",
        },
        "rules": [
            {
                "name": "fgco2",
                "cmor_variable": "fgco2",
                "model_variable": "CO2f",
                "inputs": [{"path": "/path/to/data", "pattern": "*.nc"}],
                "source_id": "AWI-CM-1-1-HR",
                "institution_id": "AWI",
                "experiment_id": "piControl",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
                "output_directory": "/path/to/output",
                # Must specify these manually without compound_name
                "frequency": "mon",
                "realm": "ocnBgchem",
                "table_id": "Omon",
            }
        ],
    }


@pytest.fixture
def cmip6_config():
    """CMIP6 configuration for comparison."""
    return {
        "general": {
            "name": "test-cmip6",
            "cmor_version": "CMIP6",
            "CV_Dir": "/path/to/CMIP6_CVs",
            "CMIP_Tables_Dir": "/path/to/cmip6-cmor-tables/Tables",
        },
        "rules": [
            {
                "name": "fgco2",
                "cmor_variable": "fgco2",
                "model_variable": "CO2f",
                "inputs": [{"path": "/path/to/data", "pattern": "*.nc"}],
                "source_id": "AWI-CM-1-1-HR",
                "experiment_id": "piControl",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
                "output_directory": "/path/to/output",
                "model_component": "ocnBgchem",
            }
        ],
    }


def test_cmip7_minimal_config_validates(cmip7_minimal_config):
    """Test that minimal CMIP7 configuration validates."""
    # Validate general section
    assert GENERAL_VALIDATOR.validate({"general": cmip7_minimal_config["general"]}), GENERAL_VALIDATOR.errors

    # Validate rules section
    assert RULES_VALIDATOR.validate({"rules": cmip7_minimal_config["rules"]}), RULES_VALIDATOR.errors


def test_cmip7_full_config_validates(cmip7_full_config):
    """Test that full CMIP7 configuration validates."""
    # Validate general section
    assert GENERAL_VALIDATOR.validate({"general": cmip7_full_config["general"]}), GENERAL_VALIDATOR.errors

    # Validate rules section
    assert RULES_VALIDATOR.validate({"rules": cmip7_full_config["rules"]}), RULES_VALIDATOR.errors


def test_cmip7_without_compound_name_validates(cmip7_without_compound_name):
    """Test that CMIP7 config without compound name validates."""
    # Validate general section
    assert GENERAL_VALIDATOR.validate({"general": cmip7_without_compound_name["general"]}), GENERAL_VALIDATOR.errors

    # Validate rules section
    assert RULES_VALIDATOR.validate({"rules": cmip7_without_compound_name["rules"]}), RULES_VALIDATOR.errors


def test_cmip6_config_validates(cmip6_config):
    """Test that CMIP6 configuration still validates."""
    # Validate general section
    assert GENERAL_VALIDATOR.validate({"general": cmip6_config["general"]}), GENERAL_VALIDATOR.errors

    # Validate rules section
    assert RULES_VALIDATOR.validate({"rules": cmip6_config["rules"]}), RULES_VALIDATOR.errors


def test_cmip7_cv_dir_is_optional():
    """Test that CV_Dir is optional for CMIP7 (uses ResourceLoader fallback)."""
    config = {
        "general": {
            "name": "test",
            "cmor_version": "CMIP7",
            # CV_Dir is optional - will use ResourceLoader priority chain
        }
    }
    assert GENERAL_VALIDATOR.validate(config), GENERAL_VALIDATOR.errors


def test_cmip7_cv_version_field():
    """Test that CV_version field is accepted."""
    config = {
        "general": {
            "name": "test",
            "cmor_version": "CMIP7",
            "CV_version": "src-data",
        }
    }
    assert GENERAL_VALIDATOR.validate(config), GENERAL_VALIDATOR.errors


def test_cmip7_dreq_version_field():
    """Test that CMIP7_DReq_version field is accepted."""
    config = {
        "general": {
            "name": "test",
            "cmor_version": "CMIP7",
            "CMIP7_DReq_version": "v1.2.2.2",
        }
    }
    assert GENERAL_VALIDATOR.validate(config), GENERAL_VALIDATOR.errors


def test_cmip7_compound_name_field_accepted():
    """Test that compound_name field is accepted in rules."""
    config = {
        "rules": [
            {
                "name": "tas",
                "compound_name": "atmos.tas.tavg-h2m-hxy-u.mon.GLB",
                "model_variable": "temp2",
                "inputs": [{"path": "/path", "pattern": "*.nc"}],
                "source_id": "AWI-CM-1-1-HR",
                "institution_id": "AWI",
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
                "output_directory": "/path/to/output",
            }
        ]
    }
    assert RULES_VALIDATOR.validate(config), RULES_VALIDATOR.errors


def test_cmip7_optional_fields_accepted():
    """Test that CMIP7 optional fields are accepted."""
    config = {
        "rules": [
            {
                "name": "tas",
                "cmor_variable": "tas",
                "model_variable": "temp2",
                "inputs": [{"path": "/path", "pattern": "*.nc"}],
                "source_id": "AWI-CM-1-1-HR",
                "institution_id": "AWI",
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
                "output_directory": "/path/to/output",
                # CMIP7 optional fields
                "grid": "T63 grid",
                "nominal_resolution": "250 km",
                "realm": "atmos",
                "frequency": "mon",
                "table_id": "Amon",
            }
        ]
    }
    assert RULES_VALIDATOR.validate(config), RULES_VALIDATOR.errors


def test_variant_label_format_validation():
    """Test that variant_label format is validated."""
    # Valid format
    config_valid = {
        "rules": [
            {
                "name": "tas",
                "cmor_variable": "tas",
                "model_variable": "temp2",
                "inputs": [{"path": "/path", "pattern": "*.nc"}],
                "source_id": "AWI-CM-1-1-HR",
                "experiment_id": "historical",
                "variant_label": "r1i1p1f1",
                "grid_label": "gn",
                "output_directory": "/path/to/output",
                "model_component": "atmos",
            }
        ]
    }
    assert RULES_VALIDATOR.validate(config_valid), RULES_VALIDATOR.errors

    # Invalid format
    config_invalid = {
        "rules": [
            {
                "name": "tas",
                "cmor_variable": "tas",
                "model_variable": "temp2",
                "inputs": [{"path": "/path", "pattern": "*.nc"}],
                "source_id": "AWI-CM-1-1-HR",
                "experiment_id": "historical",
                "variant_label": "invalid",  # Wrong format
                "grid_label": "gn",
                "output_directory": "/path/to/output",
                "model_component": "atmos",
            }
        ]
    }
    assert not RULES_VALIDATOR.validate(config_invalid)


def test_cmip7_dreq_metadata_field():
    """Test that CMIP7_DReq_metadata field is accepted."""
    config = {
        "general": {
            "name": "test",
            "cmor_version": "CMIP7",
            "CV_Dir": "/path/to/CMIP7-CVs",
            "CMIP7_DReq_metadata": "/path/to/dreq_metadata.json",
        }
    }
    assert GENERAL_VALIDATOR.validate(config), GENERAL_VALIDATOR.errors


def test_yaml_example_file_validates(tmp_path):
    """Test that the example YAML file validates."""
    yaml_content = """
general:
  name: "cmip7-test"
  cmor_version: "CMIP7"
  CV_Dir: "/path/to/CMIP7-CVs"
  CMIP7_DReq_metadata: "/path/to/dreq_metadata.json"

rules:
  - name: tas
    compound_name: atmos.tas.tavg-h2m-hxy-u.mon.GLB
    model_variable: temp2
    inputs:
      - path: /path/to/data
        pattern: "*.nc"
    source_id: AWI-CM-1-1-HR
    institution_id: AWI
    experiment_id: historical
    variant_label: r1i1p1f1
    grid_label: gn
    grid: "T63 Gaussian grid"
    nominal_resolution: "250 km"
    output_directory: /path/to/output
"""
    config = yaml.safe_load(yaml_content)

    # Validate general section
    assert GENERAL_VALIDATOR.validate({"general": config["general"]}), GENERAL_VALIDATOR.errors

    # Validate rules section
    assert RULES_VALIDATOR.validate({"rules": config["rules"]}), RULES_VALIDATOR.errors
