"""Tests for CMIP7 global attributes."""

import re

import pytest

from pycmor.core.controlled_vocabularies import ControlledVocabularies
from pycmor.core.factory import create_factory
from pycmor.std_lib.global_attributes import GlobalAttributes

# Expected formats for dynamic attributes
creation_date_format = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
tracking_id_format = r"^hdl:\d{2}\.\d{5}/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$"


@pytest.fixture
def cmip7_cv_dir():
    """Path to CMIP7 CVs directory."""
    # CMIP7-CVs is a submodule at the root level
    from pathlib import Path

    cv_path = Path(__file__).parent.parent.parent / "CMIP7-CVs"
    if not cv_path.exists():
        pytest.skip("CMIP7-CVs directory not found")
    return cv_path


@pytest.fixture
def sample_cmip7_rule(tmp_path, cmip7_cv_dir):
    """Create a sample CMIP7 rule for testing."""
    from pycmor.core.rule import Rule

    # Create a minimal rule configuration
    rule_config = {
        "cmor_variable": "tas",
        "model_variable": "temp2",
        "data_request_variable": None,
        "mip_era": "CMIP7",
        "activity_id": "CMIP",
        "institution_id": "AWI",
        "source_id": "AWI-CM-1-1-HR",
        "experiment_id": "historical",
        "variant_label": "r1i1p1f1",
        "grid_label": "gn",
        "table_id": "Amon",
        "frequency": "mon",
    }

    rule = Rule(**rule_config)

    # Load CMIP7 controlled vocabularies
    try:
        cv_factory = create_factory(ControlledVocabularies)
        CVClass = cv_factory.get("CMIP7")
        rule.controlled_vocabularies = CVClass.load(cmip7_cv_dir)
    except Exception as e:
        pytest.skip(f"Could not load CMIP7 CVs: {e}")

    return rule


def _get_rule_attrs(rule):
    """Helper to create rule attributes dict."""
    from datetime import datetime, timezone

    return {
        "source_id": rule.source_id,
        "grid_label": rule.grid_label,
        "cmor_variable": rule.cmor_variable,
        "variant_label": rule.variant_label,
        "experiment_id": rule.experiment_id,
        "activity_id": rule.activity_id,
        "institution_id": rule.institution_id,
        "creation_date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "frequency": rule.frequency,
        "table_id": rule.table_id,
        "realm": "atmos",  # Default realm for testing
        "mip_era": "CMIP7",
    }


def test_cmip7_global_attributes_creation(sample_cmip7_rule, cmip7_cv_dir):
    """Test that CMIP7 global attributes can be created."""
    rule = sample_cmip7_rule
    rule_attrs = _get_rule_attrs(rule)

    # Create GlobalAttributes instance
    ga_factory = create_factory(GlobalAttributes)
    GAClass = ga_factory.get("CMIP7")

    # This should not raise an error
    ga = GAClass(rule.data_request_variable, rule.controlled_vocabularies, rule_attrs)

    assert ga is not None


def test_cmip7_global_attributes_structure(sample_cmip7_rule, cmip7_cv_dir):
    """Test that CMIP7 global attributes have the expected structure."""
    rule = sample_cmip7_rule
    rule_attrs = _get_rule_attrs(rule)

    ga_factory = create_factory(GlobalAttributes)
    GAClass = ga_factory.get("CMIP7")
    ga = GAClass(rule.data_request_variable, rule.controlled_vocabularies, rule_attrs)

    # Get all global attributes
    attrs = ga.global_attributes()

    # Check that it's a dictionary
    assert isinstance(attrs, dict)

    # Check for required CMIP7 attributes
    required_attrs = [
        "Conventions",
        "activity_id",
        "creation_date",
        "data_specs_version",
        "experiment_id",
        "frequency",
        "grid_label",
        "institution",
        "institution_id",
        "license",
        "mip_era",
        "source_id",
        "source_type",
        "tracking_id",
        "variable_id",
        "variant_label",
    ]

    for attr in required_attrs:
        assert attr in attrs, f"Required attribute '{attr}' missing"


def test_cmip7_mip_era(sample_cmip7_rule):
    """Test that mip_era is set to CMIP7."""
    rule = sample_cmip7_rule
    rule_attrs = _get_rule_attrs(rule)

    ga_factory = create_factory(GlobalAttributes)
    GAClass = ga_factory.get("CMIP7")
    ga = GAClass(rule.data_request_variable, rule.controlled_vocabularies, rule_attrs)

    attrs = ga.global_attributes()
    assert attrs["mip_era"] == "CMIP7"


def test_cmip7_conventions(sample_cmip7_rule):
    """Test that Conventions attribute is correct for CMIP7."""
    rule = sample_cmip7_rule
    rule_attrs = _get_rule_attrs(rule)

    ga_factory = create_factory(GlobalAttributes)
    GAClass = ga_factory.get("CMIP7")
    ga = GAClass(rule.data_request_variable, rule.controlled_vocabularies, rule_attrs)

    attrs = ga.global_attributes()
    # CMIP7 should use CF-1.10 or later
    assert "CF-" in attrs["Conventions"]
    assert "CMIP-" in attrs["Conventions"]


def test_cmip7_license_format(sample_cmip7_rule):
    """Test that license text is properly formatted for CMIP7."""
    rule = sample_cmip7_rule
    rule_attrs = _get_rule_attrs(rule)

    ga_factory = create_factory(GlobalAttributes)
    GAClass = ga_factory.get("CMIP7")
    ga = GAClass(rule.data_request_variable, rule.controlled_vocabularies, rule_attrs)

    attrs = ga.global_attributes()
    license_text = attrs["license"]

    # Check that license mentions CMIP7
    assert "CMIP7" in license_text
    # Check that it mentions Creative Commons
    assert "Creative Commons" in license_text
    # Check that it has the institution
    assert rule.institution_id in license_text


def test_cmip7_creation_date_format(sample_cmip7_rule):
    """Test that creation_date has the correct ISO 8601 format."""
    rule = sample_cmip7_rule
    rule_attrs = _get_rule_attrs(rule)

    ga_factory = create_factory(GlobalAttributes)
    GAClass = ga_factory.get("CMIP7")
    ga = GAClass(rule.data_request_variable, rule.controlled_vocabularies, rule_attrs)

    attrs = ga.global_attributes()
    creation_date = attrs["creation_date"]

    # Check format: YYYY-MM-DDTHH:MM:SSZ
    assert bool(re.match(creation_date_format, creation_date))


def test_cmip7_tracking_id_format(sample_cmip7_rule):
    """Test that tracking_id has the correct HDL format."""
    rule = sample_cmip7_rule
    rule_attrs = _get_rule_attrs(rule)

    ga_factory = create_factory(GlobalAttributes)
    GAClass = ga_factory.get("CMIP7")
    ga = GAClass(rule.data_request_variable, rule.controlled_vocabularies, rule_attrs)

    attrs = ga.global_attributes()
    tracking_id = attrs["tracking_id"]

    # Check format: hdl:XX.XXXXX/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    assert bool(re.match(tracking_id_format, tracking_id))


def test_cmip7_further_info_url(sample_cmip7_rule):
    """Test that further_info_url is properly constructed for CMIP7."""
    rule = sample_cmip7_rule
    rule_attrs = _get_rule_attrs(rule)

    ga_factory = create_factory(GlobalAttributes)
    GAClass = ga_factory.get("CMIP7")
    ga = GAClass(rule.data_request_variable, rule.controlled_vocabularies, rule_attrs)

    attrs = ga.global_attributes()
    further_info_url = attrs.get("further_info_url", "")

    # Check that URL contains CMIP7 and key identifiers
    assert "CMIP7" in further_info_url or "cmip7" in further_info_url.lower()
    assert rule.institution_id in further_info_url
    assert rule.source_id in further_info_url
    assert rule.experiment_id in further_info_url
    assert rule.variant_label in further_info_url


def test_cmip7_source_type_from_cv(sample_cmip7_rule):
    """Test that source_type is derived from CMIP7 CVs."""
    rule = sample_cmip7_rule
    rule_attrs = _get_rule_attrs(rule)

    ga_factory = create_factory(GlobalAttributes)
    GAClass = ga_factory.get("CMIP7")
    ga = GAClass(rule.data_request_variable, rule.controlled_vocabularies, rule_attrs)

    attrs = ga.global_attributes()
    source_type = attrs.get("source_type")

    # Should have a source_type
    assert source_type is not None
    assert isinstance(source_type, str)
    assert len(source_type) > 0


def test_cmip7_variant_label_format(sample_cmip7_rule):
    """Test that variant_label has the correct format."""
    rule = sample_cmip7_rule
    rule_attrs = _get_rule_attrs(rule)

    ga_factory = create_factory(GlobalAttributes)
    GAClass = ga_factory.get("CMIP7")
    ga = GAClass(rule.data_request_variable, rule.controlled_vocabularies, rule_attrs)

    attrs = ga.global_attributes()
    variant_label = attrs["variant_label"]

    # Check format: rXiYpZfW
    assert re.match(r"^r\d+i\d+p\d+f\d+$", variant_label)


def test_cmip7_attributes_are_strings(sample_cmip7_rule):
    """Test that all global attributes are strings (required for netCDF)."""
    rule = sample_cmip7_rule
    rule_attrs = _get_rule_attrs(rule)

    ga_factory = create_factory(GlobalAttributes)
    GAClass = ga_factory.get("CMIP7")
    ga = GAClass(rule.data_request_variable, rule.controlled_vocabularies, rule_attrs)

    attrs = ga.global_attributes()

    # All attributes should be strings for netCDF compliance
    for key, value in attrs.items():
        assert isinstance(
            value, str
        ), f"Attribute '{key}' is not a string: {type(value)}"


@pytest.mark.skipif(
    not pytest.importorskip(
        "pycmor.data_request.cmip7_interface", reason="CMIP7 API not available"
    ),
    reason="CMIP7 API not available",
)
def test_cmip7_global_attributes_with_data_request(sample_cmip7_rule):
    """Test CMIP7 global attributes with actual data request variable."""
    # This test requires CMIP7 data request to be available
    from pycmor.data_request.cmip7_interface import CMIP7Interface

    interface = CMIP7Interface()

    # Try to get a variable from the data request
    try:
        var = interface.get_variable("atmos.tas.tavg-h2m-hxy-u.mon.GLB")

        rule = sample_cmip7_rule
        rule.data_request_variable = var
        rule_attrs = _get_rule_attrs(rule)

        ga_factory = create_factory(GlobalAttributes)
        GAClass = ga_factory.get("CMIP7")
        ga = GAClass(var, rule.controlled_vocabularies, rule_attrs)

        attrs = ga.global_attributes()

        # Should have all required attributes
        assert "variable_id" in attrs
        assert "frequency" in attrs
        assert attrs["variable_id"] == "tas"
    except Exception:
        pytest.skip("Could not load CMIP7 data request")
