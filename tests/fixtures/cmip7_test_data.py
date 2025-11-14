"""
Fixtures and test data for CMIP7 interface tests.
"""

import json
import shutil
import subprocess

import pytest

# Sample metadata for testing CMIP7 interface
SAMPLE_CMIP7_METADATA = {
    "Header": {
        "Description": "Test metadata",
        "no. of variables": 3,
        "dreq content version": "v1.2.2.2",
    },
    "Compound Name": {
        "atmos.tas.tavg-h2m-hxy-u.mon.GLB": {
            "frequency": "mon",
            "modeling_realm": "atmos",
            "standard_name": "air_temperature",
            "units": "K",
            "cell_methods": "area: time: mean",
            "cell_measures": "area: areacella",
            "long_name": "Near-Surface Air Temperature",
            "comment": "Near-surface air temperature",
            "dimensions": "longitude latitude time height2m",
            "out_name": "tas",
            "type": "real",
            "positive": "",
            "spatial_shape": "XY-na",
            "temporal_shape": "time-mean",
            "cmip6_table": "Amon",
            "physical_parameter_name": "tas",
            "branding_label": "tavg-h2m-hxy-u",
            "region": "GLB",
            "cmip6_compound_name": "Amon.tas",
            "cmip7_compound_name": "atmos.tas.tavg-h2m-hxy-u.mon.GLB",
        },
        "atmos.clt.tavg-u-hxy-u.mon.GLB": {
            "frequency": "mon",
            "modeling_realm": "atmos",
            "standard_name": "cloud_area_fraction",
            "units": "1",
            "cell_methods": "area: time: mean",
            "cell_measures": "area: areacella",
            "long_name": "Total Cloud Fraction",
            "comment": "Total cloud fraction",
            "dimensions": "longitude latitude time",
            "out_name": "clt",
            "type": "real",
            "positive": "",
            "spatial_shape": "XY-na",
            "temporal_shape": "time-mean",
            "cmip6_table": "Amon",
            "physical_parameter_name": "clt",
            "branding_label": "tavg-u-hxy-u",
            "region": "GLB",
            "cmip6_compound_name": "Amon.clt",
            "cmip7_compound_name": "atmos.clt.tavg-u-hxy-u.mon.GLB",
        },
        "atmos.clt.tavg-u-hxy-u.day.GLB": {
            "frequency": "day",
            "modeling_realm": "atmos",
            "standard_name": "cloud_area_fraction",
            "units": "1",
            "cell_methods": "area: time: mean",
            "cell_measures": "area: areacella",
            "long_name": "Total Cloud Fraction",
            "comment": "Total cloud fraction",
            "dimensions": "longitude latitude time",
            "out_name": "clt",
            "type": "real",
            "positive": "",
            "spatial_shape": "XY-na",
            "temporal_shape": "time-mean",
            "cmip6_table": "day",
            "physical_parameter_name": "clt",
            "branding_label": "tavg-u-hxy-u",
            "region": "GLB",
            "cmip6_compound_name": "day.clt",
            "cmip7_compound_name": "atmos.clt.tavg-u-hxy-u.day.GLB",
        },
    },
}

# Sample experiments data for testing CMIP7 interface
SAMPLE_CMIP7_EXPERIMENTS_DATA = {
    "Header": {
        "Description": "Test experiments",
        "Opportunities supported": ["Test Opportunity"],
        "Priority levels supported": ["Core", "High", "Medium", "Low"],
        "Experiments included": ["historical", "piControl"],
    },
    "experiment": {
        "historical": {
            "Core": [
                "atmos.tas.tavg-h2m-hxy-u.mon.GLB",
                "atmos.clt.tavg-u-hxy-u.mon.GLB",
            ],
            "High": [
                "atmos.clt.tavg-u-hxy-u.day.GLB",
            ],
        },
        "piControl": {
            "Core": [
                "atmos.tas.tavg-h2m-hxy-u.mon.GLB",
            ],
        },
    },
}


@pytest.fixture
def cmip7_sample_metadata():
    """Return sample CMIP7 metadata dictionary."""
    return SAMPLE_CMIP7_METADATA


@pytest.fixture
def cmip7_sample_experiments_data():
    """Return sample CMIP7 experiments data dictionary."""
    return SAMPLE_CMIP7_EXPERIMENTS_DATA


@pytest.fixture
def cmip7_metadata_file(tmp_path, cmip7_sample_metadata):
    """Create a temporary CMIP7 metadata JSON file."""
    metadata_file = tmp_path / "test_cmip7_metadata.json"
    with open(metadata_file, "w") as f:
        json.dump(cmip7_sample_metadata, f)
    return metadata_file


@pytest.fixture
def cmip7_experiments_file(tmp_path, cmip7_sample_experiments_data):
    """Create a temporary CMIP7 experiments JSON file."""
    experiments_file = tmp_path / "test_cmip7_experiments.json"
    with open(experiments_file, "w") as f:
        json.dump(cmip7_sample_experiments_data, f)
    return experiments_file


@pytest.fixture
def cmip7_interface_with_metadata(cmip7_metadata_file):
    """Create a CMIP7Interface instance with loaded metadata."""
    from pycmor.data_request.cmip7_interface import CMIP7_API_AVAILABLE, CMIP7Interface

    if not CMIP7_API_AVAILABLE:
        pytest.skip("CMIP7 API not available")

    interface = CMIP7Interface()
    interface.load_metadata(metadata_file=cmip7_metadata_file)
    return interface


@pytest.fixture
def cmip7_interface_with_all_data(cmip7_metadata_file, cmip7_experiments_file):
    """Create a CMIP7Interface instance with metadata and experiments loaded."""
    from pycmor.data_request.cmip7_interface import CMIP7_API_AVAILABLE, CMIP7Interface

    if not CMIP7_API_AVAILABLE:
        pytest.skip("CMIP7 API not available")

    interface = CMIP7Interface()
    interface.load_metadata(metadata_file=cmip7_metadata_file)
    interface.load_experiments_data(cmip7_experiments_file)
    return interface


@pytest.fixture(scope="session")
def cmip7_data_request_dir(tmp_path_factory):
    """Create CMIP7 data request directory with metadata JSON file.

    This fixture runs get_variables_metadata to generate the required
    metadata file in the CMIP7_DReq_Software/scripts/variable_info/ directory
    structure expected by CMIP7 tests.

    Returns
    -------
    Path
        Path to the created CMIP7_DReq_Software/scripts/variable_info directory
    """
    # Check if get_variables_metadata command is available
    if not shutil.which("get_variables_metadata"):
        pytest.skip("get_variables_metadata command not available (CMIP7 Data Request API not installed)")

    # Create the directory structure
    base_dir = tmp_path_factory.mktemp("cmip7_test")
    variable_info_dir = base_dir / "CMIP7_DReq_Software" / "scripts" / "variable_info"
    variable_info_dir.mkdir(parents=True, exist_ok=True)

    # Generate metadata JSON file using get_variables_metadata
    output_file = variable_info_dir / "all_vars_info.json"
    version = "v1.2.2.2"

    result = subprocess.run(
        ["get_variables_metadata", version, str(output_file)],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        pytest.skip(
            f"Failed to generate CMIP7 metadata: {result.stderr}\n"
            f"Command: get_variables_metadata {version} {output_file}"
        )

    if not output_file.exists():
        pytest.skip(f"Metadata file not found after export: {output_file}")

    return variable_info_dir
