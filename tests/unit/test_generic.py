import shutil
import tempfile
from pathlib import Path

from pycmor.std_lib.generic import create_cmor_directories


def test_create_cmor_directories():
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp()

    # TODO(PG): Move this to a fixture later
    # Define a sample config dictionary
    config = {
        "mip_era": "CMIP6",
        "activity_id": "ScenarioMIP",
        "institution_id": "AWI",
        "source_id": "AWI-ESM-1-1-LR",
        "experiment_id": "ssp126",
        "member_id": "r1i1p1f1",
        "table_id": "Amon",
        "variable_id": "tas",
        "grid_label": "gn",
        "version": "v20190719",
        "output_root": temp_dir,
    }

    # Call the function with the sample config
    updated_config = create_cmor_directories(config)

    # Construct the expected output directory path
    expected_output_dir = (
        Path(temp_dir)
        / "CMIP6"
        / "ScenarioMIP"
        / "AWI"
        / "AWI-ESM-1-1-LR"
        / "ssp126"
        / "r1i1p1f1"
        / "Amon"
        / "tas"
        / "gn"
        / "v20190719"
    )

    # Assert that the output directory was created
    assert expected_output_dir.exists()

    # Assert that the output directory path was added to the config dictionary
    assert "output_dir" in updated_config
    assert updated_config["output_dir"] == expected_output_dir

    # Clean up the temporary directory
    shutil.rmtree(temp_dir)
