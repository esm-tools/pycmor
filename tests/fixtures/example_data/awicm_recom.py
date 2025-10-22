"""Example data for AWICM 1.0 RECOM model."""

import pytest

from tests.fixtures.helpers.data_download import download_and_extract_fixture

URL = "https://nextcloud.awi.de/s/DaQjtTS9xB7o7pL/download/awicm_1p0_recom.tar"
"""str : URL to download the example data from."""


@pytest.fixture(scope="session")
def awicm_1p0_recom_data(tmp_path_factory):
    """Fixture providing AWICM 1.0 RECOM test data.

    Downloads and extracts test data from AWI Nextcloud.
    Data is cached across test sessions to avoid repeated downloads.

    Returns:
        Path to the extracted awicm_1p0_recom directory
    """
    return download_and_extract_fixture(
        tmp_path_factory,
        url=URL,
        archive_name="awicm_1p0_recom.tar",
        extracted_subdir="awicm_1p0_recom",
    )
