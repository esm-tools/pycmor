"""Example data for FESOM 2.6 PI mesh."""

import pytest

from tests.fixtures.helpers.data_download import download_and_extract_fixture

URL = "https://nextcloud.awi.de/s/AL2cFQx5xGE473S/download/fesom_2p6_pimesh.tar"
"""str : URL to download the example data from."""


@pytest.fixture(scope="session")
def fesom_2p6_pimesh_esm_tools_data(tmp_path_factory):
    """Fixture providing FESOM 2.6 PI mesh test data.

    Downloads and extracts test data from AWI Nextcloud.
    Data is cached across test sessions to avoid repeated downloads.

    Returns:
        Path to the extracted fesom_2p6_pimesh directory
    """
    return download_and_extract_fixture(
        tmp_path_factory,
        url=URL,
        archive_name="fesom_2p6_pimesh.tar",
        extracted_subdir="fesom_2p6_pimesh",
    )
