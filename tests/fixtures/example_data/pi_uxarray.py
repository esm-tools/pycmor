"""Example data for PI mesh with uxarray support."""

import pytest

from tests.fixtures.helpers.data_download import download_and_extract_fixture

URL = "https://nextcloud.awi.de/s/swqyFgbL2jjgjRo/download/pi_uxarray.tar"
"""str : URL to download the example data from."""

MESH_URL = "https://nextcloud.awi.de/s/FCPZmBJGeGaji4y/download/pi_mesh.tgz"
"""str : URL to download the mesh data from."""


@pytest.fixture(scope="session")
def pi_uxarray_data(tmp_path_factory):
    """Fixture providing PI uxarray test data.

    Downloads and extracts test data from AWI Nextcloud.
    Data is cached across test sessions to avoid repeated downloads.

    Returns:
        Path to the extracted pi_uxarray directory
    """
    return download_and_extract_fixture(
        tmp_path_factory,
        url=URL,
        archive_name="pi_uxarray.tar",
        extracted_subdir="pi_uxarray",
    )


@pytest.fixture(scope="session")
def pi_uxarray_mesh(tmp_path_factory):
    """Fixture providing PI mesh data.

    Downloads and extracts mesh data from AWI Nextcloud.
    Data is cached across test sessions to avoid repeated downloads.

    Returns:
        Path to the extracted pi directory containing mesh files
    """
    return download_and_extract_fixture(
        tmp_path_factory,
        url=MESH_URL,
        archive_name="pi_mesh.tgz",
        extracted_subdir="pi",
    )
