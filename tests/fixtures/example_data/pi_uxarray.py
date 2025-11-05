"""Example data for the FESOM model."""

import tarfile
from pathlib import Path

import pytest
import requests

URL = "https://nextcloud.awi.de/s/swqyFgbL2jjgjRo/download/pi_uxarray.tar"
"""str : URL to download the example data from."""

MESH_URL = "https://nextcloud.awi.de/s/FCPZmBJGeGaji4y/download/pi_mesh.tgz"
"""str : URL to download the mesh data from."""


@pytest.fixture(scope="session")
def pi_uxarray_download_data(tmp_path_factory):
    # Use persistent cache in $HOME/.cache/pycmor instead of ephemeral /tmp
    cache_dir = Path.home() / ".cache" / "pycmor" / "test_data"
    cache_dir.mkdir(parents=True, exist_ok=True)
    data_path = cache_dir / "pi_uxarray.tar"

    if not data_path.exists():
        response = requests.get(URL)
        response.raise_for_status()
        with open(data_path, "wb") as f:
            f.write(response.content)
        print(f"Data downloaded: {data_path}.")
    else:
        print(f"Using cached data: {data_path}.")

    return data_path


@pytest.fixture(scope="session")
def pi_uxarray_data(pi_uxarray_download_data):

    data_dir = Path(pi_uxarray_download_data).parent
    with tarfile.open(pi_uxarray_download_data, "r") as tar:
        tar.extractall(data_dir)

    return data_dir / "pi_uxarray"


@pytest.fixture(scope="session")
def pi_uxarray_download_mesh(tmp_path_factory):
    # Use persistent cache in $HOME/.cache/pycmor instead of ephemeral /tmp
    cache_dir = Path.home() / ".cache" / "pycmor" / "test_data"
    cache_dir.mkdir(parents=True, exist_ok=True)
    data_path = cache_dir / "pi_mesh.tar"

    if not data_path.exists():
        response = requests.get(MESH_URL)
        response.raise_for_status()
        with open(data_path, "wb") as f:
            f.write(response.content)
        print(f"Data downloaded: {data_path}.")
    else:
        print(f"Using cached data: {data_path}.")

    return data_path


@pytest.fixture(scope="session")
def pi_uxarray_mesh(pi_uxarray_download_mesh):
    data_dir = Path(pi_uxarray_download_mesh).parent
    with tarfile.open(pi_uxarray_download_mesh, "r") as tar:
        tar.extractall(data_dir)

    return data_dir / "pi"
