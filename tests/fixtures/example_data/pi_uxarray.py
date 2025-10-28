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
    cache_dir = tmp_path_factory.getbasetemp() / "cached_data"
    cache_dir.mkdir(exist_ok=True)
    data_path = cache_dir / "pi_uxarray.tar"

    if not data_path.exists():
        print(f"Downloading data from {URL}...")
        response = requests.get(URL, stream=True)
        response.raise_for_status()

        # Download with streaming to avoid memory issues
        total_size = int(response.headers.get("content-length", 0))
        with open(data_path, "wb") as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)

        # Verify download completed
        actual_size = data_path.stat().st_size
        if total_size > 0 and actual_size != total_size:
            data_path.unlink()
            raise RuntimeError(
                f"Download incomplete: expected {total_size} bytes, got {actual_size} bytes"
            )

        print(f"Data downloaded: {data_path} ({actual_size} bytes).")
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
    cache_dir = tmp_path_factory.getbasetemp() / "cached_data"
    cache_dir.mkdir(exist_ok=True)
    data_path = cache_dir / "pi_mesh.tar"

    if not data_path.exists():
        print(f"Downloading mesh data from {MESH_URL}...")
        response = requests.get(MESH_URL, stream=True)
        response.raise_for_status()

        # Download with streaming to avoid memory issues
        total_size = int(response.headers.get("content-length", 0))
        with open(data_path, "wb") as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)

        # Verify download completed
        actual_size = data_path.stat().st_size
        if total_size > 0 and actual_size != total_size:
            data_path.unlink()
            raise RuntimeError(
                f"Download incomplete: expected {total_size} bytes, got {actual_size} bytes"
            )

        print(f"Mesh data downloaded: {data_path} ({actual_size} bytes).")
    else:
        print(f"Using cached mesh data: {data_path}.")

    return data_path


@pytest.fixture(scope="session")
def pi_uxarray_mesh(pi_uxarray_download_mesh):
    data_dir = Path(pi_uxarray_download_mesh).parent
    with tarfile.open(pi_uxarray_download_mesh, "r") as tar:
        tar.extractall(data_dir)

    return data_dir / "pi"
