"""Example data for the FESOM model."""

import os
import tarfile
from pathlib import Path

import pytest
import requests

from tests.fixtures.stub_generator import generate_stub_files

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
        print(f"Downloading test data from {URL}...")
        try:
            response = requests.get(URL, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            error_msg = (
                f"Failed to download test data from {URL}\n"
                f"Error type: {type(e).__name__}\n"
                f"Error details: {str(e)}\n"
            )
            if hasattr(e, "response") and e.response is not None:
                error_msg += (
                    f"HTTP Status Code: {e.response.status_code}\n"
                    f"Response Headers: {dict(e.response.headers)}\n"
                    f"Response Content (first 500 chars): {e.response.text[:500]}\n"
                )
            print(error_msg)
            raise RuntimeError(error_msg) from e

        with open(data_path, "wb") as f:
            f.write(response.content)
        print(f"Data downloaded: {data_path}.")
    else:
        print(f"Using cached data: {data_path}.")

    return data_path


@pytest.fixture(scope="session")
def pi_uxarray_real_data(pi_uxarray_download_data):

    data_dir = Path(pi_uxarray_download_data).parent
    with tarfile.open(pi_uxarray_download_data, "r") as tar:
        tar.extractall(data_dir)

    return data_dir / "pi_uxarray"


@pytest.fixture(scope="session")
def pi_uxarray_stub_data(tmp_path_factory):
    """
    Generate stub data for pi_uxarray from YAML manifest.
    Returns the data directory containing generated NetCDF files.
    """
    # Create temporary directory for stub data
    stub_dir = tmp_path_factory.mktemp("pi_uxarray_stub")

    # Path to the YAML manifest
    manifest_file = Path(__file__).parent.parent / "stub_data" / "pi_uxarray.yaml"

    # Generate stub files from manifest
    generate_stub_files(manifest_file, stub_dir)

    return stub_dir


@pytest.fixture(scope="session")
def pi_uxarray_data(request, pi_uxarray_real_data, pi_uxarray_stub_data):
    """
    Router fixture that returns either real or stub data based on:
    1. The PYCMOR_USE_STUB_DATA environment variable
    2. The use_stub_data pytest marker
    """
    # Check for environment variable
    use_stub = os.environ.get("PYCMOR_USE_STUB_DATA", "").lower() in ("1", "true", "yes")

    # Check for pytest marker
    if hasattr(request, "node") and request.node.get_closest_marker("use_stub_data"):
        use_stub = True

    if use_stub:
        print("Using STUB data for pi_uxarray")
        return pi_uxarray_stub_data
    else:
        print("Using REAL data for pi_uxarray")
        return pi_uxarray_real_data


@pytest.fixture(scope="session")
def pi_uxarray_download_mesh(tmp_path_factory):
    # Use persistent cache in $HOME/.cache/pycmor instead of ephemeral /tmp
    cache_dir = Path.home() / ".cache" / "pycmor" / "test_data"
    cache_dir.mkdir(parents=True, exist_ok=True)
    data_path = cache_dir / "pi_mesh.tar"

    if not data_path.exists():
        print(f"Downloading mesh data from {MESH_URL}...")
        try:
            response = requests.get(MESH_URL, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            error_msg = (
                f"Failed to download mesh data from {MESH_URL}\n"
                f"Error type: {type(e).__name__}\n"
                f"Error details: {str(e)}\n"
            )
            if hasattr(e, "response") and e.response is not None:
                error_msg += (
                    f"HTTP Status Code: {e.response.status_code}\n"
                    f"Response Headers: {dict(e.response.headers)}\n"
                    f"Response Content (first 500 chars): {e.response.text[:500]}\n"
                )
            print(error_msg)
            raise RuntimeError(error_msg) from e

        with open(data_path, "wb") as f:
            f.write(response.content)
        print(f"Data downloaded: {data_path}.")
    else:
        print(f"Using cached data: {data_path}.")

    return data_path


@pytest.fixture(scope="session")
def pi_uxarray_real_mesh(pi_uxarray_download_mesh):
    data_dir = Path(pi_uxarray_download_mesh).parent
    with tarfile.open(pi_uxarray_download_mesh, "r") as tar:
        tar.extractall(data_dir)

    return data_dir / "pi"


@pytest.fixture(scope="session")
def pi_uxarray_stub_mesh(tmp_path_factory):
    """
    Generate stub mesh for pi_uxarray from YAML manifest.
    Returns the mesh directory containing fesom.mesh.diag.nc.
    """
    # Create temporary directory for stub mesh
    stub_dir = tmp_path_factory.mktemp("pi_uxarray_stub_mesh")

    # Path to the YAML manifest
    manifest_file = Path(__file__).parent.parent / "stub_data" / "pi_uxarray.yaml"

    # Generate stub files from manifest
    # Note: This generates all files from the manifest, including the mesh file
    generate_stub_files(manifest_file, stub_dir)

    return stub_dir


@pytest.fixture(scope="session")
def pi_uxarray_mesh(request, pi_uxarray_real_mesh, pi_uxarray_stub_mesh):
    """
    Router fixture that returns either real or stub mesh based on:
    1. The PYCMOR_USE_STUB_DATA environment variable
    2. The use_stub_data pytest marker
    """
    # Check for environment variable
    use_stub = os.environ.get("PYCMOR_USE_STUB_DATA", "").lower() in ("1", "true", "yes")

    # Check for pytest marker
    if hasattr(request, "node") and request.node.get_closest_marker("use_stub_data"):
        use_stub = True

    if use_stub:
        print("Using STUB mesh for pi_uxarray")
        return pi_uxarray_stub_mesh
    else:
        print("Using REAL mesh for pi_uxarray")
        return pi_uxarray_real_mesh
