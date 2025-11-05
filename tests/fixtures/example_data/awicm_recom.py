"""Example data for the FESOM model."""

import os
import tarfile
from pathlib import Path

import pytest
import requests

URL = "https://nextcloud.awi.de/s/DaQjtTS9xB7o7pL/download/awicm_1p0_recom.tar"
"""str : URL to download the example data from."""


@pytest.fixture(scope="session")
def awicm_1p0_recom_download_data(tmp_path_factory):
    # Use persistent cache in $HOME/.cache/pycmor instead of ephemeral /tmp
    cache_dir = Path.home() / ".cache" / "pycmor" / "test_data"
    cache_dir.mkdir(parents=True, exist_ok=True)
    data_path = cache_dir / "awicm_1p0_recom.tar"

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
def awicm_1p0_recom_data(awicm_1p0_recom_download_data):
    data_dir = Path(awicm_1p0_recom_download_data).parent / "awicm_1p0_recom"
    if not data_dir.exists():
        with tarfile.open(awicm_1p0_recom_download_data, "r") as tar:
            tar.extractall(data_dir)
        print(f"Data extracted to: {data_dir}.")
    else:
        print(f"Using cached extraction: {data_dir}.")

    for root, dirs, files in os.walk(data_dir):
        print(f"Root: {root}")
        for file in files:
            print(f"File: {os.path.join(root, file)}")

    print(f">>> RETURNING: {data_dir / 'awicm_1p0_recom' }")
    return data_dir / "awicm_1p0_recom"
