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
    cache_dir = tmp_path_factory.getbasetemp() / "cached_data"
    cache_dir.mkdir(exist_ok=True)
    data_path = cache_dir / "awicm_1p0_recom.tar"

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
def awicm_1p0_recom_data(awicm_1p0_recom_download_data):
    cache_dir = Path(awicm_1p0_recom_download_data).parent
    data_dir = cache_dir / "awicm_1p0_recom"

    if not data_dir.exists():
        print("Extracting tarball...")
        with tarfile.open(awicm_1p0_recom_download_data, "r") as tar:
            # Extract to cache_dir - tarball should contain awicm_1p0_recom/ as root
            tar.extractall(cache_dir)
        print(f"Data extracted to: {data_dir}.")
    else:
        print(f"Using cached extraction: {data_dir}.")

    for root, dirs, files in os.walk(data_dir):
        print(f"Root: {root}")
        for file in files:
            print(f"File: {os.path.join(root, file)}")

    print(f">>> RETURNING: {data_dir}")
    return data_dir
