"""Example data for the FESOM model."""

import hashlib
import os
import tarfile
from pathlib import Path

import pytest
import requests

URL = "https://nextcloud.awi.de/s/DaQjtTS9xB7o7pL/download/awicm_1p0_recom.tar"
"""str : URL to download the example data from."""

# Expected SHA256 checksum of the tar file (update this when data changes)
# Set to None to skip validation
EXPECTED_SHA256 = None
"""str : Expected SHA256 checksum of the downloaded tar file."""


def verify_file_integrity(file_path, expected_sha256=None):
    """
    Verify file integrity using SHA256 checksum.

    Parameters
    ----------
    file_path : Path
        Path to the file to verify
    expected_sha256 : str, optional
        Expected SHA256 checksum. If None, verification is skipped.

    Returns
    -------
    bool
        True if file is valid, False otherwise
    """
    if expected_sha256 is None:
        return True

    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

    actual_sha256 = sha256_hash.hexdigest()
    is_valid = actual_sha256 == expected_sha256

    if not is_valid:
        print(f"Checksum mismatch for {file_path}")
        print(f"Expected: {expected_sha256}")
        print(f"Got:      {actual_sha256}")

    return is_valid


@pytest.fixture(scope="session")
def awicm_1p0_recom_download_data(tmp_path_factory):
    # Use persistent cache in $HOME/.cache/pycmor instead of ephemeral /tmp
    cache_dir = Path.home() / ".cache" / "pycmor" / "test_data"
    cache_dir.mkdir(parents=True, exist_ok=True)
    data_path = cache_dir / "awicm_1p0_recom.tar"

    # Check if cached file exists and is valid
    if data_path.exists():
        if verify_file_integrity(data_path, EXPECTED_SHA256):
            print(f"Using cached data: {data_path}.")
            return data_path
        else:
            print("Cached data is corrupted. Re-downloading...")
            data_path.unlink()

    # Download the file
    print(f"Downloading test data from {URL}...")
    response = requests.get(URL, stream=True)
    response.raise_for_status()

    # Download with progress indication
    total_size = int(response.headers.get("content-length", 0))
    with open(data_path, "wb") as f:
        if total_size == 0:
            f.write(response.content)
        else:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                downloaded += len(chunk)
                f.write(chunk)
                if downloaded % (1024 * 1024) == 0:  # Print every MB
                    print(f"Downloaded {downloaded / (1024 * 1024):.1f} MB / {total_size / (1024 * 1024):.1f} MB")

    print(f"Data downloaded: {data_path}.")

    # Verify the downloaded file
    if not verify_file_integrity(data_path, EXPECTED_SHA256):
        raise RuntimeError(f"Downloaded file {data_path} failed integrity check!")

    return data_path


@pytest.fixture(scope="session")
def awicm_1p0_recom_data(awicm_1p0_recom_download_data):
    import shutil

    data_dir = Path(awicm_1p0_recom_download_data).parent / "awicm_1p0_recom"
    final_data_path = data_dir / "awicm_1p0_recom"

    # Check if extraction already exists
    if data_dir.exists():
        # Verify one of the known problematic files exists and is valid
        test_file = (
            final_data_path / "awi-esm-1-1-lr_kh800" / "piControl" / "outdata" / "fesom" / "thetao_fesom_2686-01-05.nc"
        )
        if test_file.exists():
            try:
                # Try to open the file to verify it's not corrupted
                import h5py

                with h5py.File(test_file, "r"):
                    print(f"Using cached extraction: {data_dir}.")
                    print(f">>> RETURNING: {final_data_path}")
                    return final_data_path
            except (OSError, IOError) as e:
                print(f"Cached extraction is corrupted ({e}). Re-extracting...")
                shutil.rmtree(data_dir)

    # Extract the tar file
    print(f"Extracting test data to: {data_dir}...")
    data_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(awicm_1p0_recom_download_data, "r") as tar:
        tar.extractall(data_dir)
    print(f"Data extracted to: {data_dir}.")

    # List extracted files for debugging
    for root, dirs, files in os.walk(data_dir):
        print(f"Root: {root}")
        for file in files:
            print(f"File: {os.path.join(root, file)}")

    print(f">>> RETURNING: {final_data_path}")
    return final_data_path
