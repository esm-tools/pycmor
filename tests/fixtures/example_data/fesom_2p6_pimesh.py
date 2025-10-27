"""Example data for the FESOM model."""

import shutil
import tarfile
from pathlib import Path

import pytest
import requests

URL = "https://nextcloud.awi.de/s/AL2cFQx5xGE473S/download/fesom_2p6_pimesh.tar"
"""str : URL to download the example data from."""


@pytest.fixture(scope="session")
def fesom_2p6_esm_tools_download_data(tmp_path_factory):
    cache_dir = tmp_path_factory.getbasetemp() / "cached_data"
    cache_dir.mkdir(exist_ok=True)
    data_path = cache_dir / "fesom_2p6_pimesh.tar"

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
def fesom_2p6_pimesh_esm_tools_data(fesom_2p6_esm_tools_download_data):
    I_need_to_make_a_local_copy = True
    # Check if you have a local copy
    # Useful for testing on your local laptop
    local_cache_path = Path("~/.cache/pytest/github.com/esm-tools/pycmor").expanduser()
    local_cache_path = local_cache_path / "fesom_2p6_pimesh"
    if local_cache_path.exists():
        I_need_to_make_a_local_copy = False
        print(f"Using local cache: {local_cache_path}")
        return local_cache_path
    cache_dir = Path(fesom_2p6_esm_tools_download_data).parent
    data_dir = cache_dir / "fesom_2p6_pimesh"

    if not data_dir.exists():
        print("Extracting tarball...")
        with tarfile.open(fesom_2p6_esm_tools_download_data, "r") as tar:
            # Extract to cache_dir - tarball should contain fesom_2p6_pimesh/ as root
            tar.extractall(cache_dir)

        # Verify extraction - check that expected files exist
        expected_data = data_dir / "outdata" / "fesom"
        if not expected_data.exists():
            raise RuntimeError(
                f"Extraction failed: expected directory not found at {expected_data}"
            )

        # Check that NetCDF files exist and are non-empty
        nc_files = list(expected_data.glob("temp.fesom.*.nc"))
        if len(nc_files) == 0:
            raise RuntimeError(f"No NetCDF files found in {expected_data}")

        for nc_file in nc_files:
            size = nc_file.stat().st_size
            if size < 1000:  # NetCDF files should be at least 1KB
                raise RuntimeError(
                    f"NetCDF file {nc_file} appears corrupted ({size} bytes)"
                )

        print(f"Data extracted to: {data_dir}. Verified {len(nc_files)} NetCDF files.")
    else:
        print(f"Using cached extraction: {data_dir}.")

    if I_need_to_make_a_local_copy:
        local_cache_path.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copytree(
                data_dir,
                local_cache_path,
                dirs_exist_ok=True,
                ignore_dangling_symlinks=True,
            )
            print(f"Local cache created: {local_cache_path}")
        except Exception as e:
            print(f"Failed to create local cache: {e}")
            shutil.rmtree(local_cache_path, ignore_errors=True)

    print(f">>> RETURNING: {data_dir}")
    return data_dir
