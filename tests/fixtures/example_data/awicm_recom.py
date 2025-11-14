"""Example data for the FESOM model."""

import hashlib
import os
import tarfile
from pathlib import Path

import pytest
import requests

from tests.fixtures.stub_generator import generate_stub_files

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
    try:
        response = requests.get(URL, stream=True, timeout=30)
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
def awicm_1p0_recom_real_data(awicm_1p0_recom_download_data):
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


@pytest.fixture(scope="session")
def awicm_1p0_recom_stub_data(tmp_path_factory):
    """Generate stub data from YAML manifest."""
    manifest_file = Path(__file__).parent.parent / "stub_data" / "awicm_1p0_recom.yaml"
    output_dir = tmp_path_factory.mktemp("awicm_1p0_recom")

    # Generate stub files
    stub_dir = generate_stub_files(manifest_file, output_dir)

    # Create mesh files (always generate them even if not all tests need them)
    mesh_dir = stub_dir / "awi-esm-1-1-lr_kh800" / "piControl" / "input" / "fesom" / "mesh"
    mesh_dir.mkdir(parents=True, exist_ok=True)
    _create_minimal_mesh_files(mesh_dir)

    # Return the equivalent path structure that real data returns
    # (should match what awicm_1p0_recom_real_data returns)
    # The stub_dir contains awi-esm-1-1-lr_kh800/piControl/... structure
    return stub_dir


def _create_minimal_mesh_files(mesh_dir: Path):
    """Create minimal FESOM mesh files for testing."""
    # nod2d.out: 2D nodes (lon, lat)
    with open(mesh_dir / "nod2d.out", "w") as f:
        f.write("10\n")
        for i in range(1, 11):
            lon = 300.0 + i * 0.1
            lat = 74.0 + i * 0.05
            f.write(f"{i:8d} {lon:14.7f}  {lat:14.7f}        0\n")

    # elem2d.out: 2D element connectivity
    with open(mesh_dir / "elem2d.out", "w") as f:
        f.write("5\n")
        for i in range(1, 6):
            n1, n2, n3 = i, i + 1, i + 2
            f.write(f"{i:8d} {n1:8d} {n2:8d}\n")
            f.write(f"{n2:8d} {n3:8d} {(i % 8) + 1:8d}\n")

    # nod3d.out: 3D nodes (lon, lat, depth)
    with open(mesh_dir / "nod3d.out", "w") as f:
        f.write("30\n")
        for i in range(1, 31):
            lon = 300.0 + (i % 10) * 0.1
            lat = 74.0 + (i % 10) * 0.05
            depth = -100.0 * (i // 10)
            f.write(f"{i:8d} {lon:14.7f}  {lat:14.7f} {depth:14.7f}        0\n")

    # elem3d.out: 3D element connectivity (tetrahedra)
    with open(mesh_dir / "elem3d.out", "w") as f:
        f.write("10\n")  # 10 3D elements
        for i in range(1, 11):
            n1, n2, n3, n4 = i, i + 1, i + 2, i + 10
            f.write(f"{n1:8d} {n2:8d} {n3:8d} {n4:8d}\n")

    # aux3d.out: auxiliary 3D info (layer indices)
    # Format: num_layers \n layer_start_indices...
    with open(mesh_dir / "aux3d.out", "w") as f:
        f.write("3\n")  # 3 vertical layers
        f.write("       1\n")  # Layer 1 starts at node 1
        f.write("      11\n")  # Layer 2 starts at node 11
        f.write("      21\n")  # Layer 3 starts at node 21

    # depth.out: depth values at each node
    with open(mesh_dir / "depth.out", "w") as f:
        for i in range(10):
            f.write(f"   {-100.0 - i * 50:.1f}\n")


@pytest.fixture(scope="session")
def awicm_1p0_recom_data(request):
    """Router fixture: return stub or real data based on marker/env var."""
    # Check for environment variable
    use_real = os.getenv("PYCMOR_USE_REAL_TEST_DATA", "").lower() in ("1", "true", "yes")

    # Check for pytest marker
    if hasattr(request, "node") and request.node.get_closest_marker("real_data"):
        use_real = True

    if use_real:
        print("Using real downloaded test data")
        # Request real data fixture lazily
        return request.getfixturevalue("awicm_1p0_recom_real_data")
    else:
        print("Using stub test data")
        # Request stub data fixture lazily
        return request.getfixturevalue("awicm_1p0_recom_stub_data")
