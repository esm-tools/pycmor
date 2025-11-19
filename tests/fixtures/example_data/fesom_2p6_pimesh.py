"""Example data for the FESOM model."""

import os
import tarfile
from pathlib import Path

import pytest
import requests

from tests.fixtures.stub_generator import generate_stub_files

URL = "https://nextcloud.awi.de/s/AL2cFQx5xGE473S/download/fesom_2p6_pimesh.tar"
"""str : URL to download the example data from."""

PYCMOR_TEST_DATA_CACHE_DIR = Path(
    os.getenv("PYCMOR_TEST_DATA_CACHE_DIR")
    or Path(os.getenv("XDG_CACHE_HOME") or Path.home() / ".cache") / "pycmor" / "test_data"
)


@pytest.fixture(scope="session")
def fesom_2p6_esm_tools_download_data(tmp_path_factory):
    # Use persistent cache in $HOME/.cache/pycmor instead of ephemeral /tmp
    cache_dir = PYCMOR_TEST_DATA_CACHE_DIR
    cache_dir.mkdir(parents=True, exist_ok=True)
    data_path = cache_dir / "fesom_2p6_pimesh.tar"

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
def fesom_2p6_pimesh_esm_tools_real_data(fesom_2p6_esm_tools_download_data):
    data_dir = Path(fesom_2p6_esm_tools_download_data).parent / "fesom_2p6_pimesh"
    if not data_dir.exists():
        with tarfile.open(fesom_2p6_esm_tools_download_data, "r") as tar:
            tar.extractall(data_dir)
        print(f"Data extracted to: {data_dir}.")
    else:
        print(f"Using cached extraction: {data_dir}.")

    print(f">>> RETURNING: {data_dir / 'fesom_2p6_pimesh' }")
    return data_dir / "fesom_2p6_pimesh"


@pytest.fixture(scope="session")
def fesom_2p6_pimesh_esm_tools_stub_data(tmp_path_factory):
    """Generate stub data from YAML manifest."""
    manifest_file = Path(__file__).parent.parent / "stub_data" / "fesom_2p6_pimesh.yaml"
    output_dir = tmp_path_factory.mktemp("fesom_2p6_pimesh")

    # Generate stub files
    stub_dir = generate_stub_files(manifest_file, output_dir)

    # Create mesh files (always generate them even if not all tests need them)
    mesh_dir = stub_dir / "input" / "fesom" / "mesh" / "pi"
    mesh_dir.mkdir(parents=True, exist_ok=True)
    _create_minimal_mesh_files(mesh_dir)

    # Return the equivalent path structure that real data returns
    # (should match what fesom_2p6_pimesh_esm_tools_real_data returns)
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
def fesom_2p6_pimesh_esm_tools_data(request):
    """Router fixture: return stub or real data based on marker/env var."""
    # Check for environment variable
    use_real = os.getenv("PYCMOR_USE_REAL_TEST_DATA", "").lower() in ("1", "true", "yes")

    # Check for pytest marker
    if hasattr(request, "node") and request.node.get_closest_marker("real_data"):
        use_real = True

    if use_real:
        print("Using real downloaded test data")
        return request.getfixturevalue("fesom_2p6_pimesh_esm_tools_real_data")
    else:
        print("Using stub test data")
        return request.getfixturevalue("fesom_2p6_pimesh_esm_tools_stub_data")
