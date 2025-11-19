"""Example data for the FESOM model."""

import os
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest
import requests

from tests.fixtures.stub_generator import generate_stub_files

URL = "https://nextcloud.awi.de/s/swqyFgbL2jjgjRo/download/pi_uxarray.tar"
"""str : URL to download the example data from."""

MESH_GIT_REPO = "https://gitlab.awi.de/fesom/pi"
"""str : Git repository URL for the FESOM PI mesh data."""

PYCMOR_TEST_DATA_CACHE_DIR = Path(
    os.getenv("PYCMOR_TEST_DATA_CACHE_DIR")
    or Path(os.getenv("XDG_CACHE_HOME") or Path.home() / ".cache") / "pycmor" / "test_data"
)


@pytest.fixture(scope="session")
def pi_uxarray_download_data(tmp_path_factory):
    # Use persistent cache in $HOME/.cache/pycmor instead of ephemeral /tmp
    cache_dir = PYCMOR_TEST_DATA_CACHE_DIR
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
def pi_uxarray_data(request):
    """
    Router fixture that returns stub data by default, or real data if:
    1. The PYCMOR_USE_REAL_TEST_DATA environment variable is set
    2. The real_data pytest marker is present
    """
    # Check for environment variable
    use_real = os.getenv("PYCMOR_USE_REAL_TEST_DATA", "").lower() in ("1", "true", "yes")

    # Check for pytest marker
    if hasattr(request, "node") and request.node.get_closest_marker("real_data"):
        use_real = True

    if use_real:
        print("Using REAL data for pi_uxarray")
        return request.getfixturevalue("pi_uxarray_real_data")
    else:
        print("Using STUB data for pi_uxarray")
        return request.getfixturevalue("pi_uxarray_stub_data")


@pytest.fixture(scope="session")
def pi_uxarray_download_mesh(tmp_path_factory):
    """
    Clone FESOM PI mesh from GitLab using git-lfs.
    Uses persistent cache in $HOME/.cache/pycmor instead of ephemeral /tmp.
    """
    # Use persistent cache in $HOME/.cache/pycmor instead of ephemeral /tmp
    cache_dir = PYCMOR_TEST_DATA_CACHE_DIR
    cache_dir.mkdir(parents=True, exist_ok=True)
    mesh_dir = cache_dir / "pi_mesh_git"

    if mesh_dir.exists() and (mesh_dir / ".git").exists():
        print(f"Using cached git mesh repository: {mesh_dir}")
        return mesh_dir

    # Clone the repository with git-lfs
    print(f"Cloning FESOM PI mesh from {MESH_GIT_REPO}...")
    try:
        # Check if git-lfs is available
        result = subprocess.run(["git", "lfs", "version"], capture_output=True, text=True, timeout=10, check=False)
        if result.returncode != 0:
            raise RuntimeError(
                "git-lfs is not installed. Please install git-lfs to download mesh data.\n"
                "See: https://git-lfs.github.com/"
            )

        # Remove directory if it exists but is incomplete
        if mesh_dir.exists():
            shutil.rmtree(mesh_dir)

        # Clone with git-lfs
        result = subprocess.run(
            ["git", "clone", MESH_GIT_REPO, str(mesh_dir)],
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )
        if result.returncode != 0:
            error_msg = (
                f"Failed to clone mesh repository from {MESH_GIT_REPO}\n"
                f"Git error: {result.stderr}\n"
                f"Git output: {result.stdout}\n"
            )
            print(error_msg)
            raise RuntimeError(error_msg)

        print(f"Mesh repository cloned to: {mesh_dir}")
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"Git clone timed out after {e.timeout} seconds") from e
    except FileNotFoundError as e:
        raise RuntimeError("git command not found. Please install git.") from e

    return mesh_dir


@pytest.fixture(scope="session")
def pi_uxarray_real_mesh(pi_uxarray_download_mesh):
    """Return the cloned git repository directory containing FESOM PI mesh files."""
    return pi_uxarray_download_mesh


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

    # Create mesh files directly in stub_dir (not in a subdirectory)
    _create_minimal_mesh_files(stub_dir)

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
def pi_uxarray_mesh(request):
    """
    Router fixture that returns stub mesh by default, or real mesh if:
    1. The PYCMOR_USE_REAL_TEST_DATA environment variable is set
    2. The real_data pytest marker is present
    """
    # Check for environment variable
    use_real = os.getenv("PYCMOR_USE_REAL_TEST_DATA", "").lower() in ("1", "true", "yes")

    # Check for pytest marker
    if hasattr(request, "node") and request.node.get_closest_marker("real_data"):
        use_real = True

    if use_real:
        print("Using REAL mesh for pi_uxarray")
        return request.getfixturevalue("pi_uxarray_real_mesh")
    else:
        print("Using STUB mesh for pi_uxarray")
        return request.getfixturevalue("pi_uxarray_stub_mesh")
