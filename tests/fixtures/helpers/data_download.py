"""Reusable utilities for downloading and caching test data.

This module provides clean, efficient helpers for downloading and extracting
test data archives with proper caching and error handling.

Test data is cached permanently in ~/.cache/pycmor/test-data/ to avoid
repeated downloads across test sessions.
"""

import logging
import os
import tarfile
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)


def get_cache_dir() -> Path:
    """Get the persistent cache directory for test data.

    Uses XDG_CACHE_HOME if set, otherwise defaults to ~/.cache/pycmor/test-data/

    Returns:
        Path to the cache directory
    """
    xdg_cache = os.environ.get("XDG_CACHE_HOME")
    if xdg_cache:
        cache_base = Path(xdg_cache)
    else:
        cache_base = Path.home() / ".cache"

    cache_dir = cache_base / "pycmor" / "test-data"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def download_file(url: str, destination: Path, chunk_size: int = 8192) -> Path:
    """Download a file with streaming to avoid loading entire file in memory.

    Args:
        url: URL to download from
        destination: Path where file should be saved
        chunk_size: Size of chunks to download (default: 8KB)

    Returns:
        Path to the downloaded file

    Raises:
        requests.HTTPError: If download fails
    """
    destination.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Downloading from {url}")
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(destination, "wb") as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)

    logger.info(f"Download complete: {destination}")
    return destination


def extract_tarfile(
    archive_path: Path, extract_dir: Path, expected_subdir: Optional[str] = None
) -> Path:
    """Extract a tar archive to a directory.

    Args:
        archive_path: Path to the tar archive
        extract_dir: Directory to extract into
        expected_subdir: If provided, returns extract_dir / expected_subdir
                        instead of extract_dir

    Returns:
        Path to the extracted data (either extract_dir or extract_dir/expected_subdir)
    """
    extract_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Extracting {archive_path.name} to {extract_dir}")
    with tarfile.open(archive_path, "r") as tar:
        tar.extractall(extract_dir)

    result_path = extract_dir / expected_subdir if expected_subdir else extract_dir
    logger.info(f"Extraction complete: {result_path}")
    return result_path


def download_and_extract_fixture(
    tmp_path_factory,
    url: str,
    archive_name: str,
    extracted_subdir: Optional[str] = None,
) -> Path:
    """Download and extract test data with permanent caching.

    This is the main function to use in pytest fixtures. It handles:
    - Permanent caching in ~/.cache/pycmor/test-data/ (persists across sessions)
    - Won't re-download if archive exists
    - Won't re-extract if data directory exists
    - Proper error handling and logging

    Args:
        tmp_path_factory: pytest's tmp_path_factory fixture (not used, kept for API compatibility)
        url: URL to download the archive from
        archive_name: Name for the downloaded archive (e.g., "data.tar")
        extracted_subdir: If the archive contains a subdirectory with this name,
                         return that path instead of the extraction root

    Returns:
        Path to the extracted data directory

    Example:
        @pytest.fixture(scope="session")
        def my_test_data(tmp_path_factory):
            return download_and_extract_fixture(
                tmp_path_factory,
                url="https://example.com/data.tar",
                archive_name="my_data.tar",
                extracted_subdir="my_data"
            )
    """
    cache_dir = get_cache_dir()

    # Download archive if not cached
    archive_path = cache_dir / archive_name
    if not archive_path.exists():
        download_file(url, archive_path)
    else:
        logger.info(f"Using cached archive: {archive_path}")

    # Determine extraction directory and final result path
    extract_base = archive_path.with_suffix("")  # Remove .tar extension
    if extract_base.suffix in [".tar", ".tgz", ".gz"]:
        extract_base = extract_base.with_suffix("")  # Remove .tar from .tar.gz

    result_path = extract_base / extracted_subdir if extracted_subdir else extract_base

    # Extract if not already extracted
    if not result_path.exists():
        extract_tarfile(archive_path, extract_base, extracted_subdir)
    else:
        logger.info(f"Using cached extraction: {result_path}")

    return result_path


def create_download_fixture(
    url: str, archive_name: str, extracted_subdir: Optional[str] = None
):
    """Create a pytest fixture function for downloading and extracting test data.

    This is a factory function that creates properly configured pytest fixtures.

    Args:
        url: URL to download the archive from
        archive_name: Name for the downloaded archive
        extracted_subdir: Subdirectory within the archive to return

    Returns:
        A pytest fixture function

    Example:
        # In your fixture file:
        my_test_data = create_download_fixture(
            url="https://example.com/data.tar",
            archive_name="my_data.tar",
            extracted_subdir="my_data"
        )
    """

    def fixture_func(tmp_path_factory):
        return download_and_extract_fixture(
            tmp_path_factory, url, archive_name, extracted_subdir
        )

    return fixture_func
