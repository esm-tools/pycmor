"""
Centralized test data fetcher using pooch.

This module provides utilities to download and cache test data files
defined in test_data_registry.yaml.
"""

import logging
import os
import time
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

# Waits between download attempts. The data lives on one Nextcloud server, and
# CI starts dozens of jobs at once, which it answers with 503s and dropped connections.
RETRY_WAITS_SEC = (30, 60, 120)


def get_cache_dir() -> Path:
    """Get the cache directory for test data."""
    return Path(
        os.getenv("PYCMOR_TEST_DATA_CACHE_DIR")
        or Path(os.getenv("XDG_CACHE_HOME") or Path.home() / ".cache") / "pycmor" / "test_data"
    )


def _retrieve(url, known_hash, path, fname):
    """``pooch.retrieve``, retrying when the server fails."""
    import pooch
    import requests

    for wait in RETRY_WAITS_SEC:
        try:
            return pooch.retrieve(url=url, known_hash=known_hash, path=path, fname=fname)
        except requests.exceptions.RequestException as exc:
            logger.warning(f"Download of {fname} failed, retrying in {wait}s: {exc}")
            time.sleep(wait)
    return pooch.retrieve(url=url, known_hash=known_hash, path=path, fname=fname)


def load_registry(registry_path=None):
    """Load the test data registry from YAML.

    Parameters
    ----------
    registry_path : Path or str, optional
        Path to a model-specific registry file. If not provided, uses the
        default central registry at test_data_registry.yaml
    """
    if registry_path is None:
        registry_file = Path(__file__).parent / "test_data_registry.yaml"
    else:
        registry_file = Path(registry_path)

    with open(registry_file) as f:
        return yaml.safe_load(f)


def fetch_and_extract(filename: str, registry_path=None) -> Path:
    """
    Fetch and extract a test data tarball.

    Uses pooch to download and cache the file, then extracts it.
    The extracted directory is cached, so subsequent calls are fast.

    Parameters
    ----------
    filename : str
        Name of the file in the registry (e.g., "fesom_2p6_pimesh.tar")
    registry_path : Path or str, optional
        Path to a model-specific registry file. If not provided, uses the
        default central registry.

    Returns
    -------
    Path
        Path to the extracted directory

    Raises
    ------
    ValueError
        If filename not found in registry or URL is not set
    RuntimeError
        If download or extraction fails

    Examples
    --------
    >>> data_dir = fetch_and_extract("fesom_2p6_pimesh.tar")  # doctest: +SKIP
    >>> print(data_dir)  # doctest: +SKIP
    /home/user/.cache/pycmor/test_data/fesom_2p6_pimesh
    """
    registry = load_registry(registry_path)
    logger.info(f"Loaded registry from: {registry_path or 'default test_data_registry.yaml'}")

    if filename not in registry:
        raise ValueError(f"Unknown test data file: {filename}. " f"Available files: {list(registry.keys())}")

    entry = registry[filename]
    url = entry.get("url")
    checksum = entry.get("sha256")
    extract_dir = entry.get("extract_dir", filename.replace(".tar", ""))

    logger.info(f"Registry entry for '{filename}':")
    logger.info(f"  url: {url}")
    logger.info(f"  sha256: {checksum}")
    logger.info(f"  extract_dir: {extract_dir}")

    if url is None:
        raise ValueError(f"URL not set for {filename}. " f"Please update test_data_registry.yaml")

    cache_dir = get_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Cache directory: {cache_dir}")

    # Path where extracted data will be
    extracted_path = cache_dir / extract_dir
    logger.info(f"Expected extraction path: {extracted_path}")

    # If already extracted, return it
    if extracted_path.exists():
        logger.info(f"Using cached extraction: {extracted_path}")
        # List contents to verify
        contents = list(extracted_path.iterdir())
        logger.info(f"Cached directory contains {len(contents)} items:")
        for item in contents[:10]:  # Show first 10 items
            logger.info(f"  - {item.name} ({'dir' if item.is_dir() else 'file'})")
        if len(contents) > 10:
            logger.info(f"  ... and {len(contents) - 10} more items")
        return extracted_path

    # Download and extract
    logger.info(f"Downloading and extracting {filename}...")

    # Download the tarball with pooch (no extraction yet)
    downloaded_path = _retrieve(
        url=url,
        known_hash=f"sha256:{checksum}" if checksum else None,
        path=cache_dir,
        fname=filename,
    )

    # Extract manually into cache_dir directly -- tarballs already contain
    # the extract_dir as their top-level directory prefix.
    # Also handles absolute symlinks (Python 3.12+ rejects them with default filter).
    import sys
    import tarfile

    extract_kwargs = {}
    if sys.version_info >= (3, 12):
        extract_kwargs["filter"] = "tar"

    with tarfile.open(downloaded_path) as tar:
        tar.extractall(path=cache_dir, **extract_kwargs)

    logger.info(f"Data extracted to: {extracted_path}")
    return extracted_path


def fetch_tarball(filename: str, registry_path=None) -> Path:
    """
    Fetch a test data tarball without extracting.

    Parameters
    ----------
    filename : str
        Name of the file in the registry
    registry_path : Path or str, optional
        Path to a model-specific registry file. If not provided, uses the
        default central registry.

    Returns
    -------
    Path
        Path to the downloaded tarball

    Examples
    --------
    >>> tarball = fetch_tarball("fesom_2p6_pimesh.tar")  # doctest: +SKIP
    """
    registry = load_registry(registry_path)

    if filename not in registry:
        raise ValueError(f"Unknown test data file: {filename}")

    entry = registry[filename]
    url = entry.get("url")
    checksum = entry.get("sha256")

    if url is None:
        raise ValueError(f"URL not set for {filename}")

    cache_dir = get_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Download without processing
    downloaded_path = _retrieve(
        url=url,
        known_hash=f"sha256:{checksum}" if checksum else None,
        path=cache_dir,
        fname=filename,
    )

    return Path(downloaded_path)
