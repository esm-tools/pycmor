"""
Resource loader with priority-based loading:
1. User-specified location
2. XDG cache
3. Remote git (with caching)
4. Packaged resources (importlib.resources)
5. Vendored git submodules
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional, Union

# Use importlib.resources for Python 3.9+, fallback to importlib_resources
if sys.version_info >= (3, 9):
    from importlib import resources
else:
    import importlib_resources as resources  # noqa: F401

from pycmor.core.logging import logger


class ResourceLoader:
    """
    Base class for loading resources with priority-based fallback.

    Priority order:
    1. User-specified path (highest priority)
    2. XDG cache directory
    3. Remote git repository (downloads to cache)
    4. Packaged/vendored data (lowest priority)

    Parameters
    ----------
    resource_name : str
        Name of the resource (e.g., 'cmip6-cvs', 'cmip7-cvs')
    version : str, optional
        Version identifier (e.g., '6.2.58.64', 'v1.2.2.2')
    user_path : str or Path, optional
        User-specified path to resource
    """

    def __init__(
        self,
        resource_name: str,
        version: Optional[str] = None,
        user_path: Optional[Union[str, Path]] = None,
    ):
        self.resource_name = resource_name
        self.version = version
        self.user_path = Path(user_path) if user_path else None
        self._cache_base = self._get_cache_directory()

    @staticmethod
    def _get_cache_directory() -> Path:
        """
        Get the XDG cache directory for pycmor.

        Returns
        -------
        Path
            Path to cache directory (~/.cache/pycmor or $XDG_CACHE_HOME/pycmor)
        """
        xdg_cache = os.environ.get("XDG_CACHE_HOME")
        if xdg_cache:
            cache_base = Path(xdg_cache)
        else:
            cache_base = Path.home() / ".cache"

        pycmor_cache = cache_base / "pycmor"
        pycmor_cache.mkdir(parents=True, exist_ok=True)
        return pycmor_cache

    def _get_cache_path(self) -> Path:
        """
        Get the cache path for this specific resource and version.

        Returns
        -------
        Path
            Path to cached resource directory
        """
        if self.version:
            cache_path = self._cache_base / self.resource_name / self.version
        else:
            cache_path = self._cache_base / self.resource_name
        return cache_path

    def _get_packaged_path(self) -> Optional[Path]:
        """
        Get the path to packaged resources (via importlib.resources).

        This should be overridden by subclasses to point to their
        specific packaged data location within src/pycmor/data/.

        Returns
        -------
        Path or None
            Path to packaged data, or None if not available
        """
        return None  # Override in subclasses if packaged data exists

    def _get_vendored_path(self) -> Optional[Path]:
        """
        Get the path to vendored git submodule data.

        This should be overridden by subclasses to point to their
        specific vendored data location (git submodules).

        Returns
        -------
        Path or None
            Path to vendored data, or None if not available
        """
        raise NotImplementedError("Subclasses must implement _get_vendored_path")

    def _download_from_git(self, cache_path: Path) -> bool:
        """
        Download resource from git repository to cache.

        This should be overridden by subclasses to implement their
        specific git download logic.

        Parameters
        ----------
        cache_path : Path
            Where to download the resource

        Returns
        -------
        bool
            True if download succeeded, False otherwise
        """
        raise NotImplementedError("Subclasses must implement _download_from_git")

    def load(self) -> Optional[Path]:
        """
        Load resource following 5-level priority chain.

        Returns
        -------
        Path or None
            Path to the resource, or None if not found
        """
        # Priority 1: User-specified path
        if self.user_path:
            if self.user_path.exists():
                logger.info(f"Using user-specified {self.resource_name}: {self.user_path}")
                return self.user_path
            else:
                logger.warning(
                    f"User-specified {self.resource_name} not found: {self.user_path}. "
                    "Falling back to cache/remote/packaged/vendored."
                )

        # Priority 2: XDG cache
        cache_path = self._get_cache_path()
        if cache_path.exists() and self._validate_cache(cache_path):
            logger.info(f"Using cached {self.resource_name}: {cache_path}")
            return cache_path

        # Priority 3: Remote git (download to cache)
        logger.info(f"Attempting to download {self.resource_name} from git...")
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        if self._download_from_git(cache_path):
            logger.info(f"Downloaded {self.resource_name} to cache: {cache_path}")
            return cache_path
        else:
            logger.warning(f"Failed to download {self.resource_name} from git")

        # Priority 4: Packaged resources (importlib.resources)
        packaged_path = self._get_packaged_path()
        if packaged_path and packaged_path.exists():
            logger.info(f"Using packaged {self.resource_name}: {packaged_path}")
            return packaged_path

        # Priority 5: Vendored git submodules (dev installs only)
        vendored_path = self._get_vendored_path()
        if vendored_path and vendored_path.exists():
            logger.info(f"Using vendored {self.resource_name}: {vendored_path}")
            return vendored_path

        logger.error(
            f"Could not load {self.resource_name} from any source. "
            "Tried: user path, cache, remote git, packaged resources, vendored submodules."
        )
        return None

    def _validate_cache(self, cache_path: Path) -> bool:
        """
        Validate that cached resource is valid.

        Can be overridden by subclasses for specific validation logic.

        Parameters
        ----------
        cache_path : Path
            Path to cached resource

        Returns
        -------
        bool
            True if cache is valid, False otherwise
        """
        # Basic validation: just check if path exists and is not empty
        if not cache_path.exists():
            return False

        # Check if directory has content
        if cache_path.is_dir():
            return any(cache_path.iterdir())

        # Check if file is not empty
        return cache_path.stat().st_size > 0


class CVLoader(ResourceLoader):
    """
    Loader for Controlled Vocabularies (CMIP6 or CMIP7).

    Parameters
    ----------
    cmor_version : str
        Either 'CMIP6' or 'CMIP7'
    version : str, optional
        CV version (e.g., '6.2.58.64' for CMIP6)
    user_path : str or Path, optional
        User-specified CV_Dir
    """

    def __init__(
        self,
        cmor_version: str,
        version: Optional[str] = None,
        user_path: Optional[Union[str, Path]] = None,
    ):
        self.cmor_version = cmor_version

        # Set resource name based on CMOR version
        if cmor_version == "CMIP6":
            resource_name = "cmip6-cvs"
            if version is None:
                version = "6.2.58.64"  # Default CMIP6 CV version
        elif cmor_version == "CMIP7":
            resource_name = "cmip7-cvs"
            # CMIP7 uses git branches/tags differently
        else:
            raise ValueError(f"Unknown CMOR version: {cmor_version}")

        super().__init__(resource_name, version, user_path)

    def _get_vendored_path(self) -> Optional[Path]:
        """Get path to vendored CV submodule."""
        # Get repo root (assuming we're in src/pycmor/core/)
        current_file = Path(__file__)
        repo_root = current_file.parent.parent.parent.parent

        if self.cmor_version == "CMIP6":
            cv_path = repo_root / "cmip6-cmor-tables" / "CMIP6_CVs"
        else:  # CMIP7
            cv_path = repo_root / "CMIP7-CVs"

        if not cv_path.exists():
            logger.warning(
                f"{self.cmor_version} CVs submodule not found at {cv_path}. " "Run: git submodule update --init"
            )
            return None

        return cv_path

    def _download_from_git(self, cache_path: Path) -> bool:
        """Download CVs from GitHub."""
        if self.cmor_version == "CMIP6":
            repo_url = "https://github.com/WCRP-CMIP/CMIP6_CVs.git"
            tag = self.version  # e.g., "6.2.58.64"
        else:  # CMIP7
            repo_url = "https://github.com/WCRP-CMIP/CMIP7-CVs.git"
            tag = self.version if self.version else "src-data"  # Default branch

        try:
            # Clone with depth 1 for speed, checkout specific tag/branch
            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir_path = Path(tmpdir)

                # Clone
                subprocess.run(
                    ["git", "clone", "--depth", "1", "--branch", tag, repo_url, str(tmpdir_path)],
                    check=True,
                    capture_output=True,
                )

                # Copy to cache (exclude .git directory)
                shutil.copytree(
                    tmpdir_path,
                    cache_path,
                    ignore=shutil.ignore_patterns(".git"),
                )

            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to clone {self.cmor_version} CVs: {e.stderr.decode()}")
            return False
        except Exception as e:
            logger.error(f"Error downloading {self.cmor_version} CVs: {e}")
            return False


class CMIP7MetadataLoader(ResourceLoader):
    """
    Loader for CMIP7 Data Request metadata.

    Parameters
    ----------
    version : str
        DReq version (e.g., 'v1.2.2.2')
    user_path : str or Path, optional
        User-specified CMIP7_DReq_metadata path
    """

    def __init__(
        self,
        version: str = "v1.2.2.2",
        user_path: Optional[Union[str, Path]] = None,
    ):
        super().__init__("cmip7_metadata", version, user_path)

    def _get_vendored_path(self) -> Optional[Path]:
        """CMIP7 metadata is not vendored, must be generated."""
        return None

    def _download_from_git(self, cache_path: Path) -> bool:
        """
        Generate CMIP7 metadata using export_dreq_lists_json command.

        This isn't really "downloading from git" but rather generating
        the metadata file using the installed command-line tool.
        """
        try:
            # Ensure parent directory exists
            cache_path.parent.mkdir(parents=True, exist_ok=True)

            # Generate metadata file
            experiments_file = cache_path.parent / f"{self.version}_experiments.json"
            metadata_file = cache_path  # This is what we actually want

            logger.info(f"Generating CMIP7 metadata for {self.version}...")
            subprocess.run(
                [
                    "export_dreq_lists_json",
                    "-a",
                    self.version,
                    str(experiments_file),
                    "-m",
                    str(metadata_file),
                ],
                check=True,
                capture_output=True,
                text=True,
            )

            # Clean up experiments file (we don't need it)
            if experiments_file.exists():
                experiments_file.unlink()

            return metadata_file.exists()

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to generate CMIP7 metadata: {e.stderr}")
            return False
        except FileNotFoundError:
            logger.error(
                "export_dreq_lists_json command not found. "
                "Install with: pip install git+https://github.com/WCRP-CMIP/CMIP7_DReq_Software"
            )
            return False
        except Exception as e:
            logger.error(f"Error generating CMIP7 metadata: {e}")
            return False

    def _validate_cache(self, cache_path: Path) -> bool:
        """Validate that cached metadata file is valid JSON."""
        if not super()._validate_cache(cache_path):
            return False

        # Additional validation: check it's valid JSON with expected structure
        try:
            with open(cache_path, "r") as f:
                data = json.load(f)
                # Check for expected structure
                return "Compound Name" in data or "Header" in data
        except (json.JSONDecodeError, KeyError):
            logger.warning(f"Cached metadata file is corrupted: {cache_path}")
            return False
