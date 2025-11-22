"""
Resource locator with priority-based resource location:
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
    from importlib.resources import files
else:
    import importlib_resources as resources  # noqa: F401
    from importlib_resources import files

from pycmor.core.factory import MetaFactory
from pycmor.core.logging import logger


class ResourceLocator:
    """
    Base class for locating resources with priority-based fallback.

    Priority order:
    1. User-specified path (highest priority)
    2. XDG cache directory
    3. Remote git repository (downloads to cache)
    4. Packaged resources (importlib.resources)
    5. Vendored git submodules (lowest priority)

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

    def locate(self) -> Optional[Path]:
        """
        Locate resource following 5-level priority chain.

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
            logger.debug(f"Using cached {self.resource_name}: {cache_path}")
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
            f"Could not locate {self.resource_name} from any source. "
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


class CVLocator(ResourceLocator, metaclass=MetaFactory):
    """
    Base class for Controlled Vocabularies locators.

    Subclasses should define:
    - DEFAULT_VERSION: Default version/tag/branch to use
    - RESOURCE_NAME: Name for cache directory
    - GIT_REPO_URL: GitHub repository URL
    - VENDORED_SUBDIR: Subdirectory path in repo for vendored submodule

    Parameters
    ----------
    version : str, optional
        CV version/tag/branch (uses DEFAULT_VERSION if not specified)
    user_path : str or Path, optional
        User-specified CV_Dir
    """

    DEFAULT_VERSION: str = None
    RESOURCE_NAME: str = None
    GIT_REPO_URL: str = None
    VENDORED_SUBDIR: str = None

    def __init__(
        self,
        version: Optional[str] = None,
        user_path: Optional[Union[str, Path]] = None,
    ):
        # Use class-level default version if not specified
        version = version or self.DEFAULT_VERSION
        super().__init__(self.RESOURCE_NAME, version, user_path)

    def _get_vendored_path(self) -> Optional[Path]:
        """Get path to vendored CV submodule."""
        # Get repo root (assuming we're in src/pycmor/core/)
        current_file = Path(__file__)
        repo_root = current_file.parent.parent.parent.parent

        cv_path = repo_root / self.VENDORED_SUBDIR

        if not cv_path.exists():
            logger.warning(
                f"{self.__class__.__name__} submodule not found at {cv_path}. " "Run: git submodule update --init"
            )
            return None

        return cv_path

    def _download_from_git(self, cache_path: Path) -> bool:
        """Download CVs from GitHub."""
        try:
            # Clone with depth 1 for speed, checkout specific tag/branch
            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir_path = Path(tmpdir)

                # Clone
                subprocess.run(
                    ["git", "clone", "--depth", "1", "--branch", self.version, self.GIT_REPO_URL, str(tmpdir_path)],
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
            logger.error(f"Failed to clone {self.__class__.__name__}: {e.stderr.decode()}")
            return False
        except Exception as e:
            logger.error(f"Error downloading {self.__class__.__name__}: {e}")
            return False


class CMIP6CVLocator(CVLocator):
    """Locator for CMIP6 Controlled Vocabularies."""

    DEFAULT_VERSION = "6.2.58.64"
    RESOURCE_NAME = "cmip6-cvs"
    GIT_REPO_URL = "https://github.com/WCRP-CMIP/CMIP6_CVs.git"
    VENDORED_SUBDIR = "cmip6-cmor-tables/CMIP6_CVs"


class CMIP7CVLocator(CVLocator):
    """Locator for CMIP7 Controlled Vocabularies."""

    DEFAULT_VERSION = "src-data"
    RESOURCE_NAME = "cmip7-cvs"
    GIT_REPO_URL = "https://github.com/WCRP-CMIP/CMIP7-CVs.git"
    VENDORED_SUBDIR = "CMIP7-CVs"


class TableLocator(ResourceLocator, metaclass=MetaFactory):
    """
    Base class for CMIP table locators.

    Subclasses should define:
    - DEFAULT_VERSION: Default version/tag/branch to use
    - RESOURCE_NAME: Name for cache directory
    - GIT_REPO_URL: GitHub repository URL (or None for packaged-only)
    - VENDORED_SUBDIR: Subdirectory path in repo for vendored submodule

    Parameters
    ----------
    version : str, optional
        Table version/tag/branch (uses DEFAULT_VERSION if not specified)
    user_path : str or Path, optional
        User-specified CMIP_Tables_Dir
    """

    DEFAULT_VERSION: str = None
    RESOURCE_NAME: str = None
    GIT_REPO_URL: str = None
    VENDORED_SUBDIR: str = None

    def __init__(
        self,
        version: Optional[str] = None,
        user_path: Optional[Union[str, Path]] = None,
    ):
        # Use class-level default version if not specified
        version = version or self.DEFAULT_VERSION
        super().__init__(self.RESOURCE_NAME, version, user_path)

    def _get_vendored_path(self) -> Optional[Path]:
        """Get path to vendored table submodule."""
        if self.VENDORED_SUBDIR is None:
            return None

        # Get repo root (assuming we're in src/pycmor/core/)
        current_file = Path(__file__)
        repo_root = current_file.parent.parent.parent.parent

        table_path = repo_root / self.VENDORED_SUBDIR

        if not table_path.exists():
            logger.warning(
                f"{self.__class__.__name__} submodule not found at {table_path}. " "Run: git submodule update --init"
            )
            return None

        return table_path

    def _download_from_git(self, cache_path: Path) -> bool:
        """Download tables from GitHub."""
        if self.GIT_REPO_URL is None:
            # No remote repository (e.g., CMIP7 uses packaged data)
            return False

        try:
            # Clone with depth 1 for speed, checkout specific tag/branch
            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir_path = Path(tmpdir)

                # Clone
                subprocess.run(
                    ["git", "clone", "--depth", "1", "--branch", self.version, self.GIT_REPO_URL, str(tmpdir_path)],
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
            logger.error(f"Failed to clone {self.__class__.__name__}: {e.stderr.decode()}")
            return False
        except Exception as e:
            logger.error(f"Error downloading {self.__class__.__name__}: {e}")
            return False


class CMIP6TableLocator(TableLocator):
    """Locator for CMIP6 data request tables."""

    DEFAULT_VERSION = "main"
    RESOURCE_NAME = "cmip6-tables"
    GIT_REPO_URL = "https://github.com/PCMDI/cmip6-cmor-tables.git"
    VENDORED_SUBDIR = "cmip6-cmor-tables/Tables"


class CMIP7TableLocator(TableLocator):
    """Locator for CMIP7 data request tables."""

    DEFAULT_VERSION = "main"
    RESOURCE_NAME = "cmip7-tables"
    GIT_REPO_URL = None  # CMIP7 uses packaged data
    VENDORED_SUBDIR = None

    def _get_packaged_path(self) -> Optional[Path]:
        """CMIP7 tables are packaged in src/pycmor/data/cmip7/."""
        return files("pycmor.data.cmip7")

    def _get_vendored_path(self) -> Optional[Path]:
        """CMIP7 has no vendored tables."""
        return None

    def _download_from_git(self, cache_path: Path) -> bool:
        """CMIP7 doesn't download tables from git."""
        return False


class MetadataLocator(ResourceLocator, metaclass=MetaFactory):
    """Base class for metadata locators."""

    pass


class CMIP6MetadataLocator(MetadataLocator):
    """
    Locator for CMIP6 metadata.

    CMIP6 doesn't use separate metadata files, so this always returns None.
    """

    def __init__(
        self,
        version: Optional[str] = None,
        user_path: Optional[Union[str, Path]] = None,
    ):
        super().__init__("cmip6-metadata", version, user_path)

    def locate(self) -> Optional[Path]:
        """CMIP6 doesn't have metadata files."""
        return None

    def _get_vendored_path(self) -> Optional[Path]:
        """CMIP6 has no vendored metadata."""
        return None

    def _download_from_git(self, cache_path: Path) -> bool:
        """CMIP6 doesn't download metadata."""
        return False


class CMIP7MetadataLocator(MetadataLocator):
    """
    Locator for CMIP7 Data Request metadata.

    Parameters
    ----------
    version : str, optional
        DReq version (e.g., 'v1.2.2.2', uses DEFAULT_VERSION if not specified)
    user_path : str or Path, optional
        User-specified CMIP7_DReq_metadata path
    """

    DEFAULT_VERSION = "v1.2.2.2"
    RESOURCE_NAME = "cmip7_metadata"

    def __init__(
        self,
        version: Optional[str] = None,
        user_path: Optional[Union[str, Path]] = None,
    ):
        # Use class-level default version if not specified
        version = version or self.DEFAULT_VERSION
        super().__init__(self.RESOURCE_NAME, version, user_path)

    def _get_cache_path(self) -> Path:
        """Override to return file path instead of directory path."""
        # For metadata, we want a file: ~/.cache/pycmor/cmip7_metadata/v1.2.2.2/metadata.json
        if self.version:
            return self._cache_base / self.resource_name / self.version / "metadata.json"
        else:
            return self._cache_base / self.resource_name / "metadata.json"

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
