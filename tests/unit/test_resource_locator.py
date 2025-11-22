"""
Unit tests for the ResourceLocator system.

Tests the 5-level priority chain for resource location:
1. User-specified path
2. XDG cache directory
3. Remote git (download to cache)
4. Packaged resources (importlib.resources)
5. Vendored git submodules
"""

import json
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from pycmor.core.resource_locator import CMIP6CVLocator, CMIP7CVLocator, CMIP7MetadataLocator, ResourceLocator


class TestResourceLocatorBase:
    """Test the base ResourceLocator class"""

    def test_can_create_instance(self):
        """Test that we can create a ResourceLocator instance"""
        locator = ResourceLocator("test-resource")
        assert locator.resource_name == "test-resource"
        assert locator.version is None
        assert locator.user_path is None

    def test_can_create_instance_with_version(self):
        """Test creating instance with version"""
        locator = ResourceLocator("test-resource", version="v1.0.0")
        assert locator.version == "v1.0.0"

    def test_can_create_instance_with_user_path(self):
        """Test creating instance with user path"""
        user_path = Path("/tmp/test")
        locator = ResourceLocator("test-resource", user_path=user_path)
        assert locator.user_path == user_path

    def test_get_cache_directory_default(self):
        """Test cache directory uses ~/.cache/pycmor by default"""
        cache_dir = ResourceLocator._get_cache_directory()
        assert cache_dir.name == "pycmor"
        assert cache_dir.parent.name == ".cache"
        assert cache_dir.exists()

    def test_get_cache_directory_respects_xdg(self):
        """Test cache directory respects XDG_CACHE_HOME"""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict("os.environ", {"XDG_CACHE_HOME": tmpdir}):
                cache_dir = ResourceLocator._get_cache_directory()
                assert cache_dir.parent == Path(tmpdir)
                assert cache_dir.name == "pycmor"

    def test_get_cache_path_without_version(self):
        """Test cache path construction without version"""
        locator = ResourceLocator("test-resource")
        cache_path = locator._get_cache_path()
        assert "test-resource" in str(cache_path)
        assert cache_path.parent.name == "pycmor"

    def test_get_cache_path_with_version(self):
        """Test cache path construction with version"""
        locator = ResourceLocator("test-resource", version="v1.0.0")
        cache_path = locator._get_cache_path()
        assert "test-resource" in str(cache_path)
        assert "v1.0.0" in str(cache_path)

    def test_validate_cache_nonexistent(self):
        """Test cache validation fails for nonexistent path"""
        locator = ResourceLocator("test-resource")
        fake_path = Path("/nonexistent/path")
        assert not locator._validate_cache(fake_path)

    def test_validate_cache_empty_directory(self):
        """Test cache validation fails for empty directory"""
        locator = ResourceLocator("test-resource")
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            assert not locator._validate_cache(tmp_path)

    def test_validate_cache_nonempty_directory(self):
        """Test cache validation succeeds for non-empty directory"""
        locator = ResourceLocator("test-resource")
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            # Create a file in the directory
            (tmp_path / "test.txt").write_text("test content")
            assert locator._validate_cache(tmp_path)

    def test_validate_cache_nonempty_file(self):
        """Test cache validation succeeds for non-empty file"""
        locator = ResourceLocator("test-resource")
        with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
            tmpfile.write(b"test content")
            tmpfile.flush()
            tmp_path = Path(tmpfile.name)
            try:
                assert locator._validate_cache(tmp_path)
            finally:
                tmp_path.unlink()

    def test_get_packaged_path_not_implemented_in_base(self):
        """Test that _get_packaged_path returns None in base class"""
        locator = ResourceLocator("test-resource")
        assert locator._get_packaged_path() is None

    def test_get_vendored_path_not_implemented_in_base(self):
        """Test that _get_vendored_path raises NotImplementedError"""
        locator = ResourceLocator("test-resource")
        with pytest.raises(NotImplementedError):
            locator._get_vendored_path()

    def test_download_from_git_not_implemented_in_base(self):
        """Test that _download_from_git raises NotImplementedError"""
        locator = ResourceLocator("test-resource")
        with pytest.raises(NotImplementedError):
            locator._download_from_git(Path("/tmp/test"))


class TestResourceLocatorPriorityChain:
    """Test the 5-level priority chain"""

    def test_priority_1_user_specified_path(self):
        """Test that user-specified path has highest priority"""
        with tempfile.TemporaryDirectory() as tmpdir:
            user_path = Path(tmpdir) / "user-cvs"
            user_path.mkdir()
            (user_path / "test.json").write_text('{"test": "data"}')

            # Mock other methods to ensure they're not called
            with patch.object(ResourceLocator, "_download_from_git", return_value=True):
                with patch.object(ResourceLocator, "_get_vendored_path") as mock_vendored:
                    mock_vendored.return_value = Path("/fake/vendored/path")

                    locator = ResourceLocator("test-resource", user_path=user_path)
                    result = locator.locate()

                    # Should return user path without calling other methods
                    assert result == user_path
                    mock_vendored.assert_not_called()

    def test_priority_2_xdg_cache(self):
        """Test that XDG cache is used when user path not available"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Set up cache directory
            cache_base = Path(tmpdir) / "pycmor"
            cache_base.mkdir(parents=True)
            cache_path = cache_base / "test-resource" / "v1.0.0"
            cache_path.mkdir(parents=True)
            (cache_path / "test.json").write_text('{"test": "cached"}')

            with patch.object(ResourceLocator, "_get_cache_directory", return_value=cache_base):
                with patch.object(ResourceLocator, "_download_from_git") as mock_git:
                    with patch.object(ResourceLocator, "_get_vendored_path") as mock_vendored:
                        mock_vendored.return_value = Path("/fake/vendored/path")

                        locator = ResourceLocator("test-resource", version="v1.0.0")
                        result = locator.locate()

                        # Should return cache path without calling git
                        assert result == cache_path
                        mock_git.assert_not_called()

    def test_priority_3_remote_git(self):
        """Test that remote git download is attempted when cache empty"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_base = Path(tmpdir) / "pycmor"
            cache_base.mkdir(parents=True)
            cache_path = cache_base / "test-resource"

            # Mock successful git download
            def mock_download(path):
                path.mkdir(parents=True, exist_ok=True)
                (path / "test.json").write_text('{"test": "from-git"}')
                return True

            with patch.object(ResourceLocator, "_get_cache_directory", return_value=cache_base):
                with patch.object(ResourceLocator, "_download_from_git", side_effect=mock_download):
                    with patch.object(ResourceLocator, "_get_vendored_path") as mock_vendored:
                        mock_vendored.return_value = None

                        locator = ResourceLocator("test-resource")
                        result = locator.locate()

                        # Should have created cache_path via git download
                        assert result == cache_path
                        assert (cache_path / "test.json").exists()

    def test_priority_5_vendored_submodules(self):
        """Test that vendored submodules are used as last resort"""
        with tempfile.TemporaryDirectory() as tmpdir:
            vendored_path = Path(tmpdir) / "vendored-cvs"
            vendored_path.mkdir()
            (vendored_path / "test.json").write_text('{"test": "vendored"}')

            cache_base = Path(tmpdir) / "pycmor"
            cache_base.mkdir(parents=True)

            # Mock failed git download and no packaged data
            with patch.object(ResourceLocator, "_get_cache_directory", return_value=cache_base):
                with patch.object(ResourceLocator, "_download_from_git", return_value=False):
                    with patch.object(ResourceLocator, "_get_packaged_path", return_value=None):
                        with patch.object(ResourceLocator, "_get_vendored_path", return_value=vendored_path):
                            locator = ResourceLocator("test-resource")
                            result = locator.locate()

                            # Should return vendored path as last resort
                            assert result == vendored_path

    def test_returns_none_when_all_sources_fail(self):
        """Test that None is returned when all sources fail"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_base = Path(tmpdir) / "pycmor"
            cache_base.mkdir(parents=True)

            with patch.object(ResourceLocator, "_get_cache_directory", return_value=cache_base):
                with patch.object(ResourceLocator, "_download_from_git", return_value=False):
                    with patch.object(ResourceLocator, "_get_packaged_path", return_value=None):
                        with patch.object(ResourceLocator, "_get_vendored_path", return_value=None):
                            locator = ResourceLocator("test-resource")
                            result = locator.locate()

                            # Should return None when everything fails
                            assert result is None


class TestCVLocator:
    """Test the CV locator factory pattern"""

    def test_can_create_cmip6_locator(self):
        """Test creating CMIP6CVLocator"""
        locator = CMIP6CVLocator()
        assert locator.resource_name == "cmip6-cvs"
        assert locator.version == "6.2.58.64"  # Default
        assert locator.DEFAULT_VERSION == "6.2.58.64"
        assert locator.GIT_REPO_URL == "https://github.com/WCRP-CMIP/CMIP6_CVs.git"

    def test_can_create_cmip6_locator_with_custom_version(self):
        """Test creating CMIP6CVLocator with custom version"""
        locator = CMIP6CVLocator(version="6.2.50.0")
        assert locator.version == "6.2.50.0"

    def test_can_create_cmip7_locator(self):
        """Test creating CMIP7CVLocator"""
        locator = CMIP7CVLocator()
        assert locator.resource_name == "cmip7-cvs"
        assert locator.version == "src-data"  # Default
        assert locator.DEFAULT_VERSION == "src-data"
        assert locator.GIT_REPO_URL == "https://github.com/WCRP-CMIP/CMIP7-CVs.git"

    def test_cmip6_class_attributes(self):
        """Test that CMIP6CVLocator has correct class attributes"""
        assert CMIP6CVLocator.DEFAULT_VERSION == "6.2.58.64"
        assert CMIP6CVLocator.RESOURCE_NAME == "cmip6-cvs"
        assert CMIP6CVLocator.VENDORED_SUBDIR == "cmip6-cmor-tables/CMIP6_CVs"

    def test_cmip7_class_attributes(self):
        """Test that CMIP7CVLocator has correct class attributes"""
        assert CMIP7CVLocator.DEFAULT_VERSION == "src-data"
        assert CMIP7CVLocator.RESOURCE_NAME == "cmip7-cvs"
        assert CMIP7CVLocator.VENDORED_SUBDIR == "CMIP7-CVs"

    def test_get_vendored_path_cmip6(self):
        """Test vendored path for CMIP6"""
        locator = CMIP6CVLocator()
        vendored = locator._get_vendored_path()

        # Should point to cmip6-cmor-tables/CMIP6_CVs
        if vendored:  # Only check if submodule exists
            assert "cmip6-cmor-tables" in str(vendored)
            assert vendored.name == "CMIP6_CVs"

    def test_get_vendored_path_cmip7(self):
        """Test vendored path for CMIP7"""
        locator = CMIP7CVLocator()
        vendored = locator._get_vendored_path()

        # Should point to CMIP7-CVs
        if vendored:  # Only check if submodule exists
            assert vendored.name == "CMIP7-CVs"

    @pytest.mark.skipif(
        not (Path(__file__).parent.parent.parent / "cmip6-cmor-tables" / "CMIP6_CVs").exists(),
        reason="CMIP6 CVs submodule not initialized",
    )
    def test_locate_cmip6_from_vendored(self):
        """Test locating CMIP6 CVs from vendored submodule"""
        locator = CMIP6CVLocator()
        result = locator.locate()
        assert result is not None
        assert result.exists()

    @pytest.mark.skipif(
        not (Path(__file__).parent.parent.parent / "CMIP7-CVs").exists(),
        reason="CMIP7 CVs submodule not initialized",
    )
    def test_locate_cmip7_from_vendored(self):
        """Test locating CMIP7 CVs from vendored submodule"""
        locator = CMIP7CVLocator()
        result = locator.locate()
        assert result is not None
        assert result.exists()


class TestCMIP7MetadataLocator:
    """Test the CMIP7MetadataLocator"""

    def test_can_create_locator(self):
        """Test creating CMIP7MetadataLocator"""
        locator = CMIP7MetadataLocator()
        assert locator.resource_name == "cmip7_metadata"
        assert locator.version == "v1.2.2.2"  # Default

    def test_can_create_locator_with_custom_version(self):
        """Test creating locator with custom version"""
        locator = CMIP7MetadataLocator(version="v1.2.0.0")
        assert locator.version == "v1.2.0.0"

    def test_can_create_locator_with_user_path(self):
        """Test creating locator with user-specified path"""
        user_path = Path("/tmp/metadata.json")
        locator = CMIP7MetadataLocator(user_path=user_path)
        assert locator.user_path == user_path

    def test_get_vendored_path_returns_none(self):
        """Test that vendored path is None for metadata (must be generated)"""
        locator = CMIP7MetadataLocator()
        assert locator._get_vendored_path() is None

    def test_validate_cache_checks_json_structure(self):
        """Test that cache validation checks JSON structure"""
        locator = CMIP7MetadataLocator()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmpfile:
            # Valid metadata structure
            json.dump({"Compound Name": {"test": "data"}, "Header": {}}, tmpfile)
            tmpfile.flush()
            tmp_path = Path(tmpfile.name)

            try:
                assert locator._validate_cache(tmp_path)
            finally:
                tmp_path.unlink()

    def test_validate_cache_rejects_invalid_json(self):
        """Test that cache validation rejects invalid JSON"""
        locator = CMIP7MetadataLocator()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmpfile:
            tmpfile.write("not valid json {")
            tmpfile.flush()
            tmp_path = Path(tmpfile.name)

            try:
                assert not locator._validate_cache(tmp_path)
            finally:
                tmp_path.unlink()

    def test_validate_cache_rejects_wrong_structure(self):
        """Test that cache validation rejects JSON with wrong structure"""
        locator = CMIP7MetadataLocator()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmpfile:
            # Wrong structure (missing expected keys)
            json.dump({"wrong": "structure"}, tmpfile)
            tmpfile.flush()
            tmp_path = Path(tmpfile.name)

            try:
                assert not locator._validate_cache(tmp_path)
            finally:
                tmp_path.unlink()

    @pytest.mark.skipif(
        shutil.which("export_dreq_lists_json") is None,
        reason="export_dreq_lists_json not installed",
    )
    def test_download_from_git_generates_metadata(self):
        """Test that download_from_git generates metadata file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "metadata.json"
            locator = CMIP7MetadataLocator()

            # This should run export_dreq_lists_json
            result = locator._download_from_git(cache_path)

            # Should have generated the file
            assert result is True
            assert cache_path.exists()

            # Should be valid JSON with expected structure
            with open(cache_path) as f:
                data = json.load(f)
                assert "Compound Name" in data or "Header" in data
