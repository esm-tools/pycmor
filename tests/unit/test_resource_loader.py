"""
Unit tests for the ResourceLoader system.

Tests the 5-level priority chain for resource loading:
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

from pycmor.core.resource_loader import CMIP7MetadataLoader, CVLoader, ResourceLoader


class TestResourceLoaderBase:
    """Test the base ResourceLoader class"""

    def test_can_create_instance(self):
        """Test that we can create a ResourceLoader instance"""
        loader = ResourceLoader("test-resource")
        assert loader.resource_name == "test-resource"
        assert loader.version is None
        assert loader.user_path is None

    def test_can_create_instance_with_version(self):
        """Test creating instance with version"""
        loader = ResourceLoader("test-resource", version="v1.0.0")
        assert loader.version == "v1.0.0"

    def test_can_create_instance_with_user_path(self):
        """Test creating instance with user path"""
        user_path = Path("/tmp/test")
        loader = ResourceLoader("test-resource", user_path=user_path)
        assert loader.user_path == user_path

    def test_get_cache_directory_default(self):
        """Test cache directory uses ~/.cache/pycmor by default"""
        cache_dir = ResourceLoader._get_cache_directory()
        assert cache_dir.name == "pycmor"
        assert cache_dir.parent.name == ".cache"
        assert cache_dir.exists()

    def test_get_cache_directory_respects_xdg(self):
        """Test cache directory respects XDG_CACHE_HOME"""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict("os.environ", {"XDG_CACHE_HOME": tmpdir}):
                cache_dir = ResourceLoader._get_cache_directory()
                assert cache_dir.parent == Path(tmpdir)
                assert cache_dir.name == "pycmor"

    def test_get_cache_path_without_version(self):
        """Test cache path construction without version"""
        loader = ResourceLoader("test-resource")
        cache_path = loader._get_cache_path()
        assert "test-resource" in str(cache_path)
        assert cache_path.parent.name == "pycmor"

    def test_get_cache_path_with_version(self):
        """Test cache path construction with version"""
        loader = ResourceLoader("test-resource", version="v1.0.0")
        cache_path = loader._get_cache_path()
        assert "test-resource" in str(cache_path)
        assert "v1.0.0" in str(cache_path)

    def test_validate_cache_nonexistent(self):
        """Test cache validation fails for nonexistent path"""
        loader = ResourceLoader("test-resource")
        fake_path = Path("/nonexistent/path")
        assert not loader._validate_cache(fake_path)

    def test_validate_cache_empty_directory(self):
        """Test cache validation fails for empty directory"""
        loader = ResourceLoader("test-resource")
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            assert not loader._validate_cache(tmp_path)

    def test_validate_cache_nonempty_directory(self):
        """Test cache validation succeeds for non-empty directory"""
        loader = ResourceLoader("test-resource")
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            # Create a file in the directory
            (tmp_path / "test.txt").write_text("test content")
            assert loader._validate_cache(tmp_path)

    def test_validate_cache_nonempty_file(self):
        """Test cache validation succeeds for non-empty file"""
        loader = ResourceLoader("test-resource")
        with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
            tmpfile.write(b"test content")
            tmpfile.flush()
            tmp_path = Path(tmpfile.name)
            try:
                assert loader._validate_cache(tmp_path)
            finally:
                tmp_path.unlink()

    def test_get_packaged_path_not_implemented_in_base(self):
        """Test that _get_packaged_path returns None in base class"""
        loader = ResourceLoader("test-resource")
        assert loader._get_packaged_path() is None

    def test_get_vendored_path_not_implemented_in_base(self):
        """Test that _get_vendored_path raises NotImplementedError"""
        loader = ResourceLoader("test-resource")
        with pytest.raises(NotImplementedError):
            loader._get_vendored_path()

    def test_download_from_git_not_implemented_in_base(self):
        """Test that _download_from_git raises NotImplementedError"""
        loader = ResourceLoader("test-resource")
        with pytest.raises(NotImplementedError):
            loader._download_from_git(Path("/tmp/test"))


class TestResourceLoaderPriorityChain:
    """Test the 5-level priority chain"""

    def test_priority_1_user_specified_path(self):
        """Test that user-specified path has highest priority"""
        with tempfile.TemporaryDirectory() as tmpdir:
            user_path = Path(tmpdir) / "user-cvs"
            user_path.mkdir()
            (user_path / "test.json").write_text('{"test": "data"}')

            # Mock other methods to ensure they're not called
            with patch.object(ResourceLoader, "_download_from_git", return_value=True):
                with patch.object(ResourceLoader, "_get_vendored_path") as mock_vendored:
                    mock_vendored.return_value = Path("/fake/vendored/path")

                    loader = ResourceLoader("test-resource", user_path=user_path)
                    result = loader.load()

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

            with patch.object(ResourceLoader, "_get_cache_directory", return_value=cache_base):
                with patch.object(ResourceLoader, "_download_from_git") as mock_git:
                    with patch.object(ResourceLoader, "_get_vendored_path") as mock_vendored:
                        mock_vendored.return_value = Path("/fake/vendored/path")

                        loader = ResourceLoader("test-resource", version="v1.0.0")
                        result = loader.load()

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

            with patch.object(ResourceLoader, "_get_cache_directory", return_value=cache_base):
                with patch.object(ResourceLoader, "_download_from_git", side_effect=mock_download):
                    with patch.object(ResourceLoader, "_get_vendored_path") as mock_vendored:
                        mock_vendored.return_value = None

                        loader = ResourceLoader("test-resource")
                        result = loader.load()

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
            with patch.object(ResourceLoader, "_get_cache_directory", return_value=cache_base):
                with patch.object(ResourceLoader, "_download_from_git", return_value=False):
                    with patch.object(ResourceLoader, "_get_packaged_path", return_value=None):
                        with patch.object(ResourceLoader, "_get_vendored_path", return_value=vendored_path):
                            loader = ResourceLoader("test-resource")
                            result = loader.load()

                            # Should return vendored path as last resort
                            assert result == vendored_path

    def test_returns_none_when_all_sources_fail(self):
        """Test that None is returned when all sources fail"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_base = Path(tmpdir) / "pycmor"
            cache_base.mkdir(parents=True)

            with patch.object(ResourceLoader, "_get_cache_directory", return_value=cache_base):
                with patch.object(ResourceLoader, "_download_from_git", return_value=False):
                    with patch.object(ResourceLoader, "_get_packaged_path", return_value=None):
                        with patch.object(ResourceLoader, "_get_vendored_path", return_value=None):
                            loader = ResourceLoader("test-resource")
                            result = loader.load()

                            # Should return None when everything fails
                            assert result is None


class TestCVLoader:
    """Test the CVLoader for controlled vocabularies"""

    def test_can_create_cmip6_loader(self):
        """Test creating CVLoader for CMIP6"""
        loader = CVLoader(cmor_version="CMIP6")
        assert loader.cmor_version == "CMIP6"
        assert loader.resource_name == "cmip6-cvs"
        assert loader.version == "6.2.58.64"  # Default

    def test_can_create_cmip6_loader_with_custom_version(self):
        """Test creating CVLoader for CMIP6 with custom version"""
        loader = CVLoader(cmor_version="CMIP6", version="6.2.50.0")
        assert loader.version == "6.2.50.0"

    def test_can_create_cmip7_loader(self):
        """Test creating CVLoader for CMIP7"""
        loader = CVLoader(cmor_version="CMIP7")
        assert loader.cmor_version == "CMIP7"
        assert loader.resource_name == "cmip7-cvs"
        assert loader.version is None  # CMIP7 uses branch

    def test_invalid_cmor_version_raises_error(self):
        """Test that invalid CMOR version raises ValueError"""
        with pytest.raises(ValueError, match="Unknown CMOR version"):
            CVLoader(cmor_version="CMIP8")

    def test_get_vendored_path_cmip6(self):
        """Test vendored path for CMIP6"""
        loader = CVLoader(cmor_version="CMIP6")
        vendored = loader._get_vendored_path()

        # Should point to cmip6-cmor-tables/CMIP6_CVs
        if vendored:  # Only check if submodule exists
            assert "cmip6-cmor-tables" in str(vendored)
            assert vendored.name == "CMIP6_CVs"

    def test_get_vendored_path_cmip7(self):
        """Test vendored path for CMIP7"""
        loader = CVLoader(cmor_version="CMIP7")
        vendored = loader._get_vendored_path()

        # Should point to CMIP7-CVs
        if vendored:  # Only check if submodule exists
            assert vendored.name == "CMIP7-CVs"

    @pytest.mark.skipif(
        not (Path(__file__).parent.parent.parent / "cmip6-cmor-tables" / "CMIP6_CVs").exists(),
        reason="CMIP6 CVs submodule not initialized",
    )
    def test_load_cmip6_from_vendored(self):
        """Test loading CMIP6 CVs from vendored submodule"""
        loader = CVLoader(cmor_version="CMIP6")
        result = loader.load()
        assert result is not None
        assert result.exists()

    @pytest.mark.skipif(
        not (Path(__file__).parent.parent.parent / "CMIP7-CVs").exists(),
        reason="CMIP7 CVs submodule not initialized",
    )
    def test_load_cmip7_from_vendored(self):
        """Test loading CMIP7 CVs from vendored submodule"""
        loader = CVLoader(cmor_version="CMIP7")
        result = loader.load()
        assert result is not None
        assert result.exists()


class TestCMIP7MetadataLoader:
    """Test the CMIP7MetadataLoader"""

    def test_can_create_loader(self):
        """Test creating CMIP7MetadataLoader"""
        loader = CMIP7MetadataLoader()
        assert loader.resource_name == "cmip7_metadata"
        assert loader.version == "v1.2.2.2"  # Default

    def test_can_create_loader_with_custom_version(self):
        """Test creating loader with custom version"""
        loader = CMIP7MetadataLoader(version="v1.2.0.0")
        assert loader.version == "v1.2.0.0"

    def test_can_create_loader_with_user_path(self):
        """Test creating loader with user-specified path"""
        user_path = Path("/tmp/metadata.json")
        loader = CMIP7MetadataLoader(user_path=user_path)
        assert loader.user_path == user_path

    def test_get_vendored_path_returns_none(self):
        """Test that vendored path is None for metadata (must be generated)"""
        loader = CMIP7MetadataLoader()
        assert loader._get_vendored_path() is None

    def test_validate_cache_checks_json_structure(self):
        """Test that cache validation checks JSON structure"""
        loader = CMIP7MetadataLoader()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmpfile:
            # Valid metadata structure
            json.dump({"Compound Name": {"test": "data"}, "Header": {}}, tmpfile)
            tmpfile.flush()
            tmp_path = Path(tmpfile.name)

            try:
                assert loader._validate_cache(tmp_path)
            finally:
                tmp_path.unlink()

    def test_validate_cache_rejects_invalid_json(self):
        """Test that cache validation rejects invalid JSON"""
        loader = CMIP7MetadataLoader()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmpfile:
            tmpfile.write("not valid json {")
            tmpfile.flush()
            tmp_path = Path(tmpfile.name)

            try:
                assert not loader._validate_cache(tmp_path)
            finally:
                tmp_path.unlink()

    def test_validate_cache_rejects_wrong_structure(self):
        """Test that cache validation rejects JSON with wrong structure"""
        loader = CMIP7MetadataLoader()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmpfile:
            # Wrong structure (missing expected keys)
            json.dump({"wrong": "structure"}, tmpfile)
            tmpfile.flush()
            tmp_path = Path(tmpfile.name)

            try:
                assert not loader._validate_cache(tmp_path)
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
            loader = CMIP7MetadataLoader()

            # This should run export_dreq_lists_json
            result = loader._download_from_git(cache_path)

            # Should have generated the file
            assert result is True
            assert cache_path.exists()

            # Should be valid JSON with expected structure
            with open(cache_path) as f:
                data = json.load(f)
                assert "Compound Name" in data or "Header" in data
