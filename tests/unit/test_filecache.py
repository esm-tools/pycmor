"""Tests for the filecache module."""

import os
import tempfile
from unittest.mock import Mock, mock_open, patch

import pandas as pd
import pytest

from pycmor.core.filecache import Filecache
from tests.fixtures.filecache import sample_cache_data, sample_netcdf_file  # noqa: F401


class TestFilecache:
    """Test cases for the Filecache class."""

    def test_init_empty(self):
        """Test initialization with empty cache."""
        cache = Filecache()
        assert isinstance(cache.df, pd.DataFrame)
        assert cache.df.empty
        assert list(cache.df.columns) == cache._fields
        assert not cache._new_record

    def test_init_with_data(self, sample_cache_data):  # noqa: F811  # noqa: F811
        """Test initialization with existing data."""
        cache = Filecache(sample_cache_data)
        assert isinstance(cache.df, pd.DataFrame)
        assert len(cache.df) == 2
        assert cache.df.iloc[0]["variable"] == "temperature"
        assert not cache._new_record

    def test_fields_property(self):
        """Test that _fields contains expected columns."""
        expected_fields = [
            "variable",
            "freq",
            "start",
            "end",
            "timespan",
            "steps",
            "units",
            "filename",
            "filesize",
            "mtime",
            "checksum",
            "filepath",
        ]
        assert Filecache._fields == expected_fields

    @patch("pandas.read_csv")
    @patch("pycmor.core.filecache.Path")
    def test_load_existing_cache(self, mock_path, mock_read_csv):
        """Test loading an existing cache file."""
        mock_path_instance = mock_path.return_value.expanduser.return_value
        mock_path_instance.exists.return_value = True
        mock_path_instance.stat.return_value.st_size = 100
        mock_read_data = "#2024-01-01;1ME\ncolumn1,column2\nvalue1,value2"
        mock_path_instance.open = mock_open(read_data=mock_read_data)
        sample_data = pd.DataFrame({"variable": ["temp"], "freq": ["M"]})
        mock_read_csv.return_value = sample_data
        cache = Filecache.load()
        assert hasattr(cache, "cache_meta")
        assert cache.cache_meta == "#2024-01-01;1ME\n"
        mock_read_csv.assert_called_once_with(str(mock_path_instance), comment="#")

    @patch("pycmor.core.filecache.Path")
    def test_load_nonexistent_cache(self, mock_path):
        """Test loading when cache file doesn't exist."""
        mock_path_instance = mock_path.return_value.expanduser.return_value
        mock_path_instance.exists.return_value = False
        mock_path_instance.stat.return_value.st_size = 0
        mock_path_instance.open = mock_open(read_data="")
        cache = Filecache.load()
        assert cache.df.empty
        mock_path_instance.parent.mkdir.assert_called_once_with(exist_ok=True, parents=True)
        mock_path_instance.touch.assert_called_once()

    @patch("builtins.open")
    @patch("shutil.copyfileobj")
    def test_save_with_new_records(self, mock_copyfileobj, mock_open):
        """Test saving cache when new records exist."""
        cache = Filecache()
        cache._new_record = True
        cache.cache_meta = "#2024-01-01;1ME\n"
        cache.save()
        mock_open.assert_called_once()
        mock_copyfileobj.assert_called_once()

    def test_save_no_new_records(self):
        """Test saving cache when no new records exist."""
        cache = Filecache()
        cache._new_record = False
        with patch("builtins.open") as mock_open:
            cache.save()
            mock_open.assert_not_called()

    @patch("xarray.open_dataset")
    @patch("os.stat")
    @patch("pycmor.core.filecache.hashfile")
    def test_make_record(
        self,
        mock_hashfile,
        mock_stat,
        mock_open_dataset,
        sample_netcdf_file,  # noqa: F811
    ):
        """Test _make_record method."""
        mock_stat.return_value.st_size = 1024
        mock_stat.return_value.st_mtime = 1234567890
        mock_hashfile.return_value = "abc123"
        time = pd.date_range("2000-01-01", periods=12, freq="ME")
        mock_ds = Mock()
        mock_ds.time.to_pandas.return_value = pd.Series(time, index=time)
        mock_ds.data_vars.keys.return_value = ["temperature"]
        mock_ds.data_vars.values.return_value = [Mock(attrs={"units": "K"})]
        mock_ds.close = Mock()
        mock_open_dataset.return_value = mock_ds
        cache = Filecache()
        with patch.object(cache, "_infer_freq_from_file", return_value="ME") as mock_infer_freq:
            record = cache._make_record(sample_netcdf_file)
            assert isinstance(record, pd.Series)
            assert record["filename"] == os.path.basename(sample_netcdf_file)
            assert record["filepath"] == sample_netcdf_file
            assert record["checksum"] == "imohash:abc123"
            assert record["filesize"] == 1024
            assert record["mtime"] == 1234567890
            assert record["variable"] == "temperature"
            assert record["units"] == "K"
            assert record["freq"] == "ME"
            assert record["steps"] == 12
            mock_infer_freq.assert_called_once_with(sample_netcdf_file, mock_ds, mock_ds.time.to_pandas.return_value)

    def test_add_file_new(self, sample_cache_data):  # noqa: F811  # noqa: F811
        """Test adding a new file to cache."""
        cache = Filecache(sample_cache_data)
        with patch.object(cache, "_make_record") as mock_make_record:
            mock_record = pd.Series(
                {
                    "variable": "humidity",
                    "filename": "humidity.nc",
                    "filepath": "/path/to/humidity.nc",
                }
            )
            mock_make_record.return_value = mock_record
            cache.add_file("/path/to/humidity.nc")
            assert cache._new_record
            assert len(cache.df) == 3
            assert "humidity.nc" in cache.df.filename.values

    def test_add_file_existing(self, sample_cache_data):  # noqa: F811  # noqa: F811
        """Test adding an existing file to cache (should not add)."""
        cache = Filecache(sample_cache_data)
        initial_length = len(cache.df)
        cache.add_file("/path/to/temp.nc")  # File already exists
        assert len(cache.df) == initial_length
        assert not cache._new_record

    def test_infer_freq_cached(self, sample_cache_data):  # noqa: F811  # noqa: F811
        """Test infer_freq when frequency is already cached."""
        test_data = sample_cache_data.copy()
        test_filename = "test_infer_freq_cached.nc"
        test_record = {
            "variable": "test_var",
            "freq": "M",
            "filepath": f"/path/to/{test_filename}",
            "filename": test_filename,
            "start": "2000-01-01",
            "end": "2000-12-31",
            "timespan": "365 days",
            "steps": 12,
            "units": "K",
            "filesize": 1024,
            "mtime": 1234567890,
            "checksum": "imohash:test123",
        }
        test_data = pd.concat([test_data, pd.DataFrame([test_record])], ignore_index=True)
        cache = Filecache(test_data)
        result = cache.infer_freq(test_filename)
        assert result == "M"

    def test_infer_freq_from_directory(self, sample_cache_data):  # noqa: F811
        """Test infer_freq when inferring from directory files."""
        extended_data = sample_cache_data.copy()
        extended_data = pd.concat(
            [
                extended_data,
                pd.DataFrame(
                    {
                        "variable": ["temperature", "temperature"],
                        "freq": [None, None],
                        "start": ["2001-01-01", "2002-01-01"],
                        "end": ["2001-12-31", "2002-12-31"],
                        "timespan": ["365 days", "365 days"],
                        "steps": [12, 12],
                        "units": ["K", "K"],
                        "filename": ["temp2.nc", "temp3.nc"],
                        "filesize": [1024, 1024],
                        "mtime": [1234567892, 1234567893],
                        "checksum": ["imohash:ghi789", "imohash:jkl012"],
                        "filepath": ["/path/to/temp2.nc", "/path/to/temp3.nc"],
                    }
                ),
            ],
            ignore_index=True,
        )
        cache = Filecache(extended_data)
        with patch.object(cache, "get") as mock_get:
            mock_info = Mock()
            mock_info.freq = None
            mock_info.filepath = "/path/to/temp.nc"
            mock_info.variable = "temperature"
            mock_get.return_value = mock_info
            with patch("pycmor.core.filecache.infer_frequency") as mock_infer:
                mock_infer.return_value = "Y"
                result = cache.infer_freq("temp.nc")
                assert result == "Y"
                mock_infer.assert_called_once()
                args = mock_infer.call_args[0][0]
                assert len(args) == 3  # Three temperature files in same directory

    def test_infer_freq_single_timestamp_files(self, sample_cache_data):  # noqa: F811
        """Test frequency inference with single timestamp files when multiple similar files are added."""
        # Start with cache containing files with single timestamps (freq=None)
        single_timestamp_data = pd.DataFrame(
            {
                "variable": ["temperature", "temperature"],
                "freq": [None, None],  # Initially no frequency
                "start": ["2000-01-01", "2000-02-01"],
                "end": [
                    "2000-01-01",
                    "2000-02-01",
                ],  # Same start/end = single timestamp
                "timespan": ["0 days", "0 days"],
                "steps": [1, 1],  # Single timestamp
                "units": ["K", "K"],
                "filename": ["temp_200001.nc", "temp_200002.nc"],
                "filesize": [1024, 1024],
                "mtime": [1234567890, 1234567891],
                "checksum": ["imohash:abc123", "imohash:def456"],
                "filepath": ["/path/to/temp_200001.nc", "/path/to/temp_200002.nc"],
            }
        )

        cache = Filecache(single_timestamp_data)

        # Add a third similar file to trigger frequency inference
        with patch.object(cache, "_make_record") as mock_make_record:
            mock_record = pd.Series(
                {
                    "variable": "temperature",
                    "freq": None,
                    "start": "2000-03-01",
                    "end": "2000-03-01",
                    "timespan": "0 days",
                    "steps": 1,
                    "units": "K",
                    "filename": "temp_200003.nc",
                    "filesize": 1024,
                    "mtime": 1234567892,
                    "checksum": "imohash:ghi789",
                    "filepath": "/path/to/temp_200003.nc",
                }
            )
            mock_make_record.return_value = mock_record
            cache.add_file("/path/to/temp_200003.nc")

        # Now test frequency inference with multiple single-timestamp files
        with patch.object(cache, "get") as mock_get:
            mock_info = Mock()
            mock_info.freq = None  # File doesn't have frequency yet
            mock_info.filepath = "/path/to/temp_200001.nc"
            mock_info.variable = "temperature"
            mock_get.return_value = mock_info

            with patch("pycmor.core.filecache.infer_frequency") as mock_infer:
                mock_infer.return_value = "MS"  # Monthly start frequency
                result = cache.infer_freq("temp_200001.nc")

                assert result == "MS"
                mock_infer.assert_called_once()
                # Should pass all 3 temperature file timestamps for frequency inference
                args = mock_infer.call_args[0][0]
                assert len(args) == 3
                # Verify that the infer_frequency function was called with timestamps
                expected_timestamps = {"2000-01-01", "2000-02-01", "2000-03-01"}
                actual_timestamps = {str(ts.date()) for ts in args}
                assert actual_timestamps == expected_timestamps

    def test_get_method_exists(self, sample_cache_data):  # noqa: F811
        """Test that get method exists and can be called."""
        cache = Filecache(sample_cache_data)
        try:
            result = cache.get("temp.nc")
            assert result is not None
        except AttributeError:
            pytest.fail("get method not implemented in Filecache class")

    def test_summary_method_exists(self, sample_cache_data):  # noqa: F811  # noqa: F811
        """Test that summary method exists and can be called."""
        cache = Filecache(sample_cache_data)
        try:
            result = cache.summary()
            assert isinstance(result, pd.DataFrame)
        except AttributeError:
            pytest.fail("summary method not implemented in Filecache class")


class TestFilecacheIntegration:
    """Integration tests for filecache with real files."""

    def test_full_workflow(self, sample_netcdf_file):  # noqa: F811  # noqa: F811
        """Test complete workflow: create cache, add file, save, load."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = os.path.join(tmpdir, "test_cache.csv")
            with patch("pycmor.core.filecache.CACHE_FILE", cache_file):
                cache = Filecache()
                cache.add_file(sample_netcdf_file)
                # Manually set frequency to ME for the test
                cache.df.loc[0, "freq"] = "ME"
                cache.cache_meta = "#2024-01-01;1ME\n"
                cache.save()
                loaded_cache = Filecache.load()
                assert len(loaded_cache.df) == 1
                assert loaded_cache.df.iloc[0]["variable"] == "temperature"
                assert loaded_cache.df.iloc[0]["freq"] == "ME"

    def test_error_handling_invalid_file(self):
        """Test error handling when adding invalid file."""
        cache = Filecache()
        with pytest.raises((FileNotFoundError, OSError)):
            cache.add_file("/nonexistent/file.nc")
