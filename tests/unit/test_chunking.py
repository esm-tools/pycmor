"""Tests for NetCDF chunking functionality."""

import numpy as np
import pytest
import xarray as xr

from pycmor.std_lib.chunking import (
    NoMatchingChunks,
    calculate_chunks_even_divisor,
    calculate_chunks_iterative,
    calculate_chunks_simple,
    get_encoding_with_chunks,
    get_memory_size,
)


@pytest.fixture
def sample_dataset():
    """Create a sample dataset for testing."""
    time = np.arange(100)
    lat = np.arange(180)
    lon = np.arange(360)
    data = np.random.rand(100, 180, 360)

    ds = xr.Dataset(
        {
            "temperature": (["time", "lat", "lon"], data),
        },
        coords={
            "time": time,
            "lat": lat,
            "lon": lon,
        },
    )
    return ds


@pytest.fixture
def small_dataset():
    """Create a small dataset for testing."""
    time = np.arange(12)
    lat = np.arange(10)
    lon = np.arange(20)
    data = np.random.rand(12, 10, 20)

    ds = xr.Dataset(
        {
            "temp": (["time", "lat", "lon"], data),
        },
        coords={
            "time": time,
            "lat": lat,
            "lon": lon,
        },
    )
    return ds


def test_get_memory_size(sample_dataset):
    """Test memory size calculation."""
    chunks = {"time": 10, "lat": 90, "lon": 180}
    mem_size = get_memory_size(sample_dataset, chunks)
    # Expected: 10 * 90 * 180 * 8 bytes (float64)
    expected = 10 * 90 * 180 * 8
    assert mem_size == expected


def test_calculate_chunks_simple_with_time(sample_dataset):
    """Test simple chunking algorithm with time preference."""
    chunks = calculate_chunks_simple(
        sample_dataset,
        target_chunk_size="1MB",
        prefer_time_chunking=True,
    )

    # Should have chunks for all dimensions
    assert "time" in chunks
    assert "lat" in chunks
    assert "lon" in chunks

    # Time should be chunked, spatial dims should be full
    assert chunks["time"] < sample_dataset.sizes["time"]
    assert chunks["lat"] == sample_dataset.sizes["lat"]
    assert chunks["lon"] == sample_dataset.sizes["lon"]


def test_calculate_chunks_simple_without_time_preference(sample_dataset):
    """Test simple chunking without time preference."""
    chunks = calculate_chunks_simple(
        sample_dataset,
        target_chunk_size="1MB",
        prefer_time_chunking=False,
    )

    # Should have chunks for all dimensions
    assert "time" in chunks
    assert "lat" in chunks
    assert "lon" in chunks

    # All dimensions should be chunked
    assert chunks["time"] >= 1
    assert chunks["lat"] >= 1
    assert chunks["lon"] >= 1


def test_calculate_chunks_even_divisor(small_dataset):
    """Test even divisor algorithm."""
    chunks = calculate_chunks_even_divisor(
        small_dataset,
        target_chunk_size="10KB",
        size_tolerance=0.5,
    )

    # Should have chunks for all dimensions
    assert "time" in chunks
    assert "lat" in chunks
    assert "lon" in chunks

    # Chunks should evenly divide dimensions
    assert small_dataset.sizes["time"] % chunks["time"] == 0
    assert small_dataset.sizes["lat"] % chunks["lat"] == 0
    assert small_dataset.sizes["lon"] % chunks["lon"] == 0


def test_calculate_chunks_even_divisor_no_match():
    """Test even divisor algorithm when no match is found."""
    ds = xr.Dataset(
        {
            "temp": (["time", "lat"], np.random.rand(13, 17)),
        },
        coords={
            "time": np.arange(13),
            "lat": np.arange(17),
        },
    )

    # Very tight tolerance should fail
    with pytest.raises(NoMatchingChunks):
        calculate_chunks_even_divisor(
            ds,
            target_chunk_size="100B",
            size_tolerance=0.01,
        )


def test_calculate_chunks_iterative(sample_dataset):
    """Test iterative algorithm."""
    chunks = calculate_chunks_iterative(
        sample_dataset,
        target_chunk_size="10MB",
        size_tolerance=0.5,
    )

    # Should have chunks for all dimensions
    assert "time" in chunks
    assert "lat" in chunks
    assert "lon" in chunks

    # All chunks should be positive
    assert all(v > 0 for v in chunks.values())


def test_calculate_chunks_iterative_no_match():
    """Test iterative algorithm when no match is found."""
    ds = xr.Dataset(
        {
            "temp": (["time"], np.random.rand(10)),
        },
        coords={
            "time": np.arange(10),
        },
    )

    # Very tight tolerance should fail
    with pytest.raises(NoMatchingChunks):
        calculate_chunks_iterative(
            ds,
            target_chunk_size="1B",
            size_tolerance=0.001,
        )


def test_get_encoding_with_chunks(sample_dataset):
    """Test encoding generation with chunks."""
    chunks = {"time": 10, "lat": 90, "lon": 180}
    encoding = get_encoding_with_chunks(
        sample_dataset,
        chunks=chunks,
        compression_level=4,
        enable_compression=True,
    )

    # Should have encoding for all data variables
    assert "temperature" in encoding

    # Should have chunksizes
    assert "chunksizes" in encoding["temperature"]
    assert encoding["temperature"]["chunksizes"] == (10, 90, 180)

    # Should have compression
    assert encoding["temperature"]["zlib"] is True
    assert encoding["temperature"]["complevel"] == 4


def test_get_encoding_without_compression(sample_dataset):
    """Test encoding generation without compression."""
    chunks = {"time": 10, "lat": 90, "lon": 180}
    encoding = get_encoding_with_chunks(
        sample_dataset,
        chunks=chunks,
        enable_compression=False,
    )

    # Should have chunksizes but no compression
    assert "chunksizes" in encoding["temperature"]
    assert "zlib" not in encoding["temperature"] or not encoding["temperature"]["zlib"]


def test_get_encoding_without_chunks(sample_dataset):
    """Test encoding generation without chunks."""
    encoding = get_encoding_with_chunks(
        sample_dataset,
        chunks=None,
        compression_level=4,
        enable_compression=True,
    )

    # Should have compression but no chunksizes
    assert "temperature" in encoding
    assert "chunksizes" not in encoding["temperature"]
    assert encoding["temperature"]["zlib"] is True


def test_chunks_with_string_size(sample_dataset):
    """Test that string sizes are parsed correctly."""
    chunks = calculate_chunks_simple(
        sample_dataset,
        target_chunk_size="100MB",
        prefer_time_chunking=True,
    )

    # Should work without error
    assert "time" in chunks
    assert all(v > 0 for v in chunks.values())


def test_chunks_with_custom_aspect_ratio(small_dataset):
    """Test even divisor with custom aspect ratio."""
    aspect_ratio = {"time": 10, "lat": 1, "lon": 1}
    chunks = calculate_chunks_even_divisor(
        small_dataset,
        target_chunk_size="5KB",
        target_chunks_aspect_ratio=aspect_ratio,
        size_tolerance=0.8,
    )

    # Should prefer more chunks in time dimension
    time_chunks = small_dataset.sizes["time"] / chunks["time"]
    lat_chunks = small_dataset.sizes["lat"] / chunks["lat"]

    # Time should have more chunks (or equal if constrained by size)
    assert time_chunks >= lat_chunks


def test_chunks_with_unchunked_dimension(small_dataset):
    """Test with a dimension that should not be chunked."""
    aspect_ratio = {"time": 10, "lat": -1, "lon": -1}
    chunks = calculate_chunks_even_divisor(
        small_dataset,
        target_chunk_size="10KB",
        target_chunks_aspect_ratio=aspect_ratio,
        size_tolerance=0.8,
    )

    # lat and lon should be unchunked (full size)
    assert chunks["lat"] == small_dataset.sizes["lat"]
    assert chunks["lon"] == small_dataset.sizes["lon"]
    # time should be chunked
    assert chunks["time"] < small_dataset.sizes["time"]
