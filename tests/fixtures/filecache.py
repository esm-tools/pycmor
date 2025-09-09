"""Fixtures for filecache tests."""

import os
import tempfile

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pycmor.core.filecache import Filecache


@pytest.fixture
def sample_netcdf_file():
    """Create a temporary NetCDF file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        # Create sample data
        time = pd.date_range("2000-01-01", periods=12, freq="ME")
        data = np.random.rand(12, 10, 10)

        # Create xarray dataset
        ds = xr.Dataset(
            {
                "temperature": (["time", "lat", "lon"], data),
            },
            coords={
                "time": time,
                "lat": np.linspace(-90, 90, 10),
                "lon": np.linspace(-180, 180, 10),
            },
        )

        # Add attributes
        ds.temperature.attrs["units"] = "K"
        ds.temperature.attrs["long_name"] = "Temperature"

        # Save to file
        ds.to_netcdf(tmp.name)
        ds.close()

        yield tmp.name

        # Cleanup
        os.unlink(tmp.name)


@pytest.fixture
def empty_filecache():
    """Create an empty filecache instance."""
    return Filecache()


@pytest.fixture
def sample_cache_data():
    """Create sample cache data for testing."""
    return pd.DataFrame(
        {
            "variable": ["temperature", "precipitation"],
            "freq": ["M", "D"],
            "start": ["2000-01-01", "2000-01-01"],
            "end": ["2000-12-31", "2000-12-31"],
            "timespan": ["365 days", "365 days"],
            "steps": [12, 365],
            "units": ["K", "mm/day"],
            "filename": ["temp.nc", "precip.nc"],
            "filesize": [1024, 2048],
            "mtime": [1234567890, 1234567891],
            "checksum": ["imohash:abc123", "imohash:def456"],
            "filepath": ["/path/to/temp.nc", "/path/to/precip.nc"],
        }
    )
