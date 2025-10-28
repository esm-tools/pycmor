"""Tests for vertical coordinate bounds calculation."""

import numpy as np
import xarray as xr

from pycmor.std_lib.bounds import add_vertical_bounds


def test_add_vertical_bounds_pressure_levels():
    """Test adding bounds for pressure level coordinates."""
    # Standard CMIP6 pressure levels (Pa)
    plev = [100000, 92500, 85000, 70000, 60000, 50000, 40000, 30000]

    ds = xr.Dataset(
        {
            "ta": (["time", "plev", "lat", "lon"], np.random.rand(10, 8, 5, 6)),
        },
        coords={
            "plev": plev,
            "lat": np.linspace(-90, 90, 5),
            "lon": np.linspace(0, 360, 6),
            "time": np.arange(10),
        },
    )
    ds["plev"].attrs["units"] = "Pa"
    ds["plev"].attrs["long_name"] = "pressure"

    ds_with_bounds = add_vertical_bounds(ds)

    # Check that bounds were added
    assert "plev_bnds" in ds_with_bounds.data_vars

    # Check bounds shape
    assert ds_with_bounds["plev_bnds"].shape == (8, 2)

    # Check that bounds attribute was added to coordinate
    assert ds_with_bounds["plev"].attrs["bounds"] == "plev_bnds"

    # Check that bounds are continuous
    for i in range(len(plev) - 1):
        np.testing.assert_almost_equal(
            ds_with_bounds["plev_bnds"][i, 1].values,
            ds_with_bounds["plev_bnds"][i + 1, 0].values,
            decimal=10,
        )


def test_add_vertical_bounds_depth():
    """Test adding bounds for depth coordinates."""
    depth = [0, 10, 20, 50, 100, 200, 500, 1000]

    ds = xr.Dataset(
        {
            "thetao": (["time", "depth", "lat", "lon"], np.random.rand(10, 8, 5, 6)),
        },
        coords={
            "depth": depth,
            "lat": np.linspace(-90, 90, 5),
            "lon": np.linspace(0, 360, 6),
            "time": np.arange(10),
        },
    )
    ds["depth"].attrs["units"] = "m"
    ds["depth"].attrs["long_name"] = "depth"
    ds["depth"].attrs["positive"] = "down"

    ds_with_bounds = add_vertical_bounds(ds)

    # Check that bounds were added
    assert "depth_bnds" in ds_with_bounds.data_vars

    # Check bounds shape
    assert ds_with_bounds["depth_bnds"].shape == (8, 2)

    # Check that bounds attribute was added
    assert ds_with_bounds["depth"].attrs["bounds"] == "depth_bnds"


def test_add_vertical_bounds_height():
    """Test adding bounds for height coordinates."""
    height = [10, 50, 100, 200, 500, 1000, 2000]

    ds = xr.Dataset(
        {
            "ua": (["time", "height", "lat", "lon"], np.random.rand(10, 7, 5, 6)),
        },
        coords={
            "height": height,
            "lat": np.linspace(-90, 90, 5),
            "lon": np.linspace(0, 360, 6),
            "time": np.arange(10),
        },
    )
    ds["height"].attrs["units"] = "m"
    ds["height"].attrs["long_name"] = "height above surface"
    ds["height"].attrs["positive"] = "up"

    ds_with_bounds = add_vertical_bounds(ds)

    # Check that bounds were added
    assert "height_bnds" in ds_with_bounds.data_vars

    # Check bounds shape
    assert ds_with_bounds["height_bnds"].shape == (7, 2)


def test_add_vertical_bounds_plev19():
    """Test with CMIP6 plev19 coordinate name."""
    # Standard 19 pressure levels
    plev19 = [
        100000,
        92500,
        85000,
        70000,
        60000,
        50000,
        40000,
        30000,
        25000,
        20000,
        15000,
        10000,
        7000,
        5000,
        3000,
        2000,
        1000,
        500,
        100,
    ]

    ds = xr.Dataset(
        {
            "ta": (["time", "plev19", "lat", "lon"], np.random.rand(5, 19, 3, 4)),
        },
        coords={
            "plev19": plev19,
            "lat": np.linspace(-90, 90, 3),
            "lon": np.linspace(0, 360, 4),
            "time": np.arange(5),
        },
    )

    ds_with_bounds = add_vertical_bounds(ds)

    # Check that bounds were added
    assert "plev19_bnds" in ds_with_bounds.data_vars
    assert ds_with_bounds["plev19_bnds"].shape == (19, 2)


def test_add_vertical_bounds_already_exists():
    """Test that existing vertical bounds are not overwritten."""
    plev = [100000, 85000, 70000, 50000]

    ds = xr.Dataset(
        {
            "ta": (["plev", "lat", "lon"], np.random.rand(4, 5, 6)),
            "plev_bnds": (["plev", "bnds"], np.random.rand(4, 2)),
        },
        coords={
            "plev": plev,
            "lat": np.linspace(-90, 90, 5),
            "lon": np.linspace(0, 360, 6),
        },
    )

    original_bounds = ds["plev_bnds"].copy()
    ds_with_bounds = add_vertical_bounds(ds)

    # Check that original bounds were preserved
    xr.testing.assert_equal(ds_with_bounds["plev_bnds"], original_bounds)


def test_add_vertical_bounds_no_vertical_coord():
    """Test that function handles datasets without vertical coordinates gracefully."""
    ds = xr.Dataset(
        {
            "tas": (["time", "lat", "lon"], np.random.rand(10, 5, 6)),
        },
        coords={
            "lat": np.linspace(-90, 90, 5),
            "lon": np.linspace(0, 360, 6),
            "time": np.arange(10),
        },
    )

    # Should not raise an error
    ds_with_bounds = add_vertical_bounds(ds)

    # No vertical bounds should be added
    assert "plev_bnds" not in ds_with_bounds.data_vars
    assert "depth_bnds" not in ds_with_bounds.data_vars


def test_add_vertical_bounds_custom_coord_names():
    """Test adding bounds for custom vertical coordinate names."""
    ds = xr.Dataset(
        {
            "var": (["lev"], np.random.rand(5)),
        },
        coords={
            "lev": [1000, 850, 700, 500, 300],
        },
    )

    ds_with_bounds = add_vertical_bounds(ds, vertical_coord_names=["lev"])

    # Check that bounds were added
    assert "lev_bnds" in ds_with_bounds.data_vars
    assert ds_with_bounds["lev_bnds"].shape == (5, 2)


def test_add_vertical_bounds_irregular_levels():
    """Test bounds calculation for irregular vertical levels."""
    # Irregular pressure levels
    plev = [100000, 95000, 85000, 60000, 40000, 20000, 5000]

    ds = xr.Dataset(
        {
            "ta": (["plev"], np.random.rand(7)),
        },
        coords={
            "plev": plev,
        },
    )

    ds_with_bounds = add_vertical_bounds(ds)

    # Check that bounds are continuous (most important property)
    for i in range(len(plev) - 1):
        np.testing.assert_almost_equal(
            ds_with_bounds["plev_bnds"][i, 1].values,
            ds_with_bounds["plev_bnds"][i + 1, 0].values,
            decimal=10,
        )


def test_add_vertical_bounds_single_level():
    """Test bounds calculation with a single vertical level."""
    ds = xr.Dataset(
        {
            "ps": (["time", "lat", "lon"], np.random.rand(10, 5, 6)),
        },
        coords={
            "plev": [100000],  # Single surface level
            "lat": np.linspace(-90, 90, 5),
            "lon": np.linspace(0, 360, 6),
            "time": np.arange(10),
        },
    )

    ds_with_bounds = add_vertical_bounds(ds)

    # Should still produce bounds
    assert "plev_bnds" in ds_with_bounds.data_vars
    assert ds_with_bounds["plev_bnds"].shape == (1, 2)

    # Bounds should bracket the single value
    assert ds_with_bounds["plev_bnds"][0, 0].values < 100000
    assert ds_with_bounds["plev_bnds"][0, 1].values > 100000


def test_add_vertical_bounds_descending_pressure():
    """Test with descending pressure levels (high to low pressure)."""
    # Pressure decreasing with height (typical atmospheric convention)
    plev = [100000, 85000, 70000, 50000, 30000, 20000, 10000]

    ds = xr.Dataset(
        {
            "ta": (["plev"], np.random.rand(7)),
        },
        coords={
            "plev": plev,
        },
    )

    ds_with_bounds = add_vertical_bounds(ds)

    # Check that bounds were added
    assert "plev_bnds" in ds_with_bounds.data_vars

    # Check continuity
    for i in range(len(plev) - 1):
        np.testing.assert_almost_equal(
            ds_with_bounds["plev_bnds"][i, 1].values,
            ds_with_bounds["plev_bnds"][i + 1, 0].values,
            decimal=10,
        )


def test_add_vertical_bounds_ascending_depth():
    """Test with ascending depth levels (surface to deep ocean)."""
    # Depth increasing downward
    depth = [0, 5, 10, 25, 50, 100, 200, 500, 1000, 2000]

    ds = xr.Dataset(
        {
            "thetao": (["depth"], np.random.rand(10)),
        },
        coords={
            "depth": depth,
        },
    )

    ds_with_bounds = add_vertical_bounds(ds)

    # Check that bounds were added
    assert "depth_bnds" in ds_with_bounds.data_vars

    # Check continuity
    for i in range(len(depth) - 1):
        np.testing.assert_almost_equal(
            ds_with_bounds["depth_bnds"][i, 1].values,
            ds_with_bounds["depth_bnds"][i + 1, 0].values,
            decimal=10,
        )


def test_add_vertical_bounds_preserves_attributes():
    """Test that coordinate attributes are preserved."""
    plev = [100000, 85000, 70000, 50000]

    ds = xr.Dataset(
        {
            "ta": (["plev"], np.random.rand(4)),
        },
        coords={
            "plev": plev,
        },
    )
    ds["plev"].attrs["units"] = "Pa"
    ds["plev"].attrs["long_name"] = "pressure"
    ds["plev"].attrs["positive"] = "down"
    ds["plev"].attrs["axis"] = "Z"

    ds_with_bounds = add_vertical_bounds(ds)

    # Check that original attributes are preserved
    assert ds_with_bounds["plev"].attrs["units"] == "Pa"
    assert ds_with_bounds["plev"].attrs["long_name"] == "pressure"
    assert ds_with_bounds["plev"].attrs["positive"] == "down"
    assert ds_with_bounds["plev"].attrs["axis"] == "Z"

    # Check that bounds attribute was added
    assert ds_with_bounds["plev"].attrs["bounds"] == "plev_bnds"


def test_add_vertical_bounds_multiple_vertical_coords():
    """Test dataset with multiple vertical coordinates (edge case)."""
    ds = xr.Dataset(
        {
            "ta": (["plev"], np.random.rand(5)),
            "depth_var": (["depth"], np.random.rand(3)),
        },
        coords={
            "plev": [100000, 85000, 70000, 50000, 30000],
            "depth": [0, 100, 500],
        },
    )

    ds_with_bounds = add_vertical_bounds(ds)

    # Both should get bounds
    assert "plev_bnds" in ds_with_bounds.data_vars
    assert "depth_bnds" in ds_with_bounds.data_vars

    assert ds_with_bounds["plev_bnds"].shape == (5, 2)
    assert ds_with_bounds["depth_bnds"].shape == (3, 2)
