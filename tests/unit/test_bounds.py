"""Tests for coordinate bounds calculation."""

import numpy as np
import xarray as xr

from pycmor.std_lib.bounds import add_bounds_from_coords, add_bounds_to_grid, calculate_bounds_1d


def test_calculate_bounds_1d_regular_grid():
    """Test bounds calculation for a regular 1D grid."""
    lat = xr.DataArray(
        [10, 20, 30, 40, 50],
        dims=["lat"],
        attrs={"long_name": "latitude", "units": "degrees_north"},
    )

    bounds = calculate_bounds_1d(lat)

    # Check shape
    assert bounds.shape == (5, 2)

    # Check that bounds are continuous
    for i in range(len(lat) - 1):
        assert bounds[i, 1].values == bounds[i + 1, 0].values

    # Check midpoints are at coordinate values
    midpoints = (bounds[:, 0] + bounds[:, 1]) / 2
    np.testing.assert_array_almost_equal(midpoints.values, lat.values)


def test_calculate_bounds_1d_irregular_grid():
    """Test bounds calculation for an irregular 1D grid."""
    lat = xr.DataArray(
        [10, 15, 25, 45, 50],
        dims=["lat"],
        attrs={"long_name": "latitude", "units": "degrees_north"},
    )

    bounds = calculate_bounds_1d(lat)

    # Check shape
    assert bounds.shape == (5, 2)

    # Check that bounds are continuous (most important property)
    for i in range(len(lat) - 1):
        np.testing.assert_almost_equal(bounds[i, 1].values, bounds[i + 1, 0].values, decimal=10)

    # For irregular grids, coordinate values should be within their bounds
    for i in range(len(lat)):
        assert bounds[i, 0].values <= lat[i].values <= bounds[i, 1].values


def test_calculate_bounds_1d_longitude():
    """Test bounds calculation for longitude."""
    lon = xr.DataArray(
        [0, 90, 180, 270],
        dims=["lon"],
        attrs={"long_name": "longitude", "units": "degrees_east"},
    )

    bounds = calculate_bounds_1d(lon)

    # Check shape
    assert bounds.shape == (4, 2)

    # Check that bounds are continuous
    for i in range(len(lon) - 1):
        assert bounds[i, 1].values == bounds[i + 1, 0].values


def test_add_bounds_from_coords_simple():
    """Test adding bounds to a simple dataset."""
    ds = xr.Dataset(
        {
            "temp": (["time", "lat", "lon"], np.random.rand(10, 5, 6)),
        },
        coords={
            "lat": np.linspace(-90, 90, 5),
            "lon": np.linspace(0, 360, 6),
            "time": np.arange(10),
        },
    )

    ds_with_bounds = add_bounds_from_coords(ds)

    # Check that bounds were added
    assert "lat_bnds" in ds_with_bounds.data_vars
    assert "lon_bnds" in ds_with_bounds.data_vars

    # Check bounds shapes
    assert ds_with_bounds["lat_bnds"].shape == (5, 2)
    assert ds_with_bounds["lon_bnds"].shape == (6, 2)

    # Check that bounds attribute was added to coordinates
    assert ds_with_bounds["lat"].attrs["bounds"] == "lat_bnds"
    assert ds_with_bounds["lon"].attrs["bounds"] == "lon_bnds"


def test_add_bounds_from_coords_already_exists():
    """Test that existing bounds are not overwritten."""
    ds = xr.Dataset(
        {
            "temp": (["lat", "lon"], np.random.rand(5, 6)),
            "lat_bnds": (["lat", "bnds"], np.random.rand(5, 2)),
        },
        coords={
            "lat": np.linspace(-90, 90, 5),
            "lon": np.linspace(0, 360, 6),
        },
    )

    original_bounds = ds["lat_bnds"].copy()
    ds_with_bounds = add_bounds_from_coords(ds)

    # Check that original bounds were preserved
    xr.testing.assert_equal(ds_with_bounds["lat_bnds"], original_bounds)


def test_add_bounds_from_coords_custom_coord_names():
    """Test adding bounds for custom coordinate names."""
    ds = xr.Dataset(
        {
            "temp": (["y", "x"], np.random.rand(5, 6)),
        },
        coords={
            "y": np.linspace(-90, 90, 5),
            "x": np.linspace(0, 360, 6),
        },
    )

    ds_with_bounds = add_bounds_from_coords(ds, coord_names=["y", "x"])

    # Check that bounds were added
    assert "y_bnds" in ds_with_bounds.data_vars
    assert "x_bnds" in ds_with_bounds.data_vars


def test_add_bounds_from_coords_missing_coords():
    """Test that function handles missing coordinates gracefully."""
    ds = xr.Dataset(
        {
            "temp": (["time"], np.random.rand(10)),
        },
        coords={
            "time": np.arange(10),
        },
    )

    # Should not raise an error
    ds_with_bounds = add_bounds_from_coords(ds)

    # No bounds should be added since lat/lon don't exist
    assert "lat_bnds" not in ds_with_bounds.data_vars
    assert "lon_bnds" not in ds_with_bounds.data_vars


def test_add_bounds_to_grid_with_lat_lon():
    """Test add_bounds_to_grid with a grid containing lat/lon."""
    ncells = 100
    lat = np.linspace(-90, 90, ncells)
    lon = np.linspace(-180, 180, ncells)

    grid = xr.Dataset(
        data_vars=dict(
            cell_area=(["ncells"], np.ones(ncells)),
        ),
        coords=dict(
            lon=("ncells", lon),
            lat=("ncells", lat),
        ),
        attrs=dict(description="test grid"),
    )

    grid_with_bounds = add_bounds_to_grid(grid)

    # Check that bounds were added
    assert "lat_bnds" in grid_with_bounds.data_vars
    assert "lon_bnds" in grid_with_bounds.data_vars

    # Check bounds shapes
    assert grid_with_bounds["lat_bnds"].shape == (ncells, 2)
    assert grid_with_bounds["lon_bnds"].shape == (ncells, 2)


def test_add_bounds_to_grid_with_existing_bounds():
    """Test that add_bounds_to_grid preserves existing bounds."""
    ncells = 100
    lat = np.linspace(-90, 90, ncells)
    lon = np.linspace(-180, 180, ncells)
    lat_bnds = np.random.rand(ncells, 2)
    lon_bnds = np.random.rand(ncells, 2)

    grid = xr.Dataset(
        data_vars=dict(
            lat_bnds=(["ncells", "vertices"], lat_bnds),
            lon_bnds=(["ncells", "vertices"], lon_bnds),
            cell_area=(["ncells"], np.ones(ncells)),
        ),
        coords=dict(
            lon=("ncells", lon),
            lat=("ncells", lat),
        ),
        attrs=dict(description="test grid with bounds"),
    )

    grid_with_bounds = add_bounds_to_grid(grid)

    # Check that original bounds were preserved
    np.testing.assert_array_equal(grid_with_bounds["lat_bnds"].values, lat_bnds)
    np.testing.assert_array_equal(grid_with_bounds["lon_bnds"].values, lon_bnds)


def test_add_bounds_to_grid_no_coords():
    """Test add_bounds_to_grid with a grid without lat/lon."""
    grid = xr.Dataset(
        data_vars=dict(
            cell_area=(["ncells"], np.ones(100)),
        ),
        coords=dict(
            x=("ncells", np.arange(100)),
        ),
        attrs=dict(description="test grid without lat/lon"),
    )

    grid_with_bounds = add_bounds_to_grid(grid)

    # Should not have added any bounds
    assert "lat_bnds" not in grid_with_bounds.data_vars
    assert "lon_bnds" not in grid_with_bounds.data_vars


def test_bounds_continuity():
    """Test that calculated bounds are continuous (no gaps)."""
    lat = xr.DataArray(
        np.linspace(-90, 90, 10),
        dims=["lat"],
        attrs={"long_name": "latitude"},
    )

    bounds = calculate_bounds_1d(lat)

    # Check continuity: upper bound of cell i equals lower bound of cell i+1
    for i in range(len(lat) - 1):
        np.testing.assert_almost_equal(
            bounds[i, 1].values,
            bounds[i + 1, 0].values,
            decimal=10,
            err_msg=f"Discontinuity at index {i}",
        )


def test_bounds_coverage():
    """Test that bounds fully cover the coordinate range."""
    lat = xr.DataArray(
        np.linspace(-90, 90, 10),
        dims=["lat"],
        attrs={"long_name": "latitude"},
    )

    bounds = calculate_bounds_1d(lat)

    # The full range should be covered
    total_coverage = bounds[-1, 1].values - bounds[0, 0].values
    coord_range = lat[-1].values - lat[0].values

    # Coverage should be larger than coordinate range (includes extrapolation)
    assert total_coverage > coord_range


def test_bounds_with_single_point():
    """Test bounds calculation with a single coordinate point."""
    lat = xr.DataArray(
        [45.0],
        dims=["lat"],
        attrs={"long_name": "latitude"},
    )

    bounds = calculate_bounds_1d(lat)

    # Should still produce bounds (extrapolated)
    assert bounds.shape == (1, 2)
    assert bounds[0, 0].values < lat[0].values
    assert bounds[0, 1].values > lat[0].values


def test_bounds_with_two_points():
    """Test bounds calculation with two coordinate points."""
    lat = xr.DataArray(
        [30.0, 60.0],
        dims=["lat"],
        attrs={"long_name": "latitude"},
    )

    bounds = calculate_bounds_1d(lat)

    # Should produce bounds
    assert bounds.shape == (2, 2)

    # Midpoint should be at 45
    midpoint = (lat[0].values + lat[1].values) / 2
    assert bounds[0, 1].values == midpoint
    assert bounds[1, 0].values == midpoint
