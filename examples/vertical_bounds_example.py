"""
Example: Adding Vertical Bounds to Climate Data
================================================

This example demonstrates how to use the add_vertical_bounds function
to automatically generate bounds for vertical coordinates (pressure levels,
depth, height) in climate model output, similar to CDO's genlevelbounds.

This is particularly useful for CMIP compliance where vertical bounds
are required for proper data interpretation.
"""

import numpy as np
import xarray as xr

from pycmor.std_lib.bounds import add_vertical_bounds


def example_pressure_levels():
    """Example with atmospheric pressure levels."""
    print("=" * 60)
    print("Example 1: Atmospheric Pressure Levels")
    print("=" * 60)

    # Create a dataset with standard CMIP6 pressure levels (in Pa)
    plev = [100000, 92500, 85000, 70000, 60000, 50000, 40000, 30000]

    ds = xr.Dataset(
        {
            "ta": (["time", "plev", "lat", "lon"], np.random.rand(5, 8, 3, 4)),
        },
        coords={
            "plev": plev,
            "lat": np.linspace(-90, 90, 3),
            "lon": np.linspace(0, 360, 4),
            "time": np.arange(5),
        },
    )
    ds["plev"].attrs["units"] = "Pa"
    ds["plev"].attrs["long_name"] = "pressure"
    ds["plev"].attrs["axis"] = "Z"

    print("\nOriginal dataset:")
    print(ds)
    print("\nPressure levels:", ds["plev"].values)

    # Add vertical bounds
    ds_with_bounds = add_vertical_bounds(ds)

    print("\nDataset with vertical bounds:")
    print(ds_with_bounds)
    print("\nPressure level bounds (first 3 levels):")
    print(ds_with_bounds["plev_bnds"][:3].values)
    print(
        "\nBounds attribute added to plev:", ds_with_bounds["plev"].attrs.get("bounds")
    )


def example_ocean_depth():
    """Example with ocean depth levels."""
    print("\n" + "=" * 60)
    print("Example 2: Ocean Depth Levels")
    print("=" * 60)

    # Create a dataset with ocean depth levels (in meters)
    depth = [0, 10, 20, 50, 100, 200, 500, 1000]

    ds = xr.Dataset(
        {
            "thetao": (["time", "depth", "lat", "lon"], np.random.rand(5, 8, 3, 4)),
        },
        coords={
            "depth": depth,
            "lat": np.linspace(-90, 90, 3),
            "lon": np.linspace(0, 360, 4),
            "time": np.arange(5),
        },
    )
    ds["depth"].attrs["units"] = "m"
    ds["depth"].attrs["long_name"] = "depth"
    ds["depth"].attrs["positive"] = "down"
    ds["depth"].attrs["axis"] = "Z"

    print("\nOriginal dataset:")
    print(ds)
    print("\nDepth levels:", ds["depth"].values)

    # Add vertical bounds
    ds_with_bounds = add_vertical_bounds(ds)

    print("\nDataset with vertical bounds:")
    print(ds_with_bounds)
    print("\nDepth bounds (first 3 levels):")
    print(ds_with_bounds["depth_bnds"][:3].values)


def example_irregular_levels():
    """Example with irregular vertical levels."""
    print("\n" + "=" * 60)
    print("Example 3: Irregular Pressure Levels")
    print("=" * 60)

    # Create a dataset with irregular pressure levels
    plev = [100000, 95000, 85000, 60000, 40000, 20000, 5000]

    ds = xr.Dataset(
        {
            "ta": (["plev", "lat", "lon"], np.random.rand(7, 3, 4)),
        },
        coords={
            "plev": plev,
            "lat": np.linspace(-90, 90, 3),
            "lon": np.linspace(0, 360, 4),
        },
    )
    ds["plev"].attrs["units"] = "Pa"

    print("\nOriginal irregular pressure levels:", ds["plev"].values)

    # Add vertical bounds
    ds_with_bounds = add_vertical_bounds(ds)

    print("\nCalculated bounds:")
    for i, (level, bounds) in enumerate(
        zip(ds_with_bounds["plev"].values, ds_with_bounds["plev_bnds"].values)
    ):
        print(
            f"  Level {i}: {level:8.0f} Pa  →  [{bounds[0]:8.0f}, {bounds[1]:8.0f}] Pa"
        )

    # Verify continuity
    print("\nVerifying bounds continuity:")
    for i in range(len(plev) - 1):
        upper = ds_with_bounds["plev_bnds"][i, 1].values
        lower_next = ds_with_bounds["plev_bnds"][i + 1, 0].values
        print(
            f"  Level {i} upper bound = Level {i+1} lower bound: {upper:.1f} == {lower_next:.1f}"
        )


def example_usage_in_pipeline():
    """Example of using add_vertical_bounds in a processing pipeline."""
    print("\n" + "=" * 60)
    print("Example 4: Usage in a Processing Pipeline")
    print("=" * 60)

    # This shows how you would use it in a pycmor pipeline
    # In a real pipeline, you would use it as a step function

    from pycmor.core.rule import Rule

    # Create sample data
    plev = [100000, 85000, 70000, 50000, 30000]
    ds = xr.Dataset(
        {
            "ta": (["plev"], np.random.rand(5)),
        },
        coords={
            "plev": plev,
        },
    )

    print("\nBefore adding bounds:")
    print(f"  Variables: {list(ds.data_vars)}")
    print(f"  Coordinates: {list(ds.coords)}")

    # In a pipeline, you would import and use:
    from pycmor.std_lib import add_vertical_bounds as pipeline_add_vertical_bounds

    # Create a dummy rule (in real usage, this comes from your pipeline config)
    rule = Rule(
        cmor_variable="ta",
        model_variable="ta",
        data_request_variable=None,
    )

    # Apply the function
    ds_processed = pipeline_add_vertical_bounds(ds, rule)

    print("\nAfter adding bounds:")
    print(f"  Variables: {list(ds_processed.data_vars)}")
    print(f"  Coordinates: {list(ds_processed.coords)}")
    print(f"  plev bounds attribute: {ds_processed['plev'].attrs.get('bounds')}")


if __name__ == "__main__":
    example_pressure_levels()
    example_ocean_depth()
    example_irregular_levels()
    example_usage_in_pipeline()

    print("\n" + "=" * 60)
    print("All examples completed successfully!")
    print("=" * 60)
