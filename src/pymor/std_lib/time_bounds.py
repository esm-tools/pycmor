import numpy as np
import xarray as xr

from ..core.logging import logger
from ..core.rule import Rule
from .dataset_helpers import get_time_label


def time_bounds(ds: xr.Dataset, rule: Rule) -> xr.Dataset:
    """
    Sets time bounds for a variable based on time method and approx_interval.

    Time bounds are set according to the following logic:
    - For instantaneous time method: bounds are same as time values (delta time = 0)
    - For mean time method: bounds span the averaging interval (delta time > 0)
    - Uses approx_interval from rule to determine appropriate bounds

    Parameters
    ----------
    ds : xr.Dataset
        The input dataset.
    rule : Rule
        Rule object containing time bounds file attribute and approx_interval

    Returns
    -------
    xr.Dataset
        The output dataset with the time bounds information.
    """
    # Get dataset name for logging
    dataset_name = ds.attrs.get("name", "unnamed_dataset")

    # Log header with markdown style
    logger.info(f"[Time bounds] {dataset_name}")
    logger.info(f"  → is dataset:   {'✅' if isinstance(ds, xr.Dataset) else '❌'}")

    # Check if input is a dataset
    if not isinstance(ds, xr.Dataset):
        logger.error("  ❌ The input is not a dataset.")
        raise ValueError("The input is not a dataset.")

    # Get time label and check if it exists
    time_label = get_time_label(ds)
    if time_label is None:
        logger.error("  ❌ The dataset does not contain a valid time coordinate.")
        raise ValueError("The dataset does not contain a valid time coordinate.")

    bounds_dim_label = "bnds"
    time_bounds_label = f"{time_label}_{bounds_dim_label}"

    # Log time and bounds info
    logger.info(f"  → time label  : {time_label}")
    logger.info(f"  → bounds label: {bounds_dim_label}")

    # Get approx_interval from rule (in days)
    approx_interval = getattr(rule, "approx_interval", None)
    logger.info(f"  → approx_interval: {approx_interval} days")

    # Get time method from rule or dataset attributes
    time_method = getattr(rule, "time_method", None) or ds.attrs.get(
        "time_method", "mean"
    )
    logger.info(f"  → time method : {time_method}")

    # Check if time bounds already exist
    has_time_bounds = time_bounds_label in ds.variables
    logger.info(f"  → has time bounds: {'✅' if has_time_bounds else '❌'}")

    if has_time_bounds:
        logger.info(f"  → using existing bounds: {time_bounds_label}")
        return ds

    # Validate time method
    if time_method not in ["mean", "instantaneous", "climatology"]:
        logger.warning(
            f"  ⚠️  Unknown time method '{time_method}', defaulting to 'mean'"
        )
        time_method = "mean"

    # Only create bounds for mean and instantaneous methods
    if time_method == "climatology":
        logger.info("  → skipping bounds creation for climatology data")
        return ds

    # Get time coordinate
    time_var = ds[time_label]
    time_values = time_var.values

    # Check if we have enough time points
    if len(time_values) < 1:
        error_msg = "Cannot create time bounds: no time points found"
        logger.error(f"  ❌ {error_msg}")
        raise ValueError(error_msg)

    # Log time values info
    logger.info(
        f"  → time values  : {len(time_values)} points from {time_values[0]} to {time_values[-1]}"
    )

    # Handle instantaneous time method
    if time_method == "instantaneous":
        logger.info("  → creating instantaneous bounds (delta time = 0)")
        # For instantaneous data, bounds are the same as time values
        bounds_data = np.column_stack([time_values, time_values])

    # Handle mean time method
    else:  # time_method == 'mean'
        logger.info("  → creating mean bounds (delta time > 0)")

        if len(time_values) < 2:
            error_msg = "Cannot create mean time bounds: need at least 2 time points"
            logger.error(f"  ❌ {error_msg}")
            raise ValueError(error_msg)

        # Calculate data frequency in days
        time_diff_seconds = np.median(
            np.diff(time_values.astype("datetime64[s]").astype(float))
        )
        data_freq_days = time_diff_seconds / (24 * 3600)  # Convert to days
        logger.info(f"  → data frequency: {data_freq_days:.2f} days")

        # Determine bounds based on approx_interval and data frequency
        if approx_interval is not None and abs(data_freq_days - approx_interval) < 0.1:
            logger.info(
                "  → data frequency matches approx_interval, using interval-based bounds"
            )

            # For monthly data (approx_interval ~30 days)
            if 28 <= approx_interval <= 32:
                logger.info("  → detected monthly data, using month-start bounds")
                bounds_data = _create_monthly_bounds(time_values)
            else:
                # For other regular intervals, use consecutive time points
                logger.info("  → using consecutive time point bounds")
                time_values_extended = np.append(
                    time_values,
                    time_values[-1] + np.timedelta64(int(data_freq_days), "D"),
                )
                bounds_data = np.column_stack(
                    [time_values_extended[:-1], time_values_extended[1:]]
                )
        else:
            # Default behavior: use consecutive time points
            logger.info("  → using default consecutive time point bounds")
            time_diff = np.median(np.diff(time_values))
            time_values_extended = np.append(time_values, time_values[-1] + time_diff)
            bounds_data = np.column_stack(
                [time_values_extended[:-1], time_values_extended[1:]]
            )

    # Create the bounds DataArray
    bounds = xr.DataArray(
        data=bounds_data,
        dims=(time_label, bounds_dim_label),
        coords={time_label: time_values, bounds_dim_label: [0, 1]},
        attrs={
            "long_name": f"time bounds for {time_label}",
            "comment": f"Generated by pymorize: {time_method} time bounds",
        },
    )

    # Add bounds to dataset as a coordinate variable
    ds = ds.assign_coords({time_bounds_label: bounds})

    # Add bounds attribute to time variable
    if "bounds" not in time_var.attrs:
        ds[time_label].attrs["bounds"] = time_bounds_label

    # Log success message with bounds info
    logger.info(f"  → set time bounds: {time_bounds_label}{bounds.shape}")
    logger.info(
        f"  → bounds range   : {bounds.values[0][0]} to {bounds.values[-1][-1]}"
    )
    logger.info("-" * 50)
    return ds


def _create_monthly_bounds(time_values):
    """
    Create monthly bounds where bounds are (month_start, next_month_start)
    regardless of where the time points are within the month.
    """
    import pandas as pd

    bounds_data = []

    for i, time_val in enumerate(time_values):
        # Convert to pandas timestamp for easier month manipulation
        ts = pd.Timestamp(time_val)

        # Get start of current month
        month_start = ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        # Get start of next month
        if ts.month == 12:
            next_month_start = month_start.replace(year=ts.year + 1, month=1)
        else:
            next_month_start = month_start.replace(month=ts.month + 1)

        bounds_data.append([month_start.to_numpy(), next_month_start.to_numpy()])

    return np.array(bounds_data)
