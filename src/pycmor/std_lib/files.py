"""
This module contains functions for handling file-related operations in the pycmor package.
It includes functions for creating filepaths based on given rules and datasets, and for
saving the resulting datasets to the generated filepaths.



Table 2: Precision of time labels used in file names
|---------------+-------------------+-----------------------------------------------|
| Frequency     | Precision of time | Notes                                         |
|               | label             |                                               |
|---------------+-------------------+-----------------------------------------------|
| yr, dec,      | “yyyy”            | Label with the years recorded in the first    |
| yrPt          |                   | and last coordinate values.                   |
|---------------+-------------------+-----------------------------------------------|
| mon, monC     | “yyyyMM”          | For “mon”, label with the months recorded in  |
|               |                   | the first and last coordinate values; for     |
|               |                   | “monC” label with the first and last months   |
|               |                   | contributing to the climatology.              |
|---------------+-------------------+-----------------------------------------------|
| day           | “yyyyMMdd”        | Label with the days recorded in the first and |
|               |                   | last coordinate values.                       |
|---------------+-------------------+-----------------------------------------------|
| 6hr, 3hr,     | “yyyyMMddhhmm”    | Label 1hrCM files with the beginning of the   |
| 1hr,          |                   | first hour and the end of the last hour       |
| 1hrCM, 6hrPt, |                   | contributing to climatology (rounded to the   |
| 3hrPt,        |                   | nearest minute); for other frequencies in     |
| 1hrPt         |                   | this category, label with the first and last  |
|               |                   | time-coordinate values (rounded to the        |
|               |                   | nearest minute).                              |
|---------------+-------------------+-----------------------------------------------|
| subhrPt       | “yyyyMMddhhmmss”  | Label with the first and last time-coordinate |
|               |                   | values (rounded to the nearest second)        |
|---------------+-------------------+-----------------------------------------------|
| fx            | Omit time label   | This frequency applies to variables that are  |
|               |                   | independent of time (“fixed”).                |
|---------------+-------------------+-----------------------------------------------|

"""

from pathlib import Path

import pandas as pd
import xarray as xr
from xarray.core.utils import is_scalar

from ..core.logging import logger
from .dataset_helpers import get_time_label, has_time_axis


def _filename_time_range(ds, rule) -> str:
    """
    Determine the time range used in naming the file.

    Parameters
    ----------
    ds : xarray.Dataset
        The input dataset.
    rule : Rule
        The rule object containing information for generating the
        filepath.

    Returns
    -------
    str
        time_range in filepath.
    """
    if not has_time_axis(ds):
        return ""
    time_label = get_time_label(ds)
    if is_scalar(ds[time_label]):
        return ""
    start = pd.Timestamp(str(ds[time_label].data[0]))
    end = pd.Timestamp(str(ds[time_label].data[-1]))
    # frequency_str = rule.get("frequency_str")
    frequency_str = rule.data_request_variable.frequency
    if frequency_str in ("yr", "yrPt", "dec"):
        # For yearly data, the end year should be the year of the last timestamp
        # not the year after it (fix off-by-one error)
        return f"{start:%Y}-{end:%Y}"
    if frequency_str in ("mon", "monC", "monPt"):
        return f"{start:%Y%m}-{end:%Y%m}"
    if frequency_str == "day":
        return f"{start:%Y%m%d}-{end:%Y%m%d}"
    if frequency_str in ("6hr", "3hr", "1hr", "6hrPt", "3hrPt", "1hrPt", "1hrCM"):
        _start = start.round("1min")
        _end = end.round("1min")
        return f"{_start:%Y%m%d%H%M}-{_end:%Y%m%d%H%M}"
    if frequency_str == "subhrPt":
        _start = start.round("1s")
        _end = end.round("1s")
        return f"{_start:%Y%m%d%H%M%S}-{_end:%Y%m%d%H%M%S}"
    if frequency_str == "fx":
        return ""
    else:
        raise NotImplementedError(f"No implementation for {frequency_str} yet.")


def _sanitize_component(component):
    """
    Sanitize filename components to comply with CMIP6 specification.

    CMIP6 spec: All strings in filename use only: a-z, A-Z, 0-9, and hyphen (-)

    Parameters
    ----------
    component : str or Mock
        The component to sanitize

    Returns
    -------
    str
        The sanitized component
    """
    import re

    # Convert component to string if it's not already (handles Mock objects)
    if not isinstance(component, str):
        component = str(component)

    # Replace periods, underscores, and spaces with hyphens
    component = re.sub(r"[._\s]+", "-", component)
    # Remove any other forbidden characters
    component = re.sub(r"[^a-zA-Z0-9-]", "", component)
    # Remove multiple consecutive hyphens
    component = re.sub(r"-+", "-", component)
    # Remove leading/trailing hyphens
    component = component.strip("-")
    return component


def _check_climatology_suffix(ds):
    """
    Check if dataset represents climatology data and should have -clim suffix.

    Parameters
    ----------
    ds : xarray.Dataset
        The dataset to check

    Returns
    -------
    str
        "-clim" if climatology, empty string otherwise
    """
    if not has_time_axis(ds):
        return ""
    time_label = get_time_label(ds)
    if time_label and "climatology" in ds[time_label].attrs:
        return "-clim"
    return ""


def create_filepath(ds, rule):
    """
    Generate a filepath when given an xarray dataset and a rule.

    This function generates a filepath for the output file based on
    the given dataset and rule.  The filepath includes the name,
    table_id, institution, source_id, experiment_id, label, grid, and
    optionally the start and end time.

    Parameters
    ----------
    ds : xarray.Dataset
        The input dataset.
    rule : Rule
        The rule object containing information for generating the
        filepath.

    Returns
    -------
    str
        The generated filepath.

    Notes
    -----
    The rule object should have the following attributes:
    cmor_variable, data_request_variable, variant_label, source_id,
    experiment_id, output_directory, and optionally institution.
    """
    name = rule.cmor_variable
    table_id = rule.data_request_variable.table_header.table_id  # Omon
    label = rule.variant_label  # r1i1p1f1
    source_id = rule.source_id  # AWI-CM-1-1-MR
    experiment_id = rule.experiment_id  # historical
    out_dir = rule.output_directory  # where to save output files
    institution = getattr(rule, "institution", "AWI")
    grid = rule.grid_label  # grid_type
    time_range = _filename_time_range(ds, rule)

    # Sanitize components to comply with CMIP6 specification
    name = _sanitize_component(name)
    table_id = _sanitize_component(table_id)
    source_id = _sanitize_component(source_id)
    experiment_id = _sanitize_component(experiment_id)
    label = _sanitize_component(label)
    institution = _sanitize_component(institution)
    grid = _sanitize_component(grid)

    # Check for climatology suffix
    clim_suffix = _check_climatology_suffix(ds)

    # check if output sub-directory is needed
    enable_output_subdirs = rule._pycmor_cfg.get("enable_output_subdirs", False)
    if enable_output_subdirs:
        subdirs = rule.ga.subdir_path()
        out_dir = f"{out_dir}/{subdirs}"

    # Build filename according to CMIP6 spec
    # For fx (time-invariant) fields, omit time_range
    frequency_str = rule.data_request_variable.frequency
    if frequency_str == "fx" or not time_range:
        filepath = (
            f"{out_dir}/{name}_{table_id}_{institution}-{source_id}_"
            f"{experiment_id}_{label}_{grid}{clim_suffix}.nc"
        )
    else:
        filepath = (
            f"{out_dir}/{name}_{table_id}_{institution}-{source_id}_"
            f"{experiment_id}_{label}_{grid}_{time_range}{clim_suffix}.nc"
        )

    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    return filepath


def get_offset(rule):
    """convert offset defined on the rule to a timedelta."""
    offset = getattr(rule, "adjust_timestamp", None)
    if offset is not None:
        offset_presets = {
            "first": 0,
            "start": 0,
            "last": 1,
            "end": 1,
            "mid": 0.5,
            "middle": 0.5,
        }
        offset = offset_presets.get(offset, offset)
        try:
            offset = float(offset)
        except (ValueError, TypeError):
            # expect offset to a literal string. Example: "14D"
            offset = pd.Timedelta(offset)
        else:
            # offset is a float value scaled by the approx_interval
            approx_interval = float(
                rule.data_request_variable.table_header.approx_interval
            )
            dt = pd.Timedelta(approx_interval, unit="d")
            offset = dt * float(offset)
    return offset


def file_timespan_tail(rule):
    """Grab the last timestamp in each file and return them as a list.
    Also account for offset (if any) defined on the rule"""
    times = []
    try:
        options = {"decode_times": xr.coders.CFDatetimeCoder(use_cftime=True)}
    except AttributeError:
        # in python3.9, xarray does not have coders
        options = {"use_cftime": True}
    for _input in rule.inputs:
        for f in sorted(_input.files):
            ds = xr.open_dataset(str(f), **options)
            time_label = get_time_label(ds)
            if time_label:
                times.append(ds[time_label].values[-1])
    offset = get_offset(rule)
    if offset is not None:
        times = xr.CFTimeIndex(times) + offset
        times = list(times.values)
    return times


def split_data_timespan(ds, rule):
    """
    Splits the dataset into chunks based on the time axis as defined in the source files.

    Parameters
    ----------
    ds : xarray.Dataset
        The dataset to split.
    rule : Rule
        The rule object containing information for generating the
        filepath.

    Returns
    -------
    list
        A list of datasets, each containing a chunk of the original dataset.
    """
    time_cuts = file_timespan_tail(rule)
    ncuts = len(time_cuts)
    time_label = get_time_label(ds)
    if not time_label:
        return [ds]
    resampled_times = ds[time_label].values
    ref = pd.DataFrame(resampled_times, columns=["dateindex"])
    ref["mark"] = ncuts
    for ind, timecut in enumerate(reversed(time_cuts), start=1):
        ref.loc[ref.dateindex < timecut, "mark"] = ncuts - ind
    result = []
    for _, grp in ref.groupby("mark"):
        result.append((grp.dateindex.iloc[0], grp.dateindex.iloc[-1]))
    data_chunks = []
    for timespan in result:
        da = ds.sel({time_label: slice(timespan[0], timespan[-1])})
        data_chunks.append(da)
    if not data_chunks:
        data_chunks.append(ds)
    return data_chunks


def _save_dataset_with_native_timespan(
    da,
    rule,
    time_label,
    time_encoding,
    **extra_kwargs,
):
    paths = []
    datasets = split_data_timespan(da, rule)

    # Ensure time encoding is properly applied to each dataset
    for i, ds in enumerate(datasets):
        if time_label in ds.variables:
            # If we have custom units and calendar, use xarray's CF encoding function
            # Only apply if both are actual strings (not Mock objects or None)
            if (
                "units" in time_encoding
                and "calendar" in time_encoding
                and isinstance(time_encoding["units"], str)
                and isinstance(time_encoding["calendar"], str)
            ):
                from xarray.coding.times import encode_cf_datetime

                # Get the current time values (should be datetime objects)
                time_values = ds[time_label].values

                # Use xarray's CF encoding function to encode the datetime values
                encoded_values, _, _ = encode_cf_datetime(
                    time_values,
                    units=time_encoding["units"],
                    calendar=time_encoding["calendar"],
                )

                # Replace the time coordinate with the encoded values
                ds[time_label] = xr.DataArray(
                    encoded_values, dims=[time_label], attrs=ds[time_label].attrs.copy()
                )

            # Set time units and calendar as attributes for consistency
            # Only set if they are actual strings (not Mock objects)
            # But avoid setting calendar attribute if it conflicts with encoding
            if "units" in time_encoding and isinstance(time_encoding["units"], str):
                ds[time_label].attrs["units"] = time_encoding["units"]
            # Only set calendar attribute if we have custom calendar (not default "standard")
            if (
                "calendar" in time_encoding
                and isinstance(time_encoding["calendar"], str)
                and time_encoding["calendar"] != "standard"
            ):
                ds[time_label].attrs["calendar"] = time_encoding["calendar"]

            # Also set the encoding directly on the variable
            ds[time_label].encoding.update(time_encoding)

        paths.append(create_filepath(ds, rule))

    # Don't pass encoding to save_mfdataset since we've already encoded the time values
    # and set the attributes - let xarray use what we've provided
    return xr.save_mfdataset(
        datasets,
        paths,
        **extra_kwargs,
    )


def save_dataset(da: xr.DataArray, rule):
    """
    Save dataset to one or more files.

    Parameters
    ----------
    da : xr.DataArray
        The dataset to be saved.
    rule : Rule
        The rule object containing information for generating the
        filepath.

    Returns
    -------
    None

    Notes
    -----
    If the dataset does not have a time axis, or if the time axis is a scalar,
    this function will save the dataset to a single file.  Otherwise, it will
    split the dataset into chunks based on the time axis and save each chunk
    to a separate file.

    The filepath will be generated based on the rule object and the time range
    of the dataset.  The filepath will include the name, table_id, institution,
    source_id, experiment_id, label, grid, and optionally the start and end time.

    If the dataset needs resampling (i.e., the time axis does not align with the
    time frequency specified in the rule object), this function will split the
    dataset into chunks based on the time axis and resample each chunk to the
    specified frequency.  The resampled chunks will then be saved to separate
    files.

    NOTE: prior to calling this function, call dask.compute() method,
    otherwise tasks will progress very slow.
    """
    time_dtype = rule._pycmor_cfg("xarray_time_dtype")
    time_unlimited = rule._pycmor_cfg("xarray_time_unlimited")
    extra_kwargs = {}
    if time_unlimited:
        extra_kwargs.update({"unlimited_dims": ["time"]})
    time_encoding = {"dtype": time_dtype}
    time_encoding = {k: v for k, v in time_encoding.items() if v is not None}
    # Allow user to define time units and calendar in the rule object
    # Martina has a usecase where she wants to set time units to
    # `days since 1850-01-01` and calendar to `proleptic_gregorian` for
    # historical experiments. See issue #215
    time_units = getattr(rule, "time_units", None)
    time_calendar = getattr(rule, "time_calendar", None)
    # Only add to encoding if they are actual strings (not Mock objects or None)
    if time_units is not None and isinstance(time_units, str):
        time_encoding["units"] = time_units
    if time_calendar is not None and isinstance(time_calendar, str):
        time_encoding["calendar"] = time_calendar
    # Set default calendar if none is specified
    if time_encoding.get("calendar") is None:
        time_encoding["calendar"] = "standard"
    if not has_time_axis(da):
        filepath = create_filepath(da, rule)
        return da.to_netcdf(
            filepath,
            mode="w",
            format="NETCDF4",
        )
    time_label = get_time_label(da)
    if is_scalar(da[time_label]):
        filepath = create_filepath(da, rule)
        return da.to_netcdf(
            filepath,
            mode="w",
            format="NETCDF4",
            encoding={time_label: time_encoding},
            **extra_kwargs,
        )
    if isinstance(da, xr.DataArray):
        da = da.to_dataset()

    # Set time variable attributes
    if rule._pycmor_cfg("xarray_time_set_standard_name"):
        da[time_label].attrs["standard_name"] = "time"
    if rule._pycmor_cfg("xarray_time_set_long_name"):
        da[time_label].attrs["long_name"] = "time"
    if rule._pycmor_cfg("xarray_time_enable_set_axis"):
        time_axis_str = rule._pycmor_cfg("xarray_time_taxis_str")
        da[time_label].attrs["axis"] = time_axis_str
    if rule._pycmor_cfg("xarray_time_remove_fill_value_attr"):
        time_encoding["_FillValue"] = None

    # If we have custom units and calendar, use xarray's CF encoding function
    # Only apply if both are actual strings (not Mock objects or None)
    if (
        "units" in time_encoding
        and "calendar" in time_encoding
        and isinstance(time_encoding["units"], str)
        and isinstance(time_encoding["calendar"], str)
    ):
        from xarray.coding.times import encode_cf_datetime

        # Convert the dataset to Dataset if it's a DataArray
        if isinstance(da, xr.DataArray):
            da = da.to_dataset()

        # Get the current time values (should be datetime objects)
        time_values = da[time_label].values

        # Use xarray's CF encoding function to encode the datetime values
        encoded_values, _, _ = encode_cf_datetime(
            time_values,
            units=time_encoding["units"],
            calendar=time_encoding["calendar"],
        )

        # Replace the time coordinate with the encoded values
        da[time_label] = xr.DataArray(
            encoded_values, dims=[time_label], attrs=da[time_label].attrs.copy()
        )

    # Set time units and calendar as attributes (for metadata)
    # Only set if they are actual strings (not Mock objects)
    # But avoid setting calendar attribute if it conflicts with encoding
    if "units" in time_encoding and isinstance(time_encoding["units"], str):
        da[time_label].attrs["units"] = time_encoding["units"]
    # Only set calendar attribute if we have custom calendar (not default "standard")
    if (
        "calendar" in time_encoding
        and isinstance(time_encoding["calendar"], str)
        and time_encoding["calendar"] != "standard"
    ):
        da[time_label].attrs["calendar"] = time_encoding["calendar"]

    # Ensure the encoding is set on the time variable itself
    if isinstance(da, xr.DataArray):
        da = da.to_dataset()
    da[time_label].encoding.update(time_encoding)

    if not has_time_axis(da):
        filepath = create_filepath(da, rule)
        return da.to_netcdf(
            filepath,
            mode="w",
            format="NETCDF4",
            **extra_kwargs,
        )

    default_file_timespan = rule._pycmor_cfg("file_timespan")
    file_timespan = getattr(rule, "file_timespan", default_file_timespan)
    if file_timespan == "file_native":
        return _save_dataset_with_native_timespan(
            da,
            rule,
            time_label,
            time_encoding,
            **extra_kwargs,
        )
    else:
        file_timespan_as_offset = pd.tseries.frequencies.to_offset(file_timespan)
        file_timespan_as_dt = (
            pd.Timestamp.now() + file_timespan_as_offset - pd.Timestamp.now()
        )
        approx_interval = float(rule.data_request_variable.table_header.approx_interval)
        dt = pd.Timedelta(approx_interval, unit="d")
        if file_timespan_as_dt < dt:
            logger.warning(
                f"file_timespan {file_timespan_as_dt} is smaller than approx_interval {dt}"
                "falling back to timespan as defined in the source file"
            )
            return _save_dataset_with_native_timespan(
                da,
                rule,
                time_label,
                time_encoding,
                **extra_kwargs,
            )
        else:
            groups = da.resample(time=file_timespan)
            paths = []
            datasets = []
            for group_name, group_ds in groups:
                paths.append(create_filepath(group_ds, rule))
                datasets.append(group_ds)
            return xr.save_mfdataset(
                datasets,
                paths,
                encoding={time_label: time_encoding},
                **extra_kwargs,
            )
