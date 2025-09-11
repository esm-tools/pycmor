import tempfile
from pathlib import Path
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pycmor.std_lib.files import (
    file_timespan_tail,
    get_offset,
    save_dataset,
    split_data_timespan,
)


@pytest.mark.parametrize(
    "adjust_timestamp,expected",
    [
        ("mid", pd.Timedelta(15, unit="d")),
        ("last", pd.Timedelta(30, unit="d")),
        ("first", pd.Timedelta(0, unit="d")),
        ("start", pd.Timedelta(0, unit="d")),
        ("end", pd.Timedelta(30, unit="d")),
        ("14D", pd.Timedelta(14, unit="d")),
        ("0.5", pd.Timedelta(15, unit="d")),
        ("0.25", pd.Timedelta(7.5, unit="d")),
        ("0.75", pd.Timedelta(22.5, unit="d")),
        (None, None),
    ],
)
def test_get_offset(adjust_timestamp, expected):
    rule = Mock()
    rule.data_request_variable.table_header.approx_interval = "30"
    rule.adjust_timestamp = adjust_timestamp
    assert get_offset(rule) == expected


def test_file_timespan_tail_no_offset():
    rule = Mock()
    rule.data_request_variable.table_header.approx_interval = "30"
    rule.adjust_timestamp = None
    timeindex = xr.cftime_range("2001", periods=120, freq="MS", calendar="standard")
    air = xr.Dataset(
        data_vars=dict(
            air=(("time", "ncells"), np.random.rand(timeindex.size, 10)),
        ),
        coords=dict(
            time=timeindex,
            ncells=np.arange(10),
        ),
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        files = []
        for group_name, group in air.groupby("time.year"):
            filename = f"{tmpdir}/air_{group_name}.nc"
            files.append(filename)
            group.to_netcdf(filename)
        rule.inputs = [Mock(files=files)]
        tails = file_timespan_tail(rule)
    timestamps = []
    for group_name, group in air.groupby("time.year"):
        timestamps.append(group.time.values[-1])
    assert tails == timestamps


def test_file_timespan_tail_with_offset():
    rule = Mock()
    rule.data_request_variable.table_header.approx_interval = "30"
    rule.adjust_timestamp = "mid"
    timeindex = xr.cftime_range("2001", periods=120, freq="MS", calendar="standard")
    air = xr.Dataset(
        data_vars=dict(
            air=(("time", "ncells"), np.random.rand(timeindex.size, 10)),
        ),
        coords=dict(
            time=timeindex,
            ncells=np.arange(10),
        ),
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        files = []
        for group_name, group in air.groupby("time.year"):
            filename = f"{tmpdir}/air_{group_name}.nc"
            files.append(filename)
            group.to_netcdf(filename)
        rule.inputs = [Mock(files=files)]
        tails = file_timespan_tail(rule)
    timestamps = []
    offset = get_offset(rule)
    for group_name, group in air.groupby("time.year"):
        timestamps.append(group.time.values[-1] + offset)
    assert set(tails) == set(timestamps)


def test_split_data_timespan():
    rule = Mock()
    rule.data_request_variable.table_header.approx_interval = "30"
    rule.adjust_timestamp = "mid"
    # creating 2 years data with daily frequency
    timeindex = xr.cftime_range("2000", periods=365 * 2, freq="D", calendar="standard")
    air = xr.Dataset(
        data_vars=dict(
            air=(("time", "ncells"), np.random.rand(timeindex.size, 10)),
        ),
        coords=dict(
            time=timeindex,
            ncells=np.arange(10),
        ),
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        files = []
        # split time into yearly chunks
        for group_name, group in air.resample(time="YS"):
            filename = f"{tmpdir}/air_{group_name}.nc"
            files.append(filename)
            group.to_netcdf(filename)
        rule.inputs = [Mock(files=files)]
        # resample to monthly frequency and calculate mean (simulate cmorize)
        ds = air.resample(time="MS").mean(dim="time")
        offset = get_offset(rule)
        if offset is not None:
            ds["time"] = ds.time + offset
        chunks = split_data_timespan(ds, rule)
        # check if chunks are in the correct time range
        tails = file_timespan_tail(rule)
        for chunk, timestamp in zip(chunks, tails):
            assert all(bool(ts < timestamp) for ts in chunk.time.values)
        # another approch is to check the year in chunk. It needs to unique and single value.
        for chunk in chunks:
            assert len(set(chunk.time.dt.year.values)) == 1
            assert len(set(chunk.time.dt.month.values)) == 12


def test_save_dataset(mocker):
    # Create a mock for _pycmor_cfg that returns required values
    mock_cfg = mocker.Mock()
    configs = {
        "xarray_time_dtype": "float64",
        "xarray_time_unlimited": False,
        "xarray_time_set_standard_name": False,
        "xarray_time_set_long_name": False,
        "xarray_time_enable_set_axis": True,  # Enable axis setting
        "xarray_time_taxis_str": "T",
        "xarray_time_remove_fill_value_attr": False,
        "file_timespan": "6MS",  # Match the file_timespan set in the test
        "enable_output_subdirs": False,  # Add this to prevent subdirectory creation
    }
    mock_cfg.side_effect = lambda key, default=None: configs.get(key, default)
    mock_cfg.get = configs.get

    # Create a mock for the table header
    table_header = Mock()
    table_header.table_id = "Omon"
    table_header.approx_interval = "30"

    # Create a mock for the data request variable
    data_request_variable = Mock()
    data_request_variable.table_header = table_header
    data_request_variable.frequency = "mon"

    # Create a mock for the ga attribute
    ga_mock = Mock()
    ga_mock.subdir_path.return_value = (
        ""  # Return empty string to match the test's expectations
    )

    rule = Mock()
    rule.ga = ga_mock
    rule.data_request_variable = data_request_variable
    rule._pycmor_cfg = mock_cfg  # Use the mock object
    rule.cmor_variable = "fgco2"
    rule.data_request_variable.table_header.table_id = "Omon"
    rule.variant_label = "r1i1p1f1"
    rule.source_id = "AWI-CM-1-1-MR"
    rule.experiment_id = "historical"
    rule.institution = "AWI"
    rule.adjust_timestamp = None
    rule.file_timespan = "6MS"
    rule.model_variable = "air"  # Add model_variable to match the data variable name
    # creating 2 years data with daily frequency
    timeindex = xr.cftime_range("2000", periods=365 * 2, freq="D", calendar="standard")
    air = xr.Dataset(
        data_vars=dict(
            air=(("time", "ncells"), np.random.rand(timeindex.size, 10)),
        ),
        coords=dict(
            time=timeindex,
            ncells=np.arange(10),
        ),
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        rule.output_directory = tmpdir
        files = []
        # split time into yearly chunks
        for group_name, group in air.resample(time="YS"):
            filename = f"{tmpdir}/air_{group_name}.nc"
            files.append(filename)
            group.to_netcdf(filename)
        rule.inputs = [Mock(files=files)]
        # resample to monthly frequency and calculate mean (simulate cmorize)
        ds = air.resample(time="MS").mean(dim="time")
        offset = get_offset(rule)
        if offset is not None:
            ds["time"] = ds.time + offset

        # Debug: Print the rule configuration
        print("\nRule configuration:")
        print(f"  output_directory: {rule.output_directory}")
        print(f"  file_timespan: {rule.file_timespan}")
        print(f"  cmor_variable: {rule.cmor_variable}")
        print(f"  model_variable: {rule.model_variable}")
        print(f"  _pymor_cfg: {rule._pymor_cfg}")

        # Debug: Print the dataset info
        print("\nDataset info:")
        print(f"  Variables: {list(ds.data_vars)}")
        print(f"  Time range: {ds.time.values[0]} to {ds.time.values[-1]}")

        # Call the function under test
        save_dataset(ds, rule)

        # Debug: List all files in the output directory
        print("\nFiles in output directory:")
        for f in Path(tmpdir).glob("*"):
            print(f"  - {f.name}")

        nfiles = len(list(Path(tmpdir).glob("fgco2*.nc")))
        print(f"\nNumber of fgco2*.nc files found: {nfiles}")

        # file-timespan is 6MS, so 2 years data should be split into 4 files
        assert nfiles == 4, f"Expected 4 files, found {nfiles}"
