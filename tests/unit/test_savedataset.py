"""Tests for saving dataset

General guide lines
===================

1. setgrid function returns `xr.Dataset` object if appropriate grid
   file is provided. Otherwise the data is still a `xr.DataArray`
   object.  This means saving data function should support both
   objects.

2. If data has chunks, then each chunk must be saved in a separate
   file.  When opening multiple files with `xr.mfdataset`, chunks are
   automatically created (one chunk per file)

3. If user provides a chunk size (timespan), it must be used instead.

4. When saving data to multiple files, save data function is also
   responsible for creating appropriate filenames. The time span
   (period) of each file is reflected in the filename but it is not
   the exact time span. It is snapped to the closest interval relative
   to data frequency. Tests should include checks for time span in the
   filename.

5. Avoid unnecessary data resampling when not required. Example: in a
   single time step dataset.


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

from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pycmor.core.config import PymorConfigManager
from pycmor.std_lib.files import _filename_time_range, save_dataset
from pycmor.std_lib.timeaverage import _get_time_method  # noqa: F401

# Tests for time-span in filename


def test_no_timespan_in_filename_when_no_time_dim_in_data():
    ncells = 100
    ds = xr.DataArray(
        np.random.rand(ncells),
        dims=[
            "ncells",
        ],
        name="notime",
    )
    rule = Mock()
    rule.data_request_variable.frequency = ""
    # rule = {"frequency_str": "", "time_method": "INSTANTANEOUS"}
    expected = ""
    result = _filename_time_range(ds, rule)
    assert expected == result, f"Must be empty string. Got: {result}"


frequency_str = (
    "yr",
    "yrPt",
    "dec",
    "mon",
    "monC",
    "monPt",
    "day",
    "6hr",
    "3hr",
    "1hr",
    "1hrCM",
    "6hrPt",
    "3hrPt",
    "1hrPt",
    "subhrPt",
    "fx",
)


@pytest.mark.parametrize("frequency", frequency_str)
def test__filename_time_range_allows_single_timestep(frequency):
    ds = xr.DataArray(
        np.random.random((1, 10)),
        coords={"time": pd.Timestamp("2020-01-02 10:10:10"), "ncells": list(range(10))},
        name="singleTS",
    )
    rule = Mock()
    rule.data_request_variable.frequency = frequency
    # rule = {
    #    "frequency_str": frequency,
    #    "time_method": _get_time_method(frequency),
    # }
    result = _filename_time_range(ds, rule)
    assert result == ""


@pytest.mark.parametrize("frequency", frequency_str)
def test__filename_time_range_multiple_timesteps(frequency):
    time = pd.date_range(
        "2020-01-01 15:12:13",
        "2021-01-01 06:15:17",
        freq="D",
    )
    ds = xr.DataArray(
        np.random.random((time.size, 2)),
        coords={
            "time": time,
            "ncells": [1, 2],
        },
        name="yeardata",
    )
    rule = Mock()
    rule.data_request_variable.frequency = frequency
    # rule = {
    #    "frequency_str": frequency,
    #    "time_method": _get_time_method(frequency),
    # }
    expected = {
        "yr": "2020-2020",
        "yrPt": "2020-2020",
        "dec": "2020-2020",
        "mon": "202001-202012",
        "monC": "202001-202012",
        "monPt": "202001-202012",
        "day": "20200101-20201231",
        "6hr": "202001011512-202012311512",
        "3hr": "202001011512-202012311512",
        "1hr": "202001011512-202012311512",
        "1hrCM": "202001011512-202012311512",
        "6hrPt": "202001011512-202012311512",
        "3hrPt": "202001011512-202012311512",
        "1hrPt": "202001011512-202012311512",
        "subhrPt": "20200101151213-20201231151213",
        "fx": "",
    }
    result = _filename_time_range(ds, rule)
    assert expected[frequency] == result


def test_save_dataset_saves_to_single_file_when_no_time_axis(tmp_path):
    t = tmp_path / "output"
    da = xr.DataArray([1, 2, 3], coords={"ncells": [1, 2, 3]}, dims=["ncells"])
    rule = Mock()
    rule.data_request_variable.table.table_id = "Omon"
    rule.data_request_variable.table_header.approx_interval = 30
    rule.cmor_variable = "CO2"
    rule.variant_label = "r1i1p1f1"
    rule.source_id = "GFDL-ESM2M"
    rule.experiment_id = "historical"
    rule.file_timespan = "1YE"
    rule.output_directory = t
    # rule["institution"] = "AWI"
    save_dataset(da, rule)


def test_save_dataset_saves_to_single_file(tmp_path):
    t = tmp_path / "output"
    dates = xr.cftime_range(start="2001", periods=24, freq="MS", calendar="noleap")
    da = xr.DataArray(np.arange(24), coords=[dates], dims=["time"], name="foo")
    rule = Mock()
    rule._pycmor_cfg = PycmorConfigManager.from_pycmor_cfg({})
    rule.data_request_variable.frequency = "mon"
    rule.data_request_variable.table.table_id = "Omon"
    rule.data_request_variable.table_header.approx_interval = 30
    rule.cmor_variable = "CO2"
    rule.variant_label = "r1i1p1f1"
    rule.source_id = "GFDL-ESM2M"
    rule.experiment_id = "historical"
    rule.file_timespan = "2YS"
    rule.output_directory = t
    save_dataset(da, rule)
    files = list(t.iterdir())
    assert len(files) == 1


def test_save_dataset_saves_to_multiple_files(tmp_path):
    t = tmp_path / "output"
    dates = xr.cftime_range(start="2001", periods=24, freq="MS", calendar="noleap")
    da = xr.DataArray(np.arange(24), coords=[dates], dims=["time"], name="foo")
    rule = Mock()
    rule._pycmor_cfg = PycmorConfigManager.from_pycmor_cfg({})
    rule.data_request_variable.frequency = "mon"
    rule.data_request_variable.table.table_id = "Omon"
    rule.data_request_variable.table_header.approx_interval = 30
    rule.cmor_variable = "CO2"
    rule.variant_label = "r1i1p1f1"
    rule.source_id = "GFDL-ESM2M"
    rule.experiment_id = "historical"
    rule.file_timespan = "6MS"
    rule.output_directory = t
    save_dataset(da, rule)
    files = list(t.iterdir())
    assert len(files) == 4
