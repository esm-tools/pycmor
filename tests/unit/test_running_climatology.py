"""A running climatology's years reach its bounds, time stamps, name and published file."""

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pycmor.std_lib.files import _ensure_climatology_bounds, _filename_time_range

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "examples"))
import custom_steps as cs  # noqa: E402


@pytest.fixture(scope="module")
def driver():
    spec = importlib.util.spec_from_file_location("run_year_driver", REPO / "examples" / "run_year_driver.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _snapshot():
    """What the accumulator hands on: twelve months stamped in the first year."""
    time = [np.datetime64(f"1850-{m:02d}-01") for m in range(1, 13)]
    da = xr.DataArray(np.arange(12.0), dims="time", coords={"time": time}, name="pfull")
    da["time"].attrs = {"standard_name": "time", "axis": "T"}
    return da.to_dataset()


def _rule(years=(1850, 1855)):
    drv = SimpleNamespace(frequency="mon", dimensions=("time2",))
    return SimpleNamespace(climatology_years=years, data_request_variable=drv)


def test_bounds_span_all_years_and_time_sits_in_the_middle():
    ds = _ensure_climatology_bounds(_snapshot(), _rule())
    assert ds["time"].attrs["climatology"] == "climatology_bnds"
    since = pd.Timestamp(ds["time"].encoding["units"].split(" since ")[1])

    def date(days):
        return since + pd.to_timedelta(days, "D")

    bnds = ds["climatology_bnds"].values
    assert date(bnds[0, 0]) == pd.Timestamp("1850-01-01")
    assert date(bnds[0, 1]) == pd.Timestamp("1855-02-01")
    assert date(bnds[11, 0]) == pd.Timestamp("1850-12-01")
    assert date(bnds[11, 1]) == pd.Timestamp("1856-01-01")


def test_time_is_exactly_the_interval_midpoint_on_disk(tmp_path):
    ds = _ensure_climatology_bounds(_snapshot(), _rule())
    path = tmp_path / "c.nc"
    ds.to_netcdf(path)
    with xr.open_dataset(path, decode_times=False) as back:
        assert back["time"].dtype == np.float64
        assert "units" not in back["climatology_bnds"].attrs
        np.testing.assert_array_equal(back["time"].values, back["climatology_bnds"].values.mean(axis=1))


def test_a_second_pass_changes_nothing():
    once = _ensure_climatology_bounds(_snapshot(), _rule())
    twice = _ensure_climatology_bounds(once, _rule())
    np.testing.assert_array_equal(once["climatology_bnds"].values, twice["climatology_bnds"].values)
    np.testing.assert_array_equal(once["time"].values, twice["time"].values)


def test_the_name_spans_the_years_before_and_after_the_bounds():
    rule = _rule()
    assert _filename_time_range(_snapshot(), rule) == "185001-185512"
    assert _filename_time_range(_ensure_climatology_bounds(_snapshot(), rule), rule) == "185001-185512"


def test_a_rule_without_years_keeps_its_stamps():
    rule = _rule(None)
    assert _filename_time_range(_snapshot(), rule) == "185001-185012"


def test_the_accumulator_leaves_its_years_on_the_rule(tmp_path, monkeypatch):
    monkeypatch.setenv("PYCMOR_CLIMATOLOGY_STATE_DIR", str(tmp_path))

    class Rule(SimpleNamespace):
        def get(self, key, default=None):
            return getattr(self, key, default)

    rule = Rule(source_id="s", experiment_id="e", variant_label="v", compound_name="c", model_variable="pfull")
    for year in (1850, 1851):
        time = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")
        da = xr.DataArray(np.ones(time.size), dims="time", coords={"time": time}, name="pfull")
        out = cs.accumulate_monthly_climatology(da, rule)
    assert rule.climatology_years == (1850, 1851)
    assert "climatology_years" not in out["time"].attrs
    assert out["time"].dt.year.values.tolist() == [1850] * 12


NAME = "pfull_tclm-al-hxy-u_mon_glb_g122_AWI-ESM3-4-2-veg-HR_piControl_r1i1p1f1_{}.nc"


def test_publishing_a_grown_climatology_removes_the_older_snapshot(driver, tmp_path):
    sub = "MIP-DRS7/CMIP7/CMIP/AWI/m/e/r/glb/mon/pfull/tclm-al-hxy-u/g122/v1"
    old = tmp_path / "out" / sub / NAME.format("185001-185412")
    old.parent.mkdir(parents=True)
    old.write_text("old")
    src = tmp_path / "work" / "cmorized" / sub / NAME.format("185001-185512")
    src.parent.mkdir(parents=True)
    src.write_text("new")
    assert driver.publish(tmp_path / "work", tmp_path / "out") == (1, 1)
    assert sorted(p.name for p in old.parent.iterdir()) == [NAME.format("185001-185512")]


@pytest.mark.parametrize(
    "name",
    [
        "pfull_tavg-al-hxy-u_mon_glb_g122_m_e_r_185001-185012.nc",
        "tas_tmaxavg-h2m-hxy-u_mon_glb_g122_m_e_r_185001-185012.nc",
        "areacella_ti-u-hxy-u_fx_glb_g122_m_e_r.nc",
    ],
)
def test_other_files_of_other_years_stay(driver, tmp_path, name):
    other = tmp_path / name.replace("185001-185012", "184901-184912")
    other.write_text("x")
    dest = tmp_path / name
    dest.write_text("y")
    assert driver.drop_older_climatologies(dest) == 0
    assert other.exists()
