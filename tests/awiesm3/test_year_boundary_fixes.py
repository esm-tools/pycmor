"""The esgf-qa findings on HR 1pctCO2 (2026-11-26): no New Year gaps, one time reference, no XIOS stream attributes."""

import importlib.util
import logging
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import cftime
import netCDF4 as nc
import numpy as np
import pytest
import xarray as xr
import yaml

from pycmor.core.gather_inputs import retain_year_end_point
from pycmor.core.rule import Rule
from pycmor.std_lib.files import _strip_xios_stream_attrs, split_data_timespan

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "examples"))
import custom_steps as cs  # noqa: E402
import repair_time_axis as rta  # noqa: E402


def _load(name):
    spec = importlib.util.spec_from_file_location(name, REPO / "examples" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def driver():
    return _load("run_year_driver")


@pytest.fixture(scope="module")
def repoint():
    return _load("repoint_hr_year")


# --- point-in-time streams keep next Jan 1 00:00 ---------------------------


@pytest.mark.parametrize(
    "value, compound, want",
    [
        ("tpt", "atmos.tas.tpt-h2m-hxy-u.3hr.glb", True),
        ("tpt", "atmos.tas.tavg-h2m-hxy-u.day.glb", False),
        ("tpt", None, False),
        (True, "atmos.tas.tavg-h2m-hxy-u.day.glb", True),
        ("false", "atmos.tas.tpt-h2m-hxy-u.3hr.glb", False),
        (None, "atmos.tas.tpt-h2m-hxy-u.3hr.glb", False),
    ],
)
def test_retain_year_end_point_values(value, compound, want):
    rule = {"compound_name": compound}
    if value is not None:
        rule["retain_year_end_point"] = value
    assert retain_year_end_point(rule) is want


def test_tpt_year_keeps_its_last_sample_in_one_file(monkeypatch):
    dates = [cftime.DatetimeProlepticGregorian(1853, 12, 31, 21), cftime.DatetimeProlepticGregorian(1854, 1, 1)]
    data = xr.Dataset({"tas": ("time", [1.0, 2.0])}, coords={"time": dates})
    monkeypatch.setattr("pycmor.std_lib.files.file_timespan_tail", lambda rule: [dates[-1]])
    tpt = Rule(
        name="tas_3hr",
        cmor_variable="tas",
        compound_name="atmos.tas.tpt-h2m-hxy-u.3hr.glb",
        retain_year_end_point="tpt",
    )
    got = split_data_timespan(data, tpt)
    assert [c.sizes["time"] for c in got] == [2]
    tavg = Rule(
        name="tas_day",
        cmor_variable="tas",
        compound_name="atmos.tas.tavg-h2m-hxy-u.day.glb",
        retain_year_end_point="tpt",
    )
    assert [c.sizes["time"] for c in split_data_timespan(data, tavg)] == [1, 1]


# --- daily storage changes start on Jan 1 -----------------------------------


def _src_file(d, year, values):
    path = d / f"atmos_day_land_src_{year}-{year}.nc"
    n = len(values)
    with nc.Dataset(path, "w") as ds:
        ds.createDimension("time_counter", None)
        ds.createDimension("cell", 2)
        t = ds.createVariable("time_counter", "f8", ("time_counter",))
        t.units, t.calendar = "days since 1850-01-01", "proleptic_gregorian"
        start = cftime.date2num(cftime.datetime(year, 1, 1, 12, calendar="proleptic_gregorian"), t.units, t.calendar)
        t[:] = start + np.arange(n)
        v = ds.createVariable("src", "f4", ("time_counter", "cell"))
        v[:] = np.stack([values, values], axis=1)
    return path


class _Rule(dict):
    def __init__(self, files, **kw):
        super().__init__(**kw)
        self.inputs = [SimpleNamespace(files=files)]


def _load_year(path):
    ds = xr.open_dataset(path, use_cftime=True).rename({"time_counter": "time"})
    return ds


@pytest.mark.parametrize("year", [1851])
def test_storage_change_uses_the_previous_december(tmp_path, year):
    _src_file(tmp_path, year - 1, np.arange(365, dtype="f4"))
    cur = _src_file(tmp_path, year, 1000.0 + np.arange(365, dtype="f4"))
    rule = _Rule([cur], model_variable="skin_reservoir", year=year)
    out = cs.compute_temporal_diff(_load_year(cur), rule)
    assert out.sizes["time"] == 365
    assert out.time.values[0] == cftime.DatetimeProlepticGregorian(year, 1, 1, 12)
    # Jan 1 minus Dec 31, in kg m-2
    np.testing.assert_allclose(out["skin_reservoir"].values[0], (1000.0 - 364.0) * 1000.0)
    np.testing.assert_allclose(out["skin_reservoir"].values[1:], 1000.0)


def test_first_year_of_a_run_keeps_its_first_day_missing(tmp_path, caplog):
    cur = _src_file(tmp_path, 1850, np.arange(365, dtype="f4"))
    rule = _Rule([cur], model_variable="skin_reservoir", year=1850)
    with caplog.at_level(logging.WARNING):
        out = cs.compute_temporal_diff(_load_year(cur), rule)
    assert out.sizes["time"] == 364
    assert "January 1 1850 stays missing" in caplog.text


def test_previous_day_must_be_one_day_earlier(tmp_path):
    _src_file(tmp_path, 1850, np.arange(300, dtype="f4"))  # ends in October
    cur = _src_file(tmp_path, 1851, np.arange(365, dtype="f4"))
    rule = _Rule([cur], model_variable="skin_reservoir", year=1851)
    with pytest.raises(ValueError, match="not one day before"):
        cs.compute_temporal_diff(_load_year(cur), rule)


# --- publishing a rerun year removes the file it replaces --------------------

TAS = "tas_tpt-h2m-hxy-u_3hr_glb_g122_AWI-ESM3-4-2-veg-HR_1pctCO2_r1i1p1f1_{}.nc"
DCW = "dcw_tavg-u-hxy-lnd_day_glb_g122_AWI-ESM3-4-2-veg-HR_1pctCO2_r1i1p1f1_{}.nc"


@pytest.mark.parametrize(
    "name, old, new, neighbours",
    [
        (TAS, "185001010300-185012312100", "185001010300-185101010000", ["185101010300-185112312100"]),
        (DCW, "18510102-18511231", "18510101-18511231", ["18500102-18501231", "18520102-18521231"]),
    ],
)
def test_publish_removes_the_superseded_file_only(driver, tmp_path, name, old, new, neighbours):
    sub = "glb/3hr/tas/tpt-h2m-hxy-u/g122/v20261029"
    out = tmp_path / "out" / "AWI-ESM3-4-2-veg-HR/1pctCO2/r1i1p1f1" / sub
    out.mkdir(parents=True)
    for stamp in [old, *neighbours]:
        (out / name.format(stamp)).write_text("old")
    src = tmp_path / "work" / "cmorized" / "AWI-ESM3-4-2-veg-HR/1pctCO2/r1i1p1f1" / sub / name.format(new)
    src.parent.mkdir(parents=True)
    src.write_text("new")
    assert driver.publish(tmp_path / "work", tmp_path / "out") == (1, 1)
    assert sorted(p.name for p in out.iterdir()) == sorted(name.format(s) for s in [new, *neighbours])


def test_publish_leaves_other_datasets_alone(driver, tmp_path):
    other = tmp_path / TAS.replace("tas_", "huss_").format("185001010300-185012312100")
    other.write_text("x")
    dest = tmp_path / TAS.format("185001010300-185101010000")
    dest.write_text("y")
    assert driver.drop_superseded(dest) == 0
    assert other.exists()


# --- rendered yamls carry both settings --------------------------------------


@pytest.mark.parametrize("model, glob", [("hr", "awi-esm3-veg-hr-variables/*/cmip7_awiesm3-veg-hr*.yaml")])
def test_every_tier_yaml_gets_time_units_and_tpt_endpoint(repoint, model, glob):
    for src in sorted(REPO.glob(glob)):
        doc = yaml.safe_load(repoint.repoint_yaml(src, "/work/run", "1852", "1pctCO2", model=model))
        inherit = doc.get("inherit") or {}
        assert inherit.get("time_units") == "days since 1850-01-01", src.name
        assert inherit.get("retain_year_end_point") == "tpt", src.name


# --- XIOS stream attributes do not reach the file ----------------------------


def test_xios_stream_attributes_are_dropped():
    ds = xr.Dataset({"tos": ("time", [1.0], {"online_operation": "average", "interval_write": "1 d", "units": "degC"})})
    ds["time"] = ("time", [0.0], {"interval_operation": "1 d"})
    ds = _strip_xios_stream_attrs(ds)
    assert ds["tos"].attrs == {"units": "degC"}
    assert ds["time"].attrs == {}


# --- repair_time_axis.py ------------------------------------------------------


def _published(path, units, values, bounds):
    path.parent.mkdir(parents=True, exist_ok=True)
    with nc.Dataset(path, "w") as ds:
        ds.setncatts({"tracking_id": "hdl:21.14107/old", "creation_date": "2026-09-30T06:40:09Z", "history": "h0"})
        ds.createDimension("time", None)
        ds.createDimension("bnds", 2)
        t = ds.createVariable("time", "f8", ("time",))
        t.setncatts({"units": units, "calendar": "proleptic_gregorian", "bounds": "time_bnds"})
        t[:] = values
        b = ds.createVariable("time_bnds", "f8", ("time", "bnds"))
        b[:] = bounds
        v = ds.createVariable("tos", "f4", ("time",))
        v.setncatts(
            {"units": "degC", "online_operation": "average", "interval_operation": "1 d", "interval_write": "1 d"}
        )
        v[:] = np.arange(len(values))
    return path


def test_repair_keeps_every_date_and_is_idempotent(tmp_path):
    member = tmp_path / "AWI-ESM3-4-2-veg-HR/1pctCO2/r1i1p1f1"
    f = _published(
        member / "glb/mon/tos/x/g010/v1/tos_185401-185412.nc", "days since 1852-01-01", [746.5], [[731.0, 762.0]]
    )
    right = _published(
        member / "glb/mon/tos/x/g010/v1/tos_185001-185012.nc", "days since 1850-01-01", [15.5], [[0.0, 31.0]]
    )
    with nc.Dataset(right, "r+") as ds:
        for a in ("online_operation", "interval_operation", "interval_write"):
            ds["tos"].delncattr(a)
    assert rta.main([str(member), "--jobs", "1", "--apply"]) == 0
    with nc.Dataset(f) as ds:
        assert ds["time"].units == "days since 1850-01-01"
        assert ds["time"][:].tolist() == [746.5 + 730]
        assert ds["time_bnds"][:].tolist() == [[1461.0, 1492.0]]
        assert cftime.num2date(ds["time"][0], ds["time"].units, "proleptic_gregorian") == cftime.datetime(
            1854, 1, 16, 12, calendar="proleptic_gregorian"
        )
        assert "interval_write" not in ds["tos"].ncattrs()
        assert ds.tracking_id != "hdl:21.14107/old"
        assert ds.history.startswith(ds.creation_date + ": repair_time_axis.py")
        assert ds["tos"][:].tolist() == [0.0]
    with nc.Dataset(right) as ds:
        assert ds.tracking_id == "hdl:21.14107/old"
    stamp = f.stat().st_mtime
    assert rta.main([str(member), "--jobs", "1", "--apply"]) == 0
    assert f.stat().st_mtime == stamp


def test_repair_removes_superseded_files_only_when_asked(tmp_path):
    member = tmp_path / "m/e/r"
    d = member / "glb/day/dcw/tavg-u-hxy-lnd/g122/v1"
    old = _published(d / DCW.format("18510102-18511231"), "days since 1850-01-01", [365.5], [[365.0, 366.0]])
    os.utime(old, (time.time() + 100, time.time() + 100))  # even when the old file looks newer
    new = _published(d / DCW.format("18510101-18511231"), "days since 1850-01-01", [365.5], [[365.0, 366.0]])
    assert rta.main([str(member), "--jobs", "1", "--apply"]) == 0
    assert old.exists()
    assert rta.main([str(member), "--jobs", "1", "--apply", "--remove-superseded"]) == 0
    assert not old.exists() and new.exists()
