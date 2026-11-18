"""repair_fesom_constants.py turns files written before 8600581a into what 8600581a writes."""

import os
import sys
from pathlib import Path

import netCDF4 as nc
import numpy as np
import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "examples"))
import repair_fesom_constants as rfc  # noqa: E402

FILL = np.float32(1e20)
APPLY = ["--apply", "--written-before", "2999-01-01", "--jobs", "1", "--no-repack"]


def _write(member, region, freq, var, branding, grid, name, values, note=None, comment="old"):
    d = member / region / freq / var / branding / grid / "v20261029"
    d.mkdir(parents=True, exist_ok=True)
    path = d / name
    with nc.Dataset(path, "w") as ds:
        ds.setncatts({"tracking_id": "hdl:21.14107/old", "creation_date": "2026-11-08T00:00:00Z", "history": "h0"})
        ds.createDimension("time", None)
        ds.createDimension("cell", values.shape[1])
        t = ds.createVariable("time", "f8", ("time",))
        t[:] = np.arange(values.shape[0])
        v = ds.createVariable(
            var,
            "f4",
            ("time", "cell"),
            fill_value=FILL,
            zlib=True,
            complevel=1,
            shuffle=True,
            significant_digits=5,
            quantize_mode="BitGroom",
        )
        v.setncatts({"units": "1", "comment": comment, "quantization": "quantization_info", "quantization_nsd": 5})
        if note:
            v.processing_note = note
        v[:] = values
    rfc._strip_underscore_attrs(path)
    return path


@pytest.fixture
def member(tmp_path):
    return tmp_path / "AWI-ESM3-4-2-veg-HR" / "1pctCO2" / "r1i1p1f1"


def _read(path, var):
    with nc.Dataset(path) as ds:
        ds.set_auto_mask(False)
        return (
            ds[var][:],
            {k: ds[var].getncattr(k) for k in ds[var].ncattrs()},
            {k: ds.getncattr(k) for k in ds.ncattrs()},
        )


def test_factors_notes_and_provenance(member):
    vals = np.array([[1.0e12, -2.5e11, FILL], [3.0e10, 0.0, 7.0e12]], dtype=np.float32)
    hfx = _write(member, "glb", "mon", "hfx", "tavg-u-hxy-sea", "g132", "hfx_x_190001-190012.nc", vals)
    note = "Computed as velocity * rho_0(1025.0) * dz * sqrt(cell_area)."
    umo = _write(member, "glb", "mon", "umo", "tavg-ol-hxy-sea", "g130", "umo_x_190001-190012.nc", vals, note=note)
    pso = np.array([[101325.0 + 1025 * 9.80665 * 0.5, FILL]], dtype=np.float32)
    psof = _write(member, "glb", "mon", "pso", "tavg-u-hxy-sea", "g130", "pso_x_190001-190012.nc", pso)
    tree = _write(member, "glb", "mon", "treeFrac", "tavg-u-hxy-u", "g122", "treeFrac_x_190001-190012.nc", vals)
    tree_before = _read(tree, "treeFrac")[0]

    assert rfc.main([str(member), *APPLY]) == 0

    got, attrs, gattrs = _read(hfx, "hfx")
    assert got[0, 2] == FILL
    ok = vals != FILL
    np.testing.assert_allclose(got[ok], vals[ok] * (4.2e6 / 4095900.0), rtol=2e-5)
    assert gattrs["tracking_id"] != "hdl:21.14107/old" and gattrs["tracking_id"].startswith("hdl:21.14107/")
    assert gattrs["history"].splitlines()[1] == "h0" and rfc.TAG in gattrs["history"].splitlines()[0]
    assert attrs["quantization_nsd"] == 5 and not any(k.startswith("_Q") for k in attrs)

    got, attrs, _ = _read(umo, "umo")
    np.testing.assert_allclose(got[ok], vals[ok] * 1030 / 1025, rtol=2e-5)
    assert attrs["processing_note"] == note.replace("1025.0", "1030.0")

    got, _, _ = _read(psof, "pso")
    assert got[0, 0] == pytest.approx(101325.0 + 1030 * 9.80665 * 0.5, rel=2e-5) and got[0, 1] == FILL

    got, attrs, _ = _read(tree, "treeFrac")
    np.testing.assert_array_equal(got, tree_before)
    assert attrs["comment"].startswith("From the LPJ-GUESS annual stand-area output")


def test_second_run_changes_nothing(member):
    vals = np.array([[330.0, 33.0]], dtype=np.float32)
    snm = _write(member, "glb", "mon", "snm", "tavg-u-hxy-si", "g130", "snm_x_190001-190012.nc", vals)
    assert rfc.main([str(member), *APPLY]) == 0
    first, _, g1 = _read(snm, "snm")
    np.testing.assert_allclose(first, [[290.0, 29.0]], rtol=2e-5)
    assert rfc.main([str(member), *APPLY]) == 0
    second, _, g2 = _read(snm, "snm")
    np.testing.assert_array_equal(first, second)
    assert g1["tracking_id"] == g2["tracking_id"]


def test_sltbasin_and_daily_sisnhc(member):
    vals = np.array([[2.0e8, -1.0e8]], dtype=np.float32)
    slt = _write(member, "glb", "mon", "sltbasin", "tavg-u-hyb-sea", "g239", "s_190001-190012.nc", vals)
    old_note = "sisnhc = -L_f*m_snow/a_ice, rho_snow=330.0, L_f=334000.0, derived from daily m_snow and a_ice"
    day = _write(member, "glb", "day", "sisnhc", "tavg-u-hxy-si", "g130", "d_19000101-19001231.nc", vals, old_note)
    rfc.main([str(member), *APPLY])
    np.testing.assert_allclose(_read(slt, "sltbasin")[0], -vals, rtol=2e-5)
    got, attrs, _ = _read(day, "sisnhc")
    np.testing.assert_allclose(got, vals * 290.0, rtol=2e-5)
    assert attrs["processing_note"].startswith("sisnhc = -rho_snow*L_f*m_snow/a_ice, rho_snow=290.0")


def test_withdrawn_products_go_cfc12_stays_in_historical_from_1950(tmp_path):
    vals = np.zeros((1, 1), np.float32)
    pct = tmp_path / "S" / "1pctCO2" / "r1i1p1f1"
    hist = tmp_path / "S" / "historical" / "r1i1p1f1"
    a = _write(pct, "glb", "mon", "cfc12", "tavg-u-hm-air", "g010", "c_190001-190012.nc", vals)
    b = _write(pct, "glb", "day", "dsw", "tavg-u-hxy-lnd", "g122", "d_19000102-19001231.nc", vals)
    c = _write(hist, "glb", "mon", "cfc12", "tavg-u-hm-air", "g010", "c_194901-194912.nc", vals)
    d = _write(hist, "glb", "mon", "cfc12", "tavg-u-hm-air", "g010", "c_195001-195012.nc", vals)
    e = _write(hist, "glb", "mon", "cfc11", "tavg-u-hm-air", "g010", "c_195001-195012.nc", vals)
    for m in (pct, hist):
        assert rfc.main([str(m), *APPLY]) == 0
    assert not a.exists() and not b.exists() and not c.exists() and not e.exists()
    assert d.exists()
    assert not (pct / "glb" / "mon" / "cfc12").exists(), "emptied product directories go too"


def test_dry_run_and_year_filter_touch_nothing(member, capsys):
    vals = np.ones((1, 2), np.float32)
    f = _write(member, "glb", "mon", "hfx", "tavg-u-hxy-sea", "g132", "hfx_x_190001-190012.nc", vals)
    g = _write(member, "glb", "mon", "hfx", "tavg-u-hxy-sea", "g132", "hfx_x_190101-190112.nc", vals)
    before = f.read_bytes()
    assert rfc.main([str(member)]) == 0
    assert f.read_bytes() == before and "would repair hfx_x_190001-190012.nc" in capsys.readouterr().out
    rfc.main([str(member), "--years", "1901-1901", *APPLY])
    assert f.read_bytes() == before
    assert _read(g, "hfx")[0][0, 0] == pytest.approx(4.2e6 / 4095900.0, rel=2e-5)


def test_files_written_after_the_repin_are_left_alone(member):
    """masscello fx comes out of every year under one name: once the repinned
    chain has published it, it is right and must not be scaled again."""
    vals = np.ones((1, 2), np.float32)
    old = _write(member, "glb", "mon", "masso", "tavg-u-hm-sea", "g010", "m_190001-190012.nc", vals)
    fx = _write(member, "glb", "fx", "masscello", "ti-ol-hxy-sea", "g130", "masscello_fx.nc", vals)
    os.utime(old, (1.7e9, 1.7e9))
    before = fx.read_bytes()
    with pytest.raises(SystemExit):
        rfc.main([str(member), "--apply", "--no-repack"])
    assert rfc.main([str(member), "--apply", "--written-before", "2024-01-01", "--jobs", "1", "--no-repack"]) == 0
    assert fx.read_bytes() == before
    assert _read(old, "masso")[0][0, 0] == pytest.approx(1030 / 1025, rel=2e-5)


def test_year_ranges_take_fx_files_only_when_asked(member, capsys):
    """Parallel tasks over different years must not all rewrite the one fx file."""
    vals = np.ones((1, 2), np.float32)
    _write(member, "glb", "fx", "masscello", "ti-ol-hxy-sea", "g130", "masscello_fx.nc", vals)
    rfc.main([str(member), "--years", "1850-1859"])
    assert "masscello_fx.nc" not in capsys.readouterr().out
    rfc.main([str(member), "--years", "1850-1859", "--with-fx"])
    assert "would repair masscello_fx.nc" in capsys.readouterr().out
