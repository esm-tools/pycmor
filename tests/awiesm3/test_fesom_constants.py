"""Conversions use FESOM's own constants, and rule limits for experiment and year hold."""

import re
import sys
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
import yaml

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "examples"))
import custom_steps as cs  # noqa: E402
import shard_tier_yaml as sty  # noqa: E402

HR = REPO / "awi-esm3-veg-hr-variables"
LR = REPO / "awi-esm3-lr-variables"
RULE_FILES = sorted(HR.glob("*/cmip7_awiesm3-veg-hr_*.yaml")) + sorted(LR.glob("*/cmip7xml_awiesm3-lr_*.yaml"))


class Rule(dict):
    model_variable = "x"


def test_constants_match_fesom_2_7():
    # oce_modules.F90 density_0 and vcpw, MOD_ICE.F90 rhosno, rhoice, rhowat
    assert cs.FESOM_RHO0 == 1030.0
    assert cs.FESOM_VCPW == 4.2e6
    assert cs.FESOM_RHOSNO == 290.0
    assert cs.FESOM_RHOICE == 910.0
    assert cs.FESOM_RHOWAT_ICE == 1025.0


def test_daily_sisnhc_uses_the_snow_density(monkeypatch):
    # FESOM m_snow is snow height per cell area in m, so rho_snow is needed.
    a_ice = xr.DataArray(np.array([[0.5, 1.0, 0.0]]), dims=("time", "nod2"))
    monkeypatch.setattr(cs, "_load_secondary_mf", lambda *a, **k: a_ice)
    m_snow = xr.DataArray(np.array([[0.29, 0.1, 0.0]]), dims=("time", "nod2"))
    got = cs.compute_sisnhc_from_msnow(m_snow, Rule()).values[0]
    assert got[0] == pytest.approx(-290.0 * 334000.0 * 0.58)
    assert got[1] == pytest.approx(-290.0 * 334000.0 * 0.1)
    assert got[2] == 0.0


def test_daily_and_monthly_sisnhc_agree(monkeypatch):
    a_ice = xr.DataArray(np.array([[0.25, 0.8]]), dims=("time", "nod2"))
    monkeypatch.setattr(cs, "_load_secondary_mf", lambda *a, **k: a_ice)
    m_snow = xr.DataArray(np.array([[0.05, 0.4]]), dims=("time", "nod2"))
    daily = cs.compute_sisnhc_from_msnow(m_snow, Rule())
    monthly = cs.compute_sisnhc(m_snow / a_ice, Rule())
    np.testing.assert_allclose(daily.values, monthly.values)


def test_sltbasin_keeps_tripyview_orientation():
    # hfbasin keeps tripyview's (-1) and matches RAPID; salt must keep it too.
    src = (REPO / "examples" / "custom_steps.py").read_text()
    body = src[src.index("def compute_sltbasin_tripyview") :]
    body = body[: body.index("\ndef ")]
    assert "factor = 1e12 / _CP" in body
    assert "factor = -1e12" not in body


def _values(path, key):
    out = []
    for line in path.read_text().splitlines():
        if line.strip().startswith("#"):
            continue
        m = re.match(rf"^\s*{key}:\s*([-0-9.e+]+)", line)
        if m:
            out.append(float(m.group(1)))
    return out


@pytest.mark.parametrize("path", RULE_FILES, ids=lambda p: p.name)
def test_rules_use_fesom_densities(path):
    scales = _values(path, "scale_factor")
    for bad in (1027.0, 1.027, 1.025, 4095900.0, 4097273.0, 330.0, -330.0):
        assert bad not in scales, f"{path.name}: scale_factor {bad}"
    assert 330.0 not in _values(path, "rho_snow")
    for value in _values(path, "reference_density"):
        # sea ice salt fluxes come from fw_ice, which FESOM scales by the ice model's rhowat
        assert value == (1025.0 if "seaice" in path.name else 1030.0), path.name


@pytest.mark.parametrize("root,pattern", [(HR, "cmip7_awiesm3-veg-hr_*.yaml"), (LR, "cmip7xml_awiesm3-lr_*.yaml")])
def test_catalogue_drops_cfc11_and_dsw_and_reads_rootd_from_1850(root, pattern):
    rules = [r for p in root.glob(f"*/{pattern}") for r in yaml.safe_load(p.read_text())["rules"]]
    compounds = {r.get("compound_name") for r in rules}
    assert "atmosChem.cfc11.tavg-u-hm-air.mon.glb" not in compounds
    assert "land.dsw.tavg-u-hxy-lnd.day.glb" not in compounds
    (rootd,) = [r for r in rules if r.get("compound_name") == "land.rootd.ti-u-hxy-lnd.fx.glb"]
    assert rootd["inputs"][0]["pattern"].endswith(r"_1850-1850\.nc")
    assert rootd["skip_input_year_filter"] is True
    (cfc12,) = [r for r in rules if r.get("compound_name") == "atmosChem.cfc12.tavg-u-hm-air.mon.glb"]
    assert cfc12["only_experiments"] == ["historical"] and cfc12["first_year"] == 1950


@pytest.mark.parametrize(
    "experiment,year,kept",
    [("1pctCO2", 1960, False), ("historical", 1949, False), ("historical", 1950, True), ("historical", None, True)],
)
def test_shard_filter_honours_experiment_and_first_year(experiment, year, kept):
    rule = {"name": "cfc12_mon", "only_experiments": ["historical"], "first_year": 1950}
    inherit = {"experiment_id": experiment, "year": year}
    assert sty._applicable(rule, inherit) is kept


def test_shard_mode_leaves_out_rules_not_delivered(tmp_path):
    src = {
        "inherit": {"experiment_id": "1pctCO2", "year": 1900},
        "rules": [{"name": "a"}, {"name": "cfc12_mon", "only_experiments": ["historical"]}],
    }
    path = tmp_path / "tier.yaml"
    path.write_text(yaml.safe_dump(src))
    written = sty.shard_mode(str(path), 20, str(tmp_path / "out"))
    names = [r["name"] for w in written for r in yaml.safe_load(Path(w).read_text())["rules"]]
    assert names == ["a"]
