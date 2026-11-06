"""repoint_hr_year.py: input patterns are year-locked and match whole filenames."""

import importlib.util
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def repoint():
    spec = importlib.util.spec_from_file_location("repoint_hr_year", REPO / "examples" / "repoint_hr_year.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize(
    "pat, want",
    [
        (r"temp\.fesom\.gr\.\d{4}\.nc", r"temp\.fesom\.gr\.1852\.nc$"),
        (r"temp\.fesom\..*\.nc", r"temp\.fesom\.1852\.nc$"),
        (r"atmos_mon_land_mrro_.*\.nc", r"atmos_mon_land_mrro_1852-1852\.nc$"),
        ('"sst\\\\.fesom\\\\.\\\\d{4}\\\\.nc"', '"sst\\\\.fesom\\\\.1852\\\\.nc$"'),
        ('"*/run1/cLand_monthly.out"', '"*/run1/cLand_monthly.out"'),
        ("mesh.nc", "mesh.nc"),
    ],
)
def test_patterns_are_anchored(repoint, pat, want):
    assert repoint.anchor_pattern(repoint.add_year_to_pattern(pat, "1852")) == want


def test_damaged_siblings_do_not_match(repoint):
    pat = re.compile(repoint.anchor_pattern(repoint.add_year_to_pattern(r"temp\.fesom\.gr\.\d{4}\.nc", "1852")))
    assert pat.match("temp.fesom.gr.1852.nc")
    assert not pat.match("temp.fesom.gr.1852.nc.damaged")


def test_tier_yamls_end_every_regex_pattern(repoint, tmp_path):
    for src in sorted((REPO / "awi-esm3-veg-hr-variables").glob("*/cmip7_awiesm3-veg-hr_*.yaml")):
        text = repoint.repoint_yaml(src, "/work/run", "1852", model="hr")
        for line in text.splitlines():
            m = re.match(r"\s*pattern:\s*(\S+)", line)
            if m and m.group(1).strip("\"'").endswith((r"\.nc", r"\.nc$")):
                assert m.group(1).strip("\"'").endswith("$"), f"{src.name}: {line.strip()}"
