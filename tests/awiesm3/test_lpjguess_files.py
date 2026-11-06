"""LPJ-GUESS loaders read only the period directories of the requested years."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "examples"))
import custom_steps as cs  # noqa: E402

FILES = [
    f"/run/outdata/lpj_guess/{p}/run1/x_monthly.out"
    for p in ("18500101-18511231", "18520101-18531231", "18540101-18541231")
]


@pytest.mark.parametrize(
    "start, end, kept",
    [
        (1852, 1852, [1]),
        (1853, 1853, [1]),
        (1851, 1852, [0, 1]),
        ("1854", "1854", [2]),
        (1852, None, [1, 2]),
        (None, 1851, [0]),
        (1900, 1900, []),
    ],
)
def test_only_overlapping_periods_are_kept(start, end, kept):
    rule = SimpleNamespace(year_start=start, year_end=end)
    assert cs._lpjguess_files_for_years(FILES, rule) == [FILES[i] for i in kept]


def test_without_bounds_every_file_is_kept():
    assert cs._lpjguess_files_for_years(FILES, SimpleNamespace()) == FILES


def test_a_file_outside_a_period_directory_is_kept():
    rule = SimpleNamespace(year_start=1852, year_end=1852)
    assert cs._lpjguess_files_for_years(["/run/x_monthly.out"], rule) == ["/run/x_monthly.out"]


def test_the_loader_only_opens_the_requested_period(tmp_path, monkeypatch):
    for period, year in (("18500101-18501231", 1850), ("18510101-18511231", 1851)):
        d = tmp_path / period / "run1"
        d.mkdir(parents=True)
        months = " ".join(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
        (d / "x_monthly.out").write_text(f"Lon Lat Year {months}\n1.0 2.0 {year} " + "1 " * 12 + "\n")
    opened = []
    read_csv = pd.read_csv
    monkeypatch.setattr(pd, "read_csv", lambda f, **kw: opened.append(str(f)) or read_csv(f, **kw))
    rule = SimpleNamespace(
        inputs=[SimpleNamespace(path=tmp_path, pattern_str="*/run1/x_monthly.out")],
        year_start=1851,
        year_end=1851,
        cmor_variable="x",
        name="x_mon",
        model_variable="x",
    )
    rule.get = lambda key, default=None: getattr(rule, key, default)
    try:
        cs.load_lpjguess_monthly(None, rule)
    except Exception:
        pass  # only which files it opened matters here
    assert opened == [str(tmp_path / "18510101-18511231" / "run1" / "x_monthly.out")]
