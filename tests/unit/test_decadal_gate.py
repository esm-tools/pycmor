"""decadal_gate: dec rules write only in the year closing a decade from the run start."""

import types

import cftime
import numpy as np
import pytest

from pycmor.core.gather_inputs import InputFileCollection
from pycmor.core.pipeline import Pipeline
from pycmor.core.skip import RuleSkipped
from pycmor.std_lib.decadal import decadal_gate
from pycmor.std_lib.time_bounds import _create_decadal_bounds


class _Rule(types.SimpleNamespace):
    def get(self, key, default=None):
        return getattr(self, key, default)


def _rule(tmp_path, years, year):
    for y in years:
        (tmp_path / f"temp.fesom.{y}.nc").touch()
    # repoint_hr_year.py pins the pattern to the submitted year
    coll = InputFileCollection(tmp_path, rf"temp\.fesom\.{year}\.nc")
    return _Rule(name="thetao_dec", year=year, inputs=[coll])


def test_closing_year_reads_the_decade(tmp_path):
    rule = _rule(tmp_path, range(1850, 1862), 1859)
    assert decadal_gate("payload", rule) == "payload"
    assert (rule.year_start, rule.year_end) == (1850, 1859)
    assert len(rule.inputs[0].files) == 12  # widened back to every year


@pytest.mark.parametrize("year", [1850, 1855, 1858, 1860, 1861])
def test_other_years_write_nothing(tmp_path, year):
    rule = _rule(tmp_path, range(1850, 1862), year)
    out = decadal_gate("payload", rule)
    assert isinstance(out, RuleSkipped)
    assert not hasattr(rule, "year_start")


def test_decades_count_from_the_run_start(tmp_path):
    years = range(2024, 2040)
    assert isinstance(decadal_gate(None, _rule(tmp_path, years, 2029)), RuleSkipped)
    rule = _rule(tmp_path, years, 2033)
    decadal_gate(None, rule)
    assert (rule.year_start, rule.year_end) == (2024, 2033)


def test_trailing_partial_decade_is_not_written(tmp_path):
    # piControl 1850-2045: 2045 is year 6 of 2040-2049
    assert isinstance(decadal_gate(None, _rule(tmp_path, range(1850, 2046), 2045)), RuleSkipped)


def test_missing_year_in_closing_decade_is_an_error(tmp_path):
    years = [y for y in range(1850, 1860) if y != 1854]
    with pytest.raises(FileNotFoundError, match="1854"):
        decadal_gate(None, _rule(tmp_path, years, 1859))


def test_pipeline_stops_at_skip():
    def skip(data, rule):
        return RuleSkipped("not this year")

    def must_not_run(data, rule):
        raise AssertionError("step after the skip ran")

    fake = types.SimpleNamespace(steps=[skip, must_not_run])
    assert isinstance(Pipeline._run_native(fake, None, None), RuleSkipped)


def test_bounds_follow_the_gated_decade():
    rule = _Rule(year_start=2024, year_end=2033)
    stamp = np.array(["2029-01-01"], dtype="datetime64[ns]")
    b = _create_decadal_bounds(stamp, rule)
    assert b[0, 0] == np.datetime64("2024-01-01") and b[0, 1] == np.datetime64("2034-01-01")
    ct = np.array([cftime.DatetimeProlepticGregorian(2029, 1, 1)], dtype=object)
    cb = _create_decadal_bounds(ct, rule)
    assert (cb[0, 0].year, cb[0, 1].year) == (2024, 2034)
    # without a gated decade the calendar decade still applies
    assert _create_decadal_bounds(stamp)[0, 0] == np.datetime64("2020-01-01")
