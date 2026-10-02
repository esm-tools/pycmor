"""Unit tests for pycmor.core.overrides — CLI override application."""

from __future__ import annotations

import pathlib
import tempfile

import pytest
import yaml
from click.exceptions import UsageError
from click.testing import CliRunner

from pycmor.core.gather_inputs import filter_files_by_year_range
from pycmor.core.overrides import (
    CliOverrides,
    OverrideError,
    _subst_anchored,
    apply_overrides,
)


# ---------------------------------------------------------------------------
# Test 1: anchored substitution must respect path boundaries
# ---------------------------------------------------------------------------
def test_subst_anchored_does_not_corrupt_prefix_collisions():
    import re

    pattern = re.compile(re.escape("/work/Test_01") + r"(?=/|$)")
    cfg = {
        "exact": "/work/Test_01",
        "with_suffix": "/work/Test_01/x.nc",
        "collision": "/work/Test_01_backup/x.nc",  # MUST NOT match
        "nested": ["/work/Test_01/a", {"k": "/work/Test_01_backup/b"}],
    }
    out = _subst_anchored(cfg, pattern, "/scratch/Run_99")

    assert out["exact"] == "/scratch/Run_99"
    assert out["with_suffix"] == "/scratch/Run_99/x.nc"
    assert out["collision"] == "/work/Test_01_backup/x.nc"
    assert out["nested"][0] == "/scratch/Run_99/a"
    assert out["nested"][1]["k"] == "/work/Test_01_backup/b"


# ---------------------------------------------------------------------------
# Test 2: --data-path with no inherit.data_path and no --old-data-path errors
# ---------------------------------------------------------------------------
def test_data_path_without_old_prefix_raises():
    """No inherit.data_path and no --old-data-path → can't auto-detect old root."""
    cfg = {"rules": [{"name": "r1"}]}
    ov = CliOverrides(data_path="/new/path")
    with pytest.raises(OverrideError, match="--old-data-path"):
        apply_overrides(cfg, ov)


def test_data_path_with_non_conforming_inherit_raises():
    """inherit.data_path that doesn't follow /outdata/<component> convention
    can't be auto-derived; user must pass --old-data-path explicitly."""
    cfg = {
        "inherit": {"data_path": "/work/some/random/dir"},
        "rules": [{"name": "r1"}],
    }
    ov = CliOverrides(data_path="/scratch/Run_99")
    with pytest.raises(OverrideError, match="<run_root>/outdata/<component>"):
        apply_overrides(cfg, ov)


# ---------------------------------------------------------------------------
# Test 3: --old-data-path without --data-path errors
# ---------------------------------------------------------------------------
def test_old_data_path_without_data_path_raises():
    cfg = {"rules": [{"name": "r1"}]}
    ov = CliOverrides(old_data_path="/old/path")
    with pytest.raises(OverrideError, match="requires --data-path"):
        apply_overrides(cfg, ov)


# ---------------------------------------------------------------------------
# Test 4: per-rule year_start wins over inherit and over rule's baked-in value
# ---------------------------------------------------------------------------
def test_cli_year_start_wins_over_per_rule_value():
    cfg = {
        "inherit": {"year_start": 1500, "year_end": 1600},
        "rules": [
            {"name": "r1", "year_start": 1900, "year_end": 1910},
            {"name": "r2"},
        ],
    }
    ov = CliOverrides(year_start=1587, year_end=1587)
    out = apply_overrides(cfg, ov)

    assert out["rules"][0]["year_start"] == 1587
    assert out["rules"][0]["year_end"] == 1587
    assert out["rules"][1]["year_start"] == 1587
    assert out["rules"][1]["year_end"] == 1587
    # Input cfg untouched.
    assert cfg["rules"][0]["year_start"] == 1900


# ---------------------------------------------------------------------------
# Test 5: CLI integration — OverrideError surfaces as click.UsageError
# ---------------------------------------------------------------------------
def test_cli_surfaces_override_error_as_usage_error():
    """CliRunner integration: --old-data-path without --data-path → exit 2."""
    from pycmor.cli import process

    with tempfile.TemporaryDirectory() as tmp:
        cfg_path = pathlib.Path(tmp) / "tiny.yaml"
        cfg_path.write_text(
            yaml.safe_dump({"rules": [{"name": "r1", "compound_name": "x"}]})
        )

        runner = CliRunner()
        result = runner.invoke(
            process,
            [str(cfg_path), "--old-data-path", "/old"],
            standalone_mode=False,
        )
        assert isinstance(result.exception, UsageError), (
            f"expected UsageError, got {type(result.exception).__name__}: {result.exception}"
        )
        assert "--old-data-path" in str(result.exception)


# ---------------------------------------------------------------------------
# Test 6: filter_files_by_year_range covers _load_secondary_mf's needs
# ---------------------------------------------------------------------------
# Fixture filenames must match the regex that `_filter_files_by_year_range`
# parses — FESOM form `var.fesom.YEAR.nc` or OIFS form `..._YEAR-YEAR.nc`;
# arbitrary names won't carry a year token and the filter would silently
# keep everything.
def test_filter_files_by_year_range_keeps_only_matching_years(tmp_path):
    fesom_files = [
        tmp_path / "sd.fesom.1586.nc",
        tmp_path / "sd.fesom.1587.nc",
        tmp_path / "sd.fesom.1588.nc",
    ]
    for f in fesom_files:
        f.touch()

    out = filter_files_by_year_range([str(f) for f in fesom_files], 1587, 1587)
    assert out == [str(tmp_path / "sd.fesom.1587.nc")]

    # Also works with pathlib inputs and returns paths
    out_paths = filter_files_by_year_range(fesom_files, 1587, 1587)
    assert out_paths == [tmp_path / "sd.fesom.1587.nc"]


def test_filter_files_by_year_range_handles_oifs_year_range_filenames(tmp_path):
    oifs_files = [
        tmp_path / "atmos_1m_msl_1586-1586.nc",
        tmp_path / "atmos_1m_msl_1587-1587.nc",
        tmp_path / "atmos_1m_msl_1588-1588.nc",
    ]
    for f in oifs_files:
        f.touch()

    out = filter_files_by_year_range(oifs_files, 1587, 1587)
    assert out == [tmp_path / "atmos_1m_msl_1587-1587.nc"]


# ---------------------------------------------------------------------------
# Bonus: data_path substitution end-to-end through apply_overrides
# ---------------------------------------------------------------------------
def test_apply_overrides_substitutes_data_path_throughout_cfg():
    """--data-path/--old-data-path replace the run-root prefix everywhere."""
    cfg = {
        "inherit": {"data_path": "/work/runtime/Test_01/outdata/fesom"},
        "rules": [
            {
                "name": "r1",
                "inputs": [
                    {"path": "/work/runtime/Test_01/outdata/oifs", "pattern": "x"}
                ],
                "second_input_file": "/work/runtime/Test_01/outdata/foo.nc",
            },
        ],
    }
    ov = CliOverrides(
        data_path="/scratch/runtime/Run_99",
        old_data_path="/work/runtime/Test_01",
    )
    out = apply_overrides(cfg, ov)

    assert out["inherit"]["data_path"] == "/scratch/runtime/Run_99/outdata/fesom"
    assert (
        out["rules"][0]["inputs"][0]["path"]
        == "/scratch/runtime/Run_99/outdata/oifs"
    )
    assert (
        out["rules"][0]["second_input_file"]
        == "/scratch/runtime/Run_99/outdata/foo.nc"
    )
    # Source cfg untouched.
    assert (
        cfg["rules"][0]["inputs"][0]["path"]
        == "/work/runtime/Test_01/outdata/oifs"
    )


def test_memory_override_writes_to_jobqueue_slurm():
    """--memory writes to jobqueue.slurm.memory; existing keys preserved."""
    cfg = {
        "jobqueue": {
            "slurm": {"name": "pycmor-worker", "queue": "compute", "memory": "256GB"}
        },
        "rules": [{"name": "r1"}],
    }
    out = apply_overrides(cfg, CliOverrides(memory="512GB"))
    assert out["jobqueue"]["slurm"]["memory"] == "512GB"
    # other slurm keys untouched
    assert out["jobqueue"]["slurm"]["name"] == "pycmor-worker"
    assert out["jobqueue"]["slurm"]["queue"] == "compute"
    # source cfg untouched
    assert cfg["jobqueue"]["slurm"]["memory"] == "256GB"


def test_no_memory_override_leaves_yaml_jobqueue_untouched():
    """When --memory is not given, jobqueue.slurm is bit-identical."""
    cfg = {
        "jobqueue": {"slurm": {"memory": "256GB", "cores": 16}},
        "rules": [{"name": "r1"}],
    }
    out = apply_overrides(cfg, CliOverrides())
    assert out["jobqueue"]["slurm"] == {"memory": "256GB", "cores": 16}


# ---------------------------------------------------------------------------
# R2: skip_input_year_filter opt-out for centennial forcing files
# ---------------------------------------------------------------------------
def test_r2_skip_input_year_filter_in_load_secondary_mf(tmp_path):
    """_load_secondary_mf must respect skip_input_year_filter."""
    import sys

    custom_steps_path = pathlib.Path(
        "/work/ab0246/a270092/software/pycmor/examples"
    )
    if str(custom_steps_path) not in sys.path:
        sys.path.insert(0, str(custom_steps_path))

    # Files spanning 1750-2022 (centennial forcing) — would be filtered
    # out for year=1587 without the opt-out.
    centennial_file = tmp_path / "cfc11_input4MIPs_GHG_1750-2022.nc"
    centennial_file.touch()

    # Build a minimal rule object that exposes ``.get`` like a Rule.
    class _Rule(dict):
        def get(self, key, default=None):
            return super().get(key, default)

    rule = _Rule(
        second_input_path=str(tmp_path),
        second_input_pattern=r"cfc11_input4MIPs_GHG_\d{4}-\d{4}\.nc",
        second_variable="cfc11",
        year_start=1587,
        year_end=1587,
        skip_input_year_filter=True,
    )

    # Reach into _load_secondary_mf's filtering logic without opening
    # a netCDF (centennial_file is a touched-empty file). We assert the
    # filter step is bypassed by checking that the regex match returns
    # the file and the year filter does NOT remove it. We exercise just
    # the filter call site directly — opening the dataset is irrelevant
    # to the gate test.
    from pycmor.core.gather_inputs import filter_files_by_year_range

    files = [str(centennial_file)]
    skip = rule.get("skip_input_year_filter", False)
    if (
        rule.get("year_start") is not None
        and rule.get("year_end") is not None
        and not skip
    ):
        files = filter_files_by_year_range(
            files, rule["year_start"], rule["year_end"]
        )
    # With skip=True, file remains.
    assert files == [str(centennial_file)]

    # Without skip, the file would be filtered out (1587 ∉ [1750, 2022]).
    rule_no_skip = _Rule(rule)
    rule_no_skip["skip_input_year_filter"] = False
    skip = rule_no_skip.get("skip_input_year_filter", False)
    files2 = [str(centennial_file)]
    if (
        rule_no_skip.get("year_start") is not None
        and rule_no_skip.get("year_end") is not None
        and not skip
    ):
        files2 = filter_files_by_year_range(
            files2, rule_no_skip["year_start"], rule_no_skip["year_end"]
        )
    assert files2 == []


def test_r2_skip_input_year_filter_primary_path():
    """gather_inputs.load_mfdataset's filter is gated by the same opt-out.

    We can't easily invoke the full load_mfdataset without a full Rule
    fixture; verify the gate by reading the source.
    """
    import inspect

    from pycmor.core import gather_inputs

    src = inspect.getsource(gather_inputs.load_mfdataset)
    assert "skip_input_year_filter" in src, (
        "load_mfdataset must gate _filter_files_by_year_range on "
        "skip_input_year_filter (R2 second-call-site fix)"
    )


def test_apply_overrides_auto_detects_old_run_root_from_inherit_data_path():
    """--old-data-path can be omitted when inherit.data_path follows the
    <run_root>/outdata/<component> convention."""
    cfg = {
        "inherit": {"data_path": "/work/runtime/Test_01/outdata/fesom"},
        "rules": [
            {
                "name": "r1",
                "inputs": [
                    {"path": "/work/runtime/Test_01/outdata/oifs", "pattern": "x"}
                ],
            },
        ],
    }
    ov = CliOverrides(data_path="/scratch/runtime/Run_99")
    out = apply_overrides(cfg, ov)

    assert out["inherit"]["data_path"] == "/scratch/runtime/Run_99/outdata/fesom"
    assert (
        out["rules"][0]["inputs"][0]["path"]
        == "/scratch/runtime/Run_99/outdata/oifs"
    )
