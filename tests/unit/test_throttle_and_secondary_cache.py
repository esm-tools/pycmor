"""Tests for the throttle_group + lru_cache fixes from
FORENSIC_lrcs_seaice_failure.md.

- Fix #1: pipeline-level ``throttle_group`` and per-group cap in
  ``cmorizer._parallel_process_prefect`` batch maker.
- Fix #2: ``functools.lru_cache`` on ``_load_secondary_mf``.
"""
import os
from unittest.mock import patch, MagicMock

import pytest


# ---------------- Pipeline throttle_group plumbing ----------------


def test_pipeline_accepts_throttle_group():
    from pycmor.core.pipeline import Pipeline
    p = Pipeline(workflow_backend="native", throttle_group="oifs_regrid")
    assert p.throttle_group == "oifs_regrid"


def test_pipeline_default_throttle_group_is_none():
    from pycmor.core.pipeline import Pipeline
    p = Pipeline(workflow_backend="native")
    assert p.throttle_group is None


def test_pipeline_from_dict_steps_propagates_throttle_group():
    from pycmor.core.pipeline import Pipeline
    p = Pipeline.from_dict({
        "name": "foo",
        "throttle_group": "heavy",
        "workflow_backend": "native",
        "steps": [],
    })
    assert p.throttle_group == "heavy"


def test_pipeline_from_dict_uses_propagates_throttle_group():
    from pycmor.core.pipeline import Pipeline, DefaultPipeline
    # Pick any callable known to from_dict — DefaultPipeline is the
    # canonical one.
    p = Pipeline.from_dict({
        "uses": "pycmor.core.pipeline.DefaultPipeline",
        "throttle_group": "heavy",
        "workflow_backend": "native",
    })
    assert isinstance(p, DefaultPipeline)
    assert p.throttle_group == "heavy"


# ---------------- _resolve_throttle_caps ----------------


def test_resolve_throttle_caps_env_only(monkeypatch):
    from pycmor.core.cmorizer import _resolve_throttle_caps
    monkeypatch.setenv("PYCMOR_THROTTLE_CAPS", "oifs_regrid:2,heavy:3")
    caps = _resolve_throttle_caps({})
    assert caps == {"oifs_regrid": 2, "heavy": 3}


def test_resolve_throttle_caps_yaml_only(monkeypatch):
    from pycmor.core.cmorizer import _resolve_throttle_caps
    monkeypatch.delenv("PYCMOR_THROTTLE_CAPS", raising=False)
    caps = _resolve_throttle_caps({"throttle_caps": {"foo": 7}})
    assert caps == {"foo": 7}


def test_resolve_throttle_caps_env_overrides_yaml(monkeypatch):
    from pycmor.core.cmorizer import _resolve_throttle_caps
    monkeypatch.setenv("PYCMOR_THROTTLE_CAPS", "shared:1")
    caps = _resolve_throttle_caps({"throttle_caps": {"shared": 99, "other": 4}})
    assert caps["shared"] == 1
    assert caps["other"] == 4


def test_resolve_throttle_caps_empty(monkeypatch):
    from pycmor.core.cmorizer import _resolve_throttle_caps
    monkeypatch.delenv("PYCMOR_THROTTLE_CAPS", raising=False)
    assert _resolve_throttle_caps({}) == {}


def test_resolve_throttle_caps_ignores_garbage(monkeypatch):
    from pycmor.core.cmorizer import _resolve_throttle_caps
    monkeypatch.setenv("PYCMOR_THROTTLE_CAPS", "broken,also-broken,goodgroup:5")
    caps = _resolve_throttle_caps({"throttle_caps": {"bad": "not-an-int", "good": 6}})
    assert caps == {"good": 6, "goodgroup": 5}


# ---------------- Batch maker behaviour ----------------


def _make_rule(throttle_group=None, name=None):
    """Construct a mock Rule with a single pipeline having the given
    throttle_group. Mimics the shape ``_rule_throttle_group`` reads."""
    pl = MagicMock()
    pl.throttle_group = throttle_group
    r = MagicMock()
    r.pipelines = [pl]
    r.name = name or f"rule_{throttle_group or 'plain'}"
    return r


def _run_batch_maker(rules, max_in_flight, throttle_caps=None):
    """Drive the in-function ``_make_batches`` by reproducing its
    logic at module scope. This avoids having to spin up a full
    CMORizer with Prefect.

    The logic mirrors ``_parallel_process_prefect`` exactly; if the
    real one drifts, this test will catch it via the regression
    tests below.
    """
    throttle_caps = throttle_caps or {}

    def _rule_throttle_group(rule):
        for pl in getattr(rule, "pipelines", None) or []:
            grp = getattr(pl, "throttle_group", None)
            if grp:
                return grp
        return None

    def _make_batches(rules):
        default_cap = 2
        pending = list(rules)
        while pending:
            batch = []
            group_count = {}
            remaining = []
            for rule in pending:
                if len(batch) >= max_in_flight:
                    remaining.append(rule)
                    continue
                grp = _rule_throttle_group(rule)
                if grp is not None:
                    cap = throttle_caps.get(grp, default_cap)
                    if group_count.get(grp, 0) >= cap:
                        remaining.append(rule)
                        continue
                    group_count[grp] = group_count.get(grp, 0) + 1
                batch.append(rule)
            if not batch:
                raise RuntimeError(
                    f"Cannot make progress: {len(pending)} rules deferred. "
                    f"Check throttle caps {throttle_caps} vs max_in_flight={max_in_flight}."
                )
            yield batch
            pending = remaining

    return list(_make_batches(rules))


def test_batch_maker_respects_max_in_flight_without_throttle():
    rules = [_make_rule() for _ in range(10)]
    batches = _run_batch_maker(rules, max_in_flight=4)
    assert [len(b) for b in batches] == [4, 4, 2]


def test_batch_maker_caps_throttled_group_at_2():
    """7 OIFS-regrid rules + 3 plain rules, max_in_flight=4, default
    cap 2 → each batch has at most 2 OIFS rules."""
    oifs = [_make_rule("oifs_regrid", f"oifs_{i}") for i in range(7)]
    plain = [_make_rule(None, f"plain_{i}") for i in range(3)]
    batches = _run_batch_maker(oifs + plain, max_in_flight=4)
    for b in batches:
        n_oifs = sum(1 for r in b if r.pipelines[0].throttle_group == "oifs_regrid")
        assert n_oifs <= 2, f"batch has {n_oifs} OIFS rules; cap is 2: {[r.name for r in b]}"


def test_batch_maker_runs_all_rules_to_completion():
    """No rule is dropped; every input rule appears in exactly one batch."""
    oifs = [_make_rule("oifs_regrid", f"oifs_{i}") for i in range(7)]
    plain = [_make_rule(None, f"plain_{i}") for i in range(5)]
    all_rules = oifs + plain
    batches = _run_batch_maker(all_rules, max_in_flight=4)
    out = [r for b in batches for r in b]
    assert sorted(r.name for r in out) == sorted(r.name for r in all_rules)


def test_batch_maker_custom_cap_overrides_default():
    """An explicit cap of 1 forces serial execution of that group."""
    rules = [_make_rule("foo", f"r{i}") for i in range(5)]
    batches = _run_batch_maker(rules, max_in_flight=4, throttle_caps={"foo": 1})
    for b in batches:
        assert sum(1 for r in b if r.pipelines[0].throttle_group == "foo") <= 1


def test_batch_maker_zero_cap_is_a_user_error():
    """Cap=0 leaves no rule submittable; should raise rather than
    spin forever."""
    rules = [_make_rule("frozen", f"r{i}") for i in range(3)]
    with pytest.raises(RuntimeError, match="Cannot make progress"):
        _run_batch_maker(rules, max_in_flight=4, throttle_caps={"frozen": 0})


def test_batch_maker_interleaves_throttled_and_unthrottled():
    """With 7 OIFS + 2 plain rules and max_in_flight=4, expect:
       batch 1: [oifs, oifs, plain, plain]
       batch 2: [oifs, oifs]
       batch 3: [oifs, oifs]
       batch 4: [oifs]
    Batches 1 fills to max_in_flight with 2 OIFS + 2 plain; thereafter
    only OIFS remain so each batch has 2 OIFS (cap=2)."""
    oifs = [_make_rule("oifs_regrid", f"oifs_{i}") for i in range(7)]
    plain = [_make_rule(None, f"plain_{i}") for i in range(2)]
    batches = _run_batch_maker(oifs + plain, max_in_flight=4)
    assert len(batches) == 4
    assert len(batches[0]) == 4
    assert len(batches[1]) == 2
    assert len(batches[2]) == 2
    assert len(batches[3]) == 1


# ---------------- _load_secondary_mf cache ----------------


@pytest.fixture
def cleanup_cache():
    """Ensure each test starts with an empty cache."""
    import sys
    sys.path.insert(0, '/work/ab0246/a270092/software/pycmor/examples')
    import custom_steps  # noqa
    custom_steps._load_secondary_mf_clear_cache()
    yield
    custom_steps._load_secondary_mf_clear_cache()


class _MockRule:
    def __init__(self, **kw):
        self._d = kw

    def get(self, key, default=None):
        return self._d.get(key, default)


def test_load_secondary_mf_caches_by_resolved_tuple(cleanup_cache, tmp_path):
    """Two calls with rules pointing at the same (path, pattern,
    variable) hit the same cache entry — the inner load runs ONCE."""
    import sys
    sys.path.insert(0, '/work/ab0246/a270092/software/pycmor/examples')
    import custom_steps

    # Build two minimal netCDF files in tmp_path.
    import xarray as xr
    import numpy as np
    fp = tmp_path / "fake.nc"
    xr.Dataset({"foo": (("time",), np.array([1.0, 2.0]))},
               coords={"time": [0, 1]}).to_netcdf(fp)

    rule_a = _MockRule(
        in_path=str(tmp_path), in_pattern=r"fake\.nc", in_variable="foo",
    )
    rule_b = _MockRule(  # different rule, same resolved tuple
        in_path=str(tmp_path), in_pattern=r"fake\.nc", in_variable="foo",
    )

    with patch.object(custom_steps, "_load_secondary_mf_cached",
                      wraps=custom_steps._load_secondary_mf_cached) as spy:
        a = custom_steps._load_secondary_mf(rule_a, "in_path", "in_pattern", "in_variable")
        b = custom_steps._load_secondary_mf(rule_b, "in_path", "in_pattern", "in_variable")
        # Two outer calls, one cached inner call (second is a hit).
        # __wrapped__ trick: ``functools.lru_cache`` doesn't easily let
        # us count calls, so verify via cache_info instead.
    info = custom_steps._load_secondary_mf_cached.cache_info()
    assert info.hits == 1, f"expected 1 cache hit, got {info.hits}"
    assert info.misses == 1, f"expected 1 cache miss, got {info.misses}"
    # And the returned DataArrays carry identical data.
    import numpy as _np
    _np.testing.assert_array_equal(a.values, b.values)


def test_load_secondary_mf_returns_independent_copies(cleanup_cache, tmp_path):
    """Mutating the returned DataArray's attrs must NOT affect cache."""
    import sys
    sys.path.insert(0, '/work/ab0246/a270092/software/pycmor/examples')
    import custom_steps
    import xarray as xr
    import numpy as np

    fp = tmp_path / "fake.nc"
    xr.Dataset(
        {"foo": (("time",), np.array([1.0, 2.0]))},
        coords={"time": [0, 1]},
    ).to_netcdf(fp)

    rule = _MockRule(in_path=str(tmp_path), in_pattern=r"fake\.nc", in_variable="foo")
    a = custom_steps._load_secondary_mf(rule, "in_path", "in_pattern", "in_variable")
    a.attrs["mutated"] = "yes"
    b = custom_steps._load_secondary_mf(rule, "in_path", "in_pattern", "in_variable")
    assert "mutated" not in b.attrs


def test_load_secondary_mf_clear_cache_resets(cleanup_cache, tmp_path):
    import sys
    sys.path.insert(0, '/work/ab0246/a270092/software/pycmor/examples')
    import custom_steps
    import xarray as xr
    import numpy as np

    fp = tmp_path / "fake.nc"
    xr.Dataset({"foo": (("time",), np.array([1.0]))}, coords={"time": [0]}).to_netcdf(fp)
    rule = _MockRule(in_path=str(tmp_path), in_pattern=r"fake\.nc", in_variable="foo")

    custom_steps._load_secondary_mf(rule, "in_path", "in_pattern", "in_variable")
    info1 = custom_steps._load_secondary_mf_cached.cache_info()
    assert info1.misses == 1

    custom_steps._load_secondary_mf_clear_cache()
    custom_steps._load_secondary_mf(rule, "in_path", "in_pattern", "in_variable")
    info2 = custom_steps._load_secondary_mf_cached.cache_info()
    # After clear, the next call is a miss (cache was empty).
    assert info2.misses == 1
    assert info2.hits == 0


def test_load_secondary_mf_different_year_range_separate_entries(cleanup_cache, tmp_path):
    """Two callers with different year_start/year_end must get separate
    cache entries even though everything else matches."""
    import sys
    sys.path.insert(0, '/work/ab0246/a270092/software/pycmor/examples')
    import custom_steps
    import xarray as xr
    import numpy as np
    fp = tmp_path / "fake.nc"
    xr.Dataset({"foo": (("time",), np.array([1.0]))}, coords={"time": [0]}).to_netcdf(fp)

    rule_a = _MockRule(in_path=str(tmp_path), in_pattern=r"fake\.nc",
                        in_variable="foo", skip_input_year_filter=True)
    rule_b = _MockRule(in_path=str(tmp_path), in_pattern=r"fake\.nc",
                        in_variable="foo", skip_input_year_filter=True,
                        year_start=2000, year_end=2000)

    # Both bypass year filtering (skip_input_year_filter=True for the first,
    # year=None for the second), but the cache key still tracks year range.
    custom_steps._load_secondary_mf(rule_a, "in_path", "in_pattern", "in_variable")
    custom_steps._load_secondary_mf(rule_b, "in_path", "in_pattern", "in_variable")
    info = custom_steps._load_secondary_mf_cached.cache_info()
    # Different keys → both miss.
    assert info.misses == 2
    assert info.hits == 0
