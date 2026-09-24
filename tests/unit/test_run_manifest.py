"""Tests for making failed or missing output visible to production tooling.

Covers the three pieces that let a year be driven without a human watching:
the per-rule run manifest, the heartbeat that must not go silent while a save
is still running, and the year driver's completeness check.
"""

import importlib.util
import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner
from loguru import logger

from pycmor.core.cmorizer import CMORizer
from pycmor.std_lib.files import _Heartbeat, _record_written_file, _save_progress_bytes

REPO = Path(__file__).resolve().parents[2]


def _rule(name, written=None, skipped=None):
    r = SimpleNamespace(name=name, compound_name=f"ocean.{name}.tavg-u-hxy-sea.mon.glb")
    for path in written or []:
        _record_written_file(r, path)
    if skipped:
        r._skipped_reason = skipped
    return r


def _manifest_for(rules, succeeded, failed, monkeypatch, tmp_path=None):
    fake = SimpleNamespace(rules=rules)
    if tmp_path is not None:
        monkeypatch.setenv("PYCMOR_MANIFEST", str(tmp_path / "m" / "shard.json"))
    else:
        monkeypatch.delenv("PYCMOR_MANIFEST", raising=False)
    CMORizer._write_run_manifest(fake, succeeded, failed)
    return fake.run_report


def test_manifest_classifies_every_outcome(monkeypatch):
    rules = [
        _rule("tos", written=["/out/tos_1859.nc"]),
        _rule("thetao_dec", skipped="1859 does not close a decade"),
        _rule("silent"),  # returned cleanly, wrote nothing
        _rule("boom"),
    ]
    report = _manifest_for(rules, ["tos", "thetao_dec", "silent"], {"boom": RuntimeError("x")}, monkeypatch)

    status = {name: entry["status"] for name, entry in report["rules"].items()}
    assert status == {"tos": "ok", "thetao_dec": "skipped", "silent": "no_output", "boom": "failed"}
    # A gated-off rule is complete; a silent no-op is not.
    assert report["incomplete"] == ["boom", "silent"]
    assert report["n_ok"] == 1 and report["n_skipped"] == 1
    assert report["rules"]["tos"]["files"] == ["/out/tos_1859.nc"]
    assert "RuntimeError" in report["rules"]["boom"]["reason"]


def test_manifest_is_written_atomically_when_requested(monkeypatch, tmp_path):
    report = _manifest_for([_rule("tos", written=["/out/a.nc"])], ["tos"], {}, monkeypatch, tmp_path)
    on_disk = json.loads((tmp_path / "m" / "shard.json").read_text())
    assert on_disk["incomplete"] == [] == report["incomplete"]
    assert not (tmp_path / "m" / "shard.json.tmp").exists()


def test_all_good_run_has_nothing_incomplete(monkeypatch):
    rules = [_rule("a", written=["/o/a.nc"]), _rule("b", written=["/o/b1.nc", "/o/b2.nc"])]
    report = _manifest_for(rules, ["a", "b"], {}, monkeypatch)
    assert report["incomplete"] == []
    assert report["rules"]["b"]["files"] == ["/o/b1.nc", "/o/b2.nc"]


def test_heartbeat_keeps_beating_after_a_stall():
    """cli120: every heartbeat went quiet at once, the log stopped changing,
    and the shard watchdog scancelled a job that was still working."""
    lines = []
    sink = logger.add(lambda m: lines.append(str(m)), level="INFO", format="{message}")
    try:
        hb = _Heartbeat("save_dataset[x]", interval=0.02, watch_path=lambda: 0, timeout_minutes=0.001)
        with hb:
            time.sleep(1.5)
            assert hb._th.is_alive(), "heartbeat thread must survive a stall"
    finally:
        logger.remove(sink)

    warn_idx = [i for i, line in enumerate(lines) if "no I/O progress detected" in line]
    assert len(warn_idx) == 1, "the stall warning should fire once, not every tick"
    after = [line for line in lines[warn_idx[0] + 1 :] if "still running" in line]
    assert after, "heartbeats must continue after the stall so the log stays alive"
    assert all("no I/O progress yet" in line for line in after)
    assert hb.timed_out


def _wait_until(predicate, timeout=5.0):
    # Ticks are only as fast as the log handler, so poll rather than sleep a
    # fixed amount; a fixed 0.1 s was shorter than one Rich-formatted warning.
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def test_heartbeat_recovers_when_io_resumes():
    size = {"n": 0}
    hb = _Heartbeat("save_dataset[y]", interval=0.02, watch_path=lambda: size["n"], timeout_minutes=0.001)
    with hb:
        assert _wait_until(lambda: hb._stalled), "should notice the stall"
        size["n"] = 100
        assert _wait_until(lambda: not hb._stalled), "should clear the stall once bytes land"
    assert not any(t.name == "hb-save_dataset[y]" and t.is_alive() for t in threading.enumerate())


def test_save_progress_counts_tmpfs_staging(tmp_path):
    """The cli120 root cause: saves stage in node-local /tmp and only reach
    the output directory at the end, so watching out_dir alone saw nothing
    for the whole write and every heartbeat reported a stall at 15 min."""
    out_dir, tmpfs = tmp_path / "out", tmp_path / "tmpfs"
    out_dir.mkdir()
    tmpfs.mkdir()
    assert _save_progress_bytes(str(out_dir), str(tmpfs)) == 0

    staged = tmpfs / "pr_tavg-u-hxy-u_1hr_glb_g122_x_185901010030-185912312330.nc.k3j9.tmp"
    staged.write_bytes(b"x" * 1000)
    assert _save_progress_bytes(str(out_dir), str(tmpfs)) == 1000, "staging bytes are progress"

    staged.write_bytes(b"x" * 5000)
    assert _save_progress_bytes(str(out_dir), str(tmpfs)) == 5000, "a growing staged file keeps counting"

    (tmpfs / "prefect.db").write_bytes(b"x" * 99)  # unrelated node-local files are ignored
    (out_dir / "done.nc").write_bytes(b"x" * 7)
    assert _save_progress_bytes(str(out_dir), str(tmpfs)) == 5007


def test_save_progress_survives_missing_dirs(tmp_path):
    assert _save_progress_bytes(str(tmp_path / "nope"), str(tmp_path / "also_nope")) == 0
    assert _save_progress_bytes(None, str(tmp_path / "also_nope")) == 0


@pytest.fixture
def driver():
    spec = importlib.util.spec_from_file_location("run_year_driver", REPO / "examples" / "run_year_driver.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _workdir(tmp_path, shards, manifests):
    for stem in shards:
        tier = stem.rsplit("_shard_", 1)[0]
        (tmp_path / "shards" / tier).mkdir(parents=True, exist_ok=True)
        (tmp_path / "shards" / tier / f"{stem}.yaml").write_text("rules: []\n")
    (tmp_path / "cmorized" / "_manifests").mkdir(parents=True)
    for stem, incomplete in manifests.items():
        (tmp_path / "cmorized" / "_manifests" / f"{stem}.json").write_text(json.dumps({"incomplete": incomplete}))
    return tmp_path


def test_driver_accepts_a_complete_year(driver, tmp_path):
    wd = _workdir(
        tmp_path, ["core_atm_shard_00", "lrcs_ocean_shard_00"], {"core_atm_shard_00": [], "lrcs_ocean_shard_00": []}
    )
    assert driver.assess(wd) == (set(), [])


def test_driver_flags_a_shard_that_died_without_a_manifest(driver, tmp_path):
    """The cli120 case: extra_atm was scancelled and never wrote a manifest."""
    wd = _workdir(tmp_path, ["core_atm_shard_00", "extra_atm_shard_00"], {"core_atm_shard_00": []})
    tiers, problems = driver.assess(wd)
    assert tiers == {"extra_atm"}
    assert "no manifest" in problems[0]


def test_driver_flags_rules_without_output(driver, tmp_path):
    wd = _workdir(tmp_path, ["core_atm_shard_03"], {"core_atm_shard_03": ["pr_1hr", "hurs_1hr"]})
    tiers, problems = driver.assess(wd)
    assert tiers == {"core_atm"}
    assert "hurs_1hr, pr_1hr" in problems[0]


def test_driver_gives_up_after_three_attempts_and_emails(driver, tmp_path, monkeypatch):
    wd = _workdir(tmp_path, ["extra_atm_shard_00"], {})
    submits, mails = [], []
    monkeypatch.setattr(driver, "submit", lambda *a, **k: submits.append(k.get("tier")) or ["1"])
    monkeypatch.setattr(driver, "wait_for", lambda *a, **k: None)
    monkeypatch.setattr(driver, "notify", lambda email, subject, body: mails.append((email, subject, body)) or True)
    monkeypatch.setattr("sys.argv", ["run_year_driver.py", "/run", "1859", str(wd), "--email", "me@example.org"])

    assert driver.main() == 1
    assert submits == [None, "extra_atm", "extra_atm"], "full year first, then only the failing tier"
    assert len(mails) == 1
    email, subject, body = mails[0]
    assert email == "me@example.org" and "1859" in subject and "extra_atm_shard_00" in body
    assert not (wd / "1859.done").exists()


def test_driver_marks_the_year_done_once_a_retry_fills_the_gap(driver, tmp_path, monkeypatch):
    wd = _workdir(tmp_path, ["extra_atm_shard_00"], {})
    manifest = wd / "cmorized" / "_manifests" / "extra_atm_shard_00.json"
    calls = []

    def fake_submit(*a, **k):
        calls.append(k.get("tier"))
        if len(calls) == 2:  # the retry succeeds
            manifest.write_text(json.dumps({"incomplete": []}))
        return ["1"]

    monkeypatch.setattr(driver, "submit", fake_submit)
    monkeypatch.setattr(driver, "wait_for", lambda *a, **k: None)
    monkeypatch.setattr(driver, "notify", lambda *a: pytest.fail("must not email when a retry succeeds"))
    monkeypatch.setattr("sys.argv", ["run_year_driver.py", "/run", "1859", str(wd)])

    assert driver.main() == 0
    assert calls == [None, "extra_atm"]
    assert (wd / "1859.done").exists()


@pytest.mark.parametrize("incomplete, exit_code", [([], 0), (["pr_1hr"], 1)])
def test_process_exit_code_reflects_missing_output(monkeypatch, tmp_path, incomplete, exit_code):
    """A shard whose rules all failed used to exit 0 and look COMPLETED."""
    import pycmor.cli as cli_mod
    import pycmor.core.banner as banner
    import pycmor.core.env_check as env_check

    class FakeCMORizer:
        _cluster = None

        def process(self):
            self.run_report = {"n_rules": 3, "incomplete": incomplete}

    monkeypatch.setattr(cli_mod.CMORizer, "from_dict", staticmethod(lambda cfg: FakeCMORizer()))
    monkeypatch.setattr(cli_mod, "Client", lambda *a, **k: None)
    monkeypatch.setattr(banner, "show_banner", lambda: None)
    monkeypatch.setattr(env_check, "run_env_check", lambda: None)

    cfg = tmp_path / "c.yaml"
    cfg.write_text("general: {}\n")
    result = CliRunner().invoke(cli_mod.cli, ["process", str(cfg)])
    assert result.exit_code == exit_code, result.output
