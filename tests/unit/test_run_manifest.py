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


def test_driver_stops_at_once_when_every_tier_fails(driver, tmp_path, monkeypatch):
    """cli122: a bookkeeping bug marked every tier incomplete, and the driver
    recomputed a complete year twice before giving up. All tiers failing
    together is systematic, so it gives up after the first attempt."""
    stems = ["core_atm_shard_00", "core_atm_shard_01", "lrcs_ocean_shard_00", "veg_land_shard_00"]
    wd = _workdir(tmp_path, stems, {s: ["x"] for s in stems})
    submits, mails = [], []
    monkeypatch.setattr(driver, "submit", lambda *a, **k: submits.append(k.get("tier")) or ["1"])
    monkeypatch.setattr(driver, "wait_for", lambda *a, **k: None)
    monkeypatch.setattr(driver, "notify", lambda email, subject, body: mails.append((subject, body)) or True)
    monkeypatch.setattr("sys.argv", ["run_year_driver.py", "/run", "1852", str(wd), "--email", "me@example.org"])

    assert driver.main() == 1
    assert submits == [None], "no retries"
    assert len(mails) == 1 and "systematic" in mails[0][0]
    assert "not retried" in (wd / "1852.FAILED").read_text()


def test_driver_still_retries_when_one_tier_survives(driver, tmp_path, monkeypatch):
    stems = ["core_atm_shard_00", "lrcs_ocean_shard_00", "veg_land_shard_00"]
    wd = _workdir(tmp_path, stems, {"core_atm_shard_00": [], "lrcs_ocean_shard_00": ["x"], "veg_land_shard_00": ["y"]})
    submits = []
    monkeypatch.setattr(driver, "submit", lambda *a, **k: submits.append(k.get("tier")) or ["1"])
    monkeypatch.setattr(driver, "wait_for", lambda *a, **k: None)
    monkeypatch.setattr("sys.argv", ["run_year_driver.py", "/run", "1852", str(wd), "--email", ""])

    assert driver.main() == 1
    assert submits == [None, "lrcs_ocean", "veg_land", "lrcs_ocean", "veg_land"]
    assert "3 attempts" in (wd / "1852.FAILED").read_text()


def _year_with_output(tmp_path):
    wd = _workdir(tmp_path / "y1850", ["core_atm_shard_00", "veg_land_shard_00"], {})
    drs = wd / "cmorized" / "MIP-DRS7" / "CMIP7" / "CMIP" / "AWI" / "M" / "1pctCO2" / "r1i1p1f1" / "glb"
    (drs / "mon" / "tas").mkdir(parents=True)
    (drs / "mon" / "tas" / "tas_1850.nc").write_text("tas")
    (drs / "fx" / "areacella").mkdir(parents=True)
    (drs / "fx" / "areacella" / "areacella.nc").write_text("new")
    (wd / "cmorized" / "qc_atmos.tas.json").write_text("{}")
    return wd


def test_publish_moves_a_finished_year_into_the_shared_tree(driver, tmp_path):
    wd = _year_with_output(tmp_path)
    out = tmp_path / "esgf"
    fx = out / "MIP-DRS7" / "CMIP7" / "CMIP" / "AWI" / "M" / "1pctCO2" / "r1i1p1f1" / "glb" / "fx" / "areacella"
    fx.mkdir(parents=True)
    (fx / "areacella.nc").write_text("old")  # written by an earlier year

    assert driver.publish(wd, out) == (2, 1)
    assert (fx / "areacella.nc").read_text() == "new"
    assert (out / fx.relative_to(out).parent.parent / "mon" / "tas" / "tas_1850.nc").read_text() == "tas"
    assert not list((wd / "cmorized").rglob("*.nc")), "nothing left behind"
    assert (wd / "cmorized" / "qc_atmos.tas.json").exists(), "QC reports stay with the year, out of the ESGF tree"
    assert not list(out.rglob("*.json"))


def test_publish_copies_when_rename_crosses_project_quotas(driver, tmp_path, monkeypatch):
    import errno
    import os

    wd = _year_with_output(tmp_path)
    real_replace = os.replace

    def no_cross_rename(src, dst):
        if str(src).startswith(str(wd)):
            raise OSError(errno.EXDEV, "Invalid cross-device link")
        return real_replace(src, dst)

    monkeypatch.setattr(driver.os, "replace", no_cross_rename)
    assert driver.publish(wd, tmp_path / "esgf") == (2, 0)
    assert len(list((tmp_path / "esgf").rglob("*.nc"))) == 2
    assert not list((tmp_path / "esgf").rglob("*.publishing"))


def test_restart_of_a_complete_year_publishes_without_recomputing(driver, tmp_path, monkeypatch):
    """A chain killed after the shards finished (or while publishing) must
    not spend another year of node hours on the restart."""
    wd = _year_with_output(tmp_path)
    for stem in ("core_atm_shard_00", "veg_land_shard_00"):
        (wd / "cmorized" / "_manifests" / f"{stem}.json").write_text(json.dumps({"incomplete": []}))
    monkeypatch.setattr(driver, "submit", lambda *a, **k: pytest.fail("a complete year is not resubmitted"))
    out = tmp_path / "esgf"
    monkeypatch.setattr("sys.argv", ["run_year_driver.py", "/run", "1850", str(wd), "--publish-to", str(out)])

    assert driver.main() == 0
    assert "published 2 file(s)" in (wd / "1850.done").read_text()
    assert len(list(out.rglob("*.nc"))) == 2


def test_a_fresh_year_is_submitted_even_without_manifests(driver, tmp_path, monkeypatch):
    submits = []
    monkeypatch.setattr(driver, "submit", lambda *a, **k: submits.append(k.get("tier")) or [])
    monkeypatch.setattr(driver, "wait_for", lambda *a, **k: None)
    monkeypatch.setattr("sys.argv", ["run_year_driver.py", "/run", "1850", str(tmp_path / "y1850"), "--email", ""])
    driver.main()
    assert submits[0] is None, "no shards yet must not look like a complete year"


def test_a_stale_manifest_cannot_vouch_for_a_shard_that_died(driver, tmp_path, monkeypatch):
    """test-1pctCO2 y1850: extra_land_shard_01 hit its walltime on the rerun
    and wrote no manifest, but its manifest from the run three days earlier
    still said "all ok". The year was declared complete and published with
    two files from the earlier run. The real submit() is used here, with a
    stand-in for the submit script that lets one shard die."""
    stems = ["core_atm_shard_00", "extra_land_shard_00", "extra_land_shard_01"]
    wd = _workdir(tmp_path, stems, {"core_atm_shard_00": ["x"], "extra_land_shard_00": [], "extra_land_shard_01": []})
    mdir = wd / "cmorized" / "_manifests"
    fake = tmp_path / "fake_submit.sh"
    fake.write_text(
        "#!/bin/bash\n"
        f"for s in core_atm_shard_00 extra_land_shard_00; do echo '{{\"incomplete\": []}}' > {mdir}/$s.json; done\n"
        "echo 'submitted x jid=1'\n"
    )
    fake.chmod(0o755)
    monkeypatch.setattr(driver, "SUBMIT", fake)
    monkeypatch.setattr(driver, "wait_for", lambda *a, **k: None)
    monkeypatch.setattr("sys.argv", ["run_year_driver.py", "/run", "1850", str(wd), "--attempts", "1", "--email", ""])

    assert driver.main() == 1
    assert "extra_land_shard_01: no manifest" in (wd / "1850.FAILED").read_text()
    assert not (wd / "1850.done").exists()


def test_resubmitting_one_tier_clears_only_its_manifests(driver, tmp_path):
    stems = ["core_atm_shard_00", "extra_land_shard_00", "extra_land_shard_01"]
    wd = _workdir(tmp_path, stems, {s: [] for s in stems})
    assert driver.clear_manifests(wd, "extra_land") == 2
    assert [p.stem for p in (wd / "cmorized" / "_manifests").glob("*.json")] == ["core_atm_shard_00"]
    assert driver.clear_manifests(wd) == 1


def test_resubmitting_a_tier_also_clears_its_gr_variant(driver, tmp_path):
    """TIER=extra_land resubmits extra_land_gr as well (the submit script
    globs *TIER*), so its manifests must go with it."""
    stems = ["extra_land_shard_00", "extra_land_gr_shard_00", "core_land_shard_00"]
    wd = _workdir(tmp_path, stems, {s: [] for s in stems})
    assert driver.clear_manifests(wd, "extra_land") == 2
    assert [p.stem for p in (wd / "cmorized" / "_manifests").glob("*.json")] == ["core_land_shard_00"]


def test_a_failed_submission_cancels_what_it_queued(driver, tmp_path, monkeypatch):
    """test-1pctCO2 y1850 with WITH_GR=yes: the submit script queued 22 tiers,
    then died on the empty veg_seaice_gr. The driver exited and left the 22
    arrays running with nothing to assess them, and wrote no .FAILED."""
    wd = _workdir(tmp_path, ["core_atm_shard_00"], {})
    fake = tmp_path / "fake_submit.sh"
    fake.write_text("#!/bin/bash\necho '  submitted a jid=101'\necho '  submitted b jid=102'\nexit 2\n")
    fake.chmod(0o755)
    monkeypatch.setattr(driver, "SUBMIT", fake)
    cancelled = []
    real_run = driver.subprocess.run

    def run(cmd, *a, **k):
        if cmd[0] == "scancel":
            cancelled.extend(cmd[1:])
            return SimpleNamespace(returncode=0)
        return real_run(cmd, *a, **k)

    monkeypatch.setattr(driver.subprocess, "run", run)

    with pytest.raises(SystemExit, match="rc=2"):
        driver.submit("/run", "1850", wd)
    assert cancelled == ["101", "102"]
    failed = (wd / "1850.FAILED").read_text()
    assert "Jobs cancelled: 101, 102" in failed and "jid=102" in failed


def test_a_manifest_from_other_code_does_not_count(driver, tmp_path, monkeypatch):
    """A year restarted after a repin must not be accepted on the strength of
    what the previous code wrote."""
    wd = _workdir(tmp_path, ["core_atm_shard_00", "veg_land_shard_00"], {})
    mdir = wd / "cmorized" / "_manifests"
    (mdir / "core_atm_shard_00.json").write_text(json.dumps({"incomplete": [], "code_commit": "b" * 40}))
    (mdir / "veg_land_shard_00.json").write_text(json.dumps({"incomplete": [], "code_commit": "a" * 40}))
    monkeypatch.setenv("PYCMOR_CODE_COMMIT", "b" * 40)
    tiers, problems = driver.assess(wd)
    assert tiers == {"veg_land"} and "pinned to bbbbbbbbbb" in problems[0]
    monkeypatch.delenv("PYCMOR_CODE_COMMIT")
    assert driver.assess(wd) == (set(), []), "outside a campaign there is no pin to compare with"


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


def test_driver_leaves_details_and_stays_quiet_when_slurm_mails(driver, tmp_path, monkeypatch):
    """Under run_year_chain.sbatch SLURM sends the one FAIL mail; the driver
    must not add its own, and must leave the details where the mail points."""
    wd = _workdir(tmp_path, ["extra_atm_shard_00"], {})
    monkeypatch.setattr(driver, "submit", lambda *a, **k: ["1"])
    monkeypatch.setattr(driver, "wait_for", lambda *a, **k: None)
    monkeypatch.setattr(driver, "notify", lambda *a: pytest.fail("SLURM does the mailing"))
    monkeypatch.setattr("sys.argv", ["run_year_driver.py", "/run", "1859", str(wd), "--email", ""])

    assert driver.main() == 1
    failed = (wd / "1859.FAILED").read_text()
    assert "extra_atm_shard_00" in failed and "3 attempts" in failed


def test_driver_clears_an_old_failure_once_the_year_completes(driver, tmp_path, monkeypatch):
    wd = _workdir(tmp_path, ["core_atm_shard_00"], {"core_atm_shard_00": []})
    (wd / "1859.FAILED").write_text("from an earlier chain\n")
    monkeypatch.setattr(driver, "submit", lambda *a, **k: ["1"])
    monkeypatch.setattr(driver, "wait_for", lambda *a, **k: None)
    monkeypatch.setattr("sys.argv", ["run_year_driver.py", "/run", "1859", str(wd), "--email", ""])

    assert driver.main() == 0
    assert (wd / "1859.done").exists() and not (wd / "1859.FAILED").exists()


def test_driver_skips_a_finished_year_without_submitting(driver, tmp_path, monkeypatch):
    """Restarting a chain from an earlier year must not redo finished years."""
    (tmp_path / "1859.done").write_text("ok\n")
    monkeypatch.setattr(driver, "submit", lambda *a, **k: pytest.fail("finished years are not resubmitted"))
    monkeypatch.setattr("sys.argv", ["run_year_driver.py", "/run", "1859", str(tmp_path)])
    assert driver.main() == 0


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


def _fake_worker_process_rule(rule):
    """Stand-in for CMORizer._process_rule as it behaves on a dask worker:
    it acts on a copy of the rule, so nothing it records is visible on the
    driver's rule object. Only the return value comes back."""
    import copy

    from pycmor.core.cmorizer import _rule_outcome

    rule = copy.deepcopy(rule)
    if rule.name == "boom":
        raise RuntimeError("worker blew up")
    if rule.name == "gated":
        rule._skipped_reason = "1852 does not close a decade"
    elif rule.name != "silent":
        _record_written_file(rule, f"/out/{rule.name}.nc")
    return _rule_outcome(rule, rule.name)


def test_dask_path_writes_the_manifest_from_worker_results(monkeypatch, tmp_path):
    """Under the dask backend the rule copies on the workers are the only ones
    that see the writes, so the manifest must come from the returned
    outcomes."""
    from dask.distributed import Client, LocalCluster

    manifest = tmp_path / "shard.json"
    monkeypatch.setenv("PYCMOR_MANIFEST", str(manifest))

    fake = CMORizer.__new__(CMORizer)
    fake.rules = [SimpleNamespace(name=n, compound_name=None) for n in ("tos", "gated", "silent", "boom")]
    fake._pymor_cfg = {"dask_n_workers": 1, "dask_threads_per_worker": 2}
    fake._process_rule = _fake_worker_process_rule
    fake._cleanup_dask_workers = lambda: None

    with LocalCluster(n_workers=1, threads_per_worker=2, processes=False, dashboard_address=None) as cluster:
        with Client(cluster) as client:
            CMORizer._parallel_process_dask(fake, external_client=client)

    # Nothing leaked back onto the driver's rules, so this is the real test.
    assert not any(getattr(r, "_written_files", None) for r in fake.rules)
    on_disk = json.loads(manifest.read_text())
    status = {n: e["status"] for n, e in on_disk["rules"].items()}
    assert status == {"tos": "ok", "gated": "skipped", "silent": "no_output", "boom": "failed"}
    assert on_disk["rules"]["tos"]["files"] == ["/out/tos.nc"]
    assert on_disk["incomplete"] == ["boom", "silent"]
    assert fake.run_report["incomplete"] == ["boom", "silent"]


def test_production_dispatch_writes_the_manifest(monkeypatch, tmp_path):
    """cli121/cli122: run_hr_shard.sh asks for ``pipeline_orchestrator: dask``,
    but that key is not in the config schema, so ``process()`` falls through
    to the prefect path. The manifest was hooked into the other two paths and
    no production shard wrote one. Go through the real config and dispatch so
    this test follows whatever path production takes."""
    from prefect import task

    from pycmor.core.config import PycmorConfigManager

    manifest = tmp_path / "shard.json"
    monkeypatch.setenv("PYCMOR_MANIFEST", str(manifest))

    fake = CMORizer.__new__(CMORizer)
    fake.rules = [SimpleNamespace(name=n, compound_name=None) for n in ("tos", "gated", "silent", "boom")]
    fake._pymor_cfg = PycmorConfigManager.from_pycmor_cfg(
        {"parallel": True, "pipeline_orchestrator": "dask", "dask_n_workers": 2, "dask_threads_per_worker": 1}
    )
    fake._process_rule = task(name="Process rule")(_fake_worker_process_rule)
    fake.pipelines = []
    fake._match_pipelines_in_rules = lambda: None
    fake._cleanup_dask_workers = lambda: None
    fake._cluster = None

    with pytest.raises(RuntimeError, match="worker blew up"):
        CMORizer.process(fake)

    on_disk = json.loads(manifest.read_text())
    status = {n: e["status"] for n, e in on_disk["rules"].items()}
    assert status == {"tos": "ok", "gated": "skipped", "silent": "no_output", "boom": "failed"}
    assert on_disk["rules"]["tos"]["files"] == ["/out/tos.nc"]
    assert on_disk["incomplete"] == ["boom", "silent"]


def test_rules_that_never_report_count_as_failed(monkeypatch):
    """An aborted run must not produce a manifest that looks clean."""
    rules = [_rule("tos", written=["/out/tos.nc"]), _rule("never")]
    report = _manifest_for(rules, ["tos"], {}, monkeypatch)
    assert report["rules"]["never"]["status"] == "failed"
    assert report["incomplete"] == ["never"]


def test_drs_version_is_pinned_by_inherit_or_env(monkeypatch):
    """cli121: the version directory was the date of each write, so one run
    straddling midnight produced two versions of every dataset."""
    import datetime

    from pycmor.std_lib.global_attributes import drs_version

    monkeypatch.delenv("PYCMOR_DRS_VERSION", raising=False)
    assert drs_version({"directory_date": "v20260925"}) == "v20260925"
    assert drs_version({"directory_date": 20260925}) == "v20260925", "a bare yaml date gets its v"
    assert drs_version({"directory_date": datetime.date(2026, 9, 25)}) == "v20260925", "unquoted yaml date"

    monkeypatch.setenv("PYCMOR_DRS_VERSION", "v20260101")
    assert drs_version({}) == "v20260101"
    assert drs_version({"directory_date": "v20260925"}) == "v20260925", "an explicit rule setting wins"

    monkeypatch.setenv("PYCMOR_DRS_VERSION", "2026-09-25")
    with pytest.raises(ValueError, match="vYYYYMMDD"):
        drs_version({})


def test_drs_version_falls_back_to_today(monkeypatch):
    import datetime

    from pycmor.std_lib.global_attributes import drs_version

    monkeypatch.delenv("PYCMOR_DRS_VERSION", raising=False)
    assert drs_version(None) == datetime.datetime.today().strftime("v%Y%m%d")


def test_driver_keeps_one_drs_version_across_attempts(driver, tmp_path, monkeypatch):
    wd = _workdir(tmp_path, ["extra_atm_shard_00"], {})
    monkeypatch.delenv("PYCMOR_DRS_VERSION", raising=False)
    seen = []
    monkeypatch.setattr(driver, "submit", lambda *a, **k: seen.append(driver.os.environ["PYCMOR_DRS_VERSION"]) or ["1"])
    monkeypatch.setattr(driver, "wait_for", lambda *a, **k: None)
    monkeypatch.setattr("sys.argv", ["run_year_driver.py", "/run", "1852", str(wd), "--email", ""])

    driver.main()
    assert len(seen) == 3 and len(set(seen)) == 1, seen
