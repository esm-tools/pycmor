"""Campaign setup: one pinned code copy and one config file per experiment."""

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def campaign():
    spec = importlib.util.spec_from_file_location("campaign", REPO / "examples" / "campaign.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _git(*args, cwd):
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
    )


@pytest.fixture
def code_repo(tmp_path):
    repo = tmp_path / "src_repo"
    repo.mkdir()
    _git("init", "-q", "-b", "main", cwd=repo)
    (repo / "README").write_text("v1\n")
    _git("add", "README", cwd=repo)
    _git("commit", "-q", "-m", "one", cwd=repo)
    return repo


def _write_campaign(tmp_path, code_repo, **over):
    cdir = tmp_path / "1pctCO2"
    cdir.mkdir(exist_ok=True)
    cfg = {
        "name": "1pctCO2",
        "run_root": "/work/run",
        "first_year": 1850,
        "last_year": 1851,
        "output_root": str(tmp_path / "out"),
        "drs_version": "v20261001",
        "mail": "me@example.org",
        "code_ref": "main",
        "code_repo": str(code_repo),
        "experiment": "1pctCO2",
        # The production profiles are tested in test_campaign_production.py.
        "production": None,
        "campaign_root": str(tmp_path),
    }
    cfg.update(over)
    path = cdir / "campaign.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


@pytest.mark.parametrize(
    "over, msg",
    [
        ({"drs_version": "20261001"}, "vYYYYMMDD"),
        ({"name": "1pct CO2"}, "letters, digits"),
        ({"first_year": 1900, "last_year": 1850}, "after last_year"),
        ({"experiment": "1pctCO3"}, "no profile"),
        ({"inherit": {"branch_time_in_parent": 1.0}}, "owns"),
        ({"inherit": {"experiment_id": "piControl"}}, "owns"),
        ({"output_root": "relative/out"}, "absolute"),
    ],
)
def test_load_rejects_bad_campaigns(campaign, tmp_path, code_repo, over, msg):
    path = _write_campaign(tmp_path, code_repo, **over)
    with pytest.raises(campaign.CampaignError, match=msg):
        campaign.load(path)


def test_setup_pins_and_verify_catches_every_kind_of_drift(campaign, tmp_path, code_repo, monkeypatch):
    """The point of the pin: a 150-year chain must not pick up new code, a
    hand edit, or changed experiment metadata halfway through."""
    monkeypatch.setenv("CMIP7_TABLES_PATH", str(tmp_path))
    path = _write_campaign(tmp_path, code_repo)
    cfg = campaign.load(path)
    sha = campaign.setup(cfg)
    code = Path(cfg["_code"])
    assert (code / "README").read_text() == "v1\n"
    assert json.loads(Path(cfg["_pin"]).read_text())["code_commit"] == sha
    assert campaign.verify(cfg) == sha
    assert (tmp_path / "out").is_dir() and Path(cfg["_logs"]).is_dir()

    # Someone edits the pinned copy by hand.
    (code / "README").write_text("hacked\n")
    with pytest.raises(campaign.CampaignError, match="local changes"):
        campaign.verify(cfg)
    _git("checkout", "-q", "README", cwd=code)

    # Someone commits in it, or checks out something else.
    (code / "README").write_text("v2\n")
    _git("commit", "-q", "-am", "two", cwd=code)
    with pytest.raises(campaign.CampaignError, match="pinned to"):
        campaign.verify(cfg)
    _git("checkout", "-q", "--detach", sha, cwd=code)
    assert campaign.verify(cfg) == sha

    # Someone changes the experiment, or the extra inherit values.
    path = _write_campaign(tmp_path, code_repo, experiment="abrupt-4xCO2")
    with pytest.raises(campaign.CampaignError, match="experiment"):
        campaign.verify(campaign.load(path))
    path = _write_campaign(tmp_path, code_repo, inherit={"forcing_year": 1851})
    with pytest.raises(campaign.CampaignError, match="inherit"):
        campaign.verify(campaign.load(path))


def test_branch_moving_needs_a_deliberate_repin(campaign, tmp_path, code_repo, monkeypatch):
    monkeypatch.setenv("CMIP7_TABLES_PATH", str(tmp_path))
    path = _write_campaign(tmp_path, code_repo)
    first = campaign.setup(campaign.load(path))

    (code_repo / "README").write_text("v2\n")
    _git("commit", "-q", "-am", "two", cwd=code_repo)
    with pytest.raises(campaign.CampaignError, match="code_commit"):
        campaign.setup(campaign.load(path))
    cfg = campaign.load(path)
    assert campaign.verify(cfg) == first, "a refused repin leaves the campaign as it was"

    second = campaign.setup(cfg, repin=True)
    assert second != first and campaign.verify(cfg) == second
    history = (Path(cfg["_dir"]) / "pin_history.log").read_text().splitlines()
    assert len(history) == 2 and second in history[1]


def test_env_is_valid_shell(campaign, tmp_path, code_repo, monkeypatch):
    monkeypatch.setenv("CMIP7_TABLES_PATH", str(tmp_path))
    monkeypatch.delenv("PYTHONPATH", raising=False)
    path = _write_campaign(tmp_path, code_repo, mail="a b@example.org")
    cfg = campaign.load(path)
    campaign.setup(cfg)
    script = (
        campaign.env(cfg)
        + '\nprintf "%s|%s|%s|%s" "$PYTHONPATH" "$MAIL_USER" "$JOB_TAG" "$PYCMOR_CLIMATOLOGY_STATE_DIR"'
    )
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True).stdout
    assert out == f"{cfg['_code']}/src|a b@example.org|1pctCO2|{cfg['_dir']}/climatologies"


TIER = """\
pipelines:
  - name: p
    steps:
      - script:///work/ab0246/a270092/software/pycmor/examples/custom_steps.py:load
inherit:
  # Parent/branch metadata
  experiment_id: piControl
  parent_experiment_id: no parent
  parent_time_units: "days since 1350-01-01"
  branch_time_in_parent: 120522.0
  forcing_year: 1850
  grid: "OpenIFS: regridded"
  qc_checker_options: ["aicc:grid_config:/work/ab0246/a270092/software/pycmor/x.json"]
  activity_id: CMIP

rules:
  - name: tas
    compound_name: atmos.tas.tavg-h2m-hxy-u.mon.glb
"""


@pytest.fixture(scope="module")
def repoint():
    spec = importlib.util.spec_from_file_location("repoint_hr_year", REPO / "examples" / "repoint_hr_year.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _repointed(tmp_path, repoint, text=TIER, experiment="1pctCO2"):
    """What submit_hr_year_shards.sh hands to apply: repoint's output."""
    ydir = tmp_path / "yamls"
    ydir.mkdir(exist_ok=True)
    (ydir / "core_atm.yaml").write_text(repoint.apply_experiment(text, experiment, "core_atm.yaml"))
    return ydir


def test_profile_metadata_arrives_and_repo_paths_are_rebound(campaign, repoint, tmp_path, code_repo):
    cfg = campaign.load(_write_campaign(tmp_path, code_repo))
    ydir = _repointed(tmp_path, repoint)
    assert campaign.apply(cfg, ydir) == 1

    text = (ydir / "core_atm.yaml").read_text()
    got = yaml.safe_load(text)["inherit"]
    assert got["experiment_id"] == "1pctCO2"
    assert got["parent_experiment_id"] == "piControl"
    assert got["parent_time_units"] == "days since 1850-01-01"
    assert got["branch_time_in_parent"] == 36524.0
    assert got["branch_time_in_child"] == 0.0
    assert got["grid"] == "OpenIFS: regridded"
    assert campaign.TEMPLATE_REPO not in text
    assert f"script://{cfg['_code']}/examples/custom_steps.py:load" in text
    assert "# Parent/branch metadata" in text, "comments and layout survive"


def test_apply_stops_when_repoint_ran_without_the_profile(campaign, repoint, tmp_path, code_repo):
    """EXPERIMENT not reaching repoint would file 1pctCO2 data as piControl."""
    cfg = campaign.load(_write_campaign(tmp_path, code_repo))
    ydir = _repointed(tmp_path, repoint, experiment="piControl")
    with pytest.raises(campaign.CampaignError, match="Was EXPERIMENT passed"):
        campaign.apply(cfg, ydir)


def test_campaign_inherit_adds_values_no_profile_covers(campaign, repoint, tmp_path, code_repo):
    cfg = campaign.load(_write_campaign(tmp_path, code_repo, inherit={"forcing_year": 1851, "qc_repack": False}))
    ydir = _repointed(tmp_path, repoint)
    campaign.apply(cfg, ydir)
    got = yaml.safe_load((ydir / "core_atm.yaml").read_text())["inherit"]
    assert got["forcing_year"] == 1851 and got["qc_repack"] is False


def test_apply_refuses_when_a_rule_overrides_the_campaign(campaign, repoint, tmp_path, code_repo):
    cfg = campaign.load(_write_campaign(tmp_path, code_repo, inherit={"forcing_year": 1851}))
    ydir = _repointed(tmp_path, repoint, text=TIER + "    forcing_year: 1850\n")
    with pytest.raises(campaign.CampaignError, match="would not reach it"):
        campaign.apply(cfg, ydir)


def test_apply_refuses_a_value_that_does_not_parse_back(campaign, repoint, tmp_path, code_repo):
    """A multi-line value would leave continuation lines behind; the
    structural check must catch it instead of writing a broken yaml."""
    cfg = campaign.load(_write_campaign(tmp_path, code_repo, inherit={"grid": "new"}))
    folded = TIER.replace('  grid: "OpenIFS: regridded"\n', "  grid: >\n    folded\n    text\n")
    ydir = _repointed(tmp_path, repoint, text=folded)
    with pytest.raises(campaign.CampaignError, match="changed more than"):
        campaign.apply(cfg, ydir)


@pytest.mark.parametrize("experiment", ["piControl", "1pctCO2", "abrupt-4xCO2", "historical"])
@pytest.mark.parametrize("model", ["hr", "lr"])
def test_every_profile_binds_all_real_tier_yamls(campaign, repoint, tmp_path, code_repo, experiment, model):
    """All real tier yamls of each model, through repoint and apply as the shards get them."""
    cfg = campaign.load(_write_campaign(tmp_path, code_repo, experiment=experiment, model=model))
    ydir = tmp_path / "yamls"
    ydir.mkdir()
    spec = repoint.MODELS[model]
    srcs = sorted(spec["src_dir"].glob(spec["yaml_glob"]))
    assert len(srcs) >= 17, "one yaml per tier, 17 tiers"
    for src in srcs:
        (ydir / f"{src.parent.name}.yaml").write_text(repoint.repoint_yaml(src, "/work/run", "1850", experiment, model))
    assert campaign.apply(cfg, ydir) == len(srcs)
    for f in ydir.glob("*.yaml"):
        text = f.read_text()
        inherit = yaml.safe_load(text)["inherit"]
        assert inherit["experiment_id"] == experiment, f.name
        assert inherit["source_id"] == spec["source_id"], f.name
        if experiment != "piControl":
            assert inherit["parent_source_id"] == spec["source_id"], f.name
        assert "__LR_" not in text, f"{f.name}: an EMD placeholder was not filled in"
        assert "/work/run/outdata/" in text and spec["old_run_token"] + "/outdata" not in text, f.name


def test_hr_model_is_the_unchanged_default(repoint):
    """MODEL=hr must leave every HR value as it was before models existed."""
    assert repoint.DEFAULT_MODEL == "hr"
    hr = repoint.MODELS["hr"]
    assert hr["placeholders"] == {}
    assert (hr["runtime_root"], hr["old_run_token"]) == (repoint.RUNTIME_ROOT, repoint.OLD_RUN_TOKEN)
    assert repoint.experiment_profiles("hr") == repoint.EXPERIMENTS


def test_lr_rules_point_at_lr_grids_and_paths(repoint):
    """No HR grid label, mesh or source_id may survive in the LR tree."""
    spec = repoint.MODELS["lr"]
    for src in sorted(spec["src_dir"].glob(spec["yaml_glob"])):
        y = yaml.safe_load(repoint.repoint_yaml(src, "/work/run", "1850", "piControl", "lr"))
        labels = {y["inherit"]["grid_label"]} | {r["grid_label"] for r in y["rules"] if "grid_label" in r}
        assert not labels & {"g122", "g129", "g130", "g132"}, (src.name, labels)
        assert "dars2" not in str(y), src.name
        assert y["inherit"]["source_id"] == spec["source_id"]


LR_KEEPS_WHAT_HR_DROPPED = {
    # HR switched these off because its plev3 XIOS axis was not a zoom of the
    # FullPos axis and wrote the wrong levels. The v3.5.1 XML fixes the axis,
    # so LR must keep them; an HR deactivation copied into the LR tree would
    # silently lose them.
    "atmos.ta.tpt-p3-hxy-air.6hr.glb",
    "atmos.ua.tpt-p3-hxy-air.6hr.glb",
    "atmos.va.tpt-p3-hxy-air.6hr.glb",
}


def test_lr_keeps_the_rules_hr_dropped_for_its_own_output_faults(repoint):
    spec = repoint.MODELS["lr"]
    active = set()
    for src in sorted(spec["src_dir"].glob(spec["yaml_glob"])):
        active |= {r["compound_name"] for r in yaml.safe_load(src.read_text())["rules"]}
    assert LR_KEEPS_WHAT_HR_DROPPED <= active, sorted(LR_KEEPS_WHAT_HR_DROPPED - active)


def test_campaign_model_is_validated_exported_and_pinned(campaign, tmp_path, code_repo, monkeypatch):
    monkeypatch.setenv("CMIP7_TABLES_PATH", str(tmp_path))
    with pytest.raises(campaign.CampaignError, match="model"):
        campaign.load(_write_campaign(tmp_path, code_repo, model="mr"))
    hr = campaign.load(_write_campaign(tmp_path, code_repo))
    assert hr["model"] == "hr" and "model" not in campaign._pinned_view(hr, "x"), "HR pins stay as they were"
    lr = campaign.load(_write_campaign(tmp_path, code_repo, model="lr"))
    assert campaign._pinned_view(lr, "x")["model"] == "lr"
    campaign.setup(lr)
    assert "export MODEL=lr" in campaign.env(lr)
    # Switching an LR campaign to HR afterwards changes what it produces.
    with pytest.raises(campaign.CampaignError, match="model"):
        campaign.verify(campaign.load(_write_campaign(tmp_path, code_repo)))


SQUEUE = """\
28504340 pycmor-cli127-y1852
28504368 pycmor-hr-cli127-core_atm-y1852-sh
28504368 pycmor-hr-cli127-core_atm-y1852-sh
28505007 pycmor-hr-cli127-lrcs_seaice-y1852-sh
28504100 pycmor-cli12-y1852
28504101 pycmor-hr-cli12-core_atm-y1852-sh
28504102 pycmor-hr-cli127-2-core_atm-y1852-sh
28504103 pycmor-cli127-2-y1852
28504104 sigcheck-shard
"""


def _fake_slurm(monkeypatch, campaign, calls):
    def run(cmd, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout=SQUEUE if cmd[0] == "squeue" else "")

    monkeypatch.setattr(campaign.subprocess, "run", run)


def test_queued_lists_the_chain_and_its_shards_only(campaign, monkeypatch):
    calls = []
    _fake_slurm(monkeypatch, campaign, calls)
    assert campaign.queued({"name": "cli127"}) == [
        ("28504340", "pycmor-cli127-y1852"),
        ("28504368", "pycmor-hr-cli127-core_atm-y1852-sh"),
        ("28505007", "pycmor-hr-cli127-lrcs_seaice-y1852-sh"),
    ]


def test_stop_cancels_by_job_id(campaign, monkeypatch):
    calls = []
    _fake_slurm(monkeypatch, campaign, calls)
    campaign.stop({"name": "cli127-2"})
    assert calls[-1] == ["scancel", "28504102", "28504103"]


def test_stop_with_nothing_queued_calls_no_scancel(campaign, monkeypatch):
    calls = []
    _fake_slurm(monkeypatch, campaign, calls)
    assert campaign.stop({"name": "1pctCO2"}) == []
    assert [c[0] for c in calls] == ["squeue"]
