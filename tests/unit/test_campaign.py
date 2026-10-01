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
    script = campaign.env(cfg) + '\nprintf "%s|%s|%s" "$PYTHONPATH" "$MAIL_USER" "$JOB_TAG"'
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True).stdout
    assert out == f"{cfg['_code']}/src|a b@example.org|1pctCO2"


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
def test_every_profile_binds_all_real_tier_yamls(campaign, repoint, tmp_path, code_repo, experiment):
    """All 17 real tier yamls, through repoint and apply as the shards get them."""
    cfg = campaign.load(_write_campaign(tmp_path, code_repo, experiment=experiment))
    ydir = tmp_path / "yamls"
    ydir.mkdir()
    for src in sorted((REPO / "awi-esm3-veg-hr-variables").glob("*/cmip7_awiesm3-veg-hr*.yaml")):
        (ydir / f"{src.parent.name}.yaml").write_text(repoint.repoint_yaml(src, "/work/run", "1850", experiment))
    assert campaign.apply(cfg, ydir) == 17
    for f in ydir.glob("*.yaml"):
        assert yaml.safe_load(f.read_text())["inherit"]["experiment_id"] == experiment, f.name
