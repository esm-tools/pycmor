"""Campaign setup: one pinned code copy and one config file per experiment."""

import importlib.util
import json
import shutil
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
        "inherit": {"experiment_id": "1pctCO2", "parent_experiment_id": "piControl", "branch_time_in_parent": 36524.0},
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
        ({"inherit": {"parent_experiment_id": "piControl"}}, "experiment_id"),
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

    # Someone changes the experiment metadata in campaign.yaml.
    path = _write_campaign(tmp_path, code_repo, inherit={"experiment_id": "abrupt-4xCO2"})
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
  branch_time_in_parent: 120522.0
  grid: "OpenIFS: regridded"
  qc_checker_options: ["aicc:grid_config:/work/ab0246/a270092/software/pycmor/x.json"]
  activity_id: CMIP

rules:
  - name: tas
    compound_name: atmos.tas.tavg-h2m-hxy-u.mon.glb
"""


def test_apply_sets_campaign_values_and_rebinds_repo_paths(campaign, tmp_path, code_repo):
    path = _write_campaign(tmp_path, code_repo)
    cfg = campaign.load(path)
    ydir = tmp_path / "yamls"
    ydir.mkdir()
    (ydir / "core_atm.yaml").write_text(TIER)
    assert campaign.apply(cfg, ydir) == 1

    text = (ydir / "core_atm.yaml").read_text()
    got = yaml.safe_load(text)
    assert got["inherit"]["experiment_id"] == "1pctCO2"
    assert got["inherit"]["parent_experiment_id"] == "piControl"
    assert got["inherit"]["branch_time_in_parent"] == 36524.0
    assert got["inherit"]["grid"] == "OpenIFS: regridded"
    assert campaign.TEMPLATE_REPO not in text
    assert f"script://{cfg['_code']}/examples/custom_steps.py:load" in text
    assert "# Parent/branch metadata" in text, "comments and layout survive"


def test_apply_adds_keys_the_tier_did_not_have(campaign, tmp_path, code_repo):
    path = _write_campaign(tmp_path, code_repo, inherit={"experiment_id": "1pctCO2", "branch_time_in_child": 0.0})
    ydir = tmp_path / "yamls"
    ydir.mkdir()
    (ydir / "t.yaml").write_text(TIER)
    campaign.apply(campaign.load(path), ydir)
    assert yaml.safe_load((ydir / "t.yaml").read_text())["inherit"]["branch_time_in_child"] == 0.0


def test_apply_refuses_when_a_rule_overrides_the_campaign(campaign, tmp_path, code_repo):
    path = _write_campaign(tmp_path, code_repo)
    ydir = tmp_path / "yamls"
    ydir.mkdir()
    (ydir / "t.yaml").write_text(TIER + "    experiment_id: piControl\n")
    with pytest.raises(campaign.CampaignError, match="would not reach it"):
        campaign.apply(campaign.load(path), ydir)


def test_apply_refuses_a_value_that_does_not_parse_back(campaign, tmp_path, code_repo):
    """A multi-line value would leave continuation lines behind; the
    structural check must catch it instead of writing a broken yaml."""
    path = _write_campaign(tmp_path, code_repo, inherit={"experiment_id": "1pctCO2", "grid": "new"})
    ydir = tmp_path / "yamls"
    ydir.mkdir()
    (ydir / "t.yaml").write_text(TIER.replace('  grid: "OpenIFS: regridded"\n', "  grid: >\n    folded\n    text\n"))
    with pytest.raises(campaign.CampaignError, match="changed more than"):
        campaign.apply(campaign.load(path), ydir)


def test_apply_binds_every_real_tier_yaml(campaign, tmp_path, code_repo):
    """All 17 real tier yamls, exactly as repoint copies them."""
    path = _write_campaign(tmp_path, code_repo)
    cfg = campaign.load(path)
    ydir = tmp_path / "yamls"
    ydir.mkdir()
    for src in sorted((REPO / "awi-esm3-veg-hr-variables").glob("*/cmip7_awiesm3-veg-hr*.yaml")):
        shutil.copy(src, ydir / f"{src.parent.name}.yaml")
    assert campaign.apply(cfg, ydir) == 17
    for f in ydir.glob("*.yaml"):
        inherit = yaml.safe_load(f.read_text())["inherit"]
        assert inherit["experiment_id"] == "1pctCO2", f.name
        assert inherit["parent_experiment_id"] == "piControl", f.name
