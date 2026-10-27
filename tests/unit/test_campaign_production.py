"""Production campaigns: their own env, tables, CVs, Data Request and inputs."""

import importlib.util
import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
PROFILE = REPO / "examples" / "production" / "awiesm3-veg-hr-cmip7-1"


@pytest.fixture(scope="module")
def campaign():
    spec = importlib.util.spec_from_file_location("campaign", REPO / "examples" / "campaign.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _campaign_file(tmp_path, **over):
    cdir = tmp_path / "root" / "1pctCO2"
    cdir.mkdir(parents=True, exist_ok=True)
    cfg = {
        "name": "1pctCO2",
        "run_root": "/work/run",
        "first_year": 1850,
        "last_year": 1851,
        "output_root": str(tmp_path / "out"),
        "drs_version": "v20261001",
        "mail": "me@example.org",
        "code_ref": "main",
        "experiment": "1pctCO2",
    }
    cfg.update(over)
    path = cdir / "campaign.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


def test_campaign_root_resolution(campaign, tmp_path, monkeypatch):
    monkeypatch.delenv("PYCMOR_CAMPAIGN_ROOT", raising=False)
    monkeypatch.setenv("USER", "a999999")
    cfg = campaign.load(_campaign_file(tmp_path))
    assert cfg["campaign_root"] == "/work/bb1469/a999999/cmor_campaigns"
    assert cfg["production"] == "awiesm3-veg-hr-cmip7-1", "HR runs the production profile unless told otherwise"
    assert cfg["_env"] == "/work/bb1469/a999999/cmor_campaigns/envs/awiesm3-veg-hr-cmip7-1"
    assert cfg["_tables"].startswith(str(tmp_path / "root" / "1pctCO2"))
    monkeypatch.setenv("PYCMOR_CAMPAIGN_ROOT", "/work/xy1234/$USER/c")
    assert campaign.load(_campaign_file(tmp_path))["campaign_root"] == "/work/xy1234/a999999/c"
    cfg = campaign.load(_campaign_file(tmp_path, campaign_root="/work/zz/own"))
    assert cfg["campaign_root"] == "/work/zz/own", "campaign.yaml wins over the environment"
    with pytest.raises(campaign.CampaignError, match="campaign_root"):
        campaign.load(_campaign_file(tmp_path, campaign_root="/work/$NOT_SET_ANYWHERE/x"))
    assert campaign.load(_campaign_file(tmp_path, model="lr"))["production"] is None
    assert "_env" not in campaign.load(_campaign_file(tmp_path, production=None))


def test_production_is_pinned(campaign, tmp_path):
    cfg = campaign.load(_campaign_file(tmp_path))
    assert campaign._pinned_view(cfg, "x")["production"] == "awiesm3-veg-hr-cmip7-1"
    assert "production" not in campaign._pinned_view(campaign.load(_campaign_file(tmp_path, production=None)), "x")


def test_regridded_output_is_a_pinned_campaign_key(campaign, tmp_path, monkeypatch):
    plain = campaign.load(_campaign_file(tmp_path, production=None))
    assert plain["with_gr"] is False and "with_gr" not in campaign._pinned_view(plain, "x")
    gr = campaign.load(_campaign_file(tmp_path, production=None, with_gr=True))
    assert campaign._pinned_view(gr, "x")["with_gr"] is True
    with pytest.raises(campaign.CampaignError, match="with_gr"):
        campaign.load(_campaign_file(tmp_path, with_gr="yes"))
    # Knobs left in the shell must not reach the jobs.
    monkeypatch.setenv("WITH_GR", "no")
    monkeypatch.setenv("TIER", "core_atm")
    monkeypatch.setenv("RULES", "tas")
    gr["_pin"] = str(tmp_path / "pin.json")
    Path(gr["_pin"]).write_text(json.dumps({"code_commit": "abc"}))
    script = campaign.env(gr) + '\nprintf "%s|%s|%s" "$WITH_GR" "$TIER" "$RULES"'
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True).stdout
    assert out == "yes||"


def test_missing_profile_is_refused(campaign, tmp_path):
    cfg = campaign.load(_campaign_file(tmp_path, production="no-such-profile"))
    with pytest.raises(campaign.CampaignError, match="lacks"):
        campaign._production(cfg)


def test_requirement_comparison(campaign):
    norm = campaign._normalized_requirements
    assert norm(["Cf_Xarray==0.10.11", "packaging @ file:///home/conda/x", "# c", ""]) == {"cf-xarray==0.10.11"}
    assert norm(["tripyview @ git+https://h/t.git@abc"]) == norm(["tripyview @ git+https://h/t.git@abc  "])
    assert norm(["dask==2026.3.0"]) != norm(["dask==2026.3.1"])


def test_dreq_rebinding(campaign):
    text = 'inherit:\n  CMIP7_DReq_metadata: "/home/x/.cache/pycmor/cmip7_metadata/v1.2.2.5/metadata.json"\n'
    out = campaign._rebind_dreq(text, "t.yaml", "v1.2.2.5", "/c/dreq/v1.2.2.5/metadata.json")
    assert yaml.safe_load(out)["inherit"]["CMIP7_DReq_metadata"] == "/c/dreq/v1.2.2.5/metadata.json"
    with pytest.raises(campaign.CampaignError, match="not Data Request v1.2.2.5"):
        campaign._rebind_dreq(text.replace("v1.2.2.5", "v1.2.2.4"), "t.yaml", "v1.2.2.5", "/c/m.json")


def test_inputs_are_checksummed_then_watched(campaign, tmp_path):
    f = tmp_path / "mesh.nc"
    f.write_bytes(b"mesh")
    cdir = tmp_path / "c"
    cdir.mkdir()
    cfg = {"_dir": str(cdir)}
    pins = {"inputs": {str(f): campaign._sha256(f, "md5")}}
    campaign._ensure_inputs(cfg, pins)
    campaign._check_inputs(cfg, pins)
    f.write_bytes(b"MESH")
    os.utime(f, ns=(1, 1))
    with pytest.raises(campaign.CampaignError, match="changed since setup"):
        campaign._check_inputs(cfg, pins)
    with pytest.raises(campaign.CampaignError, match="differs"):
        campaign._ensure_inputs(cfg, pins)


def test_env_marker_guards_reuse_and_half_builds(campaign, tmp_path, monkeypatch):
    pdir = tmp_path / "profile"
    pdir.mkdir()
    for name in campaign.PRODUCTION_FILES:
        (pdir / name).write_text(name)
    cfg = {"production": "p1", "_env": str(tmp_path / "envs" / "p1"), "conda_init": "/dev/null"}
    pins = {"source_builds": {"packages": ["h5py"], "env": {"HDF5_DIR": "/sw/hdf5"}}}
    calls = []
    monkeypatch.setattr(campaign, "_run", lambda cmd, **kw: calls.append(cmd) or "")
    # A prefix without the marker is an interrupted build.
    Path(cfg["_env"]).mkdir(parents=True)
    with pytest.raises(campaign.CampaignError, match="did not finish"):
        campaign._ensure_env(cfg, pdir, pins)
    # A finished env is reused without building anything.
    (Path(cfg["_env"]) / campaign.ENV_MARKER).write_text(json.dumps(campaign._env_want(cfg, pdir, pins)))
    assert campaign._ensure_env(cfg, pdir, pins) == Path(cfg["_env"]) and calls == []
    # Changing a lock file, the hook or the source builds under an existing env is refused.
    for change in (
        lambda: (pdir / "requirements.lock").write_text("dask==1\n"),
        lambda: (pdir / "activate.sh").write_text("export X=1\n"),
        lambda: pins["source_builds"]["env"].update(HDF5_DIR="/sw/other"),
    ):
        (Path(cfg["_env"]) / campaign.ENV_MARKER).write_text(json.dumps(campaign._env_want(cfg, pdir, pins)))
        change()
        with pytest.raises(campaign.CampaignError, match="other lock files"):
            campaign._ensure_env(cfg, pdir, pins)


def test_source_builds_skip_the_wheels(campaign, tmp_path, monkeypatch):
    pdir = tmp_path / "profile"
    pdir.mkdir()
    (pdir / "requirements.lock").write_text("# c\ndask==1\nh5py==3.16.0\nnetCDF4==1.7.4\n")
    prefix = tmp_path / "env"
    prefix.mkdir()
    calls = []

    def run(cmd, env=None, what=None):
        if "-r" in cmd:
            calls.append(("wheels", Path(cmd[cmd.index("-r") + 1]).read_text().split()))
        else:
            calls.append(("source", cmd[cmd.index("--no-binary") + 1 :], env["HDF5_DIR"]))
        return ""

    monkeypatch.setattr(campaign, "_run", run)
    builds = {"packages": ["h5py", "netCDF4"], "env": {"HDF5_DIR": "/sw/hdf5"}}
    campaign._pip_install(prefix, pdir, builds)
    assert calls == [
        ("wheels", ["dask==1"]),
        ("source", ["h5py,netCDF4", "h5py==3.16.0", "netCDF4==1.7.4"], "/sw/hdf5"),
    ]
    assert not (prefix / "pycmor-wheels.txt").exists()


def test_library_check_refuses_wheels(campaign, tmp_path, monkeypatch):
    expect = {"hdf5": "1.14.2", "hdf5_threadsafe": True, "netcdf": "4.9.3-development"}
    good = {"hdf5": "1.14.2", "hdf5_netcdf4": "1.14.2", "hdf5_threadsafe": True, "netcdf": "4.9.3-development"}
    monkeypatch.setattr(campaign, "_run", lambda cmd, **kw: json.dumps(good) + "\n")
    campaign._check_libs(tmp_path, expect)
    wheel = {**good, "hdf5_netcdf4": "1.14.6", "hdf5_threadsafe": False}
    monkeypatch.setattr(campaign, "_run", lambda cmd, **kw: json.dumps(wheel) + "\n")
    with pytest.raises(campaign.CampaignError, match="production expects"):
        campaign._check_libs(tmp_path, expect)


def _git(*args, cwd):
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
    )


def test_tables_follow_the_pin(campaign, tmp_path):
    src = tmp_path / "tables_src"
    (src / "tables").mkdir(parents=True)
    _git("init", "-q", "-b", "main", cwd=src)
    (src / "tables" / "CMIP7_atmos.json").write_text("{}")
    _git("add", ".", cwd=src)
    _git("commit", "-q", "-m", "one", cwd=src)
    first = subprocess.run(["git", "rev-parse", "HEAD"], cwd=src, capture_output=True, text=True).stdout.strip()
    (src / "tables" / "CMIP7_atmos.json").write_text('{"a": 1}')
    _git("commit", "-q", "-am", "two", cwd=src)
    cfg = {"_tables": str(tmp_path / "c" / "cmip7-cmor-tables")}
    pins = {"cmip7_tables": {"repo": str(src), "commit": first}}
    campaign._ensure_tables(cfg, pins)
    campaign._check_tables(cfg, pins)
    assert (Path(cfg["_tables"]) / "tables" / "CMIP7_atmos.json").read_text() == "{}"
    (Path(cfg["_tables"]) / "tables" / "CMIP7_atmos.json").write_text("edited")
    with pytest.raises(campaign.CampaignError, match="local changes"):
        campaign._check_tables(cfg, pins)


# ---------------------------------------------------------------- the real profile


def test_real_profile_is_complete_and_installable_as_written(campaign):
    for name in campaign.PRODUCTION_FILES:
        assert (PROFILE / name).is_file(), name
    lock = (PROFILE / "requirements.lock").read_text().splitlines()
    pins = [ln for ln in lock if ln and not ln.startswith("#")]
    assert not [
        ln for ln in pins if ln.startswith("-e") or "file://" in ln
    ], "editable and local installs cannot be rebuilt"
    for ln in pins:
        assert "==" in ln or ("@ git+" in ln and len(ln.rsplit("@", 1)[1]) == 40), f"not pinned exactly: {ln}"
    explicit = (PROFILE / "conda-explicit.txt").read_text()
    assert "@EXPLICIT" in explicit and all("#" in ln for ln in explicit.splitlines() if ln.startswith("https"))


def test_real_profile_owns_its_system_libraries(campaign):
    pins = yaml.safe_load((PROFILE / "pins.yaml").read_text())
    builds = pins["source_builds"]
    lock = {campaign._req_name(ln.split("==")[0]) for ln in (PROFILE / "requirements.lock").read_text().splitlines()}
    assert {campaign._req_name(p) for p in builds["packages"]} <= lock
    assert {"cython", "pkgconfig"} <= lock, "build requirements of --no-build-isolation"
    assert builds["expect"]["hdf5_threadsafe"] is True
    hook = (PROFILE / "activate.sh").read_text()
    # The plugin path must not inherit whatever the user's modules put there.
    plugin = [ln for ln in hook.splitlines() if ln.startswith("export HDF5_PLUGIN_PATH=")]
    assert len(plugin) == 1 and "$HDF5_PLUGIN_PATH" not in plugin[0]
    assert "PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS" in hook
    # Every system directory the build and the hook use has its libraries pinned.
    dirs = {builds["env"]["HDF5_DIR"], builds["env"]["NETCDF4_DIR"]}
    assert all(d in hook for d in dirs)
    for d in dirs:
        assert any(p.startswith(d + "/") for p in pins["inputs"]), d


def test_hr_tier_yamls_name_the_pinned_data_request():
    pins = yaml.safe_load((PROFILE / "pins.yaml").read_text())
    version = pins["dreq"]["version"]
    for f in sorted((REPO / "awi-esm3-veg-hr-variables").glob("*/cmip7_awiesm3-veg-hr_*.yaml")):
        meta = (yaml.safe_load(f.read_text()).get("inherit") or {}).get("CMIP7_DReq_metadata")
        if meta:
            assert f"/{version}/" in meta, f"{f.name}: {meta}"


def test_no_hr_rule_outside_the_pinned_data_request():
    pins = yaml.safe_load((PROFILE / "pins.yaml").read_text())
    meta = Path.home() / ".cache" / "pycmor" / "cmip7_metadata" / pins["dreq"]["version"] / "metadata.json"
    if not meta.exists():
        pytest.skip(f"{meta} not exported here")
    known = set(json.loads(meta.read_text())["Compound Name"])
    missing = []
    for f in sorted((REPO / "awi-esm3-veg-hr-variables").glob("*/cmip7_awiesm3-veg-hr_*.yaml")):
        for rule in yaml.safe_load(f.read_text()).get("rules") or []:
            name = rule.get("compound_name")
            if name and rule.get("enabled", True) is not False and name not in known:
                missing.append(f"{f.parent.name}:{rule.get('name')}:{name}")
    assert not missing, missing
