#!/usr/bin/env python3
"""One cmorization campaign per experiment, pinned to one version of the code.

A campaign is a directory holding a ``campaign.yaml``. Everything that differs
between experiments lives in that file; the tier yamls and scripts are never
copied or edited per experiment. ``setup`` clones pycmor into ``code/`` at
``code_ref`` and pins the resulting commit, so fixes pushed while a 150-year
chain is running do not change the code halfway through it. Every job checks
the pin before it starts (``verify``) and refuses to run if the code or the
data-defining part of the campaign has moved.

Subcommands
-----------
    campaign.py setup  <campaign.yaml> [--repin]   clone and pin, create dirs
    campaign.py env    <campaign.yaml>             shell exports for the chain
    campaign.py verify <campaign.yaml>             exit 1 if code or campaign moved
    campaign.py apply  <campaign.yaml> <yamls_dir> bind generated tier yamls

Example ``campaign.yaml``::

    name: 1pctCO2                  # job names: pycmor-1pctCO2-y1850
    run_root: /work/bb1469/a270089/runtime/awiesm3-v3.4.2/AWI-ESM3-VEG-HR-CMIP7-1pctCO2_1949
    first_year: 1850
    last_year: 1999
    output_root: /work/<esgf-project>/CMIP7   # complete years are moved here
    drs_version: v20261001
    mail: jan.streffing@awi.de
    code_ref: feat/cmip7-awiesm3-veg-hr       # branch, tag or commit; pinned at setup
    experiment: 1pctCO2            # metadata profile in repoint_hr_year.EXPERIMENTS
    model: hr                      # optional, hr (default) or lr; repoint_hr_year.MODELS

The experiment metadata (experiment_id, parent and branch attributes) comes
from the named profile in ``repoint_hr_year.py``, so it is reviewed in git and
pinned with the code. ``inherit`` (optional) adds tier ``inherit`` values that
no profile covers; it may not repeat a key the profile already sets.

Optional: ``workroot`` (default ``<campaign>/work``; put it in the same
project as ``output_root`` so publishing is a rename, not a copy),
``account`` (ab0246), ``driver_attempts`` (3), ``first_year_attempts`` (1, for
the first year of every launch), ``max_consecutive_fails`` (2),
``conda_init``, ``conda_env``, ``code_repo`` (default: this checkout),
``with_gr`` (false; true also writes the regridded FESOM output, g131).

Where campaigns run: ``campaign_root`` (campaign.yaml, then
``$PYCMOR_CAMPAIGN_ROOT``, then ``/work/bb1469/$USER/cmor_campaigns``). Put
each campaign in ``<campaign_root>/<name>/``; the production envs are built in
``<campaign_root>/envs/``. Nothing here is the output, which goes to
``output_root``.

Production: ``production`` names a profile in ``examples/production/`` of the
pinned code (default ``awiesm3-veg-hr-cmip7-1`` for HR, none for LR;
``production: null`` keeps the shared ``conda_env``). Setup builds that
profile's env once per campaign_root from its lock files and gives the
campaign its own CMIP7 tables, CVs and Data Request at the pinned versions,
and checksums the static inputs. ``verify`` checks all of it again before
every year, so work elsewhere (LR, a pip install, a git pull, a CV refresh)
cannot reach a running campaign.
"""

import argparse
import copy
import datetime
import fcntl
import hashlib
import importlib.util
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
# The tier yamls name this checkout by absolute path (aicc grid config,
# LPJ-GUESS custom steps). ``apply`` rebinds them to the campaign's code.
TEMPLATE_REPO = "/work/ab0246/a270092/software/pycmor"

REQUIRED = (
    "name",
    "run_root",
    "first_year",
    "last_year",
    "output_root",
    "drs_version",
    "mail",
    "code_ref",
    "experiment",
)
# What determines the content of the output. Changing any of these after
# setup would make one campaign's years disagree with each other.
PINNED_FIELDS = ("run_root", "output_root", "drs_version", "experiment", "inherit")
DEFAULTS = {
    "account": "ab0246",
    "driver_attempts": 3,
    "first_year_attempts": 1,
    "max_consecutive_fails": 2,
    "conda_init": "/work/ab0246/a270092/software/miniforge3/etc/profile.d/conda.sh",
    "conda_env": "pycmor_py312",
}
DEFAULT_CAMPAIGN_ROOT = "/work/bb1469/${USER}/cmor_campaigns"
# The production profile each model gets unless campaign.yaml names one.
DEFAULT_PRODUCTION = {"hr": "awiesm3-veg-hr-cmip7-1"}
PRODUCTION_FILES = ("conda-explicit.txt", "requirements.lock", "pins.yaml")
# Written into a production env as the last step of a build that checked out.
ENV_MARKER = "pycmor-production.json"


class CampaignError(Exception):
    pass


def _repoint_module():
    spec = importlib.util.spec_from_file_location("repoint_hr_year", HERE / "repoint_hr_year.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def experiment_profiles(model="hr"):
    """The metadata profiles repoint_hr_year.py applies, keyed by experiment."""
    return _repoint_module().experiment_profiles(model)


def load(path):
    path = Path(path).resolve()
    with open(path) as fh:
        cfg = yaml.safe_load(fh) or {}
    missing = [k for k in REQUIRED if k not in cfg]
    if missing:
        raise CampaignError(f"{path}: missing {', '.join(missing)}")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]*", str(cfg["name"])):
        raise CampaignError(f"name {cfg['name']!r}: letters, digits and '-' only (it goes into job names)")
    if not re.fullmatch(r"v\d{8}", str(cfg["drs_version"])):
        raise CampaignError(f"drs_version {cfg['drs_version']!r} is not vYYYYMMDD")
    if int(cfg["first_year"]) > int(cfg["last_year"]):
        raise CampaignError("first_year is after last_year")
    cfg.setdefault("model", "hr")
    models = _repoint_module().MODELS
    if cfg["model"] not in models:
        raise CampaignError(f"model {cfg['model']!r} unknown; known: {', '.join(sorted(models))}")
    profiles = experiment_profiles(cfg["model"])
    if cfg["experiment"] not in profiles:
        raise CampaignError(f"experiment {cfg['experiment']!r} has no profile; known: {', '.join(sorted(profiles))}")
    cfg.setdefault("inherit", {})
    if not isinstance(cfg["inherit"], dict):
        raise CampaignError("inherit must be a mapping")
    clash = sorted(set(cfg["inherit"]) & (set(profiles[cfg["experiment"]]) | {"experiment_id"}))
    if clash:
        raise CampaignError(
            f"inherit sets {', '.join(clash)}, which the {cfg['experiment']!r} profile in repoint_hr_year.py "
            "owns. Change the profile there instead, so there is one source for experiment metadata."
        )
    for key in ("run_root", "output_root"):
        if not str(cfg[key]).startswith("/"):
            raise CampaignError(f"{key} must be an absolute path")
    for k, v in DEFAULTS.items():
        cfg.setdefault(k, v)
    cfg["first_year"], cfg["last_year"] = int(cfg["first_year"]), int(cfg["last_year"])
    cdir = path.parent
    if str(cdir).startswith(TEMPLATE_REPO + "/") or str(cdir) == TEMPLATE_REPO:
        raise CampaignError(f"keep campaigns outside the template checkout {TEMPLATE_REPO}")
    cfg.setdefault("workroot", str(cdir / "work"))
    cfg.setdefault("code_repo", str(HERE.parent))
    root = cfg.get("campaign_root") or os.environ.get("PYCMOR_CAMPAIGN_ROOT") or DEFAULT_CAMPAIGN_ROOT
    cfg["campaign_root"] = os.path.expandvars(str(root))
    if "$" in cfg["campaign_root"] or not cfg["campaign_root"].startswith("/"):
        raise CampaignError(f"campaign_root {root!r} does not resolve to an absolute path")
    cfg.setdefault("with_gr", False)
    if not isinstance(cfg["with_gr"], bool):
        raise CampaignError(f"with_gr must be true or false, not {cfg['with_gr']!r}")
    cfg.setdefault("production", DEFAULT_PRODUCTION.get(cfg["model"]))
    if cfg["production"] is not None and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", str(cfg["production"])):
        raise CampaignError(f"production {cfg['production']!r} is not a profile name")
    cfg["_file"] = str(path)
    cfg["_dir"] = str(cdir)
    cfg["_code"] = str(cdir / "code")
    cfg["_logs"] = str(cdir / "logs")
    cfg["_pin"] = str(cdir / "pinned.json")
    if cfg["production"]:
        cfg["_env"] = str(Path(cfg["campaign_root"]) / "envs" / cfg["production"])
        cfg["_tables"] = str(cdir / "cmip7-cmor-tables")
        cfg["_esgvoc"] = str(cdir / "esgvoc")
        cfg["_dreq"] = str(cdir / "dreq")
    return cfg


def _git(*args, cwd=None, check=True):
    res = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)
    if check and res.returncode != 0:
        raise CampaignError(f"git {' '.join(args)} failed: {res.stderr.strip()}")
    return res.stdout.strip()


def _resolve(code, ref):
    # origin/ first: a branch name must follow the branch as fetched, not the
    # local branch the clone happened to create.
    for cand in (f"origin/{ref}", ref):
        res = subprocess.run(
            ["git", "rev-parse", "--verify", "-q", f"{cand}^{{commit}}"], cwd=code, capture_output=True, text=True
        )
        if res.returncode == 0:
            return res.stdout.strip()
    raise CampaignError(f"code_ref {ref!r} not found in {code}")


def _pinned_view(cfg, sha):
    view = {"code_commit": sha, **{k: cfg[k] for k in PINNED_FIELDS}}
    # The model is pinned too, but only recorded when it is not the default,
    # so the pins of the HR campaigns started before the field existed still
    # verify.
    if cfg.get("model", "hr") != "hr":
        view["model"] = cfg["model"]
    if cfg.get("production"):
        view["production"] = cfg["production"]
    if cfg.get("with_gr"):
        view["with_gr"] = True
    return view


def _sha256(path, algo="sha256"):
    h = hashlib.new(algo)
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 24), b""):
            h.update(chunk)
    return h.hexdigest()


def _production(cfg):
    """The production profile of the pinned code: its directory and pins."""
    pdir = Path(cfg["_code"]) / "examples" / "production" / cfg["production"]
    missing = [f for f in PRODUCTION_FILES if not (pdir / f).is_file()]
    if missing:
        raise CampaignError(f"production {cfg['production']!r}: {pdir} lacks {', '.join(missing)}")
    pins = yaml.safe_load((pdir / "pins.yaml").read_text())
    return pdir, pins


def _env_want(cfg, pdir):
    return {
        "production": cfg["production"],
        **{f: _sha256(pdir / f) for f in ("conda-explicit.txt", "requirements.lock")},
    }


def _run(cmd, env=None, what=None):
    res = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if res.returncode != 0:
        tail = "\n".join((res.stderr or res.stdout).strip().splitlines()[-15:])
        raise CampaignError(f"{what or ' '.join(map(str, cmd))} failed:\n{tail}")
    return res.stdout


def _normalized_requirements(lines):
    """pip requirement lines as a comparable set. Packages conda installed
    (``name @ file://...`` in pip freeze) belong to conda-explicit.txt."""
    out = set()
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, sep, rest = re.split(r"(==| @ )", line, maxsplit=1)
        if rest.startswith("file://"):
            continue
        out.add(re.sub(r"[-_.]+", "-", name.strip().lower()) + sep + rest.strip())
    return out


def _ensure_env(cfg, pdir):
    """Build the production env once per campaign_root, or check the one there.

    conda envs cannot be moved after they are built (scripts carry their own
    path), so the build happens in place and the marker file is written last:
    a prefix without it is an interrupted build and is refused."""
    prefix = Path(cfg["_env"])
    want = _env_want(cfg, pdir)
    prefix.parent.mkdir(parents=True, exist_ok=True)
    with open(prefix.parent / f".{prefix.name}.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        marker = prefix / ENV_MARKER
        if marker.exists():
            have = json.loads(marker.read_text())
            if have != want:
                raise CampaignError(
                    f"{prefix} was built from other lock files than {pdir}. A profile's lock files must not "
                    "change once envs exist; make a new profile instead."
                )
            return prefix
        if prefix.exists():
            raise CampaignError(f"{prefix} has no {ENV_MARKER}: an earlier build did not finish. Remove it and rerun.")
        print(f"building production env {prefix} (about 15 minutes, once per campaign_root)", file=sys.stderr)
        _run(
            [
                "bash",
                "-c",
                f"source {shlex.quote(cfg['conda_init'])} && conda create -y -q -p {shlex.quote(str(prefix))} "
                f"--file {shlex.quote(str(pdir / 'conda-explicit.txt'))}",
            ],
            what="conda create",
        )
        py = str(prefix / "bin" / "python")
        _run([py, "-m", "pip", "install", "-q", "--no-deps", "-r", str(pdir / "requirements.lock")], what="pip install")
        _run([py, "-m", "pip", "check"], what="pip check")
        got = _normalized_requirements(_run([py, "-m", "pip", "freeze"]).splitlines())
        locked = _normalized_requirements((pdir / "requirements.lock").read_text().splitlines())
        if got != locked:
            extra, short = sorted(got - locked), sorted(locked - got)
            raise CampaignError(f"{prefix} does not match the lock: extra {extra[:10]}, missing {short[:10]}")
        marker.write_text(json.dumps(want, indent=1) + "\n")
        subprocess.run(["chmod", "-R", "a-w", str(prefix)], check=True)
    return prefix


def _ensure_tables(cfg, pins):
    tables = Path(cfg["_tables"])
    if not tables.exists():
        _git("clone", "--quiet", pins["cmip7_tables"]["repo"], str(tables))
    if _git("rev-parse", "HEAD", cwd=tables) != pins["cmip7_tables"]["commit"]:
        _git("checkout", "--quiet", "--detach", pins["cmip7_tables"]["commit"], cwd=tables)


def _check_tables(cfg, pins):
    tables = Path(cfg["_tables"])
    head = _git("rev-parse", "HEAD", cwd=tables)
    if head != pins["cmip7_tables"]["commit"]:
        raise CampaignError(f"{tables} is at {head[:10]}, the production pins {pins['cmip7_tables']['commit'][:10]}")
    if _git("status", "--porcelain", "--untracked-files=no", cwd=tables):
        raise CampaignError(f"{tables} has local changes")


def _esgvoc_env(cfg):
    return {**os.environ, "ESGVOC_HOME": cfg["_esgvoc"]}


def _ensure_esgvoc(cfg, pins):
    """Install the pinned CV versions into the campaign's own ESGVOC_HOME."""
    marker = Path(cfg["_esgvoc"]) / "pycmor-pinned.json"
    want = pins["esgvoc"]
    if marker.exists():
        if json.loads(marker.read_text()) != want:
            raise CampaignError(f"{cfg['_esgvoc']} holds other CV versions than the production pins")
        return
    Path(cfg["_esgvoc"]).mkdir(parents=True, exist_ok=True)
    esgvoc = str(Path(cfg["_env"]) / "bin" / "esgvoc")
    _run([esgvoc, "use", *(f"{p}@{v}" for p, v in want.items())], env=_esgvoc_env(cfg), what="esgvoc use")
    marker.write_text(json.dumps(want, indent=1) + "\n")


def _dreq_file(cfg, pins):
    return Path(cfg["_dreq"]) / pins["dreq"]["version"] / "metadata.json"


def _ensure_dreq(cfg, pins):
    meta = _dreq_file(cfg, pins)
    if not meta.exists():
        meta.parent.mkdir(parents=True, exist_ok=True)
        tool = str(Path(cfg["_env"]) / "bin" / "export_dreq_lists_json")
        experiments = meta.parent / f"{pins['dreq']['version']}_experiments.json"
        _run([tool, "-a", pins["dreq"]["version"], str(experiments), "-m", str(meta)], what="export_dreq_lists_json")
    _check_dreq(cfg, pins)


def _check_dreq(cfg, pins):
    meta = _dreq_file(cfg, pins)
    if not meta.exists() or _sha256(meta) != pins["dreq"]["metadata_sha256"]:
        raise CampaignError(f"{meta} is missing or differs from the Data Request {pins['dreq']['version']} pinned")


def _inputs_record(cfg):
    return Path(cfg["_dir"]) / "provenance" / "inputs.json"


def _ensure_inputs(cfg, pins):
    """md5 every pinned static input, then remember its size and mtime."""
    record = {}
    for path, md5 in pins.get("inputs", {}).items():
        if not Path(path).is_file():
            raise CampaignError(f"input {path} is missing")
        if _sha256(path, "md5") != md5:
            raise CampaignError(f"input {path} differs from the file the production was validated with")
        st = os.stat(path)
        record[path] = [st.st_size, st.st_mtime_ns]
    _inputs_record(cfg).parent.mkdir(exist_ok=True)
    _inputs_record(cfg).write_text(json.dumps(record, indent=1) + "\n")


def _check_inputs(cfg, pins):
    rec = _inputs_record(cfg)
    if not rec.exists():
        raise CampaignError(f"{rec} missing; run setup")
    record = json.loads(rec.read_text())
    if set(record) != set(pins.get("inputs", {})):
        raise CampaignError(f"{rec} covers other inputs than the production pins; run setup")
    for path, (size, mtime) in record.items():
        st = os.stat(path) if Path(path).exists() else None
        if st is None or [st.st_size, st.st_mtime_ns] != [size, mtime]:
            raise CampaignError(f"input {path} changed since setup")


def setup_production(cfg):
    """Everything outside the pinned code that a production campaign reads."""
    pdir, pins = _production(cfg)
    _ensure_env(cfg, pdir)
    _ensure_tables(cfg, pins)
    _check_tables(cfg, pins)
    _ensure_esgvoc(cfg, pins)
    _ensure_dreq(cfg, pins)
    _ensure_inputs(cfg, pins)


def verify_production(cfg):
    pdir, pins = _production(cfg)
    marker = Path(cfg["_env"]) / ENV_MARKER
    if not marker.exists() or json.loads(marker.read_text()) != _env_want(cfg, pdir):
        raise CampaignError(f"{cfg['_env']} is missing or not built from the pinned lock files; run setup")
    _check_tables(cfg, pins)
    if json.loads((Path(cfg["_esgvoc"]) / "pycmor-pinned.json").read_text()) != pins["esgvoc"]:
        raise CampaignError(f"{cfg['_esgvoc']} holds other CV versions than the production pins")
    _check_dreq(cfg, pins)
    _check_inputs(cfg, pins)


def setup(cfg, repin=False):
    code = Path(cfg["_code"])
    pin_path = Path(cfg["_pin"])
    if not code.exists():
        if pin_path.exists():
            raise CampaignError(f"{pin_path} exists but {code} is missing; restore it or remove the pin deliberately")
        _git("clone", "--quiet", "--no-checkout", cfg["code_repo"], str(code))
    else:
        _git("fetch", "--quiet", "origin", cwd=code)
    sha = _resolve(code, cfg["code_ref"])
    view = _pinned_view(cfg, sha)
    if pin_path.exists():
        old = json.loads(pin_path.read_text())
        if old != view and not repin:
            changed = sorted(k for k in set(view) | set(old) if old.get(k) != view.get(k))
            raise CampaignError(
                f"campaign is pinned and {', '.join(changed)} changed since. Years already done used the "
                "pinned values. If the change is deliberate, rerun setup with --repin."
            )
    _git("checkout", "--quiet", "--detach", sha, cwd=code)
    if not pin_path.exists() or repin:
        pin_path.write_text(json.dumps(view, indent=1) + "\n")
        stamp = datetime.datetime.now().isoformat(timespec="seconds")
        with open(Path(cfg["_dir"]) / "pin_history.log", "a") as fh:
            fh.write(f"{stamp} {os.environ.get('USER', '?')} pinned {sha} ({cfg['code_ref']})\n")
    for d in (cfg["_logs"], cfg["workroot"], cfg["output_root"]):
        Path(d).mkdir(parents=True, exist_ok=True)
    if not Path(cfg["_dir"]).is_relative_to(cfg["campaign_root"]):
        print(f"note: {cfg['_dir']} is outside campaign_root {cfg['campaign_root']}", file=sys.stderr)
    prov = Path(cfg["_dir"]) / "provenance"
    prov.mkdir(exist_ok=True)
    python = sys.executable
    if cfg["production"]:
        setup_production(cfg)
        python = str(Path(cfg["_env"]) / "bin" / "python")
    freeze = subprocess.run([python, "-m", "pip", "freeze"], capture_output=True, text=True)
    (prov / "pip_freeze.txt").write_text(freeze.stdout)
    tables = _env_values(cfg)["CMIP7_TABLES_PATH"]
    tsha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=tables, capture_output=True, text=True).stdout.strip()
    (prov / "cmip7_tables.txt").write_text(f"{tables} {tsha or 'unknown'}\n")
    verify(cfg)
    return sha


def verify(cfg):
    """Raise unless code/ is exactly the pinned commit, clean, and the
    data-defining part of campaign.yaml still matches the pin."""
    code = Path(cfg["_code"])
    pin_path = Path(cfg["_pin"])
    if not pin_path.exists():
        raise CampaignError(f"{pin_path} missing; run setup first")
    pin = json.loads(pin_path.read_text())
    head = _git("rev-parse", "HEAD", cwd=code)
    if head != pin["code_commit"]:
        raise CampaignError(f"{code} is at {head[:10]} but the campaign is pinned to {pin['code_commit'][:10]}")
    dirty = _git("status", "--porcelain", "--untracked-files=no", cwd=code)
    if dirty:
        raise CampaignError(f"{code} has local changes:\n{dirty}")
    current = _pinned_view(cfg, head)
    if current != pin:
        changed = sorted(k for k in set(current) | set(pin) if pin.get(k) != current.get(k))
        raise CampaignError(f"campaign.yaml changed {', '.join(changed)} since it was pinned; rerun setup with --repin")
    if cfg["production"]:
        verify_production(cfg)
    return head


def _env_values(cfg):
    """Where the campaign's software and reference data come from."""
    if not cfg["production"]:
        return {
            "PYCMOR_CONDA_ENV": cfg["conda_env"],
            "CMIP7_TABLES_PATH": os.environ.get(
                "CMIP7_TABLES_PATH", "/work/ab0246/a270092/software/cmip7-cmor-tables/tables"
            ),
        }
    _, pins = _production(cfg)
    return {
        "PYCMOR_PRODUCTION": cfg["production"],
        "PYCMOR_CONDA_ENV": cfg["_env"],
        "CMIP7_TABLES_PATH": str(Path(cfg["_tables"]) / "tables"),
        "ESGVOC_HOME": cfg["_esgvoc"],
        # run_hr_shard.sh refreshes the CVs to @latest unless this is set
        "PYCMOR_ESGVOC_PINNED": 1,
        "PYCMOR_DREQ_METADATA": str(_dreq_file(cfg, pins)),
    }


def env(cfg):
    pin = json.loads(Path(cfg["_pin"]).read_text())
    pythonpath = f"{cfg['_code']}/src"
    if os.environ.get("PYTHONPATH"):
        pythonpath += ":" + os.environ["PYTHONPATH"]
    values = {
        "PYCMOR_CAMPAIGN": cfg["_file"],
        "CAMPAIGN_DIR": cfg["_dir"],
        "JOB_TAG": cfg["name"],
        "PYCMOR_HOME": cfg["_code"],
        "PYTHONPATH": pythonpath,
        "PYCMOR_CODE_COMMIT": pin["code_commit"],
        "EXPERIMENT": cfg["experiment"],
        "MODEL": cfg["model"],
        "RUN_ROOT": cfg["run_root"],
        "FIRST_YEAR": cfg["first_year"],
        "LAST_YEAR": cfg["last_year"],
        "WORKROOT": cfg["workroot"],
        "OUTPUT_ROOT": cfg["output_root"],
        "LOGS_DIR": cfg["_logs"],
        "PYCMOR_DRS_VERSION": cfg["drs_version"],
        # Multi-year accumulators (pfull_mon) belong to the campaign.
        "PYCMOR_CLIMATOLOGY_STATE_DIR": str(Path(cfg["_dir"]) / "climatologies"),
        "MAIL_USER": cfg["mail"],
        "ACCOUNT": cfg["account"],
        "DRIVER_ATTEMPTS": cfg["driver_attempts"],
        "FIRST_YEAR_ATTEMPTS": cfg["first_year_attempts"],
        "MAX_CONSECUTIVE_FAILS": cfg["max_consecutive_fails"],
        "PYCMOR_CONDA_INIT": cfg["conda_init"],
        "PYCMOR_CAMPAIGN_ROOT": cfg["campaign_root"],
        # Knobs of submit_hr_year_shards.sh that change what a year contains.
        # Set here so a value left in the shell that starts the campaign
        # cannot reach its jobs through --export=ALL. The driver sets TIER
        # itself when it resubmits a tier.
        "WITH_GR": "yes" if cfg["with_gr"] else "no",
        "TIER": "",
        "RULES": "",
        **_env_values(cfg),
    }
    return "\n".join(f"export {k}={shlex.quote(str(v))}" for k, v in values.items())


def _scalar(value):
    # JSON scalars and flow sequences are valid YAML and keep strings quoted.
    return json.dumps(value)


def _patch_inherit(text, overrides):
    lines = text.splitlines(keepends=True)
    try:
        start = next(i for i, ln in enumerate(lines) if re.match(r"^inherit:\s*(#.*)?$", ln))
    except StopIteration:
        raise CampaignError("no top-level inherit: block")
    end = next((i for i in range(start + 1, len(lines)) if re.match(r"^[A-Za-z_]", lines[i])), len(lines))
    indent = next(
        (
            re.match(r"^(\s+)[A-Za-z_]", lines[i]).group(1)
            for i in range(start + 1, end)
            if re.match(r"^\s+[A-Za-z_]", lines[i])
        ),
        "  ",
    )
    missing = []
    for key, value in overrides.items():
        pat = re.compile(rf"^{re.escape(indent)}{re.escape(key)}:(\s.*)?$")
        hit = next((i for i in range(start + 1, end) if pat.match(lines[i].rstrip("\n"))), None)
        new = f"{indent}{key}: {_scalar(value)}\n"
        if hit is None:
            missing.append(new)
        else:
            lines[hit] = new
    lines[start + 1 : start + 1] = missing
    return "".join(lines)


def _rebind_dreq(text, name, version, path):
    """Point CMIP7_DReq_metadata at the campaign's checked copy. The tier yaml
    must already name the pinned version, so a yaml and its production cannot
    disagree about which Data Request the output follows."""
    pat = re.compile(r"^(\s*CMIP7_DReq_metadata:\s*)(\S+)\s*$", re.M)
    values = [m.group(2).strip("\"'") for m in pat.finditer(text)]
    wrong = [v for v in values if f"/{version}/" not in v]
    if wrong:
        raise CampaignError(f"{name}: CMIP7_DReq_metadata {wrong[0]} is not Data Request {version}")
    return pat.sub(lambda m: f"{m.group(1)}{_scalar(path)}", text)


def apply(cfg, yamls_dir):
    """Bind every generated tier yaml in ``yamls_dir`` to this campaign: point
    in-repo paths at the pinned code and add the campaign's extra inherit
    values. The experiment metadata itself was written by repoint_hr_year.py
    from the campaign's profile; this checks that it arrived.
    The result is checked structurally, so a value that did not land, or an
    edit that disturbed anything else, stops the run."""
    overrides = cfg["inherit"]
    code = cfg["_code"]
    # Profile values are yaml text as repoint writes them; compare parsed.
    profile = {k: yaml.safe_load(str(v)) for k, v in experiment_profiles(cfg["model"])[cfg["experiment"]].items()}
    profile.setdefault("experiment_id", cfg["experiment"])
    files = sorted(Path(yamls_dir).glob("*.yaml"))
    if not files:
        raise CampaignError(f"no yamls in {yamls_dir}")
    dreq = None
    if cfg["production"]:
        _, pins = _production(cfg)
        dreq = (pins["dreq"]["version"], str(_dreq_file(cfg, pins)))
    for f in files:
        original = f.read_text()
        rebound = original.replace(TEMPLATE_REPO + "/", code + "/")
        if dreq:
            rebound = _rebind_dreq(rebound, f.name, *dreq)
        patched = _patch_inherit(rebound, overrides)
        expected = yaml.safe_load(rebound)
        expected.setdefault("inherit", {})
        expected["inherit"].update(copy.deepcopy(overrides))
        try:
            got = yaml.safe_load(patched)
        except yaml.YAMLError as exc:
            got = exc
        if got != expected:
            raise CampaignError(f"{f.name}: patching inherit changed more than the campaign values")
        if TEMPLATE_REPO + "/" in patched:
            raise CampaignError(f"{f.name}: still refers to {TEMPLATE_REPO}")
        for key, value in profile.items():
            have = got["inherit"].get(key)
            if have != value:
                raise CampaignError(
                    f"{f.name}: inherit {key}={have!r}, but the {cfg['experiment']!r} profile says {value!r}. "
                    "Was EXPERIMENT passed to repoint_hr_year.py (and MODEL, for LR)?"
                )
        for rule in got.get("rules") or []:
            if rule.get("experiment_id", cfg["experiment"]) != cfg["experiment"]:
                raise CampaignError(f"{f.name}: rule {rule.get('name')!r} sets experiment_id={rule['experiment_id']!r}")
            for key, value in overrides.items():
                if key in rule and rule[key] != value:
                    raise CampaignError(
                        f"{f.name}: rule {rule.get('name')!r} sets {key}={rule[key]!r} itself, "
                        f"so the campaign value {value!r} would not reach it"
                    )
        f.write_text(patched)
    return len(files)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("setup")
    p.add_argument("campaign")
    p.add_argument("--repin", action="store_true", help="accept a changed code_ref or campaign values")
    for name in ("env", "verify"):
        sub.add_parser(name).add_argument("campaign")
    p = sub.add_parser("apply")
    p.add_argument("campaign")
    p.add_argument("yamls_dir")
    args = ap.parse_args(argv)
    try:
        cfg = load(args.campaign)
        if args.cmd == "setup":
            sha = setup(cfg, repin=args.repin)
            print(f"campaign {cfg['name']}: code pinned at {sha[:10]} in {cfg['_code']}")
        elif args.cmd == "env":
            print(env(cfg))
        elif args.cmd == "verify":
            print(f"campaign {cfg['name']}: code {verify(cfg)[:10]} matches the pin")
        elif args.cmd == "apply":
            print(f"campaign {cfg['name']}: bound {apply(cfg, args.yamls_dir)} tier yaml(s)")
    except CampaignError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
