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
    inherit:                       # overrides for every tier's inherit block
      experiment_id: 1pctCO2
      parent_experiment_id: piControl
      parent_time_units: days since 1850-01-01
      branch_time_in_parent: 36524.0
      branch_time_in_child: 0.0

Optional: ``workroot`` (default ``<campaign>/work``; put it in the same
project as ``output_root`` so publishing is a rename, not a copy),
``account`` (ab0246), ``driver_attempts`` (3), ``first_year_attempts`` (1, for
the first year of every launch), ``max_consecutive_fails`` (2),
``conda_init``, ``conda_env``, ``code_repo`` (default: this checkout).
"""

import argparse
import copy
import datetime
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

REQUIRED = ("name", "run_root", "first_year", "last_year", "output_root", "drs_version", "mail", "code_ref", "inherit")
# What determines the content of the output. Changing any of these after
# setup would make one campaign's years disagree with each other.
PINNED_FIELDS = ("run_root", "output_root", "drs_version", "inherit")
DEFAULTS = {
    "account": "ab0246",
    "driver_attempts": 3,
    "first_year_attempts": 1,
    "max_consecutive_fails": 2,
    "conda_init": "/work/ab0246/a270092/software/miniforge3/etc/profile.d/conda.sh",
    "conda_env": "pycmor_py312",
}


class CampaignError(Exception):
    pass


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
    if not isinstance(cfg["inherit"], dict) or "experiment_id" not in cfg["inherit"]:
        raise CampaignError("inherit must be a mapping that sets at least experiment_id")
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
    cfg["_file"] = str(path)
    cfg["_dir"] = str(cdir)
    cfg["_code"] = str(cdir / "code")
    cfg["_logs"] = str(cdir / "logs")
    cfg["_pin"] = str(cdir / "pinned.json")
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
    return {"code_commit": sha, **{k: cfg[k] for k in PINNED_FIELDS}}


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
            changed = sorted(k for k in view if old.get(k) != view.get(k))
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
    prov = Path(cfg["_dir"]) / "provenance"
    prov.mkdir(exist_ok=True)
    freeze = subprocess.run([sys.executable, "-m", "pip", "freeze"], capture_output=True, text=True)
    (prov / "pip_freeze.txt").write_text(freeze.stdout)
    tables = os.environ.get("CMIP7_TABLES_PATH", "/work/ab0246/a270092/software/cmip7-cmor-tables/tables")
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
        changed = sorted(k for k in current if pin.get(k) != current.get(k))
        raise CampaignError(f"campaign.yaml changed {', '.join(changed)} since it was pinned; rerun setup with --repin")
    return head


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
        "RUN_ROOT": cfg["run_root"],
        "FIRST_YEAR": cfg["first_year"],
        "LAST_YEAR": cfg["last_year"],
        "WORKROOT": cfg["workroot"],
        "OUTPUT_ROOT": cfg["output_root"],
        "LOGS_DIR": cfg["_logs"],
        "PYCMOR_DRS_VERSION": cfg["drs_version"],
        "MAIL_USER": cfg["mail"],
        "ACCOUNT": cfg["account"],
        "DRIVER_ATTEMPTS": cfg["driver_attempts"],
        "FIRST_YEAR_ATTEMPTS": cfg["first_year_attempts"],
        "MAX_CONSECUTIVE_FAILS": cfg["max_consecutive_fails"],
        "PYCMOR_CONDA_INIT": cfg["conda_init"],
        "PYCMOR_CONDA_ENV": cfg["conda_env"],
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


def apply(cfg, yamls_dir):
    """Bind every generated tier yaml in ``yamls_dir`` to this campaign: point
    in-repo paths at the pinned code and set the campaign's inherit values.
    The result is checked structurally, so a value that did not land, or an
    edit that disturbed anything else, stops the run."""
    overrides = cfg["inherit"]
    code = cfg["_code"]
    files = sorted(Path(yamls_dir).glob("*.yaml"))
    if not files:
        raise CampaignError(f"no yamls in {yamls_dir}")
    for f in files:
        original = f.read_text()
        rebound = original.replace(TEMPLATE_REPO + "/", code + "/")
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
        for rule in got.get("rules") or []:
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
