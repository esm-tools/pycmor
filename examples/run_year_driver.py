#!/usr/bin/env python3
"""Drive one cmorization year to completion without a human in the loop.

Submit the year, wait, check what each shard actually produced, resubmit the
tiers that came up short, and give up after a bounded number of attempts with
an email rather than silently shipping an incomplete year.

Why this exists
---------------
Two failure modes cost us a full year of output before this script existed:

1. A rule could raise, be caught by ``serial_process``, and leave the shard
   exiting 0. SLURM recorded COMPLETED and nothing pointed at the gap. The
   companion fix is in ``cli.py``: a shard now exits non-zero when any rule
   produced no output.
2. A healthy shard could be scancelled by its own inactivity watchdog
   (cli120 extra_atm, 17 files). Retrying by hand is fine once; across ~200
   years per experiment it is not.

Completion is judged from the per-shard manifests written by pycmor
(``PYCMOR_MANIFEST``), not from SLURM job state, because job state cannot see
a rule that returned cleanly and wrote nothing. A rule the decadal gate turned
off counts as complete: it is *meant* to produce nothing in that year.

Usage
-----
    run_year_driver.py <RUN_ROOT> <YEAR> <WORKDIR> [--attempts 3]
                       [--email ADDR] [--poll 120] [--dry-run]

On success it writes ``<WORKDIR>/YEAR.done`` so a supervisor loop can march
through years by checking for that file. On giving up it writes the list of
problems to ``<WORKDIR>/YEAR.FAILED`` and exits 1. In production this runs
inside ``run_year_chain.sbatch``, whose SLURM FAIL mail is the notification.
"""

import argparse
import glob
import json
import os
import re
import shutil
import smtplib
import subprocess
import sys
import time
from email.message import EmailMessage
from pathlib import Path

HERE = Path(__file__).resolve().parent
SUBMIT = HERE / "submit_hr_year_shards.sh"
DEFAULT_EMAIL = os.environ.get("NOTIFY_EMAIL", "jan.streffing@awi.de")


def log(msg):
    print(f"[driver {time.strftime('%Y-%m-%dT%H:%M:%S')}] {msg}", flush=True)


def tier_of(manifest_or_shard_name):
    """`extra_atm_shard_00` -> `extra_atm`."""
    return re.sub(r"_shard_\d+$", "", manifest_or_shard_name)


def expected_shards(workdir):
    """Shard yamls that were generated, as {shard_stem: tier}."""
    out = {}
    for path in glob.glob(str(Path(workdir) / "shards" / "*" / "*.yaml")):
        stem = Path(path).stem
        out[stem] = tier_of(stem)
    return out


def read_manifests(workdir):
    """{shard_stem: manifest dict} for every manifest written so far."""
    found = {}
    for path in glob.glob(str(Path(workdir) / "cmorized" / "_manifests" / "*.json")):
        try:
            with open(path) as fh:
                found[Path(path).stem] = json.load(fh)
        except Exception as exc:  # a truncated manifest is itself a failure
            log(f"WARNING: could not read {path}: {exc!r}")
    return found


def assess(workdir):
    """Return (tiers_needing_retry, human_readable_problems)."""
    shards = expected_shards(workdir)
    manifests = read_manifests(workdir)
    bad_tiers = set()
    problems = []

    for stem, tier in sorted(shards.items()):
        man = manifests.get(stem)
        if man is None:
            bad_tiers.add(tier)
            problems.append(f"{stem}: no manifest (shard died before finishing)")
            continue
        incomplete = man.get("incomplete") or []
        if incomplete:
            bad_tiers.add(tier)
            problems.append(f"{stem}: {len(incomplete)} rule(s) without output: " + ", ".join(sorted(incomplete)))
    return bad_tiers, problems


def submit(run_root, year, workdir, tier=None, dry_run=False):
    env = dict(os.environ)
    if tier:
        env["TIER"] = tier
    cmd = [str(SUBMIT), str(run_root), str(year), str(workdir)]
    log(f"submit: {'TIER=' + tier + ' ' if tier else ''}{' '.join(cmd)}")
    if dry_run:
        return []
    res = subprocess.run(cmd, env=env, capture_output=True, text=True)
    sys.stdout.write(res.stdout)
    sys.stderr.write(res.stderr)
    if res.returncode != 0:
        raise SystemExit(f"submit failed with rc={res.returncode}")
    return re.findall(r"jid=(\d+)", res.stdout)


def wait_for(job_ids, poll=120):
    """Block until none of the given job ids are in the queue."""
    if not job_ids:
        return
    log(f"waiting on {len(job_ids)} job id(s): {', '.join(job_ids)}")
    while True:
        alive = []
        for jid in job_ids:
            res = subprocess.run(["squeue", "-h", "-j", jid, "-o", "%i"], capture_output=True, text=True)
            if res.stdout.strip():
                alive.append(jid)
        if not alive:
            log("all submitted jobs have left the queue")
            return
        time.sleep(poll)


def notify(email, subject, body):
    """Send mail, preferring the local MTA and falling back to SMTP."""
    log(f"notifying {email}: {subject}")
    if shutil.which("mail"):
        res = subprocess.run(["mail", "-s", subject, email], input=body, text=True)
        if res.returncode == 0:
            return True
        log(f"`mail` returned rc={res.returncode}; trying SMTP")
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = email
    msg["To"] = email
    msg.set_content(body)
    try:
        with smtplib.SMTP("localhost", timeout=30) as smtp:
            smtp.send_message(msg)
        return True
    except Exception as exc:
        log(f"ERROR: could not send mail: {exc!r}")
        return False


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run_root")
    ap.add_argument("year")
    ap.add_argument("workdir")
    ap.add_argument("--attempts", type=int, default=3, help="total attempts before giving up (default 3)")
    ap.add_argument(
        "--email",
        default=DEFAULT_EMAIL,
        help="address to mail when the year gives up; pass '' when SLURM's --mail-type=FAIL does the mailing",
    )
    ap.add_argument("--poll", type=int, default=120, help="seconds between queue checks")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    workdir = Path(args.workdir)
    done_marker = workdir / f"{args.year}.done"
    failed_marker = workdir / f"{args.year}.FAILED"
    if done_marker.exists():
        log(f"{done_marker} already exists; nothing to do")
        return 0
    workdir.mkdir(parents=True, exist_ok=True)
    # All attempts of this year write into one DRS version directory. Under
    # run_year_chain.sbatch this is already set for the whole campaign.
    os.environ.setdefault("PYCMOR_DRS_VERSION", time.strftime("v%Y%m%d"))
    log(f"DRS version: {os.environ['PYCMOR_DRS_VERSION']}")

    problems = []
    bad_tiers = set()
    for attempt in range(1, args.attempts + 1):
        log(f"=== attempt {attempt}/{args.attempts} for year {args.year} ===")
        if attempt == 1:
            jids = submit(args.run_root, args.year, workdir, dry_run=args.dry_run)
        else:
            jids = []
            for tier in sorted(bad_tiers):
                jids += submit(args.run_root, args.year, workdir, tier=tier, dry_run=args.dry_run)
        if args.dry_run:
            log("dry run; stopping before the wait")
            return 0

        wait_for(jids, poll=args.poll)
        bad_tiers, problems = assess(workdir)

        if not bad_tiers:
            done_marker.write_text(f"completed after {attempt} attempt(s) at {time.strftime('%Y-%m-%dT%H:%M:%S')}\n")
            failed_marker.unlink(missing_ok=True)  # a rerun fixed an earlier give-up
            log(f"year {args.year} complete; wrote {done_marker}")
            return 0

        log(f"attempt {attempt} incomplete; tiers needing retry: {', '.join(sorted(bad_tiers))}")
        for p in problems:
            log(f"  {p}")

    body = (
        f"Cmorization of year {args.year} is still incomplete after "
        f"{args.attempts} attempts and will not be retried automatically.\n\n"
        f"Run root : {args.run_root}\n"
        f"Workdir  : {workdir}\n"
        f"Outputs  : {workdir / 'cmorized'}\n"
        f"Manifests: {workdir / 'cmorized' / '_manifests'}\n\n"
        "Outstanding problems:\n" + "\n".join(f"  - {p}" for p in problems) + "\n\n"
        "Nothing was deleted. Re-running the driver will pick up from the "
        "existing outputs.\n"
    )
    # SLURM's own mail only says "job failed", so the details live here where
    # the mail's job name points you.
    failed_marker.write_text(body)
    log(f"giving up; details in {failed_marker}")
    if args.email:
        notify(args.email, f"[pycmor] year {args.year} incomplete after {args.attempts} attempts", body)
    return 1


if __name__ == "__main__":
    sys.exit(main())
