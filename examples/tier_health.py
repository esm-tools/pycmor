#!/usr/bin/env python3
"""
Per-rule health summary for a pycmor tier run.

Parses a single pycmor SLURM log file and reports, per rule, whether it
succeeded, failed, or was silent (started but produced no `Good job! :-)`
marker). Useful as a tier-level health check on top of SLURM's coarse
COMPLETED/FAILED state, which doesn't reflect rule-level outcomes.

Usage:
  tier_health.py <pycmor_log_file>
  tier_health.py --glob '/path/to/pycmor_hr_*.log'   # multi-tier overview

Output (per-rule mode):
  <tier>  rules_started=N  succeeded=S  failed=F  silent=W

Exit code:
  0 if all started rules succeeded
  1 if any failed
  2 if any silent
"""
from __future__ import annotations

import argparse
import glob
import pathlib
import re
import sys
from collections import defaultdict


_BEGIN = re.compile(r"Beginning flow run '([^']+) - ([^']+)' for flow")
_RULE_FAIL = re.compile(r"ERROR: Rule '([^']+)' failed")
_GOOD = re.compile(r"Good job! :-\)")


def parse(log: pathlib.Path) -> dict:
    """Return {'started': set, 'succeeded': set, 'failed': set}."""
    started: set[str] = set()
    succeeded: set[str] = set()
    failed: set[str] = set()

    last_rule: str | None = None
    with log.open(errors="ignore") as f:
        for line in f:
            m = _BEGIN.search(line)
            if m:
                last_rule = m.group(2)
                started.add(last_rule)
                continue
            m = _RULE_FAIL.search(line)
            if m:
                failed.add(m.group(1))
                continue
            if _GOOD.search(line) and last_rule:
                succeeded.add(last_rule)
    silent = started - succeeded - failed
    return {
        "started": started,
        "succeeded": succeeded,
        "failed": failed,
        "silent": silent,
    }


def report(log: pathlib.Path, verbose: bool = False) -> int:
    state = parse(log)
    tier = log.name
    n_start = len(state["started"])
    n_ok = len(state["succeeded"])
    n_fail = len(state["failed"])
    n_silent = len(state["silent"])
    print(
        f"{tier}  rules_started={n_start}  succeeded={n_ok}  "
        f"failed={n_fail}  silent={n_silent}"
    )
    if verbose:
        if state["failed"]:
            print("  FAILED:", *sorted(state["failed"]))
        if state["silent"]:
            print("  SILENT:", *sorted(state["silent"]))
    if n_fail:
        return 1
    if n_silent:
        return 2
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("logs", nargs="*", help="log file(s) to analyze")
    p.add_argument("--glob", help="glob pattern matching log files")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="list silent + failed rule names")
    args = p.parse_args()

    paths: list[pathlib.Path] = []
    for f in args.logs:
        paths.append(pathlib.Path(f))
    if args.glob:
        paths.extend(pathlib.Path(p) for p in glob.glob(args.glob))
    if not paths:
        p.error("no log files given (use positional args or --glob)")
        return 2

    rcs = [report(p, verbose=args.verbose) for p in sorted(paths)]
    return max(rcs)


if __name__ == "__main__":
    raise SystemExit(main())
