#!/usr/bin/env python3
"""Validate shard outputs and emit a fixup yaml for any missing rules.

Implements step 4b of PLAN_slurm_shard_isolation.md.

For each tier yaml in <yamls-dir>, this script:
  1. Reads the rules:list
  2. Checks whether each rule produced its output(s) in OUTROOT/<tier>/
  3. Reports any rules with missing output
  4. Optionally writes a fixup yaml via shard_tier_yaml.py:fixup_mode

Output detection (must stay aligned with src/pycmor/std_lib/files.py
:create_filepath): a rule is considered "done" if its output_directory
contains at least one non-empty .nc file whose basename starts with the
rule's cmor_variable name + "_".

This is a deliberately conservative heuristic — it matches the same
check used by ``--skip-existing`` in ``_process_rule`` so the two stay
consistent.

Usage:
  validate_shards.py <workdir>
      Walk workdir/yamls/*.yaml, check workdir/cmorized/<tier>/cmorized/.
      Print missing-rule report. Exit 1 if anything missing.

  validate_shards.py <workdir> --emit-fixup
      As above, plus write workdir/fixup/<tier>_fixup.yaml for each tier
      that has missing rules. Print the fixup-yaml paths.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import List, Tuple

import yaml

# Reuse the fixup-yaml writer from the splitter so the two stay in sync.
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import shard_tier_yaml  # noqa: E402


def _output_dir_for_tier(workdir: Path, tier_yaml: Path) -> Path:
    """Mirror what submit_hr_year_shards.sh produces:
    OUTROOT/<short_tier>/cmorized/ where short_tier is the yaml stem
    with the cmip7_awiesm3-veg-hr_ prefix stripped."""
    short_tier = tier_yaml.stem.replace("cmip7_awiesm3-veg-hr_", "")
    return workdir / "cmorized" / short_tier / "cmorized"


def _rule_done(rule: dict, out_dir: Path) -> bool:
    """Return True if any non-empty .nc file in out_dir starts with
    rule['cmor_variable']_. Falls back to rule['compound_name'] parsing
    if cmor_variable is absent."""
    cmor_var = rule.get("cmor_variable")
    if not cmor_var:
        # CMIP7 rules may only have compound_name; the variable id is the
        # second dot-segment per CMIP7 DRS (e.g. seaIce.sidconcdyn.tavg-...).
        compound = rule.get("compound_name", "")
        parts = compound.split(".")
        if len(parts) >= 2:
            cmor_var = parts[1]
    if not cmor_var:
        return False  # can't determine name → run it
    prefix = f"{cmor_var}_"
    if not out_dir.is_dir():
        return False
    try:
        for entry in out_dir.iterdir():
            if entry.name.startswith(prefix) and entry.name.endswith(".nc"):
                try:
                    if entry.stat().st_size > 0:
                        return True
                except OSError:
                    pass
    except OSError:
        return False
    return False


def validate_tier(tier_yaml: Path, out_dir: Path) -> Tuple[List[str], List[str]]:
    """Return (done_rules, missing_rules) by rule-name for this tier."""
    with open(tier_yaml, "r") as fh:
        data = yaml.safe_load(fh)
    rules = data.get("rules", [])
    done: List[str] = []
    missing: List[str] = []
    for rule in rules:
        name = rule.get("name") or rule.get("cmor_variable") or "?"
        if _rule_done(rule, out_dir):
            done.append(name)
        else:
            missing.append(name)
    return done, missing


def main(argv: List[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("workdir", type=Path, help="campaign workdir (contains yamls/ and cmorized/)")
    p.add_argument("--emit-fixup", action="store_true", help="write fixup yamls for tiers with missing rules")
    args = p.parse_args(argv)

    yamls_dir = args.workdir / "yamls"
    if not yamls_dir.is_dir():
        raise SystemExit(f"no yamls directory at {yamls_dir}")

    fixup_dir = args.workdir / "fixup"
    if args.emit_fixup:
        fixup_dir.mkdir(parents=True, exist_ok=True)

    total_done = 0
    total_missing = 0
    tiers_with_missing = []
    for tier_yaml in sorted(yamls_dir.glob("*.yaml")):
        out_dir = _output_dir_for_tier(args.workdir, tier_yaml)
        done, missing = validate_tier(tier_yaml, out_dir)
        total_done += len(done)
        total_missing += len(missing)
        status = "OK" if not missing else f"{len(missing)} missing"
        print(f"  {tier_yaml.stem}: {len(done)}/{len(done) + len(missing)} done [{status}]")
        if missing:
            for name in missing:
                print(f"    MISSING: {name}")
            tiers_with_missing.append((tier_yaml, missing))

    print()
    print(f"Total: {total_done} done, {total_missing} missing across {len(list(yamls_dir.glob('*.yaml')))} tiers.")

    if args.emit_fixup:
        if not tiers_with_missing:
            print("No fixup needed.")
        else:
            print()
            print("Fixup yamls written:")
            for tier_yaml, missing in tiers_with_missing:
                fixup_path = fixup_dir / f"{tier_yaml.stem}_fixup.yaml"
                shard_tier_yaml.fixup_mode(str(tier_yaml), missing, str(fixup_path))
                print(f"  {fixup_path}  ({len(missing)} rules)")

    return 0 if total_missing == 0 else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
