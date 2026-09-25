#!/usr/bin/env python3
"""Shard a pycmor tier yaml into N shard yamls for SLURM-level isolation.

Implements step 1 of PLAN_slurm_shard_isolation.md.

Two modes:

1. **shard mode** (default): split the ``rules:`` list into K shards of
   roughly ``shard_size`` rules each. Uses a *deterministic shuffle* to
   defeat alphabetical / grouped-by-frequency clustering in the source
   yaml, then chunks. Writes ``<tier>_shard_NN.yaml`` files.

2. **fixup mode**: produce a single yaml containing only the rules whose
   ``name`` matches one of ``--rule-names``. Used by ``validate_shards.py``
   to re-run only failed rules.

All non-``rules:`` sections (``general``, ``pycmor``, ``jobqueue``,
``pipelines``, ``inherit``, ``distributed``) are copied verbatim from
the source yaml. Comments and YAML anchors are not preserved in the
output (yaml.safe_dump materialises anchors) but pycmor only reads the
parsed data, not the source text.
"""

import argparse
import os
import random
import sys
from typing import List

import yaml


def _load_source_yaml(path: str) -> dict:
    with open(path, "r") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise SystemExit(f"{path}: yaml root is not a mapping")
    if "rules" not in data or not isinstance(data["rules"], list):
        raise SystemExit(f"{path}: yaml has no rules: list")
    return data


def _emit_shard_yaml(source: dict, rules: list, out_path: str) -> None:
    """Write one shard yaml by cloning source and replacing rules:."""
    shard = dict(source)
    shard["rules"] = rules
    with open(out_path, "w") as fh:
        yaml.safe_dump(shard, fh, sort_keys=False)


def _shuffle_rules(rules: list, seed: int) -> list:
    """Deterministic shuffle. Same seed → same order. Used to defeat
    alphabetical / grouped-by-frequency clustering before chunking."""
    shuffled = list(rules)
    random.Random(seed).shuffle(shuffled)
    return shuffled


def shard_mode(
    source_path: str,
    shard_size: int,
    out_dir: str,
    seed: int = 42,
) -> List[str]:
    """Split source yaml into shards of ``shard_size`` rules each.

    Returns the list of written shard-yaml paths.
    """
    source = _load_source_yaml(source_path)
    rules = source["rules"]
    n_rules = len(rules)
    if shard_size < 1:
        raise SystemExit(f"shard_size must be >=1 (got {shard_size})")

    # shard_size is an UPPER BOUND. Compute K = ceil(N/shard_size) and
    # distribute rules as evenly as possible across K shards, so the
    # last shard isn't a tiny outlier (e.g. 64 rules @ size 20 →
    # 4 shards of 16, not [20,20,20,4]). Even distribution keeps
    # per-shard wall time uniform, which matters because the SLURM
    # array's total wall time is dominated by its slowest shard.
    n_shards = (n_rules + shard_size - 1) // shard_size
    shuffled = _shuffle_rules(rules, seed)
    # ``shuffled[i::n_shards]`` is the canonical "deal cards" partition:
    # each shard gets every K-th element. After the shuffle, this is
    # equivalent to a uniform random partition of size ⌈N/K⌉ or ⌊N/K⌋.
    chunks = [shuffled[i::n_shards] for i in range(n_shards)]

    tier_stem = os.path.splitext(os.path.basename(source_path))[0]
    os.makedirs(out_dir, exist_ok=True)
    written: List[str] = []
    for i, chunk in enumerate(chunks):
        out_path = os.path.join(out_dir, f"{tier_stem}_shard_{i:02d}.yaml")
        _emit_shard_yaml(source, chunk, out_path)
        written.append(out_path)
    return written


def fixup_mode(
    source_path: str,
    rule_names: List[str],
    out_path: str,
) -> str:
    """Emit a single yaml containing only the rules whose ``name`` is in
    ``rule_names``. Used by the validator to re-run failures."""
    source = _load_source_yaml(source_path)
    keep_set = set(rule_names)
    kept = [r for r in source["rules"] if r.get("name") in keep_set]
    missing = keep_set - {r.get("name") for r in kept}
    if missing:
        raise SystemExit(f"rule names not found in {source_path}: {sorted(missing)}")
    if not kept:
        raise SystemExit(f"fixup yaml would be empty (no matching rules in {source_path})")
    _emit_shard_yaml(source, kept, out_path)
    return out_path


def _parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("source", help="source tier yaml")
    sub = p.add_subparsers(dest="mode", required=True)

    s = sub.add_parser("shard", help="split source into shard yamls")
    s.add_argument("--shard-size", type=int, default=20, help="rules per shard (default: 20)")
    s.add_argument("--out-dir", required=True, help="output directory for shard yamls")
    s.add_argument("--seed", type=int, default=42, help="shuffle seed for reproducibility (default: 42)")

    f = sub.add_parser("fixup", help="emit single yaml with only named rules")
    f.add_argument("--rule-names", required=True, help="comma-separated rule names to keep")
    f.add_argument("--out", required=True, help="output yaml path")

    return p.parse_args(argv)


def main(argv: List[str]) -> int:
    args = _parse_args(argv)
    if args.mode == "shard":
        written = shard_mode(args.source, args.shard_size, args.out_dir, args.seed)
        for path in written:
            print(path)
        sys.stderr.write(f"# wrote {len(written)} shard yamls to {args.out_dir}\n")
    else:
        rule_names = [r.strip() for r in args.rule_names.split(",") if r.strip()]
        if not rule_names:
            raise SystemExit("--rule-names is empty")
        out_path = fixup_mode(args.source, rule_names, args.out)
        print(out_path)
        sys.stderr.write(f"# wrote fixup yaml with {len(rule_names)} rules to {out_path}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
