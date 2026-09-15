#!/usr/bin/env python
"""Print a per-dir, per-var summary of a sanity_check.py JSONL,
classified by likely cause (data integrity / unit mismatch / sign /
piControl non-zero / bounds-too-tight / bounds-or-peak).

Reads PYCMOR_SANITY_JSONL env var or /tmp/sanity_check_results.jsonl by default.
Pass an explicit path as the first arg to override.
"""
import json
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

JSONL = Path(sys.argv[1] if len(sys.argv) > 1
             else os.environ.get("PYCMOR_SANITY_JSONL",
                                 "/tmp/sanity_check_results.jsonl"))
records = []
with open(JSONL) as fh:
    for line in fh:
        line = line.strip()
        if not line: continue
        try:
            records.append(json.loads(line, parse_constant=lambda x: float("nan")))
        except Exception:
            pass

print("=" * 72)
print(f"SANITY CHECK FINAL SUMMARY — {len(records)} files processed")
print("=" * 72)

status_count = Counter(r.get("status","?") for r in records)
print("\n>>> Overall\n")
for s in ("PASS","WARN","FAIL","ERROR","NOBOUNDS"):
    n = status_count.get(s,0)
    pct = 100.0*n/max(len(records),1)
    print(f"  {s:9s} {n:4d}  ({pct:5.1f}%)")

print("\n>>> By directory\n")
by_dir = defaultdict(Counter)
for r in records:
    by_dir[r.get("dir","?")][r.get("status","?")] += 1
for d in sorted(by_dir):
    c = by_dir[d]
    parts = [f"{s}={c[s]}" for s in ("PASS","WARN","FAIL","ERROR","NOBOUNDS") if c.get(s)]
    print(f"  {d:18s}  {' '.join(parts)}")

# Slowest files
print("\n>>> Slowest 10 files (chunked-read elapsed)\n")
slow = sorted([r for r in records if r.get("elapsed_s") is not None],
              key=lambda r: r.get("elapsed_s") or 0, reverse=True)
for r in slow[:10]:
    print(f"  {r.get('elapsed_s',0):7.1f}s  [{r.get('dir','?')}]  {Path(r['file']).name[:80]}")

# Group failures
def severity_class(notes):
    text = "; ".join(notes).lower()
    if "non-finite" in text:
        return "DATA_INTEGRITY"
    if "wrong sign" in text:
        return "SIGN_FLIP"
    m = re.search(r"off by ([\d.eE+-]+)x", text)
    if m:
        try:
            f = float(m.group(1))
            if f > 1e3:
                return "UNIT_MISMATCH"
        except Exception:
            pass
    if "above expected_max 0" in text or "below expected_min 0" in text:
        return "PICONTROL_NONZERO"
    if "slightly" in text:
        return "BOUNDS_TIGHT_MINOR"
    return "BOUNDS_OR_PEAK"

# Per-variable collapse
var_status = defaultdict(lambda: {"counts": Counter(), "examples": []})
for r in records:
    v = r.get("var","?")
    var_status[v]["counts"][r.get("status","?")] += 1
    if r.get("status") == "FAIL" and len(var_status[v]["examples"]) < 1:
        var_status[v]["examples"].append({
            "file": Path(r.get("file","")).name,
            "dir": r.get("dir","?"),
            "notes": r.get("notes",[]),
            "min": r.get("min"), "mean": r.get("mean"), "max": r.get("max"),
            "expected_min": r.get("expected_min"),
            "expected_mean": r.get("expected_mean"),
            "expected_max": r.get("expected_max"),
            "units": r.get("units"),
            "units_in_file": r.get("units_in_file"),
        })
fail_vars = [(v,x) for v,x in var_status.items() if x["counts"].get("FAIL",0) > 0]
fail_vars.sort(key=lambda v: (-v[1]["counts"].get("FAIL",0), v[0]))

by_sev = defaultdict(list)
for v, x in fail_vars:
    ex = x["examples"][0] if x["examples"] else {"notes": []}
    sev = severity_class(ex["notes"])
    by_sev[sev].append((v, x["counts"], ex))

SEV_ORDER = ["DATA_INTEGRITY", "UNIT_MISMATCH", "SIGN_FLIP",
             "PICONTROL_NONZERO", "BOUNDS_OR_PEAK", "BOUNDS_TIGHT_MINOR"]

print("\n>>> Variables with FAIL, grouped by severity\n")
for sev in SEV_ORDER:
    grp = by_sev.get(sev, [])
    if not grp: continue
    print(f"\n--- {sev} ({len(grp)} vars) ---")
    for v, c, ex in grp:
        cs = " ".join(f"{s}={c[s]}" for s in c if c[s])
        note = "; ".join(ex.get("notes",[]))[:140]
        print(f"  {v:25s}  [{cs}]  {note}")
        if ex.get("min") is not None:
            print(f"     actual=[{ex['min']:.3g} | {ex['mean']:.3g} | {ex['max']:.3g}] "
                  f"file_units={ex.get('units_in_file','?')}")
        print(f"     expected=[{ex.get('expected_min')} | {ex.get('expected_mean')} | "
              f"{ex.get('expected_max')}] table_units={ex.get('units','?')}")
        print(f"     example file: {ex.get('dir','?')}/{ex.get('file','?')[:80]}")

# Errors
errs = [r for r in records if r.get("status") == "ERROR"]
if errs:
    print(f"\n>>> ERRORs ({len(errs)})\n")
    for r in errs[:30]:
        print(f"  [{r.get('dir','?')}] {Path(r['file']).name[:80]}  {'; '.join(r.get('notes',[]))[:160]}")

# WARN-only (no FAIL)
warn_only = sorted({v for v, x in var_status.items()
                    if x["counts"].get("WARN",0) > 0 and x["counts"].get("FAIL",0) == 0})
print(f"\n>>> Variables with WARN only ({len(warn_only)})")
print("  ", ", ".join(warn_only))
