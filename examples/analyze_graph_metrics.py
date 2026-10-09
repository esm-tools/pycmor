#!/usr/bin/env python3
"""Parse GRAPH_METRIC / GRAPH_RESULT records from a campaign's per-shard
logs and tabulate per-rule + per-shard scheduler load.

Wires into the instrumentation added to ``_safe_to_netcdf`` and
``_save_mfdataset_worker_or_sync`` in ``pycmor/std_lib/files.py``. Each
save attempt emits two lines like:

    GRAPH_METRIC rule=<id> backend=<worker_compute|sync|eager> nodes=<n> layers=<n> bytes=<n> chunks=<n>
    GRAPH_RESULT rule=<id> backend=<...> status=<ok|fallback> elapsed_s=<float> [exc=<typename>]

We want to answer:
  - Per-rule: how big is the graph that gets shipped to the scheduler?
  - Per-shard: peak concurrent in-flight bytes at any moment?
  - Which rules / shards exceeded the scheduler's apparent bandwidth?

Usage:
    analyze_graph_metrics.py <log-dir-or-glob>
    analyze_graph_metrics.py /work/ab0246/a270092/software/pycmor/pycmor_hr_shard_*24838726*.log

Reports printed:
  1. Per-rule top-20 by graph bytes
  2. Per-shard peak concurrent in-flight bytes (start-overlap heuristic)
  3. Stuck/failed shards: which rules had outstanding GRAPH_METRIC with no
     matching GRAPH_RESULT (i.e. computation never returned)
"""
from __future__ import annotations

import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Iterable

METRIC_RE = re.compile(
    r"GRAPH_METRIC rule=(?P<rule>\S+) backend=(?P<backend>\S+) "
    r"nodes=(?P<nodes>\S+) layers=(?P<layers>\S+) bytes=(?P<bytes>\S+) chunks=(?P<chunks>\S+)"
)
RESULT_RE = re.compile(
    r"GRAPH_RESULT rule=(?P<rule>\S+) backend=(?P<backend>\S+) "
    r"status=(?P<status>\S+) elapsed_s=(?P<elapsed>\S+)"
)
# Timestamp at start of line: 08:00:37.414 | LEVEL or YYYY-MM-DD HH:MM:SS
TS_RE = re.compile(r"^(?:\d{4}-\d{2}-\d{2} )?(\d{2}):(\d{2}):(\d{2})(?:[\.,](\d+))?")


def _as_int(x: str):
    if x in ("None", "?"):
        return None
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def _as_float(x: str):
    if x in ("None", "?"):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _parse_ts(line: str):
    m = TS_RE.match(line)
    if not m:
        return None
    h, mi, s = int(m.group(1)), int(m.group(2)), int(m.group(3))
    frac = m.group(4)
    sub = float("0." + frac) if frac else 0.0
    return h * 3600 + mi * 60 + s + sub


def parse_log(path: Path):
    """Yield (kind, ts, rule, backend, fields_dict) per record."""
    with open(path, "r", errors="replace") as fh:
        for line in fh:
            mm = METRIC_RE.search(line)
            if mm:
                ts = _parse_ts(line)
                yield ("metric", ts, mm.group("rule"), mm.group("backend"), {
                    "nodes": _as_int(mm.group("nodes")),
                    "layers": _as_int(mm.group("layers")),
                    "bytes": _as_int(mm.group("bytes")),
                    "chunks": _as_int(mm.group("chunks")),
                })
                continue
            rm = RESULT_RE.search(line)
            if rm:
                ts = _parse_ts(line)
                yield ("result", ts, rm.group("rule"), rm.group("backend"), {
                    "status": rm.group("status"),
                    "elapsed": _as_float(rm.group("elapsed")),
                })


def main(argv):
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2

    paths = []
    for arg in argv[1:]:
        p = Path(arg)
        if p.is_dir():
            paths.extend(sorted(p.glob("pycmor_hr_shard_*.log")))
        else:
            from glob import glob
            paths.extend(Path(x) for x in glob(str(p)))

    if not paths:
        print("no logs found", file=sys.stderr)
        return 1

    # Per-rule + per-shard tables
    all_metrics = []  # (shard_log_name, ts, rule, backend, nodes, layers, bytes, chunks)
    pending = defaultdict(dict)  # (shard, rule) -> last unmatched metric record

    per_shard_records = defaultdict(list)
    for path in paths:
        shard = path.stem.replace("pycmor_hr_shard_", "")
        for kind, ts, rule, backend, fields in parse_log(path):
            if kind == "metric":
                rec = dict(fields)
                rec.update(ts=ts, rule=rule, backend=backend, shard=shard,
                           resolved=False, elapsed=None, status=None)
                per_shard_records[shard].append(rec)
                pending[(shard, rule)] = rec
            else:  # result
                key = (shard, rule)
                if key in pending:
                    rec = pending.pop(key)
                    rec["resolved"] = True
                    rec["elapsed"] = fields.get("elapsed")
                    rec["status"] = fields.get("status")
                    rec["t_end"] = ts

    # === Report 1: top-20 rules by graph bytes ===
    rules_by_bytes = []
    for shard, recs in per_shard_records.items():
        for r in recs:
            if r.get("bytes") is not None:
                rules_by_bytes.append((r["bytes"], r["nodes"], r["chunks"],
                                       r["rule"], shard, r["elapsed"], r["status"]))
    rules_by_bytes.sort(reverse=True)
    print("=" * 88)
    print("TOP 20 RULES BY GRAPH BYTES")
    print(f"{'bytes':>12} {'nodes':>7} {'chunks':>7}  rule / shard")
    print(f"{'(approx)':>12} {'':>7} {'':>7}  elapsed status")
    print("-" * 88)
    for b, n, c, rule, shard, el, st in rules_by_bytes[:20]:
        el_s = f"{el:.1f}s" if el is not None else "?"
        st_s = st if st else "?"
        print(f"{b:>12,d} {n!r:>7} {c!r:>7}  {rule} @ {shard}")
        print(f"{'':>28}  {el_s} / {st_s}")
    print()

    # === Report 2: per-shard peak concurrent in-flight bytes ===
    # Heuristic: for each shard, walk records in time order; add bytes when
    # GRAPH_METRIC fires, subtract when GRAPH_RESULT lands (we tracked t_end).
    print("=" * 88)
    print("PER-SHARD PEAK CONCURRENT IN-FLIGHT BYTES (worker_compute path only)")
    print(f"{'shard':<50} {'n_rules':>8} {'peak_in_flight':>14} {'unresolved':>11}")
    print("-" * 88)
    shard_peaks = []
    for shard, recs in sorted(per_shard_records.items()):
        # Only consider worker_compute path — sync path is in-process.
        events = []
        for r in recs:
            if r["backend"] != "worker_compute":
                continue
            if r["ts"] is None or r.get("bytes") is None:
                continue
            events.append((r["ts"], "start", r["bytes"]))
            if r["resolved"] and r.get("t_end") is not None:
                events.append((r["t_end"], "end", r["bytes"]))
        events.sort()
        cur = 0
        peak = 0
        for _, kind, b in events:
            if kind == "start":
                cur += b
            else:
                cur -= b
            if cur > peak:
                peak = cur
        n_rules = sum(1 for r in recs if r["backend"] in ("worker_compute", "sync"))
        unresolved = sum(1 for r in recs if not r["resolved"])
        shard_peaks.append((peak, shard, n_rules, unresolved))
    shard_peaks.sort(reverse=True)
    for peak, shard, n_rules, unresolved in shard_peaks:
        mark = " ← unresolved" if unresolved > 0 else ""
        print(f"{shard:<50} {n_rules:>8} {peak:>14,d} {unresolved:>11}{mark}")
    print()

    # === Report 3: unresolved rules — got GRAPH_METRIC but no GRAPH_RESULT ===
    # These are the wedged rules.
    print("=" * 88)
    print("UNRESOLVED RULES (saw GRAPH_METRIC, never saw GRAPH_RESULT — scheduler-wedge candidates)")
    print(f"{'rule':<25} {'shard':<50} {'bytes':>12} {'nodes':>7}")
    print("-" * 96)
    unresolved_list = []
    for shard, recs in per_shard_records.items():
        for r in recs:
            if not r["resolved"]:
                unresolved_list.append((r.get("bytes") or 0, r["rule"], shard, r.get("nodes")))
    unresolved_list.sort(reverse=True)
    for b, rule, shard, n in unresolved_list[:30]:
        print(f"{rule:<25} {shard:<50} {b:>12,d} {n!r:>7}")
    if not unresolved_list:
        print("(none — all rules' computes either completed or fell back successfully)")
    print()

    # === Report 4: aggregate ===
    n_total = sum(len(rs) for rs in per_shard_records.values())
    n_unresolved = len(unresolved_list)
    print("=" * 88)
    print(f"TOTAL GRAPH_METRIC records: {n_total}")
    print(f"UNRESOLVED: {n_unresolved}")
    print(f"PEAK PER-SHARD IN-FLIGHT BYTES: {max((p for p,_,_,_ in shard_peaks), default=0):,d}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
