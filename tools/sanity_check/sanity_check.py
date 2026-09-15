#!/usr/bin/env python
"""Sanity-check pycmor HR output against doc/sanity_check_ranges.md.

Features:
  - Chunked reads via netCDF4 directly (no full-array load), so 4D model-level
    files don't blow memory or hang.
  - Per-file subprocess with hard timeout: a stuck file is killed, walk continues.
  - Streaming JSONL output: each result is appended on completion, so a kill
    or crash never loses what was already done.
  - Resume: reads existing JSONL and skips files already recorded.
  - Explicit handling of _FillValue / missing_value plus common CF sentinels
    (1e20, 9.97e36) so all-fill files surface as 'non-finite' rather than
    polluting min/max/mean with sentinel values.

Two modes:
  Driver:   python sanity_check.py [DIR ...]
  Worker:   python sanity_check.py --worker FILE   (single file, JSON to stdout)
"""
from __future__ import annotations

import argparse
import json
import os
# Must be set BEFORE blosc/HDF5 is loaded (i.e. before importing netCDF4).
# Pycmor writes with blosc_zstd; on big files single-thread decompression
# bottlenecks at 100-300 MB/s. With 4 threads the worker is ~3-4x faster.
os.environ.setdefault("BLOSC_NTHREADS", "4")
os.environ.setdefault("HDF5_USE_FILE_LOCKING", "FALSE")
import re
import subprocess
import sys
import time
import warnings
from collections import Counter, defaultdict
from pathlib import Path
import math

warnings.filterwarnings("ignore")

# Defaults can be overridden by env vars or CLI args (see argparse below).
ROOT = Path(os.environ.get(
    "PYCMOR_SANITY_ROOT",
    "/scratch/a/a270092/pycmor_hr/Test_16n_y1587/cmorized"))
TABLE = Path(os.environ.get(
    "PYCMOR_SANITY_TABLE",
    str(Path(__file__).resolve().parents[2] / "doc" / "sanity_check_ranges.md")))
JSONL = Path(os.environ.get(
    "PYCMOR_SANITY_JSONL",
    "/tmp/sanity_check_results.jsonl"))
LIVE_DIRS = [
    "cap7_aerosol", "cap7_atm", "cap7_land", "cap7_ocean", "cap7_seaice",
    "core_atm", "core_land", "core_ocean", "core_seaice",
    "extra_atm", "extra_land",
    "lrcs_land", "lrcs_ocean", "lrcs_seaice",
    "veg_atm", "veg_land", "veg_seaice",
]


# ---------- bounds parsing ----------

def parse_value(s: str):
    if s is None:
        return None
    s = s.strip().lstrip("~")
    if "/" in s:
        for part in re.split(r"[/]", s):
            v = parse_value(part)
            if v is not None:
                return v
        return None
    if "varies" in s.lower():
        return None
    s = re.sub(r"\(.*?\)", "", s).strip()
    tokens = s.split()
    if not tokens:
        return None
    try:
        return float(tokens[0].replace(",", ""))
    except ValueError:
        return None


def parse_table(path: Path) -> dict:
    out = {}
    with open(path) as fh:
        for line in fh:
            if not line.startswith("|"):
                continue
            if line.strip().startswith("|---"):
                continue
            if "Variable" in line and "Realm" in line:
                continue
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if len(cells) < 7:
                continue
            var, realm, units, vmin, vmean, vmax, rationale = cells[:7]
            if not re.match(r"^[A-Za-z][A-Za-z0-9_]*$", var):
                continue
            out[var] = {
                "realm": realm, "units": units,
                "min": parse_value(vmin), "mean": parse_value(vmean), "max": parse_value(vmax),
                "min_raw": vmin, "mean_raw": vmean, "max_raw": vmax,
                "rationale": rationale,
            }
    return out


# ---------- chunked stats ----------

def _mask_fills(arr, fills):
    """Set any fill-sentinel match to NaN in one pass (one mask alloc)."""
    import numpy as np
    if not fills:
        return arr
    mask = np.zeros(arr.shape, dtype=bool)
    for fv in fills:
        tol = max(abs(fv) * 1e-6, 1e-30)
        mask |= np.abs(arr - fv) <= tol
    arr[mask] = np.nan
    return arr


def chunked_stats(nc_var):
    """Compute global min/max/sum/count over a netCDF4 Variable in chunks.

    Strategy: iterate the outermost axis (usually time). For each slab,
    mask any value matching _FillValue/missing_value, then nanmin/max/sum/count.
    """
    import numpy as np
    # Force auto-masking on (default True; this is belt-and-suspenders)
    try:
        nc_var.set_auto_mask(True)
        nc_var.set_auto_scale(True)
    except Exception:
        pass
    # Manual fill-value list as fallback for variables missing _FillValue
    fills = []
    for attr in ("_FillValue", "missing_value", "fill_value"):
        if hasattr(nc_var, attr):
            v = getattr(nc_var, attr)
            try:
                fills.append(float(v))
            except Exception:
                try:
                    fills.extend(float(x) for x in v)
                except Exception:
                    pass
    # Common CF fills that some pipelines forget to declare
    fills.extend([1.0e20, 9.969209968386869e+36])
    fills = list(set(fills))  # dedupe; cheap
    shape = nc_var.shape
    if not shape:
        # Scalar
        v = float(nc_var[()])
        return v, v, v, 1, 1

    # If only one dim or first dim is small, do whole-array
    if len(shape) == 1 or shape[0] == 1:
        sl = nc_var[...]
        if isinstance(sl, np.ma.MaskedArray):
            arr = sl.filled(np.nan).astype(np.float64, copy=False)
        else:
            arr = np.asarray(sl, dtype=np.float64)
        arr = _mask_fills(arr, fills)
        return _stats_one(arr)

    # Outermost dim = time (usually). Step through.
    # If the time dim is very large but per-slab too big for memory,
    # we could subdivide further; for HR daily 4D this is ~2GB/year — fine.
    n0 = shape[0]
    # Adaptive chunk: target ~256 MB per chunk
    per_step_size = 1
    for s in shape[1:]:
        per_step_size *= s
    target_bytes = 256 * 1024 * 1024
    bytes_per_step = per_step_size * 8  # float64
    chunk = max(1, target_bytes // max(bytes_per_step, 1))
    chunk = min(chunk, n0)

    gmin = math.inf
    gmax = -math.inf
    gsum = 0.0
    gn_finite = 0
    gn_total = 0

    for i0 in range(0, n0, chunk):
        i1 = min(i0 + chunk, n0)
        sl = nc_var[i0:i1]
        if isinstance(sl, np.ma.MaskedArray):
            arr = sl.filled(np.nan).astype(np.float64, copy=False)
        else:
            arr = np.asarray(sl, dtype=np.float64)
        arr = _mask_fills(arr, fills)
        finite_mask = np.isfinite(arr)
        n_fin = int(finite_mask.sum())
        n_tot = int(arr.size)
        if n_fin > 0:
            chunk_min = float(np.nanmin(arr))
            chunk_max = float(np.nanmax(arr))
            chunk_sum = float(np.nansum(arr))
            if chunk_min < gmin: gmin = chunk_min
            if chunk_max > gmax: gmax = chunk_max
            gsum += chunk_sum
        gn_finite += n_fin
        gn_total += n_tot

    if gn_finite == 0:
        return float("nan"), float("nan"), float("nan"), 0, gn_total
    gmean = gsum / gn_finite
    return gmin, gmean, gmax, gn_finite, gn_total


def _stats_one(arr):
    import numpy as np
    arr = np.asarray(arr, dtype=np.float64)
    finite = np.isfinite(arr)
    n_fin = int(finite.sum()); n_tot = int(arr.size)
    if n_fin == 0:
        return float("nan"), float("nan"), float("nan"), 0, n_tot
    return (float(np.nanmin(arr)), float(np.nansum(arr) / n_fin),
            float(np.nanmax(arr)), n_fin, n_tot)


def find_primary_var_nc(nc, hint):
    """Pick the data variable matching the filename hint."""
    if hint in nc.variables:
        return hint
    for v in nc.variables:
        if v.lower() == hint.lower():
            return v
    # Pick variable with most dims, excluding known coord/bound names
    excludes = {"time", "lat", "lon", "lev", "vertices", "bnds", "type",
                "time_bnds", "lat_bnds", "lon_bnds", "lev_bnds", "longitude",
                "latitude", "depth", "depth_bnds", "ncells", "nv", "basin"}
    candidates = []
    for vn, var in nc.variables.items():
        if vn in excludes or vn.endswith("_bnds") or vn.endswith("_bounds"):
            continue
        candidates.append((vn, var.ndim, var.size))
    if not candidates:
        return None
    candidates.sort(key=lambda x: (-x[1], -x[2]))
    return candidates[0][0]


def fname_var(name):
    return name.split("_", 1)[0]


# CMIP table-id-like cadence tokens that may appear in cmorized filenames
# between the branding suffix and the area/grid fields, e.g.
# ``pr_tavg-u-hxy-u_1hr_glb_gn_...nc``  →  cadence ``1hr``.
_CADENCE_TOKENS = frozenset(
    ("1hr", "3hr", "6hr", "day", "mon", "yr", "dec",
     "monC", "monPt", "decC", "yrPt", "fx")
)


def fname_cadence(name):
    """Pull the CMIP cadence/table-id token from a cmorized filename.

    Returns the token (e.g. ``"1hr"``) or ``None`` if no recognised cadence
    token is present. Robust to optional extra tokens before/after.
    """
    parts = Path(name).stem.split("_")
    for p in parts[1:]:
        if p in _CADENCE_TOKENS:
            return p
    return None


def lookup_bounds(var, cadence, bounds_table):
    """Most-specific bounds lookup: ``var_cadence`` first, fallback to ``var``.

    Lets the sanity_check_ranges table carry per-cadence overrides where the
    physical range scales with the averaging interval (e.g. precipitation
    extremes), while leaving variables without cadence-dependent extremes on
    a single row keyed by plain variable name.
    """
    if cadence is not None:
        b = bounds_table.get(f"{var}_{cadence}")
        if b is not None:
            return b
    return bounds_table.get(var)


# ---------- classification ----------

def classify(actual_min, actual_mean, actual_max, bounds):
    notes = []
    em, ex, en = bounds["min"], bounds["max"], bounds["mean"]
    sev = "PASS"
    def upd(level):
        nonlocal sev
        order = {"PASS": 0, "WARN": 1, "FAIL": 2}
        if order[level] > order[sev]:
            sev = level
    if not math.isfinite(actual_min) or not math.isfinite(actual_max):
        return "FAIL", ["non-finite min/max"]
    # Wide-envelope: expected min and max span >2 orders of magnitude (e.g.
    # grid-cell area for 2-120 km edges). For these the natural value scales
    # with resolution, so a single expected_mean and the "shrunk-field" spread
    # check produce false positives. Suppress those two checks; the min/max
    # envelope alone is the meaningful sanity check.
    em_abs = abs(em) if em is not None else 0.0
    ex_abs = abs(ex) if ex is not None else 0.0
    wide_envelope = (em_abs > 0 and ex_abs > 0
                     and max(em_abs, ex_abs) / min(em_abs, ex_abs) > 100)
    if em is not None:
        scale = max(abs(em), abs(ex or 0), 1e-30)
        slack = 0.2 * scale
        if actual_min < em - slack:
            ratio = (em - actual_min) / max(abs(em), 1e-30)
            if ratio > 5 or actual_min < em - 10 * slack:
                upd("FAIL"); notes.append(f"min {actual_min:.3g} below expected_min {em:.3g}")
            else:
                upd("WARN"); notes.append(f"min {actual_min:.3g} slightly below expected_min {em:.3g}")
    if ex is not None:
        scale = max(abs(ex), abs(em or 0), 1e-30)
        slack = 0.2 * scale
        if actual_max > ex + slack:
            ratio = (actual_max - ex) / max(abs(ex), 1e-30)
            if ratio > 5:
                upd("FAIL"); notes.append(f"max {actual_max:.3g} above expected_max {ex:.3g}")
            else:
                upd("WARN"); notes.append(f"max {actual_max:.3g} slightly above expected_max {ex:.3g}")
    if en is not None and math.isfinite(actual_mean) and not wide_envelope:
        if en == 0.0:
            scale = max(abs(ex or 1.0), abs(em or 1.0), 1e-30)
            ratio = abs(actual_mean) / scale
            if ratio > 0.1:
                # 10–100% of the expected envelope is suspicious; >100% is wrong
                lvl = "FAIL" if ratio > 1.0 else "WARN"
                upd(lvl); notes.append(f"mean {actual_mean:.3g} far from expected_mean ~0")
        else:
            ratio = actual_mean / en
            if ratio < 0 and abs(en) > 1e-12:
                upd("FAIL"); notes.append(f"mean {actual_mean:.3g} wrong sign vs expected ~{en:.3g}")
            elif abs(ratio) > 0:
                # Magnitude check: how many orders of magnitude off?
                if abs(ratio) >= 100 or abs(ratio) <= 0.01:
                    # Two or more orders of magnitude — almost certainly a
                    # unit / scale bug, not a tuning issue.
                    upd("FAIL"); notes.append(f"mean {actual_mean:.3g} off by {ratio:.2g}x vs expected ~{en:.3g}")
                elif abs(ratio) > 5 or abs(ratio) < 0.2:
                    upd("WARN"); notes.append(f"mean {actual_mean:.3g} off by {ratio:.2g}x vs expected ~{en:.3g}")
    # Scale-too-small check: the observed range can be entirely inside the
    # expected envelope yet much smaller in magnitude than expected. That
    # happens for balanced quantities like sea-ice mass transport
    # (expected_mean=0, bounds ±1e8 kg/s) where a sign / unit bug shrinks
    # the field to ±1. The mean-check misses this because the mean is also
    # ~0. Compare observed magnitude (max of |min|, |max|) to the expected
    # envelope magnitude.
    if (em is not None and ex is not None and not wide_envelope
            and math.isfinite(actual_min) and math.isfinite(actual_max)):
        observed_spread = max(abs(actual_max), abs(actual_min))
        expected_spread = max(abs(em), abs(ex))
        if expected_spread > 0 and observed_spread > 0:
            srat = observed_spread / expected_spread
            if srat <= 0.1:
                # 10x or more smaller than expected — likely missing unit
                # conversion, wrong source field, or a flux that should be
                # accumulating but isn't.
                upd("FAIL"); notes.append(
                    f"observed range [{actual_min:.3g}, {actual_max:.3g}] is "
                    f"{srat:.2g}x the expected envelope — magnitude too small"
                )
            elif srat <= 0.3:
                upd("WARN"); notes.append(
                    f"observed range [{actual_min:.3g}, {actual_max:.3g}] is "
                    f"{srat:.2g}x the expected envelope — possibly too small"
                )
    return sev, notes


# ---------- worker mode ----------

def worker_main(filepath, table_path):
    """Read one file with netCDF4 (chunked), classify, emit JSON to stdout."""
    import netCDF4
    bounds_table = parse_table(Path(table_path))
    p = Path(filepath)
    var_name = fname_var(p.name)
    cadence = fname_cadence(p.name)
    bounds = lookup_bounds(var_name, cadence, bounds_table)
    record = {"file": str(p), "var": var_name, "cadence": cadence}
    try:
        nc = netCDF4.Dataset(str(p), "r")
    except Exception as e:
        record.update({"status": "ERROR", "notes": [f"open failed: {type(e).__name__}: {e}"]})
        print(json.dumps(record))
        return
    try:
        primary = find_primary_var_nc(nc, var_name)
        if primary is None:
            record.update({"status": "ERROR", "notes": ["no data variable"]})
            print(json.dumps(record))
            return
        var = nc.variables[primary]
        t0 = time.time()
        gmin, gmean, gmax, n_fin, n_tot = chunked_stats(var)
        # The file's own ``:realm`` global attribute is authoritative — it
        # reflects the compound's branding (e.g. rlds_tavg-u-hxy-si carries
        # ``realm=seaIce``, distinct from rlds_tavg-u-hxy-u's ``realm=atmos``
        # even though the bounds table is keyed by out_name and only stores
        # one realm per variable). Without this, every branded variant of a
        # shared out_name gets routed to the bounds-table realm's HTML page.
        file_realm = getattr(nc, "realm", None)
        record.update({
            "primary": primary, "shape": list(var.shape),
            "n_finite": n_fin, "n_total": n_tot,
            "min": gmin, "mean": gmean, "max": gmax,
            "elapsed_s": round(time.time() - t0, 2),
            "units_in_file": getattr(var, "units", "?"),
        })
        if bounds is None:
            record.update({"status": "NOBOUNDS",
                           "notes": ["no entry in sanity table"]})
            if file_realm:
                record["realm"] = str(file_realm)
        else:
            status, notes = classify(gmin, gmean, gmax, bounds)
            record.update({
                "status": status, "notes": notes,
                "expected_min": bounds["min_raw"], "expected_mean": bounds["mean_raw"],
                "expected_max": bounds["max_raw"],
                "units": bounds["units"],
                "realm": str(file_realm) if file_realm else bounds["realm"],
            })
        print(json.dumps(record))
    except Exception as e:
        record.update({"status": "ERROR",
                       "notes": [f"read failed: {type(e).__name__}: {str(e)[:200]}"]})
        print(json.dumps(record))
    finally:
        try: nc.close()
        except Exception: pass


# ---------- driver mode ----------

def already_done():
    """Return set of file paths that already have a result in JSONL."""
    seen = set()
    if JSONL.exists():
        with open(JSONL) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                    seen.add(r["file"])
                except Exception:
                    continue
    return seen


def append_jsonl(record):
    with open(JSONL, "a") as fh:
        fh.write(json.dumps(record) + "\n")


def _run_one(args):
    """Module-level so it can be pickled by ProcessPoolExecutor."""
    fpath, dname, timeout, table_path, self_path = args
    try:
        r = subprocess.run(
            [sys.executable, self_path, "--worker", fpath,
             "--table", table_path],
            capture_output=True, text=True, timeout=timeout,
        )
        line = r.stdout.strip().splitlines()[-1] if r.stdout.strip() else ""
        try:
            rec = json.loads(line) if line else {}
        except Exception:
            rec = {}
        if not rec:
            rec = {"file": fpath, "var": fname_var(Path(fpath).name),
                   "status": "ERROR",
                   "notes": [f"worker no JSON: rc={r.returncode}; "
                             f"stderr={r.stderr.strip()[:200]}"]}
    except subprocess.TimeoutExpired:
        rec = {"file": fpath, "var": fname_var(Path(fpath).name),
               "status": "ERROR", "notes": [f"timeout {timeout}s"]}
    except Exception as e:
        rec = {"file": fpath, "var": fname_var(Path(fpath).name),
               "status": "ERROR",
               "notes": [f"driver: {type(e).__name__}: {e}"]}
    rec["dir"] = dname
    return rec


def driver_main(dirs, timeout, max_parallel):
    from concurrent.futures import ProcessPoolExecutor, as_completed

    seen = already_done()
    print(f"Resume: {len(seen)} already in {JSONL}", file=sys.stderr)

    self_path = os.path.abspath(__file__)
    work = []
    for d in dirs:
        dpath = ROOT / d
        if not dpath.is_dir():
            print(f"SKIP {d}: not a directory", file=sys.stderr)
            continue
        for f in sorted(dpath.rglob("*.nc")):
            if str(f) in seen:
                continue
            work.append((str(f), d, timeout, str(TABLE), self_path))
    print(f"Queue: {len(work)} files (parallel={max_parallel}, timeout={timeout}s/file)",
          file=sys.stderr)

    with ProcessPoolExecutor(max_workers=max_parallel) as ex:
        for i, rec in enumerate(ex.map(_run_one, work, chunksize=1)):
            append_jsonl(rec)
            tag = rec.get("status", "?")
            short = Path(rec["file"]).name[:80]
            note = "; ".join(rec.get("notes", []))[:160]
            elap = rec.get("elapsed_s", "")
            print(f"  [{tag:9s}] [{rec['dir']}] {short}  {note}  ({elap}s)",
                  file=sys.stderr)
            if (i + 1) % 50 == 0:
                print(f"  ... {i+1}/{len(work)} done", file=sys.stderr)


# ---------- entry ----------

def main():
    global ROOT, TABLE, JSONL
    p = argparse.ArgumentParser(
        description="Sanity-check CMORized output against literature bounds.",
    )
    p.add_argument("--worker", help="single-file worker mode: path to .nc")
    p.add_argument("--root", default=str(ROOT),
                   help=f"root of cmorized output (default: {ROOT})")
    p.add_argument("--table", default=str(TABLE),
                   help="path to sanity_check_ranges.md")
    p.add_argument("--jsonl", default=str(JSONL),
                   help="output JSONL path (resume key)")
    p.add_argument("--timeout", type=int, default=180,
                   help="per-file timeout in seconds (driver only)")
    p.add_argument("--parallel", type=int,
                   default=int(os.environ.get("NPROC", "8")))
    p.add_argument("dirs", nargs="*",
                   help=f"subdirs to walk under --root (default: {LIVE_DIRS})")
    a = p.parse_args()
    ROOT = Path(a.root)
    TABLE = Path(a.table)
    JSONL = Path(a.jsonl)
    if a.worker:
        worker_main(a.worker, a.table)
        return
    dirs = a.dirs or LIVE_DIRS
    driver_main(dirs, a.timeout, a.parallel)


if __name__ == "__main__":
    main()
