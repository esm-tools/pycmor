#!/usr/bin/env python3
"""Give published AWI-ESM3 files one time reference and drop XIOS attributes, in place.

Before this was fixed, pycmor kept whatever time reference its input brought.
OpenIFS and FESOM write a different one per leg, so one dataset changed
``time:units`` from file to file (``days since 1850-01-01``, then
``days since 1852-01-01``, ...), and the pfull climatology said
``days since 1950-01-01``. esgf-qa reports that for nearly every dataset, and
``branch_time_in_child``, which CMIP7 defines in the child's own time units,
was only right in the files that happened to say 1850. The same files carry
XIOS's ``online_operation``, ``interval_operation`` and ``interval_write``
attributes, which describe the raw model stream rather than the product and
change within a dataset when the XIOS setup changed during a run (tos and
sos from 1873 in 1pctCO2).

This script rewrites ``time``, its ``bounds`` or ``climatology`` variable and
``time:units`` to ``days since 1850-01-01``, removes those three attributes,
and gives the file a new tracking_id and creation_date. The times themselves
do not move: every value is checked to decode to the same date as before. The
edit happens in place and keeps the file's size and layout, so cmip7repack's
packing stays as it was (checked with check_cmip7_packing), and the data are
not read at all. A file that already says ``days since 1850-01-01`` and has
none of the attributes is left alone, so the script can run any number of
times, also over a campaign that is still writing: files published later with
the fixed code are skipped.

With ``--remove-superseded`` it also removes a file that another file of the
same dataset covers completely. Rerunning the point-in-time rules (1hr, 3hr,
6hr tpt) and the daily storage changes (dcw, dgw, dslw, dsn, dsw) with the
fixed code writes their years under new names (``..._185001010100-185101010000``
instead of ``..._185001010100-185012312300``, ``..._18510101-18511231`` instead
of ``..._18510102-18511231``), and the old file would otherwise stay next to
the new one. Publishing removes them too from now on; this catches the ones
published before.

``--report-gaps`` changes nothing. It lists the hourly to daily datasets that
miss records between two files, as esgf-qa does, and the rules that wrote
them, ready for the ``rules:`` of a fix campaign.

Usage::

    repair_time_axis.py <member dir> [<member dir> ...] [--jobs 8] [--remove-superseded] [--apply]
    repair_time_axis.py <member dir> [<member dir> ...] --report-gaps

<member dir> is the ``.../<source_id>/<experiment_id>/<variant_label>``
directory of a DRS tree, for example
``/work/ik1017/ESGF_Buff/CMIP7/CMIP/AWI/AWI-ESM3-4-2-veg-HR/1pctCO2/r1i1p1f1``.
Without ``--apply`` it only lists what it would do.
"""

import argparse
import datetime
import re
import sys
import uuid
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import cftime
import netCDF4 as nc
import numpy as np

TAG = "repair_time_axis"
UNITS = "days since 1850-01-01"
XIOS_ATTRS = ("online_operation", "interval_operation", "interval_write")
RANGE = re.compile(r"^(?P<stem>.+)_(?P<start>\d{4,12})-(?P<end>\d{4,12})\.nc$")


def _time_vars(ds):
    """The time coordinate and the bounds/climatology variables that share its units."""
    if "time" not in ds.variables:
        return None, []
    t = ds["time"]
    extra = [getattr(t, a) for a in ("bounds", "climatology") if a in t.ncattrs()]
    return t, [n for n in extra if n in ds.variables]


def needs_repair(path):
    """What would change in ``path``: (new units?, attribute names to drop)."""
    with nc.Dataset(path) as ds:
        t, _ = _time_vars(ds)
        units = t is not None and "units" in t.ncattrs() and t.units != UNITS
        drop = sorted({a for v in ds.variables.values() for a in v.ncattrs() if a in XIOS_ATTRS})
    return units, drop


def _decoded(var, units, calendar):
    values = np.ma.filled(var[:].astype("f8"), np.nan)
    flat = values.reshape(-1)
    good = np.isfinite(flat)
    out = np.full(flat.shape, None, dtype=object)
    out[good] = cftime.num2date(flat[good], units, calendar)
    return out


def repair_file(path):
    path = Path(path)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        units, drop = needs_repair(path)
        if not units and not drop:
            return f"skip   {path.name}"
        what = []
        with nc.Dataset(path, "r+") as ds:
            ds.set_auto_maskandscale(False)
            t, others = _time_vars(ds)
            if units:
                old = t.units
                calendar = getattr(t, "calendar", "standard")
                before = {n: _decoded(ds[n], old, calendar) for n in [t.name, *others]}
                origin = cftime.num2date(0.0, old, calendar)
                shift = float(cftime.date2num(origin, UNITS, calendar))
                for name in [t.name, *others]:
                    v = ds[name]
                    v[...] = v[...].astype("f8") + shift
                t.units = UNITS
                for name, dates in before.items():
                    after = _decoded(ds[name], UNITS, calendar)
                    if not all(a == b for a, b in zip(after, dates)):
                        raise RuntimeError(f"{name}: dates changed while shifting by {shift} days")
                what.append(f"time units {old!r} -> {UNITS!r}")
            for v in ds.variables.values():
                for a in XIOS_ATTRS:
                    if a in v.ncattrs():
                        v.delncattr(a)
            if drop:
                what.append("dropped " + ", ".join(drop))
            ds.tracking_id = f"hdl:21.14107/{uuid.uuid4()}"
            ds.creation_date = stamp
            line = f"{stamp}: {TAG}.py: " + "; ".join(what)
            history = getattr(ds, "history", "")
            ds.history = line + ("\n" + history if history else "")
    except Exception as exc:
        return f"FAILED {path}: {type(exc).__name__}: {exc}"
    return f"done   {path.name} ({'; '.join(what)})"


def _stamp(text, end):
    """A file-name time stamp as a comparable tuple; an end stamp covers its whole last unit."""
    fields = [int(text[0:4])] + [int(text[i : i + 2]) for i in range(4, len(text), 2)]
    return tuple(fields + [99 if end else 0] * (6 - len(fields)))


def superseded(member):
    """Files another file of the same dataset covers completely.

    Within one dataset that only happens when a rerun wrote a year under a
    wider range, so the covered file is the old one. Modification times are
    no help: this script's own repair touches the old file too.
    """
    out = []
    for leaf in sorted({p.parent for p in member.rglob("*.nc")}):
        spans = []
        for p in leaf.glob("*.nc"):
            m = RANGE.match(p.name)
            if m and len(m["start"]) == len(m["end"]):
                spans.append((p, m["stem"], _stamp(m["start"], False), _stamp(m["end"], True)))
        for p, stem, start, end in spans:
            for q, qstem, qstart, qend in spans:
                if q != p and qstem == stem and qstart <= start and end <= qend:
                    out.append((p, q))
                    break
    return out


# Sampling interval in days of the frequencies whose files are years of a
# regular series; monthly and longer have no fixed step.
STEP = {"1hr": 1 / 24, "3hr": 1 / 8, "6hr": 1 / 4, "day": 1.0}


def _first_last(path):
    """First and last time of ``path`` in days since 1850-01-01, whatever units it says."""
    with nc.Dataset(path) as ds:
        t = ds["time"]
        calendar = getattr(t, "calendar", "standard")
        ends = cftime.num2date([float(t[0]), float(t[-1])], t.units, calendar)
        return tuple(float(v) for v in cftime.date2num(ends, UNITS, calendar))


def _rule_names():
    """{(region, frequency, variable, branding): [rule names]} of the HR tier yamls next to this script."""
    import yaml

    out = {}
    for f in sorted((Path(__file__).resolve().parents[1] / "awi-esm3-veg-hr-variables").glob("*/*.yaml")):
        doc = yaml.safe_load(f.read_text()) or {}
        for rule in doc.get("rules") or []:
            parts = str(rule.get("compound_name", "")).split(".")
            if len(parts) == 5:
                _, variable, branding, frequency, region = parts
                out.setdefault((region, frequency, variable, branding), []).append(rule["name"])
    return out


def gaps(member, jobs):
    """{dataset dir: (missing steps, first gap)} for the regular series under ``member``."""
    leaves = sorted(
        {p.parent for p in member.rglob("*.nc") if p.relative_to(member).parts[1] in STEP},
    )
    out = {}
    with ProcessPoolExecutor(max(1, jobs)) as pool:
        for leaf in leaves:
            files = sorted(leaf.glob("*.nc"))
            if len(files) < 2:
                continue
            step = STEP[leaf.relative_to(member).parts[1]]
            spans = list(pool.map(_first_last, files, chunksize=8))
            missing, first = 0, None
            for (a, (_, a_last)), (b, (b_first, _)) in zip(zip(files, spans), zip(files[1:], spans[1:])):
                n = round((b_first - a_last) / step) - 1
                if n > 0:
                    missing += n
                    first = first or (a.name, b.name)
            if missing:
                out[leaf] = (missing, first)
    return out


def report_gaps(members, jobs):
    names = _rule_names()
    rules, unknown = set(), []
    for member in members:
        found = gaps(member.resolve(), jobs)
        print(f"# {member}: {len(found)} dataset(s) with gaps between files")
        for leaf, (missing, (a, b)) in sorted(found.items()):
            region, frequency, variable, branding = leaf.relative_to(member.resolve()).parts[:4]
            hit = names.get((region, frequency, variable, branding))
            if hit:
                rules.update(hit)
            else:
                unknown.append(leaf)
            print(f"gap  {leaf.relative_to(member.resolve())}: {missing} missing, first between {a} and {b}")
    print(f"# rules to run again: [{', '.join(sorted(rules))}]")
    for leaf in unknown:
        print(f"# no HR rule found for {leaf}")
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("members", type=Path, nargs="+", help=".../<source_id>/<experiment_id>/<variant_label>")
    p.add_argument("--jobs", type=int, default=8)
    p.add_argument("--remove-superseded", action="store_true", help="also remove files a newer file covers")
    p.add_argument(
        "--report-gaps",
        action="store_true",
        help="only list hourly to daily datasets with records missing between files, and the rules that wrote them",
    )
    p.add_argument("--apply", action="store_true", help="change files; without it, only list what would change")
    args = p.parse_args(argv)
    if args.report_gaps:
        return report_gaps(args.members, args.jobs)

    files, gone = [], []
    for member in args.members:
        member = member.resolve()
        if not member.is_dir():
            p.error(f"{member} is not a directory")
        files += sorted(str(f) for f in member.rglob("*.nc"))
        if args.remove_superseded:
            gone += superseded(member)
    gone_set = {str(old) for old, _ in gone}
    files = [f for f in files if f not in gone_set]

    if not args.apply:
        with ProcessPoolExecutor(max(1, args.jobs)) as pool:
            todo = [(f, r) for f, r in zip(files, pool.map(needs_repair, files, chunksize=64)) if r[0] or r[1]]
        print(f"# {len(todo)} of {len(files)} file(s) to repair, {len(gone)} superseded file(s) to remove")
        for f, (units, drop) in todo:
            print(
                f"would repair {f} ({'time units' if units else ''}{'; ' if units and drop else ''}{', '.join(drop)})"
            )
        for old, new in gone:
            print(f"would remove {old} (covered by {new.name})")
        return 0

    failed = done = 0
    with ProcessPoolExecutor(max(1, args.jobs)) as pool:
        for line in pool.map(repair_file, files, chunksize=16):
            if not line.startswith("skip"):
                print(line, flush=True)
            failed += line.startswith("FAILED")
            done += line.startswith("done")
    for old, new in gone:
        old.unlink()
        print(f"removed {old} (covered by {new.name})", flush=True)
    print(f"# {done} repaired, {len(files) - done - failed} already right, {failed} failed, {len(gone)} removed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
