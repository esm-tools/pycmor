#!/usr/bin/env python3
"""Bring published AWI-ESM3 veg-HR files up to the FESOM constants of 8600581a, in place.

8600581a (2026-11-17) changed what some products are, without changing the
model output they come from:

- FESOM's own constants: density_0 = 1030 instead of 1025 or 1027,
  vcpw = 4.2e6 J m-3 K-1 instead of 1027*3988 or 1025*3996, snow density
  rhosno = 290 instead of 330. These products are a constant times what was
  published, or, for pso, a constant plus a constant times it.
- sltbasin keeps tripyview's orientation, so its sign flips (it divides by
  tripyview's own cp = 3850 before and after).
- daily sisnhc had no snow density at all.
- mrsol, rhSoil and treeFrac got a comment that says what the model does.
- cfc11 is not delivered, cfc12 only for historical from 1950, dsw not at all.

This script applies exactly those changes to files already written, so the
years do not have to be cmorized again. Products whose change is not a
constant (sifb, msftm at tavg-rho-hyb-sea, rootd) need the rules run again;
use a campaign with ``rules:`` for those (see campaign.py).

Each repaired file is written next to the original, quantized as the original
was (BitGroom, ``quantization_nsd`` significant digits), repacked with
cmip7repack, checked against the original times the factor, and then renamed
over it. It gets a new tracking_id and creation_date and a history line naming
this script. A file whose history already names it is left alone, so the
script can be run again after an interruption. So is a file written after
``--written-before``: give the time the campaign was repinned to 8600581a or
later, because files written since then are right already, and fx files
(masscello) have no year in their name that would say so.

Usage::

    repair_fesom_constants.py <member dir> --written-before 2026-11-18T12:00 \
        [--years 1850-1950] [--jobs 8] [--apply]

<member dir> is the ``.../<source_id>/<experiment_id>/<variant_label>``
directory of a DRS tree, for example
``/work/ik1017/ESGF_Buff/CMIP7/CMIP/AWI/AWI-ESM3-4-2-veg-HR/1pctCO2/r1i1p1f1``.
Without ``--apply`` it only lists what it would do.
"""

import argparse
import datetime
import os
import re
import shutil
import subprocess
import sys
import uuid
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import netCDF4 as nc
import numpy as np

TAG = "repair_fesom_constants"
CODE = "8600581a"

RHO0, VCPW = 1030.0, 4.2e6
RHO_1025, RHO_1027 = 1030.0 / 1025.0, 1030.0 / 1027.0
SNOW = 290.0 / 330.0

# The 1025 -> 1030 processing notes all spell the density one of these ways.
RHO_NOTE = (("rho_0(1025.0)", "rho_0(1030.0)"), ("rho_0=1025.0", "rho_0=1030.0"))
SNOW_NOTE = (("rho_snow=330.0", "rho_snow=290.0"),)

COMMENTS = {
    "mrsol": "Negative values, which arise in post-processing and are not in the LPJ-GUESS output, are set to zero.",
    "rhSoil": (
        "Heterotrophic respiration from the soil carbon pool as an efflux; negative values, which arise in "
        "post-processing and are not in the LPJ-GUESS output, are set to zero."
    ),
    "treeFrac": (
        "From the LPJ-GUESS annual stand-area output; every month of a year carries that year's value. "
        "LPJ-GUESS has no monthly tree area: its monthly tree fraction is weighted by leaf area and so carries "
        "a seasonal cycle that the tree-covered area does not have."
    ),
}


def _r(variable, branding, frequency, region, *, factor=1.0, offset=0.0, notes=(), comment=None, why):
    return dict(
        variable=variable,
        branding=branding,
        frequency=frequency,
        region=region,
        factor=factor,
        offset=offset,
        notes=notes,
        comment=comment,
        why=why,
    )


# (variable, branding, frequency, region): new = offset + factor * (old - offset).
# Every entry was checked against 8600581a on 1pctCO2 1900 (cli134 -> cli135).
REPAIRS = [
    *[
        _r(v, b, f, "glb", factor=RHO_1025, notes=RHO_NOTE, why="density_0 1025 -> 1030")
        for v, b, f in [
            ("masscello", "tavg-ol-hxy-sea", "mon"),
            ("masscello", "ti-ol-hxy-sea", "fx"),
            ("masso", "tavg-u-hm-sea", "mon"),
            ("msftbarot", "tavg-u-hxy-sea", "mon"),
            ("umo", "tavg-ol-hxy-sea", "mon"),
            ("vmo", "tavg-ol-hxy-sea", "mon"),
            ("wmo", "tavg-ol-hxy-sea", "mon"),
            ("sfx", "tavg-ol-hxy-sea", "mon"),
            ("sfx", "tavg-u-hxy-sea", "mon"),
            ("sfy", "tavg-ol-hxy-sea", "mon"),
            ("sfy", "tavg-u-hxy-sea", "mon"),
        ]
    ],
    # steric height is -1/rho_0 times the integrated density anomaly
    _r("zostoga", "tavg-u-hm-sea", "mon", "glb", factor=1.0 / RHO_1025, notes=RHO_NOTE, why="density_0 1025 -> 1030"),
    _r(
        "pso",
        "tavg-u-hxy-sea",
        "mon",
        "glb",
        factor=RHO_1025,
        offset=101325.0,
        why="density_0 1025 -> 1030 in p_atm + rho_0*g*ssh",
    ),
    *[
        _r(v, b, f, "glb", factor=RHO_1027, why="density_0 1027 -> 1030")
        for v, b, f in [
            ("scint", "tavg-op4-hxy-sea", "mon"),
            ("absscint", "tavg-op4-hxy-sea", "mon"),
            ("somint", "tavg-u-hxy-sea", "yr"),
            ("opottempmint", "tavg-u-hxy-sea", "yr"),
        ]
    ],
    *[
        _r(v, b, "mon", "glb", factor=VCPW / 4095900.0, why="rho_0*cp 4095900 -> vcpw 4.2e6")
        for v in ("hfx", "hfy")
        for b in ("tavg-ol-hxy-sea", "tavg-u-hxy-sea")
    ],
    _r("phcint", "tavg-op4-hxy-sea", "mon", "glb", factor=VCPW / 4097273.0, why="rho_0*cp 4097273 -> vcpw 4.2e6"),
    _r(
        "hfbasin",
        "tavg-u-hyb-sea",
        "mon",
        "glb",
        factor=VCPW / (1030.0 * 3850.0),
        why="tripyview rho0*cp 1030*3850 -> vcpw 4.2e6",
    ),
    _r(
        "sltbasin",
        "tavg-u-hyb-sea",
        "mon",
        "glb",
        factor=-1.0,
        why="tripyview orientation kept",
    ),
    _r("snm", "tavg-u-hxy-si", "mon", "glb", factor=SNOW, why="rhosno 330 -> 290"),
    _r("sisnhc", "tavg-u-hxy-si", "mon", "glb", factor=SNOW, notes=SNOW_NOTE, why="rhosno 330 -> 290"),
    *[
        _r("sisnmass", "tavg-u-hm-si", f, r, factor=SNOW, why="rhosno 330 -> 290")
        for f in ("day", "mon")
        for r in ("nh", "sh")
    ],
    _r(
        "sisnhc",
        "tavg-u-hxy-si",
        "day",
        "glb",
        factor=290.0,
        notes=(("sisnhc = -L_f*m_snow/a_ice, rho_snow=330.0", "sisnhc = -rho_snow*L_f*m_snow/a_ice, rho_snow=290.0"),),
        why="daily sisnhc lacked the snow density",
    ),
    *[
        _r(v, b, "mon", "glb", comment=COMMENTS[v], why="comment")
        for v, b in [
            ("mrsol", "tavg-sl-hxy-lnd"),
            ("rhSoil", "tavg-u-hxy-lnd"),
            ("treeFrac", "tavg-u-hxy-u"),
        ]
    ],
]

# (variable, branding, frequency, region, {experiment it stays in: from which year})
WITHDRAWN = [
    ("cfc11", "tavg-u-hm-air", "mon", "glb", {}),
    ("cfc12", "tavg-u-hm-air", "mon", "glb", {"historical": 1950}),
    ("dsw", "tavg-u-hxy-lnd", "day", "glb", {}),
]

# Products that need their rule run again; the script only reports them.
RERUN = ["sifb (rule sifb, g130 and g131)", "msftm tavg-rho-hyb-sea (rule msftm_density)", "rootd fx (rule rootd)"]

PERIOD = re.compile(r"_(\d{4})(\d{2})?(\d{2})?(\d{4})?-(\d{4})(\d{2})?(\d{2})?(\d{4})?\.nc$")


def years_of(path):
    """(first, last) year of a file from its name; None for fx files."""
    m = PERIOD.search(path.name)
    return None if m is None else (int(m[1]), int(m[5]))


def _in_years(path, years, with_fx=True):
    """Whether ``path`` overlaps ``years``. A file with no years in its name
    (fx) belongs to every year, so with a year range it is only taken when
    ``with_fx`` says so: parallel tasks over different years must not all
    rewrite the same fx file."""
    span = years_of(path)
    if years is None:
        return True
    if span is None:
        return with_fx
    return span[0] <= years[1] and span[1] >= years[0]


def find(member, repair):
    """Files of one product below <member>/<region>/<frequency>/<variable>/<branding>/<grid>/<version>/."""
    base = member / repair["region"] / repair["frequency"] / repair["variable"] / repair["branding"]
    return sorted(base.glob("*/v*/*.nc"))


def _older(path, cutoff):
    """Whether ``path`` was written before ``cutoff`` (a POSIX time).

    Publishing renames or copies with copy2, so a file keeps the time it was
    written. A file written by 8600581a or later is already right, and fx
    files carry no year that would tell them apart."""
    return cutoff is None or path.stat().st_mtime < cutoff


def _quantized(var):
    nsd = var.getncattr("quantization_nsd") if "quantization_nsd" in var.ncattrs() else None
    return int(nsd) if nsd is not None else None


def _history_has_tag(path):
    with nc.Dataset(path) as ds:
        return TAG in str(getattr(ds, "history", ""))


def _new_note(text, notes):
    for old, new in notes:
        text = text.replace(old, new)
    return text


def _copy(src, dst, repair, stamp):
    """Write src to dst with the main variable repaired; returns its name."""
    variable = repair["variable"]
    with nc.Dataset(src) as S, nc.Dataset(dst, "w", format=S.data_model) as D:
        S.set_auto_maskandscale(False)
        attrs = {k: S.getncattr(k) for k in S.ncattrs()}
        attrs["tracking_id"] = f"hdl:21.14107/{uuid.uuid4()}"
        attrs["creation_date"] = stamp
        line = f"{stamp}: {TAG}.py (pycmor {CODE}): {repair['why']}"
        attrs["history"] = line + ("\n" + attrs["history"] if attrs.get("history") else "")
        D.setncatts(attrs)
        for name, dim in S.dimensions.items():
            D.createDimension(name, None if dim.isunlimited() else len(dim))
        for name, v in S.variables.items():
            filt = v.filters() or {}
            kw = {"endian": v.endian()}
            chunking = v.chunking()
            if chunking == "contiguous":
                kw["contiguous"] = True
            elif chunking is not None:
                kw["chunksizes"] = chunking
            if filt.get("zlib"):
                kw.update(zlib=True, complevel=filt.get("complevel") or 4)
            kw["shuffle"] = bool(filt.get("shuffle"))
            kw["fletcher32"] = bool(filt.get("fletcher32"))
            target = name == variable and (repair["factor"] != 1.0 or repair["offset"] != 0.0)
            nsd = _quantized(v) if target else None
            if nsd:
                kw.update(significant_digits=nsd, quantize_mode="BitGroom")
            fill = v.getncattr("_FillValue") if "_FillValue" in v.ncattrs() else False
            out = D.createVariable(name, v.datatype, v.dimensions, fill_value=fill, **kw)
            out.set_auto_maskandscale(False)
            vattrs = {k: v.getncattr(k) for k in v.ncattrs() if not k.startswith("_")}
            if name == variable:
                if "processing_note" in vattrs and repair["notes"]:
                    vattrs["processing_note"] = _new_note(vattrs["processing_note"], repair["notes"])
                if repair["comment"] is not None:
                    vattrs["comment"] = repair["comment"]
            out.setncatts(vattrs)
            if not v.dimensions:
                out[...] = v[...]
                continue
            step = max(1, int(5e7 // max(1, int(np.prod(v.shape[1:])))))
            for i in range(0, v.shape[0], step):
                block = v[i : i + step]
                if target:
                    keep = np.zeros(block.shape, bool) if fill is False else (block == fill)
                    keep |= ~np.isfinite(block)
                    new = repair["offset"] + repair["factor"] * (block.astype(np.float64) - repair["offset"])
                    block = np.where(keep, block, new.astype(v.datatype))
                # an unlimited dimension grows to whatever slice is written, so name its true length
                out[i : i + block.shape[0]] = block
    return variable


def _strip_underscore_attrs(path):
    """netCDF-C leaves _QuantizeBitGroomNumberOfSignificantDigits on quantized
    variables; CF forbids leading underscores, and pycmor strips them too."""
    with nc.Dataset(path, "r+") as ds:
        for v in ds.variables.values():
            for k in list(v.ncattrs()):
                if k.startswith("_") and k != "_FillValue":
                    v.delncattr(k)


def _repack(path, binary):
    if binary is None:
        return
    proc = subprocess.run([binary, "-o", str(path)], capture_output=True, text=True)
    orphan = path.with_name(path.name + "_cmip7repack")
    if orphan.exists():
        orphan.unlink()
    if proc.returncode != 0:
        raise RuntimeError(f"cmip7repack rc={proc.returncode} on {path.name}: {(proc.stderr or proc.stdout).strip()}")


def _check(src, dst, repair, nsd_default=5):
    """The repaired file holds offset + factor*(old - offset) to the precision
    its quantization allows, the same missing values, and everything else as
    before."""
    variable = repair["variable"]
    with nc.Dataset(src) as S, nc.Dataset(dst) as D:
        S.set_auto_maskandscale(False)
        D.set_auto_maskandscale(False)
        if set(S.variables) != set(D.variables) or {k: len(d) for k, d in S.dimensions.items()} != {
            k: len(d) for k, d in D.dimensions.items()
        }:
            raise RuntimeError("variables or dimensions changed")
        for name, v in S.variables.items():
            w = D[name]
            if v.shape != w.shape or v.dtype != w.dtype:
                raise RuntimeError(f"{name}: shape or type changed")
            target = name == variable and (repair["factor"] != 1.0 or repair["offset"] != 0.0)
            if not v.dimensions:
                continue
            fill = v.getncattr("_FillValue") if "_FillValue" in v.ncattrs() else None
            nsd = _quantized(v) or nsd_default
            # BitGroom keeps nsd digits; the old file was groomed once and the new one once more
            tol = 2.0 * 10.0 ** (1 - nsd)
            step = max(1, int(5e7 // max(1, int(np.prod(v.shape[1:])))))
            for i in range(0, v.shape[0], step):
                a, b = v[i : i + step], w[i : i + step]
                if not target:
                    if not np.array_equal(a, b, equal_nan=a.dtype.kind == "f"):
                        raise RuntimeError(f"{name}: values changed")
                    continue
                miss_a = ~np.isfinite(a) | (a == fill if fill is not None else False)
                miss_b = ~np.isfinite(b) | (b == fill if fill is not None else False)
                if not np.array_equal(miss_a, miss_b):
                    raise RuntimeError(f"{name}: missing values moved")
                want = repair["offset"] + repair["factor"] * (a[~miss_a].astype(np.float64) - repair["offset"])
                got = b[~miss_b].astype(np.float64)
                if want.size and np.any(np.abs(got - want) > tol * np.abs(want) + 1e-30):
                    worst = float(np.max(np.abs(got - want) / np.maximum(np.abs(want), 1e-30)))
                    raise RuntimeError(f"{name}: relative error {worst:.2g} above {tol:.2g}")


def repair_file(args):
    path, repair, binary = args
    path = Path(path)
    if _history_has_tag(path):
        return f"skip   {path.name} (already repaired)"
    tmp = path.with_name(path.name + f".{TAG}.{os.getpid()}.tmp")
    tmp.unlink(missing_ok=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        _copy(path, tmp, repair, stamp)
        _strip_underscore_attrs(tmp)
        _repack(tmp, binary)
        _check(path, tmp, repair)
        shutil.copymode(path, tmp)
        os.replace(tmp, path)
    except Exception as exc:
        tmp.unlink(missing_ok=True)
        return f"FAILED {path.name}: {type(exc).__name__}: {exc}"
    return f"done   {path.name} ({repair['why']})"


def withdrawn_files(member, experiment, years):
    out = []
    for variable, branding, frequency, region, keep in WITHDRAWN:
        base = member / region / frequency / variable / branding
        for p in sorted(base.glob("*/v*/*.nc")):
            span = years_of(p)
            if not _in_years(p, years):
                continue
            if experiment in keep and span is not None and span[0] >= keep[experiment]:
                continue
            out.append(p)
    return out


def _remove_empty_dirs(path, stop):
    for d in path.parents:
        if d == stop or not d.is_relative_to(stop):
            break
        try:
            d.rmdir()
        except OSError:
            break


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("member", type=Path, help=".../<source_id>/<experiment_id>/<variant_label> of a DRS tree")
    p.add_argument("--years", help="first-last, e.g. 1850-1950 (default: every file)")
    p.add_argument("--with-fx", action="store_true", help="with --years, also take fx files (no year in the name)")
    p.add_argument("--jobs", type=int, default=4)
    p.add_argument(
        "--written-before",
        help="YYYY-MM-DD[THH:MM]: only touch files written before then, i.e. before the campaign was repinned "
        "to 8600581a or later (required with --apply)",
    )
    p.add_argument("--apply", action="store_true", help="change files; without it, only list what would change")
    p.add_argument("--cmip7repack", default=shutil.which("cmip7repack"), help="binary (default: from PATH)")
    p.add_argument("--no-repack", action="store_true", help="skip cmip7repack (tests only)")
    args = p.parse_args(argv)

    member = args.member.resolve()
    if not member.is_dir():
        p.error(f"{member} is not a directory")
    experiment = member.parent.name
    years = tuple(int(y) for y in args.years.split("-")) if args.years else None
    binary = None if args.no_repack else args.cmip7repack
    if args.apply and not args.no_repack and not binary:
        p.error("cmip7repack not found; activate the production env or pass --cmip7repack")
    if args.apply and not args.written_before:
        p.error("--apply needs --written-before (when the campaign was repinned)")
    cutoff = datetime.datetime.fromisoformat(args.written_before).timestamp() if args.written_before else None

    work, newer = [], []
    for repair in REPAIRS:
        for f in find(member, repair):
            if not _in_years(f, years, args.with_fx):
                continue
            if _older(f, cutoff):
                work.append((str(f), repair, binary))
            else:
                newer.append(f)
    gone = withdrawn_files(member, experiment, years)

    print(f"# {member}: {len(work)} file(s) to repair, {len(gone)} withdrawn file(s) to remove")
    if newer:
        print(f"# {len(newer)} file(s) written after {args.written_before} are left alone, e.g. {newer[0].name}")
    print("# these need their rules run again, not this script: " + "; ".join(RERUN))
    if not args.apply:
        for f, repair, _ in work:
            print(f"would repair {Path(f).name} ({repair['why']})")
        for f in gone:
            print(f"would remove {f}")
        return 0

    failed = 0
    with ProcessPoolExecutor(max(1, args.jobs)) as pool:
        for line in pool.map(repair_file, work):
            print(line, flush=True)
            failed += line.startswith("FAILED")
    for f in gone:
        f.unlink()
        _remove_empty_dirs(f, member)
        print(f"removed {f}", flush=True)
    print(f"# {len(work) - failed} repaired or already repaired, {failed} failed, {len(gone)} removed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
