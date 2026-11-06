#!/usr/bin/env python3
"""
build_html_report.py
====================

Convert a pycmor sanity-check JSONL plus the literature bounds table into a
static HTML site, split by realm (atm / oce / ice / veg) plus an index page.

Inputs
------
* ``--jsonl``    Path to JSONL produced by ``sanity_check.py``
                 (default ``/tmp/sanity_check_results.jsonl``).
* ``--table``    Markdown table with literature bounds and rationale
                 (default ``doc/sanity_check_ranges.md``).
* ``--out-dir``  Directory to write the HTML report into.
* ``--label``    Label used in titles. Inferred from JSONL paths if omitted.

Outputs
-------
``<out-dir>/{index,atm,oce,ice,veg}.html`` plus a tiny ``assets/`` folder.

Self-contained: no external CDNs, no matplotlib, no images.

Run
---
    python build_html_report.py --jsonl /tmp/sanity_check_results.jsonl \
        --table /work/.../doc/sanity_check_ranges.md \
        --out-dir tools/sanity_check/reports/myrun_html
"""

from __future__ import annotations

import argparse
import html
import json
import math
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


# ---------------------------------------------------------------------------
# Severity classifier (mirrors build_issues_md.py / sanity_summary.py)
# ---------------------------------------------------------------------------

# Variables that physically cannot be negative (mass per area, cell
# thickness). The classifier marks them PHYS_IMPOSSIBLE only when the
# OBSERVED min is actually negative — not based on name alone, so cli7's
# masscello bug shows up as PHYS_IMPOSSIBLE but cli9's positive values
# fall through to the regular bounds check.
SIGN_BUGS = {"masscello", "thkcello"}


def _is_phys_impossible(var: str, notes: Sequence[str]) -> bool:
    """True iff `var` is in SIGN_BUGS AND the current observed min is < 0."""
    if var not in SIGN_BUGS:
        return False
    text = "; ".join(notes).lower()
    m = re.search(r"min ([\-\d.eE+]+) below expected_min", text)
    if not m:
        return False
    try:
        return float(m.group(1)) < 0
    except Exception:
        return False

SEVERITY_ORDER = [
    "DATA_INTEGRITY",
    "PHYS_IMPOSSIBLE",
    "UNIT_MISMATCH",
    "SIGN_FLIP",
    "EXTREME_OUTLIER",
    "PICONTROL_NONZERO",
    "PHYS_NEG_VALUES",
    "BOUNDS_OR_PEAK",
    "BOUNDS_TIGHT_MINOR",
]
SEVERITY_RANK = {s: i for i, s in enumerate(SEVERITY_ORDER)}

# Status worst-of ordering: FAIL > ERROR > WARN > NOBOUNDS > PASS
STATUS_RANK = {"FAIL": 0, "ERROR": 1, "WARN": 2, "NOBOUNDS": 3, "PASS": 4}


# Keywords in the bounds-table rationale that mark a variable as one whose
# 0 bound is there because the ANTHROPOGENIC forcing is absent in piControl
# (not because of natural physical/climate balance). Bare "piControl" was
# removed — it matched balanced natural quantities (nbp, rtmt, fco2nat,
# opottemptend) which are near-zero for *energy/mass balance* reasons, not
# for forcing-absence reasons.
_PICONTROL_RATIONALE_HINTS = (
    "anthropogenic", "luh2", "luc",
    "harvest", "harvested", "fertilis", "fertiliz",
    "no synthetic", "no harvest", "no luc",
    "no land-use", "no land use",
    "no deforestation", "no lu transitions", "no lu products",
    "no land-use products",
)


def severity_of(var: str, notes: Sequence[str], rationale: str = "") -> str:
    if _is_phys_impossible(var, notes):
        return "PHYS_IMPOSSIBLE"
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
    # Hard zero bound violation. Two distinct causes:
    #   * piControl / anthropogenic forcing: variable should be ~0 because the
    #     model isn't run with that forcing on. The bounds-table rationale will
    #     mention piControl, LUH2, anthropogenic, harvest, fertiliser, etc.
    #   * physical lower bound: variable cannot be negative on physical
    #     grounds (precipitation, evaporation, snow melt, etc.). Negative
    #     values are likely numerical noise or a sign bug, NOT forcing leakage.
    # Match a hard zero bound only — "above expected_max 0.0003" does NOT
    # trigger; "above expected_max 0;" or "above expected_max 0$" does.
    has_zero_bound = (re.search(r"above expected_max 0(?:\s|;|,|$)", text)
                      or re.search(r"below expected_min 0(?:\s|;|,|$)", text))
    if has_zero_bound:
        rat = rationale.lower()
        if any(kw in rat for kw in _PICONTROL_RATIONALE_HINTS):
            return "PICONTROL_NONZERO"
        return "PHYS_NEG_VALUES"
    # If the overshoot is enormous (>=20x the bound magnitude), it's not an
    # HR-vs-LR bound issue any more — likely a numerical spike, sentinel leak,
    # or unit error. Parse the largest overshoot factor visible in the notes.
    biggest = 0.0
    for m in re.finditer(r"(?:max|min) ([\-\d.eE+]+) (?:above|below) expected_(?:max|min) ([\-\d.eE+]+)", text):
        try:
            actual = float(m.group(1))
            bound  = float(m.group(2))
            if bound == 0:
                continue
            ratio = abs(actual / bound)
            if ratio > biggest:
                biggest = ratio
        except Exception:
            continue
    if biggest >= 20:
        return "EXTREME_OUTLIER"
    if "slightly" in text:
        return "BOUNDS_TIGHT_MINOR"
    return "BOUNDS_OR_PEAK"


# ---------------------------------------------------------------------------
# Realm bucket assignment
# ---------------------------------------------------------------------------

ATM_REALMS = {"atmos", "atmoschem", "aerosol"}
OCE_REALMS = {"ocean"}
ICE_REALMS = {"seaice"}
# CMIP `landIce` formally includes ice sheets, but in this run it's the
# realm of common snow-on-land variables (snw, snd, snm, tsn, sbl, ...) and
# permafrost (mrfso). All of those are land-model outputs, not ice-sheet
# model outputs, so we put them on the land page where they make sense.
VEG_REALMS = {"land", "landice"}


def domain_of(realm: Optional[str], directory: Optional[str]) -> Optional[str]:
    """Return one of {'atm','oce','ice','veg'} or None."""
    r = (realm or "").strip().lower()
    d = (directory or "").strip().lower()

    if r in ATM_REALMS:
        return "atm"
    if r in OCE_REALMS:
        return "oce"
    if r in ICE_REALMS:
        return "ice"
    if r in VEG_REALMS:
        return "veg"

    # Fallbacks based on directory
    if not r:
        if "cap7_aerosol" in d or d.endswith("_atm") or "_atm" in d:
            return "atm"
        if d.endswith("_ocean") or "_ocean" in d:
            return "oce"
        if d.endswith("_seaice") or "_seaice" in d:
            return "ice"
        if d.endswith("_land") or "_land" in d:
            return "veg"
    return None


DOMAIN_LABELS = {
    "atm": "Atmosphere",
    "oce": "Ocean",
    "ice": "Sea Ice",
    "veg": "Land, Vegetation & Snow",
}


# ---------------------------------------------------------------------------
# JSONL + markdown table parsing
# ---------------------------------------------------------------------------

def _nan_parse_constant(token: str) -> float:
    return float("nan")


def parse_jsonl(path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line, parse_constant=_nan_parse_constant)
            except json.JSONDecodeError as exc:
                print(f"warn: skipping malformed JSONL line {lineno}: {exc}",
                      file=sys.stderr)
                continue
            records.append(rec)
    return records


def parse_metadata_json(path: Path) -> Dict[str, Dict[str, str]]:
    """Return {out_name: {'long_name','comment','standard_name'}} from CMIP7 metadata JSON.

    The JSON has a top-level "Compound Name" mapping where each value is a
    record describing one (variable, frequency, branding) tuple. Multiple
    records exist per variable; they share long_name/comment, so the first
    one wins.
    """
    metadata_by_var: Dict[str, Dict[str, str]] = {}
    if not path.exists():
        return metadata_by_var
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"warn: could not read metadata JSON {path}: {exc}",
              file=sys.stderr)
        return metadata_by_var

    compound = raw.get("Compound Name") or {}
    if not isinstance(compound, dict):
        return metadata_by_var

    # Several records may share the same out_name (one per branding/freq).
    # Score them so the most generic global record wins:
    #   * branding ending in "-u" (universal/all-tiles)         > tile-specific
    #   * tavg-* (time-mean)                                    > tmax/tmin/tpt
    #   * region GLB                                            > 30S-90S etc
    def _score(compound_key: str) -> int:
        parts = str(compound_key).split(".")
        branding = parts[2] if len(parts) >= 3 else ""
        region = parts[4] if len(parts) >= 5 else ""
        s = 0
        if branding.endswith("-u"):
            s += 1000
        if branding.startswith("tavg-"):
            s += 500
        if region.upper() == "GLB":
            s += 200
        return s

    best_for: Dict[str, Tuple[int, str, Dict[str, Any]]] = {}
    for compound_key, rec in compound.items():
        if not isinstance(rec, dict):
            continue
        var = rec.get("out_name")
        if not var:
            parts = str(compound_key).split(".")
            if len(parts) >= 2:
                var = parts[1]
        if not var:
            continue
        score = _score(compound_key)
        prev = best_for.get(var)
        if prev is None or score > prev[0]:
            best_for[var] = (score, compound_key, rec)

    for var, (_score_v, _key, rec) in best_for.items():
        metadata_by_var[var] = {
            "long_name": str(rec.get("long_name", "") or ""),
            "comment": str(rec.get("comment", "") or ""),
            "standard_name": str(rec.get("standard_name", "") or ""),
        }
    return metadata_by_var


def parse_bounds_table(path: Path) -> Dict[str, Dict[str, Any]]:
    """Return {var_name: {'realm','units','min','mean','max','source'}}."""
    out: Dict[str, Dict[str, Any]] = {}
    if not path.exists():
        return out

    header_seen = False
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip().startswith("|"):
                continue
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if len(cells) < 7:
                continue
            if not header_seen:
                if cells[0].lower() == "variable":
                    header_seen = True
                continue
            # Skip alignment row of dashes
            if all(set(c.replace(":", "")).issubset({"-", " "}) for c in cells):
                continue
            var = cells[0]
            if not var:
                continue
            out[var] = {
                "realm": cells[1],
                "units": cells[2],
                "expected_min": cells[3],
                "expected_mean": cells[4],
                "expected_max": cells[5],
                "source": cells[6],
            }
    return out


# ---------------------------------------------------------------------------
# Numeric coercion helpers
# ---------------------------------------------------------------------------

def to_float(value: Any) -> float:
    """Convert string like '~1.4e5', '~10', '-' to float; '-' / 'varies' -> nan."""
    if value is None:
        return float("nan")
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return float("nan")
    if s in {"-", "—", "?", "n/a", "N/A"} or "vari" in s.lower() or "pft" in s.lower():
        return float("nan")
    s = s.lstrip("~").replace(",", "")
    try:
        return float(s)
    except ValueError:
        # try first numeric token
        m = re.search(r"-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?", s)
        if m:
            try:
                return float(m.group(0))
            except ValueError:
                pass
        return float("nan")


def is_finite(x: float) -> bool:
    try:
        return math.isfinite(x)
    except TypeError:
        return False


def fmt_num(x: Any, sig: int = 4) -> str:
    """Pretty number for tables."""
    if isinstance(x, str):
        return html.escape(x) if x.strip() else "&mdash;"
    try:
        v = float(x)
    except (TypeError, ValueError):
        return "&mdash;"
    if not is_finite(v):
        return "&mdash;"
    if v == 0:
        return "0"
    av = abs(v)
    if av >= 1e5 or av < 1e-3:
        return f"{v:.{sig - 1}e}"
    if av >= 100:
        return f"{v:.1f}"
    if av >= 1:
        return f"{v:.3f}"
    return f"{v:.{sig}g}"


# ---------------------------------------------------------------------------
# Variable aggregation
# ---------------------------------------------------------------------------

@dataclass
class VarEntry:
    var: str
    files: List[Dict[str, Any]] = field(default_factory=list)
    realm: str = ""
    units_table: str = ""
    units_file: str = ""
    expected_min: Any = None
    expected_mean: Any = None
    expected_max: Any = None
    source: str = ""
    domain: str = ""
    worst_status: str = "PASS"
    worst_severity: Optional[str] = None
    worst_notes: List[str] = field(default_factory=list)
    n_total: int = 0
    obs_min: float = float("nan")
    obs_mean: float = float("nan")
    obs_max: float = float("nan")
    units_in_file: str = ""
    directory: str = ""
    # Filename whose stats drove the worst-status (so a card whose stats
    # come from a broken hxy-si variant doesn't look like the whole var is
    # broken if the global variant is fine).
    worst_file: str = ""

    @property
    def severity_rank(self) -> int:
        if self.worst_severity is None:
            return len(SEVERITY_ORDER) + 1
        return SEVERITY_RANK.get(self.worst_severity, len(SEVERITY_ORDER))


def collapse(records: Sequence[Dict[str, Any]],
             bounds_meta: Dict[str, Dict[str, Any]]) -> List[VarEntry]:
    # Key by (var, realm) — same out_name can belong to different realms
    # across branded compounds (e.g. rlds.tavg-u-hxy-u → atmos,
    # rlds.tavg-u-hxy-si → seaIce). Keying by var alone bundled every
    # branding into one card and routed it to whichever realm appeared
    # first in the JSONL.
    by_var: Dict[Tuple[str, str], VarEntry] = {}

    for rec in records:
        var = rec.get("var") or rec.get("primary") or "?"
        # Realm key: prefer the per-record value (now sourced from the
        # file's own :realm global attribute, see sanity_check.py
        # worker_main). Fall back to bounds-table realm so records that
        # never got a realm written (e.g. open-failed ERRORs) still group
        # with their siblings.
        realm = str(rec.get("realm") or bounds_meta.get(var, {}).get("realm", "") or "")
        key = (var, realm)
        ent = by_var.get(key)
        if ent is None:
            ent = VarEntry(var=var, realm=realm)
            by_var[key] = ent

        ent.files.append(rec)
        if not ent.directory and rec.get("dir"):
            ent.directory = str(rec.get("dir") or "")

        # Expected values: take from JSONL if present, else table
        for k in ("expected_min", "expected_mean", "expected_max"):
            if getattr(ent, k) in (None, "", float("nan")) and rec.get(k) not in (None, ""):
                setattr(ent, k, rec.get(k))
        if not ent.units_table and rec.get("units"):
            ent.units_table = str(rec.get("units"))
        if not ent.units_in_file and rec.get("units_in_file"):
            ent.units_in_file = str(rec.get("units_in_file") or "")

        # Track worst observation (we want representative numbers).
        # Use the file with the highest STATUS_RANK contribution.
        status = str(rec.get("status") or "PASS").upper()
        rank = STATUS_RANK.get(status, 99)
        cur_rank = STATUS_RANK.get(ent.worst_status, 99)
        if rank < cur_rank:
            ent.worst_status = status
            ent.worst_notes = list(rec.get("notes") or [])
            ent.obs_min = to_float(rec.get("min"))
            ent.obs_mean = to_float(rec.get("mean"))
            ent.obs_max = to_float(rec.get("max"))
            ent.n_total = int(rec.get("n_total") or 0)
            from pathlib import Path as _Path
            ent.worst_file = _Path(str(rec.get("file") or "")).name
        elif rank == cur_rank and not ent.worst_notes:
            ent.worst_notes = list(rec.get("notes") or [])
            if not is_finite(ent.obs_mean):
                ent.obs_min = to_float(rec.get("min"))
                ent.obs_mean = to_float(rec.get("mean"))
                ent.obs_max = to_float(rec.get("max"))
                ent.n_total = int(rec.get("n_total") or 0)

    # Now decorate each entry with bounds-table metadata, severity, domain.
    out: List[VarEntry] = []
    for (var, _realm), ent in by_var.items():
        meta = bounds_meta.get(var, {})
        if not ent.realm:
            ent.realm = str(meta.get("realm", "") or "")
        if not ent.units_table:
            ent.units_table = str(meta.get("units", "") or "")
        if ent.expected_min in (None, ""):
            ent.expected_min = meta.get("expected_min", "")
        if ent.expected_mean in (None, ""):
            ent.expected_mean = meta.get("expected_mean", "")
        if ent.expected_max in (None, ""):
            ent.expected_max = meta.get("expected_max", "")
        ent.source = str(meta.get("source", "") or "")
        ent.domain = domain_of(ent.realm, ent.directory) or ""

        if ent.worst_status == "FAIL":
            ent.worst_severity = severity_of(var, ent.worst_notes,
                                             rationale=ent.source)
        else:
            ent.worst_severity = None

        out.append(ent)
    return out


# ---------------------------------------------------------------------------
# SVG plot
# ---------------------------------------------------------------------------

def build_svg(entry: VarEntry,
              width: int = 480,
              height: int = 120) -> Optional[str]:
    """Return an SVG string showing expected band + observed range, or None."""
    em = to_float(entry.expected_min)
    ee = to_float(entry.expected_mean)
    ex = to_float(entry.expected_max)
    am = entry.obs_min
    ae = entry.obs_mean
    ax = entry.obs_max

    candidates = [v for v in (em, ee, ex, am, ae, ax) if is_finite(v)]
    if not candidates:
        return None
    # Need at least observed or expected pair to be meaningful.
    have_expected = is_finite(em) and is_finite(ex)
    have_observed = is_finite(am) and is_finite(ax)
    if not (have_expected or have_observed):
        return None

    vmin = min(candidates)
    vmax = max(candidates)
    if vmin == vmax:
        # Pad
        pad = abs(vmin) * 0.1 if vmin != 0 else 1.0
        vmin -= pad
        vmax += pad

    # Decide log scale
    positive = [abs(v) for v in candidates if v != 0]
    use_log = False
    if positive:
        big = max(abs(v) for v in candidates)
        small = min(positive)
        if small > 0 and big / small > 1000 and vmin > 0:
            use_log = True

    pad_frac = 0.05
    span = vmax - vmin
    plot_min = vmin - span * pad_frac
    plot_max = vmax + span * pad_frac
    if use_log:
        plot_min = max(plot_min, min(positive) * 0.5)
        log_min = math.log10(plot_min)
        log_max = math.log10(plot_max)

    margin_l = 50
    margin_r = 20
    margin_t = 20
    margin_b = 35
    plot_w = width - margin_l - margin_r
    plot_h = height - margin_t - margin_b
    y_band_top = margin_t + 10
    y_band_bot = margin_t + plot_h - 10
    y_obs = margin_t + plot_h / 2

    def x_of(v: float) -> float:
        if use_log:
            if v <= 0:
                return margin_l  # clamp to left edge
            return margin_l + (math.log10(v) - log_min) / (log_max - log_min) * plot_w
        return margin_l + (v - plot_min) / (plot_max - plot_min) * plot_w

    parts: List[str] = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'width="{width}" height="{height}" role="img" '
        f'aria-label="Range plot for {html.escape(entry.var)}">'
    )

    # Background
    parts.append(
        f'<rect x="{margin_l}" y="{margin_t}" width="{plot_w}" height="{plot_h}" '
        'fill="#fafafa" stroke="#ddd"/>'
    )

    # Expected band
    if have_expected:
        x_em = x_of(em)
        x_ex = x_of(ex)
        if x_em > x_ex:
            x_em, x_ex = x_ex, x_em
        parts.append(
            f'<rect x="{x_em:.1f}" y="{y_band_top:.1f}" '
            f'width="{max(x_ex - x_em, 1):.1f}" height="{y_band_bot - y_band_top:.1f}" '
            'fill="#e7e7e7" stroke="#bbb"/>'
        )
        if is_finite(ee):
            x_ee = x_of(ee)
            parts.append(
                f'<line x1="{x_ee:.1f}" y1="{y_band_top:.1f}" x2="{x_ee:.1f}" '
                f'y2="{y_band_bot:.1f}" stroke="#888" stroke-width="1.2" '
                'stroke-dasharray="3 2"/>'
            )

    # Observed range
    if have_observed:
        x_am = x_of(am)
        x_ax = x_of(ax)
        if x_am > x_ax:
            x_am, x_ax = x_ax, x_am
        color = "#3a3"
        if entry.worst_status in ("FAIL", "ERROR"):
            color = "#c33"
        elif entry.worst_status == "WARN":
            color = "#e80"
        parts.append(
            f'<line x1="{x_am:.1f}" y1="{y_obs:.1f}" x2="{x_ax:.1f}" y2="{y_obs:.1f}" '
            f'stroke="{color}" stroke-width="3" stroke-linecap="round"/>'
        )
        # End caps
        for xp in (x_am, x_ax):
            parts.append(
                f'<line x1="{xp:.1f}" y1="{y_obs - 8:.1f}" x2="{xp:.1f}" '
                f'y2="{y_obs + 8:.1f}" stroke="{color}" stroke-width="2"/>'
            )
        if is_finite(ae):
            x_ae = x_of(ae)
            parts.append(
                f'<line x1="{x_ae:.1f}" y1="{y_obs - 12:.1f}" x2="{x_ae:.1f}" '
                f'y2="{y_obs + 12:.1f}" stroke="{color}" stroke-width="3"/>'
            )

    # Axis
    axis_y = margin_t + plot_h
    parts.append(
        f'<line x1="{margin_l}" y1="{axis_y}" x2="{margin_l + plot_w}" '
        f'y2="{axis_y}" stroke="#444"/>'
    )
    if use_log:
        ticks_log = [log_min, (log_min + log_max) / 2, log_max]
        ticks = [10 ** v for v in ticks_log]
    else:
        ticks = [plot_min, (plot_min + plot_max) / 2, plot_max]
    for t in ticks:
        xt = x_of(t)
        parts.append(
            f'<line x1="{xt:.1f}" y1="{axis_y}" x2="{xt:.1f}" y2="{axis_y + 4}" '
            'stroke="#444"/>'
        )
        parts.append(
            f'<text x="{xt:.1f}" y="{axis_y + 16}" font-size="10" fill="#333" '
            f'text-anchor="middle">{html.escape(fmt_num(t))}</text>'
        )

    # Y label hints
    parts.append(
        f'<text x="6" y="{y_band_top + 4:.1f}" font-size="9" fill="#666">expected</text>'
    )
    parts.append(
        f'<text x="6" y="{y_obs + 3:.1f}" font-size="9" fill="#666">observed</text>'
    )

    # Units (right-bottom)
    units = entry.units_table or entry.units_in_file
    if units:
        parts.append(
            f'<text x="{width - margin_r}" y="{height - 6}" font-size="10" '
            f'fill="#444" text-anchor="end">{html.escape(units)}{" (log)" if use_log else ""}</text>'
        )
    elif use_log:
        parts.append(
            f'<text x="{width - margin_r}" y="{height - 6}" font-size="10" '
            'fill="#444" text-anchor="end">log scale</text>'
        )

    parts.append("</svg>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# Diagnosis text
# ---------------------------------------------------------------------------

def diagnosis_text(entry: VarEntry) -> str:
    var = entry.var
    units = entry.units_table or entry.units_in_file or ""
    file_units = entry.units_in_file or ""
    n_total = entry.n_total

    am = entry.obs_min
    ae = entry.obs_mean
    ax = entry.obs_max
    em = to_float(entry.expected_min)
    ee = to_float(entry.expected_mean)
    ex = to_float(entry.expected_max)
    notes_text = "; ".join(entry.worst_notes)

    sev = entry.worst_severity

    if entry.worst_status == "PASS":
        return "Within bounds."
    if entry.worst_status == "ERROR":
        msg = notes_text or "(no detail)"
        return f"Read failed: {msg}."
    if entry.worst_status == "NOBOUNDS":
        return "No literature bound available; observed values logged but not validated."
    if entry.worst_status == "WARN":
        msg = notes_text or ""
        return f"Within tolerance of the bound. {msg}".strip()

    # FAIL branches — prepare a generic suffix to flag "bulk of field
    # within bounds, single-cell extremes triggered the fail" cases. The
    # walker doesn't store the violation fraction, but if the mean sits
    # inside the expected [min, max] window then by construction the
    # offending values must be confined to outlier cells.
    mean_in_bounds = (
        is_finite(ae) and is_finite(em) and is_finite(ex) and em <= ae <= ex
    )
    extreme_suffix = ""
    if mean_in_bounds:
        extreme_suffix = (
            f" The field mean ({fmt_num(ae)}) sits inside the expected "
            f"window [{fmt_num(em)}, {fmt_num(ex)}], so the bound "
            "violation is confined to outlier cells; the bulk of the "
            "field looks healthy (compare map)."
        )

    if sev == "DATA_INTEGRITY":
        return (
            f"All {n_total} cells are non-finite (NaN/fill-value). "
            "The producing rule emitted a file with no real data — likely "
            "the source field is missing/empty or a divide-by-zero in the "
            f"compute step. Investigate the rule's pipeline in "
            f"`awi-esm3-veg-hr-variables/{entry.directory or '?'}/`."
        )
    if sev == "PHYS_IMPOSSIBLE":
        return (
            f"Output contains physically impossible values "
            f"(min={fmt_num(am)} {html.escape(units)}). For `masscello` "
            "(mass per area) and `thkcello` (cell thickness) any negative "
            "value indicates an upstream sign or differencing bug."
        )
    if sev == "UNIT_MISMATCH":
        factor = "?"
        if is_finite(ae) and is_finite(ee) and ee != 0:
            try:
                factor = fmt_num(ae / ee)
            except Exception:
                factor = "?"
        return (
            f"Observed mean {fmt_num(ae)} is {factor}x the expected mean "
            f"{fmt_num(ee)}. Likely a missing unit conversion: the file "
            f"declares `{html.escape(file_units or '?')}` but the CMIP table "
            f"expects `{html.escape(units or '?')}`. Add `source_units:` "
            "in the rule yaml."
        )
    if sev == "SIGN_FLIP":
        return (
            f"Mean {fmt_num(ae)} has the wrong sign vs the expected "
            f"{fmt_num(ee)}. The rule may be saving an anomaly or has "
            "the wrong source variable."
        )
    if sev == "PICONTROL_NONZERO":
        return (
            "These should be ~0 in piControl (no anthropogenic forcing) "
            f"but the model emits min={fmt_num(am)}, mean={fmt_num(ae)}, "
            f"max={fmt_num(ax)}. Either the LUC forcing dataset isn't "
            "being honoured, or this is documented internal model "
            "behaviour — investigate, don't fix in pycmor."
        )
    if sev == "PHYS_NEG_VALUES":
        # Hard zero bound on a physical quantity that cannot be negative
        # (precipitation, evaporation, snow melt, etc.) — but the file has
        # negative values somewhere. Mean and max are usually fine.
        return (
            f"Negative values found (min={fmt_num(am)}) despite a physical "
            f"lower bound of 0 — {entry.var} cannot physically be negative. "
            f"Mean={fmt_num(ae)} and max={fmt_num(ax)} are within range; "
            "the violation is at the lower end and likely numerical noise "
            "(e.g. flux scheme overshoot, regridding artefact) rather than "
            "a forcing issue. Check whether to clip to 0 in the rule, or "
            "whether the source field has a known sign-error."
        ) + extreme_suffix
    if sev == "EXTREME_OUTLIER":
        return (
            f"Extreme outlier: observed range "
            f"(min={fmt_num(am)}, max={fmt_num(ax)}) overshoots the literature "
            "bound by >20x. This is far beyond any HR-vs-LR resolution effect; "
            "likely a numerical instability, sentinel-value leak, double "
            "unit conversion, or accumulated drift. The bound is probably "
            "correct — investigate the rule's compute step rather than "
            "loosen it."
        ) + extreme_suffix
    if sev == "BOUNDS_OR_PEAK":
        return (
            f"Grid-cell extremes (min={fmt_num(am)} / max={fmt_num(ax)}) "
            f"overshoot the bound, but the global mean ({fmt_num(ae)}) is "
            "reasonable. The bound was set for global mean at LR resolution; "
            "HR cells legitimately have higher peaks. Loosen the bound "
            "rather than touch the model."
        )
    if sev == "BOUNDS_TIGHT_MINOR":
        return (
            "Marginal overshoot of the literature bound. "
            "The bound likely needs widening."
        ) + extreme_suffix
    return (notes_text or "Failed sanity check.") + extreme_suffix


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

CSS = """
:root {
  --fg: #222;
  --muted: #666;
  --bg: #fff;
  --bg2: #fafafa;
  --border: #d4d4d4;
  --pill-fail: #c33;
  --pill-warn: #e80;
  --pill-pass: #3a3;
  --pill-nobounds: #888;
}
* { box-sizing: border-box; }
html, body {
  margin: 0;
  padding: 0;
  background: var(--bg);
  color: var(--fg);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
               Oxygen, Ubuntu, Cantarell, "Helvetica Neue", Arial, sans-serif;
  font-size: 14px;
  line-height: 1.45;
}
nav.top {
  position: sticky;
  top: 0;
  z-index: 10;
  background: #f3f3f3;
  border-bottom: 1px solid var(--border);
  padding: 8px 16px;
}
nav.top a {
  margin-right: 14px;
  color: #134;
  text-decoration: none;
  font-weight: 500;
}
nav.top a.active {
  text-decoration: underline;
}
main {
  max-width: 1500px;
  margin: 0 auto;
  padding: 18px 24px 80px;
}
h1 { font-size: 22px; margin: 14px 0 6px; }
h2 { font-size: 18px; margin: 28px 0 10px; border-bottom: 1px solid var(--border); padding-bottom: 4px; }
h3 { font-size: 15px; margin: 0; }
.subtle { color: var(--muted); font-size: 12px; }
.pill {
  display: inline-block;
  padding: 1px 8px;
  border-radius: 10px;
  color: white;
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.3px;
  vertical-align: middle;
}
.pill.fail, .pill.error { background: var(--pill-fail); }
.pill.warn { background: var(--pill-warn); }
.pill.pass { background: var(--pill-pass); }
.pill.nobounds { background: var(--pill-nobounds); }
.sev-tag {
  display: inline-block;
  font-size: 10px;
  background: #222;
  color: #fff;
  padding: 1px 6px;
  border-radius: 3px;
  margin-left: 4px;
  letter-spacing: 0.5px;
}
.var-card {
  border: 1px solid var(--border);
  background: var(--bg);
  border-radius: 6px;
  padding: 12px 14px;
  margin: 10px 0;
}
.var-card.compact {
  padding: 6px 10px;
  background: var(--bg2);
}
.var-card .header {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
}
.var-card .header .name {
  font-weight: 600;
  font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
  font-size: 14px;
}
.var-card .meta {
  margin-left: auto;
  color: var(--muted);
  font-size: 12px;
}
.var-card svg { margin: 8px 0; display: block; }
p.longname {
  margin: 6px 0 2px 0;
  font-size: 1.05em;
  color: #222;
}
p.stdname {
  margin: 0 0 4px 0;
  font-size: 0.85em;
  color: #555;
}
p.description {
  margin: 0 0 10px 0;
  font-size: 0.9em;
  color: #333;
  max-width: 800px;
  line-height: 1.4;
}
img.varmap {
  display: block;
  max-width: 1400px;
  width: 100%;
  height: auto;
  margin: 8px 0;
  border: 1px solid #ddd;
  background: #fafafa;
}
table.numbers {
  border-collapse: collapse;
  margin: 8px 0;
  font-size: 13px;
}
table.numbers th, table.numbers td {
  border: 1px solid var(--border);
  padding: 3px 8px;
  text-align: right;
  font-variant-numeric: tabular-nums;
}
table.numbers th { background: var(--bg2); text-align: center; }
table.numbers td.label { text-align: left; font-weight: 500; }
.diagnosis {
  background: #fff8e8;
  border-left: 3px solid #e80;
  padding: 6px 10px;
  margin-top: 6px;
  font-size: 13px;
}
.var-card.fail .diagnosis,
.var-card.error .diagnosis {
  background: #fdecea;
  border-left-color: #c33;
}
.source {
  color: var(--muted);
  font-size: 12px;
  margin-top: 4px;
}
details.files {
  margin-top: 6px;
  font-size: 12px;
  color: var(--muted);
}
details.files summary {
  cursor: pointer;
  color: #134;
}
details.files ul { margin: 4px 0 4px 18px; padding: 0; }
.worstfile {
  font-size: 12px;
  color: #666;
  margin: 4px 0 8px;
}
p.filename {
  font-size: 12px;
  color: #666;
  margin: 0 0 4px;
}
p.filename code {
  background: var(--bg2);
  padding: 1px 4px;
  border-radius: 3px;
}
table.files-table {
  border-collapse: collapse;
  margin-top: 10px;
  font-size: 12px;
  width: 100%;
}
table.files-table th, table.files-table td {
  border-top: 1px solid var(--border);
  padding: 4px 8px;
  vertical-align: top;
}
table.files-table th {
  background: var(--bg2);
  text-align: left;
}
table.files-table td:nth-child(3),
table.files-table td:nth-child(4),
table.files-table td:nth-child(5) {
  font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
  text-align: right;
  white-space: nowrap;
}
table.files-table .diagnosis {
  font-size: 11px;
  margin: 4px 0 0 0;
  padding: 4px 6px;
}
table.summary {
  border-collapse: collapse;
  width: 100%;
  margin: 12px 0;
}
table.summary th, table.summary td {
  border: 1px solid var(--border);
  padding: 6px 10px;
  text-align: left;
}
table.summary th { background: var(--bg2); }
.callout {
  background: #fdecea;
  border: 1px solid #c33;
  border-radius: 6px;
  padding: 10px 14px;
  margin: 16px 0;
}
.callout h2 {
  margin-top: 0;
  border: none;
  color: #c33;
}
.callout ul { margin: 4px 0; padding-left: 22px; }
"""


def _pill(status: str) -> str:
    s = status.upper()
    cls = {
        "FAIL": "fail",
        "ERROR": "error",
        "WARN": "warn",
        "PASS": "pass",
        "NOBOUNDS": "nobounds",
    }.get(s, "nobounds")
    return f'<span class="pill {cls}">{html.escape(s)}</span>'


def _nav(active: str, label: str) -> str:
    items = [
        ("index", "Summary"),
        ("atm", "Atmosphere"),
        ("oce", "Ocean"),
        ("ice", "Sea Ice"),
        ("veg", "Land/Snow/Veg"),
    ]
    out = ['<nav class="top">']
    out.append(f'<strong>{html.escape(label)}</strong> &middot; ')
    for key, lbl in items:
        cls = ' class="active"' if key == active else ""
        href = f"{key}.html"
        out.append(f'<a href="{href}"{cls}>{html.escape(lbl)}</a>')
    out.append("</nav>")
    return "".join(out)


def _page_shell(title: str, label: str, active: str, body: str) -> str:
    return (
        "<!DOCTYPE html>\n"
        '<html lang="en"><head>'
        '<meta charset="utf-8"/>'
        f'<title>{html.escape(title)}</title>'
        '<link rel="stylesheet" href="assets/style.css"/>'
        "</head><body>"
        + _nav(active, label)
        + "<main>"
        + body
        + "</main>"
        "</body></html>\n"
    )


def render_file_card(ent: VarEntry,
                     rec: Dict[str, Any],
                     out_dir: Optional[Path] = None,
                     metadata_by_var: Optional[Dict[str, Dict[str, str]]] = None) -> str:
    """Render one card for a single .nc file.

    Variable-level metadata (long_name, description, expected bounds,
    source/rationale) comes from `ent`; per-file numbers and diagnosis
    come from `rec`. Each file gets its own status pill, severity,
    diagnosis paragraph.
    """
    fname = os.path.basename(str(rec.get("file") or ""))
    status = str(rec.get("status") or "PASS").upper()
    status_cls = status.lower()
    sev = _file_card_severity(ent.var, rec, ent.source)
    sev_tag = (f'<span class="sev-tag">{html.escape(sev)}</span>'
               if sev else "")

    units_table = ent.units_table or "?"
    units_file = str(rec.get("units_in_file") or ent.units_in_file or "?")
    realm = ent.realm or "?"

    obs_min = to_float(rec.get("min"))
    obs_mean = to_float(rec.get("mean"))
    obs_max = to_float(rec.get("max"))

    # Cadence-aware: walker stores per-file bounds in the JSONL rec
    # (e.g. hfls mon row vs hfls_day row). Prefer those; fall back to
    # the variable-group entry only if absent.
    def _pref(key):
        v = rec.get(key)
        if v in (None, ""):
            return getattr(ent, key)
        return v
    rec_emin = _pref("expected_min")
    rec_emean = _pref("expected_mean")
    rec_emax = _pref("expected_max")

    # Per-file VarEntry-like snapshot for build_svg / diagnosis_text.
    file_entry = VarEntry(
        var=ent.var,
        realm=ent.realm,
        expected_min=rec_emin,
        expected_mean=rec_emean,
        expected_max=rec_emax,
        source=ent.source,
        worst_status=status,
        worst_severity=sev,
        worst_notes=list(rec.get("notes") or []),
        n_total=int(rec.get("n_total") or 0),
        obs_min=obs_min,
        obs_mean=obs_mean,
        obs_max=obs_max,
        units_in_file=units_file,
        units_table=ent.units_table,
        directory=str(rec.get("dir") or ""),
    )

    # Unique anchor per file: var + branding from the filename (without
    # the .nc and the date/ensemble suffix).
    anchor = re.sub(r"\.nc$", "", fname)
    anchor = re.sub(r"[^A-Za-z0-9_-]+", "-", anchor)

    parts: List[str] = []
    parts.append(f'<div class="var-card {status_cls}" id="file-{anchor}">')
    parts.append('<div class="header">')
    parts.append(f'<span class="name">{html.escape(ent.var)}</span>')
    parts.append(_pill(status))
    parts.append(sev_tag)
    parts.append(
        f'<span class="meta">realm={html.escape(realm)} '
        f"&middot; units(file)={html.escape(units_file)} "
        f"&middot; units(table)={html.escape(units_table)}"
        "</span>"
    )
    parts.append("</div>")
    # Filename right below the header for unambiguous identification
    parts.append(f'<p class="filename"><code>{html.escape(fname)}</code></p>')

    # CMIP long_name / standard_name / description (var-level)
    if metadata_by_var:
        meta = metadata_by_var.get(ent.var) or {}
        ln = meta.get("long_name", "")
        sn = meta.get("standard_name", "")
        cm = meta.get("comment", "")
        # Hemispheric scalar files (siarea, siextent, sisnmass, sivol, ...)
        # carry _nh_ / _sh_ in the branding but share one variable-level
        # long_name in the CMIP7 metadata — which always says "North" by
        # default. Substitute the hemisphere word when the file is SH so
        # we don't show "Sea-Ice Area North (SH)" for an Antarctic file.
        hem_tag = ""
        parts_fn = fname.split("_")
        if "nh" in parts_fn:
            hem_tag = " (NH)"
        elif "sh" in parts_fn:
            hem_tag = " (SH)"
            if ln:
                ln = re.sub(r"\bNorthern\b", "Southern", ln)
                ln = re.sub(r"\bnorthern\b", "southern", ln)
                ln = re.sub(r"\bNorth\b", "South", ln)
                ln = re.sub(r"\bnorth\b", "south", ln)
        if ln:
            parts.append(
                f'<p class="longname"><strong>'
                f'{html.escape(ln)}{html.escape(hem_tag)}'
                f'</strong></p>'
            )
        if sn:
            parts.append(
                f'<p class="stdname">CF: <em>{html.escape(sn)}</em></p>'
            )
        if cm:
            parts.append(f'<p class="description">{html.escape(cm)}</p>')

    # SVG range plot using THIS file's numbers
    svg = build_svg(file_entry)
    if svg:
        parts.append(svg)

    # Per-file map: PNG name = .nc filename stem
    if out_dir is not None and fname:
        png_name = re.sub(r"\.nc$", ".png", fname)
        map_path = out_dir / "assets" / "maps" / png_name
        if map_path.exists():
            parts.append(
                f'<img class="varmap" src="assets/maps/{html.escape(png_name)}" '
                f'alt="time-mean map of {html.escape(fname)}" loading="lazy"/>'
            )
        else:
            # Fallback to legacy per-variable PNG if the per-file one
            # hasn't been generated yet
            legacy = out_dir / "assets" / "maps" / f"{ent.var}.png"
            if legacy.exists():
                parts.append(
                    f'<img class="varmap" src="assets/maps/{html.escape(ent.var)}.png" '
                    f'alt="time-mean map of {html.escape(ent.var)} (shared, not specific to this file)" loading="lazy"/>'
                )

    # Numbers table for this file
    parts.append(
        '<table class="numbers">'
        "<thead><tr><th>Quantity</th><th>Expected</th><th>Observed</th></tr></thead>"
        "<tbody>"
        f'<tr><td class="label">min</td><td>{fmt_num(rec_emin)}</td>'
        f"<td>{fmt_num(obs_min)}</td></tr>"
        f'<tr><td class="label">mean</td><td>{fmt_num(rec_emean)}</td>'
        f"<td>{fmt_num(obs_mean)}</td></tr>"
        f'<tr><td class="label">max</td><td>{fmt_num(rec_emax)}</td>'
        f"<td>{fmt_num(obs_max)}</td></tr>"
        "</tbody></table>"
    )

    if ent.source:
        parts.append(
            f'<div class="source"><strong>Source / rationale:</strong> '
            f"{html.escape(ent.source)}</div>"
        )

    diag = diagnosis_text(file_entry)
    if diag:
        parts.append(f'<div class="diagnosis">{html.escape(diag)}</div>')

    parts.append("</div>")
    return "".join(parts)


def render_var_card(entry: VarEntry,
                    out_dir: Optional[Path] = None,
                    metadata_by_var: Optional[Dict[str, Dict[str, str]]] = None) -> str:
    status = entry.worst_status
    status_cls = status.lower()

    # Every card gets the full layout — including PASS — so each variable
    # has a map plot regardless of status.
    sev_tag = ""
    if entry.worst_severity:
        sev_tag = f'<span class="sev-tag">{html.escape(entry.worst_severity)}</span>'

    units_table = entry.units_table or "?"
    units_file = entry.units_in_file or "?"

    parts: List[str] = []
    parts.append(
        f'<div class="var-card {status_cls}" id="var-{html.escape(entry.var)}">'
    )
    parts.append('<div class="header">')
    parts.append(f'<span class="name">{html.escape(entry.var)}</span>')
    parts.append(_pill(status))
    parts.append(sev_tag)
    parts.append(
        f'<span class="meta">realm={html.escape(entry.realm or "?")} '
        f"&middot; units(file)={html.escape(units_file)} "
        f"&middot; units(table)={html.escape(units_table)}"
        "</span>"
    )
    parts.append("</div>")

    # CMIP7 long_name / standard_name / description block.
    meta = (metadata_by_var or {}).get(entry.var, {})
    long_name = (meta.get("long_name") or "").strip()
    description = (meta.get("comment") or "").strip()
    standard_name = (meta.get("standard_name") or "").strip()
    if long_name:
        parts.append(
            f'<p class="longname"><strong>{html.escape(long_name)}</strong></p>'
        )
    if standard_name:
        parts.append(
            f'<p class="stdname">CF: <em>{html.escape(standard_name)}</em></p>'
        )
    if description:
        parts.append(
            f'<p class="description">{html.escape(description)}</p>'
        )

    svg = build_svg(entry)
    if svg:
        parts.append(svg)

    # Optional time-mean map image, if present alongside the report.
    if out_dir is not None:
        map_path = out_dir / "assets" / "maps" / f"{entry.var}.png"
        if map_path.exists():
            parts.append(
                f'<img class="varmap" src="assets/maps/{html.escape(entry.var)}.png" '
                f'alt="time-mean map of {html.escape(entry.var)}" loading="lazy"/>'
            )

    # Numbers table
    parts.append(
        '<table class="numbers">'
        "<thead><tr><th>Quantity</th><th>Expected</th><th>Observed</th></tr></thead>"
        "<tbody>"
        f'<tr><td class="label">min</td><td>{fmt_num(entry.expected_min)}</td>'
        f"<td>{fmt_num(entry.obs_min)}</td></tr>"
        f'<tr><td class="label">mean</td><td>{fmt_num(entry.expected_mean)}</td>'
        f"<td>{fmt_num(entry.obs_mean)}</td></tr>"
        f'<tr><td class="label">max</td><td>{fmt_num(entry.expected_max)}</td>'
        f"<td>{fmt_num(entry.obs_max)}</td></tr>"
        "</tbody></table>"
    )

    # Note the file whose stats drove the worst-status row above. The map
    # below comes from a DIFFERENT (representative) file picked by
    # build_maps.py, which is confusing without this hint.
    if entry.worst_file and len(entry.files) > 1:
        parts.append(
            '<p class="worstfile">Observed numbers above are from '
            f'<code>{html.escape(entry.worst_file)}</code>. '
            "Per-file detail and diagnosis below.</p>"
        )

    if entry.source:
        parts.append(
            f'<div class="source"><strong>Source / rationale:</strong> '
            f"{html.escape(entry.source)}</div>"
        )

    if entry.files:
        # Per-file diagnosis: each file gets its own row with status pill,
        # severity tag (for FAIL/ERROR), numbers, and a short diagnosis
        # snippet. This is the actionable view — readers can see exactly
        # which file is broken and which are fine.
        rows = []
        for r in sorted(entry.files, key=lambda r: os.path.basename(r.get("file", ""))):
            base = os.path.basename(r.get("file", ""))
            st = r.get("status", "?")
            notes = list(r.get("notes") or [])
            sev = ""
            diag = ""
            if st == "FAIL":
                sev = severity_of(entry.var, notes, entry.source)
                # Build a per-file diagnosis using the same templates, but
                # with this file's stats.
                file_entry = VarEntry(
                    var=entry.var,
                    realm=entry.realm,
                    expected_min=entry.expected_min,
                    expected_mean=entry.expected_mean,
                    expected_max=entry.expected_max,
                    source=entry.source,
                    worst_status=st,
                    worst_severity=sev,
                    worst_notes=notes,
                    n_total=int(r.get("n_total") or 0),
                    obs_min=to_float(r.get("min")),
                    obs_mean=to_float(r.get("mean")),
                    obs_max=to_float(r.get("max")),
                    units_in_file=str(r.get("units_in_file") or ""),
                    units_table=entry.units_table,
                    directory=str(r.get("dir") or ""),
                )
                diag = diagnosis_text(file_entry)
            elif st == "WARN":
                diag = "; ".join(notes) if notes else "Within tolerance."
            elif st == "ERROR":
                diag = "; ".join(notes) if notes else "Read failed."
            elif st == "NOBOUNDS":
                diag = "No entry in the sanity-check table for this variable."
            # PASS: no diagnosis
            pill = _pill(st)
            sev_tag = (f' <span class="sev-tag">{html.escape(sev)}</span>'
                       if sev else "")
            mn = fmt_num(r.get("min"))
            me = fmt_num(r.get("mean"))
            mx = fmt_num(r.get("max"))
            diag_html = (f'<div class="diagnosis">{html.escape(diag)}</div>'
                         if diag else "")
            rows.append(
                f"<tr><td>{pill}{sev_tag}</td>"
                f"<td><code>{html.escape(base)}</code>{diag_html}</td>"
                f"<td>{mn}</td><td>{me}</td><td>{mx}</td></tr>"
            )
        # Make per-file table visible (not behind <details>) so the
        # information is immediately available.
        parts.append(
            "<table class=\"files-table\">"
            "<thead><tr><th>Status</th>"
            f"<th>File ({len(rows)})</th>"
            "<th>min</th><th>mean</th><th>max</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table>"
        )

    parts.append("</div>")
    return "".join(parts)


def sort_key(entry: VarEntry) -> Tuple[int, int, str]:
    status = entry.worst_status
    if status == "FAIL":
        return (0, entry.severity_rank, entry.var.lower())
    if status == "WARN":
        return (1, 0, entry.var.lower())
    if status == "PASS":
        return (2, 0, entry.var.lower())
    # ERROR / NOBOUNDS at the bottom
    return (3, 0, entry.var.lower())


def _file_card_severity(var: str, rec: Dict[str, Any], rationale: str) -> Optional[str]:
    if str(rec.get("status") or "").upper() != "FAIL":
        return None
    notes = list(rec.get("notes") or [])
    return severity_of(var, notes, rationale=rationale)


def _file_card_sort_key(item: Tuple[VarEntry, Dict[str, Any]]) -> Tuple[int, int, str, str]:
    ent, rec = item
    st = str(rec.get("status") or "PASS").upper()
    s_rank = STATUS_RANK.get(st, 99)
    sev_rank = len(SEVERITY_ORDER) + 1
    if st == "FAIL":
        sev = _file_card_severity(ent.var, rec, ent.source)
        if sev:
            sev_rank = SEVERITY_RANK.get(sev, len(SEVERITY_ORDER))
    return (s_rank, sev_rank, ent.var, os.path.basename(rec.get("file","")))


def render_domain_page(domain: str,
                       entries: Sequence[VarEntry],
                       label: str,
                       out_dir: Optional[Path] = None,
                       metadata_by_var: Optional[Dict[str, Dict[str, str]]] = None) -> str:
    """Render one card PER FILE (not per variable).

    Each .nc file produces its own card with its own status, severity,
    numbers, and diagnosis. Files of the same variable share the
    variable-level metadata (long_name, description, expected bounds,
    source/rationale) and the same map plot.
    """
    title = f"{label} — {DOMAIN_LABELS[domain]}"

    # Flatten to (var_entry, file_record) pairs — one per .nc file.
    pairs: List[Tuple[VarEntry, Dict[str, Any]]] = []
    for ent in entries:
        for rec in ent.files:
            pairs.append((ent, rec))

    pairs.sort(key=_file_card_sort_key)

    def by_status(st: str) -> List[Tuple[VarEntry, Dict[str, Any]]]:
        return [(e, r) for (e, r) in pairs
                if str(r.get("status") or "PASS").upper() == st]

    fails = by_status("FAIL")
    warns = by_status("WARN")
    passes = by_status("PASS")
    others = [p for p in pairs
              if str(p[1].get("status") or "PASS").upper() in ("ERROR","NOBOUNDS")]

    body: List[str] = []
    body.append(f"<h1>{html.escape(title)}</h1>")
    body.append(
        f'<p class="subtle">{len(pairs)} file(s) across '
        f"{len(entries)} variables: "
        f"{len(fails)} FAIL, {len(warns)} WARN, {len(passes)} PASS, "
        f"{len(others)} other.</p>"
    )

    def render_pairs(pp):
        return [render_file_card(e, r, out_dir, metadata_by_var)
                for (e, r) in pp]

    if fails:
        body.append("<h2>FAIL</h2>")
        body.extend(render_pairs(fails))
    if warns:
        body.append("<h2>WARN</h2>")
        body.extend(render_pairs(warns))
    if passes:
        body.append("<h2>PASS</h2>")
        body.extend(render_pairs(passes))
    if others:
        body.append("<h2>Other (ERROR / NOBOUNDS)</h2>")
        body.extend(render_pairs(others))

    if not pairs:
        body.append("<p>No files in this domain.</p>")

    return _page_shell(title, label, domain, "".join(body))


def render_index(all_entries: Sequence[VarEntry], label: str) -> str:
    title = f"{label} — Sanity Check Summary"

    # Per-variable aggregate (worst-of)
    var_counts: Dict[str, int] = defaultdict(int)
    for e in all_entries:
        var_counts[e.worst_status] += 1
    total_vars = sum(var_counts.values())

    # Per-file aggregate (each .nc file counts once, by its own status)
    file_counts: Dict[str, int] = defaultdict(int)
    for e in all_entries:
        for r in e.files:
            s = str(r.get("status") or "PASS").upper()
            file_counts[s] += 1
    total_files = sum(file_counts.values())

    def pct(n: int, t: int) -> str:
        if t == 0:
            return "—"
        return f"{(100.0 * n / t):.1f}%"

    # Per-realm breakdown
    by_dom: Dict[str, List[VarEntry]] = defaultdict(list)
    for e in all_entries:
        if e.domain:
            by_dom[e.domain].append(e)

    body: List[str] = []
    body.append(f"<h1>{html.escape(title)}</h1>")
    body.append(
        f'<p class="subtle">{total_files} files (across {total_vars} '
        f"unique variables). Each frequency / level / region variant "
        "is assessed independently — see the domain pages for per-file "
        "cards. The variable counts below use the worst-of-files status.</p>"
    )

    # Totals — show vars and files side by side
    body.append('<table class="summary"><thead><tr>'
                "<th>Status</th>"
                "<th>Variables</th><th>%</th>"
                "<th>Files</th><th>%</th>"
                "</tr></thead><tbody>")
    for status in ("FAIL", "WARN", "PASS", "ERROR", "NOBOUNDS"):
        nv = var_counts.get(status, 0)
        nf = file_counts.get(status, 0)
        body.append(
            f"<tr><td>{_pill(status)}</td>"
            f"<td>{nv}</td><td>{pct(nv, total_vars)}</td>"
            f"<td>{nf}</td><td>{pct(nf, total_files)}</td></tr>"
        )
    body.append("</tbody></table>")

    # Per-realm — split var-level and file-level too
    body.append("<h2>By realm</h2>")
    body.append('<table class="summary"><thead><tr>'
                "<th rowspan=\"2\">Domain</th>"
                "<th colspan=\"4\">Variables (worst-of)</th>"
                "<th colspan=\"4\">Files</th>"
                "<th rowspan=\"2\">Link</th>"
                "</tr><tr>"
                "<th>FAIL</th><th>WARN</th><th>PASS</th><th>Other</th>"
                "<th>FAIL</th><th>WARN</th><th>PASS</th><th>Other</th>"
                "</tr></thead><tbody>")
    for dom in ("atm", "oce", "ice", "veg"):
        ents = by_dom.get(dom, [])
        # variable counts (worst-of)
        d_var = defaultdict(int)
        for e in ents:
            d_var[e.worst_status] += 1
        d_var_other = d_var.get("ERROR", 0) + d_var.get("NOBOUNDS", 0)
        # file counts
        d_file = defaultdict(int)
        for e in ents:
            for r in e.files:
                d_file[str(r.get("status") or "PASS").upper()] += 1
        d_file_other = d_file.get("ERROR", 0) + d_file.get("NOBOUNDS", 0)
        body.append(
            f"<tr><td>{html.escape(DOMAIN_LABELS[dom])}</td>"
            f"<td>{d_var.get('FAIL',0)}</td>"
            f"<td>{d_var.get('WARN',0)}</td>"
            f"<td>{d_var.get('PASS',0)}</td>"
            f"<td>{d_var_other}</td>"
            f"<td>{d_file.get('FAIL',0)}</td>"
            f"<td>{d_file.get('WARN',0)}</td>"
            f"<td>{d_file.get('PASS',0)}</td>"
            f"<td>{d_file_other}</td>"
            f'<td><a href="{dom}.html">{dom}.html</a></td></tr>'
        )
    body.append("</tbody></table>")

    # Critical issues callout — now per-FILE, not per-variable.
    critical_sevs = {"DATA_INTEGRITY", "PHYS_IMPOSSIBLE",
                     "UNIT_MISMATCH", "SIGN_FLIP"}
    critical: List[Tuple[VarEntry, Dict[str, Any], str]] = []
    for e in all_entries:
        for r in e.files:
            if str(r.get("status") or "").upper() != "FAIL":
                continue
            sev = severity_of(e.var, list(r.get("notes") or []), e.source)
            if sev in critical_sevs:
                critical.append((e, r, sev))
    # Sort by severity, then var, then filename
    critical.sort(key=lambda x: (SEVERITY_RANK.get(x[2], 99),
                                  x[0].var,
                                  os.path.basename(str(x[1].get("file","")))))
    if critical:
        body.append('<div class="callout">')
        body.append(f"<h2>Critical issues ({len(critical)})</h2>")
        body.append("<ul>")
        for e, r, sev in critical:
            dom = e.domain or "?"
            fname = os.path.basename(str(r.get("file") or ""))
            anchor = re.sub(r"\.nc$", "", fname)
            anchor = re.sub(r"[^A-Za-z0-9_-]+", "-", anchor)
            href = f"{dom}.html#file-{anchor}"
            body.append(
                f'<li><a href="{html.escape(href)}"><code>{html.escape(fname)}</code></a> '
                f'<span class="sev-tag">{html.escape(sev)}</span> '
                f'<span class="subtle">({html.escape(DOMAIN_LABELS.get(dom, dom))})</span></li>'
            )
        body.append("</ul></div>")

    body.append("<h2>Pages</h2><ul>")
    for dom in ("atm", "oce", "ice", "veg"):
        body.append(
            f'<li><a href="{dom}.html">{html.escape(DOMAIN_LABELS[dom])}</a></li>'
        )
    body.append("</ul>")

    return _page_shell(title, label, "index", "".join(body))


# ---------------------------------------------------------------------------
# Label inference
# ---------------------------------------------------------------------------

def infer_label(records: Sequence[Dict[str, Any]]) -> str:
    """Look at file paths; the parent of /cmorized/ is the label."""
    for rec in records:
        path = rec.get("file")
        if not path:
            continue
        m = re.search(r"/([^/]+)/cmorized/", path)
        if m:
            return m.group(1)
    return "sanity-check"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--jsonl", default="/tmp/sanity_check_results.jsonl",
                   help="Sanity-check JSONL output")
    p.add_argument("--table",
                   default=str(Path(__file__).resolve().parents[2]
                               / "doc" / "sanity_check_ranges.md"),
                   help="Bounds-table markdown file")
    p.add_argument("--out-dir", default=None,
                   help="Output directory (default: tools/sanity_check/reports/<label>_html)")
    p.add_argument("--label", default=None,
                   help="Experiment label (default: infer from JSONL paths)")
    p.add_argument("--metadata",
                   default="/home/a/a270092/.cache/pycmor/cmip7_metadata/v1.2.2.2/metadata.json",
                   help="CMIP7 metadata JSON for long names + descriptions")
    args = p.parse_args(argv)

    jsonl_path = Path(args.jsonl)
    table_path = Path(args.table)
    metadata_path = Path(args.metadata)

    if not jsonl_path.exists():
        print(f"error: jsonl not found: {jsonl_path}", file=sys.stderr)
        return 2

    records = parse_jsonl(jsonl_path)
    bounds_meta = parse_bounds_table(table_path)
    metadata_by_var = parse_metadata_json(metadata_path)
    label = args.label or infer_label(records)

    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        out_dir = (Path(__file__).resolve().parent / "reports"
                   / f"{label}_html")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "assets").mkdir(parents=True, exist_ok=True)

    # Write CSS
    (out_dir / "assets" / "style.css").write_text(CSS, encoding="utf-8")

    # Collapse to per-variable entries
    entries = collapse(records, bounds_meta)

    # Bucket by domain
    by_dom: Dict[str, List[VarEntry]] = {"atm": [], "oce": [], "ice": [], "veg": []}
    for e in entries:
        if e.domain in by_dom:
            by_dom[e.domain].append(e)
        # entries without a domain are still in `entries` for the index totals,
        # but won't appear on any domain page

    # Index
    (out_dir / "index.html").write_text(
        render_index(entries, label), encoding="utf-8"
    )

    # Per-domain
    for dom in ("atm", "oce", "ice", "veg"):
        page = render_domain_page(dom, by_dom[dom], label, out_dir,
                                  metadata_by_var)
        (out_dir / f"{dom}.html").write_text(page, encoding="utf-8")

    print(f"wrote {out_dir}/index.html and {len(by_dom)} domain pages")
    print(f"variables: {len(entries)} total; "
          f"atm={len(by_dom['atm'])} oce={len(by_dom['oce'])} "
          f"ice={len(by_dom['ice'])} veg={len(by_dom['veg'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
