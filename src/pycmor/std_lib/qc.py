"""
Per-shard compliance-checker step.

Runs ``cchecker.py`` against every NetCDF file the current rule wrote into
``rule.output_directory`` and stores a JSON sidecar summary alongside them.

Opt-in: set ``qc_enabled: true`` on the rule (or in ``inherit:``). Off by
default so existing configs are unchanged.

Rule attributes consumed (all optional):

- ``qc_enabled`` (bool, default False) — master switch
- ``qc_tests`` (list[str], default ``["cf"]``) — checker suites; ``cf``,
  ``acdd``, ``wcrp_cmip7`` if the plugin is installed, ``aicc`` if
  ``cc-plugin-aicc`` is installed.

  ``aicc`` (DKRZ) validates coordinates against the CMIP7 CMOR tables and
  needs ``CMIP7_TABLES_PATH`` pointing at ``cmip7-cmor-tables/tables``.
  Note that it does not degrade gracefully: with the variable unset or
  wrong it raises ``FileNotFoundError`` out of
  ``ComplianceChecker.run_checker``, so the whole call fails and *no*
  report is written, losing the ``cf`` and ``wcrp_cmip7`` results for
  that file as well. Since a missing sidecar is not counted anywhere,
  the run looks clean rather than broken. ``submit_hr_year_shards.sh``
  pre-flights the path for this reason.
- ``qc_criteria`` (str, default ``"normal"``) — lenient | normal | strict
- ``qc_fail_on_mandatory`` (bool, default False) — raise on any Mandatory
  finding instead of just logging.
- ``qc_binary`` (str, default ``"cchecker.py"``) — override path.
- ``qc_checker_options`` (list[str], default ``[]``) — passed to cchecker as
  ``-O``, one per entry, e.g. ``aicc:grid_config:/path/grid_config.json``.
- ``qc_attempts`` (int, default 3) — how often to re-run the
  checker when it produced no report at all. Concurrent shards
  racing on the CF standard-name table cache can make it die at
  import time; that is transient and a retry clears it.
- ``qc_repack`` (bool, default False) — run ``cmip7repack -o`` in-place
  on each file before checking. Clears the wcrp_cmip7 FILE004a
  "missing consolidated internal metadata" finding.
- ``qc_repack_binary`` (str, default ``"cmip7repack"``) — override path.
- ``qc_ignore_codes`` (list[str], default ``[]``) — finding codes to
  mute. Matched against the leading ``[CODE]`` token in the finding's
  ``name`` field, or against the parenthesised section number for cf
  findings. Example: ``["VAR004"]`` to mute FESOM ``(N, 4)`` lat/lon
  bounds shape; ``["§7.1"]`` for cf bounds-attr findings.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Iterable

from ..core.logging import logger


class QCFailure(RuntimeError):
    """Raised when ``qc_fail_on_mandatory`` is True and the run had Mandatory findings."""


def _rule_files(rule) -> list[Path]:
    """Find the .nc files this rule wrote.

    Walks ``rule.output_directory`` recursively so the CMIP7 DRS layout
    (``MIP-DRS7/CMIP7/.../<frequency>/<variable>/<branding>/<grid>/<version>/``)
    is covered. Filters by ``<cmor_variable>_`` filename prefix, which
    is the first token in both the CMIP6 and CMIP7 DRS filename specs.
    For CMIP7, also filters by the branding_suffix (parsed from
    ``rule.compound_name``) so two rules that share a variable but
    differ in branding (e.g. ``siconc`` mon vs day) don't pick up each
    other's files.
    """
    out_dir = getattr(rule, "output_directory", None)
    if not out_dir or not os.path.isdir(out_dir):
        return []
    cmor_var = getattr(rule, "cmor_variable", None) or getattr(rule, "name", None)
    if not cmor_var:
        return []
    # CMIP7 compound_name format: <realm>.<var>.<branding>.<frequency>.<region>
    # Include branding + frequency + region in the prefix so two rules
    # that share cmor_variable + branding + frequency but DIFFER in
    # region (e.g. rlds_1hr_south30 vs rlds_1hr glb) don't pick up each
    # other's files. The DRS filename layout is
    # <var>_<branding>_<freq>_<region>_<grid>_..., so anchoring on
    # ``..._<region>_`` is sufficient. Without this the south30 rule's
    # qc strip + cchecker race against the glb rule's concurrent
    # cmip7repack on the glb file and the sidecar records stale
    # pre-strip findings (cli72 §2.3 _QuantizeBitGroom* on rlds_1hr_glb).
    compound = getattr(rule, "compound_name", "") or ""
    if compound.count(".") >= 4:
        parts = compound.split(".")
        prefix = f"{cmor_var}_{parts[2]}_{parts[3]}_{parts[4]}_"
    else:
        prefix = f"{cmor_var}_"
    matches: list[Path] = []
    for root, _dirs, files in os.walk(out_dir):
        for name in files:
            if name.endswith(".nc") and name.startswith(prefix):
                matches.append(Path(root) / name)
    return sorted(matches)


def _finding_codes(name: str | None) -> set[str]:
    """Extract identifying codes from a finding's ``name`` field.

    wcrp findings start with a bracketed code: ``[ATTR004] Global attr…``.
    cf findings start with a section number: ``§7.1 Cell Boundaries…``.
    """
    if not name:
        return set()
    codes: set[str] = set()
    if name.startswith("[") and "]" in name:
        codes.add(name[1 : name.index("]")])
    if "§" in name:
        for tok in name.split():
            if tok.startswith("§"):
                # Match "§7.1" and the looser "§7" prefix for allowlist convenience.
                codes.add(tok.rstrip(".,;:"))
                head = tok.split(".")[0].rstrip(".,;:")
                codes.add(head)
    return codes


def _count_findings(entries, ignore: set[str] | None = None) -> int:
    n = 0
    ignore = ignore or set()
    for f in entries or []:
        if f.get("msgs") and not (_finding_codes(f.get("name")) & ignore):
            n += 1
        n += _count_findings(f.get("children"), ignore)
    return n


def _suite_counts(suite_result: dict, ignore: set[str] | None = None) -> dict:
    """Prefer cchecker's pre-tallied counts when nothing is ignored;
    otherwise recompute from priorities so the allowlist actually lowers
    the totals."""
    if not ignore and all(k in suite_result for k in ("high_count", "medium_count", "low_count")):
        return {
            "high": int(suite_result["high_count"]),
            "medium": int(suite_result["medium_count"]),
            "low": int(suite_result["low_count"]),
        }
    return {
        "high": _count_findings(suite_result.get("high_priorities"), ignore),
        "medium": _count_findings(suite_result.get("medium_priorities"), ignore),
        "low": _count_findings(suite_result.get("low_priorities"), ignore),
    }


def _summarize(report: dict, ignore: set[str] | None = None) -> dict:
    totals = {"high": 0, "medium": 0, "low": 0}
    by_file: dict[str, dict] = {}
    for file_path, suites in report.items():
        file_counts: dict[str, dict] = {}
        for suite_name, suite_result in (suites or {}).items():
            c = _suite_counts(suite_result or {}, ignore)
            file_counts[suite_name] = c
            for k, v in c.items():
                totals[k] += v
        by_file[file_path] = file_counts
    return {"totals": totals, "by_file": by_file}


def _run_cchecker(
    binary: str,
    tests: Iterable[str],
    criteria: str,
    out_json: Path,
    files: list[Path],
    attempts: int = 3,
    options: Iterable[str] = (),
) -> tuple[int, str]:
    cmd = [binary, "-f", "json_new", "-o", str(out_json), "-c", criteria]
    for t in tests:
        cmd += ["-t", t]
    for o in options:
        cmd += ["-O", o]
    cmd += [str(p) for p in files]
    logger.info(f"qc: running {' '.join(cmd)}")
    attempts = max(1, int(attempts))
    rc, err = 1, ""
    for attempt in range(1, attempts + 1):
        proc = subprocess.run(cmd, capture_output=True, text=True)
        rc, err = proc.returncode, (proc.stderr or proc.stdout or "")
        # A report on disk means the checker ran; rc!=0 then just means it
        # found something. Only a missing report is worth retrying.
        if out_json.exists():
            return rc, err
        if attempt < attempts:
            # Stagger, so shards that collided do not collide again.
            delay = 5 * attempt + (os.getpid() % 7)
            logger.warning(
                f"qc: cchecker.py rc={rc} and no report on attempt "
                f"{attempt}/{attempts}; retrying in {delay}s. stderr:\n{err}"
            )
            time.sleep(delay)
    return rc, err


def _clean_cmip7repack_orphans(files: list[Path]) -> None:
    """Remove ``<file>_cmip7repack`` intermediates next to each input.
    cmip7repack writes the rechunked output to that suffix and then
    renames it over the original. If the previous job was killed (SLURM
    walltime, OOM, ...) between "successfully created" and the rename,
    the partial intermediate stays on disk forever — corrupted (HDF
    error on open) and taking GB per file on large 3D atm fields.
    Sweep before and after each cmip7repack run so the next pass doesn't
    inherit zombies and the current pass doesn't leave them."""
    for fp in files:
        orphan = fp.with_suffix(fp.suffix + "_cmip7repack")
        if orphan.exists():
            try:
                size = orphan.stat().st_size
                orphan.unlink()
                logger.info(f"qc: removed stale cmip7repack orphan {orphan.name} " f"({size / 1024 / 1024:.0f} MB)")
            except Exception as exc:
                logger.warning(f"qc: could not remove cmip7repack orphan {orphan.name}: {exc}")


def _run_cmip7repack(binary: str, files: list[Path]) -> None:
    """In-place ``cmip7repack -o`` over each file. Best-effort: per-file
    failures are logged but do not raise, so a single bad file doesn't
    block the rest of the QC pass.

    Sweeps stale ``_cmip7repack`` intermediates before AND after each
    invocation so a job that times out mid-repack doesn't leave 30 GB
    of corrupted zombies on /scratch (cli72 observed pfull/cl_day
    daily files at 137 levels — each repack takes well over an hour,
    and 3-hour SLURM walltimes kill them mid-rename)."""
    _clean_cmip7repack_orphans(files)
    for fp in files:
        cmd = [binary, "-o", str(fp)]
        logger.info(f"qc: running {' '.join(cmd)}")
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            logger.warning(
                f"qc: cmip7repack rc={proc.returncode} on {fp.name}\n"
                f"  stderr: {(proc.stderr or proc.stdout or '').strip()}"
            )
    _clean_cmip7repack_orphans(files)


def _strip_leading_underscore_attrs(files: list[Path]) -> None:
    """CF §2.3 forbids leading-``_`` attribute names on data variables
    (the netCDF-reserved ``_FillValue`` is the one exception). netCDF4
    writes ``_QuantizeBitGroomNumberOfSignificantDigits`` (and similar)
    when ``significant_digits`` is set in encoding, and ``cmip7repack``
    preserves those attrs through its rewrite. Strip them here so the cf
    §2.3 finding clears without changing the lossy-compression behaviour.
    Operates in place via netCDF4 (re-opens each file ``r+``).
    """
    try:
        import netCDF4  # noqa: F401  (lazy import — qc.py shouldn't pull netCDF4 unless used)
    except ImportError:
        logger.warning("qc: netCDF4 not available; skipping leading-_ attr strip")
        return
    for fp in files:
        try:
            with netCDF4.Dataset(str(fp), "r+") as ds:
                stripped = []
                for vname, var in ds.variables.items():
                    if vname in ds.dimensions:
                        continue  # leave coords / coord-like vars alone
                    for attr in list(var.ncattrs()):
                        if attr.startswith("_") and attr != "_FillValue":
                            var.delncattr(attr)
                            stripped.append(f"{vname}:{attr}")
                if stripped:
                    logger.info(f"qc: stripped leading-_ attrs from {fp.name}: " f"{', '.join(stripped)}")
        except Exception as exc:
            logger.warning(f"qc: leading-_ attr strip failed for {fp.name}: {exc}")


def run_compliance_checker(data, rule):
    """Pipeline step: run ``cchecker.py`` against this rule's output files.

    Returns ``data`` unchanged so it can be appended after ``save_dataset``
    without altering the pipeline contract.
    """
    if not getattr(rule, "qc_enabled", False):
        return data

    binary = getattr(rule, "qc_binary", None) or "cchecker.py"
    if not shutil.which(binary):
        logger.warning(f"qc: {binary} not on PATH — skipping compliance check")
        return data

    files = _rule_files(rule)
    if not files:
        logger.warning(
            f"qc: no output files found for rule "
            f"{getattr(rule, 'cmor_variable', '?')} in "
            f"{getattr(rule, 'output_directory', '?')} — skipping"
        )
        return data

    # Opt-in pre-step: cmip7repack -o in place. Run BEFORE cchecker so
    # the FILE004a finding clears on this pass instead of the next.
    if getattr(rule, "qc_repack", False):
        repack_binary = getattr(rule, "qc_repack_binary", None) or "cmip7repack"
        if shutil.which(repack_binary):
            _run_cmip7repack(repack_binary, files)
        else:
            logger.warning(f"qc: {repack_binary} not on PATH — skipping repack")

    # CF §2.3 cleanup: strip leading-_ attrs that the netCDF4 library and
    # cmip7repack both leave behind on data variables (chiefly
    # _QuantizeBitGroomNumberOfSignificantDigits).
    _strip_leading_underscore_attrs(files)

    tests = list(getattr(rule, "qc_tests", None) or ["cf"])
    criteria = getattr(rule, "qc_criteria", None) or "normal"
    ignore = set(getattr(rule, "qc_ignore_codes", None) or [])
    cmor_var = getattr(rule, "cmor_variable", "var")
    # Sidecar filename must uniquely identify the rule, or one rule's report
    # silently overwrites another's and its findings vanish from the run.
    #
    # rule.name is not enough. It collides two ways: a name can repeat across
    # tier yamls (``evspsbl`` exists as seaIce, atmos and ocean; ``areacella``
    # as native and regridded), and two rules can share name, realm and
    # grid_label while differing only in branding (``sbl_mon`` on hxy-u
    # against hxy-lnd). On the AWI-ESM3-veg-HR recipes that lost 8 of 540
    # reports.
    #
    # compound_name plus grid_label is unique, because it is exactly the DRS
    # identity of the output: no two rules may write the same path. Fall back
    # to rule.name when there is no compound_name (CMIP6-style rules).
    compound = getattr(rule, "compound_name", None)
    grid_label = getattr(rule, "grid_label", None)
    if compound:
        rule_id = f"{compound}_{grid_label}" if grid_label else str(compound)
    else:
        rule_id = getattr(rule, "name", None) or cmor_var
    out_json = Path(rule.output_directory) / f"qc_{rule_id}.json"
    # Reflect the actual rule_id in the log header for grep-ability.
    table_id = rule_id

    attempts = int(getattr(rule, "qc_attempts", 3) or 3)
    options = list(getattr(rule, "qc_checker_options", None) or [])
    rc, err = _run_cchecker(binary, tests, criteria, out_json, files, attempts, options)
    if not out_json.exists():
        logger.error(
            f"qc: cchecker.py rc={rc} after {attempts} attempt(s); no JSON "
            f"produced, so this rule has NO qc report. stderr:\n{err}"
        )
        if getattr(rule, "qc_fail_on_mandatory", False):
            raise QCFailure(f"qc: cchecker.py produced no report for {cmor_var}")
        return data

    try:
        report = json.loads(out_json.read_text())
    except json.JSONDecodeError as exc:
        logger.error(f"qc: failed to parse {out_json}: {exc}")
        return data

    summary = _summarize(report, ignore)
    totals = summary["totals"]
    ignored_note = f" (ignored: {sorted(ignore)})" if ignore else ""
    logger.info(
        f"qc[{cmor_var}/{table_id}]: {len(files)} file(s); "
        f"high={totals['high']} medium={totals['medium']} low={totals['low']}{ignored_note}; "
        f"report={out_json}"
    )
    for fp, suite_counts in summary["by_file"].items():
        for suite, fc in suite_counts.items():
            if any(fc.values()):
                logger.warning(
                    f"qc[{cmor_var}]: {Path(fp).name} {suite} "
                    f"high={fc['high']} medium={fc['medium']} low={fc['low']}"
                )

    if totals["high"] and getattr(rule, "qc_fail_on_mandatory", False):
        raise QCFailure(f"qc[{cmor_var}]: {totals['high']} Mandatory finding(s) — see {out_json}")

    return data
