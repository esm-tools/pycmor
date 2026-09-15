#!/usr/bin/env python3
"""
Repoint the 17 HR pycmor tier yamls at a different model run + year.

Reads each `awi-esm3-veg-hr-variables/<tier>/cmip7_awiesm3-veg-hr*.yaml`,
swaps the hard-coded HR_test_01 paths to the requested run, and adds a
year filter to the input regex `pattern:` entries:

  - FESOM patterns          \\.fesom\\..*\\.nc      ->  \\.fesom\\.<YEAR>\\.nc
  - OIFS  patterns          atm[...]_.*\\.nc        ->  atm[...]_<YEAR>-<YEAR>\\.nc
  - LPJ-GUESS patterns      */run1/*.out            ->  unchanged (per-var file
                                                       contains all years)

Outputs the modified yamls into <workdir>, leaving the source tree clean.

Usage:
  repoint_hr_year.py <RUN_NAME_OR_ABSPATH> <YEAR> <WORKDIR>

Examples:
  repoint_hr_year.py Test_16n 1587 /scratch/$USER/cmorize_Test_16n_y1587
  repoint_hr_year.py /work/bb1469/.../runtime/awiesm3-develop/Test_16n 1587 ./out
"""
from __future__ import annotations

import pathlib
import re
import sys

SRC_DIR = pathlib.Path(__file__).resolve().parent.parent / "awi-esm3-veg-hr-variables"
# The token to swap in the source yamls' hardcoded data paths.
# Must match what's literally in awi-esm3-veg-hr-variables/<tier>/*.yaml today
# (current: ``Final_CMIP7_IO_Test_01``). If the hardcoded path drifts, update
# this string. Run ``grep -h 'data_path:' awi-esm3-veg-hr-variables/*/*.yaml |
# awk -F/ '{print $(NF-1)}' | sort -u`` to find the current value.
OLD_RUN_TOKEN = "Final_CMIP7_IO_Test_01"
RUNTIME_ROOT = "/work/bb1469/a270092/runtime/awiesm3-develop"


def resolve_run_dir(arg: str) -> str:
    return arg if arg.startswith("/") else f"{RUNTIME_ROOT}/{arg}"


def add_year_to_pattern(pat: str, year: str) -> str:
    """Apply year filter to a single pattern/file string (yaml-as-written form).

    Accepts both unquoted (`pattern: foo_.*\\.nc`) and double-quoted
    (`pattern: "foo_.*\\\\.nc"`) yaml forms; the quoted form is unescaped to
    the canonical regex before pattern matching, then re-escaped on return.
    Without this, double-quoted second_input_pattern values silently bypass
    the year-lock.
    """
    quote = ""
    canonical = pat
    if len(pat) >= 2 and pat[0] == pat[-1] and pat[0] in ('"', "'"):
        quote = pat[0]
        inner = pat[1:-1]
        # YAML double-quote semantics: \\ -> \. Single-quoted strings are literal.
        canonical = inner.replace("\\\\", "\\") if quote == '"' else inner

    # FESOM regex form: <var>\.fesom\..*\.nc  OR  <var>\.fesom\.\d{4}\.nc
    # (used in `pattern:` lines). Two placeholder forms are accepted:
    #   `.*`     — legacy, matches any text between `.fesom.` and `.nc`
    #   `\d{4}`  — tightened (commit 4fdade1), matches the 4-digit year
    # Without the tightened branch the substitution silently no-ops, the
    # pattern keeps matching every year on disk, and cli51-style year
    # mixing happens. Also handles the gr-prefixed variant
    # `<var>\.fesom\.gr\.\d{4}\.nc` for symmetry with generate_gr_yaml.py.
    if r"\.fesom\." in canonical:
        out = re.sub(r"\\\.\.\*\\\.nc$", rf"\\.{year}\\.nc", canonical)
        out = out.replace(r"\d{4}", year)
    # FESOM glob form: <var>.fesom.*.nc  (used in `*_file:` lines, literal path)
    elif ".fesom." in canonical and canonical.endswith(".nc"):
        out = re.sub(r"\.\*\.nc$", rf".{year}.nc", canonical)
    # OIFS convention: atm[os|_remapped]_..._<year>-<year>.nc
    elif canonical.startswith("atmos_") or canonical.startswith("atm_remapped_"):
        out = re.sub(r"_\.\*\\\.nc$", rf"_{year}-{year}\\.nc", canonical)
    # LPJ-GUESS .out files contain all years inline
    else:
        out = canonical

    if quote == '"':
        return f'"{out.replace(chr(92), chr(92) * 2)}"'
    if quote == "'":
        return f"'{out}'"
    return out


def repoint_yaml(src: pathlib.Path, run_dir: str, year: str) -> str:
    text = src.read_text()
    # Path swap: any HR_test_01 path -> the requested run dir.
    #
    # Guard: every tier yaml references the template run at least once
    # (checked 2026-07-29: all 17 do, between 1 and 27 times each). Without
    # this check, a drift in the hardcoded path would make the substitution
    # below silently match nothing -- re.sub returns the text unchanged and
    # raises nothing -- so this script would still report success while the
    # yamls kept pointing at the TEMPLATE run. The cmorization would then
    # process the wrong model run and the output would look entirely normal.
    # Fail loudly instead of producing plausible, wrong results.
    old_path = f"{RUNTIME_ROOT}/{OLD_RUN_TOKEN}"
    if old_path not in text:
        raise SystemExit(
            f"ERROR: {src.name} does not contain the expected template path:\n"
            f"           {old_path}\n"
            f"       The path hardcoded in the source yamls has probably changed.\n"
            f"       Fix RUNTIME_ROOT / OLD_RUN_TOKEN at the top of this script.\n"
            f"       Refusing to continue: otherwise the run would quietly use\n"
            f"       the template run's data instead of {run_dir}."
        )
    text = re.sub(
        rf"{re.escape(RUNTIME_ROOT)}/{OLD_RUN_TOKEN}",
        run_dir,
        text,
    )

    # Year filter on `pattern:` and `second_input_pattern:` lines
    def _repl(m: re.Match) -> str:
        prefix, pat = m.group(1), m.group(2)
        return f"{prefix}{add_year_to_pattern(pat, year)}"

    text = re.sub(
        r"(^\s*(?:pattern|second_input_pattern|hnode_pattern):\s*)(\S+)",
        _repl,
        text,
        flags=re.M,
    )

    # Year filter on absolute file specs (salt_file, mice_file, second_input_file, ...)
    # These look like "*_file: /abs/path/<var>\\.fesom\\..*\\.nc" — same FESOM rule.
    def _repl_file(m: re.Match) -> str:
        prefix, val = m.group(1), m.group(2)
        return f"{prefix}{add_year_to_pattern(val, year)}"

    # NB: variable names can contain digits (sgm22, sgm12, etc.). The
    # earlier ``[a-z_]+`` form silently skipped year-filtering for those
    # and left the regex-form pattern in the yaml, which pycmor's
    # ``*_file:`` resolver then tried to open as a literal filename.
    text = re.sub(
        r"(^\s*[a-z0-9_]+_file:\s*)(\S+)",
        _repl_file,
        text,
        flags=re.M,
    )

    # Inject `year:` into the inherit block so year-aware steps
    # (e.g. cap7_aerosol's select_year on centennial GHG forcing files)
    # know which run-year to slice. LPJ-GUESS / OIFS / FESOM patterns
    # already filter by year via the regex; this is for steps that
    # operate on data that the regex couldn't filter.
    text = re.sub(
        r"(^inherit:\s*\n)",
        rf"\1  year: {year}\n",
        text,
        count=1,
        flags=re.M,
    )

    return text


def main() -> int:
    if len(sys.argv) != 4:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    run_arg, year, workdir_arg = sys.argv[1], sys.argv[2], sys.argv[3]
    run_dir = resolve_run_dir(run_arg)
    workdir = pathlib.Path(workdir_arg).resolve()

    if not pathlib.Path(run_dir).is_dir():
        print(f"ERROR: run dir not found: {run_dir}", file=sys.stderr)
        return 1
    if not re.fullmatch(r"\d{4}", year):
        print(f"ERROR: year must be 4 digits, got {year!r}", file=sys.stderr)
        return 1

    workdir.mkdir(parents=True, exist_ok=True)

    yamls = sorted(SRC_DIR.glob("*/cmip7_awiesm3-veg-hr*.yaml"))
    if not yamls:
        print(f"ERROR: no HR yamls under {SRC_DIR}", file=sys.stderr)
        return 1

    for src in yamls:
        tier = src.parent.name
        dest = workdir / f"{tier}.yaml"
        dest.write_text(repoint_yaml(src, run_dir, year))
        print(f"  {tier:<14}  ->  {dest}")

    print(f"\nWrote {len(yamls)} repointed yamls into {workdir}")
    print(f"  source run: {run_dir}")
    print(f"  year filter: {year}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
