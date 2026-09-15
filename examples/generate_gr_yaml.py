#!/usr/bin/env python3
"""Generate a gr-grid variant of a FESOM-ingesting pycmor tier yaml.

Input:  the source-of-truth tier yaml (produces `gn` output).
Output: a derived yaml that, when run by pycmor, produces `gr` (regular
        0.5° lat/lon) cmorized output from the same experiment's
        `<var>.fesom.gr.<year>.nc` XIOS-regridded files.

This is launcher-side preprocessing — there is no parallel `_gr/` source
tree to maintain. The gn yaml is the only file humans edit; this script
deterministically derives the gr counterpart on every run.

Filtering (rule level):
  Kept   — rules whose PRIMARY input pattern references `fesom` (substring).
  Dropped — rules with no inputs (mesh-derived fx), rules whose primary
            input is OIFS/atm-side, etc. They have no gr equivalent
            under this naming scheme.

Pattern rewrites (string level, applied recursively to every str value):
  `\\.fesom\\.\\d{4}\\.nc`  →  `\\.fesom\\.gr\\.\\d{4}\\.nc`

Inherit overrides:
  grid_label          → g131       (CMIP7 CV: regular-lat-lon 0.5°, n_cells=259200,
                                    southernmost_latitude=-89.75, westernmost_longitude=0.25;
                                    verified against sst.fesom.gr lat[0]=-89.75 lon[0]=0.25)
  grid                → "regular 0.5° lat/lon (XIOS interpolation from FESOM DARS, 720x360)"
  nominal_resolution  → "50 km"   (CMIP7 CV-bin for ~44 km area-weighted √mean cell area)
  name                → original + " (gr)"   (for log clarity)

Usage:
    generate_gr_yaml.py <input.yaml> <output.yaml>
"""
import functools
import os
import re
import sys
import yaml
from pathlib import Path

NATIVE_PAT = r"\.fesom\.\d{4}\.nc"
GR_PAT = r"\.fesom\.gr\.\d{4}\.nc"
# Post-repoint patterns have the year already substituted:
#   \.fesom\.1851\.nc  (instead of \.fesom\.\d{4}\.nc)
# This regex matches either placeholder form for the gr rewrite.
NATIVE_PAT_REGEX = re.compile(r"\\\.fesom\\\.(\\d\{4\}|\d{4})\\\.nc")

# Custom-step name fragments that mark a pipeline as
# FESOM-unstructured-mesh-dependent. Rules using such a pipeline
# cannot run against the gr (regular lat/lon) data because the steps
# look up mesh cell_area/coords aligned to the source nod2/elem dims.
# Discovered the hard way in cli46:
#   mass_transport_pipeline      → core_ocean_gr_1/_2 FAILED
#   ice_mass_transport_pipeline  → lrcs_seaice_gr_1/2/3 FAILED
# Add more substrings here as new gr failures surface.
FESOM_MESH_STEP_SUBSTRINGS = (
    # Transport calcs needing FESOM cell_area aligned to nod2/elem
    "compute_mass_transport",
    "compute_ice_mass_transport",
    "compute_salt_transport",
    "compute_heat_transport",
    # MOC / barotropic streamfunctions on FESOM mesh
    "compute_msftbarot",
    "compute_msftmz",
    "compute_msftm_density",
    # Volume / vertical-integration steps needing mesh
    "compute_volcello",
    "vertical_integrate",
    # Basin diagnostics via tripyview (mesh-bound)
    "compute_hfbasin",
    "compute_sltbasin",
    # Bottom extraction by mesh indexer (function: extract_bottom)
    "extract_bottom",
    # Hemispheric integration expects nod2 horizontal dim
    # (function: integrate_over_hemisphere)
    "integrate_over_hemisphere",
    # Steric SSH from FESOM column
    "compute_zostoga",
    # FESOM w on layer interfaces → midpoints
    "average_w_interfaces_to_midpoints",
)

# Steps that are correct on the FESOM native mesh and WRONG on gr, but that
# do not fail -- they run to completion and write plausible, incorrect data.
# The mesh lists above are populated from crashes; nothing surfaces this
# class, so it has to be listed deliberately.
#
# nan_to_zero: FESOM/XIOS writes _FillValue where a field is physically
# zero (a_ice and every other sea-ice field contains no exact zeros at
# all), so the cmorized value over ice-free ocean must be 0, not missing.
# On nod2 that is safe because the mesh is ocean-only -- there is no land
# node to turn into "0 % sea ice". On gr there is: land cells are stored
# as fill too, so an unconditional fillna reports 0 % ice over every
# continent. The gr counterpart takes the ocean footprint from an
# always-wet reference (sst) produced by the same regridding, so the
# coastlines agree cell-for-cell.
NATIVE_ONLY_STEP_REPLACEMENTS = {
    "nan_to_zero": "nan_to_zero_over_ocean",
}

# Reference used by nan_to_zero_over_ocean to tell land from ice-free
# water. Must be wet wherever the ocean is and come from the same XIOS
# regridding as the data. Measured on the final gr format (year 1851):
# sst, ssh and sss are each fill in exactly 82431 of 259200 cells, and
# that set is bit-identical at every timestep.
OCEAN_REF_VARIABLE = "sst"

# Leading variable token of a FESOM filename pattern, e.g. the `a_ice` in
# `a_ice\.fesom\.gr\.1851\.nc`. Used to derive the reference pattern from
# the rule's own primary pattern, which keeps run and year correct
# automatically -- generate_gr_yaml runs after repoint_hr_year.py, so the
# year is already substituted.
VAR_TOKEN_REGEX = re.compile(r"^[A-Za-z0-9_]+(?=\\\.fesom\\\.)")

# Some pipelines share step functions with working pipelines (compute_sisnhc
# is used by both the simple sisnhc_pipeline and the mesh-dependent
# sisnhc_from_msnow_pipeline). Substring-match these pipeline NAMES to
# filter the broken variants surgically.
FESOM_MESH_PIPELINE_NAME_SUBSTRINGS = (
    # veg_seaice sisnhc_from_msnow_pipeline: cli47 MemoryError
    # (297708764688000,) from a broken broadcast on gr.
    "sisnhc_from_msnow",
)


def is_fesom_primary(rule):
    inputs = rule.get("inputs") or []
    if not inputs:
        return False
    pattern = inputs[0].get("pattern", "")
    return "fesom" in pattern


def pipeline_needs_fesom_mesh(pl_def):
    # First: pipeline-name match (catches variants whose step function is
    # shared with a working pipeline)
    name = pl_def.get("name", "")
    if any(needle in name for needle in FESOM_MESH_PIPELINE_NAME_SUBSTRINGS):
        return True
    # Then: step-substring match
    for step in pl_def.get("steps", []) or []:
        if not isinstance(step, str):
            continue
        if any(needle in step for needle in FESOM_MESH_STEP_SUBSTRINGS):
            return True
    return False


def rule_uses_fesom_mesh(rule, mesh_pipeline_names):
    pls = rule.get("pipelines") or []
    return any(p in mesh_pipeline_names for p in pls)


@functools.lru_cache(maxsize=16)
def _listdir(path):
    try:
        return tuple(os.listdir(path))
    except OSError:
        return ()


def gr_input_files_exist(rule):
    """Return True if at least one file in the rule's primary input path
    matches the (already gr-rewritten) pattern.

    cli47 surfaced a separate failure class: rules whose primary input
    variable is in the FESOM gn set but not in Patrick's XIOS regrid set
    (Test_v342_1y_01: 87 gn vs 78 gr variables = 9 variable gap). Those
    rules fail with `OSError('no files to open')`. Drop them here so the
    gr derivative only contains rules with actual source data.
    """
    inputs = rule.get("inputs") or []
    if not inputs:
        return True
    inp = inputs[0]
    data_path = inp.get("path", "")
    pattern_re = inp.get("pattern", "")
    if not data_path or not pattern_re:
        return True
    try:
        regex = re.compile(pattern_re)
    except re.error:
        return True
    return any(regex.fullmatch(fn) for fn in _listdir(data_path))


def swap_native_only_steps(pipelines):
    """Swap native-mesh-only steps for their gr counterparts, in place.

    Returns (n_swapped, names of the pipelines touched). The pipeline
    names are what tells the rule pass which rules need the reference
    triplet injected.
    """
    n = 0
    touched = set()
    for pl in pipelines:
        steps = pl.get("steps")
        if not steps:
            continue
        new_steps = []
        for s in steps:
            if isinstance(s, str):
                for native, gr in NATIVE_ONLY_STEP_REPLACEMENTS.items():
                    if native in s and gr not in s:
                        s = s.replace(native, gr)
                        n += 1
                        touched.add(pl.get("name"))
                        break
            new_steps.append(s)
        pl["steps"] = new_steps
    return n, touched


def inject_ocean_ref(rule):
    """Give a rule the ocean_ref triplet nan_to_zero_over_ocean needs.

    Derived from the rule's own primary input so the run directory and
    the year are inherited rather than configured. Returns True if the
    triplet could be built.
    """
    inputs = rule.get("inputs") or []
    if not inputs:
        return False
    path = inputs[0].get("path")
    pattern = inputs[0].get("pattern", "")
    if not path or not VAR_TOKEN_REGEX.match(pattern):
        return False
    rule["ocean_ref_path"] = path
    rule["ocean_ref_pattern"] = VAR_TOKEN_REGEX.sub(OCEAN_REF_VARIABLE, pattern)
    rule["ocean_ref_variable"] = OCEAN_REF_VARIABLE
    return True


def rewrite_patterns(obj):
    if isinstance(obj, str):
        # Insert `gr\.` between `\.fesom\.` and the year token (which may
        # be the placeholder `\d{4}` or a literal 4-digit year if repoint
        # has already substituted it).
        return NATIVE_PAT_REGEX.sub(r"\.fesom\.gr\.\1\.nc", obj)
    if isinstance(obj, dict):
        return {k: rewrite_patterns(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [rewrite_patterns(x) for x in obj]
    return obj


def main():
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    src, dst = sys.argv[1], sys.argv[2]
    d = yaml.safe_load(Path(src).read_text())

    pipelines = d.get("pipelines", []) or []
    mesh_pls = {
        p["name"]
        for p in pipelines
        if "name" in p and pipeline_needs_fesom_mesh(p)
    }

    rules = d.get("rules", []) or []
    after_fesom = [r for r in rules if is_fesom_primary(r)]
    after_mesh = [r for r in after_fesom if not rule_uses_fesom_mesh(r, mesh_pls)]
    # Pattern rewrite happens BEFORE file-existence check so the gr-prefixed
    # filenames are what we look for on disk.
    after_mesh = rewrite_patterns(after_mesh)
    after_files = [r for r in after_mesh if gr_input_files_exist(r)]

    # Steps that silently produce wrong data on gr (see the list's comment),
    # swapped for their gr counterparts. The rules using those pipelines then
    # need the reference triplet the replacement step reads.
    n_steps_swapped, swapped_pls = swap_native_only_steps(pipelines)
    n_refs = 0
    for r in after_files:
        if set(r.get("pipelines") or []) & swapped_pls:
            if inject_ocean_ref(r):
                n_refs += 1
            else:
                print(
                    f"  WARNING: {r.get('name')!r} uses a swapped pipeline but its primary input "
                    "does not yield an ocean reference; it would mask nothing.",
                    file=sys.stderr,
                )

    d["rules"] = after_files
    n_mesh_dropped = len(after_fesom) - len(after_mesh)
    n_missing_dropped = len(after_mesh) - len(after_files)
    kept = after_files

    inh = d.setdefault("inherit", {})
    # CMIP7 CV grid_label: g131 is the registered horizontal_grid_cell for
    # regular-latitude-longitude, n_cells=259200 (720x360), 0.5° x 0.5°
    # offsets lat[0]=-89.75, lon[0]=0.25 — matches the FESOM regrid exactly.
    # Verified 2026-06-16 against EMD_RESPONSE_cmip7_grid_label_cv.md.
    inh["grid_label"] = "g131"
    inh["grid"] = (
        "regular 0.5° lat/lon (XIOS interpolation from FESOM DARS, 720x360 cells)"
    )
    inh["nominal_resolution"] = "50 km"

    gen = d.setdefault("general", {})
    base_name = gen.get("name", "")
    if base_name and not base_name.endswith(" (gr)"):
        gen["name"] = f"{base_name} (gr)"

    Path(dst).write_text(
        yaml.safe_dump(
            d,
            default_flow_style=False,
            sort_keys=False,
            width=200,
            allow_unicode=True,
        )
    )
    print(
        f"  {Path(src).name} -> {Path(dst).name}: "
        f"{len(kept)}/{len(rules)} rules kept "
        f"({n_mesh_dropped} fesom-mesh-only, "
        f"{n_missing_dropped} no-gr-input dropped, "
        f"{n_steps_swapped} native-only steps swapped, "
        f"{n_refs} ocean refs injected)",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
