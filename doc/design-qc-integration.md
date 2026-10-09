# Design note: integrate CMIP7 QC into pycmor pipelines

Goal: run the WCRP compliance-checker (`cc-plugin-wcrp` → `wcrp_cmip7`)
automatically on every produced NetCDF and aggregate findings per
simulation with `esgf-qa`. Required for ESGF publication; a non-zero
"Mandatory" count on any file blocks publication.

## External tools (already usable on levante)

- `cchecker.py -c strict -t cf -t wcrp_cmip7 <file.nc>` — per-file CF
  and CMIP7 compliance.
- `esgqa -t cf -t wcrp_cmip7 -o <results_dir> <simulation_root>` —
  full-simulation compliance + consistency checks (attribute stability
  across files, time-series gaps, etc.).
- CVs managed via `esgvoc` (`esgvoc install` after `esgvoc config add cmip7`).

Packages `esgf-qa`, `cc-plugin-wcrp`, `esgvoc` install cleanly into
the existing `pycmor_py312` env with pip; no separate env required.

## Integration sketch

1. **New std_lib step** `pycmor.std_lib.qc.run_compliance_checker`:
   given a freshly-written file path, shell out to `cchecker.py` with
   `-f json -o <file>.qc.json` and parse the result. Attach a summary
   to the rule's report log; fail the task if any `Mandatory` errors
   outside a configurable allowlist (e.g. unregistered source_id during
   development).
2. **Pipeline placement**: immediately after
   `pycmor.std_lib.files.save_dataset`. Keeps QC co-located with the
   artifact it verifies and fires per-file, giving fast feedback in
   Prefect/Dask.
3. **Config switches** (`pycmor` section):
   - `qc_enabled: true|false` (default true for CMIP7, false for CMIP6)
   - `qc_checkers: ["cf", "wcrp_cmip7"]`
   - `qc_strictness: strict|normal|lenient`
   - `qc_allow_mandatory_codes: [...]` — ids to downgrade to warnings
     while CVs are incomplete (e.g. unregistered `source_id`,
     `institution_id`, custom `grid_label`).
4. **End-of-run aggregation**: a CLI subcommand `pycmor qc <config>` that
   invokes `esgqa` over the run's `output_directory` once `CMORizer.process()`
   finishes. Writes a consolidated report next to `pycmor_report.log`.
5. **CI hook**: run cchecker on the tiny fixture files used in
   `tests/integration/` so breakages in attribute emission surface
   before user-facing pipelines regress.

## Dependencies

Add to `[cmip7]` extra in `pyproject.toml`:

```toml
esgf-qa       # simulation-level QC
cc-plugin-wcrp # file-level CMIP7 checker
esgvoc        # CV lookups
```

Users still run `esgvoc config add cmip7 && esgvoc install` once per
user (CVs are stored in `~/.local/share/esgvoc/`).

## Known limitations

- **Unstructured grids (FESOM)** trigger `[VAR005] Coordinate
  monotonicity for 'lat'/'lon'`: cchecker requires dimension
  coordinates to be strictly increasing, but FESOM emits unsorted
  1-D node arrays as `lat`/`lon`. Two fixes possible:
  1. Restructure to use a generic `node` dimension with `lat`/`lon`
     as auxiliary coordinates (CF-compliant; cchecker only checks
     monotonicity on dimension coords). Requires changes in
     `dimensions.map_dimensions`.
  2. Regrid to a structured lat/lon grid before write (covered by
     `pycmor.fesom_2p1`). Required path for ESGF publication anyway.
  Document this in the FESOM section of the user guide and treat
  VAR005 as expected for unstructured-mode QC runs.

## Open questions

- How to cleanly suppress CV-lookup failures for temporary unregistered
  values (our AWI-ESM3-* source_ids, custom grid labels) without silently
  hiding real problems later. An allowlist keyed on the specific CV
  collection + invalid value is probably the right granularity.
- Whether to block `save_dataset` on a failed check (fail-fast) or
  always write and mark the rule as QC-failed (fail-soft). Likely
  fail-soft by default, with a `qc_fail_fast` opt-in for release runs.
- Performance: cchecker spins a Python subprocess + CV DB access per
  file. For runs with thousands of files, consider a single process
  with the check function imported, or batched esgqa at the end and
  skip per-file step.
