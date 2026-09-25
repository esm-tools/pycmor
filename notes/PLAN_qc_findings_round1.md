# PLAN: QC findings — round 1

Source: smoke-test of `pycmor.std_lib.qc.run_compliance_checker` against
`ta_tavg-al-hxy-u_mon_glb_gn_AWI-ESM-3_picontrol_r1i1p1f1_158701-158712.nc`
(cli44_test18, cap7_atm).

Sidecar: `/tmp/claude-24456/pycmor_qc_smoke_fhc1vdp0/qc_ta_tavg-al-hxy-u_mon_glb_gn.json`

Suite totals:
- `wcrp_cmip7`: 14 high / 3 medium / 0 low — score 192/209
- `cf`: 1 high / 3 medium / 0 low — score 233/238

Three external truth sources to consult before opening tickets here:
1. **WCRP Essential-Model-Documentation** — https://github.com/WCRP-CMIP/Essential-Model-Documentation —
   despite the name, EMD only holds *technical descriptors* (`calendar`,
   `component_type`, `grid_type`, `vertical_coordinate`, etc.).
   `activity` / `experiment` / `institution` / `source` are NOT here.
2. **WCRP-universe** (`esgvoc` branch) — https://github.com/WCRP-CMIP/WCRP-universe —
   the source of truth for activity / experiment / institution / source /
   organisation / region / variable / etc. **`institution/awi.json`
   exists** with `drs_name: AWI`. `source/` has 7 AWI entries
   (`awi-cm-1-1-{hr,lr,mr}`, `awi-esm-1-1-lr`, `awi-esm-1-recom`,
   `awi-hirham5`) but no AWI-ESM3 family yet.
3. **CMIP7-CVs** (`esgvoc` branch) — https://github.com/WCRP-CMIP/CMIP7-CVs —
   the project-level overlay the `wcrp_cmip7` checker actually queries.
   Currently extremely sparse: 9 activities (incl. `cmip.json`),
   72 experiments (incl. `picontrol.json`), **only 4 institutions**
   (`cccma`, `cnrm-cerfacs`, `ipsl`, `mohc` — **AWI missing**),
   **only 2 sources** (`cnrm_esm2_1e`, `dummy_model`), grid_label uses
   new `g###` scheme (g100-g104, g999) instead of CMIP6 `gn`/`gr`.

**Confirmed AWI HR ID stack from EMD work-in-progress (2026-06-03):**
- `source_id`: `AWI-ESM3-4-2-veg-HR`
- `model_family`: `awi-esm3`
- Component configs (Stage 3): `atmosphere_openifs-48r1_h107_v113`,
  `land_surface_lpj-guess-4.1_h106_v112`, `ocean_fesom-2.7_h114_v116`,
  `sea_ice_fesim-2-7_h114_no-vertical`
- Horizontal computational grids (Stage 2a): h106 (g122 mass-only,
  LPJ-GUESS), h107 (g122 mass+xy-velocity, OpenIFS), h114 (Arakawa-B
  g130 mass + g132 xy-velocity, FESOM2/FESIM2)
- Vertical computational grids (Stage 2b): v112 (LPJ-GUESS soil),
  v113 (IFS L137), v116 (FESOM 57 z-levels)
- Grid cells (Stage 1): g122 (TCO319 reduced Gaussian, atm/land),
  g130 (DARS2 vertices ~3.15M, FESOM scalar nodes),
  g132 (DARS2 element centroids ~6.23M, FESOM velocity points)
- **NOT in this stack:** SR variant (TCO95 + L91 + CORE3) — open for
  later submission. LR variant (TCO95 + CORE2) similarly pending.

**HR configs swept (2026-06-03):**
- 28 `cmip7_bench_hr_*.yaml`: `source_id` → `AWI-ESM3-4-2-veg-HR`,
  `grid_label` → `g122` (26 atmospheric: ua/uas/wap/zg/ta) or `g130`
  (2 ocean: tossq).
- LR / SR / test / seaice CORE2 configs (`cmip7_core_*`, `cmip7_cap7_*`,
  `cmip7_extra_*`, `cmip7_veg_*`, `cmip7_lrcs_*`, `awiesm3-cmip7-*`)
  left on placeholder values — they need their own `source_id` /
  `grid_label` once registered.

---

## Group A — Fixable now in YAML / config (CV value correction)

These look like wrong-cased or wrong-value strings in the config, *not*
checker bugs. Confirm against (1) above before changing.

### A1. `activity_id: cmip` → `CMIP`
- Finding: `ATTR004 activity_id vocabulary check: Invalid value(s) ['cmip']`.
- Action: grep all configs for `activity_id:` and verify the casing the
  pipeline actually writes; the EMD CV uses `CMIP` (upper).
- If lower-case is set in YAML, fix at source. If YAML is already `CMIP`
  but the file came out `cmip`, this is a code bug — see Group B.

### A2. `experiment_id: picontrol` → `piControl`
- Same as A1; EMD CV is `piControl`.

### A3. `grid_label: gn` — verify
- Finding flags `gn` as invalid. EMD CV for CMIP7 may differ from CMIP6.
- Action: check what the WCRP CMIP7 CV expects for native-grid label;
  could legitimately be `gn` (carry-over) or could be renamed.

### A4. `parent_experiment_id` empty
- Finding: "Vocabulary lookup error: value should be set".
- Action: when no parent, EMD requires the literal string `"no parent"`
  (already set this way in `cmip7_core_seaice_core2_test.yaml`, but the
  smoke-test file came from an older config where it was unset).
  Confirm all current configs set it.

### A5. `institution_id: AWI` not in ESGF vocab
- Finding `ATTR009`: `institution_id 'AWI'` not in ESGF vocab.
- Action: check EMD's `institution` collection. If `AWI` isn't there
  but `AWI-...` is, update config; if `AWI` is missing entirely from
  the published CV, that's an upstream PR against the EMD repo.

### A6. `experiment` / `institution` description text not in CV (Medium)
- Findings: free-text descriptions don't match any CV term's
  `description` field.
- Action: copy the exact `description` string from the EMD CV entry into
  the YAML rather than authoring our own.

---

## Group B — Fixable now in pycmor code

### B1. Investigate possible downcasing of global-attr values
- Pre-req: confirm whether A1/A2 are config-side or code-side. Smoke
  file shows `cmip`/`picontrol` lower-case, but the source YAML for
  cli44_test18 needs to be checked (different config from the seaice
  one I wired QC into).
- If pycmor downcases: trace through
  `src/pycmor/std_lib/global_attributes.py` and
  `src/pycmor/std_lib/attributes.py:set_global`; look for `.lower()` on
  CV-typed values.

### B2. `longitude` coord var missing `standard_name='longitude'` (cf §5.1, Mandatory)
- Finding: `Coordinate variable 'longitude' should have standard_name='longitude', found: 'None'`.
- Likely site: `src/pycmor/std_lib/coordinate_attributes.py:set_coordinate_attributes`.
- Action: ensure `lat` *and* `lon` always get `standard_name` set even
  when the coord name is already CMIP-compliant (the gap may be that
  the lookup keys on the *source* name, not the post-rename name).

### B3. `longitude_bnds` stale `long_name` (cf §7.1, Recommended)
- Finding: `'longitude_bnds' has attr 'long_name' with value 'vertical model levels bounds'`.
- This is a *vertical-coord* long_name leaking onto a horizontal bounds
  variable. Almost certainly from a copy in `_recover_bounds_from_inputs`
  or in `_ensure_lat_lon_bounds_and_external_vars` at
  [src/pycmor/std_lib/files.py:295](src/pycmor/std_lib/files.py#L295)–[350](src/pycmor/std_lib/files.py#L350).
- Action: strip / overwrite `long_name` on `<coord>_bnds` to match the
  parent coord, not whatever was inherited.

### B4. `_QuantizeBitGroomNumberOfSignificantDigits` attr name (cf §2.3, Recommended)
- Finding: attr starts with `_` (CF reserves leading-underscore for
  netCDF-internal).
- This attr is written by quantize/compression encoding. Either:
  - rename to non-underscore form in our encoding setup, or
  - drop it from the final file via `_strip_unportable_encoding`
    at [src/pycmor/std_lib/files.py:265](src/pycmor/std_lib/files.py#L265).
- Action: add it to the strip list.

### B5. Missing `units_metadata` on temperature variables (cf §3.1.2, Recommended)
- Finding: temperature vars are recommended to carry `units_metadata`
  ∈ `{temperature: difference, temperature: on_scale, temperature: unknown}`.
- For absolute temperature (`ta`, `tas`, `ts`), the right value is
  `temperature: on_scale`.
- Action: in `set_variable_attrs`, when the data request unit is `K`
  or `°C` and the standard_name maps to temperature, set
  `units_metadata="temperature: on_scale"`.

### B6. `calendar: standard` → `proleptic_gregorian` (wcrp TIME003a, Recommended)
- Finding: recommend `proleptic_gregorian` over `standard`.
- Already handled by per-rule `time_calendar` config knob (issue #215);
  document that CMIP7 configs should set
  `time_calendar: proleptic_gregorian`, or change the default in
  `_save_dataset_impl` at
  [src/pycmor/std_lib/files.py:1629](src/pycmor/std_lib/files.py#L1629)
  from `"standard"` to `"proleptic_gregorian"` for CMIP7.

---

## Group C — DRS / directory layout (FILE001, PATH001, PATH002)

### C1. DRS root `cmip7` missing from output path
- Finding `FILE001`: project root `cmip7` not in file path.
- Current configs write to `./cmorized_output/<experiment_tag>/`. The
  CMIP7 DRS expects a `<mip_era>/<activity_id>/<institution_id>/...`
  hierarchy.
- Action: add a `cmip7_drs_root: true` toggle (or always-on for
  `cmor_version: CMIP7`) in `create_filepath` /
  [src/pycmor/std_lib/files.py:1048](src/pycmor/std_lib/files.py#L1048),
  so the output directory follows MIP-DRS7. Then PATH001/PATH002
  consistency checks become live.

### C2. Filename term positions 5–7 invalid
- Finding `FILE001`: `gn`, `AWI-ESM-3`, `picontrol` at positions 5/6/7.
- Blocked by A1 / A2 / A3 / final `source_id`. Once CV values are right
  the filename is correct by construction.

---

## Group D — Blocked on external

### D1. Final `source_id`
- Every ATTR004 / FILE001 hit involving `source_id` stays open until
  the official registration lands. Keep `AWI-ESM-3` / `AWI-ESM3-VEG-LR`
  as known-bad placeholders; once registered, do a sweep across all
  YAML configs + any `source` references in code/docs.

### D2. `institution_id=AWI` — depends on A5 outcome
- If A5 reveals `AWI` is simply missing from the published CV, the fix
  is an upstream PR (EMD repo) — track separately.

---

## Group E — Defer / discuss (FESOM-specific)

### E1. `lat_bnds` / `lon_bnds` shape `(N, 4)` (wcrp VAR004, ×2 Mandatory)
- The checker expects regular-grid `(N, 2)` cell-edge pairs; FESOM
  unstructured output legitimately has 4-corner cells.
- Options:
  - upstream PR against `cc-plugin-wcrp` to allow `(N, 4)` when
    `grid_label` indicates unstructured;
  - emit `(N, 2)` *additional* bounds derived from the 4-corner ones
    to satisfy the regular-grid expectation (wasteful);
  - allowlist this finding in our QC step (`qc_ignore_codes: [VAR004]`)
    when the rule is on an unstructured grid.
- Action: open a discussion with Martin / Ayoub at IPSL/DKRZ before
  picking one.

---

## Group F — Tooling

### F1. `FILE004a` — missing consolidated internal metadata
- Finding: "File does not have consolidated internal metadata. Run
  `cmip7repack` to fix this."
- `cmip7repack` is part of the CMIP7 tooling ecosystem; currently
  applied post-hoc.
- Action: add an opt-in pipeline step
  `pycmor.std_lib.qc.cmip7repack` (or a `post_save` hook) that calls
  `cmip7repack` on each just-written file before QC runs. Move the
  QC step to fire *after* repack so this finding goes away in one
  cycle.

---

## Suggested order of attack

1. **A1–A4 + A6** — config-side string fixes (cheap, may clear several
   wcrp mandatory findings in one pass).
2. **B2, B3, B4** — three small `std_lib` fixes for the cf-side issues;
   tiny, isolated, easy to ship behind a unit test.
3. **B1** — only do this *if* A1/A2 turn out to be config-side; don't
   touch code if YAML was the source of truth.
4. **B5, B6** — recommended-tier; nice-to-have, batch after B2–B4.
5. **F1** — bake `cmip7repack` into the per-shard QC step; clears
   FILE004a uniformly.
6. **C1** — DRS rewrite; bigger change, do once everything above is
   green so the consistency checks (PATH001/002) have a clean baseline.
7. **E1** — schedule the FESOM bounds-shape conversation; this is the
   only finding likely to need an upstream patch.
8. **D1, D2** — track as blockers, sweep once unblocked.

---

## Re-test after each step

```bash
# Once the seaice run produces output:
pycmor process examples/cmip7_core_seaice_core2_test.yaml

# Or smoke-test against any directory of CMIP7-named files:
python -c "
from types import SimpleNamespace
from pycmor.std_lib.qc import run_compliance_checker
rule = SimpleNamespace(
    cmor_variable='ta', table_id='tavg-al-hxy-u_mon_glb_gn',
    output_directory='<dir>',
    qc_enabled=True, qc_tests=['cf','wcrp_cmip7'],
    qc_criteria='normal', qc_fail_on_mandatory=False)
run_compliance_checker('x', rule)
"
```

Track scores in this file as the totals drop. Target for "green":
- cf high == 0, medium ≤ 2 (deferred low-priority recs OK)
- wcrp_cmip7 high == 0 (once D1 + D2 unblocked), medium == 0
