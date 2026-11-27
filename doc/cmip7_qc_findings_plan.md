# CMIP7 QC findings — categorization and plan

Source file analyzed:
`cmorized_output/verify_sidmassth/.../sidmassth_tavg-u-hxy-si_mon_GLB_gn_AWI-ESM3-VEG-LR_piControl_r1i1p1f1_190901-190912.nc`

Reports: [qc_reports/sidmassth_cf_final.txt](../qc_reports/sidmassth_cf_final.txt),
[qc_reports/sidmassth_wcrp_final.txt](../qc_reports/sidmassth_wcrp_final.txt)

Checker versions (local forks, integrated):
- `compliance-checker` branch `fix/check-cell-boundaries-interval-perf`
- `cc-plugin-wcrp` branch `local/integration` (merges `fix/var005-skip-aux-coords` + `fix/var004-allow-polygon-bounds`)

## Guiding principle

Fix at the pycmor source, not per-rule in each config. A single write-site change
should take effect for every rule and every variable.

---

## CF 1.11 remaining (2 findings)

| # | Finding | Action |
|---|---|---|
| CF1 | 1 lat point outside `lat_bnds` (cell 41056, North Pole) | Decide: accept / extend checker / narrow pycmor clamp for polar cells |
| CF2 | `cell_measures: areacello` referenced but not shipped | Decide: drop attr or ship `areacello` |

---

## wcrp_cmip7 remaining (25 findings)

### Category A — pycmor is writing wrong CV values (fix in pycmor source)

For every global attribute below, the value pycmor writes is not a registered
CMIP7 CV term. Fix the emission site so all rules/variables benefit.

| Attribute | Written | Valid CMIP7 CV term(s) | Fix type |
|---|---|---|---|
| `Conventions` | `CF-1.11 CMIP-7.0` | `CF-1.11`, `CF-1.12`, `CF-1.13` | Drop `CMIP-7.0` suffix |
| `drs_specs` | `CMIP7` | `MIP-DRS7` | Constant rename |
| `data_specs_version` | `1.0.0` | `MIP-DS7.1.0.0` | Constant rename |
| `license_id` | `cc-by-4-0` | `CC-BY-4.0` | Case/punctuation |
| `region` | `GLB` | `glb` (lowercase) | Lowercase when extracted from compound_name |
| `nominal_resolution` | `none` | `100 km` (and 13 others) | **Bug**: config says `"100 km"` but serializer emitted `none` |
| `parent_experiment_id` | `no parent` | (no such term) | Use CMIP7 convention for "no parent" (likely omit attr or empty) |

### Category B — branded_variable format mismatch

`branded_variable: seaIce.sidmassth.tavg-u-hxy-si.mon.GLB`

CMIP7 CV uses the DRS format: `sidmassth_tavg-u-hxy-si` (variable_id + branding_suffix).
Transform at write time — `compound_name` internally stays dotted, but the
`branded_variable` global attribute should hold the DRS form.

Cascades to 5 Warning-level registry checks (`standard_name`, `units`, `cell_methods`,
`cell_measures`, `long_name`) that all say "Registry rule enabled but expected_term
is None" because the branded-variable lookup failed.

### Category C — True EMD (blocks us, AWI action needed)

| Attribute | Issue | Path forward |
|---|---|---|
| `source_id: AWI-ESM3-VEG-LR` | CMIP7 source CV only contains `CNRM-ESM2-1e`, `DUMMY-MODEL` | AWI registers model in WCRP-CMIP/CMIP7 source CV |
| `grid_label: gn` | CMIP7 uses registered grid IDs `g100`..`g104`, `g999` | AWI registers FESOM native grid (or use `g999` = "unregistered" slot) |

### Category D — Upstream WCRP CV gap (not AWI's problem)

- `organisation` CV in CMIP7 has **0 terms**. `institution_id: AWI` + `institution`
  attr can't validate. Fix: wait for WCRP to populate, or open an issue on
  WCRP-CMIP/CMIP7-CVs.

### Category E — Not real failures

- `[VAR012] × 2` — "Skipping bounds check — non-interval bounds" — informational.
- Optional-tier `[ATTR004] 'source'` description mismatch.

### Dependent (will resolve automatically when the above resolve)

- `[FILE001]` DRS Directory Vocabulary Check (fails because source/region/grid_label/institution fail)
- `[FILE001]` DRS Filename Vocabulary Check (same)
- `[ATTR009]` institution_id vs institution (depends on `organisation` CV)

---

## Execution plan (this session)

1. Fix Category A (7 global-attribute emissions) in pycmor source.
2. Fix Category B (branded_variable format transform).
3. Re-run pipeline; confirm counts.
4. File WCRP upstream issue for Category D organisation CV.
5. Park Category C pending AWI registration.
6. Decide polar cell + areacello (CF1, CF2).

## Out of scope (for now)

- AWI model registration (Category C) — separate track.
- WCRP organisation CV (Category D) — upstream.
