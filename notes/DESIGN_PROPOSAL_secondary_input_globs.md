# Design proposal: drop literal-glob `*_file:` in favor of `*_pattern:` form

## Context

R1 of [PLAN_cli_override_regressions.md](PLAN_cli_override_regressions.md)
added an `*` → `<year>` expansion inside `apply_overrides` so existing yaml
entries like

```yaml
aice_file: /work/.../outdata/fesom/a_ice.fesom.*.nc
```

still resolve to a real file after we removed `repoint_hr_year.py`. The
expansion is a workaround, not a fix: it recreates repoint's regex rewrite
inside the CLI layer instead of removing the underlying yaml smell.

This proposal: migrate all 10 affected entries to the regex-pattern form
that secondary inputs already use, and remove R1.

### Forcing function: 1700-year cmorization in multi-year chunks

Upcoming workload is to cmorize **1700 simulation years**, processed in
chunks of multiple years per pycmor run (not one-year-at-a-time). With
FESOM's typical one-file-per-year naming (`<var>.fesom.<year>.nc`,
verified at e.g.
`/work/ab0246/a270092/runtime/fesom-2.7/ice_strength/run_19600101-19601231/`
and the existing y1587 archive), each chunk needs `open_mfdataset` over
N files.

**R1's literal-glob form structurally cannot represent this.** It
requires `--year-start == --year-end` and raises `OverrideError`
otherwise. The 1700-year run hits that error on the first chunk that
spans more than one year — which is most of them. The migration is a
hard prerequisite for the upcoming workload, not a stylistic cleanup.

---

## Does globbing affect years? Yes — it locks single-year only

The R1 expansion takes `--year-start` and substitutes it for the literal
`*` in `*_file:` values. That's the **only** year handling these rules
get. Three properties fall out of that:

1. **Year is injected, not detected.** The expansion blindly writes the
   CLI year into the filename. There's no check that the resulting file
   actually exists; runtime is the first place a typo or missing file
   surfaces.

2. **Multi-year is structurally impossible.** `xr.open_dataset(literal_path)`
   takes one file. R1 raises `OverrideError` for `--year-start !=
   --year-end` because there's no way to expand a single literal `*` to
   multiple files inside a single string. Users hitting a multi-year
   range get a migration message — but that migration is exactly what
   this proposal does.

3. **`skip_input_year_filter` does nothing for `*_file:` consumers.** R2
   gates `_filter_files_by_year_range` at both call sites, but `*_file:`
   resolution doesn't go through either path — it's a literal `open_dataset`
   call. Centennial-forcing rules can't use a `*_file:` form.

Compare with the pattern form:

```yaml
aice_path:    /work/.../outdata/fesom
aice_pattern: a_ice\.fesom\..*\.nc
aice_variable: a_ice
```

- File list comes from the directory + regex.
- `filter_files_by_year_range` narrows by `year_start`/`year_end` —
  **range, not equality**.
- `open_mfdataset` handles 1+ files transparently.
- `skip_input_year_filter: true` opts out cleanly.

So globbing in `*_file:` form is a year-mangler in disguise. Removing it
removes a hidden coupling between yaml syntax and CLI semantics.

---

## Affected entries (audit)

```
$ grep -rn 'fesom\.\*\.nc' awi-esm3-veg-hr-variables/ | grep '_file:'
```

| Tier | Rule | Key(s) |
|---|---|---|
| `lrcs_seaice` | sispeed | `aice_file`, `vice_file`, V-component file |
| `lrcs_seaice` | sidmasstranx, sidmasstrany | `aice_file`, `vice_file` |
| `lrcs_seaice` | sistressave, sistressmax | `aice_file` (or similar) |
| `lrcs_seaice` | siflcondtop, sifb, sihc | (single fesom file each) |
| `lrcs_seaice` | simpeffconc | (single fesom file) |
| `lrcs_seaice` | sispeed_day | per-day equivalent |
| `core_ocean` | zostoga | (fesom 3D file) |

Exact key names per rule need a finer audit before migration. Static-mesh
keys (`grid_file`, `basin_mask_file`) and any `*_file:` value without a
literal `*` are unaffected.

---

## Migration mechanics

### Yaml side

For each entry:

```yaml
# before
aice_file: /work/.../outdata/fesom/a_ice.fesom.*.nc
aice_variable: a_ice
```

```yaml
# after
aice_path: /work/.../outdata/fesom
aice_pattern: a_ice\.fesom\..*\.nc
aice_variable: a_ice
```

The key triplet matches `_load_secondary_mf`'s convention
([custom_steps.py:2153](examples/custom_steps.py#L2153)).

### Step function side

For each custom step that reads a `*_file:` attribute:

```python
# before
ds = xr.open_dataset(rule.aice_file, use_cftime=True)
aice = ds[rule.get("aice_variable", "a_ice")]
```

```python
# after
aice = _load_secondary_mf(rule, "aice_path", "aice_pattern", "aice_variable")
```

`_load_secondary_mf` already:
- regex-matches files in the directory;
- year-filters via `filter_files_by_year_range` (with the
  `skip_input_year_filter` opt-out from R2);
- opens via `open_mfdataset` (handles 1+ files);
- renames `time_counter` → `time` if requested;
- drops residual XIOS time bounds vars;
- selects the variable by name or auto-picks.

Most call sites that read `*_file:` do those steps manually anyway —
this consolidates them.

### CLI override side

Remove R1 entirely:

- delete `_FESOM_FILE_RE` and `_expand_year_in_file_keys` from
  [overrides.py](src/pycmor/core/overrides.py);
- delete the `if ov.year_start == ov.year_end` / `else` block in
  `apply_overrides`;
- delete the R1-specific tests in
  [test_overrides.py](tests/unit/test_overrides.py).

R2's `skip_input_year_filter` plumbing stays — it serves the centennial-
forcing rules independent of this migration.

---

## Scope of changes

| Component | Change |
|---|---|
| Yamls in `awi-esm3-veg-hr-variables/` | ~10 rule entries across 2 tiers (lrcs_seaice + core_ocean) |
| `examples/custom_steps.py` | ~10 custom step functions edited to call `_load_secondary_mf` |
| `src/pycmor/core/overrides.py` | net deletion — `_expand_year_in_file_keys`, `_FESOM_FILE_RE`, multi-year `OverrideError`, the entire R1 block in `apply_overrides` |
| `tests/unit/test_overrides.py` | drop R1 tests; R2 tests stay |
| `PLAN_cli_override_regressions.md` | mark R1 superseded |

---

## Trade-offs vs the workaround

| | R1 workaround | Proposed migration |
|---|---|---|
| Multi-year support | **impossible (raises OverrideError)** — blocks the 1700-year chunked run | works via `open_mfdataset` |
| Year filter is range-aware | no (single year only) | yes |
| Centennial-forcing opt-out | not applicable | works via `skip_input_year_filter` |
| Year/path coupling lives in | apply_overrides regex | rule yaml + helper |
| Net code in CLI override layer | grew by ~40 lines | shrinks by ~40 lines |

---

## Recommendation

Do the migration. R1 was the right call as a hot-fix to unblock the y1587
single-year run, but the upcoming 1700-year chunked workload makes it a
blocker. The migration:

- unifies all secondary-input handling on one helper (`_load_secondary_mf`),
- removes a code path from the CLI override layer that had to know about
  FESOM filename conventions,
- shrinks `apply_overrides` by ~40 lines,
- enables multi-year ranges (the 1700-year chunked case),
- preserves R2's `skip_input_year_filter` semantics for centennial inputs.

There's no value in deferring. R1 stays only as long as nothing needs
multi-year secondary inputs.

---

## Open questions — resolved

### Q1: Per-rule key audit

To do during migration with
`grep -E '_file:.*fesom\.\*\.nc' awi-esm3-veg-hr-variables/`. No upfront
input needed.

### Q2: `open_mfdataset` smoke test — passed

Tested against
`/work/bb1469/a270092/runtime/awiesm3-develop/after_lpjg_spinup_work_01/outdata/fesom/a_ice.fesom.{1900..1903}.nc`:

| Call | Result | Time |
|---|---|---|
| `xr.open_dataset(one_file)` | `time=12, nod2=126858` | 176 ms |
| `xr.open_mfdataset([one_file])` | `time=12, nod2=126858` | 22 ms |
| `xr.open_mfdataset(four_files)` | `time=48, nod2=126858` (concatenated correctly) | 260 ms |

Single-file `open_mfdataset` is in fact **faster** than `open_dataset`
(lazy-load); multi-file concatenates correctly along `time`. No
behavior regression for the migration. The y1700 chunked workload —
the original concern — gets the right shape automatically.

### Q3: Promote `_load_secondary_mf` to `pycmor.std_lib`?

**No.** Audit of all callers
(`grep -rn '_load_secondary_mf' --include='*.py'`) shows every caller
is inside `examples/custom_steps.py` itself. No external consumers, no
yaml indirection that imports it from a stable path. User confirmed
`custom_steps.py` is user-owned and free to modify.

Keep it private. If a future project outside this codebase wants the
helper, that's the trigger to promote — premature now.
