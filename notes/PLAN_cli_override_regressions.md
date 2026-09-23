# Plan: Fix CLI-override regressions

Scope: only the regressions caused by `pycmor process` CLI overrides
replacing `repoint_hr_year.py`. Rule-level / pipeline failures that exist
independently of how the yaml was repointed are out of scope.

Two regressions identified mid-run.

> **R1 superseded by
> [DESIGN_PROPOSAL_secondary_input_globs.md](DESIGN_PROPOSAL_secondary_input_globs.md).**
> The literal-glob `*_file:` form has been removed entirely from the
> yamls; consumers now use the `*_path:` + `*_pattern:` triplet via
> `_load_secondary_mf`, which handles year ranges natively. The R1
> expansion logic and tests are deleted from `apply_overrides`. The
> section below is kept for historical context.
>
> **R2 remains in effect** — `skip_input_year_filter` opt-out is
> implemented at both `_filter_files_by_year_range` call sites.

---

## R1 — Literal `*` in `*_file:` values is no longer expanded to the year [SUPERSEDED]

### What broke

Yaml carries secondary-input attrs as **literal globs**:

```yaml
aice_file: /work/.../outdata/fesom/a_ice.fesom.*.nc
salt_file: /work/.../outdata/fesom/salt.fesom.*.nc
sgm22_file: /work/.../outdata/fesom/sgm22.fesom.*.nc
```

Custom steps consume these via `xr.open_dataset(rule.<key>_file)` —
treating the string as a **literal path**, not a glob. Pre-CLI,
`repoint_hr_year.py` rewrote `\.fesom\.\*\.nc$` → `\.fesom\.<year>\.nc$`
([repoint_hr_year.py:64-65](examples/repoint_hr_year.py#L64-L65)).

The new CLI's `--data-path` does anchored *prefix* substitution but does
**not** touch the trailing `*`. The literal `*` survives into runtime →
`FileNotFoundError: ....*.nc`.

### Affected rules

Source: y1587 mid-run snapshot 2026-05-07, slurm jobs in
`pycmor_hr_par_pycmor-hr-*-y1587_2473*.log` (most recent batch
24733517–24733545).

| Tier | Rules |
|---|---|
| `lrcs_seaice` | sispeed, sidmasstran[xy], sistressave, sistressmax, siflcondtop, sifb, sihc, simpeffconc, sispeed_day |
| `core_ocean`  | zostoga |

### Audit: scope is FESOM only

`grep -rn '_file:' awi-esm3-veg-hr-variables/` confirms two flavors of
`*_file:` in the yamls — year-varying `\.fesom\.\*\.nc` (the R1 target)
and static mesh references (`grid_file`, `basin_mask_file`) with no `*`
that the regex naturally excludes. **No OIFS / LPJ-GUESS `*_file:` keys
with `*` exist.** A future component using literal-path `*_file:` globs
would need this list extended; for now the FESOM-only regex is correct
and complete.

`xr.open_dataset(file)` is the only consumer of these values
(custom_steps.py uses `open_dataset`, not `open_mfdataset`, at lines 83,
104, 336, 590, 1294, 1335, 1425, 1456, 1568, etc.) — so single-year-only
expansion is the only sound mode and the multi-year `OverrideError` is
the right call.

### Fix: expand `*` in `*_file:` values inside `apply_overrides`

In [src/pycmor/core/overrides.py](src/pycmor/core/overrides.py), after the
`year_start` / `year_end` block, add a pass that:

1. Triggers only when **both** year flags are set and **equal**.
2. Walks `cfg["rules"]` and `cfg["inherit"]`.
3. For any key matching `^[a-z0-9_]+_file$`, looks at the string value.
4. If the value matches `\.fesom\.\*\.nc$`, substitutes
   `\.fesom\.<year>\.nc$`.
5. If `year_start != year_end` and a `*_file:` value contains a literal
   `*`, raise `OverrideError("multi-year range cannot expand literal '*' in <key>; convert to a *_pattern: glob and let _load_secondary_mf year-filter")`.

This mirrors repoint's existing logic, scoped to the CLI-override layer.

```python
# overrides.py — sketch
_FESOM_FILE_RE = re.compile(r"\.fesom\.\*\.nc$")

def _expand_year_in_file_keys(rule_or_inherit: dict, year: int) -> None:
    for k, v in list(rule_or_inherit.items()):
        if isinstance(v, str) and k.endswith("_file") and _FESOM_FILE_RE.search(v):
            rule_or_inherit[k] = _FESOM_FILE_RE.sub(f".fesom.{year}.nc", v)

# inside apply_overrides, AFTER the year_start/year_end loop.
# _expand_year_in_file_keys mutates the dict it receives — safe because
# apply_overrides operates on the per-rule and inherit shallow copies it
# created earlier; the caller's input dict is unaffected.
if ov.year_start is not None and ov.year_end is not None:
    if ov.year_start == ov.year_end:
        for rule in rules:
            _expand_year_in_file_keys(rule, ov.year_start)
        _expand_year_in_file_keys(inherit, ov.year_start)
    else:
        # multi-year + literal '*' is unrepresentable — fail loudly
        for rule in rules + [inherit]:
            for k, v in rule.items():
                if k.endswith("_file") and isinstance(v, str) and "*" in v:
                    raise OverrideError(
                        f"--year-start != --year-end cannot expand literal '*' "
                        f"in {k}={v!r}. Migrate this entry from "
                        f"`{k}: /path/foo.fesom.*.nc` (literal-path form, "
                        "consumed by xr.open_dataset) to "
                        f"`{k.replace('_file', '_pattern')}: foo\\.fesom\\..*\\.nc` "
                        f"plus matching `{k.replace('_file', '_path')}: /path` "
                        "(regex form, consumed by _load_secondary_mf which "
                        "year-filters via filter_files_by_year_range)."
                    )
```

### Tests

- Single-year: yaml with `aice_file: .../a_ice.fesom.*.nc`, override
  `--year-start=1587 --year-end=1587` → value becomes
  `.../a_ice.fesom.1587.nc`.
- Multi-year + literal `*` raises `OverrideError`.
- Non-`_file` keys with `*` in them are untouched.
- **Regression guard: regex `pattern:` values are bytewise unchanged.**
  Yaml carries `inputs: [{path: /p, pattern: a_ice\.fesom\..*\.nc}]`;
  after `apply_overrides` with `--year-start=--year-end=1587`,
  `cfg["rules"][0]["inputs"][0]["pattern"]` must equal the input
  bytewise. This is what guards against silently double-rewriting a
  yaml when both `*_file:` and `pattern:` exist on the same rule.

### Out-of-scope: yaml conversion to `*_pattern`

The plan from review round 2 (§3 outlier note in
[PLAN_cli_overrides.md](PLAN_cli_overrides.md)) flagged converting these
`*_file:` literals to `*_pattern:` glob form so they flow through
`_load_secondary_mf` and naturally pick up the year filter. That removes
the need for the override-time expansion entirely. Recommended as a
separate cleanup pass; not blocking R1.

---

## R2 — Centennial input4MIPs forcing files filtered out

### What broke

cap7_aerosol rules (cfc11, cfc12, ch4, n2o) read input4MIPs forcing files
named `..._1750-2022.nc`. With CLI `--year-start 1587 --year-end 1587`,
[`_filter_files_by_year_range`](src/pycmor/core/gather_inputs.py#L281)
applies its overlap test:

```python
if file_start <= year_end and file_end >= year_start:
```

`1750 <= 1587` is `False` → file dropped → no inputs → rule fails.

### Why this is CLI-override-caused (and *not* a rule bug)

Pre-CLI, `repoint_hr_year.py` injected `year:` into the inherit block,
NOT `year_start`/`year_end`. The filter at
[gather_inputs.py:382](src/pycmor/core/gather_inputs.py#L382) only fires
when both `year_start` and `year_end` are set, so it never ran for these
rules. Files passed through; downstream `select_year` extrapolated 1587
from the 1750-2022 series.

The new CLI sets `year_start`/`year_end` on every rule (as the round-1
review correctly required for per-rule precedence). That triggers the
filter for *all* rules — including those whose inputs span outside the
target year by design.

### User position (from the status snapshot)

> ...that was a correctness improvement by the new CLI, not really a
> regression. But the user-facing outcome is "more failures."

Acknowledged: dropping centennial-extrapolated zeros is arguably correct.
But four rules now fail loudly that previously produced (incorrect) output.

### Recommended fix (rule-level, deferred)

Add an opt-out attribute consumed at **both** call sites of the year
filter — `_filter_files_by_year_range` is invoked from two paths:

1. Primary input gather:
   [gather_inputs.py:396-400](src/pycmor/core/gather_inputs.py#L396-L400)
   (was 380-384 pre-rebase) — gates with `year_start is not None and year_end is not None`.
2. Secondary input gather: `_load_secondary_mf` in
   [examples/custom_steps.py](examples/custom_steps.py) calls the public
   wrapper `filter_files_by_year_range` (added in
   [PLAN_cli_overrides.md](PLAN_cli_overrides.md) §3), which itself
   calls `_filter_files_by_year_range` at
   [gather_inputs.py:332](src/pycmor/core/gather_inputs.py#L332). R2
   extends that year filter with a `skip_input_year_filter` opt-out.

Putting the opt-out check **only** at site 1 leaves site 2 silently
filtering, which would surprise a future maintainer. Two options:

**Option A (preferred): gate at both call sites.** Add the
`skip_input_year_filter` check at gather_inputs.py:396 (primary) AND in
`_load_secondary_mf` before it calls `filter_files_by_year_range`
(secondary). Keep the public wrapper a dumb utility — no rule-state
inside it.

**Option B**: extend the public wrapper's signature
(`filter_files_by_year_range(files, year_start, year_end, *, rule=None)`)
and short-circuit when `rule.get("skip_input_year_filter")`. Centralizes
the policy but couples the utility to rule semantics.

Plan goes with **Option A**. The yaml side is unchanged:

```yaml
# in cap7_aerosol cfc11/cfc12/ch4/n2o rules:
skip_input_year_filter: true
```

```python
# gather_inputs.py — primary
if (
    year_start is not None
    and year_end is not None
    and not rule_spec.get("skip_input_year_filter", False)
):
    all_files = _filter_files_by_year_range(...)

# custom_steps.py — _load_secondary_mf
if (
    year_start is not None
    and year_end is not None
    and not rule.get("skip_input_year_filter", False)
):
    files = filter_files_by_year_range(files, year_start, year_end)
```

The CLI override layer does NOT need to learn about this — the rule
yamls carry the opt-out. This keeps `apply_overrides` agnostic to which
rules need centennial inputs.

**Caveat**: the opt-out is rule-wide, not per-input. Fine for the four
cap7_aerosol rules (each reads one centennial forcing into a single
input). If a future rule mixes year-bound and centennial inputs, the
opt-out would skip filtering for both — at that point a per-input
attribute would be needed.

### Why deferred from this pass

The user's directive: "totally ignore rule-based failures." The yaml
attribute lives in the rule, not in the CLI override layer. The single
line of plumbing in `gather_inputs.py` is small enough that it could be
added now or as part of the rule-level fix-up.

**Decision**: include the `skip_input_year_filter` plumbing in
`gather_inputs.py` (1-line change) so the yaml fix becomes a one-liner
per affected rule. Yaml edits themselves are out of scope.

### Tests

- Primary path: rule with `year_start=year_end=1587` and
  `skip_input_year_filter: true` → `_filter_files_by_year_range` not
  called from `gather_inputs.load_mfdataset`, all files returned.
- Same rule without the opt-out → filter applied (existing behavior).
- Secondary path: `_load_secondary_mf` called with rule that has
  `skip_input_year_filter: true` returns all matched files; without it
  the filter narrows to the year range.

---

## Other items in the status snapshot — explicitly out of scope

The user flagged these but classified them as rule/recipe failures, not
CLI-control issues:

- D regrid time-counter / KilledWorker on `rlds`/`rsds`/`rlus`/`rsus_seaice`
- G `vertical_integrate` units for `hfx_int_day`
- `sltbasin`/`hfbasin` shape mismatch
- `sfdsi`/`sfdsi_seaice` 12-vs-7 cadence
- `tas_1hr`, `hfls_1hr` KilledWorker (suspected flaky)

Not addressed here.

---

## Implementation order

1. R1 fix in `apply_overrides` + 3 tests. Self-contained.
2. R2 plumbing in `gather_inputs.py` + 2 tests. Self-contained.
3. Commit each separately — R1 is the user-blocking regression; R2 is
   the enabler for a yaml-level rule fix that the user can apply
   independently.
