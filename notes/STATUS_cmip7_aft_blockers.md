# CMIP7 AFT blockers status — AWI-ESM3-4-2-veg-HR

Snapshot 2026-06-17. HR seaice end-to-end now passes at **cf high=0 medium=0, wcrp_cmip7 high=0 medium=0** against current esgvoc cmip7@1.2.6 / WCRP-universe / CMIP7-CVs. Publishable. Remaining items are upstream housekeeping.

## Already done

### IPO operational asks

- **JotForm centre-specific status update** — filed 2026-06-06.
- **EMD reviewer pool** — active reviewer with 30+ reviews.

### EMD work (`WCRP-CMIP/Essential-Model-Documentation`)

- **Model family `awi-esm3`** — EMD#500, merged 2026-06-01.
- **Component config `atmosphere_openifs-48r1_h107_v113`** — EMD#483, merged 2026-06-02.
- **Stage 1-2 grid layer** (h106/h107/h114 horizontal + v112/v113/v116 vertical + g122/g130/g132 cells) — merged.
- **Stage 4 model registration `AWI-ESM3-4-2-veg-HR`** — EMD#640 (auto-PR from #563), merged 2026-06-11 15:02 UTC by `@charliepascoe`.
- **EMD bot fix** — EMD#575 (`name | missing | Field required`), merged in time to unblock #563.

### CMIP7-CVs and esgvoc registry

- AWI institution thin entry (CMIP7-CVs#439) — merged.
- `AWI-ESM3-4-2-veg-HR` source entry — present in current esgvoc db (`cmip7@1.2.6`). The wcrp_cmip7 ATTR004 / FILE001 source_id checks pass. (PR#461 was the wrong path: targets `esgvoc_dev`, requires WCRP-Universe term first. Laurent Troussellier said he'd handle it; the entry is now live in the registry.)

### pycmor (this repo, `feat/cmip7-awiesm3-veg-hr`)

- All QC plumbing (cf + wcrp_cmip7 suites, per-shard step, cmip7repack -o pre-pass).
- All CV-compliant global attribute + DRS layout fixes (cli66/67).
- v1.2.2.2 → v1.2.2.3 → v1.2.2.4 metadata bumps swept across 52 configs.
- esgvoc 4.1.1 pinned in `[qc]` extras.
- **TIME001 fix** (commit `553c1c3c`): `time = midpoint(time_bnds)` with cftime calendar support, calendar promoted `standard`/`gregorian` → `proleptic_gregorian`, monthly-detection relaxed when `approx_interval` is unset. `set_time_bounds` step wired into DefaultPipeline and 111 custom pipelines across 17 HR yamls.
- **Mesh-prefer + float64 fix** (commit `fdf444f7`): when `rule.grid_file` is configured, drop the FESOM/XIOS-truncated `bounds_lat`/`bounds_lon` (degenerate line/thin-triangle polygons whose bbox doesn't contain the centroid) and pull the canonical 16-vertex polygons from `mesh.nc`. lat/lon promoted to float64 per CMIP cmor-tables spec (`type=double`). HR seaice cf §7.1 went from 22k+ outliers to 0.
- End-to-end HR seaice run: cf high=0 medium=0, wcrp_cmip7 high=0 medium=0.

### Checker work (`ESGF/cc-plugin-wcrp`)

- VAR005 aux-coord skip — merged in 2.2.0.
- VAR012 silent-skip-on-non-interval — merged in 2.3.0.
- VAR004 1-D polygon bounds — [cc-plugin-wcrp#38](https://github.com/ESGF/cc-plugin-wcrp/pull/38), waiting for the release that includes it; master fix is verified clean on HR.
- TIME003 sub-daily time-range tokens — [cc-plugin-wcrp#46](https://github.com/ESGF/cc-plugin-wcrp/pull/46), opened 2026-06-17 (regex only accepted 6 or 8 digits; CMIP7 DRS allows 4/6/8/10/12/14).

## In flight (waiting on others)

### cc-plugin-wcrp releases

- **PR#38** (VAR004 polygon bounds) — merged on master, awaiting release tag.
- **PR#46** (TIME003 12-digit tokens) — opened, awaiting review/merge.

Once a release containing both lands, install-from-pypi resumes; nothing needs to change in our configs.

### Upstream WCRP-universe registry data

- **WCRP-universe#190** — opened 2026-06-17. Several root `variable/*.json` entries carry a tile- or operation-specific `long_name` that belongs to a variant: `cveg` (Grass Tiles), `hurs` (Daily Minimum), `tas` (Daily Minimum), `mrsol`, `npp` (Grass), `ra` (Shrub), `rh` (Shrub), `gpp` (units bracketed in the name). Triggers `[ATTR004]` long_name registry expected-term failures on the ~18 affected branded files. Proposed fix: generalize the root term `long_name` and move per-variant text from `description` to `long_name` in `cveggrass`/`cvegshrub`/`cvegtree`/`hursmin`/`hursmax`/`tasmin`/`tasmax` etc.
- The cc-plugin-wcrp side has a sister bug: variable long_name lookup uses `variable_id.lower()` (the root) not the branded-variant id. After the registry side is settled, the plugin lookup needs to fall through to the variant. Flagged in #190; PR to follow.

### `time1` XIOS quirk

- One of the OIFS 6hr streams writes its time coordinate as `time1` instead of `time`. CMIP7 expects `time`. Separate fix on the pycmor writer side (rename in the OIFS load step). Not yet implemented; non-blocking for the seaice/ocean side that uses canonical `time`.

## QC score progression (HR seaice config, real run 2026-06-17)

| State | cf high | cf medium | wcrp_cmip7 high | wcrp_cmip7 medium |
|---|---|---|---|---|
| before TIME001 + mesh-prefer + float64 fixes | 0 | 1 (22k+ §7.1 outliers) | 3 | 0 |
| now | 0 | **0** | **0** | 0 |

## Blocker dependency tree

```
ESGF-ready for AWI-ESM3-4-2-veg-HR
├── cc-plugin-wcrp release containing PR#38 + PR#46  ← upstream packaging
└── WCRP-universe#190 long_name registry fix         ← upstream data
```

Two humans across two repos. Neither blocks publication (master-fix verified clean for PR#38; PR#46 only affects sub-daily 12-digit tokens; #190 affects ~18 land/ATM files with registry-mismatched long_name — Recommended severity).

## Next decision points

- If cc-plugin-wcrp PR#46 hasn't moved in a week, nudge.
- If WCRP-universe#190 hasn't been triaged in a week, ping the IPO via the CMIP7-CVs channel.
- LR/SR variant Stage 4 EMD submissions are out of scope per IPO email; revisit post-AFT.
