# Sanity-check reports

One subdirectory per experiment run. Each run includes:

- `<run>.md` — categorised markdown report (FAIL/WARN/PASS by severity)
- `<run>.jsonl` — raw per-file results (resume key for the walker)
- `<run>_html/` — multi-page HTML site:
  - `index.html` — overall summary + critical-issues callout
  - `atm.html` — atmosphere (atmos / atmosChem / aerosol)
  - `oce.html` — ocean
  - `ice.html` — sea ice + land ice
  - `veg.html` — land / vegetation

Within each domain page, variables are sorted **FAIL → WARN → PASS**,
with FAIL further sorted by severity
(DATA_INTEGRITY → PHYS_IMPOSSIBLE → UNIT_MISMATCH → SIGN_FLIP →
PICONTROL_NONZERO → BOUNDS_OR_PEAK → BOUNDS_TIGHT_MINOR).

To regenerate from a JSONL:

```bash
python tools/sanity_check/build_issues_md.py \
    --jsonl tools/sanity_check/reports/<run>.jsonl \
    --out   tools/sanity_check/reports/<run>.md \
    --label "<run-label>"

python tools/sanity_check/build_html_report.py \
    --jsonl   tools/sanity_check/reports/<run>.jsonl \
    --table   doc/sanity_check_ranges.md \
    --out-dir tools/sanity_check/reports/<run>_html \
    --label   "<run-label>"
```
