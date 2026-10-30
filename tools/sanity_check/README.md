# Output sanity checker

Walks a CMORized output tree and compares each variable's global
min/mean/max against literature bounds in
[`doc/sanity_check_ranges.md`](../../doc/sanity_check_ranges.md).

## Files

- `sanity_check.py` — walker. Driver mode walks all files; worker mode
  (subprocess-per-file) reads one file with chunked netCDF4, masks
  fill-values, and emits a JSON record. Per-file timeout, streaming JSONL
  output, resume-on-restart.
- `sanity_summary.py` — prints a categorised text summary of a JSONL.
- `build_issues_md.py` — generates a Markdown issues report grouped by
  severity (data-integrity, physical-impossibility, unit-mismatch, sign-flip,
  piControl-residual, bounds-too-tight).

## Quickstart

```bash
# Walk an experiment's cmorized output (parallel = NPROC, default 8)
NPROC=12 python tools/sanity_check/sanity_check.py \
    --root /scratch/.../Test_16n_y1587/cmorized \
    --timeout 600

# Summarise on the terminal
python tools/sanity_check/sanity_summary.py

# Generate a markdown report at <repo>/issues_sanity.md
python tools/sanity_check/build_issues_md.py
```

## Knobs

- `--root DIR` — cmorized root (also `PYCMOR_SANITY_ROOT`)
- `--table FILE` — sanity_check_ranges.md (also `PYCMOR_SANITY_TABLE`)
- `--jsonl FILE` — results path (also `PYCMOR_SANITY_JSONL`)
- `--timeout SEC` — kill a stuck file after this many seconds
- `BLOSC_NTHREADS` env var — set to 4–8 for big blosc_zstd 4D files

The walker auto-skips files already in the JSONL, so killing and
re-running picks up where it left off. To retry a file, delete its line
from the JSONL.
