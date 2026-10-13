#!/bin/bash
#SBATCH --job-name=pycmor-sanity-walker
#SBATCH --partition=compute
#SBATCH --account=ab0246
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --time=01:00:00
#SBATCH --output=pycmor_sanity_walker_%j.log
#SBATCH --error=pycmor_sanity_walker_%j.log

# Run the sanity-check walker on a compute node. The login node hits
# RLIMIT_NPROC (OpenBLAS pthread_create failures) when --parallel > 4,
# and Lustre contention crashes the worker pool on the big 8-19 GB
# 1-hourly / model-level files (pr_1hr, cl_day, pfull, rsus_1hr, etc.).
# On a compute node these process cleanly.
#
# Usage:  sbatch tools/sanity_check/run_walker_compute.sh [--wipe]
#   --wipe   delete the existing JSONL before walking (full re-diagnose).
#            Without --wipe the walker resumes — only files NOT already
#            in the JSONL are read. Use --wipe after bounds-table edits
#            that affect many variables.

set -e

source ~/loadconda.sh
conda activate pycmor_py312

# Refresh esgvoc CV cache on this compute node before the walker
# imports esgvoc. A stale CV snapshot on the worker emits spurious
# HIGH ATTR004 / FILE001 findings even when the term IS in the
# upstream registry (cli69 30s-90s region case). Each `esgvoc use` is
# idempotent and roughly a second when already current. Soft-fail so
# an offline registry doesn't abort the walk; stale-cache findings
# are the signal that the refresh didn't take.
esgvoc use universe@latest 2>/dev/null || echo "esgvoc universe refresh failed (continuing with current cache)"
esgvoc use cmip7@latest 2>/dev/null || echo "esgvoc cmip7 refresh failed (continuing with current cache)"

# BLOSC threads for fast zstd decompression; BLAS/OMP threads pinned to
# 1 per worker so the 64 cores go to multiprocessing, not nested BLAS.
export BLOSC_NTHREADS=4
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

cd /work/ab0246/a270092/software/pycmor

ROOT=/scratch/a/a270092/pycmor_hr/cli37_extraatm_512g_filecache_vec/cmorized
JSONL=tools/sanity_check/reports/cli37_extraatm_512g_filecache_vec.jsonl
TABLE=doc/sanity_check_ranges.md

if [[ "${1:-}" == "--wipe" ]]; then
    if [[ -f "$JSONL" ]]; then
        cp "$JSONL" "${JSONL}.bak.$(date +%Y%m%d_%H%M%S)"
        echo "backed up existing JSONL to ${JSONL}.bak.*"
    fi
    rm -f "$JSONL"
    echo "wiped $JSONL — full re-walk"
fi

TIMEOUT="${PYCMOR_SANITY_TIMEOUT:-900}"
echo "=== walker: ROOT=$ROOT  JSONL=$JSONL  parallel=12  timeout=${TIMEOUT}s ==="
# 900s default — covers HR 1-hourly TCo319 files (~15 GB each) and 19-level
# daily 3D atm files (~6 GB each); the 180s upstream default is too short.
NPROC=12 python tools/sanity_check/sanity_check.py \
    --root "$ROOT" \
    --table "$TABLE" \
    --jsonl "$JSONL" \
    --parallel 12 \
    --timeout "$TIMEOUT"

echo ""
echo "=== status summary ==="
python -c "
import json
records = [json.loads(l) for l in open('$JSONL')]
from collections import Counter
c = Counter(r.get('status','?') for r in records)
print(f'Total: {len(records)}  by status:', dict(c))
"

# Then on the login node:
#   python tools/sanity_check/build_html_report.py \
#       --jsonl tools/sanity_check/reports/cli37_extraatm_512g_filecache_vec.jsonl \
#       --out-dir tools/sanity_check/reports/cli37_extraatm_512g_filecache_vec_html
