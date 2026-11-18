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

set -e

source ~/loadconda.sh
conda activate pycmor_py312

export BLOSC_NTHREADS=4
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

cd /work/ab0246/a270092/software/pycmor

ROOT=/scratch/a/a270092/pycmor_hr/cli50_test_v342_gr/cmorized
JSONL=tools/sanity_check/reports/cli50_test_v342_gr.jsonl
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
