#!/bin/bash
#SBATCH --job-name=pycmor-bench-tossq-2x
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --mem=256G
#SBATCH --time=00:30:00
#SBATCH --output=pycmor_bench_hr_tossq_2x_parallel_%j.log
#SBATCH --error=pycmor_bench_hr_tossq_2x_parallel_%j.log

# Diagnostic for "does pycmor actually fan out rules across workers on the
# same node when parallel=True + pipeline_orchestrator=dask".
# Single node, 128 cores, 256 GB. Two identical tossq_day rules.
# Expected outcomes:
#   wall ~= 1x single-rule (10:23 baseline) -> rule parallelism works
#   wall ~= 2x  -> rules still serialised somewhere; need deeper fix

set -euo pipefail

source ~/loadconda.sh
conda activate pycmor_py312

cd /work/ab0246/a270092/software/pycmor
export PYCMOR_HOME=/work/ab0246/a270092/software/pycmor

PYCMOR_SCRATCH=/scratch/a/a270092/pycmor_tmp/$$
mkdir -p $PYCMOR_SCRATCH/prefect/storage
export PREFECT_HOME=$PYCMOR_SCRATCH/prefect
export PREFECT_LOCAL_STORAGE_PATH=$PYCMOR_SCRATCH/prefect/storage
export TMPDIR=$PYCMOR_SCRATCH
export HDF5_USE_FILE_LOCKING=FALSE
export OMP_NUM_THREADS=1

OUTROOT=${OUTROOT:-/scratch/a/a270092/pycmor_bench_scale}
OUTDIR=$OUTROOT/tossq_2x_${SLURM_JOB_ID:-$$}
mkdir -p "$OUTDIR" "$OUTDIR"_b
command -v lfs >/dev/null && lfs setstripe -c 8 "$OUTDIR" 2>/dev/null || true

# Repoint output dirs into the per-job scratch location.
python3 - <<PY
import yaml
src = "examples/cmip7_bench_hr_tossq_day_2x_parallel.yaml"
y = yaml.safe_load(open(src))
y["inherit"]["output_directory"] = "${OUTDIR}"
# rule-level override for the second rule keeps separate output dir
for r in y["rules"]:
    if r["name"] == "tossq_day_b":
        r["output_directory"] = "${OUTDIR}_b"
yaml.safe_dump(y, open("$PYCMOR_SCRATCH/bench.yaml", "w"), sort_keys=False)
PY

echo "=== 2-rule parallel diagnostic ==="
echo "=== node $(hostname), $(nproc) cores, $(free -g | awk '/^Mem:/{print $2}') GB ==="
echo "=== Input file (each rule reads same SST, writes own output) ==="
ls -lh /work/bb1469/a270092/runtime/awiesm3-develop/Test_16n/outdata/fesom/sst.fesom.1587.nc

echo "=== Start pycmor process ==="
date +%s.%N
/usr/bin/time -v pycmor process $PYCMOR_SCRATCH/bench.yaml
date +%s.%N

echo "=== Output ==="
find "$OUTDIR" "$OUTDIR"_b -type f -printf '%s %p\n' | sort -n
