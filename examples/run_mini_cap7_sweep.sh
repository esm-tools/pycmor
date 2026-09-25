#!/bin/bash
#SBATCH --job-name=pycmor-mini-cap7
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --time=01:30:00
# %x will be set via -J at submission time

set -euo pipefail

# Args from sbatch --export:
#   COPY_N    1, 2, or 3
#   N_WORKERS dask worker count
#   MEM_LIMIT per-worker memory limit (e.g. "32GB")
#   TAG       short identifier used in job name + output dir
COPY_N=${COPY_N:?must set}
N_WORKERS=${N_WORKERS:?must set}
MEM_LIMIT=${MEM_LIMIT:?must set}
TAG=${TAG:?must set}

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
# Round-2 collapse: all pipeline steps run in ONE Prefect task per
# rule, avoiding inter-task dataset serialization (which fails with
# "Could not serialize object of type _HLGExprSequence" / "cannot
# pickle '_thread.lock' object" under parallel mode). Round-2 showed
# 1-sec diff in single-rule mode but the real win is here in parallel.
export PYCMOR_PREFECT_COLLAPSE=1

DATA_PATH=/work/ab0246/a270092/bench_copies/copy${COPY_N}
OUTROOT=/scratch/a/a270092/pycmor_mini_cap7_sweep
OUTDIR=$OUTROOT/${TAG}_copy${COPY_N}_${SLURM_JOB_ID}
mkdir -p "$OUTDIR"
command -v lfs >/dev/null && lfs setstripe -c 8 "$OUTDIR" 2>/dev/null || true

# Substitute the template
sed \
  -e "s|{{DATA_PATH}}|$DATA_PATH|g" \
  -e "s|{{N_WORKERS}}|$N_WORKERS|g" \
  -e "s|{{MEM_LIMIT}}|$MEM_LIMIT|g" \
  -e "s|{{TAG}}|$TAG|g" \
  -e "s|{{OUTPUT_DIR}}|$OUTDIR/cmorized|g" \
  examples/cmip7_bench_mini_cap7_template.yaml \
  > $PYCMOR_SCRATCH/bench.yaml

echo "=== mini-cap7 sweep ==="
echo "node: $(hostname), $(nproc) cores, $(free -g | awk '/^Mem:/{print $2}') GB RAM"
echo "config: TAG=$TAG COPY=$COPY_N WORKERS=$N_WORKERS MEM=$MEM_LIMIT (TPW=4)"
echo "data: $DATA_PATH ($(ls $DATA_PATH | wc -l) files)"
echo "out:  $OUTDIR"

# cgroup-v2 watchdog
WATCH_LOG=$OUTDIR/cgroup_mem_v2.tsv
JOB=${SLURM_JOB_ID:-$$}
CG_PATH=/sys/fs/cgroup/system.slice/slurmstepd.scope/job_$JOB/memory.current
(
  echo -e "epoch\tmem_GB"
  while true; do
    if [ -r "$CG_PATH" ]; then
      m=$(awk '{printf "%.2f", $1/1024/1024/1024}' "$CG_PATH" 2>/dev/null)
      [ -n "$m" ] && echo -e "$(date +%s)\t$m"
    fi
    sleep 5
  done
) > "$WATCH_LOG" &
WATCH_PID=$!
trap "kill $WATCH_PID 2>/dev/null || true" EXIT

echo "=== start ==="
date +%s.%N
/usr/bin/time -v pycmor process $PYCMOR_SCRATCH/bench.yaml || echo "PYCMOR EXIT $?"
date +%s.%N

echo "=== output summary ==="
find "$OUTDIR/cmorized" -name '*.nc' 2>/dev/null | wc -l
find "$OUTDIR/cmorized" -name '*.nc' -printf '%s\n' 2>/dev/null | awk '{s+=$1}END{printf "%.1f GB total\n", s/1e9}'

echo "=== cgroup peak ==="
awk -F'\t' 'NR>1 && $2>m{m=$2} END{printf "%.2f GB\n", m+0}' "$WATCH_LOG"
