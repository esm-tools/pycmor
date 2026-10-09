#!/bin/bash
#SBATCH --job-name=pycmor-hr
#SBATCH --partition=compute
#SBATCH --account=ab0246
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=256G
#SBATCH --time=04:00:00
#SBATCH --output=pycmor_hr_%x_%j.log
#SBATCH --error=pycmor_hr_%x_%j.log

# Generic HR runner: takes an absolute yaml path as $1 and an optional
# output subdir name as $2 (default: derived from the yaml's parent dir).
# All 17 HR production yamls share the same compute-node topology
# (TCo319, 256 GB, 16 cores, 4 h wall); only the yaml + output dir change.
#
# Resubmit isolation: by default each run goes to <OUTROOT>/<tier>/. If you
# want per-attempt isolation (so re-running a tier doesn't mix output with
# a prior failed attempt), set ATTEMPT_SUBDIR=1 and the run will write to
# <OUTROOT>/<tier>/job_${SLURM_JOB_ID}/ instead. The latest-attempt
# symlink is updated to point at the most recent attempt.

set -euo pipefail

YAML="${1:?usage: sbatch run_hr_yaml.sh <yaml-path> [output-subdir]}"
OUTSUB="${2:-$(basename "$(dirname "$YAML")")}"

source /home/a/a270092/loadconda.sh
conda activate pycmor_py312

cd /work/bb1469/a270089/CMOR_OUTPUT/pycmor
export PYCMOR_HOME=/work/bb1469/a270089/CMOR_OUTPUT/pycmor

PYCMOR_SCRATCH=/scratch/a/a270089/pycmor_tmp/$$
mkdir -p $PYCMOR_SCRATCH/prefect/storage
export PREFECT_HOME=$PYCMOR_SCRATCH/prefect
export PREFECT_LOCAL_STORAGE_PATH=$PYCMOR_SCRATCH/prefect/storage
export TMPDIR=$PYCMOR_SCRATCH
export OMP_NUM_THREADS=1

# Use /scratch for outputs (fast). Setstripe 8 for parallel I/O if lfs exists.
OUTROOT=${OUTROOT:-/scratch/a/a270089/pycmor_hr_out}
ATTEMPT_SUBDIR=${ATTEMPT_SUBDIR:-0}
if [ "$ATTEMPT_SUBDIR" = "1" ] && [ -n "${SLURM_JOB_ID:-}" ]; then
    OUTDIR=$OUTROOT/$OUTSUB/job_$SLURM_JOB_ID
    LATEST_LINK=$OUTROOT/$OUTSUB/latest
    mkdir -p "$OUTDIR"
    ln -sfn "job_$SLURM_JOB_ID" "$LATEST_LINK"
else
    OUTDIR=$OUTROOT/$OUTSUB
    mkdir -p "$OUTDIR"
fi
command -v lfs >/dev/null && lfs setstripe -c 8 "$OUTDIR" 2>/dev/null || true

# Override yaml: use local dask cluster on this compute node (slurm dispatch
# adds overhead we don't want), 1 worker + blosc internal threads, and write
# to our target output dir.
sed -e 's/dask_cluster: "slurm"/dask_cluster: "local"/' \
    -e 's|output_directory: .*|output_directory: '"$OUTDIR"'|' \
    "$YAML" > $PYCMOR_SCRATCH/hr.yaml

echo "=== yaml: $YAML  ->  out: $OUTDIR ==="
pycmor process $PYCMOR_SCRATCH/hr.yaml
