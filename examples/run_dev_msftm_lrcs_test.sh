#!/bin/bash
#SBATCH --job-name=pycmor-dev-msftm-lrcs
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=00:20:00
#SBATCH --output=pycmor_dev_msftm_lrcs_%j.log
#SBATCH --error=pycmor_dev_msftm_lrcs_%j.log

# Focused iteration loop for the three MOC streamfunction custom steps
# (compute_msftm_density / compute_msftmmpa_depth / compute_msftmmpa_density).
# Runs the 3-rule dev yaml against LR_test_01 CORE2 output in a few minutes.

source ~/loadconda.sh
conda activate pycmor_py312

cd /work/ab0246/a270092/software/pycmor

PYCMOR_SCRATCH=/scratch/a/a270092/pycmor_tmp/$$
mkdir -p $PYCMOR_SCRATCH/prefect/storage
export PREFECT_HOME=$PYCMOR_SCRATCH/prefect
export PREFECT_LOCAL_STORAGE_PATH=$PYCMOR_SCRATCH/prefect/storage
export TMPDIR=$PYCMOR_SCRATCH
export OMP_NUM_THREADS=1

OUTROOT=${OUTROOT:-/scratch/a/a270092/pycmor_dev_msftm}
mkdir -p "$OUTROOT"
command -v lfs >/dev/null && lfs setstripe -c 4 "$OUTROOT" 2>/dev/null || true

sed -e "s|output_directory: .*|output_directory: $OUTROOT|" \
    examples/cmip7_dev_msftm_lrcs_test.yaml > $PYCMOR_SCRATCH/dev_msftm.yaml

pycmor process $PYCMOR_SCRATCH/dev_msftm.yaml

echo "=== outputs ==="
find "$OUTROOT" -name '*.nc' -printf '%s %p\n' | sort -n
