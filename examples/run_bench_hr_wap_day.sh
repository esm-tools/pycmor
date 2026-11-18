#!/bin/bash
#SBATCH --job-name=pycmor-bench-hr-wap-day
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=256G
#SBATCH --time=00:45:00
#SBATCH --output=pycmor_bench_hr_wap_day_%j.log
#SBATCH --error=pycmor_bench_hr_wap_day_%j.log

# Isolate wap_day (second-slowest real rule from HR job 24405290, 403 s).
# 9.1 GB input, comparable output, lazy_write=true → save_dataset is
# where all the work happens. This is the candidate for the
# "single-threaded zlib in scheduler=synchronous" write bottleneck.

source ~/loadconda.sh
conda activate pycmor_py312

cd /work/ab0246/a270092/software/pycmor

PYCMOR_SCRATCH=/scratch/a/a270092/pycmor_tmp/$$
mkdir -p $PYCMOR_SCRATCH/prefect/storage
export PREFECT_HOME=$PYCMOR_SCRATCH/prefect
export PREFECT_LOCAL_STORAGE_PATH=$PYCMOR_SCRATCH/prefect/storage
export TMPDIR=$PYCMOR_SCRATCH
export HDF5_USE_FILE_LOCKING=FALSE
export OMP_NUM_THREADS=1

OUTROOT=${OUTROOT:-/scratch/a/a270092/pycmor_bench}
mkdir -p "$OUTROOT"

sed -e "s|output_directory: .*|output_directory: $OUTROOT/bench_hr_wap_day|" \
    examples/cmip7_bench_hr_wap_day.yaml > $PYCMOR_SCRATCH/bench.yaml

echo "=== Input file ==="
ls -lh /work/bb1469/a270092/runtime/awiesm3-develop/HR_test_01/outdata/oifs/atm_remapped_1d_pl_cmip7_w_1d_pl_cmip7_1586-1586.nc

echo "=== Start pycmor process ==="
date +%s.%N
/usr/bin/time -v pycmor process $PYCMOR_SCRATCH/bench.yaml
date +%s.%N

echo "=== Output ==="
find "$OUTROOT/bench_hr_wap_day" -type f -printf '%s %p\n' | sort -n
