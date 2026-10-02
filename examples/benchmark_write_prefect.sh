#!/bin/bash
#SBATCH --job-name=pycmor-bench-write
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=256G
#SBATCH --time=02:00:00
#SBATCH --output=pycmor_bench_write_%j.log
#SBATCH --error=pycmor_bench_write_%j.log

# Benchmark write throughput: raw vs dask-streaming vs dask-rechunk vs prefect
# Writes a 41 GB 3D model-level field with each approach and measures throughput.

source ~/loadconda.sh
conda activate pycmor_py312

cd /work/ab0246/a270092/software/pycmor

export TMPDIR=/scratch/a/a270092/pycmor_tmp/bench_$$
mkdir -p $TMPDIR
export PREFECT_HOME=$TMPDIR/prefect
mkdir -p $PREFECT_HOME/storage
export PREFECT_LOCAL_STORAGE_PATH=$PREFECT_HOME/storage
export HDF5_USE_FILE_LOCKING=FALSE
export OMP_NUM_THREADS=1

python examples/benchmark_write_prefect.py
