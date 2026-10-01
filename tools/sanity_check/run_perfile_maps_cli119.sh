#!/bin/bash
#SBATCH --job-name=pycmor-perfile-maps
#SBATCH --partition=compute
#SBATCH --account=ab0246
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --time=01:30:00
#SBATCH --output=pycmor_perfile_maps_%j.log
#SBATCH --error=pycmor_perfile_maps_%j.log

set -e

source ~/loadconda.sh
conda activate pycmor_py312

export BLOSC_NTHREADS=4
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

cd /work/ab0246/a270092/software/pycmor

python tools/sanity_check/build_maps.py \
    --jsonl tools/sanity_check/reports/cli119.jsonl \
    --out-dir tools/sanity_check/reports/cli119_html \
    --parallel 12
