#!/bin/bash
#SBATCH --job-name=pycmor-perfile-maps
#SBATCH --partition=compute
#SBATCH --account=ab0246
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --time=01:00:00
#SBATCH --output=pycmor_perfile_maps_%j.log
#SBATCH --error=pycmor_perfile_maps_%j.log

# Regenerate per-file map PNGs on a compute node.
# Login-node RLIMIT_NPROC + Lustre contention kills the worker pool when
# decompressing the 8-19 GB 1-hourly / model-level atmospheric files
# (cl, pfull, pr_1hr, rsus_1hr, rlus_1hr, hfls_1hr, hfss_1hr, etc.).
# On a compute node these process cleanly.

set -e

# Resume mode: build_maps.py skips PNGs that already exist on disk, so
# this only renders the 14-ish missing files unless the maps/ dir is
# wiped first. To force a fresh full regen, uncomment:
# rm -f /work/ab0246/a270092/software/pycmor/tools/sanity_check/reports/cli37_extraatm_512g_filecache_vec_html/assets/maps/*.png

source ~/loadconda.sh
conda activate pycmor_py312

# Pin BLOSC threads for fast zstd decompression of the big compressed
# files; OpenBLAS / OMP threads pinned to 1 per worker so the 64 cores
# go to the multiprocessing pool, not BLAS.
export BLOSC_NTHREADS=4
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

cd /work/ab0246/a270092/software/pycmor

python tools/sanity_check/build_maps.py \
    --jsonl tools/sanity_check/reports/cli37_extraatm_512g_filecache_vec.jsonl \
    --out-dir tools/sanity_check/reports/cli37_extraatm_512g_filecache_vec_html \
    --parallel 12

# Then on the login node:
#   python tools/sanity_check/build_html_report.py \
#       --jsonl tools/sanity_check/reports/cli37_extraatm_512g_filecache_vec.jsonl \
#       --out-dir tools/sanity_check/reports/cli37_extraatm_512g_filecache_vec_html
