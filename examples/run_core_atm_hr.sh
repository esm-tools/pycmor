#!/bin/bash
#SBATCH --job-name=pycmor-core-atm-hr
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=256G
#SBATCH --time=04:00:00
#SBATCH --output=pycmor_core_atm_hr_%j.log
#SBATCH --error=pycmor_core_atm_hr_%j.log

# HR production yaml against HR_test_01 (TCo319, year 1586).

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

# HR has 10x the LR grid size; fewer, larger workers fit better.
# Also inject the blosc_zstd + threaded + larger time chunks knobs into
# the inherit: block so every rule uses them without yaml edits.
# (BitGroom-5 is already pycmor's default.)
sed -e 's/dask_cluster: "slurm"/dask_cluster: "local"/' \
    -e '/dask_cluster:/a\  dask_n_workers: 1' \
    -e 's|output_directory: .*|output_directory: ./cmorized_output/core_atm_hr|' \
    -e '/^inherit:/a\  netcdf_compression_codec: blosc_zstd\n  netcdf_compression_level: 3\n  netcdf_write_scheduler: threads' \
    awi-esm3-veg-hr-variables/core_atm/cmip7_awiesm3-veg-hr_atmos.yaml > $PYCMOR_SCRATCH/core_atm_hr.yaml

pycmor process $PYCMOR_SCRATCH/core_atm_hr.yaml
