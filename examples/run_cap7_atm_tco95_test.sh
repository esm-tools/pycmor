#!/bin/bash
#SBATCH --job-name=pycmor-cap7-atm-tco95-test
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=256G
#SBATCH --time=08:00:00
#SBATCH --output=pycmor_cap7_atm_tco95_test_%j.log
#SBATCH --error=pycmor_cap7_atm_tco95_test_%j.log

# Run pycmor cap7_atm test on compute node
# 66 rules: daily/3hr/1hr/6hr/monthly cap7 atmosphere variables

source ~/loadconda.sh
conda activate pycmor_py312

cd /work/ab0246/a270092/software/pycmor

# Use scratch for Prefect DB and temp files to avoid $HOME quota and /tmp size limits
PYCMOR_SCRATCH=/scratch/a/a270092/pycmor_tmp/$$
mkdir -p $PYCMOR_SCRATCH/prefect/storage
export PREFECT_HOME=$PYCMOR_SCRATCH/prefect
export PREFECT_LOCAL_STORAGE_PATH=$PYCMOR_SCRATCH/prefect/storage
export TMPDIR=$PYCMOR_SCRATCH
export HDF5_USE_FILE_LOCKING=FALSE
export OMP_NUM_THREADS=1

# Use local Dask cluster with 4 workers (64 GB each)
sed -e 's/dask_cluster: "slurm"/dask_cluster: "local"/' \
    -e '/dask_cluster:/a\  dask_n_workers: 4' \
    examples/cmip7_cap7_atm_tco95_test.yaml > $PYCMOR_SCRATCH/cap7_atm_tco95_test_local.yaml

pycmor process $PYCMOR_SCRATCH/cap7_atm_tco95_test_local.yaml
