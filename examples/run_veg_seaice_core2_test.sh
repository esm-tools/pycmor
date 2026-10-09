#!/bin/bash
#SBATCH --job-name=pycmor-veg-seaice-core2-test
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=256G
#SBATCH --time=01:00:00
#SBATCH --output=pycmor_veg_seaice_core2_test_%j.log
#SBATCH --error=pycmor_veg_seaice_core2_test_%j.log

# Run pycmor process entirely on compute node (including Prefect server)

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

# Use local Dask cluster since we're already on a compute node
sed -e 's/dask_cluster: "slurm"/dask_cluster: "local"/' \
    -e '/dask_cluster:/a\  dask_n_workers: 4' \
    examples/cmip7_veg_seaice_core2_test.yaml > $PYCMOR_SCRATCH/veg_seaice_core2_test_local.yaml

pycmor process $PYCMOR_SCRATCH/veg_seaice_core2_test_local.yaml
