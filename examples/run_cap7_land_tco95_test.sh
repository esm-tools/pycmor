#!/bin/bash
#SBATCH --job-name=pycmor-cap7-land-tco95-test
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=256G
#SBATCH --time=04:00:00
#SBATCH --output=pycmor_cap7_land_tco95_test_%j.log
#SBATCH --error=pycmor_cap7_land_tco95_test_%j.log

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

sed -e 's/dask_cluster: "slurm"/dask_cluster: "local"/' \
    -e '/dask_cluster:/a\  dask_n_workers: 12' \
    examples/cmip7_cap7_land_tco95_test.yaml > $PYCMOR_SCRATCH/cap7_land_tco95_test_local.yaml

pycmor process $PYCMOR_SCRATCH/cap7_land_tco95_test_local.yaml
