#!/bin/bash -l
#SBATCH --job-name=pycmor-controller  # <<< This is the main job, it will launch subjobs if you have Dask enabled.
#SBATCH --account=ab0995
#SBATCH --partition=compute
#SBATCH --nodes=1
###############################################################################
# Prefect settings:
#SBATCH --time=00:30:00                 # <<< You may need more time, adapt as needed!
export PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=True
export PREFECT_SERVER_API_HOST=0.0.0.0
# For more info about Prefect caching, see:
# https://docs-3.prefect.io/v3/develop/settings-ref#local-storage-path
export PREFECT_RESULTS_LOCAL_STORAGE_PATH=/scratch/a/${USER}/prefect
conda activate pycmor
prefect server start -b
time pycmor process sample.yaml
prefect server stop
