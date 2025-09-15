#!/bin/bash -l
#SBATCH --job-name=pymor-controller  # <<< This is the main job, it will launch subjobs if you have Dask enabled.
#SBATCH --account=ab0246                # <<< Adapt this to your computing account!
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --time=00:30:00                 # <<< You may need more time, adapt as needed!
export PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=True
export PREFECT_SERVER_API_HOST=0.0.0.0
# For more info about Prefect caching, see:
# https://docs-3.prefect.io/v3/develop/settings-ref#local-storage-path
export PREFECT_RESULTS_LOCAL_STORAGE_PATH=/scratch/a/${USER}/prefect
# Load conda
module load python3
source $(conda info --base)/etc/profile.d/conda.sh
conda activate pymor
prefect server start -b
time pymor process units-example.yaml
