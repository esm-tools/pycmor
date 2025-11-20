#!/bin/bash -e
#SBATCH --job-name=pycmor-pytest-integration
#SBATCH --account=ab0995
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --time=02:00:00
###############################################################################
export CONDA_ENV_NAME=pycmor
export PYCMOR_USE_REAL_TEST_DATA=1
export PYCMOR_TEST_DATA_CACHE_DIR=/scratch/a/${USER}/pycmor/test_data

export PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=True
export PREFECT_SERVER_API_HOST=0.0.0.0
# For more info about Prefect caching, see:
# https://docs-3.prefect.io/v3/develop/settings-ref#local-storage-path
export PREFECT_RESULTS_LOCAL_STORAGE_PATH=/scratch/a/${USER}/prefect

source $(conda info --base)/etc/profile.d/conda.sh
conda activate $CONDA_ENV_NAME
echo "########################################################################"
pytest -v tests/unit
echo "########################################################################"
pytest -v tests/integration
echo "########################################################################"
