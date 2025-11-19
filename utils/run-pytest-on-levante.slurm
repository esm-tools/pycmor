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

source $(conda info --base)/etc/profile.d/conda.sh
conda activate $CONDA_ENV_NAME
pytest -v tests/integration
