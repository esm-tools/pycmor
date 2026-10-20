#!/bin/bash
# Worker environment; Slurm resources come from the campaign YAML.
set -euo pipefail
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
campaign="${PYCMOR_LR_ROOT:-/work/ab0995/a270270/cmorization_LR}"
venv="${PYCMOR_LR_VENV:-$campaign/venv}"
cache="${PYCMOR_LR_CACHE:-$campaign/cache}"
export PATH="$venv/bin:$PATH"
export PYCMOR_LR_EXECUTABLE="$venv/bin/pycmor"
export PYCMOR_HOME="${PYCMOR_LR_REPO:-$(dirname "$script_dir")}"
export ESGVOC_HOME="$cache/esgvoc"
export XDG_CACHE_HOME="$cache/xdg"
export XDG_CONFIG_HOME="$cache/config"
# Keep SQLite and transient worker state off the shared work filesystem.
# Each invocation gets a private, node-local directory, including array tasks.
export TMPDIR
TMPDIR=$(mktemp -d "/tmp/cmor-lr-${SLURM_JOB_ID:-local}.XXXXXX")
export PREFECT_HOME="$TMPDIR/prefect"
export PREFECT_SERVER_ANALYTICS_ENABLED=false
export PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS=300
export NO_PROXY="${NO_PROXY:+$NO_PROXY,}127.0.0.1,localhost"
export no_proxy="$NO_PROXY"
export DASK_TEMPORARY_DIRECTORY="$TMPDIR"
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
mkdir -p "$TMPDIR" "$PREFECT_HOME"
echo "LR job ${SLURM_JOB_ID:-local}: node-local state $TMPDIR; Prefect startup limit 300 seconds"
cd "$campaign"
exec python "${PYCMOR_LR_RUNNER:-$PYCMOR_HOME/awi-esm3-lr-variables/run_recipe.py}" "$1"
