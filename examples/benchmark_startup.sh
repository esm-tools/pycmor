#!/bin/bash
#SBATCH --job-name=pycmor-bench-startup
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=256G
#SBATCH --time=00:30:00
#SBATCH --output=pycmor_bench_startup_%j.log
#SBATCH --error=pycmor_bench_startup_%j.log

# Benchmark every step from job start to first rule processing.
# Measures: conda activation, Python import, config parsing, Dask cluster,
# Prefect server, and first rule execution.

echo "$(date +%T.%3N) | SLURM job started"

echo "$(date +%T.%3N) | Loading conda..."
source ~/loadconda.sh
echo "$(date +%T.%3N) | Conda loaded"

echo "$(date +%T.%3N) | Activating environment..."
conda activate pycmor_py312
echo "$(date +%T.%3N) | Environment active"

cd /work/ab0246/a270092/software/pycmor

PYCMOR_SCRATCH=/scratch/a/a270092/pycmor_tmp/bench_$$
mkdir -p $PYCMOR_SCRATCH/prefect/storage
export PREFECT_HOME=$PYCMOR_SCRATCH/prefect
export PREFECT_LOCAL_STORAGE_PATH=$PYCMOR_SCRATCH/prefect/storage
export TMPDIR=$PYCMOR_SCRATCH
export HDF5_USE_FILE_LOCKING=FALSE
export OMP_NUM_THREADS=1

# Prepare config before Python uses it
sed -e 's/dask_cluster: "slurm"/dask_cluster: "local"/' \
    -e '/dask_cluster:/a\  dask_n_workers: 4' \
    examples/cmip7_cap7_atm_tco95_test.yaml > $PYCMOR_SCRATCH/cap7_atm_test_local.yaml

echo "$(date +%T.%3N) | Starting Python import benchmark..."

python3 -c "
import time
t0 = time.time()

print(f'{time.time()-t0:6.2f}s | Importing standard library...')
import os, sys, logging

t1 = time.time()
print(f'{time.time()-t0:6.2f}s | Importing numpy...')
import numpy as np

print(f'{time.time()-t0:6.2f}s | Importing xarray...')
import xarray as xr

print(f'{time.time()-t0:6.2f}s | Importing dask...')
import dask
import dask.distributed

print(f'{time.time()-t0:6.2f}s | Importing prefect...')
import prefect

print(f'{time.time()-t0:6.2f}s | Importing pycmor...')
import pycmor
from pycmor.core.cmorizer import CMORizer

print(f'{time.time()-t0:6.2f}s | All imports done')

print(f'{time.time()-t0:6.2f}s | Loading config...')
import yaml
with open('$PYCMOR_SCRATCH/cap7_atm_test_local.yaml') as f:
    config = yaml.safe_load(f)
print(f'{time.time()-t0:6.2f}s | Config loaded ({len(config.get(\"rules\", []))} rules)')

print(f'{time.time()-t0:6.2f}s | Creating CMORizer...')
cmorizer = CMORizer.from_dict(config)
print(f'{time.time()-t0:6.2f}s | CMORizer created')

print(f'{time.time()-t0:6.2f}s | Starting Dask local cluster (4 workers)...')
from dask.distributed import LocalCluster, Client
cluster = LocalCluster(n_workers=4, threads_per_worker=64, memory_limit='64GB')
client = Client(cluster)
print(f'{time.time()-t0:6.2f}s | Dask cluster ready: {cluster.scheduler_address}')

print(f'{time.time()-t0:6.2f}s | Starting Prefect temporary server...')
# Just test the import and basic setup, not a full server
from prefect import flow, task
@task
def dummy_task():
    return 42
@flow
def dummy_flow():
    return dummy_task()
result = dummy_flow()
print(f'{time.time()-t0:6.2f}s | Prefect flow executed (result={result})')

print(f'{time.time()-t0:6.2f}s | Test: opening a NetCDF file...')
ds = xr.open_dataset('/work/bb1469/a270092/runtime/awiesm3-develop/cmip7_output_006/outdata/oifs/atmos_day_cap7_hfls_day_cap7_1900-1900.nc')
print(f'{time.time()-t0:6.2f}s | File opened: {dict(ds.sizes)}')
ds.close()

print(f'{time.time()-t0:6.2f}s | Test: opening a large 3D file (lazy)...')
ds = xr.open_dataset('/work/bb1469/a270092/runtime/awiesm3-develop/cmip7_output_006/outdata/oifs/atmos_6h_ml_ta_6h_ml_1900-1900.nc')
print(f'{time.time()-t0:6.2f}s | 3D file opened: {dict(ds.sizes)}, {ds[\"ta\"].nbytes/1e9:.1f} GB')
ds.close()

client.close()
cluster.close()
print(f'{time.time()-t0:6.2f}s | Done. Total: {time.time()-t0:.1f}s')
"

echo "$(date +%T.%3N) | Python finished"
