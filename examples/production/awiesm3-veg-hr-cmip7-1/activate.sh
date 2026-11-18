#!/bin/bash
# conda activate hook of the awiesm3-veg-hr-cmip7-1 production env, installed
# as etc/conda/activate.d/pycmor-production.sh. Everything a shard needs is set
# here, so nothing depends on the modules or the ~/.prefect profile of whoever
# runs the campaign. Mirrors the hook of the env that cmorized the validated
# year (pycmor_py312, 10_pycmor_sys_libs.sh).

# h5py and netCDF4 are built against these (source_builds in pins.yaml):
# DKRZ's thread-safe HDF5 1.14.2 and libnetcdf 4.9.3pre with zstd and blosc.
export LD_LIBRARY_PATH="/sw/spack-levante/hdf5-1.14.2-gxhi2f/lib:/sw/spack-levante/netcdf-c-main-bdxvs5/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
# Set, not prepended: filter plugins from other HDF5 builds (the netcdf-c
# module puts some on this path) load a second libhdf5 and crash the shard.
export HDF5_PLUGIN_PATH="/sw/spack-levante/netcdf-c-main-bdxvs5/plugins"

# HDF5 file locks on Lustre stall parallel writers.
export HDF5_USE_FILE_LOCKING=FALSE

# Let blosc use the cores of the job (default 4).
export BLOSC_NTHREADS=16

# The ephemeral Prefect server needs more than the default 20 s to boot on a
# cold Lustre start.
export PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=true
export PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS=120
export PREFECT_API_REQUEST_TIMEOUT=120
export PREFECT_API_DATABASE_TIMEOUT=60
export PREFECT_API_DATABASE_CONNECTION_TIMEOUT=30
