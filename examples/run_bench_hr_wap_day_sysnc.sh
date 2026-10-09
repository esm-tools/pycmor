#!/bin/bash
#SBATCH --job-name=pycmor-bench-hr-wap-day-sysnc
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=256G
#SBATCH --time=00:30:00
#SBATCH --output=pycmor_bench_hr_wap_day_sysnc_%j.log
#SBATCH --error=pycmor_bench_hr_wap_day_sysnc_%j.log

# Same wap_day bench but running in pycmor_py312_ts, which uses the
# system (module-loaded) thread-safe HDF5 + full-codec libnetcdf.
# Modules loaded before pycmor starts so Python's dynamic loader
# picks up the system libs via LD_LIBRARY_PATH.

source ~/loadconda.sh
module --force purge 2>/dev/null
module load gcc/11.2.0-gcc-11.2.0
module load netcdf-c/4.9.3pre-gcc-11.2.0
# netcdf-c/4.9.3pre pulls hdf5-1.14.2 via its RPATHs; that build happens to
# also be threadsafe (verified with H5is_library_threadsafe).
export LD_LIBRARY_PATH=/sw/spack-levante/hdf5-1.14.2-gxhi2f/lib:$LD_LIBRARY_PATH
conda activate pycmor_py312

cd /work/ab0246/a270092/software/pycmor

PYCMOR_SCRATCH=/scratch/a/a270092/pycmor_tmp/$$
mkdir -p $PYCMOR_SCRATCH/prefect/storage
export PREFECT_HOME=$PYCMOR_SCRATCH/prefect
export PREFECT_LOCAL_STORAGE_PATH=$PYCMOR_SCRATCH/prefect/storage
export TMPDIR=$PYCMOR_SCRATCH
export HDF5_USE_FILE_LOCKING=FALSE
export OMP_NUM_THREADS=1
# Let blosc use all cores allocated to this task; default is 4.
export BLOSC_NTHREADS=16
# We isolate PREFECT_HOME per job, so ~/.prefect/profiles.toml timeouts
# don't apply. Inject them directly (see memory: project_prefect_boot_flakiness).
export PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS=120
export PREFECT_API_REQUEST_TIMEOUT=120
export PREFECT_API_DATABASE_TIMEOUT=60
export PREFECT_API_DATABASE_CONNECTION_TIMEOUT=30

OUTROOT=${OUTROOT:-/scratch/a/a270092/pycmor_bench_sysnc}
mkdir -p "$OUTROOT"

echo "=== HDF5/netCDF environment ==="
which nc-config
nc-config --version
nc-config --has-zstd
echo "HDF5_PLUGIN_PATH: $HDF5_PLUGIN_PATH"
python3 -c "
import netCDF4, h5py, ctypes, os
print('netCDF4:', netCDF4.__version__, 'libnetcdf:', netCDF4.__netcdf4libversion__)
print('h5py:', h5py.__version__, 'HDF5:', h5py.version.hdf5_version)
for lib_name in ('libhdf5.so',):
    try:
        lib = ctypes.CDLL(lib_name)
        flag = ctypes.c_int(0)
        lib.H5is_library_threadsafe(ctypes.byref(flag))
        print(f'{lib_name} H5is_library_threadsafe: flag={flag.value}')
    except Exception as e:
        print(f'{lib_name} probe failed: {e}')
"

sed -e "s|output_directory: .*|output_directory: $OUTROOT/bench_hr_wap_day_sysnc|" \
    examples/cmip7_bench_hr_wap_day.yaml > $PYCMOR_SCRATCH/bench.yaml

echo "=== Input file ==="
ls -lh /work/bb1469/a270092/runtime/awiesm3-develop/HR_test_01/outdata/oifs/atm_remapped_1d_pl_cmip7_w_1d_pl_cmip7_1586-1586.nc

echo "=== Start pycmor process ==="
date +%s.%N
/usr/bin/time -v pycmor process $PYCMOR_SCRATCH/bench.yaml
date +%s.%N

echo "=== Output ==="
find "$OUTROOT/bench_hr_wap_day_sysnc" -type f -printf '%s %p\n' | sort -n
