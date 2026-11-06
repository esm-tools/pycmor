#!/usr/bin/env bash
# Build wheels only. Installation is an explicit subsequent step with no workers active.
# Usage: build-native-io.sh EXTERNAL_WORKSPACE THREADSAFE_HDF5_PREFIX PYTHON
set -euo pipefail
cmor_workspace="$(realpath "$1")"
cmor_hdf5="$(realpath "$2")"
cmor_python="$3"
cmor_prefix="$cmor_workspace/software/netcdf-c-4.9.3-threadsafe"
cmor_build="$(mktemp -d "${TMPDIR:-/tmp}/cmor-native-io.XXXXXX")"
export HDF5_DIR="$cmor_hdf5" HDF5_INCDIR="$cmor_hdf5/include" HDF5_LIBDIR="$cmor_hdf5/lib"
export NETCDF4_DIR="$cmor_prefix" USE_NCCONFIG=1
export CPPFLAGS="-I$cmor_hdf5/include"
export LDFLAGS="-L$cmor_hdf5/lib -Wl,-rpath,$cmor_hdf5/lib -Wl,-rpath,$cmor_prefix/lib"
export LD_LIBRARY_PATH="$cmor_hdf5/lib:$cmor_prefix/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export HDF5_PLUGIN_PATH="$cmor_prefix/lib/plugin"
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
"$cmor_python" - "$cmor_hdf5" <<'PY'
import ctypes,sys
from pathlib import Path
p=Path(sys.argv[1])/'lib/libhdf5.so'
lib=ctypes.CDLL(str(p));flag=ctypes.c_uint()
assert lib.H5is_library_threadsafe(ctypes.byref(flag))==0 and flag.value==1, 'HDF5 must be thread safe'
print('Verified thread-safe HDF5:',p,flush=True)
PY
if [[ ! -f "$cmor_prefix/.cmor_build_complete" ]]; then
  tar -xzf "$cmor_workspace/software/sources/netcdf-c-4.9.3.tar.gz" -C "$cmor_build"
  cd "$cmor_build/netcdf-c-4.9.3"
  ./configure --prefix="$cmor_prefix" --disable-dap --disable-byterange --disable-libxml2 --disable-nczarr --disable-static
  make -j "${SLURM_CPUS_PER_TASK:-2}"
  make install
  "$cmor_prefix/bin/nc-config" --all
  touch "$cmor_prefix/.cmor_build_complete"
fi
mkdir -p "$cmor_workspace/software/wheels"
unset HDF5_INCDIR HDF5_LIBDIR
"$cmor_python" -m pip wheel --no-index --no-deps --no-build-isolation \
  "$cmor_workspace/software/sources/h5py-3.16.0.tar.gz" \
  "$cmor_workspace/software/sources/netcdf4-1.7.4.tar.gz" \
  -w "$cmor_workspace/software/wheels"
