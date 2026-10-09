#!/bin/bash
#SBATCH --job-name=pycmor-bench-load-uas
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=256G
#SBATCH --time=00:30:00
#SBATCH --output=pycmor_bench_load_uas_%j.log
#SBATCH --error=pycmor_bench_load_uas_%j.log

set -euo pipefail
source ~/loadconda.sh
conda activate pycmor_py312
cd /work/ab0246/a270092/software/pycmor
export HDF5_USE_FILE_LOCKING=FALSE

# Load-only bench on uas_1hr (atmos_1h_pt_10u): source 8760 chunks
# (1, 421120) vs repacked 12 chunks (720, 421120). Three passes of
# each (alternating order) to characterise cold-vs-warm cache effects.

SRC=/work/bb1469/a270092/runtime/awiesm3-develop/Final_CMIP7_IO_Test_01/outdata/oifs/atmos_1h_pt_10u_1587-1587.nc
R720=$(ls /scratch/a/a270092/pycmor_repack_uas_720/*/atmos_1h_pt_10u_1587-1587_repacked720.nc 2>/dev/null | head -1)

echo "files:"
echo "  src:  $(ls -lh $SRC | awk '{print $5}')  $SRC"
echo "  r720: $(ls -lh $R720 | awk '{print $5}')  $R720"
echo ""

pass() {
  N=$1
  ORDER=$2
  echo "=== PASS $N (order: $ORDER) ==="
  for tag in $ORDER; do
    case $tag in
      src) FP=$SRC ;;
      r720) FP=$R720 ;;
    esac
    echo "--- $tag (pass $N) ---"
    python3 examples/bench_load_only.py "$FP" 10u
    echo ""
  done
}

# Alternate the order so the first read of each variant is cold-cache
# the first time it's seen this job.
pass 1 "src r720"
pass 2 "r720 src"
pass 3 "src r720"
