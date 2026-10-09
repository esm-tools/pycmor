#!/bin/bash
#SBATCH --job-name=pycmor-bench-load
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=256G
#SBATCH --time=00:30:00
#SBATCH --output=pycmor_bench_load_only_%j.log
#SBATCH --error=pycmor_bench_load_only_%j.log

set -euo pipefail
source ~/loadconda.sh
conda activate pycmor_py312
cd /work/ab0246/a270092/software/pycmor
export HDF5_USE_FILE_LOCKING=FALSE

# Three input files to compare:
#   src   = original 5840 chunks (1, 2, 421120)
#   r120  = repacked to (120, 7, 421120) → 13 chunks
#   r720  = repacked to (720, 7, 421120) → 2 chunks
SRC=/work/bb1469/a270092/runtime/awiesm3-develop/Final_CMIP7_IO_Test_01/outdata/oifs/atmos_6h_pl7h_ua_1587-1587.nc
R120=/scratch/a/a270092/pycmor_repack/24692402/atmos_6h_pl7h_ua_1587-1587_repacked.nc
R720=$(ls /scratch/a/a270092/pycmor_repack_720/*/atmos_6h_pl7h_ua_1587-1587_repacked720.nc 2>/dev/null | head -1)

echo "files:"
echo "  src:  $(ls -lh $SRC | awk '{print $5}')  $SRC"
echo "  r120: $(ls -lh $R120 | awk '{print $5}')  $R120"
echo "  r720: $(ls -lh $R720 | awk '{print $5}')  $R720"
echo ""

# Each runs SEQUENTIALLY in this job. First touch is cold-cache,
# subsequent reads of the same file are warm. We do 3 passes per file.
# To make pass 1 of EACH file approximately cold-equivalent, we shuffle
# the per-pass order: pass1 sees src then r120 then r720 (each cold the
# first time), pass 2 reads them all warm, pass 3 too.

pass() {
  N=$1
  echo "=== PASS $N ==="
  for tag in src r120 r720; do
    case $tag in
      src) FP=$SRC ;;
      r120) FP=$R120 ;;
      r720) FP=$R720 ;;
    esac
    echo "--- $tag (pass $N) ---"
    python3 examples/bench_load_only.py "$FP" ua
    echo ""
  done
}

pass 1
pass 2
pass 3
