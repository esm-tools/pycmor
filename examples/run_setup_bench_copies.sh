#!/bin/bash
#SBATCH --job-name=pycmor-bench-copy
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=14
#SBATCH --mem=32G
#SBATCH --time=00:30:00
#SBATCH --output=pycmor_bench_copy_%j.log
#SBATCH --error=pycmor_bench_copy_%j.log

set -euo pipefail

SRC=/work/bb1469/a270092/runtime/awiesm3-develop/Final_CMIP7_IO_Test_01/outdata/oifs
DST_ROOT=/work/ab0246/a270092/bench_copies

FILES=(
  atmos_6h_pl7h_ua_1587-1587.nc
  atmos_6h_pl7h_va_1587-1587.nc
  atmos_6h_pl7h_ta_1587-1587.nc
  atmos_6h_pl7h_hus_1587-1587.nc
  atmos_6h_pl7h_zg_1587-1587.nc
  atmos_1h_pt_10u_1587-1587.nc
  atmos_1h_ts_ts_1587-1587.nc
)

echo "=== bench-copy setup ==="
date +%s.%N
for n in 1 2 3; do
  D=$DST_ROOT/copy$n
  mkdir -p "$D"
  # explicit Lustre striping so all three copies have identical layout
  lfs setstripe -c 8 -S 1M "$D" 2>/dev/null || echo "warn: stripe set failed on $D"
  echo "--- copy$n stripe ---"
  lfs getstripe -d "$D" 2>/dev/null | head -3 || true
done

echo ""
echo "=== copying 7 files x 3 copies (21 cps), parallel within each copy ==="
for n in 1 2 3; do
  D=$DST_ROOT/copy$n
  echo "--- copy$n ---"
  date +%s.%N
  # Parallel cp within this copy dir (7 cps in flight)
  for f in "${FILES[@]}"; do
    cp "$SRC/$f" "$D/$f" &
  done
  wait
  date +%s.%N
done
echo ""
echo "=== verify ==="
for n in 1 2 3; do
  D=$DST_ROOT/copy$n
  echo "--- copy$n ---"
  ls -l "$D"/ | awk '{s+=$5; n++} END {printf "%d files, %.1f GB\n", n-1, s/1e9}'
  lfs getstripe -d "$D" 2>/dev/null | grep --color=never -E "stripe_count|stripe_size" | head -2 || true
done
date +%s.%N
echo "DONE"
