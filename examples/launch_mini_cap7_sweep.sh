#!/bin/bash
# Launch the mini-cap7 contention sweep: 8 configs x 3 ensemble = 24 jobs.
# Each ensemble member reads from a different /work/.../copyN/ to avoid
# page-cache sharing between siblings.
set -euo pipefail

cd /work/ab0246/a270092/software/pycmor

# config: TAG W MEM
CONFIGS=(
  "2x4x64GB  2 64GB"
  "2x4x32GB  2 32GB"
  "3x4x32GB  3 32GB"
  "3x4x48GB  3 48GB"
  "4x4x16GB  4 16GB"
  "4x4x24GB  4 24GB"
  "4x4x32GB  4 32GB"
  "4x4x40GB  4 40GB"
)

echo "=== mini-cap7 sweep launch ==="
for cfg in "${CONFIGS[@]}"; do
  read -r tag W mem <<< "$cfg"
  for n in 1 2 3; do
    sbatch \
      -J "pycmor-mc7-${tag}-c${n}" \
      --output="pycmor_mini_cap7_${tag}_c${n}_%j.log" \
      --error="pycmor_mini_cap7_${tag}_c${n}_%j.log" \
      --export=ALL,COPY_N=$n,N_WORKERS=$W,MEM_LIMIT=$mem,TAG=$tag \
      examples/run_mini_cap7_sweep.sh
  done
done
echo ""
squeue -u $USER -h -O "JobID,Name,State" 2>/dev/null | grep --color=never mc7 | head -10
echo "..."
squeue -u $USER -h | grep --color=never mc7 | wc -l
echo "submitted"
