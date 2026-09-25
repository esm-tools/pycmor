#!/bin/bash
#SBATCH --job-name=pycmor-repack-one
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=00:30:00
#SBATCH --output=pycmor_repack_one_%j.log
#SBATCH --error=pycmor_repack_one_%j.log

set -euo pipefail
source ~/loadconda.sh
conda activate pycmor_py312
cd /work/ab0246/a270092/software/pycmor

INPUT=${INPUT:-/work/bb1469/a270092/runtime/awiesm3-develop/Final_CMIP7_IO_Test_01/outdata/oifs/atmos_6h_pl7h_ua_1587-1587.nc}
OUTROOT=${OUTROOT:-/scratch/a/a270092/pycmor_repack}
OUTDIR=$OUTROOT/${SLURM_JOB_ID:-$$}
mkdir -p "$OUTDIR"
command -v lfs >/dev/null && lfs setstripe -c 8 "$OUTDIR" 2>/dev/null || true
OUTPUT=$OUTDIR/$(basename $INPUT .nc)_repacked.nc

# cgroup v2 watchdog
WATCH_LOG=$OUTDIR/cgroup_mem_v2.tsv
JOB=${SLURM_JOB_ID:-$$}
CG_PATH=/sys/fs/cgroup/system.slice/slurmstepd.scope/job_$JOB/memory.current
(
  echo -e "epoch\tmem_GB"
  while true; do
    if [ -r "$CG_PATH" ]; then
      m=$(awk '{printf "%.2f", $1/1024/1024/1024}' "$CG_PATH" 2>/dev/null)
      [ -n "$m" ] && echo -e "$(date +%s)\t$m"
    fi
    sleep 5
  done
) > "$WATCH_LOG" &
WATCH_PID=$!
trap "kill $WATCH_PID 2>/dev/null || true" EXIT

echo "=== repack-one ==="
echo "node: $(hostname), $(nproc) cores"
echo "input:  $(ls -lh $INPUT)"
echo "output: $OUTPUT"
echo "=== chunks BEFORE ==="
ncdump -hs "$INPUT" 2>/dev/null | grep --color=never -E "ChunkSizes|Filter" | head -10
echo ""
echo "=== running repack ==="
date +%s.%N
/usr/bin/time -v python3 examples/repack_one.py "$INPUT" "$OUTPUT" --time-chunk 120 --slab 240
date +%s.%N
echo ""
echo "=== chunks AFTER ==="
ncdump -hs "$OUTPUT" 2>/dev/null | grep --color=never -E "ChunkSizes|Filter" | head -10
echo ""
echo "=== output size ==="
ls -lh "$OUTPUT"
echo ""
echo "=== cgroup peak ==="
awk -F'\t' 'NR>1 && $2>m{m=$2} END{printf "%.2f GB\n", m+0}' "$WATCH_LOG"
