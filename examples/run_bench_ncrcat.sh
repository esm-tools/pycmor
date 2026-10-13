#!/bin/bash
#SBATCH --job-name=pycmor-bench-ncrcat
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --time=00:30:00
#SBATCH --output=pycmor_bench_ncrcat_%j.log
#SBATCH --error=pycmor_bench_ncrcat_%j.log

set -euo pipefail

module load nco/5.0.6-gcc-11.2.0

SRC_DIR=/scratch/a/a270092/pycmor_bench_ua_6hr_v13/24676992
DST_DIR=/scratch/a/a270092/pycmor_bench_ncrcat/${SLURM_JOB_ID:-$$}
mkdir -p "$DST_DIR"
DST=$DST_DIR/ua_combined.nc

echo "=== ncrcat bench ==="
echo "node: $(hostname), $(nproc) cores, $(free -g | awk '/^Mem:/{print $2}') GB RAM"
echo "input: $(ls $SRC_DIR/*.nc | wc -l) files in $SRC_DIR"
echo "output: $DST"

# cgroup-v2 watchdog
WATCH_LOG=$DST_DIR/cgroup_mem_v2.tsv
JOB=${SLURM_JOB_ID:-$$}
CG_PATH=/sys/fs/cgroup/system.slice/slurmstepd.scope/job_$JOB/memory.current
(
  echo -e "epoch\tmem_GB"
  while true; do
    if [ -r "$CG_PATH" ]; then
      m=$(awk '{printf "%.2f", $1/1024/1024/1024}' "$CG_PATH" 2>/dev/null)
      [ -n "$m" ] && echo -e "$(date +%s)\t$m"
    fi
    sleep 2
  done
) > "$WATCH_LOG" &
WATCH_PID=$!
trap "kill $WATCH_PID 2>/dev/null || true" EXIT

REC_DIR=$DST_DIR/rec
mkdir -p "$REC_DIR"

echo "=== start ==="
date +%s.%N
echo "--- pass 1: ncks --mk_rec_dmn time on each slab ---"
/usr/bin/time -v bash -c "
  for f in $SRC_DIR/ua_6hr_pl7h_slab*.nc; do
    ncks -O --mk_rec_dmn time \$f $REC_DIR/\$(basename \$f) || exit 1
  done
"
echo "--- pass 2: ncrcat the rec-dim slabs ---"
/usr/bin/time -v ncrcat -O $REC_DIR/ua_6hr_pl7h_slab*.nc $DST
date +%s.%N

# Cleanup intermediate
rm -rf "$REC_DIR"

echo "=== output ==="
ls -lh $DST
echo "=== mem peak ==="
awk -F'\t' 'NR>1 && $2>m{m=$2} END{printf "ncrcat peak: %.2f GB\n", m+0}' "$WATCH_LOG"
echo "=== last 20 mem samples ==="
tail -20 "$WATCH_LOG"
