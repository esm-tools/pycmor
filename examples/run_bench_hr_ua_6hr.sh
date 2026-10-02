#!/bin/bash
#SBATCH --job-name=pycmor-bench-ua-6hr
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --time=01:00:00
#SBATCH --output=pycmor_bench_hr_ua_6hr_%j.log
#SBATCH --error=pycmor_bench_hr_ua_6hr_%j.log

# Single-rule benchmark for the heaviest cap7_atm rule class
# (6hr_pl7h fields). Designed for memory-pressure investigation by
# follow-up analysis. Not a throughput test.
#
# What this captures:
#   - Wall time from /usr/bin/time -v
#   - Maximum resident set size (whole job, all PIDs in cgroup)
#   - File system inputs/outputs
#   - Voluntary/involuntary context switches
#
# Optional add-ons for the next AI:
#   PYSPY=1   ./this        -> attach py-spy to the pycmor process
#                              after 60s and capture a 5min sample
#   MEMRAY=1  ./this        -> run pycmor under memray for a heap profile
#                              (pip install memray first)

set -euo pipefail

source ~/loadconda.sh
conda activate pycmor_py312

cd /work/ab0246/a270092/software/pycmor
export PYCMOR_HOME=/work/ab0246/a270092/software/pycmor

PYCMOR_SCRATCH=/scratch/a/a270092/pycmor_tmp/$$
mkdir -p $PYCMOR_SCRATCH/prefect/storage
export PREFECT_HOME=$PYCMOR_SCRATCH/prefect
export PREFECT_LOCAL_STORAGE_PATH=$PYCMOR_SCRATCH/prefect/storage
export TMPDIR=$PYCMOR_SCRATCH
export HDF5_USE_FILE_LOCKING=FALSE
export OMP_NUM_THREADS=1

OUTROOT=${OUTROOT:-/scratch/a/a270092/pycmor_bench_ua_6hr}
OUTDIR=$OUTROOT/${SLURM_JOB_ID:-$$}
mkdir -p "$OUTDIR"
command -v lfs >/dev/null && lfs setstripe -c 8 "$OUTDIR" 2>/dev/null || true

# Repoint the yaml's output_directory into per-run scratch.
python3 - <<PY
import yaml
y = yaml.safe_load(open("examples/cmip7_bench_hr_ua_6hr.yaml"))
y["inherit"]["output_directory"] = "${OUTDIR}"
yaml.safe_dump(y, open("$PYCMOR_SCRATCH/bench.yaml", "w"), sort_keys=False)
PY

echo "=== ua_6hr_pl7h benchmark ==="
echo "node: $(hostname), $(nproc) cores, $(free -g | awk '/^Mem:/{print $2}') GB RAM"
echo "input file:"
ls -lh /work/bb1469/a270092/runtime/awiesm3-develop/Final_CMIP7_IO_Test_01/outdata/oifs/atmos_6h_pl7h_ua_1587-1587.nc

# Memory watchdog: log the cgroup-v2 memory.current every 5 s.
# Levante uses cgroup-v2 hybrid hierarchy; the job slice is
# /sys/fs/cgroup/system.slice/slurmstepd.scope/job_<id>/memory.current
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

# Optional py-spy attach (60s warmup, then 5min sample). Requires
# pip install py-spy in the active env.
if [ "${PYSPY:-0}" = "1" ]; then
  (
    sleep 60
    PID=$(pgrep -f "pycmor process" | head -1)
    [ -n "$PID" ] && py-spy record \
        -d 300 -r 50 -o "$OUTDIR/pyspy.svg" --pid "$PID" 2>&1 \
      || echo "py-spy not available or pycmor PID not found"
  ) &
fi

# Optional memray profile. Requires pip install memray.
if [ "${MEMRAY:-0}" = "1" ]; then
  PYCMOR_CMD="memray run --output $OUTDIR/memray.bin --follow-fork pycmor"
else
  PYCMOR_CMD="pycmor"
fi

echo "=== start ==="
date +%s.%N
/usr/bin/time -v $PYCMOR_CMD process $PYCMOR_SCRATCH/bench.yaml
date +%s.%N

echo "=== output ==="
find "$OUTDIR" -type f -printf '%s %p\n' | sort -n
echo "=== cgroup memory log: $WATCH_LOG ==="
tail -5 "$WATCH_LOG"
echo "=== cgroup peak ==="
awk -F'\t' 'NR>1 && $2>m{m=$2} END{printf "%.2f GB\n", m+0}' "$WATCH_LOG"
