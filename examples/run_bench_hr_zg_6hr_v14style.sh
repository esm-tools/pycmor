#!/bin/bash
#SBATCH --job-name=pycmor-bench-zg_6hr_v14style-v14
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --time=01:00:00
#SBATCH --output=pycmor_bench_hr_zg_6hr_v14style_%j.log
#SBATCH --error=pycmor_bench_hr_zg_6hr_v14style_%j.log

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

OUTROOT=${OUTROOT:-/scratch/a/a270092/pycmor_bench_zg_6hr_v14style}
OUTDIR=$OUTROOT/${SLURM_JOB_ID:-$$}
mkdir -p "$OUTDIR"
command -v lfs >/dev/null && lfs setstripe -c 8 "$OUTDIR" 2>/dev/null || true

python3 - <<PY
import yaml
y = yaml.safe_load(open("examples/cmip7_bench_hr_zg_6hr_v14style.yaml"))
y["inherit"]["output_directory"] = "${OUTDIR}"
yaml.safe_dump(y, open("$PYCMOR_SCRATCH/bench.yaml", "w"), sort_keys=False)
PY

echo "=== ua_6hr_pl7h v14 benchmark ==="
echo "node: $(hostname), $(nproc) cores, $(free -g | awk '/^Mem:/{print $2}') GB RAM"
echo "input file:"
ls -lh /work/bb1469/a270092/runtime/awiesm3-develop/Final_CMIP7_IO_Test_01/outdata/oifs/atmos_6h_pl7h_ua_1587-1587.nc

# cgroup v2 watchdog: read job-level memory.current every 5s
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

echo "=== start ==="
date +%s.%N
/usr/bin/time -v pycmor process $PYCMOR_SCRATCH/bench.yaml
date +%s.%N

echo "=== output ==="
find "$OUTDIR" -type f -printf '%s %p\n' | sort -n
echo "=== cgroup memory log: $WATCH_LOG ==="
tail -10 "$WATCH_LOG"
