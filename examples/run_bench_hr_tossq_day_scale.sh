#!/bin/bash
#SBATCH --job-name=pycmor-bench-tossq-scale
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --mem=256G
#SBATCH --time=00:30:00
#SBATCH --output=pycmor_bench_hr_tossq_day_scale_%j.log
#SBATCH --error=pycmor_bench_hr_tossq_day_scale_%j.log

# Ocean counterpart of run_bench_hr_wap_day_scale.sh. Same env-var knobs,
# same dask memory accounting; only the bench yaml differs.
#
#   N_WORKERS         dask LocalCluster n_workers           (default 16)
#   TPW               threads per worker                    (default 4)
#   MEM_PER_WORKER    per-worker memory cap, e.g. '12GB'    (default 12GB)
#
# Sweep:  for n in 1 4 8 16; do N_WORKERS=$n MEM_PER_WORKER=$((192/$n))GB \
#           sbatch run_bench_hr_tossq_day_scale.sh; done

set -euo pipefail

source ~/loadconda.sh
conda activate pycmor_py312

cd /work/ab0246/a270092/software/pycmor
export PYCMOR_HOME=/work/ab0246/a270092/software/pycmor

N_WORKERS=${N_WORKERS:-16}
TPW=${TPW:-4}
MEM_PER_WORKER=${MEM_PER_WORKER:-12GB}

PYCMOR_SCRATCH=/scratch/a/a270092/pycmor_tmp/$$
mkdir -p $PYCMOR_SCRATCH/prefect/storage
export PREFECT_HOME=$PYCMOR_SCRATCH/prefect
export PREFECT_LOCAL_STORAGE_PATH=$PYCMOR_SCRATCH/prefect/storage
export TMPDIR=$PYCMOR_SCRATCH
export HDF5_USE_FILE_LOCKING=FALSE
export OMP_NUM_THREADS=1

OUTROOT=${OUTROOT:-/scratch/a/a270092/pycmor_bench_scale}
OUTDIR=$OUTROOT/tossq_day_n${N_WORKERS}_t${TPW}_m${MEM_PER_WORKER}_${SLURM_JOB_ID:-$$}
mkdir -p "$OUTDIR"
command -v lfs >/dev/null && lfs setstripe -c 8 "$OUTDIR" 2>/dev/null || true

python3 - <<PY
import yaml
src = "examples/cmip7_bench_hr_tossq_day.yaml"
y = yaml.safe_load(open(src))
y["pycmor"]["dask_cluster"] = "local"
y["pycmor"]["dask_n_workers"] = ${N_WORKERS}
y["pycmor"]["dask_threads_per_worker"] = ${TPW}
y["pycmor"]["dask_memory_limit"] = "${MEM_PER_WORKER}"
y["inherit"]["output_directory"] = "${OUTDIR}"
yaml.safe_dump(y, open("$PYCMOR_SCRATCH/bench.yaml", "w"), sort_keys=False)
PY

echo "=== config: N_WORKERS=${N_WORKERS} TPW=${TPW} MEM_PER_WORKER=${MEM_PER_WORKER} ==="
echo "=== total dask commit: $((N_WORKERS * ${MEM_PER_WORKER%GB})) GB / 256 GB cgroup ==="
echo "=== Input file ==="
ls -lh /work/bb1469/a270092/runtime/awiesm3-develop/Test_16n/outdata/fesom/sst.fesom.1587.nc

echo "=== Start pycmor process ==="
date +%s.%N
/usr/bin/time -v pycmor process $PYCMOR_SCRATCH/bench.yaml
date +%s.%N

echo "=== Output ==="
find "$OUTDIR" -type f -printf '%s %p\n' | sort -n
