#!/bin/bash
#SBATCH --job-name=pycmor-hr-cli
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --mem=256G
#SBATCH --time=03:00:00
#SBATCH --output=pycmor_hr_cli_%x_%j.log
#SBATCH --error=pycmor_hr_cli_%x_%j.log

# Same parallel-runtime setup as run_hr_yaml_parallel.sh, but uses the
# new ``pycmor process`` CLI overrides (commit 8046000) for run-root,
# year range, output directory, and slurm worker memory — instead of
# pre-repointing the yaml with examples/repoint_hr_year.py.
#
# Required positional args (passed by submit_hr_year_cli.sh):
#   $1  YAML       — source yaml from awi-esm3-veg-hr-variables/<tier>/
#   $2  RUN_ROOT   — full path to model run root (Final_CMIP7_IO_Test_03/)
#   $3  YEAR       — 4-digit year to filter
#   $4  OUTSUB     — output sub-directory name (typically the tier name)
#
# Optional knobs (env, same defaults as run_hr_yaml_parallel.sh):
#   N_WORKERS               (default 4)
#   TPW                     (default 4)
#   MEM_PER_WORKER          (default 16GB)
#   CGROUP_GB               (default 256, raise to 512 when sbatch'd with --mem=512G)
#   MEMORY                  pycmor --memory override (jobqueue.slurm.memory). Optional.
#   OUTROOT                 root for output (default /scratch/$USER/pycmor_hr_cli_out)
#   PYCMOR_PREFECT_COLLAPSE (default 1)

set -euo pipefail

YAML="${1:?usage: run_hr_yaml_cli.sh <source-yaml> <run-root> <year> [output-subdir]}"
RUN_ROOT="${2:?usage: run_hr_yaml_cli.sh <source-yaml> <run-root> <year> [output-subdir]}"
YEAR="${3:?usage: run_hr_yaml_cli.sh <source-yaml> <run-root> <year> [output-subdir]}"
OUTSUB="${4:-$(basename "$YAML" .yaml)_cli}"

source ~/loadconda.sh
conda activate pycmor_py312

cd /work/ab0246/a270092/software/pycmor
export PYCMOR_HOME=/work/ab0246/a270092/software/pycmor

N_WORKERS=${N_WORKERS:-4}
TPW=${TPW:-4}
MEM_PER_WORKER=${MEM_PER_WORKER:-16GB}
CGROUP_GB=${CGROUP_GB:-256}
# pycmor.std_lib.files atomic-write staging uses /tmp (tmpfs) — RAM-backed
# on Levante compute, sized 63 GB. Reserve enough headroom so concurrent
# staged writes can't OOM the node. 1 GB per worker is conservative
# (matches the typical _day-cadence output size); raise for tiers with
# multi-GB files (set PYCMOR_TMPFS_BUDGET_GB before submit).
TMPFS_BUDGET_GB=${PYCMOR_TMPFS_BUDGET_GB:-$(( N_WORKERS * 1 ))}

# Pre-submit budget check (same as run_hr_yaml_parallel.sh) — now also
# accounts for tmpfs RAM that staged writes can consume.
mem_gb=${MEM_PER_WORKER%GB}
mem_gb=${mem_gb%gb}
worker_gb=$(( N_WORKERS * mem_gb ))
total_gb=$(( worker_gb + TMPFS_BUDGET_GB ))
budget_gb=$(( CGROUP_GB * 75 / 100 ))
if [ "$total_gb" -gt "$budget_gb" ]; then
  echo "ABORT: N_W*MEM_PER_W + tmpfs = ${worker_gb}+${TMPFS_BUDGET_GB} = ${total_gb} GB exceeds budget ${budget_gb} GB"
  echo "       (75% of CGROUP_GB=${CGROUP_GB}). Lower N_WORKERS or MEM_PER_WORKER,"
  echo "       or lower PYCMOR_TMPFS_BUDGET_GB if you've disabled staging."
  exit 2
fi
echo "=== budget: ${worker_gb} GB dask + ${TMPFS_BUDGET_GB} GB tmpfs / ${budget_gb} GB allowed (${CGROUP_GB} GB cgroup) ==="

# Dask spill thresholds (fractions of MEM_PER_WORKER).
export DASK_DISTRIBUTED__WORKER__MEMORY__TARGET=0.50
export DASK_DISTRIBUTED__WORKER__MEMORY__SPILL=0.60
export DASK_DISTRIBUTED__WORKER__MEMORY__PAUSE=0.75
export DASK_DISTRIBUTED__WORKER__MEMORY__TERMINATE=0.90

PYCMOR_SCRATCH=/scratch/a/a270092/pycmor_tmp/$$
mkdir -p $PYCMOR_SCRATCH/prefect/storage
PREFECT_NODELOCAL=/tmp/pycmor_prefect_${SLURM_JOB_ID:-$$}
mkdir -p $PREFECT_NODELOCAL/storage
export PREFECT_HOME=$PREFECT_NODELOCAL
export PREFECT_LOCAL_STORAGE_PATH=$PREFECT_NODELOCAL/storage
trap "rm -rf $PREFECT_NODELOCAL" EXIT
export TMPDIR=$PYCMOR_SCRATCH
export HDF5_USE_FILE_LOCKING=FALSE
export OMP_NUM_THREADS=1
export PYCMOR_PREFECT_COLLAPSE=${PYCMOR_PREFECT_COLLAPSE:-1}

OUTROOT=${OUTROOT:-/scratch/a/a270092/pycmor_hr_cli_out}
OUTDIR=$OUTROOT/$OUTSUB
mkdir -p "$OUTDIR"
command -v lfs >/dev/null && lfs setstripe -c 8 "$OUTDIR" 2>/dev/null || true

# Inject runtime parallel knobs into a copy of the source yaml. The
# data-path / year / output-directory / memory knobs are handled by the
# pycmor process CLI flags below — no yaml-rewriting for those.
python3 - <<PY
import yaml
y = yaml.safe_load(open("$YAML"))
y.setdefault("pycmor", {})
y["pycmor"]["parallel"] = True
y["pycmor"]["pipeline_orchestrator"] = "dask"
y["pycmor"]["dask_cluster"] = "local"
y["pycmor"]["dask_n_workers"] = ${N_WORKERS}
y["pycmor"]["dask_threads_per_worker"] = ${TPW}
y["pycmor"]["dask_memory_limit"] = "${MEM_PER_WORKER}"
yaml.safe_dump(y, open("$PYCMOR_SCRATCH/par.yaml", "w"), sort_keys=False)
PY

# Build CLI override args.
CLI_ARGS=(
  --data-path "${RUN_ROOT}"
  --year-start "${YEAR}"
  --year-end "${YEAR}"
  --output-directory "${OUTDIR}"
)
if [ -n "${MEMORY:-}" ]; then
  CLI_ARGS+=(--memory "${MEMORY}")
fi

echo "=== yaml: $YAML  ->  $OUTDIR ==="
echo "=== --data-path ${RUN_ROOT}  --year ${YEAR}  --memory ${MEMORY:-<unset>} ==="
echo "=== config: parallel=True orchestrator=dask N_WORKERS=${N_WORKERS} TPW=${TPW} MEM_PER_WORKER=${MEM_PER_WORKER} ==="
echo "=== node $(hostname), $(nproc) cores, $(free -g | awk '/^Mem:/{print $2}') GB ==="
date +%s.%N
/usr/bin/time -v pycmor process $PYCMOR_SCRATCH/par.yaml "${CLI_ARGS[@]}"
date +%s.%N

echo "=== Output files ==="
find "$OUTDIR" -type f -printf '%s %p\n' | sort -n
