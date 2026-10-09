#!/bin/bash
#SBATCH --job-name=pycmor-hr-par
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --mem=256G
#SBATCH --time=01:00:00
#SBATCH --output=pycmor_hr_par_%x_%j.log
#SBATCH --error=pycmor_hr_par_%x_%j.log

# Single-node, multi-rule throughput test.  Same hardware as the existing
# run_hr_yaml.sh path (256 GB, full node), but flips two pycmor knobs to
# stop serialising rules on the driver:
#
#   parallel: True
#   pipeline_orchestrator: dask
#
# That fans rules out across LocalCluster worker processes on the SAME
# node.  No additional resources requested — just higher utilisation.
#
# Knobs (env, defaults sized to leave headroom on a 256 GB cgroup):
#   N_WORKERS         dask LocalCluster n_workers           (default 4)
#   TPW               threads per worker                    (default 4)
#   MEM_PER_WORKER    per-worker memory cap, e.g. '48GB'    (default 48GB)
#   CGROUP_GB         total cgroup memory in GB             (default 256)
#   YAML              source yaml path                       (required, $1)
#   OUTSUB            output subdir name                    (default basename of yaml dir)
#
# OOM protections (in order of activation):
#   1. Pre-submit budget: refuse to run if N_WORKERS * MEM_PER_WORKER >
#      0.75 * CGROUP_GB (driver/prefect/OS/page-cache need the rest).
#   2. Per-worker dask spill thresholds (env exports below): start
#      serializing at 50% of MEM_PER_WORKER, spill to disk at 60%, pause
#      new tasks at 75%, kill the worker at 90%. Tightened from dask
#      defaults (70/80/95) so we spill rather than die.
#   3. Per-worker memory_limit (forwarded to LocalCluster by the pycmor
#      patch): once a worker's RSS hits MEM_PER_WORKER * 0.90, dask
#      terminates it; total RSS therefore can't exceed N_WORKERS *
#      MEM_PER_WORKER * 0.95.
#
# Sizing: N_WORKERS = max-concurrent-rules. MEM_PER_WORKER must be larger
# than the heaviest single rule in this yaml. cap7_atm sfcWind peaks at
# ~17 GB so 48 GB/worker has ~30 GB of in-rule headroom.

set -euo pipefail

YAML="${1:?usage: sbatch run_hr_yaml_parallel.sh <yaml-path> [output-subdir]}"
OUTSUB="${2:-$(basename "$YAML" .yaml)_par}"

source ~/loadconda.sh
conda activate pycmor_py312

cd /work/ab0246/a270092/software/pycmor
export PYCMOR_HOME=/work/ab0246/a270092/software/pycmor

N_WORKERS=${N_WORKERS:-4}
TPW=${TPW:-4}
MEM_PER_WORKER=${MEM_PER_WORKER:-48GB}
CGROUP_GB=${CGROUP_GB:-256}

# Protection 1: pre-submit budget check.
mem_gb=${MEM_PER_WORKER%GB}
mem_gb=${mem_gb%gb}
total_gb=$(( N_WORKERS * mem_gb ))
budget_gb=$(( CGROUP_GB * 75 / 100 ))
if [ "$total_gb" -gt "$budget_gb" ]; then
  echo "ABORT: N_WORKERS * MEM_PER_WORKER = ${total_gb} GB exceeds budget ${budget_gb} GB"
  echo "       (75% of CGROUP_GB=${CGROUP_GB}). Lower N_WORKERS or MEM_PER_WORKER."
  exit 2
fi
echo "=== budget: ${total_gb} GB dask commit / ${budget_gb} GB allowed (${CGROUP_GB} GB cgroup) ==="

# Protection 2: tighter dask spill thresholds (fractions of MEM_PER_WORKER).
export DASK_DISTRIBUTED__WORKER__MEMORY__TARGET=0.50
export DASK_DISTRIBUTED__WORKER__MEMORY__SPILL=0.60
export DASK_DISTRIBUTED__WORKER__MEMORY__PAUSE=0.75
export DASK_DISTRIBUTED__WORKER__MEMORY__TERMINATE=0.90

PYCMOR_SCRATCH=/scratch/a/a270092/pycmor_tmp/$$
mkdir -p $PYCMOR_SCRATCH/prefect/storage
# Prefect ephemeral server's SQLite DB MUST live on node-local fast disk.
# On Lustre /scratch, alembic migrations on aiosqlite hit
# "sqlite3.OperationalError: disk I/O error" under concurrent SLURM
# job ramp-up — when 17 jobs simultaneously initialise their per-job
# Prefect DBs against the same Lustre filesystem, file-locking
# semantics break and the server boot is non-deterministic.
# Symptom: every gate-A log has the disk I/O error; some recover, some
# crash at 7-8 min wall with 0 task starts. Putting PREFECT_HOME on
# node-local /tmp eliminates this entirely.
PREFECT_NODELOCAL=/tmp/pycmor_prefect_${SLURM_JOB_ID:-$$}
mkdir -p $PREFECT_NODELOCAL/storage
export PREFECT_HOME=$PREFECT_NODELOCAL
export PREFECT_LOCAL_STORAGE_PATH=$PREFECT_NODELOCAL/storage
trap "rm -rf $PREFECT_NODELOCAL" EXIT
# Big HDF5 spill stays on /scratch; only Prefect's small (<10 MB) DB
# moves to /tmp.
export TMPDIR=$PYCMOR_SCRATCH
export HDF5_USE_FILE_LOCKING=FALSE
export OMP_NUM_THREADS=1

# Round-2/4 fix (commit 3169ce5): collapse each rule's pipeline into a
# single Prefect task to avoid the
# "Could not serialize object of type _HLGExprSequence" /
#  "cannot pickle '_thread.lock' object"
# error path. Defensive default in case submit_hr_year.sh's export
# didn't propagate (SLURM env inheritance is environment-dependent).
# Set to 0 to opt out per-rule when debugging individual step caching.
export PYCMOR_PREFECT_COLLAPSE=${PYCMOR_PREFECT_COLLAPSE:-1}

OUTROOT=${OUTROOT:-/scratch/a/a270092/pycmor_hr_par_out}
OUTDIR=$OUTROOT/$OUTSUB
mkdir -p "$OUTDIR"
command -v lfs >/dev/null && lfs setstripe -c 8 "$OUTDIR" 2>/dev/null || true

# Override yaml: force local cluster on this compute node, enable rule
# parallelism via dask backend, set memory cap, point output dir.
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
y.setdefault("inherit", {})["output_directory"] = "${OUTDIR}"
yaml.safe_dump(y, open("$PYCMOR_SCRATCH/par.yaml", "w"), sort_keys=False)
PY

echo "=== yaml: $YAML  ->  $OUTDIR ==="
echo "=== config: parallel=True orchestrator=dask N_WORKERS=${N_WORKERS} TPW=${TPW} MEM_PER_WORKER=${MEM_PER_WORKER} ==="
echo "=== node $(hostname), $(nproc) cores, $(free -g | awk '/^Mem:/{print $2}') GB ==="
date +%s.%N
/usr/bin/time -v pycmor process $PYCMOR_SCRATCH/par.yaml
date +%s.%N

echo "=== Output files ==="
find "$OUTDIR" -type f -printf '%s %p\n' | sort -n
