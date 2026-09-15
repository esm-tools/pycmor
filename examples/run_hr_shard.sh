#!/bin/bash
#SBATCH --job-name=pycmor-hr-shard
#SBATCH --partition=compute
#SBATCH --account=ab0246
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --mem=0
#SBATCH --time=03:00:00
#SBATCH --output=pycmor_hr_shard_%x_%A_%a.log
#SBATCH --error=pycmor_hr_shard_%x_%A_%a.log

# Step 2 of PLAN_slurm_shard_isolation.md (Option 2-ish: ran on `shared`
# at first, hit queue contention + worker-memory undersizing; pivoted to
# `compute` per the plan's §0 fallback).
#
# Runs ONE shard yaml (≈16-20 rules) in a single Python process on a
# dedicated `compute` node. Driver memory bounded by N rules, not by 70+.
#
# Why `compute`:
#   - `shared` partition was queue-saturated at 35-array submit;
#     compute has 2931 nodes → no wait.
#   - `compute` is OverSubscribe=EXCLUSIVE. ``--mem=0`` tells SLURM
#     "give me all memory on whichever node I land on" — that's
#     ~256 GB on the common-tier nodes (2931 total), more on the
#     512/1024 GB tiers. To force a 512 GB+ node for memory-pressure
#     experiments, override via env: `MEM_SBATCH=--mem=512G` to the
#     submitter or pass `--mem=512G` directly to sbatch. Default
#     ``--mem=0`` keeps the full ~2931-node pool available.
#   - The 94% CPU waste (8-9 active / 128 allocated) is the price
#     of failure isolation per shard.
#
# Designed to be sbatched as a SLURM array (one task per shard):
#   sbatch --array=1-4 examples/run_hr_shard.sh <shards-dir> <run-root> <year> <output-subdir>
#
# Required positional args:
#   $1  SHARDS_DIR  — dir containing <tier>_shard_NN.yaml files
#                     (produced by examples/shard_tier_yaml.py)
#   $2  RUN_ROOT    — full path to model run root
#   $3  YEAR        — 4-digit year to process
#   $4  OUTSUB      — output sub-directory name (typically <tier>/cmorized/<tier>)
#
# Each array task picks shard `$((SLURM_ARRAY_TASK_ID - 1))` from
# SHARDS_DIR. Shard yamls are sorted by filename so shard_00 → array
# task 1, shard_01 → array task 2, etc.
#
# Optional knobs (env):
#   N_WORKERS               (default 2)
#   TPW                     (default 4)
#   MEM_PER_WORKER          (default 8GB) — 2 × 8 GB = 16 GB worker total
#                                           leaving ~44 GB driver headroom
#   CGROUP_GB               (default 60) — matches the 60 GB SBATCH alloc
#   PYCMOR_PREFECT_COLLAPSE (default 1)
#   OUTROOT                 root for output (default /scratch/$USER/pycmor_hr_shard_out)
#   MEMORY                  pycmor --memory override (optional)

set -euo pipefail

SHARDS_DIR="${1:?usage: run_hr_shard.sh <shards-dir> <run-root> <year> [output-subdir]}"
RUN_ROOT="${2:?usage: run_hr_shard.sh <shards-dir> <run-root> <year> [output-subdir]}"
YEAR="${3:?usage: run_hr_shard.sh <shards-dir> <run-root> <year> [output-subdir]}"
OUTSUB="${4:-shard_out}"

# Pick the shard yaml that corresponds to this array task.
# SLURM_ARRAY_TASK_ID is 1-based; shard files are 0-based.
task_idx=${SLURM_ARRAY_TASK_ID:-1}
shard_idx=$((task_idx - 1))
shard_yaml=$(ls -1 "$SHARDS_DIR"/*.yaml | sort | sed -n "$((shard_idx + 1))p")
if [ -z "${shard_yaml:-}" ]; then
  echo "ABORT: shard index $shard_idx not found in $SHARDS_DIR"
  exit 2
fi
shard_basename=$(basename "$shard_yaml" .yaml)

# Conda entry point. Defaults to the shared miniforge install on /work rather
# than a personal ~/loadconda.sh wrapper, which only sources this same file and
# depends on one user's home directory staying readable. Override either value
# to use your own environment.
PYCMOR_CONDA_INIT="${PYCMOR_CONDA_INIT:-/work/ab0246/a270092/software/miniforge3/etc/profile.d/conda.sh}"
PYCMOR_CONDA_ENV="${PYCMOR_CONDA_ENV:-pycmor_py312}"
source "$PYCMOR_CONDA_INIT"
conda activate "$PYCMOR_CONDA_ENV"

# Refresh esgvoc CV cache on this compute node before pycmor / esgvoc
# imports. SLURM nodes can hold an older snapshot than the one used to
# draft rules locally. cli69 showed 28 HIGH findings (14 ATTR004 +
# 14 FILE001) for the `30s-90s` region term that IS in upstream
# cmip7@1.2.6 but was missing on the workers. Each `esgvoc use` is
# idempotent and roughly a second when already current. Soft-fail so a
# briefly-down registry doesn't abort the run. The HIGH finding
# reappearing is itself the signal that the refresh didn't take.
esgvoc use universe@latest 2>/dev/null || echo "esgvoc universe refresh failed (continuing with current cache)"
esgvoc use cmip7@latest 2>/dev/null || echo "esgvoc cmip7 refresh failed (continuing with current cache)"

# Repo root. Deriving this from the script's own path does NOT work under
# sbatch: SLURM copies the batch script to /var/spool/slurmd/job*/slurm_script
# before running it, so BASH_SOURCE points at the spool copy. The submitter
# exports PYCMOR_HOME (it knows its own location); SLURM_SUBMIT_DIR is the
# fallback when this script is sbatched directly from the repo root.
PYCMOR_HOME="${PYCMOR_HOME:-${SLURM_SUBMIT_DIR:-$PWD}}"
cd "$PYCMOR_HOME"
export PYCMOR_HOME

# cc-plugin-aicc (DKRZ, Schupfner/Mazumder) validates coordinates against the
# CMIP7 CMOR tables and is the third suite in qc_tests. It reads the tables
# from this path.
#
# This export is not optional. Without it the plugin raises FileNotFoundError
# out of ComplianceChecker.run_checker, which kills the whole cchecker call:
# no JSON is written at all, so cf and wcrp_cmip7 findings are lost too and
# the file ends up with no QC result rather than an aicc-specific error.
# Verified: without the variable exit=1 and no report; with it, exit=1 (the
# normal "findings exist under -c strict") and all three suites present.
#
# Pinned at ee94f52 (2026-08-13). The clone is shallow; re-pull deliberately
# rather than by accident, because a table change moves our QC numbers.
export CMIP7_TABLES_PATH=/work/ab0246/a270092/software/cmip7-cmor-tables/tables

N_WORKERS=${N_WORKERS:-4}
TPW=${TPW:-4}
# 48 GB per worker (was 32 GB in cli22): hourly OIFS rules like tas
# (8760 timesteps × 421k cells × float32 ≈ 14 GiB raw) spike past 30 GB
# during regrid+aggregate; one worker OOM = whole shard dies (cli22
# cap7_land_05). 4 × 48 = 192 GB workers, ~30 GB driver, headroom OK
# on the 256 GB cgroup.
MEM_PER_WORKER=${MEM_PER_WORKER:-48GB}
CGROUP_GB=${CGROUP_GB:-256}
# Smaller tmpfs budget than the per-tier runner: at N=16-20 rules per
# process, peak concurrent staged writes is bounded by N_WORKERS.
TMPFS_BUDGET_GB=${PYCMOR_TMPFS_BUDGET_GB:-$(( N_WORKERS * 1 ))}

# Budget check — same shape as run_hr_yaml_cli.sh. The 75% factor leaves
# headroom for Python, xarray caches, etc.
mem_gb=${MEM_PER_WORKER%GB}
mem_gb=${mem_gb%gb}
worker_gb=$(( N_WORKERS * mem_gb ))
total_gb=$(( worker_gb + TMPFS_BUDGET_GB ))
budget_gb=$(( CGROUP_GB * 85 / 100 ))
if [ "$total_gb" -gt "$budget_gb" ]; then
  echo "ABORT: N_W*MEM_PER_W + tmpfs = ${worker_gb}+${TMPFS_BUDGET_GB} = ${total_gb} GB exceeds budget ${budget_gb} GB"
  echo "       (85% of CGROUP_GB=${CGROUP_GB}). Lower N_WORKERS or MEM_PER_WORKER."
  exit 2
fi
echo "=== shard: $shard_basename (array task $task_idx) ==="
echo "=== budget: ${worker_gb} GB dask + ${TMPFS_BUDGET_GB} GB tmpfs / ${budget_gb} GB allowed (${CGROUP_GB} GB cgroup) ==="

# Dask spill thresholds (fractions of MEM_PER_WORKER).
export DASK_DISTRIBUTED__WORKER__MEMORY__TARGET=0.50
export DASK_DISTRIBUTED__WORKER__MEMORY__SPILL=0.60
export DASK_DISTRIBUTED__WORKER__MEMORY__PAUSE=0.75
export DASK_DISTRIBUTED__WORKER__MEMORY__TERMINATE=0.90
# Bump dask shutdown timeouts. Default 5s is too short for cluster
# teardown: nanny SIGKILLs workers, daemon threads race with the
# interpreter's stderr lock at shutdown, and CPython aborts with
# `_enter_buffered_busy` (SIGABRT) — flagging an otherwise-successful
# pycmor run as FAILED in SLURM (cli62 veg_land_3: Flow Completed,
# Exit status: 0, then SIGABRT during dask cluster.close()).
export DASK_DISTRIBUTED__WORKER__CLOSE_TIMEOUT=60s
export DASK_DISTRIBUTED__NANNY__PROCESS_CLOSE_TIMEOUT=60s

# Scratch root, following the Levante convention /scratch/<initial>/<user>.
# Same pattern submit_hr_year_shards.sh already uses for its workdir default.
PYCMOR_SCRATCH_ROOT="${PYCMOR_SCRATCH_ROOT:-/scratch/${USER:0:1}/$USER}"
PYCMOR_SCRATCH="$PYCMOR_SCRATCH_ROOT/pycmor_tmp/${SLURM_JOB_ID:-$$}_${task_idx}"
mkdir -p "$PYCMOR_SCRATCH/prefect/storage"
PREFECT_NODELOCAL=/tmp/pycmor_prefect_${SLURM_JOB_ID:-$$}_${task_idx}
mkdir -p "$PREFECT_NODELOCAL/storage"
export PREFECT_HOME="$PREFECT_NODELOCAL"
export PREFECT_LOCAL_STORAGE_PATH="$PREFECT_NODELOCAL/storage"
trap "rm -rf $PREFECT_NODELOCAL" EXIT
# TMPDIR lives on Lustre because dask spills can be large, but the directory
# is not always usable the instant mkdir returns: the metadata has to reach the
# node first. Bash then fails the very next here-document with "cannot create
# temp file for here-document", which killed cap7_land shard 4 twice and
# core_atm shard 2 once, on three different nodes. Probe it, retry briefly, and
# fall back to node-local /tmp rather than losing the whole shard over a
# temp file.
for _try in 1 2 3 4 5; do
  if touch "$PYCMOR_SCRATCH/.probe" 2>/dev/null; then rm -f "$PYCMOR_SCRATCH/.probe"; break; fi
  echo "=== TMPDIR $PYCMOR_SCRATCH not writable yet (attempt $_try), waiting ==="
  sleep 3
  mkdir -p "$PYCMOR_SCRATCH/prefect/storage" 2>/dev/null || true
done
if ! touch "$PYCMOR_SCRATCH/.probe" 2>/dev/null; then
  PYCMOR_SCRATCH=/tmp/pycmor_tmp_${SLURM_JOB_ID:-$$}_${task_idx}
  mkdir -p "$PYCMOR_SCRATCH/prefect/storage"
  echo "=== WARNING: falling back to node-local TMPDIR $PYCMOR_SCRATCH ==="
else
  rm -f "$PYCMOR_SCRATCH/.probe"
fi
export TMPDIR="$PYCMOR_SCRATCH"
export HDF5_USE_FILE_LOCKING=FALSE
export OMP_NUM_THREADS=1
export PYCMOR_PREFECT_COLLAPSE=${PYCMOR_PREFECT_COLLAPSE:-1}
# Disable Fix #3 (client.compute on workers) by default for shard runs.
# Fix #3 was added (3604c53) to solve cli16's 87 GiB driver-RSS pileup
# at N=70 rules in one process. Shard isolation caps N at ~20, which
# already prevents that pileup (~25 GiB max driver memory). Fix #3 ON
# would still ship lazy graphs to the scheduler, which OOMs workers on
# big-graph rules (volcello, tossq_day, hfx/hfy, 3D atmos/ocean) — cf.
# cli23 stuck shards and cli24's 7/7 success with this set to "off".
# Synchronous-scheduler in-process compute is single-threaded per rule
# but max_in_flight=16 (= n_workers × tpw) still gives 16-wide
# across-rule parallelism. Throughput is unchanged for cheap rules;
# heavy rules actually complete instead of hanging.
# Default "off" — hard-coded because a stale env value (from a prior
# shell session leaking via --export=ALL) silently disabled this
# default in cli25, leading to 13 large-graph warnings and the
# cap7_ocean_0 timeout we'd otherwise dodged. The submitter
# (submit_hr_year_shards.sh) selects per-tier via SHARD_FIX3=auto for
# tiers that benefit from worker-compute parallelism (heavy 3D plev
# atmos with small output) without going through the broken default
# inheritance path. If SHARD_FIX3 is set, it takes precedence here.
if [ -n "${SHARD_FIX3:-}" ]; then
  export PYCMOR_WORKER_COMPUTE="$SHARD_FIX3"
else
  export PYCMOR_WORKER_COMPUTE=off
fi

# Per-tier malloc allocator override. Default = glibc (unset).
# SHARD_JEMALLOC=on switches to jemalloc, which bounds heap fragmentation
# by design — addresses the cli34 lrcs_seaice signal-6 SIGABRT pattern
# where MaxVMSize hit 557 GiB (vs RSS 232 GiB) on a 512 GiB cgroup,
# i.e. glibc malloc arenas fragmented enough that internal state
# corrupted and abort() fired despite the cgroup having headroom.
# malloc_trim alone wasn't enough (cli34 _2 + _3 still failed).
# Selected per-tier by submit_hr_year_shards.sh — only lrcs_seaice.
if [ "${SHARD_JEMALLOC:-off}" = "on" ]; then
  export LD_PRELOAD=/lib64/libjemalloc.so.2
  echo "=== LD_PRELOAD=$LD_PRELOAD (jemalloc) ==="
fi

OUTROOT=${OUTROOT:-$PYCMOR_SCRATCH_ROOT/pycmor_hr_shard_out}
OUTDIR="$OUTROOT/$OUTSUB"
mkdir -p "$OUTDIR"
command -v lfs >/dev/null && lfs setstripe -c 8 "$OUTDIR" 2>/dev/null || true

# Inject runtime parallel knobs into a copy of the shard yaml.
# SHARD_DRS=on enables pycmor's enable_output_subdirs, which appends the
# full CMIP DRS sub-tree
#   <mip_era>/<activity>/<institution>/<source>/<experiment>/<member>/
#       <table>/<variable>/<grid>/v<YYYYMMDD>/
# under OUTDIR. Submitter sets SHARD_DRS=on and OUTSUB="" so all tiers
# write into one shared DRS root.
SHARD_DRS_FLAG=False
if [ "${SHARD_DRS:-off}" = "on" ]; then
  SHARD_DRS_FLAG=True
  echo "=== enable_output_subdirs=True (CMIP DRS output) ==="
fi
python3 - <<PY
import yaml
y = yaml.safe_load(open("$shard_yaml"))
y.setdefault("pycmor", {})
y["pycmor"]["parallel"] = True
y["pycmor"]["pipeline_orchestrator"] = "dask"
y["pycmor"]["dask_cluster"] = "local"
y["pycmor"]["dask_n_workers"] = ${N_WORKERS}
y["pycmor"]["dask_threads_per_worker"] = ${TPW}
y["pycmor"]["dask_memory_limit"] = "${MEM_PER_WORKER}"
y["pycmor"]["enable_output_subdirs"] = $SHARD_DRS_FLAG
yaml.safe_dump(y, open("$PYCMOR_SCRATCH/par.yaml", "w"), sort_keys=False)
PY

CLI_ARGS=(
  --data-path "${RUN_ROOT}"
  --year-start "${YEAR}"
  --year-end "${YEAR}"
  --output-directory "${OUTDIR}"
)
if [ -n "${MEMORY:-}" ]; then
  CLI_ARGS+=(--memory "${MEMORY}")
fi

echo "=== shard yaml: $shard_yaml  ->  $OUTDIR ==="
echo "=== --data-path ${RUN_ROOT}  --year ${YEAR}  --memory ${MEMORY:-<unset>} ==="
echo "=== config: parallel=True orchestrator=dask N_WORKERS=${N_WORKERS} TPW=${TPW} MEM_PER_WORKER=${MEM_PER_WORKER} PYCMOR_WORKER_COMPUTE=${PYCMOR_WORKER_COMPUTE} ==="
echo "=== node $(hostname), $(nproc) cores allocated, $(free -g | awk '/^Mem:/{print $2}') GB visible ==="

# Inactivity watchdog: scancel this job if the SLURM log file's mtime
# stalls. cli60 cap7_aerosol toz_mon hung the shard for 2h after 4/5
# rules had already saved — Prefect/Dask cluster wedge with no output.
# Default 5400s (90 min): the older 1800s default caught 3D plev saves
# mid-write (cli104 lost 2 shards). zlib+shuffle silent-save windows on
# cl_day/pfull_day/hurs_3hr routinely hit 50-70 min. 90 min covers those
# without letting a genuine wedge burn the full 8h walltime. Override
# via WEDGE_TIMEOUT_SEC=<seconds>; set 0 to disable.
WEDGE_TIMEOUT_SEC="${WEDGE_TIMEOUT_SEC:-5400}"
if [ "$WEDGE_TIMEOUT_SEC" -gt 0 ] && [ -n "${SLURM_JOB_ID:-}" ]; then
  # Derive the log path from where sbatch was invoked rather than hardcoding
  # a checkout. `#SBATCH --output` above is a relative filename, so SLURM
  # writes the log into the submission directory; SLURM_SUBMIT_DIR is that
  # same directory. Hardcoding it meant the watchdog only worked for whoever
  # owned the path in the script -- for everyone else it polled a file that
  # never appeared and silently never fired, with no error to notice.
  WEDGE_LOG_FILE="${SLURM_SUBMIT_DIR:-$PWD}/pycmor_hr_shard_${SLURM_JOB_NAME:-shard}_${SLURM_ARRAY_JOB_ID:-$SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID:-1}.log"
  WEDGE_TARGET_JOB="${SLURM_ARRAY_JOB_ID:+${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}}"
  WEDGE_TARGET_JOB="${WEDGE_TARGET_JOB:-$SLURM_JOB_ID}"
  echo "=== watchdog: scancel ${WEDGE_TARGET_JOB} if ${WEDGE_LOG_FILE} mtime stalls for ${WEDGE_TIMEOUT_SEC}s ==="
  (
    # Brief grace period for the log file to appear.
    sleep 60
    while sleep 60; do
      if [ -f "$WEDGE_LOG_FILE" ]; then
        mtime=$(stat -c %Y "$WEDGE_LOG_FILE" 2>/dev/null || echo 0)
        now=$(date +%s)
        age=$((now - mtime))
        if [ "$age" -ge "$WEDGE_TIMEOUT_SEC" ]; then
          echo "[WATCHDOG] ${WEDGE_LOG_FILE} inactive for ${age}s >= ${WEDGE_TIMEOUT_SEC}s; scancel ${WEDGE_TARGET_JOB}" >&2
          scancel "$WEDGE_TARGET_JOB" || true
          break
        fi
      fi
    done
  ) &
  WEDGE_PID=$!
  trap "rm -rf $PREFECT_NODELOCAL; kill $WEDGE_PID 2>/dev/null || true" EXIT
fi

date +%s.%N
/usr/bin/time -v pycmor process "$PYCMOR_SCRATCH/par.yaml" "${CLI_ARGS[@]}"
date +%s.%N

echo "=== Output files for this shard ==="
# Don't dump the entire OUTDIR — other shards write here too. Print only
# files modified since this script started.
find "$OUTDIR" -type f -newer "$PYCMOR_SCRATCH" -printf '%s %p\n' | sort -n
