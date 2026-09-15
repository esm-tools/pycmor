#!/bin/bash
# Repoint the 17 HR tier yamls at <RUN>/<YEAR> and sbatch one job per tier.
#
# Usage:  submit_hr_year.sh <RUN_NAME_OR_ABSPATH> <YEAR> [WORKDIR]
#   e.g.  submit_hr_year.sh Test_16n 1587
#         submit_hr_year.sh Test_16n 1587 /scratch/$USER/cmorize_Test_16n_y1587
#
# WORKDIR holds the per-tier yaml copies + per-tier output dir.
# Default: /scratch/$USER/pycmor_hr/<RUN>_y<YEAR>
set -euo pipefail

RUN="${1:?usage: $0 <run_name_or_abspath> <year> [workdir]}"
YEAR="${2:?usage: $0 <run> <year> [workdir]}"
WORKDIR="${3:-/scratch/${USER:0:1}/$USER/pycmor_hr/$(basename "$RUN")_y${YEAR}}"

HERE="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$WORKDIR"

# 1. Repoint yamls into WORKDIR/yamls/
YAMLS_DIR="$WORKDIR/yamls"
mkdir -p "$YAMLS_DIR"
python3 "$HERE/repoint_hr_year.py" "$RUN" "$YEAR" "$YAMLS_DIR"

# 2. Submit one job per tier yaml against the parallel runner. Each job
# gets one node and pycmor uses parallel: True + pipeline_orchestrator:
# dask + N_WORKERS workers within that node. Knobs (override via env):
#   N_WORKERS=4  TPW=4  MEM_PER_WORKER=16GB  CGROUP_GB=256  WALLTIME=03:00:00
#   PYCMOR_PREFECT_COLLAPSE=1
#
# These defaults come from the OPTIMIZATION_PLAN.md Round 4 contention
# sweep (24-config mini-cap7) + Round 5 cap7_atm validation
# (jobs 24705082 / 24705083, 2026-05-05):
#
#   2x4x64 baseline (prior default):  2:57:00 wall, 48/52 rules,
#                                     MaxRSS 245 GB, 1 worker kill
#   3x4x48 + collapse:                2:13:23 wall, 49/52 rules,
#                                     MaxRSS 61 GB, 0 kills, 0 HLG err
#   4x4x16 + collapse (this default): 2:08:35 wall, 49/52 rules,
#                                     MaxRSS 60 GB, 0 kills, 0 HLG err
#
# 4x4x16 lands 27% wall reduction over the prior 2x4x64 default,
# completes one MORE rule (the HLG bug previously masked one), and
# eliminates the worker kills the prior config saw at peak memory.
# 16 GB/worker keeps each worker under a single rule's heap so
# dask-nanny intervenes early (kill+restart cycle) before the
# OOM-cascade pattern that bit 4x4x32 and 4x4x48. Mid-mem high-W
# configs (4x4x24/32/48) remain unsafe.
#
# PYCMOR_PREFECT_COLLAPSE=1 collapses each rule's pipeline to a
# single Prefect task, which fixes the
# "Could not serialize object of type _HLGExprSequence /
#  cannot pickle '_thread.lock' object"
# bug that wastes Prefect retries in parallel mode. See commit
# 3169ce5 and DESIGN_PROPOSAL_subflow_deadlock.md.
#
# To reproduce the prior 2x4x64 default for comparison or fall back
# in case of an outlier-rule heap spike on a new year:
#   N_WORKERS=2 MEM_PER_WORKER=64GB submit_hr_year.sh ...
OUTROOT="$WORKDIR/cmorized"
mkdir -p "$OUTROOT"
export OUTROOT
export N_WORKERS=${N_WORKERS:-4}
export TPW=${TPW:-4}
export MEM_PER_WORKER=${MEM_PER_WORKER:-16GB}
export CGROUP_GB=${CGROUP_GB:-256}
export PYCMOR_PREFECT_COLLAPSE=${PYCMOR_PREFECT_COLLAPSE:-1}
WALLTIME="${WALLTIME:-03:00:00}"

submitted=()
for yaml in "$YAMLS_DIR"/*.yaml; do
  tier="$(basename "$yaml" .yaml)"
  jobname="pycmor-hr-${tier}-y${YEAR}"
  jid=$(sbatch --parsable -J "$jobname" --time="$WALLTIME" \
        "$HERE/run_hr_yaml_parallel.sh" "$yaml" "$tier") \
    || { echo "sbatch failed for $tier"; continue; }
  submitted+=("$jid:$tier")
  echo "  submitted $jobname  job=$jid"
done

echo
echo "Submitted ${#submitted[@]} jobs.  Outputs: $OUTROOT"
echo "Watch with:  squeue -u \$USER -n pycmor-hr"
