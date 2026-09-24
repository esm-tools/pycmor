#!/bin/bash
#SBATCH --job-name=pycmor-year
#SBATCH --partition=shared
#SBATCH --account=ab0246
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=3-00:00:00
#SBATCH --mail-type=FAIL,TIME_LIMIT
#SBATCH --output=pycmor_%x_%j.log
#SBATCH --error=pycmor_%x_%j.log

# Cmorize a run year by year without anyone watching.
#
# Each job drives ONE year through examples/run_year_driver.py (submit, check
# the manifests, resubmit the tiers that came up short, three attempts), then
# submits the job for the next year and exits with the driver's result.
#
# Mail: this job carries --mail-type=FAIL,TIME_LIMIT and the shard jobs carry
# none. The driver retries inside this one job and only exits non-zero after
# its last attempt, so you get exactly one mail per year that could not be
# completed, never one per retry. The details are in <WORKDIR>/<YEAR>.FAILED;
# SLURM's mail itself only names the job (pycmor-y<YEAR>).
# TIME_LIMIT is listed separately because FAIL does not cover a timeout, and
# a timed-out driver would otherwise end the chain silently.
#
# A failed year does not stop the chain: later years only need model output,
# not earlier cmorized years. But after MAX_CONSECUTIVE_FAILS failed years in
# a row (default 2) the chain halts, because that pattern means something
# systematic (broken code, missing input, full disk) and carrying on would
# only burn node-hours and fill your inbox.
#
# Usage, from the repository root:
#   sbatch --job-name=pycmor-y1850 [--mail-user=ADDR] \
#     examples/run_year_chain.sbatch <RUN_ROOT> <FIRST_YEAR> <LAST_YEAR> <WORKROOT>
#
#   e.g. sbatch --job-name=pycmor-y1850 examples/run_year_chain.sbatch \
#          /work/bb1469/a270089/runtime/awiesm3-v3.4.2/AWI-ESM3-VEG-HR-CMIP7-piControl \
#          1850 2045 /scratch/a/a270092/pycmor_hr/piControl
#
# Each year works in <WORKROOT>/y<YEAR>. Restarting a chain from an earlier
# year is safe: years with a <YEAR>.done marker are skipped immediately.
#
# Env knobs: MAX_CONSECUTIVE_FAILS (2), DRIVER_ATTEMPTS (3), MAIL_USER,
#            PYCMOR_CONDA_INIT / PYCMOR_CONDA_ENV (as in run_hr_shard.sh).

set -uo pipefail  # deliberately no -e: a failed year must still chain onward

RUN_ROOT="${1:?usage: run_year_chain.sbatch <run-root> <year> <last-year> <workroot>}"
YEAR="${2:?usage: run_year_chain.sbatch <run-root> <year> <last-year> <workroot>}"
LAST_YEAR="${3:?usage: run_year_chain.sbatch <run-root> <year> <last-year> <workroot>}"
WORKROOT="${4:?usage: run_year_chain.sbatch <run-root> <year> <last-year> <workroot>}"

CONSECUTIVE_FAILS="${CONSECUTIVE_FAILS:-0}"
MAX_CONSECUTIVE_FAILS="${MAX_CONSECUTIVE_FAILS:-2}"
DRIVER_ATTEMPTS="${DRIVER_ATTEMPTS:-3}"

# Under sbatch this file runs from a spool copy, so its own path is useless
# for finding the repository or resubmitting itself (same trap as in
# run_hr_shard.sh). The repository is where it was submitted from.
PYCMOR_HOME="${PYCMOR_HOME:-${SLURM_SUBMIT_DIR:-$PWD}}"
cd "$PYCMOR_HOME" || { echo "ABORT: cannot cd to PYCMOR_HOME=$PYCMOR_HOME"; exit 2; }
export PYCMOR_HOME
SELF="$PYCMOR_HOME/examples/run_year_chain.sbatch"

PYCMOR_CONDA_INIT="${PYCMOR_CONDA_INIT:-/work/ab0246/a270092/software/miniforge3/etc/profile.d/conda.sh}"
PYCMOR_CONDA_ENV="${PYCMOR_CONDA_ENV:-pycmor_py312}"
# shellcheck disable=SC1090
source "$PYCMOR_CONDA_INIT"
conda activate "$PYCMOR_CONDA_ENV"

WORKDIR="$WORKROOT/y$YEAR"
echo "=== year $YEAR of $LAST_YEAR  workdir=$WORKDIR  consecutive_fails_so_far=$CONSECUTIVE_FAILS ==="

rc=0
python3 "$PYCMOR_HOME/examples/run_year_driver.py" "$RUN_ROOT" "$YEAR" "$WORKDIR" \
  --attempts "$DRIVER_ATTEMPTS" --email "" || rc=$?

if [ "$rc" -eq 0 ]; then
  CONSECUTIVE_FAILS=0
else
  CONSECUTIVE_FAILS=$((CONSECUTIVE_FAILS + 1))
  echo "=== year $YEAR incomplete after $DRIVER_ATTEMPTS attempts; see $WORKDIR/$YEAR.FAILED ==="
fi

NEXT=$((YEAR + 1))
if [ "$NEXT" -gt "$LAST_YEAR" ]; then
  echo "=== chain finished at $YEAR ==="
elif [ "$CONSECUTIVE_FAILS" -ge "$MAX_CONSECUTIVE_FAILS" ]; then
  echo "=== HALTING CHAIN: $CONSECUTIVE_FAILS consecutive failed years (limit $MAX_CONSECUTIVE_FAILS)."
  echo "    Fix the cause, then resubmit starting at $NEXT (or earlier; done years are skipped). ==="
else
  mail_args=()
  [ -n "${MAIL_USER:-}" ] && mail_args=(--mail-user="$MAIL_USER")
  next_jid=$(sbatch --parsable --chdir="$PYCMOR_HOME" --job-name="pycmor-y$NEXT" "${mail_args[@]}" \
    --export=ALL,CONSECUTIVE_FAILS="$CONSECUTIVE_FAILS" \
    "$SELF" "$RUN_ROOT" "$NEXT" "$LAST_YEAR" "$WORKROOT") \
    && echo "=== submitted year $NEXT as job $next_jid ===" \
    || { echo "=== ERROR: could not submit year $NEXT; chain stops here ==="; rc=3; }
fi

# Non-zero only when this year gave up (or the chain could not continue),
# which is what triggers SLURM's single FAIL mail.
exit "$rc"
