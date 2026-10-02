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

# Cmorize one campaign year by year without anyone watching.
#
# Start it with examples/start_campaign.sh, not by hand: that pins the code
# and passes the right job name, account, mail address and log directory.
# Everything else comes from the campaign file (see examples/campaign.py).
#
# Each job drives ONE year through run_year_driver.py (submit, check the
# manifests, resubmit the tiers that came up short), moves the finished year
# into the campaign's output root, then submits the job for the next year and
# exits with the driver's result.
#
# Code: every job runs the campaign's pinned copy in <campaign>/code and
# first checks that it has not moved. A fix pushed to the branch meanwhile
# does not reach a running campaign until someone repins it on purpose.
#
# Mail: this job carries --mail-type=FAIL,TIME_LIMIT and the shard jobs carry
# none. The driver retries inside this one job and only exits non-zero after
# its last attempt (or after the first, when every tier failed and retrying is
# pointless), so you get exactly one mail per year that could not be
# completed, never one per retry. The details are in <WORKDIR>/<YEAR>.FAILED;
# SLURM's mail itself only names the job (pycmor-<campaign>-y<YEAR>).
# TIME_LIMIT is listed separately because FAIL does not cover a timeout, and
# a timed-out driver would otherwise end the chain silently.
#
# The first year of every launch gets first_year_attempts (default 1) and a
# failure there stops the chain: it is the check that the setup is sound
# before the campaign spends node hours on the rest. After that, a failed
# year does not stop the chain, because later years only need model output.
# But after max_consecutive_fails failed years in a row (default 2) it halts,
# since that pattern means something systematic.
#
# Usage (what start_campaign.sh runs):
#   sbatch --chdir=<campaign>/logs --job-name=pycmor-<name>-y<YEAR> \
#     --export=ALL,CHAIN_START=1 <campaign>/code/examples/run_year_chain.sbatch \
#     <campaign.yaml> <YEAR>

set -uo pipefail  # deliberately no -e: a failed year must still chain onward

CAMPAIGN="$(readlink -f "${1:?usage: run_year_chain.sbatch <campaign.yaml> <year>}")"
YEAR="${2:?usage: run_year_chain.sbatch <campaign.yaml> <year>}"
CODE="$(dirname "$CAMPAIGN")/code"
CHAIN_START="${CHAIN_START:-0}"
CONSECUTIVE_FAILS="${CONSECUTIVE_FAILS:-0}"

# shellcheck disable=SC1090
source "${PYCMOR_CONDA_INIT:-/work/ab0246/a270092/software/miniforge3/etc/profile.d/conda.sh}"
conda activate "${PYCMOR_CONDA_ENV:-pycmor_py312}"

campaign_env=$(python3 "$CODE/examples/campaign.py" env "$CAMPAIGN") \
  || { echo "ABORT: cannot read campaign $CAMPAIGN"; exit 2; }
eval "$campaign_env"
python3 "$CODE/examples/campaign.py" verify "$CAMPAIGN" \
  || { echo "ABORT: the campaign's code or settings moved since it was pinned; chain stops here"; exit 2; }
cd "$LOGS_DIR" || { echo "ABORT: cannot cd to $LOGS_DIR"; exit 2; }

if [ "$CHAIN_START" = "1" ]; then
  attempts="$FIRST_YEAR_ATTEMPTS"
else
  attempts="$DRIVER_ATTEMPTS"
fi
WORKDIR="$WORKROOT/y$YEAR"
echo "=== campaign $JOB_TAG  year $YEAR of $LAST_YEAR  code $PYCMOR_CODE_COMMIT  DRS $PYCMOR_DRS_VERSION ==="
echo "=== workdir=$WORKDIR  output=$OUTPUT_ROOT  attempts=$attempts  consecutive_fails_so_far=$CONSECUTIVE_FAILS ==="

rc=0
python3 "$CODE/examples/run_year_driver.py" "$RUN_ROOT" "$YEAR" "$WORKDIR" \
  --attempts "$attempts" --email "" --publish-to "$OUTPUT_ROOT" || rc=$?

if [ "$rc" -eq 0 ]; then
  CONSECUTIVE_FAILS=0
else
  CONSECUTIVE_FAILS=$((CONSECUTIVE_FAILS + 1))
  echo "=== year $YEAR incomplete; see $WORKDIR/$YEAR.FAILED ==="
fi

NEXT=$((YEAR + 1))
if [ "$NEXT" -gt "$LAST_YEAR" ]; then
  echo "=== chain finished at $YEAR ==="
elif [ "$rc" -ne 0 ] && [ "$CHAIN_START" = "1" ]; then
  echo "=== HALTING CHAIN: the first year of this launch failed. Fix the cause, then"
  echo "    start_campaign.sh again (done years are skipped). ==="
elif [ "$CONSECUTIVE_FAILS" -ge "$MAX_CONSECUTIVE_FAILS" ]; then
  echo "=== HALTING CHAIN: $CONSECUTIVE_FAILS consecutive failed years (limit $MAX_CONSECUTIVE_FAILS)."
  echo "    Fix the cause, then start_campaign.sh again (done years are skipped). ==="
else
  next_jid=$(sbatch --parsable --chdir="$LOGS_DIR" --account="$ACCOUNT" \
    --job-name="pycmor-$JOB_TAG-y$NEXT" --mail-user="$MAIL_USER" \
    --export=ALL,CONSECUTIVE_FAILS="$CONSECUTIVE_FAILS",CHAIN_START=0 \
    "$CODE/examples/run_year_chain.sbatch" "$CAMPAIGN" "$NEXT") \
    && echo "=== submitted year $NEXT as job $next_jid ===" \
    || { echo "=== ERROR: could not submit year $NEXT; chain stops here ==="; rc=3; }
fi

# Non-zero only when this year gave up (or the chain could not continue),
# which is what triggers SLURM's single FAIL mail.
exit "$rc"
