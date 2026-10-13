#!/bin/bash
# Start (or restart) a cmorization campaign.
#
#   examples/start_campaign.sh <campaign.yaml> [START_YEAR]
#
# Pins the code into <campaign>/code on first use (see campaign.py), checks
# the pin on every later use, and submits the year chain from the pinned
# copy. START_YEAR defaults to the campaign's first_year; years that already
# have a .done marker are skipped in seconds, so restarting from the
# beginning is safe.
#
# Run it from any pycmor checkout; after the first setup only the campaign's
# own pinned copy is used.
set -euo pipefail

CAMPAIGN="$(readlink -f "${1:?usage: start_campaign.sh <campaign.yaml> [start_year]}")"
HERE="$(cd "$(dirname "$0")" && pwd)"

# shellcheck disable=SC1090
source "${PYCMOR_CONDA_INIT:-/work/ab0246/a270092/software/miniforge3/etc/profile.d/conda.sh}"
conda activate "${PYCMOR_CONDA_ENV:-pycmor_py312}"

python3 "$HERE/campaign.py" setup "$CAMPAIGN"
eval "$(python3 "$HERE/campaign.py" env "$CAMPAIGN")"
START="${2:-$FIRST_YEAR}"

jid=$(sbatch --parsable --chdir="$LOGS_DIR" --account="$ACCOUNT" \
  --job-name="pycmor-$JOB_TAG-y$START" --mail-user="$MAIL_USER" \
  --export=ALL,CHAIN_START=1,CONSECUTIVE_FAILS=0 \
  "$PYCMOR_HOME/examples/run_year_chain.sbatch" "$CAMPAIGN" "$START")
echo "campaign $JOB_TAG: chain starts at $START as job $jid"
echo "  logs:   $LOGS_DIR"
echo "  work:   $WORKROOT"
echo "  output: $OUTPUT_ROOT"
echo "  watch:  squeue -u \$USER -o '%.12i %.40j %.8T %.10M' | grep --color=never pycmor-.*$JOB_TAG"
