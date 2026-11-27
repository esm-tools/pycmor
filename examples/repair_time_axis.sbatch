#!/bin/bash
#SBATCH --job-name=repair-time-axis
#SBATCH --partition=compute
#SBATCH --account=ab0246
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --time=04:00:00
#SBATCH --mail-type=FAIL,TIME_LIMIT
#SBATCH --output=repair_time_axis_%j.log
# Run repair_time_axis.py over published member directories.
#
#   sbatch --mail-user=<you> \
#     --export=ALL,CODE=<campaign>/code,PYENV=<campaign_root>/envs/awiesm3-veg-hr-cmip7-1,\
#MEMBERS="<.../1pctCO2/r1i1p1f1> <.../piControl/r1i1p1f1>" \
#     <campaign>/code/examples/repair_time_axis.sbatch
#
# Only metadata changes, so it is quick: about an hour for 60000 files. Files
# already right are skipped, so it can be submitted again at any time, and
# again after a fix campaign; set SUPERSEDED=yes then to also remove the files
# a rerun year replaced under a new name.
set -euo pipefail
: "${CODE:?}" "${PYENV:?}" "${MEMBERS:?}"
export PATH="$PYENV/bin:$PATH" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
extra=()
if [ "${SUPERSEDED:-no}" = yes ]; then
  extra=(--remove-superseded)
fi
# shellcheck disable=SC2086
python "$CODE/examples/repair_time_axis.py" $MEMBERS --jobs "${JOBS:-32}" "${extra[@]}" --apply
