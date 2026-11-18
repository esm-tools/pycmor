#!/bin/bash
#SBATCH --job-name=repair-fesom-constants
#SBATCH --partition=compute
#SBATCH --account=ab0246
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --time=08:00:00
#SBATCH --mail-type=FAIL,TIME_LIMIT
#SBATCH --output=repair_fesom_constants_%A_%a.log
# Run repair_fesom_constants.py over published years, ten years per array task.
#
#   sbatch --array=0-10 --mail-user=<you> \
#     --export=ALL,CODE=<campaign>/code,PYENV=<campaign_root>/envs/awiesm3-veg-hr-cmip7-1,\
#MEMBER=<.../AWI-ESM3-4-2-veg-HR/1pctCO2/r1i1p1f1>,FIRST=1850,LAST=1950,WRITTEN_BEFORE=<time of the repin> \
#     <campaign>/code/examples/repair_fesom_constants.sbatch
#
# Task i repairs FIRST+10*i .. FIRST+10*i+9 (at most LAST); task 0 also takes
# the fx files, which belong to no year. The script skips files it has
# repaired before, so a failed or timed-out task can be submitted again with
# the same index (--array=<i>).
set -euo pipefail
: "${CODE:?}" "${PYENV:?}" "${MEMBER:?}" "${FIRST:?}" "${LAST:?}" "${WRITTEN_BEFORE:?}"
export PATH="$PYENV/bin:$PATH" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
lo=$((FIRST + 10 * SLURM_ARRAY_TASK_ID))
hi=$((lo + 9 < LAST ? lo + 9 : LAST))
if [ "$lo" -gt "$LAST" ]; then
  echo "task $SLURM_ARRAY_TASK_ID: nothing to do ($lo > $LAST)"
  exit 0
fi
fx=()
if [ "$SLURM_ARRAY_TASK_ID" -eq 0 ]; then
  fx=(--with-fx)
fi
python "$CODE/examples/repair_fesom_constants.py" "$MEMBER" "${fx[@]}" \
  --years "$lo-$hi" --written-before "$WRITTEN_BEFORE" --jobs "${JOBS:-16}" --apply
