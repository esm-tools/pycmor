#!/bin/bash
# Find any pycmor-hr SLURM array tasks that ended in CANCELLED+ state
# (typically from Levante's AssocMaxJobsLimit sweep on pending jobs) and
# resubmit them individually so they get fresh slots.
#
# Usage:
#   resubmit_cancelled_shards.sh <workdir> [<year>]
#
# Example:
#   resubmit_cancelled_shards.sh /scratch/$USER/pycmor_hr/cli92_full 1851
#
# The workdir is the same path passed as positional 3 to
# submit_hr_year_shards.sh. Year defaults to 1851.
#
# Strategy: parse the original submit log for jid=<id> per tier, sacct
# each, find CANCELLED+ array task indices, and resubmit each as its
# own size-1 array. This is intentionally NOT automatic — call this
# script after a sweep finishes to catch any losers.
set -euo pipefail

WORKDIR="${1:?usage: $0 <workdir> [<year>]}"
YEAR="${2:-1851}"
SUBMIT_LOG="${WORKDIR}.submit.log"

if [ ! -f "$SUBMIT_LOG" ]; then
  echo "ABORT: submit log not found at $SUBMIT_LOG"
  echo "       (expected to live next to the workdir)"
  exit 2
fi

HERE="$(cd "$(dirname "$0")" && pwd)"
RUN_ABS=$(awk '/source run:/ {print $3}' "$SUBMIT_LOG")
test -d "$RUN_ABS" || { echo "ABORT: source run from log ($RUN_ABS) missing"; exit 2; }

resubmitted=0
inspected=0
echo "=== scanning $SUBMIT_LOG for cancelled tasks ==="

# Each tier line in the submit log looks like:
#   submitted pycmor-hr-<tier>-y<year>-sh  jid=<id>  shards=<n>  ...
while read -r jobname jid_field shards_field _rest; do
  jid="${jid_field#jid=}"
  num_shards="${shards_field#shards=}"
  short_tier="${jobname#pycmor-hr-}"; short_tier="${short_tier%-y*}"

  # Pull array-task states for this jid
  state_lines=$(sacct -j "$jid" -X -P --format=JobID,State,Reason 2>/dev/null \
                | tail -n +2)
  while IFS='|' read -r task_id state reason; do
    inspected=$((inspected + 1))
    case "$state" in
      CANCELLED*|CANCELLED+|"CANCELLED by 0")
        idx="${task_id##*_}"
        echo "  found cancelled: $task_id ($state, reason=$reason) idx=$idx"
        shards_dir="$WORKDIR/shards/$short_tier"
        tier_outsub="."
        # Resubmit just that index as its own size-1 array.
        new_jid=$(sbatch --parsable \
              --array="${idx}-${idx}" \
              -J "$jobname-resubmit" \
              --time=08:00:00 \
              --export=ALL,SHARD_DRS=on \
              "$HERE/run_hr_shard.sh" \
              "$shards_dir" "$RUN_ABS" "$YEAR" "$tier_outsub" 2>&1) \
          || { echo "    sbatch failed"; continue; }
        echo "    resubmitted as $new_jid"
        resubmitted=$((resubmitted + 1))
        ;;
    esac
  done <<< "$state_lines"
done < <(grep --color=never "submitted pycmor-hr-" "$SUBMIT_LOG" \
         | awk '{print $2, $3, $4}')

echo
echo "Inspected $inspected array tasks; resubmitted $resubmitted CANCELLED+ tasks."
