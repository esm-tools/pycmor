#!/bin/bash
set -euo pipefail
exec "$1" "$2" task "$3" --manifest "$4" --index "${SLURM_ARRAY_TASK_ID:?Slurm array task required}"
