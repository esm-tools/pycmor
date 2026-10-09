#!/bin/bash
#SBATCH --job-name=pycmor-repack-720
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=00:30:00
#SBATCH --output=pycmor_repack_720_%j.log
#SBATCH --error=pycmor_repack_720_%j.log

set -euo pipefail
source ~/loadconda.sh
conda activate pycmor_py312
cd /work/ab0246/a270092/software/pycmor

INPUT=/work/bb1469/a270092/runtime/awiesm3-develop/Final_CMIP7_IO_Test_01/outdata/oifs/atmos_6h_pl7h_ua_1587-1587.nc
OUTROOT=/scratch/a/a270092/pycmor_repack_720
OUTDIR=$OUTROOT/${SLURM_JOB_ID:-$$}
mkdir -p "$OUTDIR"
command -v lfs >/dev/null && lfs setstripe -c 8 "$OUTDIR" 2>/dev/null || true
OUTPUT=$OUTDIR/$(basename $INPUT .nc)_repacked720.nc

echo "=== repack-720 ==="
echo "input: $(ls -lh $INPUT)"
echo "output: $OUTPUT"
date +%s.%N
/usr/bin/time -v python3 examples/repack_one.py "$INPUT" "$OUTPUT" --time-chunk 720 --slab 720
date +%s.%N
echo ""
echo "=== chunks AFTER ==="
ncdump -hs "$OUTPUT" 2>/dev/null | grep --color=never -E "ua:_ChunkSizes|ua:_Filter" | head -3
echo ""
ls -lh "$OUTPUT"
