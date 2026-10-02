#!/bin/bash
#SBATCH --job-name=pycmor-bench-ncrcat-blosc
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --time=00:30:00
#SBATCH --output=pycmor_bench_ncrcat_blosc_%j.log
#SBATCH --error=pycmor_bench_ncrcat_blosc_%j.log

set -euo pipefail

# Find a newer ncrcat that may have blosc support, plus the HDF5 BLOSC plugin.
# nco 5.0.6 in the spack module fails on filter id 32001 (blosc).
# Try in order: spack newer nco, conda envs.
NCRCAT=""
for cand in \
    /sw/spack-levante/miniforge3-25.11.0-1-Linux-x86_64-crksqt/bin/ncrcat \
    /sw/spack-levante/miniforge3-25.9.1-0-Linux-x86_64-oqcirx/bin/ncrcat \
    /sw/spack-levante/miniforge3-24.11.3-0-Linux-x86_64-ftdezc/bin/ncrcat \
    /sw/spack-levante/mambaforge-23.11.0-0-Linux-x86_64-befbel/bin/ncrcat \
    /sw/spack-levante/nco-5.0.6-3xkdth/bin/ncrcat ; do
  [ -x "$cand" ] || continue
  ver=$("$cand" --version 2>&1 | head -1 || true)
  echo "candidate: $cand  ($ver)"
done

NCRCAT=/sw/spack-levante/miniforge3-25.11.0-1-Linux-x86_64-crksqt/bin/ncrcat
NCKS=/sw/spack-levante/miniforge3-25.11.0-1-Linux-x86_64-crksqt/bin/ncks
echo "using $NCKS / $NCRCAT"

# HDF5 plugin path: c-blosc HDF5 filter is shipped with conda's hdf5 build
# in the same env. Search common spots.
for cand in \
    /sw/spack-levante/miniforge3-25.11.0-1-Linux-x86_64-crksqt/lib/hdf5/plugin \
    /sw/spack-levante/miniforge3-25.11.0-1-Linux-x86_64-crksqt/lib \
    /work/ab0246/a270092/software/miniforge3/envs/pycmor_py312/lib/hdf5/plugin \
    /work/ab0246/a270092/software/miniforge3/envs/pycmor_py312/lib ; do
  [ -d "$cand" ] || continue
  if ls "$cand"/libh5*blosc* "$cand"/libblosc* 2>/dev/null | head -3; then
    export HDF5_PLUGIN_PATH=$cand
    echo "set HDF5_PLUGIN_PATH=$cand"
    break
  fi
done
echo "HDF5_PLUGIN_PATH=${HDF5_PLUGIN_PATH:-(unset)}"

SRC=/scratch/a/a270092/pycmor_bench_ua_6hr_v13/24676992
DST_DIR=/scratch/a/a270092/pycmor_bench_ncrcat_blosc/${SLURM_JOB_ID:-$$}
mkdir -p "$DST_DIR/rec"
DST=$DST_DIR/ua_combined.nc
echo "src: $(ls $SRC/ua_6hr_pl7h_slab*.nc | wc -l) files"

# cgroup-v2 watchdog
WATCH_LOG=$DST_DIR/cgroup_mem_v2.tsv
JOB=${SLURM_JOB_ID:-$$}
CG_PATH=/sys/fs/cgroup/system.slice/slurmstepd.scope/job_$JOB/memory.current
(
  echo -e "epoch\tmem_GB\tphase"
  while true; do
    [ -r "$CG_PATH" ] && m=$(awk '{printf "%.2f", $1/1024/1024/1024}' "$CG_PATH" 2>/dev/null) || m=""
    [ -n "$m" ] && echo -e "$(date +%s)\t$m\t${PHASE:-?}"
    sleep 2
  done
) > "$WATCH_LOG" &
WATCH_PID=$!
trap "kill $WATCH_PID 2>/dev/null || true" EXIT

echo "=== pass 1: ncks --mk_rec_dmn time ==="
date +%s.%N
PHASE=ncks
export PHASE
/usr/bin/time -v bash -c "
  for f in $SRC/ua_6hr_pl7h_slab*.nc; do
    $NCKS -O --mk_rec_dmn time \$f $DST_DIR/rec/\$(basename \$f) || exit 1
  done
"
date +%s.%N

echo "=== pass 2: ncrcat ==="
date +%s.%N
PHASE=ncrcat
export PHASE
/usr/bin/time -v $NCRCAT -O $DST_DIR/rec/ua_6hr_pl7h_slab*.nc $DST
date +%s.%N

echo "=== output ==="
ls -lh $DST
echo "=== peak ==="
awk -F'\t' 'NR>1 && $2>m{m=$2} END{printf "overall peak: %.2f GB\n", m+0}' "$WATCH_LOG"
echo "=== mem by phase ==="
awk -F'\t' 'NR>1 {if ($2>p[$3]) p[$3]=$2} END{for (k in p) printf "%-8s peak %.2f GB\n", k, p[k]}' "$WATCH_LOG"

# Cleanup intermediate
rm -rf "$DST_DIR/rec"
