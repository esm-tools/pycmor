#!/bin/bash
#SBATCH --job-name=pycmor-bench-pyconcat
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --time=00:30:00
#SBATCH --output=pycmor_bench_pyconcat_%j.log
#SBATCH --error=pycmor_bench_pyconcat_%j.log

set -euo pipefail
source ~/loadconda.sh
conda activate pycmor_py312

SRC=/scratch/a/a270092/pycmor_bench_ua_6hr_v13/24676992
DST_DIR=/scratch/a/a270092/pycmor_bench_pyconcat/${SLURM_JOB_ID:-$$}
mkdir -p "$DST_DIR"
DST=$DST_DIR/ua_combined.nc

echo "node: $(hostname), $(nproc) cores, $(free -g | awk '/^Mem:/{print $2}') GB RAM"
ls $SRC/ua_6hr_pl7h_slab*.nc | wc -l
echo "src first: $(ls $SRC/ua_6hr_pl7h_slab*.nc | head -1)"

# cgroup-v2 watchdog
WATCH_LOG=$DST_DIR/cgroup_mem_v2.tsv
JOB=${SLURM_JOB_ID:-$$}
CG_PATH=/sys/fs/cgroup/system.slice/slurmstepd.scope/job_$JOB/memory.current
(
  echo -e "epoch\tmem_GB"
  while true; do
    [ -r "$CG_PATH" ] && m=$(awk '{printf "%.2f", $1/1024/1024/1024}' "$CG_PATH" 2>/dev/null) || m=""
    [ -n "$m" ] && echo -e "$(date +%s)\t$m"
    sleep 2
  done
) > "$WATCH_LOG" &
WATCH_PID=$!
trap "kill $WATCH_PID 2>/dev/null || true" EXIT

echo "=== start ==="
date +%s.%N
/usr/bin/time -v python3 - <<'PY'
import glob, os, time
import netCDF4 as nc

SRC = "/scratch/a/a270092/pycmor_bench_ua_6hr_v13/24676992"
DST = os.environ.get("DST") or "/tmp/ua_combined.nc"
src_files = sorted(glob.glob(f"{SRC}/ua_6hr_pl7h_slab*.nc"))
print(f"src: {len(src_files)} files")

# Discover total time length
total_time = 0
per_file_time = []
with nc.Dataset(src_files[0]) as s0:
    time_dim_names = [d for d, dim in s0.dimensions.items() if d.lower().startswith("time")]
    print(f"time dim candidates: {time_dim_names}")
    time_dim = "time"
    if time_dim not in s0.dimensions:
        time_dim = time_dim_names[0]
    print(f"using time dim '{time_dim}'")

for sp in src_files:
    with nc.Dataset(sp) as s:
        nt = s.dimensions[time_dim].size
        per_file_time.append(nt)
        total_time += nt
print(f"total time = {total_time}")

t0 = time.time()
# Create dst with structure copied from first file (time as unlimited)
with nc.Dataset(src_files[0]) as s0:
    with nc.Dataset(DST, "w", format="NETCDF4") as d:
        # globals
        d.setncatts({a: s0.getncattr(a) for a in s0.ncattrs()})
        # dims
        for name, dim in s0.dimensions.items():
            if name == time_dim:
                d.createDimension(name, None)
            else:
                d.createDimension(name, len(dim) if not dim.isunlimited() else None)
        # vars (structure only)
        for name, var in s0.variables.items():
            kw = {}
            chs = var.chunking()
            if isinstance(chs, list):
                kw["chunksizes"] = tuple(chs)
            # deflate / blosc come from filters; preserve as much as we can
            try:
                fl = var.filters() or {}
                if fl.get("zlib"):
                    kw["zlib"] = True
                    kw["complevel"] = fl.get("complevel", 1)
                    kw["shuffle"] = fl.get("shuffle", False)
            except Exception:
                pass
            fv = None
            try:
                fv = var._FillValue
            except AttributeError:
                pass
            v = d.createVariable(name, var.datatype, var.dimensions, fill_value=fv, **kw)
            v.setncatts({a: var.getncattr(a) for a in var.ncattrs() if a != "_FillValue"})
print(f"created dst structure in {time.time()-t0:.2f}s")

# Copy data slab by slab, appending along time
offset = 0
for i, sp in enumerate(src_files):
    nt = per_file_time[i]
    t1 = time.time()
    with nc.Dataset(sp) as s, nc.Dataset(DST, "a") as d:
        for name, sv in s.variables.items():
            dv = d.variables[name]
            if time_dim in sv.dimensions:
                idx = sv.dimensions.index(time_dim)
                sl = [slice(None)] * sv.ndim
                sl[idx] = slice(offset, offset + nt)
                dv[tuple(sl)] = sv[:]
            else:
                if i == 0:
                    dv[:] = sv[:]
    offset += nt
    # fadvise both src and dst to encourage page-cache reclaim
    for path in (sp, DST):
        try:
            fd = os.open(path, os.O_RDONLY)
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            finally:
                os.close(fd)
        except Exception as exc:
            print(f"fadvise {path} failed: {exc}")
    print(f"slab {i+1}/{len(src_files)} appended in {time.time()-t1:.2f}s, offset={offset}")
print(f"DONE in {time.time()-t0:.2f}s")
PY
date +%s.%N

echo "=== output ==="
ls -lh $DST
echo "=== peak ==="
awk -F'\t' 'NR>1 && $2>m{m=$2} END{printf "overall peak: %.2f GB\n", m+0}' "$WATCH_LOG"
