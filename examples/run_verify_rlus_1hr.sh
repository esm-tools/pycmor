#!/bin/bash
#SBATCH --job-name=pycmor-verify-rlus-1hr
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=256G
#SBATCH --time=00:45:00
#SBATCH --output=pycmor_verify_rlus_1hr_%j.log
#SBATCH --error=pycmor_verify_rlus_1hr_%j.log

# Single-rule HR 1-hourly rlus benchmark, with second-by-second instrumentation
# so we can see where save_dataset time goes (compute vs write vs post-cleanup).

source ~/loadconda.sh
conda activate pycmor_py312

cd /work/ab0246/a270092/software/pycmor

PYCMOR_SCRATCH=/scratch/a/a270092/pycmor_tmp/$$
mkdir -p $PYCMOR_SCRATCH/prefect/storage
export PREFECT_HOME=$PYCMOR_SCRATCH/prefect
export PREFECT_LOCAL_STORAGE_PATH=$PYCMOR_SCRATCH/prefect/storage
export TMPDIR=$PYCMOR_SCRATCH
export OMP_NUM_THREADS=1

OUTROOT=${OUTROOT:-/scratch/a/a270092/pycmor_verify_rlus_1hr}
mkdir -p "$OUTROOT"
command -v lfs >/dev/null && lfs setstripe -c 8 "$OUTROOT" 2>/dev/null || true

sed -e "s|output_directory: .*|output_directory: $OUTROOT|" \
    examples/_verify_rlus_1hr.yaml > $PYCMOR_SCRATCH/yaml

echo "=== Input file ==="
ls -lh /work/bb1469/a270092/runtime/awiesm3-develop/HR_test_01/outdata/oifs/atmos_1h_sfc_rlus_*.nc

# -------- Instrumentation --------
# Sampler: every 1 s, record (elapsed, RSS, CPU%, threads, output-file size).
SAMPLER_OUT=${SAMPLER_OUT:-$OUTROOT/_sampler.tsv}
SAMPLER_PID_FILE=$PYCMOR_SCRATCH/sampler.pid

start_sampler() {
    local pid=$1
    {
        printf "t_elapsed_s\tRSS_MB\tCPU%%\tthreads\tout_MB\tphase\n" > "$SAMPLER_OUT"
        local t0=$(date +%s)
        while kill -0 "$pid" 2>/dev/null; do
            local now=$(date +%s)
            local dt=$(( now - t0 ))
            # RSS (kB) and CPU%, threads
            local stat=$(ps -p "$pid" -o rss=,pcpu=,nlwp= 2>/dev/null | awk '{printf "%d %s %s", $1, $2, $3}')
            local rss_mb=$(echo "$stat" | awk '{printf "%.0f", $1/1024}')
            local cpu=$(echo "$stat" | awk '{print $2}')
            local threads=$(echo "$stat" | awk '{print $3}')
            # Output size (sum of .nc bytes, MB)
            local out_bytes=$(find "$OUTROOT" -maxdepth 2 -name "*.nc" -printf "%s\n" 2>/dev/null | awk '{s+=$1} END{printf "%.0f", s/1048576}')
            # Phase guess from most recent Prefect event in the log
            local phase=$(tail -50 "$SLURM_SUBMIT_DIR/pycmor_verify_rlus_1hr_${SLURM_JOB_ID}.log" 2>/dev/null \
                          | grep -oE "Task run '[A-Za-z_]+-" | tail -1 | tr -d "'" | sed 's/Task run //;s/-$//')
            [ -z "$phase" ] && phase="init"
            printf "%d\t%s\t%s\t%s\t%s\t%s\n" "$dt" "${rss_mb:-0}" "${cpu:-0}" "${threads:-0}" "${out_bytes:-0}" "$phase" >> "$SAMPLER_OUT"
            sleep 1
        done
    } &
    echo $! > "$SAMPLER_PID_FILE"
}

echo "=== Starting pycmor ==="
/usr/bin/time -v pycmor process $PYCMOR_SCRATCH/yaml &
PYCMOR_PID=$!
start_sampler "$PYCMOR_PID"
wait "$PYCMOR_PID"
PYCMOR_RC=$?

# Stop sampler
[ -f "$SAMPLER_PID_FILE" ] && kill "$(cat "$SAMPLER_PID_FILE")" 2>/dev/null || true

# -------- Post-run summary --------
echo
echo "=== Pipeline phase timings (from Prefect log) ==="
LOGFILE=$SLURM_SUBMIT_DIR/pycmor_verify_rlus_1hr_${SLURM_JOB_ID}.log
python3 - "$LOGFILE" <<'PY'
import re, sys
lines = open(sys.argv[1]).read().splitlines()
prev_ts = None
prev_name = None
rows = []
for l in lines:
    m = re.match(r"(\d\d:\d\d:\d\d\.\d+).*Task run '([^']+)' - Finished", l)
    if not m: continue
    ts_s, name = m.group(1), m.group(2)
    h,mn,s = ts_s.split(":")
    t = int(h)*3600 + int(mn)*60 + float(s)
    if prev_ts is not None:
        rows.append((name, t - prev_ts))
    prev_ts = t
    prev_name = name
for name, dt in rows:
    print(f"  {dt:8.2f}s  {name}")
PY

echo
echo "=== Sampler summary (every sample = 1s) ==="
[ -s "$SAMPLER_OUT" ] && python3 - "$SAMPLER_OUT" <<'PY'
import csv, sys
rows = list(csv.DictReader(open(sys.argv[1]), delimiter='\t'))
if not rows:
    print("(sampler produced no data)"); sys.exit(0)
# Peak RSS
peak = max(int(r['RSS_MB']) for r in rows)
print(f"peak RSS: {peak} MB")
# Output-growth rate by phase
by_phase = {}
for r in rows:
    by_phase.setdefault(r['phase'], []).append(r)
print("phase          samples   duration(s)  out_delta(MB)  inst_rate(MB/s)")
for ph, rs in by_phase.items():
    dur = int(rs[-1]['t_elapsed_s']) - int(rs[0]['t_elapsed_s'])
    delta = int(rs[-1]['out_MB']) - int(rs[0]['out_MB'])
    rate = delta/dur if dur > 0 else 0
    print(f"  {ph:12s}  {len(rs):5d}      {dur:5d}       {delta:6d}       {rate:7.1f}")
PY
echo
echo "=== Raw sampler: $SAMPLER_OUT ==="
head -1 "$SAMPLER_OUT" 2>/dev/null
tail -20 "$SAMPLER_OUT" 2>/dev/null

echo
echo "=== Output ==="
find "$OUTROOT" -name '*.nc' -printf '%s %p\n' | sort -n
exit $PYCMOR_RC
