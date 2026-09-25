#!/bin/bash
#SBATCH --job-name=pycmor-rlus-pyspy
#SBATCH --partition=compute
#SBATCH --account=ba0989
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=256G
#SBATCH --time=00:45:00
#SBATCH --output=pycmor_verify_rlus_1hr_pyspy_%j.log
#SBATCH --error=pycmor_verify_rlus_1hr_pyspy_%j.log

# rlus_1hr bench with an aggressive py-spy sampler that captures the
# pycmor main-process stack every 3 s during trigger_compute, so we can
# see where the 246 s of graph-overhead actually goes.

source ~/loadconda.sh
conda activate pycmor_py312

cd /work/ab0246/a270092/software/pycmor

PYCMOR_SCRATCH=/scratch/a/a270092/pycmor_tmp/$$
mkdir -p $PYCMOR_SCRATCH/prefect/storage
export PREFECT_HOME=$PYCMOR_SCRATCH/prefect
export PREFECT_LOCAL_STORAGE_PATH=$PYCMOR_SCRATCH/prefect/storage
export TMPDIR=$PYCMOR_SCRATCH
export OMP_NUM_THREADS=1

OUTROOT=${OUTROOT:-/scratch/a/a270092/pycmor_verify_rlus_1hr_pyspy}
mkdir -p "$OUTROOT"

sed -e "s|output_directory: .*|output_directory: $OUTROOT|" \
    examples/_verify_rlus_1hr.yaml > $PYCMOR_SCRATCH/yaml

PYSPY_DIR=$OUTROOT/_pyspy
mkdir -p "$PYSPY_DIR"

# Which log file will pycmor write? Use the SLURM-assigned name.
PY_LOG="$SLURM_SUBMIT_DIR/pycmor_verify_rlus_1hr_pyspy_${SLURM_JOB_ID}.log"

# Sampler: once every 3 s, dump py-spy stack of the main pycmor process
# whenever the last-seen Prefect event is 'trigger_compute'. Stores as
# numbered text files we can tally later.
sampler() {
    local main_pid=$1
    local n=0
    local in_tc=0
    while kill -0 "$main_pid" 2>/dev/null; do
        local phase=$(tail -80 "$PY_LOG" 2>/dev/null \
                      | grep -oE "Task run '[A-Za-z_]+-" | tail -1 | sed "s/Task run '//;s/-\$//")
        # Also detect show_data/save_dataset so we can stop gracefully
        if [ "$phase" = "trigger_compute" ]; then
            in_tc=1
            n=$((n+1))
            py-spy dump --pid "$main_pid" > "$PYSPY_DIR/stack_$(printf '%04d' $n).txt" 2>&1 || true
        elif [ "$in_tc" = "1" ] && [ "$phase" = "show_data" -o "$phase" = "save_dataset" ]; then
            # trigger_compute finished; we're done sampling
            break
        fi
        sleep 3
    done
}

echo "=== Start pycmor ==="
/usr/bin/time -v pycmor process $PYCMOR_SCRATCH/yaml &
WRAPPER_PID=$!
# Wait a beat so the Python child is spawned, then find the *real* pycmor
# Python PID (not the /usr/bin/time wrapper) for py-spy.
sleep 5
PYCMOR_PID=$(pgrep -u "$USER" -f "python.* pycmor process" | head -1)
[ -z "$PYCMOR_PID" ] && PYCMOR_PID=$(pgrep -u "$USER" -f "pycmor process" | grep -v "^$WRAPPER_PID$" | head -1)
echo "WRAPPER_PID=$WRAPPER_PID  PYCMOR_PID=$PYCMOR_PID"
sampler "$PYCMOR_PID" &
SAMPLER_PID=$!
wait "$WRAPPER_PID"
kill "$SAMPLER_PID" 2>/dev/null || true

echo
echo "=== py-spy samples captured: $(ls "$PYSPY_DIR" | wc -l) ==="
ls -la "$PYSPY_DIR" | head

echo
echo "=== Hot functions during trigger_compute (tally across all stacks) ==="
python3 <<PY
import os, re, collections
d = "$PYSPY_DIR"
if not os.path.isdir(d):
    print("no samples"); exit()
# Frames format: "    funcname (module/path.py:LINE)"
frame_counts = collections.Counter()
thread_counts = collections.Counter()
files = sorted(f for f in os.listdir(d) if f.endswith(".txt"))
for fn in files:
    txt = open(os.path.join(d, fn)).read()
    # Find each active thread's top 8 frames
    blocks = re.split(r"\nThread ", txt)
    for b in blocks:
        if "(active" not in b.split("\n", 1)[0] and "active+gil" not in b.split("\n", 1)[0]:
            continue
        frames = re.findall(r"^\s+([A-Za-z_][A-Za-z0-9_]*)\s+\(([^:]+):(\d+)\)", b, re.M)[:8]
        if not frames: continue
        thread_counts[fn] += 1
        for name, path, line in frames:
            frame_counts[f"{name} ({os.path.basename(path)}:{line})"] += 1
print(f"samples with an active thread: {sum(thread_counts.values())}/{len(files)}")
print()
print("top 20 stack frames by occurrence:")
for frame, c in frame_counts.most_common(20):
    print(f"  {c:4d}  {frame}")
PY

echo === DONE ===
