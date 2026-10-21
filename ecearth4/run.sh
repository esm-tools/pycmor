#!/usr/bin/env bash
# Render <case>.yaml.in and run pycmor on it (cases: a9kk_g2, t0i9_g3).
# Usage: PYCMOR_WORK=<dir> ECE4_SAMPLES_DIR=<samples> [OUT_DIR=<dir>] [LOG_DIR=<dir>] run.sh plan|run <case>
# OUT_DIR: CMIP7 tree root; LOG_DIR: rendered yaml and pycmor log (timestamped).
# run on a compute or interactive node; env from env_mn5.sh.
set -euo pipefail

SRC=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
MODE=${1:-}
CASE=${2:-}
TEMPLATE=$SRC/ecearth4/$CASE.yaml.in
WORK=${PYCMOR_WORK:?set PYCMOR_WORK}
SAMPLES=${ECE4_SAMPLES_DIR:?set ECE4_SAMPLES_DIR}
DREQ_VERSION=${DREQ_VERSION:-v1.2.2.4}
METADATA=$WORK/cmip7_metadata/$DREQ_VERSION/metadata.json
TS=$(date +%Y%m%d_%H%M%S)
OUT=${OUT_DIR:-$WORK/runs/out}
LOGS=${LOG_DIR:-$WORK/runs/logs}
TMP=$WORK/runs/tmp_$TS
PIXI=${PIXI:-$WORK/bin/pixi}
# Locked env only (no ~/.local); caches as env_mn5.sh.
export PYTHONNOUSERSITE=1
export PIXI_CACHE_DIR=$WORK/cache/pixi RATTLER_CACHE_DIR=$WORK/cache/rattler

render() {
    sed -e "s|@SRC@|$SRC|g" -e "s|@METADATA@|$METADATA|g" \
        -e "s|@SAMPLES@|$SAMPLES|g" -e "s|@OUT@|$OUT|g" "$TEMPLATE"
}

if [ -z "$CASE" ] || [ ! -f "$TEMPLATE" ] || { [ "$MODE" != plan ] && [ "$MODE" != run ]; }; then
    sed -n '2,4p' "$0"
    ls "$SRC"/ecearth4/*.yaml.in 2>/dev/null | sed 's|.*/||; s|\.yaml\.in$||; s|^|  case: |'
    exit 2
fi

case $MODE in
    plan) echo "# out  $OUT"; echo "# logs $LOGS/${CASE}_$TS.yaml, ${CASE}_$TS.log"; render ;;
    run)
        mkdir -p "$OUT" "$LOGS" "$TMP"
        render > "$LOGS/${CASE}_$TS.yaml"
        # pycmor writes ./logs/pycmor-process_*.log: run in TMP, then move it.
        (cd "$TMP" && "$PIXI" run --frozen --manifest-path "$SRC/pyproject.toml" \
            pycmor process "$LOGS/${CASE}_$TS.yaml") || rc=$?
        for f in "$TMP"/logs/pycmor-process_*.log; do
            [ -e "$f" ] && mv "$f" "$LOGS/${CASE}_$TS.log"
        done
        rm -rf "$TMP"
        echo "log  $LOGS/${CASE}_$TS.log"
        find "$OUT" -name '*.nc' -newer "$LOGS/${CASE}_$TS.yaml" | sort
        exit "${rc:-0}"
        ;;
esac
