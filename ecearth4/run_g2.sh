#!/usr/bin/env bash
# G2: render a9kk_g2.yaml.in and run pycmor on it.
# Usage: PYCMOR_WORK=<dir> ECE4_SAMPLES_DIR=<samples> [G2_OUT=<dir>] [G2_LOGS=<dir>] run_g2.sh plan|run
# G2_OUT: CMIP7 tree root; G2_LOGS: rendered yaml and pycmor log (timestamped).
# run on a compute or interactive node; env from env_mn5.sh.
set -euo pipefail

SRC=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
WORK=${PYCMOR_WORK:?set PYCMOR_WORK}
SAMPLES=${ECE4_SAMPLES_DIR:?set ECE4_SAMPLES_DIR}
DREQ_VERSION=${DREQ_VERSION:-v1.2.2.4}
METADATA=$WORK/cmip7_metadata/$DREQ_VERSION/metadata.json
TS=$(date +%Y%m%d_%H%M%S)
OUT=${G2_OUT:-$WORK/g2/out}
LOGS=${G2_LOGS:-$WORK/g2/logs}
TMP=$WORK/g2/run_$TS
PIXI=${PIXI:-$WORK/bin/pixi}
# Locked env only (no ~/.local); caches as env_mn5.sh.
export PYTHONNOUSERSITE=1
export PIXI_CACHE_DIR=$WORK/cache/pixi RATTLER_CACHE_DIR=$WORK/cache/rattler

render() {
    sed -e "s|@SRC@|$SRC|g" -e "s|@METADATA@|$METADATA|g" \
        -e "s|@SAMPLES@|$SAMPLES|g" -e "s|@OUT@|$OUT|g" \
        "$SRC/ecearth4/a9kk_g2.yaml.in"
}

case ${1:-} in
    plan) echo "# out  $OUT"; echo "# logs $LOGS/a9kk_g2_$TS.yaml, pycmor_g2_$TS.log"; render ;;
    run)
        mkdir -p "$OUT" "$LOGS" "$TMP"
        render > "$LOGS/a9kk_g2_$TS.yaml"
        # pycmor writes ./logs/pycmor-process_*.log: run in TMP, then move it.
        (cd "$TMP" && "$PIXI" run --frozen --manifest-path "$SRC/pyproject.toml" \
            pycmor process "$LOGS/a9kk_g2_$TS.yaml") || rc=$?
        for f in "$TMP"/logs/pycmor-process_*.log; do
            [ -e "$f" ] && mv "$f" "$LOGS/pycmor_g2_$TS.log"
        done
        rm -rf "$TMP"
        echo "log  $LOGS/pycmor_g2_$TS.log"
        find "$OUT" -name '*.nc' -newer "$LOGS/a9kk_g2_$TS.yaml" | sort
        exit "${rc:-0}"
        ;;
    *) sed -n '2,4p' "$0"; exit 2 ;;
esac
