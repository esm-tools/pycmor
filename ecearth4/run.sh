#!/usr/bin/env bash
# Render <case>.yaml.in and run pycmor on it (cases: a9kk_g2, t0i9_g3).
# Usage: run.sh plan|run <case>  (settings: ecearth4/settings.example)
# Output to <samples root>/<exp>/pycmor-out, logs to <samples root>/logs/<exp>
# (<exp> = case before "_"); OUT_DIR and LOG_DIR override.
# run on a compute or interactive node; env from env_mn5.sh.
set -euo pipefail

SRC=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
MODE=${1:-}
CASE=${2:-}
TEMPLATE=$SRC/ecearth4/$CASE.yaml.in
EXP=${CASE%%_*}
# Settings: <checkout parent>/ecearth4.env (outside the repo), or the file in
# ECEARTH4_SETTINGS. Lines NAME=value; variables already set win.
SETTINGS=${ECEARTH4_SETTINGS:-$SRC/../ecearth4.env}
if [ -f "$SETTINGS" ]; then
    while IFS='=' read -r key val; do
        case $key in ''|\#*) continue ;; esac
        [ -z "${!key:-}" ] && export "$key=$val"
    done < "$SETTINGS"
fi
WORK=${PYCMOR_WORK:?set PYCMOR_WORK in $SETTINGS}
ROOT=${ECE4_SAMPLES_ROOT:?set ECE4_SAMPLES_ROOT in $SETTINGS}
SAMPLES=${ECE4_SAMPLES_DIR:-$ROOT/samples}
DREQ_VERSION=${DREQ_VERSION:-v1.2.2.4}
METADATA=$WORK/cmip7_metadata/$DREQ_VERSION/metadata.json
TS=$(date +%Y%m%d_%H%M%S)
OUT=${OUT_DIR:-$ROOT/$EXP/pycmor-out}
LOGS=${LOG_DIR:-$ROOT/logs/$EXP}
TMP=$WORK/runs/tmp_$TS
PIXI=${PIXI:-$WORK/bin/pixi}
# Locked env only (no ~/.local); caches as env_mn5.sh.
export PYTHONNOUSERSITE=1
export PIXI_CACHE_DIR=$WORK/cache/pixi RATTLER_CACHE_DIR=$WORK/cache/rattler

render() {
    sed -e "s|@SRC@|$SRC|g" -e "s|@METADATA@|$METADATA|g" \
        -e "s|@SAMPLES@|$SAMPLES|g" -e "s|@ROOT@|$ROOT|g" -e "s|@OUT@|$OUT|g" "$TEMPLATE"
}

if [ ! -d "$SAMPLES" ]; then
    echo "samples dir not found: '$SAMPLES' (check ECE4_SAMPLES_ROOT in $SETTINGS)" >&2
    exit 2
fi
if [ -z "$CASE" ] || [ ! -f "$TEMPLATE" ] || { [ "$MODE" != plan ] && [ "$MODE" != run ]; }; then
    sed -n '2,4p' "$0"
    ls "$SRC"/ecearth4/*.yaml.in 2>/dev/null | sed 's|.*/||; s|\.yaml\.in$||; s|^|  case: |'
    exit 2
fi

case $MODE in
    plan) echo "# settings $SETTINGS"; echo "# out  $OUT"; echo "# logs $LOGS/${CASE}_$TS.yaml, ${CASE}_$TS.log"; render ;;
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
