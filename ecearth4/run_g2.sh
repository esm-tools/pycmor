#!/usr/bin/env bash
# G2: render a9kk_g2.yaml.in and run pycmor on it.
# Usage: PYCMOR_WORK=<dir> ECE4_SAMPLES_DIR=<samples> run_g2.sh plan|run
# run on a compute or interactive node; env from env_mn5.sh.
set -euo pipefail

SRC=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
WORK=${PYCMOR_WORK:?set PYCMOR_WORK}
SAMPLES=${ECE4_SAMPLES_DIR:?set ECE4_SAMPLES_DIR}
DREQ_VERSION=${DREQ_VERSION:-v1.2.2.4}
METADATA=$WORK/cmip7_metadata/$DREQ_VERSION/metadata.json
RUN=$WORK/g2/$(date +%Y%m%d_%H%M%S)
PIXI=${PIXI:-$WORK/bin/pixi}

render() {
    sed -e "s|@SRC@|$SRC|g" -e "s|@METADATA@|$METADATA|g" \
        -e "s|@SAMPLES@|$SAMPLES|g" -e "s|@OUT@|$RUN/out|g" \
        "$SRC/ecearth4/a9kk_g2.yaml.in"
}

case ${1:-} in
    plan) render ;;
    run)
        mkdir -p "$RUN"
        render > "$RUN/a9kk_g2.yaml"
        echo "run dir $RUN"
        "$PIXI" run --frozen --manifest-path "$SRC/pyproject.toml" \
            pycmor process "$RUN/a9kk_g2.yaml" 2>&1 | tee "$RUN/pycmor.log"
        find "$RUN/out" -name '*.nc' | sort
        ;;
    *) sed -n '2,4p' "$0"; exit 2 ;;
esac
