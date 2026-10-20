#!/usr/bin/env bash
# pycmor env on MN5 from pixi.lock, plus CMIP7 DReq metadata.
# Usage: PYCMOR_WORK=<dir on gpfs> env_mn5.sh plan|install|check
# install needs internet (login node with access).
set -euo pipefail

PIXI_VERSION=${PIXI_VERSION:-0.81.0}
DREQ_VERSION=${DREQ_VERSION:-v1.2.2.4}
DREQ_API_VERSION=${DREQ_API_VERSION:-1.5.1}

SRC=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
WORK=${PYCMOR_WORK:?set PYCMOR_WORK to a work dir (not HOME)}
PIXI=$WORK/bin/pixi
DREQ_VENV=$WORK/dreq-venv
METADATA=$WORK/cmip7_metadata/$DREQ_VERSION/metadata.json

# Caches off HOME (quota).
export PIXI_CACHE_DIR=$WORK/cache/pixi RATTLER_CACHE_DIR=$WORK/cache/rattler
export UV_CACHE_DIR=$WORK/cache/uv PIP_CACHE_DIR=$WORK/cache/pip

plan() {
    cat <<EOF
src        $SRC ($(git -C "$SRC" rev-parse --short HEAD))
work       $WORK
pixi       $PIXI_VERSION -> $PIXI
env        $SRC/.pixi/envs/default (pixi install --frozen)
submodules $(git -C "$SRC" submodule status | awk '{printf "%s@%.8s ", $2, $1}')
dreq       CMIP7-data-request-api $DREQ_API_VERSION in $DREQ_VENV
metadata   $METADATA
EOF
}

install() {
    curl -fsS -o /dev/null https://conda.anaconda.org/conda-forge/channeldata.json \
        || { echo "no internet: run on a login node with access" >&2; exit 1; }
    mkdir -p "$WORK/bin" "$(dirname "$METADATA")"
    if [ ! -x "$PIXI" ]; then
        curl -fsSL "https://github.com/prefix-dev/pixi/releases/download/v$PIXI_VERSION/pixi-x86_64-unknown-linux-musl.tar.gz" \
            | tar xz -C "$WORK/bin" pixi
    fi
    git -C "$SRC" submodule update --init
    # --frozen: the lock is older than pyproject.toml (--locked refuses it).
    "$PIXI" install --frozen --manifest-path "$SRC/pyproject.toml"
    # DReq API is not in the lock; separate venv, only for metadata.json.
    if [ ! -x "$DREQ_VENV/bin/export_dreq_lists_json" ]; then
        "$PIXI" run --frozen --manifest-path "$SRC/pyproject.toml" python -m venv "$DREQ_VENV"
        "$DREQ_VENV/bin/pip" install -q "CMIP7-data-request-api==$DREQ_API_VERSION"
    fi
    if [ ! -s "$METADATA" ]; then
        tmp=$(mktemp -d)
        "$DREQ_VENV/bin/export_dreq_lists_json" -a "$DREQ_VERSION" "$tmp/experiments.json" -m "$METADATA"
        rm -rf "$tmp"
    fi
    check
}

check() {
    "$PIXI" run --frozen --manifest-path "$SRC/pyproject.toml" python -c \
        'import sys, pycmor; print("pycmor", pycmor.__version__, "python", sys.version.split()[0])'
    python3 - "$METADATA" <<'EOF'
import hashlib, json, sys
p = sys.argv[1]
h = json.load(open(p))["Header"]
print("metadata", h["dreq content version"], h["no. of variables"], "variables, api", h["dreq api version"])
print("sha256", hashlib.sha256(open(p, "rb").read()).hexdigest())
EOF
}

case ${1:-} in
    plan|install|check) "$1" ;;
    *) sed -n '2,4p' "$0"; exit 2 ;;
esac
