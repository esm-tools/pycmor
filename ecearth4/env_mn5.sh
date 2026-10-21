#!/usr/bin/env bash
# pycmor env on MN5 from pixi.lock, plus CMIP7 DReq metadata.
# Usage: env_mn5.sh plan|install|check  (settings: ecearth4/settings.example)
# install needs internet (login node with access).
set -euo pipefail

PIXI_VERSION=${PIXI_VERSION:-0.81.0}
# sha256 of the 0.81.0 musl tarball; set both when changing the version.
PIXI_SHA256=${PIXI_SHA256:-7aa3ec39aecceff9062fa2ed4d42cbaa0bdc25ddea727d048e061cf188d434f6}
DREQ_VERSION=${DREQ_VERSION:-v1.2.2.4}
DREQ_API_VERSION=${DREQ_API_VERSION:-1.5.1}

SRC=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
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
PIXI=$WORK/bin/pixi
DREQ_VENV=$WORK/dreq-venv
METADATA=$WORK/cmip7_metadata/$DREQ_VERSION/metadata.json
# cmip7-cmor-common checkout, needed by ecearth4/steps.py.
COMMON=${CMIP7_CMOR_COMMON:-}
COMMON_MIN=0.3.0

# Locked env only: no ~/.local packages (pip --user) on top of it.
export PYTHONNOUSERSITE=1

# Caches off HOME (quota).
export PIXI_CACHE_DIR=$WORK/cache/pixi RATTLER_CACHE_DIR=$WORK/cache/rattler
export UV_CACHE_DIR=$WORK/cache/uv PIP_CACHE_DIR=$WORK/cache/pip

plan() {
    cat <<EOF
settings   $SETTINGS $([ -f "$SETTINGS" ] && echo found || echo missing)
src        $SRC ($(git -C "$SRC" rev-parse --short HEAD))
work       $WORK
pixi       $PIXI_VERSION -> $PIXI
env        $SRC/.pixi/envs/default (pixi install --frozen)
submodules $(git -C "$SRC" submodule status | awk '{printf "%s@%.8s ", $2, $1}')
dreq       CMIP7-data-request-api $DREQ_API_VERSION in $DREQ_VENV
metadata   $METADATA
common     ${COMMON:-not set} $([ -n "$COMMON" ] && git -C "$COMMON" log -1 --format="(%h %s)" 2>/dev/null)
EOF
}

install() {
    [ -d "$COMMON" ] || { echo "CMIP7_CMOR_COMMON not found: '$COMMON' (set it in $SETTINGS)" >&2; exit 1; }
    curl -fsS -o /dev/null https://conda.anaconda.org/conda-forge/channeldata.json \
        || { echo "no internet: run on a login node with access" >&2; exit 1; }
    mkdir -p "$WORK/bin" "$(dirname "$METADATA")"
    if [ ! -x "$PIXI" ]; then
        tgz=$(mktemp)
        curl -fsSL -o "$tgz" "https://github.com/prefix-dev/pixi/releases/download/v$PIXI_VERSION/pixi-x86_64-unknown-linux-musl.tar.gz"
        echo "$PIXI_SHA256  $tgz" | sha256sum -c --quiet || { echo "pixi checksum mismatch" >&2; rm -f "$tgz"; exit 1; }
        tar xzf "$tgz" -C "$WORK/bin" pixi
        rm -f "$tgz"
    fi
    git -C "$SRC" submodule update --init
    # --frozen: the lock is older than pyproject.toml (--locked refuses it).
    "$PIXI" install --frozen --manifest-path "$SRC/pyproject.toml"
    # DReq API is not in the lock; separate venv, only for metadata.json.
    if [ ! -x "$DREQ_VENV/bin/export_dreq_lists_json" ]; then
        "$PIXI" run --frozen --manifest-path "$SRC/pyproject.toml" python -m venv "$DREQ_VENV"
        "$DREQ_VENV/bin/pip" install -q "CMIP7-data-request-api==$DREQ_API_VERSION"
    fi
    # common deps (numpy, netCDF4) are in the lock: --no-deps keeps it intact.
    "$PIXI" run --frozen --manifest-path "$SRC/pyproject.toml" python -m ensurepip > /dev/null
    "$PIXI" run --frozen --manifest-path "$SRC/pyproject.toml" python -m pip install -q --no-deps "$COMMON"
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
    # Every module must come from the env (sys.prefix), not from ~/.local.
    "$PIXI" run --frozen --manifest-path "$SRC/pyproject.toml" python -c \
        'import sys, numpy, xarray; [print(m.__name__, m.__version__, "ok" if m.__file__.startswith(sys.prefix) else "OUTSIDE ENV " + m.__file__) for m in (numpy, xarray)]'
    "$PIXI" run --frozen --manifest-path "$SRC/pyproject.toml" python -c '
import sys
from importlib.metadata import version
v = version("cmip7-cmor-common")
ok = tuple(map(int, v.split(".")[:3])) >= tuple(map(int, sys.argv[1].split(".")))
print("cmip7-cmor-common", v, "ok" if ok else "OLDER THAN " + sys.argv[1] + ": rerun install")
' "$COMMON_MIN" 2>/dev/null || echo "cmip7-cmor-common not installed: rerun install"
    python3 - "$METADATA" <<'EOF'
import hashlib, json, sys
p = sys.argv[1]
h = json.load(open(p))["Header"]
print("metadata", h["dreq content version"], h["no. of variables"], "variables, api", h["dreq api version"])
print("sha256", hashlib.sha256(open(p, "rb").read()).hexdigest())
EOF
    # Content pycmor picks up when paths are not set explicitly (set them).
    cache=${XDG_CACHE_HOME:-$HOME/.cache}/pycmor
    echo "pycmor cache $cache:"
    ls -ld --time-style=+%F "$cache"/*/* 2>/dev/null | awk '{print "  " $6, $7}' || true
}

case ${1:-} in
    plan|install|check) "$1" ;;
    *) sed -n '2,4p' "$0"; exit 2 ;;
esac
