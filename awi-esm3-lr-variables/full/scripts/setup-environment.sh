#!/usr/bin/env bash
# Install this checkout; never fetch a different pycmor revision or submit jobs.
set -euo pipefail

usage() {
  echo "Usage: bash setup-environment.sh --venv /outside/repo/venv [--python /path/to/python3] [--lock /path/to/requirements.lock]"
}

cmor_venv=""
cmor_python="python3"
cmor_lock=""
while (($#)); do
  case "$1" in
    --venv|--python|--lock)
      if (($# < 2)) || [[ -z "$2" || "$2" == --* ]]; then
        usage >&2
        exit 2
      fi
      case "$1" in
        --venv) cmor_venv="$2" ;;
        --python) cmor_python="$2" ;;
        --lock) cmor_lock="$2" ;;
      esac
      shift 2
      ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done
if [[ -z "$cmor_venv" ]]; then usage >&2; exit 2; fi
if [[ -n "$cmor_lock" && ! -f "$cmor_lock" ]]; then
  echo "Requirements lock does not exist: $cmor_lock" >&2
  exit 2
fi

cmor_full="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cmor_repo="$(cd -- "$cmor_full/../.." && pwd)"
cmor_venv="$("$cmor_python" -c 'import pathlib, sys; print(pathlib.Path(sys.argv[1]).expanduser().resolve())' "$cmor_venv")"
case "$cmor_venv/" in
  "$cmor_repo/"*) echo "Place the environment outside the source checkout." >&2; exit 2 ;;
esac
if [[ -e "$cmor_venv" || -L "$cmor_venv" ]]; then
  echo "Environment destination already exists; refusing to overwrite: $cmor_venv" >&2
  exit 1
fi
"$cmor_python" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else "Python 3.10 or later is required by the pinned extras")'
"$cmor_python" -m venv --copies "$cmor_venv"
if [[ -n "$cmor_lock" ]]; then
  "$cmor_venv/bin/python" -m pip install -r "$cmor_lock" -r "$cmor_full/requirements-extra.txt"
  "$cmor_venv/bin/python" -m pip install --no-deps -e "$cmor_repo"
else
  "$cmor_venv/bin/python" -m pip install -e "$cmor_repo" -r "$cmor_full/requirements-extra.txt"
fi
"$cmor_venv/bin/python" -m pip check
echo "Installed this checkout in: $cmor_venv"
echo "Set the external config's python entry to: $cmor_venv/bin/python"
echo "Metadata, mesh, forcing and ESGVOC caches still require explicit external configuration."
