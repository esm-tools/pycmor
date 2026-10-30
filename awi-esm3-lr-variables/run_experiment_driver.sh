#!/bin/bash
set -euo pipefail
exec "$1" "$2" drive "$3"
