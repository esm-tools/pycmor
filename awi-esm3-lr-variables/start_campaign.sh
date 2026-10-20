#!/bin/bash
# Usage: bash start_campaign.sh /absolute/path/to/campaign.yaml
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
python="${PYCMOR_LR_PYTHON:-/work/ab0995/a270270/cmorization_LR/venv/bin/python}"
exec "$python" "$here/campaign.py" start "${1:?Supply a campaign YAML}"
