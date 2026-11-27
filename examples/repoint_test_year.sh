#!/bin/bash
# Repoint all cmip7_*_test.yaml configs at a different output run + year.
# Usage: ./repoint_test_year.sh <RUN_NUM> <YEAR>
#   e.g. ./repoint_test_year.sh 017 1909
#
# Updates: cmip7_output_NNN dir, year_start/year_end, and embedded
# .YYYY. file patterns inside rule inputs (FESOM and LPJ-GUESS).
set -eu
RUN=${1:?usage: $0 <RUN_NUM> <YEAR>}
YEAR=${2:?usage: $0 <RUN_NUM> <YEAR>}
cd "$(dirname "$0")"
for f in cmip7_*_test.yaml; do
  sed -i \
    -e "s|cmip7_output_[0-9]\{3\}|cmip7_output_${RUN}|g" \
    -e "s|year_start: [0-9]\{4\}|year_start: ${YEAR}|" \
    -e "s|year_end: [0-9]\{4\}|year_end: ${YEAR}|" \
    -e "s|\.[0-9]\{4\}\.nc|.${YEAR}.nc|g" \
    "$f"
done
echo "Repointed $(ls cmip7_*_test.yaml | wc -l) yamls to run ${RUN}, year ${YEAR}"
