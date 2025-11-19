#!/bin/bash -e
#
# Generates CMIP7 metadata files needed for this example
#

echo "Generating CMIP7 metadata files for v1.2.2.2..."
export_dreq_lists_json -a v1.2.2.2 ./experiments.json -m ./metadata.json

echo "Metadata files generated:"
echo "  - experiments.json (experiment mappings)"
echo "  - metadata.json (variable metadata with Compound Names)"
