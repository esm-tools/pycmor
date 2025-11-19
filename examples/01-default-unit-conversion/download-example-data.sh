#!/bin/bash -e
#
# Downloads the example data for this example from DRKZ's Swift object storage
#

if [ -d model_runs ]; then
  echo "Example data for $(basename $(pwd)) already downloaded..."
  exit 0
fi

module load py-python-swiftclient
swift download pymorize_demo 01-default-unit-conversion-model-runs.tgz
tar -xzvf 01-default-unit-conversion-model-runs.tgz
rm 01-default-unit-conversion-model-runs.tgz
