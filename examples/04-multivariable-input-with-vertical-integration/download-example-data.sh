#!/bin/bash -e
#
# Downloads the example data for this example from DRKZ's Swift object storage
#

if [ -d model_runs ]; then
  echo "Example data for $(basename $(pwd)) already downloaded..."
  exit 0
fi

module load py-python-swiftclient
swift download pycmor_demo_data 04-multivariable-input-with-vertical-integration-model-runs.tgz
tar -xzvf 04-multivariable-input-with-vertical-integration-model-runs.tgz
rm 04-multivariable-input-with-vertical-integration-model-runs.tgz
