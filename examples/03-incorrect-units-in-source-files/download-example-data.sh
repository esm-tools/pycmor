#!/bin/bash -e
#
# Downloads the example data for this example from DRKZ's Swift object storage
#

if [ -d model_runs ]; then
  echo "Example data for $(basename $(pwd)) already downloaded..."
  exit 0
fi

module load py-python-swiftclient
swift download pycmorize_demo_data 03-incorrect-units-in-source-files-model-runs.tgz 
tar -xzvf 03-incorrect-units-in-source-files-model-runs.tgz 
rm 03-incorrect-units-in-source-files-model-runs.tgz 
