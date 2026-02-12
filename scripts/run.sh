#!/bin/bash

set -euo pipefail

script_directory="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
cd "${script_directory}/.."
export PYTHONPATH="$(pwd):$(pwd)/app/src:${PYTHONPATH:-}"
echo "PYTHONPATH=$PYTHONPATH"
spark-submit app/src/main.py --file_path ${1:-"app/tests/unit/data/car_data.txt"}

echo "Completed Spark execution" >&2
