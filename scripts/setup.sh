#!/bin/bash

set -euo pipefail

script_directory="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
cd "${script_directory}/.."

poetry install --no-root

echo "Fetched all dependencies" >&2
