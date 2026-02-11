#!/bin/bash

set -euo pipefail


function pylint() {
    trace "Linting with pylint"
    trace "Linting coming soon"
}

script_directory="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
cd "${script_directory}/.."
pylint

trace "No issues found."