#!/bin/bash

set -euo pipefail


function trace() {
    {
        local tracing
        [[ "$-" = *"x"* ]] && tracing=true || tracing=false
        set +x
    } 2>/dev/null
    if [ "$tracing" != true ]; then
        # Bash's own trace mode is off, so explicitely write the message.
        echo "$@" >&2
    else
        # Restore trace
        set -x
    fi
}


function contains () {
  local e match="$1"
  shift
  for e; do [[ "$e" == "$match" ]] && return 0; done
  return 1
}


# Parse arguments.
operations=()
subcommand_opts=()
while true; do
    case "${1:-}" in
    setup)
        operations+=( setup )
        shift
        ;;
    lint|linting)
        operations+=( linting )
        shift
        ;;
    run-unit-tests)
        operations+=( run-unit-tests )
        shift
        ;;
    run)
        operations+=( run )
        shift
        ;;
    --)
        shift
        break
        ;;
    -h|--help)
        operations+=( usage )
        shift
        ;;
    *)
        break
        ;;
    esac
done
if [ "${#operations[@]}" -eq 0 ]; then
    operations=( usage )
fi
if [ "$#" -gt 0 ]; then
    subcommand_opts=( "$@" )
fi


function usage() {
    trace "$0 <command> [--] [options ...]"
    trace "Commands:"
    trace "    linting   Static analysis, code style, etc."
    trace "    precommit Run sensible checks before committing"
    trace "    run       Run the application"
    trace "    setup     Install dependencies"
    trace "    run-unit-tests     Run Unit tests"
    trace "Options are passed through to the sub-command."
}


function setup() {
    trace "Setting up"
    ./scripts/setup.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}


function linting() {
    trace "Linting"
    ./scripts/linting.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}


function run-unit-tests() {
    trace "Running unit tests"
    ./scripts/run-unit-tests.sh "${subcommand_opts[@]:+${subcommand_opts[@]}}"
}


script_directory="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
cd "${script_directory}/"


if contains usage "${operations[@]}"; then
    usage
    exit 1
fi
if contains setup "${operations[@]}"; then
    setup
fi
if contains linting "${operations[@]}"; then
    linting
fi
if contains run-unit-tests "${operations[@]}"; then
    run-unit-tests
fi
if contains run "${operations[@]}"; then
    run
fi


trace "Exited cleanly."
