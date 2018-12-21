#!/usr/bin/env bash

function benji::command {
    local COMMAND="$1"
    shift
    START_TIME=$(date +'%s')
    benji_command_start_time -command="$COMMAND" -auxiliary_data= -arguments="$*" set "$(date +'%s.%N')"
    try {
        benji --log-level "${BENJI_LOG_LEVEL:-INFO}" "$COMMAND" "$@"
    } catch {
        benji_command_status_failed -command="$COMMAND" -auxiliary_data= -arguments="$*" set 1
    } onsuccess {
        benji_command_status_succeeded -command="$COMMAND" -auxiliary_data= -arguments="$*" set 1
    }
    benji_command_completion_time -command="$COMMAND" -auxiliary_data= -arguments="$*" set "$(date +'%s.%N')"
    benji_command_runtime_seconds -command="$COMMAND" -auxiliary_data= -arguments="$*" set $[$(date +'%s') - $START_TIME]
}
