#!/usr/bin/env bash

function benji::push_metrics {
    echo
    io::prometheus::ExportAsText | grep -v '^#'
    io::prometheus::PushAdd job="$BENJI_INSTANCE" gateway="$PROM_PUSH_GATEWAY"
}

function benji::command {
    local COMMAND="$1"
    shift
    START_TIME=$(date +'%s')
    benji_command_start_time -command="$COMMAND" -auxiliary_data= -arguments="$*" set "$(date +'%s.%N')"
    try {
        benji --log-level "$BENJI_LOG_LEVEL" "$COMMAND" "$@"
    } catch {
        benji_command_status_failed -command="$COMMAND" -auxiliary_data= -arguments="$*" set 1
    } onsuccess {
        benji_command_status_succeeded -command="$COMMAND" -auxiliary_data= -arguments="$*" set 1
    }
    benji_command_completion_time -command="$COMMAND" -auxiliary_data= -arguments="$*" set "$(date +'%s.%N')"
    benji_command_runtime_seconds -command="$COMMAND" -auxiliary_data= -arguments="$*" set $[$(date +'%s') - $START_TIME]
}

function benji::versions_status {
    local OLDER_INCOMPLETE_VERSIONS=$(benji -m --log-level "$BENJI_LOG_LEVEL" ls 'status == "incomplete" and date < "1 day ago"' | jq '.versions | length')
    local INVALID_VERSIONS=$(benji -m --log-level "$BENJI_LOG_LEVEL" ls 'status == "invalid"' | jq '.versions | length')

    benji_older_incomplete_versions set $OLDER_INCOMPLETE_VERSIONS
    benji_invalid_versions set $INVALID_VERSIONS
}
