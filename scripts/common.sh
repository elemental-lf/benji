#!/usr/bin/env bash

set -o pipefail

: "${BENJI_INSTANCE:=benji-k8s}"
: "${BENJI_LOG_LEVEL:=INFO}"

function benji::hook::execute {
    local HOOK_FUNCTION="$1"
    shift

    if [[ -n $(type $HOOK_FUNCTION 2>/dev/null) ]]; then
        echo "Executing hook $HOOK_FUNCTION."
        $HOOK_FUNCTION "$@"
    fi
}

function benji::command {
    local COMMAND="$1"
    shift

    benji::hook::execute benji::command::pre "$COMMAND" "$@" \
        || return $?

    benji --log-level "$BENJI_LOG_LEVEL" "$COMMAND" "$@"
    EC=$?

    if [[ $EC == 0 ]]; then
        benji::hook::execute benji::command::post::success "$COMMAND" "$@" \
            || return $?
        return 0
    else
        benji::hook::execute benji::command::post::error "$COMMAND" "$@" \
            || return $?
        return $EC
    fi
}

function benji::version::uid::format {
    jq -r '"V" + ("000000000" + (. | tostring))[-10:]'
}
