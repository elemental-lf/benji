#!/usr/bin/env bash

: "${PROM_PUSH_GATEWAY:=:9091}"

. prometheus-lib.sh
. prometheus-metrics.sh

function benji::prometheus::push {
    echo
    io::prometheus::ExportAsText | grep -v '^#'
    io::prometheus::PushAdd job="$BENJI_INSTANCE" gateway="$PROM_PUSH_GATEWAY"
}

function benji::prometheus::versions_status {
    local OLDER_INCOMPLETE_VERSIONS=$(benji -m --log-level "$BENJI_LOG_LEVEL" ls 'status == "incomplete" and date < "1 day ago"' | jq '.versions | length')
    local INVALID_VERSIONS=$(benji -m --log-level "$BENJI_LOG_LEVEL" ls 'status == "invalid"' | jq '.versions | length')

    benji_older_incomplete_versions set $OLDER_INCOMPLETE_VERSIONS
    benji_invalid_versions set $INVALID_VERSIONS
}