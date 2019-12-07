#!/usr/bin/env bash

set -o pipefail

: "${BENJI_INSTANCE:=benji-k8s}"
: "${BENJI_LOG_LEVEL:=INFO}"

function _extract_version_uid {
    jq -r '.versions[0].uid'
}

function benji::backup::ceph::snapshot::create {
    local VOLUME="$1"
    local CEPH_POOL="$2"
    local CEPH_RBD_IMAGE="$3"
    local CEPH_RBD_SNAPSHOT="$4"

    # We need this to run through, so ignore termination and stop attempts for a time.
    # A trap for RETURN is not inherited by default, which is exactly what we need.
    trap -- 'trap - SIGINT SIGTERM SIGTSTP RETURN' RETURN
    trap -- '' SIGINT SIGTERM SIGTSTP

    # Start rbd with a timeout to increase the likelihood that we don't hang with signals off...
    timeout --kill-after=10 30 rbd snap create "$CEPH_POOL"/"$CEPH_RBD_IMAGE"@"$CEPH_RBD_SNAPSHOT"
    local EC=$?
    if [[ $EC == 124 ]]; then
        echo "Warning: Snapshot creation timed out for $CEPH_POOL/$CEPH_RBD_IMAGE."
    fi

    return $EC
}

# Returns:
# - version uid in global variable VERSION_UID (empty string on error)
# - stderr output of benji backup in BENJI_BACKUP_STDERR
function benji::backup::ceph::initial {
    local VOLUME="$1"
    local CEPH_POOL="$2"
    local CEPH_RBD_IMAGE="$3"
    shift 3
    local VERSION_LABELS=("$@")

    local CEPH_RBD_SNAPSHOT="b-$(date '+%Y-%m-%dT%H:%M:%S')"  # b-2017-04-19T11:33:23
    local CEPH_RBD_DIFF_FILE=$(mktemp --tmpdir ceph-rbd-diff-tmp.XXXXXXXXXX)
    local BENJI_BACKUP_STDERR_FILE=$(mktemp --tmpdir benji-backup-tmp.XXXXXXXXXX)

    trap "{ rm -f \"$CEPH_RBD_DIFF_FILE\" \"$BENJI_BACKUP_STDERR_FILE\"; }" RETURN EXIT

    echo "Performing initial backup of $VOLUME:$CEPH_POOL/$CEPH_RBD_IMAGE."

    benji::backup::ceph::snapshot::create "$VOLUME" "$CEPH_POOL" "$CEPH_RBD_IMAGE" "$CEPH_RBD_SNAPSHOT" \
        || return $?
    rbd diff --whole-object "$CEPH_POOL"/"$CEPH_RBD_IMAGE"@"$CEPH_RBD_SNAPSHOT" --format=json >"$CEPH_RBD_DIFF_FILE" \
        || return $?

    VERSION_UID="$(benji -m --log-level "$BENJI_LOG_LEVEL" backup -s "$CEPH_RBD_SNAPSHOT" -r "$CEPH_RBD_DIFF_FILE" \
        $([[ ${#VERSION_LABELS[@]} -gt 0 ]] && printf -- "-l %s " "${VERSION_LABELS[@]}") rbd:"$CEPH_POOL"/"$CEPH_RBD_IMAGE"@"$CEPH_RBD_SNAPSHOT" \
        "$VOLUME" 2> >(tee "$BENJI_BACKUP_STDERR_FILE" >&2) | _extract_version_uid)"
    local EC=$?
    BENJI_BACKUP_STDERR="$(<${BENJI_BACKUP_STDERR_FILE})"
    [[ $EC == 0 ]] || return $EC

    return 0
}

# Returns:
# - version uid in global variable VERSION_UID (empty string on error)
# - stderr output of benji backup in BENJI_BACKUP_STDERR
function benji::backup::ceph::differential {
    local VOLUME="$1"
    local CEPH_POOL="$2"
    local CEPH_RBD_IMAGE="$3"
    local CEPH_RBD_SNAPSHOT_LAST="$4"
    local BENJI_VERSION_UID_LAST="$5"
    shift 5
    local VERSION_LABELS=("$@")

    local CEPH_RBD_SNAPSHOT="b-$(date '+%Y-%m-%dT%H:%M:%S')"  # b-2017-04-20T11:33:23
    local CEPH_RBD_DIFF_FILE=$(mktemp --tmpdir ceph-rbd-diff-tmp.XXXXXXXXXX)
    local BENJI_BACKUP_STDERR_FILE=$(mktemp --tmpdir benji-backup-tmp.XXXXXXXXXX)

    trap "{ rm -f \"$CEPH_RBD_DIFF_FILE\" \"$BENJI_BACKUP_STDERR_FILE\"; }" RETURN EXIT

    echo "Performing differential backup of $VOLUME:$CEPH_POOL/$CEPH_RBD_IMAGE from RBD snapshot" \
        "$CEPH_RBD_SNAPSHOT_LAST and Benji version $BENJI_VERSION_UID_LAST."

    benji::backup::ceph::snapshot::create "$VOLUME" "$CEPH_POOL" "$CEPH_RBD_IMAGE" "$CEPH_RBD_SNAPSHOT" \
        || return $?
    rbd diff --whole-object "$CEPH_POOL"/"$CEPH_RBD_IMAGE"@"$CEPH_RBD_SNAPSHOT" --from-snap "$CEPH_RBD_SNAPSHOT_LAST" \
        --format=json >"$CEPH_RBD_DIFF_FILE" \
        || return $?
    rbd snap rm "$CEPH_POOL"/"$CEPH_RBD_IMAGE"@"$CEPH_RBD_SNAPSHOT_LAST" \
        || return $?

    VERSION_UID="$(benji -m --log-level "$BENJI_LOG_LEVEL" backup -s "$CEPH_RBD_SNAPSHOT" -r "$CEPH_RBD_DIFF_FILE" -f "$BENJI_VERSION_UID_LAST" \
        $([[ ${#VERSION_LABELS[@]} -gt 0 ]] && printf -- "-l %s " "${VERSION_LABELS[@]}") rbd:"$CEPH_POOL"/"$CEPH_RBD_IMAGE"@"$CEPH_RBD_SNAPSHOT" \
        "$VOLUME" 2> >(tee "$BENJI_BACKUP_STDERR_FILE" >&2) | _extract_version_uid)"
    local EC=$?
    BENJI_BACKUP_STDERR="$(<${BENJI_BACKUP_STDERR_FILE})"
    [[ $EC == 0 ]] || return $EC

    return 0
}

function benji::backup::ceph {
    local VOLUME="$1"
    local CEPH_POOL="$2"
    local CEPH_RBD_IMAGE="$3"
    shift 3
    local VERSION_LABELS=("$@")

    # find the latest snapshot name from rbd
    local CEPH_RBD_SNAPSHOT_LAST=$(rbd snap ls "$CEPH_POOL"/"$CEPH_RBD_IMAGE" --format=json | jq -r '[.[].name] | map(select(test("^b-"))) | sort | .[-1] // ""')
    local EC=$?; [[ $EC == 0 ]] || return $EC

    if [[ ! $CEPH_RBD_SNAPSHOT_LAST ]]; then
        echo 'No previous RBD snapshot found, reverting to initial backup.'
        benji::backup::ceph::initial "$VOLUME" "$CEPH_POOL" "$CEPH_RBD_IMAGE" "${VERSION_LABELS[@]}"
        EC=$?
    else
        echo "Snapshot found for $CEPH_POOL/$CEPH_RBD_IMAGE is $CEPH_RBD_SNAPSHOT_LAST."
        # check if a valid version of this RBD snapshot exists
        BENJI_SNAP_VERSION_UID=$(benji -m ls 'volume == "'"$VOLUME"'" and snapshot == "'"$CEPH_RBD_SNAPSHOT_LAST"'"' | jq -r '.versions[0] | select(.status == "valid") | .uid // ""')
        EC=$?

        if [[ $EC == 0 ]]; then
            if [[ ! $BENJI_SNAP_VERSION_UID ]]; then
                echo 'Existing RBD snapshot not found in Benji, deleting it and reverting to initial backup.'
                rbd snap rm "$CEPH_POOL"/"$CEPH_RBD_IMAGE"@"$CEPH_RBD_SNAPSHOT_LAST"
                EC=$?
                if [[ $EC == 0 ]]; then
                    benji::backup::ceph::initial "$VOLUME" "$CEPH_POOL" "$CEPH_RBD_IMAGE" "${VERSION_LABELS[@]}"
                    EC=$?
                fi
            else
                    benji::backup::ceph::differential "$VOLUME" "$CEPH_POOL" "$CEPH_RBD_IMAGE" "$CEPH_RBD_SNAPSHOT_LAST" "$BENJI_SNAP_VERSION_UID" "${VERSION_LABELS[@]}"
                    EC=$?
            fi
        fi
    fi

    return $EC
}
