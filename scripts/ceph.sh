#!/usr/bin/env bash

function benji::backup::ceph::initial {
    local NAME="$1"
    local POOL="$2"
    local IMAGE="$3"
    shift 3
    local LABELS=("$@")

    SNAPNAME="b-$(date '+%Y-%m-%dT%H:%M:%S')"  # b-2017-04-19T11:33:23
    TEMPFILE=$(mktemp --tmpdir ceph-rbd-diff-tmp.XXXXXXXXXX)

    echo "Performing initial backup of $NAME:$POOL/$IMAGE."

    rbd snap create "$POOL"/"$IMAGE"@"$SNAPNAME"
    rbd diff --whole-object "$POOL"/"$IMAGE"@"$SNAPNAME" --format=json >"$TEMPFILE"
    benji --log-level "${BENJI_LOG_LEVEL:-INFO}" backup -s "$SNAPNAME" -r "$TEMPFILE" \
        $(printf -- "-l \"%s\" " "${LABELS[@]}") rbd://"$POOL"/"$IMAGE"@"$SNAPNAME" "$NAME"

    rm -f "$TEMPFILE"
}

function benji::backup::ceph::differential {
    local NAME="$1"
    local POOL="$2"
    local IMAGE="$3"
    local LAST_RBD_SNAP="$4"
    local BENJI_SNAP_VERSION_UID="$5"
    shift 5
    local LABELS=("$@")

    local SNAPNAME="b-$(date '+%Y-%m-%dT%H:%M:%S')"  # b-2017-04-20T11:33:23
    local TEMPFILE=$(mktemp --tmpdir ceph-rbd-diff-tmp.XXXXXXXXXX)

    echo "Performing differential backup of $NAME:$POOL/$IMAGE from RBD snapshot $LAST_RBD_SNAP and Benji version $BENJI_SNAP_VERSION_UID."

    rbd snap create "$POOL"/"$IMAGE"@"$SNAPNAME"
    rbd diff --whole-object "$POOL"/"$IMAGE"@"$SNAPNAME" --from-snap "$LAST_RBD_SNAP" --format=json >"$TEMPFILE"
    # delete old snapshot
    rbd snap rm "$POOL"/"$IMAGE"@"$LAST_RBD_SNAP"
    # and backup
    benji --log-level "${BENJI_LOG_LEVEL:-INFO}" backup -s "$SNAPNAME" -r "$TEMPFILE" -f "$BENJI_SNAP_VERSION_UID" \
        $(printf -- "-l \"%s\" " "${LABELS[@]}") rbd://"$POOL"/"$IMAGE"@"$SNAPNAME" "$NAME"
    
    rm -f "$TEMPFILE"
}

function benji::backup::ceph {
    local NAME="$1"
    local POOL="$2"
    local IMAGE="$3"
    shift 3
    local LABELS=("$@")

    # find the latest snapshot name from rbd
    LAST_RBD_SNAP=$(rbd snap ls "$POOL"/"$IMAGE" --format=json | jq -r '[.[].name] | map(select(test("^b-"))) | sort | .[-1] // ""')
    if [[ -z $LAST_RBD_SNAP ]]; then
        echo 'No previous RBD snapshot found, reverting to initial backup.'
        START_TIME=$(date +'%s')
        benji_backup_start_time -command=backup -auxiliary_data=initial -version_name="$NAME" set $(date +'%s.%N')
        try {
            benji::backup::ceph::initial "$NAME" "$POOL" "$IMAGE" "${LABELS[@]}"
        } catch {
            benji_backup_status_failed -command=backup -auxiliary_data=initial -version_name="$NAME" set 1
        } onsuccess {
            benji_backup_status_succeeded -command=backup -auxiliary_data=initial -version_name="$NAME" set 1
        }
        benji_backup_completion_time -command=backup -auxiliary_data=initial -version_name="$NAME" set $(date +'%s.%N')
        benji_backup_runtime_seconds -command=backup -auxiliary_data=initial -version_name="$NAME" set $[$(date +'%s') - $START_TIME]
    else
        # check if a valid version of this RBD snapshot exists
        BENJI_SNAP_VERSION_UID=$(benji -m ls 'name == "$NAME" and snapshot_name == "$LAST_RBD_SNAP"' | jq -r '.versions[0] | select(.valid == true) | .uid // ""')
        if [[ -z $BENJI_SNAP_VERSION_UID ]]; then
            echo 'Existing RBD snapshot not found in Benji, deleting it and reverting to initial backup.'
            START_TIME=$(date +'%s')
            benji_backup_start_time -command=backup -auxiliary_data=initial -version_name="$NAME" set $(date +'%s.%N')
            try {
                rbd snap rm "$POOL"/"$IMAGE"@"$LAST_RBD_SNAP"
                benji::backup::ceph::initial "$NAME" "$POOL" "$IMAGE" "${LABELS[@]}"
            } catch {
                benji_backup_status_failed -command=backup -auxiliary_data=initial -version_name="$NAME" set 1
            } onsuccess {
                benji_backup_status_succeeded -command=backup -auxiliary_data=initial -version_name="$NAME" set 1
            }
            benji_backup_completion_time -command=backup -auxiliary_data=initial -version_name="$NAME" set $(date +'%s.%N')
            benji_backup_runtime_seconds -command=backup -auxiliary_data=initial -version_name="$NAME" set $[$(date +'%s') - $START_TIME]
        else
            START_TIME=$(date +'%s')
            benji_backup_start_time -version_name="$NAME" -command=backup -auxiliary_data=differential set $(date +'%s.%N')
            try {
                benji::backup::ceph::differential "$NAME" "$POOL" "$IMAGE" "$LAST_RBD_SNAP" "$BENJI_SNAP_VERSION_UID" "${LABELS[@]}"
            } catch {
                benji_backup_status_failed -command=backup -auxiliary_data=differential -version_name="$NAME" set 1
            } onsuccess {
                benji_backup_status_succeeded -command=backup -auxiliary_data=differential -version_name="$NAME" set 1
            }
            benji_backup_completion_time -command=backup -auxiliary_data=differential -version_name="$NAME" set $(date +'%s.%N')
            benji_backup_runtime_seconds -command=backup -auxiliary_data=differential -version_name="$NAME" set $[$(date +'%s') - $START_TIME]
        fi
    fi
}
