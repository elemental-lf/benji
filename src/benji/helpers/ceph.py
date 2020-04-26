import logging
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Dict, Any, Optional, Sequence, List

from benji.helpers.settings import benji_log_level
from benji.helpers.utils import subprocess_run

SIGNAL_SENDER = 'ceph'
RBD_SNAP_CREATE_TIMEOUT = 30
RBD_SNAP_RM_TIMEOUT = 30
RBD_SNAP_NAME_PREFIX = 'b-'
CEPH_DEFAULT_USER = 'admin'

logger = logging.getLogger()


def _build_ceph_credential_arguments(*, monitors: Sequence[str], user: str, keyring: str, key: str) -> List[str]:
    arguments = []
    if monitors:
        arguments += ['-m']
        arguments += ','.join(monitors)
    arguments += ['--id', user or CEPH_DEFAULT_USER]
    if key:
        arguments += [f'--key={key}']
    elif keyring:
        arguments += ['-k', keyring]
    return arguments


def snapshot_create(*,
                    pool: str,
                    image: str,
                    monitors: Sequence[str] = None,
                    user: str = None,
                    keyring: str = None,
                    key: str = None,
                    snapshot: str):
    ceph_credential_args = _build_ceph_credential_arguments(monitors=monitors, user=user, keyring=keyring, key=key)
    rbd_snap_create_args = ['rbd', 'snap', 'create', f'{pool}/{image}@{snapshot}']
    rbd_snap_create_args.extend(ceph_credential_args)
    subprocess_run(rbd_snap_create_args, timeout=RBD_SNAP_CREATE_TIMEOUT)


def snapshot_rm(*,
                pool: str,
                image: str,
                monitors: Sequence[str] = None,
                user: str = None,
                keyring: str = None,
                key: str = None,
                snapshot: str):
    ceph_credential_args = _build_ceph_credential_arguments(monitors=monitors, user=user, keyring=keyring, key=key)
    rbd_snap_rm_args = ['rbd', 'snap', 'rm', f'{pool}/{image}@{snapshot}']
    rbd_snap_rm_args.extend(ceph_credential_args)
    subprocess_run(rbd_snap_rm_args, timeout=RBD_SNAP_RM_TIMEOUT)


def backup_initial(
        *,
        volume: str,
        pool: str,
        image: str,
        monitors: Sequence[str] = None,
        user: str = None,
        keyring: str = None,
        key: str = None,
        version_labels: Dict[str, str],
        version_uid: Optional[str],
) -> Dict[str, str]:
    logger.info(f'Performing initial backup of {volume}:{pool}/{image}')

    now = datetime.utcnow()
    snapshot = now.strftime(RBD_SNAP_NAME_PREFIX + '%Y-%m-%dT%H:%M:%SZ')

    snapshot_create(pool=pool, image=image, monitors=monitors, user=user, keyring=keyring, key=key, snapshot=snapshot)
    ceph_credential_args = _build_ceph_credential_arguments(monitors=monitors, user=user, keyring=keyring, key=key)
    rbd_diff_args = ['rbd', 'diff', '--whole-object', '--format=json', f'{pool}/{image}@{snapshot}']
    rbd_diff_args.extend(ceph_credential_args)
    stdout = subprocess_run(rbd_diff_args)

    with NamedTemporaryFile(mode='w+', encoding='utf-8') as rbd_hints:
        rbd_hints.write(stdout)
        rbd_hints.flush()
        benji_args = [
            'benji', '--machine-output', '--log-level', benji_log_level, 'backup', '--snapshot', snapshot,
            '--rbd-hints', rbd_hints.name
        ]
        if version_uid is not None:
            benji_args.extend(['--uid', version_uid])
        for label_name, label_value in version_labels.items():
            benji_args.extend(['--label', f'{label_name}={label_value}'])
        benji_args.extend([f'{pool}:{pool}/{image}@{snapshot}', volume])
        result = subprocess_run(benji_args, decode_json=True)

    return result


def backup_differential(
        *,
        volume: str,
        pool: str,
        image: str,
        monitors: Sequence[str] = None,
        user: str = None,
        keyring: str = None,
        key: str = None,
        last_snapshot: str,
        base_version_uid: str,
        version_labels: Dict[str, str],
        version_uid: Optional[str],
) -> Dict[str, str]:
    logger.info(f'Performing differential backup of {volume}:{pool}/{image} from RBD snapshot" \
        "{last_snapshot} and Benji version {base_version_uid}.')

    now = datetime.utcnow()
    snapshot = now.strftime(RBD_SNAP_NAME_PREFIX + '%Y-%m-%dT%H:%M:%SZ')

    snapshot_create(pool=pool, image=image, monitors=monitors, user=user, keyring=keyring, key=key, snapshot=snapshot)
    ceph_credential_args = _build_ceph_credential_arguments(monitors=monitors, user=user, keyring=keyring, key=key)
    rbd_diff_args = [
        'rbd', 'diff', '--whole-object', '--format=json', '--from-snap', last_snapshot, f'{pool}/{image}@{snapshot}'
    ]
    rbd_diff_args.extend(ceph_credential_args)
    stdout = subprocess_run(rbd_diff_args)

    snapshot_rm(pool=pool, image=image, monitors=monitors, user=user, keyring=keyring, key=key, snapshot=last_snapshot)

    with NamedTemporaryFile(mode='w+', encoding='utf-8') as rbd_hints:
        rbd_hints.write(stdout)
        rbd_hints.flush()
        benji_args = [
            'benji', '--machine-output', '--log-level', benji_log_level, 'backup', '--snapshot', snapshot,
            '--rbd-hints', rbd_hints.name, '--base-version', base_version_uid
        ]
        if version_uid is not None:
            benji_args.extend(['--uid', version_uid])
        for label_name, label_value in version_labels.items():
            benji_args.extend(['--label', f'{label_name}={label_value}'])
        benji_args.extend([f'{pool}:{pool}/{image}@{snapshot}', volume])
        result = subprocess_run(benji_args, decode_json=True)

    return result


def backup(
    *,
    volume: str,
    pool: str,
    image: str,
    monitors: Sequence[str] = None,
    user: str = None,
    keyring: str = None,
    key: str = None,
    version_labels: Dict[str, str] = {},
    version_uid: str = None,
):
    ceph_credential_args = _build_ceph_credential_arguments(monitors=monitors, user=user, keyring=keyring, key=key)
    rbd_snap_ls_args = ['rbd', 'snap', 'ls', '--format=json', f'{pool}/{image}']
    rbd_snap_ls_args.extend(ceph_credential_args)
    rbd_snap_ls = subprocess_run(rbd_snap_ls_args, decode_json=True)
    # Snapshot are sorted by their ID, so newer snapshots come last
    benjis_snapshots = [
        snapshot['name'] for snapshot in rbd_snap_ls if snapshot['name'].startswith(RBD_SNAP_NAME_PREFIX)
    ]
    if len(benjis_snapshots) == 0:
        logger.info('No previous RBD snapshot found, performing initial backup.')
        result = backup_initial(
            volume=volume,
            pool=pool,
            image=image,
            monitors=monitors,
            user=user,
            keyring=keyring,
            key=key,
            version_uid=version_uid,
            version_labels=version_labels,
        )
    else:
        # Delete all snapshots except the newest
        for snapshot in benjis_snapshots[:-1]:
            logger.info(f'Deleting older RBD snapshot {pool}/{image}@{snapshot}.')
            snapshot_rm(pool=pool,
                        image=image,
                        monitors=monitors,
                        user=user,
                        keyring=keyring,
                        key=key,
                        snapshot=snapshot)

        last_snapshot = benjis_snapshots[-1]
        logger.info(f'Newest RBD snapshot is {pool}/{image}@{last_snapshot}.')

        benji_ls = subprocess_run([
            'benji', '--machine-output', '--log-level', benji_log_level, 'ls',
            f'volume == "{volume}" and snapshot == "{last_snapshot}" and status == "valid"'
        ],
                                  decode_json=True)
        if len(benji_ls['versions']) > 0:
            base_version_uid = benji_ls['versions'][0]['uid']
            result = backup_differential(
                volume=volume,
                pool=pool,
                image=image,
                monitors=monitors,
                user=user,
                keyring=keyring,
                key=key,
                last_snapshot=last_snapshot,
                base_version_uid=base_version_uid,
                version_uid=version_uid,
                version_labels=version_labels,
            )
        else:
            logger.info(f'Existing RBD snapshot {pool}/{image}@{last_snapshot} not found in Benji, deleting it and reverting to initial backup.')
            snapshot_rm(pool=pool,
                        image=image,
                        monitors=monitors,
                        user=user,
                        keyring=keyring,
                        key=key,
                        snapshot=last_snapshot)
            result = backup_initial(
                volume=volume,
                pool=pool,
                image=image,
                monitors=monitors,
                user=user,
                keyring=keyring,
                key=key,
                version_uid=version_uid,
                version_labels=version_labels,
            )
    return result


def restore(
    version_uid: str,
    pool: str,
    image: str,
    monitors: Sequence[str] = None,
    user: str = None,
    keyring: str = None,
    key: str = None,
):
    ceph_credential_args = _build_ceph_credential_arguments(monitors=monitors, user=user, keyring=keyring, key=key)
    result = subprocess_run([
        'benji', '--machine-output', '--log-level', benji_log_level, 'restore', '--sparse', '--force', version_uid,
        f'{pool}:{pool}/{image}'
    ])
