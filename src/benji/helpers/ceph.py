import logging
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Dict, Any, Optional

from blinker import signal

from benji.helpers.settings import benji_log_level
from benji.helpers.utils import subprocess_run

SIGNAL_SENDER = 'ceph'
RBD_SNAP_CREATE_TIMEOUT = 30
RBD_SNAP_NAME_PREFIX = 'b-'

logger = logging.getLogger()

signal_snapshot_create_pre = signal('snapshot_create_pre')
signal_snapshot_create_post_success = signal('snapshot_create_post_success')
signal_snapshot_create_post_error = signal('snapshot_create_post_error')
signal_backup_pre = signal('backup_pre')
signal_backup_post_success = signal('on_backup_post_success')
signal_backup_post_error = signal('on_backup_post_error')


def snapshot_create(*, volume: str, pool: str, image: str, snapshot: str, context: Any = None):
    signal_snapshot_create_pre.send(SIGNAL_SENDER,
                                    volume=volume,
                                    pool=pool,
                                    image=image,
                                    snapshot=snapshot,
                                    context=context)
    try:
        subprocess_run(['rbd', 'snap', 'create', f'{pool}/{image}@{snapshot}'], timeout=RBD_SNAP_CREATE_TIMEOUT)
    except Exception as exception:
        signal_snapshot_create_post_error.send(SIGNAL_SENDER,
                                               volume=volume,
                                               pool=pool,
                                               image=image,
                                               snapshot=snapshot,
                                               context=context,
                                               exception=exception)
    else:
        signal_snapshot_create_post_success.send(SIGNAL_SENDER,
                                                 volume=volume,
                                                 pool=pool,
                                                 image=image,
                                                 snapshot=snapshot,
                                                 context=context)


def backup_initial(*,
                   volume: str,
                   pool: str,
                   image: str,
                   version_labels: Dict[str, str],
                   version_uid: Optional[str],
                   context: Any = None) -> Dict[str, str]:
    logger.info(f'Performing initial backup of {volume}:{pool}/{image}')

    now = datetime.utcnow()
    snapshot = now.strftime(RBD_SNAP_NAME_PREFIX + '%Y-%m-%dT%H:%M:%SZ')

    snapshot_create(volume=volume, pool=pool, image=image, snapshot=snapshot, context=context)
    stdout = subprocess_run(['rbd', 'diff', '--whole-object', '--format=json', f'{pool}/{image}@{snapshot}'])

    with NamedTemporaryFile(mode='w+', encoding='utf-8') as rbd_hints:
        assert isinstance(stdout, str)
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
        assert isinstance(result, dict)

    return result


def backup_differential(*,
                        volume: str,
                        pool: str,
                        image: str,
                        last_snapshot: str,
                        last_version_uid: str,
                        version_labels: Dict[str, str],
                        version_uid: Optional[str],
                        context: Any = None) -> Dict[str, str]:
    logger.info(f'Performing differential backup of {volume}:{pool}/{image} from RBD snapshot" \
        "{last_snapshot} and Benji version {last_version_uid}.')

    now = datetime.utcnow()
    snapshot = now.strftime(RBD_SNAP_NAME_PREFIX + '%Y-%m-%dT%H:%M:%SZ')

    snapshot_create(volume=volume, pool=pool, image=image, snapshot=snapshot, context=context)
    stdout = subprocess_run(
        ['rbd', 'diff', '--whole-object', '--format=json', '--from-snap', last_snapshot, f'{pool}/{image}@{snapshot}'])
    subprocess_run(['rbd', 'snap', 'rm', f'{pool}/{image}@{last_snapshot}'])

    with NamedTemporaryFile(mode='w+', encoding='utf-8') as rbd_hints:
        assert isinstance(stdout, str)
        rbd_hints.write(stdout)
        rbd_hints.flush()
        benji_args = [
            'benji', '--machine-output', '--log-level', benji_log_level, 'backup', '--snapshot', snapshot,
            '--rbd-hints', rbd_hints.name, '--base-version', last_version_uid
        ]
        if version_uid is not None:
            benji_args.extend(['--uid', version_uid])
        for label_name, label_value in version_labels.items():
            benji_args.extend(['--label', f'{label_name}={label_value}'])
        benji_args.extend([f'{pool}:{pool}/{image}@{snapshot}', volume])
        result = subprocess_run(benji_args, decode_json=True)
        assert isinstance(result, dict)

    return result


def backup(*,
           volume: str,
           pool: str,
           image: str,
           version_labels: Dict[str, str] = {},
           version_uid: str = None,
           context: Any = None):
    signal_backup_pre.send(SIGNAL_SENDER,
                           volume=volume,
                           pool=pool,
                           image=image,
                           version_labels=version_labels,
                           context=context)
    version = None
    try:
        rbd_snap_ls = subprocess_run(['rbd', 'snap', 'ls', '--format=json', f'{pool}/{image}'], decode_json=True)
        assert isinstance(rbd_snap_ls, list)
        # Snapshot are sorted by their ID, so newer snapshots come last
        benjis_snapshots = [
            snapshot['name'] for snapshot in rbd_snap_ls if snapshot['name'].startswith(RBD_SNAP_NAME_PREFIX)
        ]
        if len(benjis_snapshots) == 0:
            logger.info('No previous RBD snapshot found, performing initial backup.')
            result = backup_initial(volume=volume,
                                    pool=pool,
                                    image=image,
                                    version_uid=version_uid,
                                    version_labels=version_labels,
                                    context=context)
        else:
            # Delete all snapshots except the newest
            for snapshot in benjis_snapshots[:-1]:
                logger.info(f'Deleting older RBD snapshot {pool}/{image}@{snapshot}.')
                subprocess_run(['rbd', 'snap', 'rm', f'{pool}/{image}@{snapshot}'])

            last_snapshot = benjis_snapshots[-1]
            logger.info(f'Newest RBD snapshot is {pool}/{image}@{last_snapshot}.')

            benji_ls = subprocess_run([
                'benji', '--machine-output', '--log-level', benji_log_level, 'ls',
                f'volume == "{volume}" and snapshot == "{last_snapshot}" and status == "valid"'
            ],
                                      decode_json=True)
            assert isinstance(benji_ls, dict)
            assert 'versions' in benji_ls
            assert isinstance(benji_ls['versions'], list)
            if len(benji_ls['versions']) > 0:
                assert 'uid' in benji_ls['versions'][0]
                last_version_uid = benji_ls['versions'][0]['uid']
                assert isinstance(last_version_uid, str)
                result = backup_differential(volume=volume,
                                             pool=pool,
                                             image=image,
                                             last_snapshot=last_snapshot,
                                             last_version_uid=last_version_uid,
                                             version_uid=version_uid,
                                             version_labels=version_labels,
                                             context=context)
            else:
                logger.info(f'Existing RBD snapshot {pool}/{image}@{last_snapshot} not found in Benji, deleting it and reverting to initial backup.')
                subprocess_run(['rbd', 'snap', 'rm', f'{pool}/{image}@{last_snapshot}'])
                result = backup_initial(volume=volume,
                                        pool=pool,
                                        image=image,
                                        version_uid=version_uid,
                                        version_labels=version_labels,
                                        context=context)
        assert 'versions' in result and isinstance(result['versions'], list)
        version = result['versions'][0]
    except Exception as exception:
        signal_backup_post_error.send(SIGNAL_SENDER,
                                      volume=volume,
                                      pool=pool,
                                      image=image,
                                      version_labels=version_labels,
                                      context=context,
                                      version=version,
                                      exception=exception)
    else:
        signal_backup_post_success.send(SIGNAL_SENDER,
                                        volume=volume,
                                        pool=pool,
                                        image=image,
                                        version_labels=version_labels,
                                        context=context,
                                        version=version)
