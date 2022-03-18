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


def _rbd_image_path(*, pool: str, namespace: str = '', image: str, snapshot: str = None) -> str:
    # We could always include the namespace even when it is an empty string. But this might confuse some
    # users that are not aware of RADOS namespaces.
    if namespace != '':
        image = f'{pool}/{namespace}/{image}'
    else:
        # '' (empty string) is the default namespace, no need to specify it.
        image = f'{pool}/{image}'

    if snapshot is not None:
        image = f'{image}@{snapshot}'

    return image


def snapshot_create(*, volume: str, pool: str, namespace: str = '', image: str, snapshot: str, context: Any = None):
    signal_snapshot_create_pre.send(SIGNAL_SENDER,
                                    volume=volume,
                                    pool=pool,
                                    namespace=namespace,
                                    image=image,
                                    snapshot=snapshot,
                                    context=context)
    snapshot_path = _rbd_image_path(pool=pool, namespace=namespace, image=image, snapshot=snapshot)
    try:
        subprocess_run(['rbd', 'snap', 'create', snapshot_path], timeout=RBD_SNAP_CREATE_TIMEOUT)
    except Exception as exception:
        signal_snapshot_create_post_error.send(SIGNAL_SENDER,
                                               volume=volume,
                                               pool=pool,
                                               namespace=namespace,
                                               image=image,
                                               snapshot=snapshot,
                                               context=context,
                                               exception=exception)
    else:
        signal_snapshot_create_post_success.send(SIGNAL_SENDER,
                                                 volume=volume,
                                                 pool=pool,
                                                 namespace=namespace,
                                                 image=image,
                                                 snapshot=snapshot,
                                                 context=context)


def backup_initial(*,
                   volume: str,
                   pool: str,
                   namespace: str = '',
                   image: str,
                   version_labels: Dict[str, str],
                   version_uid: Optional[str],
                   source_compare: bool = False,
                   context: Any = None) -> Dict[str, str]:

    now = datetime.utcnow()
    snapshot = now.strftime(RBD_SNAP_NAME_PREFIX + '%Y-%m-%dT%H:%M:%SZ')
    image_path = _rbd_image_path(pool=pool, namespace=namespace, image=image)
    snapshot_path = _rbd_image_path(pool=pool, namespace=namespace, image=image, snapshot=snapshot)
    logger.info(f'Performing initial backup of {volume}:{image_path}')

    snapshot_create(volume=volume, pool=pool, namespace=namespace, image=image, snapshot=snapshot, context=context)
    stdout = subprocess_run(['rbd', 'diff', '--whole-object', '--format=json', snapshot_path])

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
        benji_args.extend([f'{pool}:{snapshot_path}', volume])
        result = subprocess_run(benji_args, decode_json=True)
        assert isinstance(result, dict)

    if source_compare:
        # We won't evaluate the returned result but any failure will raise an exception.
        deep_scrub(pool=pool, namespace=namespace, image=image, snapshot=snapshot, version_uid=version_uid)

    return result


def backup_differential(*,
                        volume: str,
                        pool: str,
                        namespace: str = '',
                        image: str,
                        last_snapshot: str,
                        last_version_uid: str,
                        version_labels: Dict[str, str],
                        version_uid: Optional[str],
                        source_compare: bool = False,
                        context: Any = None) -> Dict[str, str]:

    now = datetime.utcnow()
    snapshot = now.strftime(RBD_SNAP_NAME_PREFIX + '%Y-%m-%dT%H:%M:%SZ')
    image_path = _rbd_image_path(pool=pool, namespace=namespace, image=image)
    snapshot_path = _rbd_image_path(pool=pool, namespace=namespace, image=image, snapshot=snapshot)
    last_snapshot_path = _rbd_image_path(pool=pool, namespace=namespace, image=image, snapshot=last_snapshot)

    logger.info(f'Performing differential backup of {volume}:{image_path} '
                f'from RBD snapshot {last_snapshot} and Benji version {last_version_uid}.')

    snapshot_create(volume=volume, pool=pool, namespace=namespace, image=image, snapshot=snapshot, context=context)
    stdout = subprocess_run(
        ['rbd', 'diff', '--whole-object', '--format=json', '--from-snap', last_snapshot, snapshot_path])
    subprocess_run(['rbd', 'snap', 'rm', last_snapshot_path])

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
        benji_args.extend([f'{pool}:{snapshot_path}', volume])
        result = subprocess_run(benji_args, decode_json=True)
        assert isinstance(result, dict)

    if source_compare:
        # We won't evaluate the returned result but any failure will raise an exception.
        deep_scrub(pool=pool, namespace=namespace, image=image, snapshot=snapshot, version_uid=version_uid)

    return result


def deep_scrub(*,
               pool: str,
               namespace: str = '',
               image: str,
               snapshot: str,
               version_uid: Optional[str]) -> Dict[str, str]:
    snapshot_path = _rbd_image_path(pool=pool, namespace=namespace, image=image, snapshot=snapshot)
    logger.info(f'Comparing source {pool}:{snapshot_path} to {version_uid}.')

    benji_args = [
        'benji', '--machine-output', '--log-level', benji_log_level, 'deep-scrub', '--source',
        f'{pool}:{snapshot_path}', version_uid
    ]

    result = subprocess_run(benji_args, decode_json=True)
    assert isinstance(result, dict)

    return result


def backup(*,
           volume: str,
           pool: str,
           namespace: str = '',
           image: str,
           version_labels: Dict[str, str] = {},
           version_uid: str = None,
           source_compare: bool = False,
           context: Any = None):
    signal_backup_pre.send(SIGNAL_SENDER,
                           volume=volume,
                           pool=pool,
                           namespace=namespace,
                           image=image,
                           version_labels=version_labels,
                           context=context)
    version = None
    try:
        image_path = _rbd_image_path(pool=pool, namespace=namespace, image=image)
        rbd_snap_ls = subprocess_run(['rbd', 'snap', 'ls', '--format=json', image_path], decode_json=True)
        assert isinstance(rbd_snap_ls, list)
        # Snapshot are sorted by their ID, so newer snapshots come last
        benjis_snapshots = [
            snapshot['name'] for snapshot in rbd_snap_ls if snapshot['name'].startswith(RBD_SNAP_NAME_PREFIX)
        ]
        if len(benjis_snapshots) == 0:
            logger.info('No previous RBD snapshot found, performing initial backup.')
            result = backup_initial(volume=volume,
                                    pool=pool,
                                    namespace=namespace,
                                    image=image,
                                    version_uid=version_uid,
                                    version_labels=version_labels,
                                    source_compare=source_compare,
                                    context=context)
        else:
            # Delete all snapshots except the newest
            for snapshot in benjis_snapshots[:-1]:
                snapshot_path = _rbd_image_path(pool=pool, namespace=namespace, image=image, snapshot=snapshot)
                logger.info(f'Deleting older RBD snapshot {snapshot_path}.')
                subprocess_run(['rbd', 'snap', 'rm', snapshot_path])

            last_snapshot = benjis_snapshots[-1]
            last_snapshot_path = _rbd_image_path(pool=pool, namespace=namespace, image=image, snapshot=last_snapshot)
            logger.info(f'Newest RBD snapshot is {last_snapshot_path}.')

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
                                             namespace=namespace,
                                             image=image,
                                             last_snapshot=last_snapshot,
                                             last_version_uid=last_version_uid,
                                             version_uid=version_uid,
                                             version_labels=version_labels,
                                             source_compare=source_compare,
                                             context=context)
            else:
                logger.info(f'Existing RBD snapshot {last_snapshot_path} not found in Benji, deleting it and reverting to initial backup.')
                subprocess_run(['rbd', 'snap', 'rm', last_snapshot_path])
                result = backup_initial(volume=volume,
                                        pool=pool,
                                        namespace=namespace,
                                        image=image,
                                        version_uid=version_uid,
                                        version_labels=version_labels,
                                        source_compare=source_compare,
                                        context=context)
        assert 'versions' in result and isinstance(result['versions'], list)
        version = result['versions'][0]
    except Exception as exception:
        signal_backup_post_error.send(SIGNAL_SENDER,
                                      volume=volume,
                                      pool=pool,
                                      namespace=namespace,
                                      image=image,
                                      version_labels=version_labels,
                                      context=context,
                                      version=version,
                                      exception=exception)
    else:
        signal_backup_post_success.send(SIGNAL_SENDER,
                                        volume=volume,
                                        pool=pool,
                                        namespace=namespace,
                                        image=image,
                                        version_labels=version_labels,
                                        context=context,
                                        version=version)
