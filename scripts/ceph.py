import json
import logging
import os
import re
import subprocess
from datetime import datetime
from functools import reduce
from json import JSONDecodeError
from operator import or_
from tempfile import NamedTemporaryFile
from typing import Dict, Any, Optional, List, Union

from blinker import signal

SIGNAL_SENDER = 'ceph'
RBD_SNAP_CREATE_TIMEOUT = 30
RBD_SNAP_NAME_PREFIX = 'b-'

logger = logging.getLogger(__name__)
benji_log_level = os.getenv('BENJI_LOG_LEVEL', 'INFO')

signal_snapshot_create_pre = signal('snapshot_create_pre')
signal_snapshot_create_post_success = signal('snapshot_create_post_success')
signal_snapshot_create_post_error = signal('snapshot_create_post_error')
signal_backup_pre = signal('backup_pre')
signal_backup_post_success = signal('on_backup_post_success')
signal_backup_post_error = signal('on_backup_post_error')
signal_restore_pre = signal('restore_pre')
signal_restore_post_success = signal('restore_post_success')
signal_restore_post_error = signal('restore_post_error')


def _one_line_stderr(stderr: str):
    stderr = re.sub(r'\n(?!$)', ' | ', stderr)
    stderr = re.sub(r'\s+', ' ', stderr)
    return stderr


def subprocess_run(args: List[str],
                   input: str = None,
                   timeout: int = None,
                   decode_json: bool = False,
                   args_repr: str = None) -> Union[Dict, List, str]:
    if args_repr is not None:
        logger.info(f'Running process: {args_repr}')
    else:
        logger.info('Running process: {}'.format(' '.join(args)))
    try:

        result = subprocess.run(args=args,
                                input=input,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                encoding='utf-8',
                                errors='ignore',
                                timeout=timeout)
    except subprocess.TimeoutExpired as exception:
        stderr = _one_line_stderr(exception.stderr)
        raise RuntimeError(f'{args[0]} invocation failed due to timeout with output: ' + stderr) from None
    except Exception as exception:
        raise RuntimeError(f'{args[0]} invocation failed with a {type(exception).__name__} exception: {str(exception)}') from None

    if result.stderr != '':
        for line in result.stderr.splitlines():
            logger.info(line)

    if result.returncode == 0:
        logger.debug('Process finished successfully.')
        if decode_json:
            try:
                stdout_json = json.loads(result.stdout)
            except JSONDecodeError:
                raise RuntimeError(f'{args[0]} invocation was successful but did not return valid JSON. Output on stderr was: {_one_line_stderr(result.stderr)}.')

            if stdout_json is None or not isinstance(stdout_json, (dict, list)):
                raise RuntimeError(f'{args[0]} invocation was successful but did return null or empty JSON dictonary. Output on stderr was: {_one_line_stderr(result.stderr)}.')

            return stdout_json
        else:
            return result.stdout
    else:
        raise RuntimeError(f'{args[0]} invocation failed with return code {result.returncode} and output: {_one_line_stderr(result.stderr)}')


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
        raise_exception = reduce(
            or_,
            map(
                lambda r: bool(r[1]),
                signal_snapshot_create_post_error.send(SIGNAL_SENDER,
                                                       volume=volume,
                                                       pool=pool,
                                                       image=image,
                                                       snapshot=snapshot,
                                                       context=context,
                                                       exception=exception)))
        if raise_exception:
            raise
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


def backup_differential(*,
                        volume: str,
                        pool: str,
                        image: str,
                        last_snapshot: str,
                        base_version_uid: str,
                        version_labels: Dict[str, str],
                        version_uid: Optional[str],
                        context: Any = None) -> Dict[str, str]:
    logger.info(f'Performing differential backup of {volume}:{pool}/{image} from RBD snapshot" \
        "{last_snapshot} and Benji version {base_version_uid}.')

    now = datetime.utcnow()
    snapshot = now.strftime(RBD_SNAP_NAME_PREFIX + '%Y-%m-%dT%H:%M:%SZ')

    snapshot_create(volume=volume, pool=pool, image=image, snapshot=snapshot, context=context)
    stdout = subprocess_run(
        ['rbd', 'diff', '--whole-object', '--format=json', '--from-snap', last_snapshot, f'{pool}/{image}@{snapshot}'])
    subprocess_run(['rbd', 'snap', 'rm', f'{pool}/{image}@{last_snapshot}'])

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
    result = None
    try:
        rbd_snap_ls = subprocess_run(['rbd', 'snap', 'ls', '--format=json', f'{pool}/{image}'], decode_json=True)
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
            if len(benji_ls['versions']) > 0:
                base_version_uid = benji_ls['versions'][0]['uid']
                result = backup_differential(volume=volume,
                                             pool=pool,
                                             image=image,
                                             last_snapshot=last_snapshot,
                                             base_version_uid=base_version_uid,
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
        return result
    except Exception as exception:
        raise_exception = reduce(
            or_,
            map(
                lambda r: bool(r[1]),
                signal_backup_post_error.send(SIGNAL_SENDER,
                                              volume=volume,
                                              pool=pool,
                                              image=image,
                                              version_labels=version_labels,
                                              context=context,
                                              result=result,
                                              exception=exception)))
        if raise_exception:
            raise
    else:
        signal_backup_post_success.send(SIGNAL_SENDER,
                                        volume=volume,
                                        pool=pool,
                                        image=image,
                                        version_labels=version_labels,
                                        context=context,
                                        result=result)


def restore(version_uid: str, pool: str, image: str, context: Any = None):
    signal_snapshot_create_pre.send(SIGNAL_SENDER, version_uid=version_uid, pool=pool, image=image, context=context)
    result = None
    try:
        result = subprocess_run([
            'benji', '--machine-output', '--log-level', benji_log_level, 'restore', '--sparse', '--force', version_uid,
            f'{pool}:{pool}/{image}'
        ])
    except Exception as exception:
        raise_exception = reduce(
            or_,
            map(
                lambda r: bool(r[1]),
                signal_restore_post_error.send(SIGNAL_SENDER,
                                               version_uid=version_uid,
                                               pool=pool,
                                               image=image,
                                               context=context,
                                               result=result,
                                               exception=exception)))
        if raise_exception:
            raise
    else:
        signal_restore_post_success.send(SIGNAL_SENDER, pool=pool, image=image, context=context, result=result)
