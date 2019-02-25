#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# PYTHON_ARGCOMPLETE_OK

import argparse
import fileinput
import json
import os
import sys
from functools import partial
from typing import List, NamedTuple, Type, Optional, Tuple

import argcomplete
from argcomplete.completers import ChoicesCompleter
from prettytable import PrettyTable

import benji.exception
from benji import __version__
from benji.benji import Benji, BenjiStore
from benji.config import Config
from benji.database import Version, VersionUid, VersionStatistic
from benji.factory import StorageFactory
from benji.logging import logger, init_logging
from benji.nbdserver import NbdServer
from benji.utils import hints_from_rbd_diff, PrettyPrint, InputValidation
from benji.versions import VERSIONS


class _ExceptionMapping(NamedTuple):
    exception: Type[BaseException]
    message: str
    exit_code: int


class Commands:
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, machine_output, config):
        self.machine_output = machine_output
        self.config = config

    def backup(self, version_name: str, snapshot_name: str, source: str, rbd_hints: str, base_version_uid: str,
               block_size: int, labels: List[str], storage) -> None:
        # Validate version_name and snapshot_name
        if not InputValidation.is_backup_name(version_name):
            raise benji.exception.UsageError('Version name {} is invalid.'.format(version_name))
        if not InputValidation.is_snapshot_name(snapshot_name):
            raise benji.exception.UsageError('Snapshot name {} is invalid.'.format(snapshot_name))
        base_version_uid_obj = VersionUid(base_version_uid) if base_version_uid else None
        if labels:
            label_add, label_remove = self._parse_labels(labels)
            if label_remove:
                raise benji.exception.UsageError('Wanting to delete labels on a new version is senseless.')
        benji_obj = None
        try:
            benji_obj = Benji(self.config, block_size=block_size)
            hints = None
            if rbd_hints:
                data = ''.join([line for line in fileinput.input(rbd_hints).readline()])
                hints = hints_from_rbd_diff(data)
            backup_version = benji_obj.backup(version_name, snapshot_name, source, hints, base_version_uid_obj, storage)

            if labels:
                for key, value in label_add:
                    benji_obj.add_label(backup_version.uid, key, value)
                for key in label_remove:
                    benji_obj.rm_label(backup_version.uid, key)
                if label_add:
                    logger.info('Added label(s) to version {}: {}.'.format(
                        backup_version.uid.v_string,
                        ', '.join(['{}={}'.format(name, value) for name, value in label_add])))
                if label_remove:
                    logger.info('Removed label(s) from version {}: {}.'.format(backup_version.uid.v_string,
                                                                               ', '.join(label_remove)))

            if self.machine_output:
                benji_obj.export_any({
                    'versions': [backup_version]
                },
                                     sys.stdout,
                                     ignore_relationships=[((Version,), ('blocks',))])
        finally:
            if benji_obj:
                benji_obj.close()

    def restore(self, version_uid: str, destination: str, sparse: bool, force: bool,
                database_backend_less: bool) -> None:
        version_uid_obj = VersionUid(version_uid)
        benji_obj = None
        try:
            benji_obj = Benji(self.config, in_memory_database=database_backend_less)
            if database_backend_less:
                benji_obj.metadata_restore([version_uid_obj])
            benji_obj.restore(version_uid_obj, destination, sparse, force)
        finally:
            if benji_obj:
                benji_obj.close()

    def protect(self, version_uids: List[str]) -> None:
        version_uid_objs = [VersionUid(version_uid) for version_uid in version_uids]
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            for version_uid in version_uid_objs:
                benji_obj.protect(version_uid)
        finally:
            if benji_obj:
                benji_obj.close()

    def unprotect(self, version_uids: List[str]) -> None:
        version_uid_objs = [VersionUid(version_uid) for version_uid in version_uids]
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            for version_uid in version_uid_objs:
                benji_obj.unprotect(version_uid)
        finally:
            if benji_obj:
                benji_obj.close()

    def rm(self, version_uids: List[str], force: bool, keep_metadata_backup: bool, override_lock: bool) -> None:
        version_uid_objs = [VersionUid(version_uid) for version_uid in version_uids]
        disallow_rm_when_younger_than_days = self.config.get('disallowRemoveWhenYounger', types=int)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            for version_uid in version_uid_objs:
                benji_obj.rm(
                    version_uid,
                    force=force,
                    disallow_rm_when_younger_than_days=disallow_rm_when_younger_than_days,
                    keep_metadata_backup=keep_metadata_backup,
                    override_lock=override_lock)
        finally:
            if benji_obj:
                benji_obj.close()

    def scrub(self, version_uid: str, block_percentage: int) -> None:
        version_uid_obj = VersionUid(version_uid)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            benji_obj.scrub(version_uid_obj, block_percentage=block_percentage)
        except benji.exception.ScrubbingError:
            assert benji_obj is not None
            if self.machine_output:
                benji_obj.export_any({
                    'versions': benji_obj.ls(version_uid=version_uid_obj),
                    'errors': benji_obj.ls(version_uid=version_uid_obj)
                },
                                     sys.stdout,
                                     ignore_relationships=[((Version,), ('blocks',))])
            raise
        else:
            if self.machine_output:
                benji_obj.export_any({
                    'versions': benji_obj.ls(version_uid=version_uid_obj),
                    'errors': []
                },
                                     sys.stdout,
                                     ignore_relationships=[((Version,), ('blocks',))])
        finally:
            if benji_obj:
                benji_obj.close()

    def deep_scrub(self, version_uid: str, source: str, block_percentage: int) -> None:
        version_uid_obj = VersionUid(version_uid)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            benji_obj.deep_scrub(version_uid_obj, source=source, block_percentage=block_percentage)
        except benji.exception.ScrubbingError:
            assert benji_obj is not None
            if self.machine_output:
                benji_obj.export_any({
                    'versions': benji_obj.ls(version_uid=version_uid_obj),
                    'errors': benji_obj.ls(version_uid=version_uid_obj)
                },
                                     sys.stdout,
                                     ignore_relationships=[((Version,), ('blocks',))])
            raise
        else:
            if self.machine_output:
                benji_obj.export_any({
                    'versions': benji_obj.ls(version_uid=version_uid_obj),
                    'errors': []
                },
                                     sys.stdout,
                                     ignore_relationships=[((Version,), ('blocks',))])
        finally:
            if benji_obj:
                benji_obj.close()

    def _batch_scrub(self, method: str, filter_expression: Optional[str], version_percentage: int,
                     block_percentage: int, group_label: Optional[str]) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            versions, errors = getattr(benji_obj, method)(filter_expression, version_percentage, block_percentage,
                                                          group_label)
            if errors:
                if self.machine_output:
                    benji_obj.export_any({
                        'versions': versions,
                        'errors': errors,
                    },
                                         sys.stdout,
                                         ignore_relationships=[((Version,), ('blocks',))])
                raise benji.exception.ScrubbingError('One or more version had scrubbing errors: {}.'.format(', '.join(
                    [version.uid.v_string for version in errors])))
            else:
                if self.machine_output:
                    benji_obj.export_any({
                        'versions': versions,
                        'errors': []
                    },
                                         sys.stdout,
                                         ignore_relationships=[((Version,), ('blocks',))])
        finally:
            if benji_obj:
                benji_obj.close()

    def batch_scrub(self, filter_expression: Optional[str], version_percentage: int, block_percentage: int,
                    group_label: Optional[str]) -> None:
        self._batch_scrub('batch_scrub', filter_expression, version_percentage, block_percentage, group_label)

    def batch_deep_scrub(self, filter_expression: Optional[str], version_percentage: int, block_percentage: int,
                         group_label: Optional[str]) -> None:
        self._batch_scrub('batch_deep_scrub', filter_expression, version_percentage, block_percentage, group_label)

    @classmethod
    def _ls_versions_table_output(cls, versions: List[Version], include_labels: bool) -> None:
        tbl = PrettyTable()
        # tbls.field_names.append won't work due to magic inside of PrettyTable
        if include_labels:
            tbl.field_names = [
                'date',
                'uid',
                'name',
                'snapshot_name',
                'size',
                'block_size',
                'status',
                'protected',
                'storage',
                'labels',
            ]
        else:
            tbl.field_names = [
                'date',
                'uid',
                'name',
                'snapshot_name',
                'size',
                'block_size',
                'status',
                'protected',
                'storage',
            ]
        tbl.align['name'] = 'l'
        tbl.align['snapshot_name'] = 'l'
        tbl.align['storage'] = 'l'
        tbl.align['size'] = 'r'
        tbl.align['block_size'] = 'r'
        tbl.align['labels'] = 'l'
        for version in versions:
            row = [
                PrettyPrint.local_time(version.date),
                version.uid.v_string,
                version.name,
                version.snapshot_name,
                PrettyPrint.bytes(version.size),
                PrettyPrint.bytes(version.block_size),
                version.status,
                version.protected,
                StorageFactory.storage_id_to_name(version.storage_id),
            ]
            if include_labels:
                row.append('\n'.join(sorted(['{}={}'.format(label.name, label.value) for label in version.labels])))
            tbl.add_row(row)
        print(tbl)

    @classmethod
    def _stats_table_output(cls, stats: List[VersionStatistic]) -> None:
        tbl = PrettyTable()
        tbl.field_names = [
            'date', 'uid', 'name', 'snapshot_name', 'size', 'block_size', 'storage', 'read', 'written', 'dedup',
            'sparse', 'duration'
        ]
        tbl.align['uid'] = 'l'
        tbl.align['name'] = 'l'
        tbl.align['snapshot_name'] = 'l'
        tbl.align['storage'] = 'l'
        tbl.align['size'] = 'r'
        tbl.align['block_size'] = 'r'
        tbl.align['read'] = 'r'
        tbl.align['written'] = 'r'
        tbl.align['dedup'] = 'r'
        tbl.align['sparse'] = 'r'
        tbl.align['duration'] = 'r'
        for stat in stats:
            augmented_version_uid = '{}{}{}'.format(
                stat.uid.v_string, ',\nbase {}'.format(stat.base_uid.v_string) if stat.base_uid else '',
                ', hints' if stat.hints_supplied else '')
            tbl.add_row([
                PrettyPrint.local_time(stat.date),
                augmented_version_uid,
                stat.name,
                stat.snapshot_name,
                PrettyPrint.bytes(stat.size),
                PrettyPrint.bytes(stat.block_size),
                StorageFactory.storage_id_to_name(stat.storage_id),
                PrettyPrint.bytes(stat.bytes_read),
                PrettyPrint.bytes(stat.bytes_written),
                PrettyPrint.bytes(stat.bytes_dedup),
                PrettyPrint.bytes(stat.bytes_sparse),
                PrettyPrint.duration(stat.duration),
            ])
        print(tbl)

    def ls(self, filter_expression: Optional[str], include_labels: bool) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            versions = benji_obj.ls_with_filter(filter_expression)

            if self.machine_output:
                benji_obj.export_any(
                    {
                        'versions': versions
                    },
                    sys.stdout,
                    ignore_relationships=[((Version,), ('blocks',))],
                )
            else:
                self._ls_versions_table_output(versions, include_labels)
        finally:
            if benji_obj:
                benji_obj.close()

    def stats(self, filter_expression: Optional[str], limit: Optional[int]) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            stats = benji_obj.stats(filter_expression, limit)

            if self.machine_output:
                stats = list(stats)  # resolve iterator, otherwise it's not serializable
                benji_obj.export_any(
                    {
                        'stats': stats
                    },
                    sys.stdout,
                )
            else:
                self._stats_table_output(stats)
        finally:
            if benji_obj:
                benji_obj.close()

    def cleanup(self, override_lock: bool) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            benji_obj.cleanup(override_lock=override_lock)
        finally:
            if benji_obj:
                benji_obj.close()

    def metadata_export(self, filter_expression: Optional[str], output_file: Optional[str], force: bool) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            version_uid_objs = [version.uid for version in benji_obj.ls_with_filter(filter_expression)]
            if output_file is None:
                benji_obj.metadata_export(version_uid_objs, sys.stdout)
            else:
                if os.path.exists(output_file) and not force:
                    raise FileExistsError('The output file already exists.')

                with open(output_file, 'w') as f:
                    benji_obj.metadata_export(version_uid_objs, f)
        finally:
            if benji_obj:
                benji_obj.close()

    def metadata_backup(self, filter_expression: str, force: bool = False) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            version_uid_objs = [version.uid for version in benji_obj.ls_with_filter(filter_expression)]
            benji_obj.metadata_backup(version_uid_objs, overwrite=force)
        finally:
            if benji_obj:
                benji_obj.close()

    def metadata_import(self, input_file: str = None) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            if input_file is None:
                benji_obj.metadata_import(sys.stdin)
            else:
                with open(input_file, 'r') as f:
                    benji_obj.metadata_import(f)
        finally:
            if benji_obj:
                benji_obj.close()

    def metadata_restore(self, version_uids: List[str], storage: str = None) -> None:
        version_uid_objs = [VersionUid(version_uid) for version_uid in version_uids]
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            benji_obj.metadata_restore(version_uid_objs, storage)
        finally:
            if benji_obj:
                benji_obj.close()

    def _metadata_ls_table_output(self, version_uids: List[VersionUid]):
        tbl = PrettyTable()
        tbl.field_names = ['uid']
        tbl.align['uid'] = 'l'
        for version_uid in version_uids:
            tbl.add_row([version_uid.v_string])
        print(tbl)

    def metadata_ls(self, storage: str = None) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            version_uids = benji_obj.metadata_ls(storage)
            if self.machine_output:
                json.dump(
                    [version_uid.v_string for version_uid in version_uids],
                    sys.stdout,
                    indent=2,
                )
            else:
                self._metadata_ls_table_output(version_uids)
        finally:
            if benji_obj:
                benji_obj.close()

    @staticmethod
    def _parse_labels(labels: List[str]) -> Tuple[List[Tuple[str, str]], List[str]]:
        add_list: List[Tuple[str, str]] = []
        remove_list: List[str] = []
        for label in labels:
            if len(label) == 0:
                raise benji.exception.UsageError('A zero-length label is invalid.')

            if label.endswith('-'):
                name = label[:-1]

                if not InputValidation.is_label_name(name):
                    raise benji.exception.UsageError('Label name {} is invalid.'.format(name))

                remove_list.append(name)
            elif label.find('=') > -1:
                name, value = label.split('=')

                if len(name) == 0:
                    raise benji.exception.UsageError('Missing label key in label {}.'.format(label))
                if not InputValidation.is_label_name(name):
                    raise benji.exception.UsageError('Label name {} is invalid.'.format(name))
                if not InputValidation.is_label_value(value):
                    raise benji.exception.UsageError('Label value {} is not a valid.'.format(value))

                add_list.append((name, value))
            else:
                name = label

                if not InputValidation.is_label_name(name):
                    raise benji.exception.UsageError('Label name {} is invalid.'.format(name))

                add_list.append((name, ''))

        return add_list, remove_list

    def label(self, version_uid: str, labels: List[str]) -> None:
        version_uid_obj = VersionUid(version_uid)
        label_add, label_remove = self._parse_labels(labels)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            for name, value in label_add:
                benji_obj.add_label(version_uid_obj, name, value)
            for name in label_remove:
                benji_obj.rm_label(version_uid_obj, name)
            if label_add:
                logger.info('Added label(s) to version {}: {}.'.format(
                    version_uid_obj.v_string, ', '.join(['{}={}'.format(name, value) for name, value in label_add])))
            if label_remove:
                logger.info('Removed label(s) from version {}: {}.'.format(version_uid_obj.v_string,
                                                                           ', '.join(label_remove)))
        finally:
            if benji_obj:
                benji_obj.close()

    def database_init(self) -> None:
        benji_obj = Benji(self.config, init_database=True)
        benji_obj.close()

    def database_migrate(self) -> None:
        benji_obj = Benji(self.config, migrate_database=True)
        benji_obj.close()

    def enforce_retention_policy(self, rules_spec: str, filter_expression: str, dry_run: bool,
                                 keep_metadata_backup: bool, group_label: Optional[str]) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            dismissed_versions = benji_obj.enforce_retention_policy(
                filter_expression=filter_expression,
                rules_spec=rules_spec,
                dry_run=dry_run,
                keep_metadata_backup=keep_metadata_backup,
                group_label=group_label)
            if self.machine_output:
                benji_obj.export_any({
                    'versions': dismissed_versions,
                },
                                     sys.stdout,
                                     ignore_relationships=[((Version,), ('blocks',))])
        finally:
            if benji_obj:
                benji_obj.close()

    def nbd(self, bind_address: str, bind_port: str, read_only: bool) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            store = BenjiStore(benji_obj)
            addr = (bind_address, bind_port)
            server = NbdServer(addr, store, read_only)
            logger.info("Starting to serve NBD on %s:%s" % (addr[0], addr[1]))
            server.serve_forever()
        finally:
            if benji_obj:
                benji_obj.close()

    def version_info(self) -> None:
        if not self.machine_output:
            logger.info('Benji version: {}.'.format(__version__))
            logger.info('Configuration version: {}, supported {}.'.format(VERSIONS.configuration.current,
                                                                          VERSIONS.configuration.supported))
            logger.info('Metadata version: {}, supported {}.'.format(VERSIONS.database_metadata.current,
                                                                     VERSIONS.database_metadata.supported))
            logger.info('Object metadata version: {}, supported {}.'.format(VERSIONS.object_metadata.current,
                                                                            VERSIONS.object_metadata.supported))
        else:
            versions = {
                'version': __version__,
                'configuration_version': {
                    'current': str(VERSIONS.configuration.current),
                    'supported': str(VERSIONS.configuration.supported)
                },
                'database_metadata_version': {
                    'current': str(VERSIONS.database_metadata.current),
                    'supported': str(VERSIONS.database_metadata.supported)
                },
                'object_metadata_version': {
                    'current': str(VERSIONS.object_metadata.current),
                    'supported': str(VERSIONS.object_metadata.supported)
                },
            }
            print(json.dumps(versions, indent=4))


def integer_range(minimum: int, maximum: int, arg: str) -> Optional[int]:
    if arg is None:
        return None

    try:
        value = int(arg)
    except ValueError as err:
        raise argparse.ArgumentTypeError(str(err))

    if value < minimum or (maximum is not None and value > maximum):
        raise argparse.ArgumentTypeError('Expected a value between {} and {}, got {}.'.format(minimum, maximum, value))

    return value


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-c', '--config-file', default=None, type=str, help='Specify a non-default configuration file')
    parser.add_argument(
        '-m', '--machine-output', action='store_true', default=False, help='Enable machine-readable JSON output')
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Only log messages of this level or above on the console')
    parser.add_argument(
        '--no-color', action='store_true', default=False, help='Disable colorization of console logging')

    subparsers_root = parser.add_subparsers(title='commands')

    # BACKUP
    p = subparsers_root.add_parser('backup', help='Perform a backup')
    p.add_argument('-s', '--snapshot-name', default='', help='Snapshot name (e.g. the name of the RBD snapshot)')
    p.add_argument('-r', '--rbd-hints', default=None, help='Hints in rbd diff JSON format')
    p.add_argument('-f', '--base-version', dest='base_version_uid', default=None, help='Base version UID')
    p.add_argument('-b', '--block-size', type=int, default=None, help='Block size in bytes')
    p.add_argument(
        '-l',
        '--label',
        action='append',
        dest='labels',
        metavar='label',
        default=None,
        help='Labels for this version (can be repeated)')

    p.add_argument('-S', '--storage', default='', help='Destination storage (if unspecified the default is used)')
    p.add_argument('source', help='Source URL').completer = ChoicesCompleter(('file://', 'rbd://'))  # type: ignore
    p.add_argument('version_name', help='Backup version name (e.g. the hostname)')
    p.set_defaults(func='backup')

    # RESTORE
    p = subparsers_root.add_parser('restore', help='Restore a backup')
    p.add_argument('-s', '--sparse', action='store_true', help='Restore only existing blocks')
    p.add_argument('-f', '--force', action='store_true', help='Overwrite an existing file, device or image')
    p.add_argument(
        '-d', '--database-backend-less', action='store_true', help='Restore without requiring the database backend')
    p.add_argument('version_uid', help='Version UID to restore')
    # yapf: disable (otherwise YAPF would break this line and mypy won't apply the type: ignore properly)
    p.add_argument('destination', help='Destination URL').completer = ChoicesCompleter(('file://', 'rbd://'))  # type: ignore
    # yapf: enable

    p.set_defaults(func='restore')

    # NBD
    p = subparsers_root.add_parser(
        'nbd', help='Start an NBD server', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('-a', '--bind-address', default='127.0.0.1', help='Bind to the specified IP address')
    p.add_argument('-p', '--bind-port', default=10809, help='Bind to the specified port')
    p.add_argument('-r', '--read-only', action='store_true', default=False, help='NBD device is read-only')
    p.set_defaults(func='nbd')

    # LABEL
    p = subparsers_root.add_parser('label', help='Add labels to a version')
    p.add_argument('version_uid')
    p.add_argument('labels', nargs='+')
    p.set_defaults(func='label')

    # LS
    p = subparsers_root.add_parser('ls', help='List versions')
    p.add_argument('filter_expression', nargs='?', default=None, help='Version filter expression')
    p.add_argument('-l', '--include-labels', action='store_true', help='Include labels in output')
    p.set_defaults(func='ls')

    # RM
    p = subparsers_root.add_parser('rm', help='Remove one or more versions')
    p.add_argument('-f', '--force', action='store_true', help='Force removal (overrides protection of recent versions)')
    p.add_argument('-k', '--keep-metadata-backup', action='store_true', help='Keep version metadata backup')
    p.add_argument('--override-lock', action='store_true', help='Override and release any held locks (dangerous)')
    p.add_argument('version_uids', metavar='version_uid', nargs='+', help='Version UID')
    p.set_defaults(func='rm')

    # ENFORCE
    p = subparsers_root.add_parser('enforce', help="Enforce a retention policy ")
    p.add_argument('--dry-run', action='store_true', help='Only show which versions would be removed')
    p.add_argument('-k', '--keep-metadata-backup', action='store_true', help='Keep version metadata backup')
    p.add_argument('-g', '--group_label', default=None, help='Label to find related versions to remove')
    p.add_argument('rules_spec', help='Retention rules specification')
    p.add_argument('filter_expression', nargs='?', default=None, help='Version filter expression')
    p.set_defaults(func='enforce_retention_policy')

    # CLEANUP
    p = subparsers_root.add_parser('cleanup', help='Cleanup no longer referenced blocks')
    p.add_argument('--override-lock', action='store_true', help='Override and release any held lock (dangerous)')
    p.set_defaults(func='cleanup')

    # PROTECT
    p = subparsers_root.add_parser('protect', help='Protect one or more versions')
    p.add_argument('version_uids', metavar='version_uid', nargs='+', help="Version UID")
    p.set_defaults(func='protect')

    # UNPROTECT
    p = subparsers_root.add_parser('unprotect', help='Unprotect one or more versions')
    p.add_argument('version_uids', metavar='version_uid', nargs='+', help='Version UID')
    p.set_defaults(func='unprotect')

    # SCRUB
    p = subparsers_root.add_parser(
        'scrub',
        help='Check block existence and metadata consistency of a version',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument(
        '-p',
        '--block-percentage',
        type=partial(integer_range, 1, 100),
        default=100,
        help='Check only a certain percentage of blocks')
    p.add_argument('version_uid', help='Version UID')
    p.set_defaults(func='scrub')

    # DEEP-SCRUB
    p = subparsers_root.add_parser(
        'deep-scrub',
        help='Check version data integrity of a version',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('-s', '--source', default=None, help='Additionally compare version against source URL')
    p.add_argument(
        '-p',
        '--block-percentage',
        type=partial(integer_range, 1, 100),
        default=100,
        help='Check only a certain percentage of blocks')
    p.add_argument('version_uid', help='Version UID')
    p.set_defaults(func='deep_scrub')

    # BULK-SCRUB
    p = subparsers_root.add_parser(
        'batch-scrub',
        help='Check block existence and metadata consistency of multiple versions',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument(
        '-p',
        '--block-percentage',
        type=partial(integer_range, 1, 100),
        default=100,
        help='Check only a certain percentage of blocks')
    p.add_argument(
        '-P',
        '--version-percentage',
        type=partial(integer_range, 1, 100),
        default=100,
        help='Check only a certain percentage of versions')
    p.add_argument('-g', '--group_label', default=None, help='Label to find related versions')
    p.add_argument('filter_expression', nargs='?', default=None, help='Version filter expression')
    p.set_defaults(func='batch_scrub')

    # BULK-DEEP-SCRUB
    p = subparsers_root.add_parser(
        'batch-deep-scrub',
        help='Check version data integrity of multiple versions',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument(
        '-p',
        '--block-percentage',
        type=partial(integer_range, 1, 100),
        default=100,
        help='Check only a certain percentage of blocks')
    p.add_argument(
        '-P',
        '--version-percentage',
        type=partial(integer_range, 1, 100),
        default=100,
        help='Check only a certain percentage of versions')
    p.add_argument('-g', '--group_label', default=None, help='Label to find related versions')
    p.add_argument('filter_expression', nargs='?', default=None, help='Version filter expression')
    p.set_defaults(func='batch_deep_scrub')

    # METADATA EXPORT
    p = subparsers_root.add_parser(
        'metadata-export', help='Export the metadata of one or more versions to a file or standard output')
    p.add_argument('filter_expression', nargs='?', default=None, help="Version filter expression")
    p.add_argument('-f', '--force', action='store_true', help='Overwrite an existing output file')
    p.add_argument('-o', '--output-file', default=None, help='Output file (standard output if missing)')
    p.set_defaults(func='metadata_export')

    # METADATA-IMPORT
    p = subparsers_root.add_parser(
        'metadata-import', help='Import the metadata of one or more versions from a file or standard input')
    p.add_argument('-i', '--input-file', default=None, help='Input file (standard input if missing)')
    p.set_defaults(func='metadata_import')

    # METADATA-BACKUP
    p = subparsers_root.add_parser('metadata-backup', help='Back up the metadata of one or more versions')
    p.add_argument('filter_expression', help="Version filter expression")
    p.add_argument('-f', '--force', action='store_true', help='Overwrite existing metadata backups')
    p.set_defaults(func='metadata_backup')

    # METADATA-RESTORE
    p = subparsers_root.add_parser('metadata-restore', help='Restore the metadata of one ore more versions')
    p.add_argument('-S', '--storage', default=None, help='Source storage (if unspecified the default is used)')
    p.add_argument('version_uids', metavar='VERSION_UID', nargs='+', help="Version UID")
    p.set_defaults(func='metadata_restore')

    # METADATA-LS
    p = subparsers_root.add_parser('metadata-ls', help='List the version metadata backup')
    p.add_argument('-S', '--storage', default=None, help='Source storage (if unspecified the default is used)')
    p.set_defaults(func='metadata_ls')

    # STATS
    p = subparsers_root.add_parser('stats', help='Show backup statistics')
    p.add_argument('filter_expression', nargs='?', help='Statistics filter expression')
    p.add_argument(
        '-l',
        '--limit',
        default=None,
        type=partial(integer_range, 1, None),
        help='Limit output to this number of entries')
    p.set_defaults(func='stats')

    # VERSION-INFO
    p = subparsers_root.add_parser('version-info', help='Program version information')
    p.set_defaults(func='version_info')

    # DATABASE-INIT
    p = subparsers_root.add_parser(
        'database-init', help='Initialize the database (will not delete existing tables or data)')
    p.set_defaults(func='database_init')

    # MIGRATE
    p = subparsers_root.add_parser('database-migrate', help='Migrate an existing database to a new schema revision')
    p.set_defaults(func='database_migrate')

    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    if not hasattr(args, 'func'):
        parser.print_usage()
        exit(os.EX_USAGE)

    if args.config_file is not None and args.config_file != '':
        try:
            cfg = open(args.config_file, 'r', encoding='utf-8').read()
        except FileNotFoundError:
            logger.error('File {} not found.'.format(args.config_file))
            exit(os.EX_USAGE)
        config = Config(ad_hoc_config=cfg)
    else:
        config = Config()

    init_logging(config.get('logFile', types=(str, type(None))), args.log_level, no_color=args.no_color)

    if sys.hexversion < 0x030600F0:
        raise benji.exception.InternalError('Benji only supports Python 3.6 or above.')

    if sys.hexversion < 0x030604F0:
        logger.warning('The installed Python version will use excessive amounts of memory when used with Benji. Upgrade Python to at least 3.6.4.')

    commands = Commands(args.machine_output, config)
    func = getattr(commands, args.func)

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args['config_file']
    del func_args['func']
    del func_args['log_level']
    del func_args['machine_output']
    del func_args['no_color']

    # From most specific to least specific
    exception_mappings = [
        _ExceptionMapping(exception=benji.exception.UsageError, message='Usage error', exit_code=os.EX_USAGE),
        _ExceptionMapping(
            exception=benji.exception.AlreadyLocked, message='Already locked error', exit_code=os.EX_NOPERM),
        _ExceptionMapping(exception=benji.exception.InternalError, message='Internal error', exit_code=os.EX_SOFTWARE),
        _ExceptionMapping(
            exception=benji.exception.ConfigurationError, message='Configuration error', exit_code=os.EX_CONFIG),
        _ExceptionMapping(
            exception=benji.exception.InputDataError, message='Input data error', exit_code=os.EX_DATAERR),
        _ExceptionMapping(exception=benji.exception.ScrubbingError, message='Scrubbing error', exit_code=os.EX_DATAERR),
        _ExceptionMapping(exception=PermissionError, message='Already locked error', exit_code=os.EX_NOPERM),
        _ExceptionMapping(exception=FileExistsError, message='Already exists', exit_code=os.EX_CANTCREAT),
        _ExceptionMapping(exception=FileNotFoundError, message='Not found', exit_code=os.EX_NOINPUT),
        _ExceptionMapping(exception=EOFError, message='I/O error', exit_code=os.EX_IOERR),
        _ExceptionMapping(exception=IOError, message='I/O error', exit_code=os.EX_IOERR),
        _ExceptionMapping(exception=OSError, message='Not found', exit_code=os.EX_OSERR),
        _ExceptionMapping(exception=ConnectionError, message='I/O error', exit_code=os.EX_IOERR),
        _ExceptionMapping(exception=LookupError, message='Not found', exit_code=os.EX_NOINPUT),
        _ExceptionMapping(exception=BaseException, message='Other exception', exit_code=os.EX_SOFTWARE),
    ]

    try:
        logger.debug('commands.{0}(**{1!r})'.format(args.func, func_args))
        func(**func_args)
        exit(0)
    except SystemExit:
        raise
    except BaseException as exception:
        for case in exception_mappings:
            if isinstance(exception, case.exception):
                logger.debug(case.message, exc_info=True)
                logger.error(str(exception))
                exit(case.exit_code)


if __name__ == '__main__':
    main()
