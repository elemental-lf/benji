#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# PYTHON_ARGCOMPLETE_OK

import argparse
import fileinput
import logging
import os
import random
import sys
from functools import partial
from typing import Dict, List, NamedTuple, Type

import argcomplete
import pkg_resources
from argcomplete.completers import ChoicesCompleter
from prettytable import PrettyTable

import benji.exception
from benji.benji import Benji, BenjiStore
from benji.blockuidhistory import BlockUidHistory
from benji.config import Config
from benji.factory import StorageFactory
from benji.logging import logger, init_logging
from benji.database import Version, VersionUid, Stats
from benji.nbdserver import NbdServer
from benji.utils import hints_from_rbd_diff, PrettyPrint

__version__ = pkg_resources.get_distribution('benji').version


class _ExceptionMapping(NamedTuple):
    exception: Type[BaseException]
    message: str
    exit_code: int


class Commands:
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, machine_output, config):
        self.machine_output = machine_output
        self.config = config

    def backup(self,
               version_name: str,
               snapshot_name: str,
               source: str,
               rbd_hints: str,
               base_version_uid: str,
               block_size: int = None,
               tags: List[str] = None,
               storage=None) -> None:
        base_version_uid_obj = VersionUid(base_version_uid)
        benji_obj = None
        try:
            benji_obj = Benji(self.config, block_size=block_size)
            hints = None
            if rbd_hints:
                data = ''.join([line for line in fileinput.input(rbd_hints).readline()])
                hints = hints_from_rbd_diff(data)
            backup_version_uid = benji_obj.backup(version_name, snapshot_name, source, hints, base_version_uid_obj,
                                                  tags, storage)
            if self.machine_output:
                benji_obj.export_any({
                    'versions': benji_obj.ls(version_uid=backup_version_uid)
                },
                                     sys.stdout,
                                     ignore_relationships=[((Version,), ('blocks',))])
        finally:
            if benji_obj:
                benji_obj.close()

    def restore(self,
                version_uid: str,
                destination: str,
                sparse: bool,
                force: bool,
                database_backend_less: bool = False) -> None:
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
                try:
                    benji_obj.protect(version_uid)
                except benji.exception.NoChange:
                    logger.warning('Version {} already was protected.'.format(version_uid))
        finally:
            if benji_obj:
                benji_obj.close()

    def unprotect(self, version_uids: List[str]) -> None:
        version_uid_objs = [VersionUid(version_uid) for version_uid in version_uids]
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            for version_uid in version_uid_objs:
                try:
                    benji_obj.unprotect(version_uid)
                except benji.exception.NoChange:
                    logger.warning('Version {} already was unprotected.'.format(version_uid))
        finally:
            if benji_obj:
                benji_obj.close()

    def rm(self, version_uids: List[str], force: bool, keep_backend_metadata: bool) -> None:
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
                    keep_backend_metadata=keep_backend_metadata)
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

    def _bulk_scrub(self, method: str, names: List[str], tags: List[str], version_percentage: int,
                    block_percentage: int) -> None:
        history = BlockUidHistory()
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            versions = []
            if names:
                for name in names:
                    versions.extend(benji_obj.ls(version_name=name, version_tags=tags))
            else:
                versions.extend(benji_obj.ls(version_tags=tags))
            errors = []
            if version_percentage and versions:
                # Will always scrub at least one matching version
                versions = random.sample(versions, max(1, int(len(versions) * version_percentage / 100)))
            if not versions:
                logger.info('No matching versions found.')
            for version in versions:
                try:
                    logging.info('Scrubbing version {} with name {}.'.format(version.uid.v_string, version.name))
                    getattr(benji_obj, method)(version.uid, block_percentage=block_percentage, history=history)
                except benji.exception.ScrubbingError as exception:
                    logger.error(exception)
                    errors.append(version)
                except:
                    raise
            if errors:
                if self.machine_output:
                    benji_obj.export_any({
                        'versions': [benji_obj.ls(version_uid=version.uid)[0] for version in versions],
                        'errors': [benji_obj.ls(version_uid=version.uid)[0] for version in errors]
                    },
                                         sys.stdout,
                                         ignore_relationships=[((Version,), ('blocks',))])
                raise benji.exception.ScrubbingError('One or more version had scrubbing errors: {}.'.format(', '.join(
                    [version.uid.v_string for version in errors])))
            else:
                if self.machine_output:
                    benji_obj.export_any({
                        'versions': [benji_obj.ls(version_uid=version.uid)[0] for version in versions],
                        'errors': []
                    },
                                         sys.stdout,
                                         ignore_relationships=[((Version,), ('blocks',))])
        finally:
            if benji_obj:
                benji_obj.close()

    def bulk_scrub(self, names: List[str], tags: List[str], version_percentage: int, block_percentage: int) -> None:
        self._bulk_scrub('scrub', names, tags, version_percentage, block_percentage)

    def bulk_deep_scrub(self, names: List[str], tags: List[str], version_percentage: int, block_percentage: int) -> None:
        self._bulk_scrub('deep_scrub', names, tags, version_percentage, block_percentage)

    @classmethod
    def _ls_versions_tbl_output(cls, versions: List[Version]) -> None:
        tbl = PrettyTable()
        tbl.field_names = [
            'date', 'uid', 'name', 'snapshot_name', 'size', 'block_size', 'valid', 'protected', 'storage', 'tags'
        ]
        tbl.align['name'] = 'l'
        tbl.align['snapshot_name'] = 'l'
        tbl.align['tags'] = 'l'
        tbl.align['storage'] = 'l'
        tbl.align['size'] = 'r'
        tbl.align['block_size'] = 'r'
        for version in versions:
            tbl.add_row([
                PrettyPrint.local_time(version.date),
                version.uid.v_string,
                version.name,
                version.snapshot_name,
                PrettyPrint.bytes(version.size),
                PrettyPrint.bytes(version.block_size),
                version.valid,
                version.protected,
                StorageFactory.storage_id_to_name(version.storage_id),
                ",".join(sorted([t.name for t in version.tags])),
            ])
        print(tbl)

    @classmethod
    def _stats_tbl_output(cls, stats: List[Stats]) -> None:
        tbl = PrettyTable()
        tbl.field_names = [
            'date', 'uid', 'name', 'snapshot_name', 'size', 'block_size', 'storage', 'read', 'written', 'dedup',
            'sparse', 'duration (s)'
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
        tbl.align['duration (s)'] = 'r'
        for stat in stats:
            augmented_version_uid = '{}{}{}'.format(
                stat.version_uid.v_string,
                ',\nbase {}'.format(stat.base_version_uid.v_string) if stat.base_version_uid else '',
                ', hints' if stat.hints_supplied else '')
            tbl.add_row([
                PrettyPrint.local_time(stat.version_date),
                augmented_version_uid,
                stat.version_name,
                stat.version_snapshot_name,
                PrettyPrint.bytes(stat.version_size),
                PrettyPrint.bytes(stat.version_block_size),
                StorageFactory.storage_id_to_name(stat.version_storage_id),
                PrettyPrint.bytes(stat.bytes_read),
                PrettyPrint.bytes(stat.bytes_written),
                PrettyPrint.bytes(stat.bytes_dedup),
                PrettyPrint.bytes(stat.bytes_sparse),
                PrettyPrint.duration(stat.duration_seconds),
            ])
        print(tbl)

    def ls(self, name: str, snapshot_name: str = None, tags: List[str] = None, include_blocks: bool = False) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            versions = benji_obj.ls(version_name=name, version_snapshot_name=snapshot_name, version_tags=tags)

            if self.machine_output:
                benji_obj.export_any(
                    {
                        'versions': versions
                    },
                    sys.stdout,
                    ignore_relationships=[((Version,), ('blocks',))] if not include_blocks else [],
                )
            else:
                self._ls_versions_tbl_output(versions)
        finally:
            if benji_obj:
                benji_obj.close()

    def stats(self, version_uid: str = None, limit: int = None) -> None:
        version_uid_obj = VersionUid(version_uid) if version_uid else None
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            stats = benji_obj.stats(version_uid_obj, limit)

            if self.machine_output:
                stats = list(stats)  # resolve iterator, otherwise it's not serializable
                benji_obj.export_any(
                    {
                        'stats': stats
                    },
                    sys.stdout,
                )
            else:
                self._stats_tbl_output(stats)
        finally:
            if benji_obj:
                benji_obj.close()

    def cleanup(self) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            benji_obj.cleanup()
        finally:
            if benji_obj:
                benji_obj.close()

    def metadata_export(self, version_uids: List[str], output_file: str = None, force: bool = False) -> None:
        version_uid_objs = [VersionUid(version_uid) for version_uid in version_uids]
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
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

    def metadata_backup(self, version_uids: List[str], force: bool = False) -> None:
        version_uid_objs = [VersionUid(version_uid) for version_uid in version_uids]
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
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

    def metadata_ls(self, storage: str = None) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            version_uids = benji_obj.metadata_ls(storage)
            for version_uid in version_uids:
                print(version_uid.v_string)
        finally:
            if benji_obj:
                benji_obj.close()

    def add_tag(self, version_uid: str, names: List[str]) -> None:
        version_uid_obj = VersionUid(version_uid)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            for name in names:
                try:
                    benji_obj.add_tag(version_uid_obj, name)
                except benji.exception.NoChange:
                    logger.warning('Version {} already tagged with {}.'.format(version_uid_obj, name))
        finally:
            if benji_obj:
                benji_obj.close()

    def rm_tag(self, version_uid: str, names: List[str]) -> None:
        version_uid_obj = VersionUid(version_uid)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            for name in names:
                try:
                    benji_obj.rm_tag(version_uid_obj, name)
                except benji.exception.NoChange:
                    logger.warning('Version {} has no tag {}.'.format(version_uid_obj, name))
        finally:
            if benji_obj:
                benji_obj.close()

    def init(self) -> None:
        benji_obj = Benji(self.config, init_database=True)
        benji_obj.close()

    def enforce_retention_policy(self, rules_spec, version_names, dry_run, keep_backend_metadata):
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            dismissed_version_uids = []
            for version_name in version_names:
                dismissed_version_uids.extend(
                    benji_obj.enforce_retention_policy(
                        version_name=version_name,
                        rules_spec=rules_spec,
                        dry_run=dry_run,
                        keep_backend_metadata=keep_backend_metadata))
            if self.machine_output:
                benji_obj.export_any({
                    'versions': [benji_obj.ls(version_uid=version_uid)[0] for version_uid in dismissed_version_uids]
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
            logger.info("Starting to serve nbd on %s:%s" % (addr[0], addr[1]))
            server.serve_forever()
        finally:
            if benji_obj:
                benji_obj.close()


def integer_range(minimum: int, maximum: int, arg: str) -> int:
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
        '--no-color', action='store_true', default=False, help='Disable colorization of console logging')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Enable verbose output')

    subparsers_root = parser.add_subparsers(title='commands')

    # BACKUP
    p = subparsers_root.add_parser('backup', help='Perform a backup')
    p.add_argument('-s', '--snapshot-name', default='', help='Snapshot name (e.g. the name of the RBD snapshot)')
    p.add_argument('-r', '--rbd-hints', default=None, help='Hints in rbd diff JSON format')
    p.add_argument('-f', '--base-version', dest='base_version_uid', default=None, help='Base version UID')
    p.add_argument(
        '-t', '--tag', action='append', dest='tags', metavar='TAG', default=None, help='Tag version (may be repeated)')
    p.add_argument('-b', '--block-size', type=int, help='Block size in bytes')
    p.add_argument('-S', '--storage', default='', help='Destination storage (if unspecified the default is used)')
    p.add_argument('source', help='Source URL').completer = ChoicesCompleter(('file://', 'rbd://'))  # type: ignore
    p.add_argument('version_name', help='Backup version name (e.g. the hostname)')
    p.set_defaults(func='backup')

    # RESTORE
    p = subparsers_root.add_parser('restore', help='Restore a backup')
    p.add_argument('-s', '--sparse', action='store_true', help='Restore only existing blocks')
    p.add_argument('-f', '--force', action='store_true', help='Overwrite an existing file, device or image')
    p.add_argument(
        '-M', '--metadata-backend-less', action='store_true', help='Restore without requiring the database backend')
    p.add_argument('version_uid', help='Version UID to restore')
    p.add_argument(
        'destination', help='Destination URL').completer = ChoicesCompleter(('file://', 'rbd://'))  # type: ignore

    p.set_defaults(func='restore')

    # NBD
    p = subparsers_root.add_parser(
        'nbd', help='Start an NBD server', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('-a', '--bind-address', default='127.0.0.1', help='Bind to the specified IP address')
    p.add_argument('-p', '--bind-port', default=10809, help='Bind to the specified port')
    p.add_argument('-r', '--read-only', action='store_true', default=False, help='NBD device is read-only')
    p.set_defaults(func='nbd')

    # LS
    p = subparsers_root.add_parser('ls', help='List existing versions')
    p.add_argument('name', nargs='?', default=None, help='Limit output to the specified version name')
    p.add_argument('-s', '--snapshot-name', default=None, help='Limit output to the specified version snapshot name')
    p.add_argument(
        '-t',
        '--tag',
        action='append',
        dest='tags',
        metavar='TAG',
        default=None,
        help='Limit output to versions matching tag (multiple use of this option constitutes a logical or operation)')
    p.add_argument(
        '--include-blocks',
        default=False,
        action='store_true',
        help='Include blocks in output (machine readable output only)')
    p.set_defaults(func='ls')

    # RM
    p = subparsers_root.add_parser('rm', help='Remove one or more versions')
    p.add_argument('-f', '--force', action='store_true', help='Force removal (overrides protection of recent versions)')
    p.add_argument('-k', '--keep-backend-metadata', action='store_true', help='Keep version metadata backup')
    p.add_argument('version_uids', metavar='version_uid', nargs='+', help='Version UID')
    p.set_defaults(func='rm')

    # ENFORCE
    p = subparsers_root.add_parser('enforce', help="Enforce a retention policy ")
    p.add_argument('--dry-run', action='store_true', help='Only show which versions would be removed')
    p.add_argument('-k', '--keep-backend-metadata', action='store_true', help='Keep version metadata backup')
    p.add_argument('rules_spec', help='Retention rules specification')
    p.add_argument('version_names', metavar='version_name', nargs='+', help='One or more version names')
    p.set_defaults(func='enforce_retention_policy')

    # CLEANUP
    p = subparsers_root.add_parser('cleanup', help='Cleanup no longer referenced blocks')
    p.set_defaults(func='cleanup')

    # PROTECT
    p = subparsers_root.add_parser('protect', help='Protect one or more versions')
    p.add_argument('version_uids', metavar='version_uid', nargs='+', help="Version UID")
    p.set_defaults(func='protect')

    # UNPROTECT
    p = subparsers_root.add_parser('unprotect', help='Unprotect one or more versions')
    p.add_argument('version_uids', metavar='version_uid', nargs='+', help='Version UID')
    p.set_defaults(func='unprotect')

    # ADD TAG
    p = subparsers_root.add_parser('add-tag', help='Add a tag to a version')
    p.add_argument('version_uid')
    p.add_argument('names', metavar='NAME', nargs='+')
    p.set_defaults(func='add_tag')

    # REMOVE TAG
    p = subparsers_root.add_parser('rm-tag', help='Remove a tag from a version')
    p.add_argument('version_uid')
    p.add_argument('names', metavar='NAME', nargs='+')
    p.set_defaults(func='rm_tag')

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
        'bulk-scrub',
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
        help='Check only a certain percentage of blocks')
    p.add_argument(
        '-t',
        '--tag',
        action='append',
        dest='tags',
        metavar='TAG',
        default=None,
        help='Limit scrubbed versions based on tag (multiple use of this option constitutes a logical or operation)')
    p.add_argument('names', metavar='name', nargs='*', help="Version name")
    p.set_defaults(func='bulk_scrub')

    # BULK-DEEP-SCRUB
    p = subparsers_root.add_parser(
        'bulk-deep-scrub',
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
        help='Check only a certain percentage of blocks')
    p.add_argument(
        '-t',
        '--tag',
        action='append',
        dest='tags',
        metavar='TAG',
        default=None,
        help='Limit scrubbed versions based on tag (multiple use of this option constitutes a logical or operation)')
    p.add_argument('names', metavar='name', nargs='*', help='Version name')
    p.set_defaults(func='bulk_deep_scrub')

    # METADATA EXPORT
    p = subparsers_root.add_parser(
        'metadata-export', help='Export the metadata of one or more versions to a file or standard output')
    p.add_argument('version_uids', metavar='VERSION_UID', nargs='+', help="Version UID")
    p.add_argument('-f', '--force', action='store_true', help='Overwrite an existing output file')
    p.add_argument('-o', '--output-file', help='Output file (standard output if missing)')
    p.set_defaults(func='metadata_export')

    # METADATA IMPORT
    p = subparsers_root.add_parser(
        'metadata-import', help='Import the metadata of one or more versions from a file or standard input')
    p.add_argument('-i', '--input-file', help='Input file (standard input if missing)')
    p.set_defaults(func='metadata_import')

    # METADATA BACKUP
    p = subparsers_root.add_parser('netadata-backup', help='Back up the metadata of one or more versions')
    p.add_argument('version_uids', metavar='VERSION_UID', nargs='+', help="Version UID")
    p.add_argument('-f', '--force', action='store_true', help='Overwrite existing metadata backups')
    p.set_defaults(func='metadata_backup')

    # METADATA RESTORE
    p = subparsers_root.add_parser('metadata-restore', help='Restore the metadata of one ore more versions')
    p.add_argument('-S', '--storage', help='Source storage (if unspecified the default is used)')
    p.add_argument('version_uids', metavar='VERSION_UID', nargs='+', help="Version UID")
    p.set_defaults(func='metadata_restore')

    # METADATA LS
    p = subparsers_root.add_parser('metadata-ls', help='List the version metadata backup')
    p.add_argument('-S', '--storage', help='Source storage (if unspecified the default is used)')
    p.set_defaults(func='metadata_ls')

    # STATS
    p = subparsers_root.add_parser('stats', help='Show backup statistics')
    p.add_argument('version_uid', nargs='?', default=None, help='Limit output to the specified version')
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

    # INITDB
    p = subparsers_root.add_parser('init', help='Initialize the database (will not delete existing tables or data)')
    p.set_defaults(func='init')

    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    if not hasattr(args, 'func'):
        parser.print_usage()
        exit(os.EX_USAGE)

    if args.func == 'version_info':
        print(__version__)
        exit(os.EX_OK)

    if args.verbose:
        console_level = logging.DEBUG
    else:
        console_level = logging.INFO

    if args.config_file is not None and args.config_file != '':
        try:
            cfg = open(args.config_file, 'r', encoding='utf-8').read()
        except FileNotFoundError:
            logger.error('File {} not found.'.format(args.config_file))
            exit(os.EX_USAGE)
        config = Config(ad_hoc_config=cfg)
    else:
        config = Config()

    # logging ERROR only when machine output is selected
    if args.machine_output:
        init_logging(config.get('logFile', types=(str, type(None))), logging.ERROR, no_color=args.no_color)
    else:
        init_logging(config.get('logFile', types=(str, type(None))), console_level, no_color=args.no_color)

    commands = Commands(args.machine_output, config)
    func = getattr(commands, args.func)

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args['config_file']
    del func_args['func']
    del func_args['verbose']
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
        logger.debug('backup.{0}(**{1!r})'.format(args.func, func_args))
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
