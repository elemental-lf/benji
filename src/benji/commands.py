import json
import logging
import os
import sys
from typing import List, NamedTuple, Type, Optional, Dict

from prettytable import PrettyTable

import benji.exception
from benji import __version__
from benji.benji import Benji, BenjiStore
from benji.database import Version, VersionUid
from benji.logging import logger
from benji.nbdserver import NbdServer
from benji.utils import hints_from_rbd_diff, PrettyPrint, InputValidation, random_string
from benji.versions import VERSIONS


class _ExceptionMapping(NamedTuple):
    exception: Type[BaseException]
    exit_code: int


class Commands:
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, machine_output, config):
        self.machine_output = machine_output
        self.config = config

    def backup(self, version_uid: str, volume: str, snapshot: str, source: str, rbd_hints: str, base_version_uid: str,
               block_size: int, labels: List[str], storage: str) -> None:
        if version_uid is None:
            version_uid = '{}-{}'.format(volume[:248], random_string(6))
        version_uid_obj = VersionUid(version_uid)
        base_version_uid_obj = VersionUid(base_version_uid) if base_version_uid else None

        if labels:
            label_add, label_remove = InputValidation.parse_and_validate_labels(labels)
        with Benji(self.config) as benji_obj:
            hints = None
            if rbd_hints:
                logger.debug(f'Loading RBD hints from file {rbd_hints}.')
                with open(rbd_hints, 'r') as f:
                    hints = hints_from_rbd_diff(f.read())
            backup_version = benji_obj.backup(version_uid=version_uid_obj,
                                              volume=volume,
                                              snapshot=snapshot,
                                              source=source,
                                              hints=hints,
                                              base_version_uid=base_version_uid_obj,
                                              storage_name=storage,
                                              block_size=block_size)

            if labels:
                for key, value in label_add:
                    benji_obj.add_label(backup_version.uid, key, value)
                for key in label_remove:
                    benji_obj.rm_label(backup_version.uid, key)
                if label_add:
                    logger.info('Added label(s) to version {}: {}.'.format(
                        backup_version.uid, ', '.join('{}={}'.format(name, value) for name, value in label_add)))
                if label_remove:
                    logger.info('Removed label(s) from version {}: {}.'.format(backup_version.uid,
                                                                               ', '.join(label_remove)))

            if self.machine_output:
                benji_obj.export_any({'versions': [backup_version]},
                                     sys.stdout,
                                     ignore_relationships=(((Version,), ('blocks',)),))

    def restore(self, version_uid: str, destination: str, sparse: bool, force: bool, database_less: bool,
                storage: str) -> None:
        if not database_less and storage is not None:
            raise benji.exception.UsageError('Specifying a storage location is only supported for database-less restores.')

        version_uid_obj = VersionUid(version_uid)
        with Benji(self.config, in_memory_database=database_less) as benji_obj:
            if database_less:
                benji_obj.metadata_restore([version_uid_obj], storage)
            benji_obj.restore(version_uid_obj, destination, sparse, force)

    def protect(self, version_uids: List[str]) -> None:
        version_uid_objs = [VersionUid(version_uid) for version_uid in version_uids]
        with Benji(self.config) as benji_obj:
            for version_uid in version_uid_objs:
                benji_obj.protect(version_uid, protected=True)

    def unprotect(self, version_uids: List[str]) -> None:
        version_uid_objs = [VersionUid(version_uid) for version_uid in version_uids]
        with Benji(self.config) as benji_obj:
            for version_uid in version_uid_objs:
                benji_obj.protect(version_uid, protected=False)

    def rm(self, version_uids: List[str], force: bool, keep_metadata_backup: bool, override_lock: bool) -> None:
        version_uid_objs = [VersionUid(version_uid) for version_uid in version_uids]
        disallow_rm_when_younger_than_days = self.config.get('disallowRemoveWhenYounger', types=int)
        with Benji(self.config) as benji_obj:
            for version_uid in version_uid_objs:
                benji_obj.rm(version_uid,
                             force=force,
                             disallow_rm_when_younger_than_days=disallow_rm_when_younger_than_days,
                             keep_metadata_backup=keep_metadata_backup,
                             override_lock=override_lock)

    def scrub(self, version_uid: str, block_percentage: int) -> None:
        version_uid_obj = VersionUid(version_uid)
        with Benji(self.config) as benji_obj:
            try:
                benji_obj.scrub(version_uid_obj, block_percentage=block_percentage)
            except benji.exception.ScrubbingError:
                assert benji_obj is not None
                if self.machine_output:
                    benji_obj.export_any(
                        {
                            'versions': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)],
                            'errors': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)]
                        },
                        sys.stdout,
                        ignore_relationships=(((Version,), ('blocks',)),))
                raise
            else:
                if self.machine_output:
                    benji_obj.export_any(
                        {
                            'versions': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)],
                            'errors': []
                        },
                        sys.stdout,
                        ignore_relationships=(((Version,), ('blocks',)),))

    def deep_scrub(self, version_uid: str, source: str, block_percentage: int) -> None:
        version_uid_obj = VersionUid(version_uid)
        with Benji(self.config) as benji_obj:
            try:
                benji_obj.deep_scrub(version_uid_obj, source=source, block_percentage=block_percentage)
            except benji.exception.ScrubbingError:
                assert benji_obj is not None
                if self.machine_output:
                    benji_obj.export_any(
                        {
                            'versions': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)],
                            'errors': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)]
                        },
                        sys.stdout,
                        ignore_relationships=(((Version,), ('blocks',)),))
                raise
            else:
                if self.machine_output:
                    benji_obj.export_any(
                        {
                            'versions': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)],
                            'errors': []
                        },
                        sys.stdout,
                        ignore_relationships=(((Version,), ('blocks',)),))

    def _batch_scrub(self, method: str, filter_expression: Optional[str], version_percentage: int,
                     block_percentage: int, group_label: Optional[str]) -> None:
        with Benji(self.config) as benji_obj:
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
                    version.uid for version in errors)))
            else:
                if self.machine_output:
                    benji_obj.export_any({
                        'versions': versions,
                        'errors': []
                    },
                                         sys.stdout,
                                         ignore_relationships=(((Version,), ('blocks',)),))

    def batch_scrub(self, filter_expression: Optional[str], version_percentage: int, block_percentage: int,
                    group_label: Optional[str]) -> None:
        self._batch_scrub('batch_scrub', filter_expression, version_percentage, block_percentage, group_label)

    def batch_deep_scrub(self, filter_expression: Optional[str], version_percentage: int, block_percentage: int,
                         group_label: Optional[str]) -> None:
        self._batch_scrub('batch_deep_scrub', filter_expression, version_percentage, block_percentage, group_label)

    @staticmethod
    def _ls_versions_table_output(versions: List[Version], include_labels: bool, include_stats: bool) -> None:
        tbl = PrettyTable()

        field_names = ['date', 'uid', 'volume', 'snapshot', 'size', 'block_size', 'status', 'protected', 'storage']
        if include_stats:
            field_names.extend(['read', 'written', 'deduplicated', 'sparse', 'duration'])
        if include_labels:
            field_names.append('labels')
        tbl.field_names = field_names

        tbl.align['uid'] = 'l'
        tbl.align['volume'] = 'l'
        tbl.align['snapshot'] = 'l'
        tbl.align['storage'] = 'l'
        tbl.align['size'] = 'r'
        tbl.align['block_size'] = 'r'

        tbl.align['read'] = 'r'
        tbl.align['written'] = 'r'
        tbl.align['deduplicated'] = 'r'
        tbl.align['sparse'] = 'r'
        tbl.align['duration'] = 'r'

        tbl.align['labels'] = 'l'

        for version in versions:
            row = [
                PrettyPrint.local_time(version.date),
                version.uid,
                version.volume,
                version.snapshot,
                PrettyPrint.bytes(version.size),
                PrettyPrint.bytes(version.block_size),
                version.status,
                version.protected,
                version.storage.name,
            ]

            if include_stats:
                row.extend([
                    PrettyPrint.bytes(version.bytes_read) if version.bytes_read is not None else '',
                    PrettyPrint.bytes(version.bytes_written) if version.bytes_written is not None else '',
                    PrettyPrint.bytes(version.bytes_deduplicated) if version.bytes_deduplicated is not None else '',
                    PrettyPrint.bytes(version.bytes_sparse) if version.bytes_sparse is not None else '',
                    PrettyPrint.duration(version.duration) if version.duration is not None else '',
                ])

            if include_labels:
                row.append('\n'.join(
                    sorted(['{}={}'.format(label.name, label.value) for label in version.labels.values()])))
            tbl.add_row(row)
        print(tbl)

    def ls(self, filter_expression: Optional[str], include_labels: bool, include_stats: bool) -> None:
        with Benji(self.config) as benji_obj:
            versions = benji_obj.find_versions_with_filter(filter_expression)

            if self.machine_output:
                benji_obj.export_any(
                    {'versions': versions},
                    sys.stdout,
                    ignore_relationships=[((Version,), ('blocks',))],
                )
            else:
                self._ls_versions_table_output(versions, include_labels, include_stats)

    def cleanup(self, override_lock: bool) -> None:
        with Benji(self.config) as benji_obj:
            benji_obj.cleanup(override_lock=override_lock)

    def metadata_export(self, filter_expression: Optional[str], output_file: Optional[str], force: bool) -> None:
        with Benji(self.config) as benji_obj:
            version_uid_objs = [version.uid for version in benji_obj.find_versions_with_filter(filter_expression)]
            if output_file is None:
                benji_obj.metadata_export(version_uid_objs, sys.stdout)
            else:
                if os.path.exists(output_file) and not force:
                    raise FileExistsError('The output file already exists.')

                with open(output_file, 'w') as f:
                    benji_obj.metadata_export(version_uid_objs, f)

    def metadata_backup(self, filter_expression: str, force: bool = False) -> None:
        with Benji(self.config) as benji_obj:
            version_uid_objs = [version.uid for version in benji_obj.find_versions_with_filter(filter_expression)]
            benji_obj.metadata_backup(version_uid_objs, overwrite=force)

    def metadata_import(self, input_file: str = None) -> None:
        with Benji(self.config) as benji_obj:
            if input_file is None:
                benji_obj.metadata_import(sys.stdin)
            else:
                with open(input_file, 'r') as f:
                    benji_obj.metadata_import(f)

    def metadata_restore(self, version_uids: List[str], storage: str = None) -> None:
        version_uid_objs = [VersionUid(version_uid) for version_uid in version_uids]
        with Benji(self.config) as benji_obj:
            benji_obj.metadata_restore(version_uid_objs, storage)

    @staticmethod
    def _metadata_ls_table_output(version_uids: List[VersionUid]):
        tbl = PrettyTable()
        tbl.field_names = ['uid']
        tbl.align['uid'] = 'l'
        for version_uid in version_uids:
            tbl.add_row([version_uid])
        print(tbl)

    def metadata_ls(self, storage: str = None) -> None:
        with Benji(self.config) as benji_obj:
            version_uids = benji_obj.metadata_ls(storage)
            if self.machine_output:
                json.dump(
                    [version_uid for version_uid in version_uids],
                    sys.stdout,
                    indent=2,
                )
            else:
                self._metadata_ls_table_output(version_uids)

    def label(self, version_uid: str, labels: List[str]) -> None:
        version_uid_obj = VersionUid(version_uid)
        label_add, label_remove = InputValidation.parse_and_validate_labels(labels)
        with Benji(self.config) as benji_obj:
            for name, value in label_add:
                benji_obj.add_label(version_uid_obj, name, value)
            for name in label_remove:
                benji_obj.rm_label(version_uid_obj, name)
            if label_add:
                logger.info('Added label(s) to version {}: {}.'.format(
                    version_uid_obj, ', '.join('{}={}'.format(name, value) for name, value in label_add)))
            if label_remove:
                logger.info('Removed label(s) from version {}: {}.'.format(version_uid_obj, ', '.join(label_remove)))

    def database_init(self) -> None:
        Benji(self.config, init_database=True).close()

    def database_migrate(self) -> None:
        Benji(self.config, migrate_database=True).close()

    def enforce_retention_policy(self, rules_spec: str, filter_expression: str, dry_run: bool,
                                 keep_metadata_backup: bool, group_label: Optional[str]) -> None:
        with Benji(self.config) as benji_obj:
            dismissed_versions = benji_obj.enforce_retention_policy(filter_expression=filter_expression,
                                                                    rules_spec=rules_spec,
                                                                    dry_run=dry_run,
                                                                    keep_metadata_backup=keep_metadata_backup,
                                                                    group_label=group_label)
            if self.machine_output:
                benji_obj.export_any({
                    'versions': dismissed_versions,
                },
                                     sys.stdout,
                                     ignore_relationships=(((Version,), ('blocks',)),))

    def nbd(self, bind_address: str, bind_port: str, read_only: bool) -> None:
        with Benji(self.config) as benji_obj:
            store = BenjiStore(benji_obj)
            addr = (bind_address, bind_port)
            server = NbdServer(addr, store, read_only)
            logger.info("Starting to serve NBD on %s:%s" % (addr[0], addr[1]))
            server.serve_forever()

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
            result = {
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
            print(json.dumps(result, indent=4))

    @staticmethod
    def _ls_storage_stats_table_output(objects_count: int, objects_size: int) -> None:
        tbl = PrettyTable()
        tbl.field_names = [
            'objects_count',
            'objects_size',
        ]
        tbl.align['objects_count'] = 'r'
        tbl.align['objects_size'] = 'r'
        row = [
            objects_count,
            PrettyPrint.bytes(objects_size),
        ]
        tbl.add_row(row)
        print(tbl)

    def storage_stats(self, storage_name: str = None) -> None:
        with Benji(self.config) as benji_obj:
            objects_count, objects_size = benji_obj.storage_stats(storage_name)

            if self.machine_output:
                result = {
                    'objects_count': objects_count,
                    'objects_size': objects_size,
                }
                print(json.dumps(result, indent=4))
            else:
                self._ls_storage_stats_table_output(objects_count, objects_size)

    @staticmethod
    def _storage_usage_table_output(usage: Dict[str, Dict[str, int]]) -> None:
        tbl = PrettyTable()
        tbl.field_names = [
            'storage',
            'virtual',
            'sparse',
            'shared',
            'exclusive',
            'deduplicated_exclusive',
        ]
        tbl.align['storage'] = 'l'
        tbl.align['virtual'] = 'l'
        tbl.align['sparse'] = 'l'
        tbl.align['shared'] = 'r'
        tbl.align['exclusive'] = 'r'
        tbl.align['deduplicated_exclusive'] = 'r'
        for storage_name, usage in usage.items():
            row = [
                storage_name,
                PrettyPrint.bytes(usage['virtual']),
                PrettyPrint.bytes(usage['sparse']),
                PrettyPrint.bytes(usage['shared']),
                PrettyPrint.bytes(usage['exclusive']),
                PrettyPrint.bytes(usage['deduplicated_exclusive']),
            ]
            tbl.add_row(row)
        print(tbl)

    def storage_usage(self, filter_expression: str):
        with Benji(self.config) as benji_obj:
            usage = benji_obj.storage_usage(filter_expression)
            if self.machine_output:
                print(json.dumps(usage, indent=4))
            else:
                self._storage_usage_table_output(usage)

    def rest_api(self, bind_address: str, bind_port: int, threads: int) -> None:
        from benji.restapi import RestAPI
        api = RestAPI(self.config)
        logger.info(f'Starting REST API via gunicorn on {bind_address}:{bind_port}.')
        debug = bool(logger.isEnabledFor(logging.DEBUG))
        api.run(bind_address=bind_address, bind_port=bind_port, threads=threads, debug=debug)
