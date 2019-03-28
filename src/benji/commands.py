import fileinput
import json
import os
import sys
from typing import List, NamedTuple, Type, Optional, Tuple

from prettytable import PrettyTable

import benji.exception
from benji import __version__
from benji.benji import Benji, BenjiStore
from benji.database import Version, VersionUid
from benji.factory import StorageFactory
from benji.logging import logger
from benji.nbdserver import NbdServer
from benji.utils import hints_from_rbd_diff, PrettyPrint, InputValidation
from benji.versions import VERSIONS


class _ExceptionMapping(NamedTuple):
    exception: Type[BaseException]
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

    @staticmethod
    def _ls_versions_table_output(versions: List[Version], include_labels: bool, include_stats: bool) -> None:
        tbl = PrettyTable()

        field_names = ['date', 'uid', 'name', 'snapshot_name', 'size', 'block_size', 'status', 'protected', 'storage']
        if include_stats:
            field_names.extend(['read', 'written', 'dedup', 'sparse', 'duration'])
        if include_labels:
            field_names.append('labels')
        tbl.field_names = field_names

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

            if include_stats:
                row.extend([
                    PrettyPrint.bytes(version.bytes_read) if version.bytes_read is not None else '',
                    PrettyPrint.bytes(version.bytes_written) if version.bytes_written is not None else '',
                    PrettyPrint.bytes(version.bytes_dedup) if version.bytes_dedup is not None else '',
                    PrettyPrint.bytes(version.bytes_sparse) if version.bytes_sparse is not None else '',
                    PrettyPrint.duration(version.duration) if version.duration is not None else '',
                ])

            if include_labels:
                row.append('\n'.join(sorted(['{}={}'.format(label.name, label.value) for label in version.labels])))
            tbl.add_row(row)
        print(tbl)

    def ls(self, filter_expression: Optional[str], include_labels: bool, include_stats: bool) -> None:
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
                self._ls_versions_table_output(versions, include_labels, include_stats)
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
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            objects_count, objects_size = benji_obj.storage_stats(storage_name)

            if self.machine_output:
                benji_obj.export_any({
                    'objects_count': objects_count,
                    'objects_size': objects_size,
                }, sys.stdout)
            else:
                self._ls_storage_stats_table_output(objects_count, objects_size)
        finally:
            if benji_obj:
                benji_obj.close()
