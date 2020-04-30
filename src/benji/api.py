import json
from io import StringIO
from typing import Optional, Dict, Any, Sequence

import benji.exception
from benji import __version__
from benji.benji import Benji
from benji.celery import RPCServer
from benji.config import Config
from benji.database import Version, VersionUid
from benji.utils import hints_from_rbd_diff, InputValidation, random_string
from benji.versions import VERSIONS

DEFAULT_RPC_QUEUE = 'benji-rpc'


def register_as_task(func):
    func.is_task = True
    return func


class APIServer:

    def __init__(self, *, config: Config, queue: str, threads: int) -> None:
        self._rpc_server = RPCServer(queue=queue, threads=threads)
        self._config = config
        self._install_tasks()

    def _install_tasks(self) -> None:
        for kw in dir(self):
            attr = getattr(self, kw)
            if getattr(attr, 'is_task', False):
                self._rpc_server.register_as_task()(attr)

    def serve(self) -> None:
        self._rpc_server.serve()

    @register_as_task
    def core_v1_backup(self,
                       *,
                       version_uid: str = None,
                       volume: str,
                       snapshot: str,
                       source: str,
                       rbd_hints: str = None,
                       base_version_uid: str = None,
                       block_size: int = None,
                       storage_name: str = None) -> StringIO:
        if version_uid is None:
            version_uid = '{}-{}'.format(volume[:248], random_string(6))
        version_uid_obj = VersionUid(version_uid)
        base_version_uid_obj = VersionUid(base_version_uid) if base_version_uid else None

        result = StringIO()
        with Benji(self._config) as benji_obj:
            hints = None
            if rbd_hints:
                with open(rbd_hints, 'r') as f:
                    hints = hints_from_rbd_diff(f.read())
            backup_version = benji_obj.backup(version_uid=version_uid_obj,
                                              volume=volume,
                                              snapshot=snapshot,
                                              source=source,
                                              hints=hints,
                                              base_version_uid=base_version_uid_obj,
                                              storage_name=storage_name,
                                              block_size=block_size)

            benji_obj.export_any({'versions': [backup_version]},
                                 result,
                                 ignore_relationships=[((Version,), ('blocks',))])

        return result

    @register_as_task
    def core_v1_restore(self,
                        *,
                        version_uid: str,
                        destination: str,
                        sparse: bool = False,
                        force: bool = False,
                        database_backend_less: bool = False) -> StringIO:
        version_uid_obj = VersionUid(version_uid)
        result = StringIO()
        with Benji(self._config, in_memory_database=database_backend_less) as benji_obj:
            if database_backend_less:
                benji_obj.metadata_restore([version_uid_obj])
            benji_obj.restore(version_uid_obj, destination, sparse, force)

            benji_obj.export_any({'versions': [version_uid_obj]},
                                 result,
                                 ignore_relationships=[((Version,), ('blocks',))])

        return result

    @register_as_task
    def core_v1_get(self, *, version_uid: str) -> StringIO:
        version_uid_obj = VersionUid(version_uid)
        result = StringIO()
        with Benji(self._config) as benji_obj:
            benji_obj.export_any({'versions': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)]},
                                 result,
                                 ignore_relationships=[((Version,), ('blocks',))])

        return result

    @register_as_task
    def core_v1_protect(self, *, version_uid: str, protected: bool = True) -> StringIO:
        version_uid_obj = VersionUid(version_uid)
        result = StringIO()
        with Benji(self._config) as benji_obj:
            if protected is not None:
                benji_obj.protect(version_uid_obj, protected=protected)

            benji_obj.export_any({'versions': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)]},
                                 result,
                                 ignore_relationships=[((Version,), ('blocks',))])

        return result

    @register_as_task
    def core_v1_label(self, *, version_uid: str, labels: Sequence[str]) -> StringIO:
        version_uid_obj = VersionUid(version_uid)
        label_add, label_remove = InputValidation.parse_and_validate_labels(labels)
        result = StringIO()
        with Benji(self._config) as benji_obj:
            for name, value in label_add:
                benji_obj.add_label(version_uid_obj, name, value)
            for name in label_remove:
                benji_obj.rm_label(version_uid_obj, name)

            benji_obj.export_any({'versions': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)]},
                                 result,
                                 ignore_relationships=[((Version,), ('blocks',))])

        return result

    @register_as_task
    def core_v1_rm(self,
                   *,
                   version_uid: str,
                   force: bool = False,
                   keep_metadata_backup: bool = False,
                   override_lock: bool = False) -> StringIO:
        version_uid_obj = VersionUid(version_uid)
        result = StringIO()
        with Benji(self._config) as benji_obj:
            # Do this before deleting the version
            benji_obj.export_any({'versions': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)]},
                                 result,
                                 ignore_relationships=[((Version,), ('blocks',))])

            benji_obj.rm(version_uid_obj,
                         force=force,
                         keep_metadata_backup=keep_metadata_backup,
                         override_lock=override_lock)

        return result

    @register_as_task
    def core_v1_scrub(self, *, version_uid: str, block_percentage: int = 100) -> StringIO:
        version_uid_obj = VersionUid(version_uid)
        result = StringIO()
        with Benji(self._config) as benji_obj:
            try:
                benji_obj.scrub(version_uid_obj, block_percentage=block_percentage)
            except benji.exception.ScrubbingError:
                benji_obj.export_any(
                    {
                        'versions': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)],
                        'errors': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)]
                    },
                    result,
                    ignore_relationships=[((Version,), ('blocks',))])
            else:
                benji_obj.export_any(
                    {
                        'versions': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)],
                        'errors': []
                    },
                    result,
                    ignore_relationships=[((Version,), ('blocks',))])

        return result

    @register_as_task
    def core_v1_deep_scrub(self, *, version_uid: str, source: str = None, block_percentage: int = 100) -> StringIO:
        version_uid_obj = VersionUid(version_uid)
        result = StringIO()
        with Benji(self._config) as benji_obj:
            try:
                benji_obj = Benji(self._config)
                benji_obj.deep_scrub(version_uid_obj, source=source, block_percentage=block_percentage)
            except benji.exception.ScrubbingError:
                assert benji_obj is not None
                benji_obj.export_any(
                    {
                        'versions': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)],
                        'errors': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)]
                    },
                    result,
                    ignore_relationships=[((Version,), ('blocks',))])
            else:
                benji_obj.export_any(
                    {
                        'versions': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)],
                        'errors': []
                    },
                    result,
                    ignore_relationships=[((Version,), ('blocks',))])

        return result

    def _batch_scrub(self, method: str, filter_expression: Optional[str], version_percentage: int,
                     block_percentage: int, group_label: Optional[str]) -> StringIO:
        with Benji(self._config) as benji_obj:
            versions, errors = getattr(benji_obj, method)(filter_expression, version_percentage, block_percentage,
                                                          group_label)

            result = StringIO()
            benji_obj.export_any({
                'versions': versions,
                'errors': errors,
            },
                                 result,
                                 ignore_relationships=[((Version,), ('blocks',))])

            return result

    @register_as_task
    def core_v1_batch_scrub(self,
                            *,
                            filter_expression: str = None,
                            version_percentage: int = 100,
                            block_percentage: int = 100,
                            group_label: str = None) -> StringIO:
        return self._batch_scrub('batch_scrub', filter_expression, version_percentage, block_percentage, group_label)

    @register_as_task
    def core_v1_batch_deep_scrub(self,
                                 *,
                                 filter_expression: str = None,
                                 version_percentage: int = 100,
                                 block_percentage: int = 100,
                                 group_label: str = None) -> StringIO:
        return self._batch_scrub('batch_deep_scrub', filter_expression, version_percentage, block_percentage,
                                 group_label)

    @register_as_task
    def core_v1_ls(self, *, filter_expression: str = None, include_blocks: bool = False) -> StringIO:
        with Benji(self._config) as benji_obj:
            versions = benji_obj.find_versions_with_filter(filter_expression)

            result = StringIO()
            benji_obj.export_any(
                {'versions': versions},
                result,
                ignore_relationships=[((Version,), ('blocks',) if not include_blocks else ())],
            )

            return result

    @register_as_task
    def core_v1_cleanup(self, *, override_lock: bool = False) -> None:
        with Benji(self._config) as benji_obj:
            benji_obj.cleanup(override_lock=override_lock)

    @register_as_task
    def core_v1_metadata_backup(self, *, filter_expression: str = None, force: bool = False) -> None:
        with Benji(self._config) as benji_obj:
            version_uid_objs = [version.uid for version in benji_obj.find_versions_with_filter(filter_expression)]
            benji_obj.metadata_backup(version_uid_objs, overwrite=force)

    @register_as_task
    def core_v1_metadata_import(self, data: str) -> None:
        with Benji(self._config) as benji_obj:
            benji_obj.metadata_import(data)

    @register_as_task
    def core_v1_metadata_restore(self, *, version_uids: Sequence[str], storage_name: str = None) -> None:
        version_uid_objs = [VersionUid(version_uid) for version_uid in version_uids]
        with Benji(self._config) as benji_obj:
            benji_obj.metadata_restore(version_uid_objs, storage_name)

    @register_as_task
    def core_v1_storages(self) -> StringIO:
        with Benji(self._config) as benji_obj:
            return json.dumps(benji_obj.list_storages())

    @register_as_task
    def core_v1_database_init(self) -> None:
        Benji(self._config, init_database=True).close()

    @register_as_task
    def core_v1_database_migrate(self) -> None:
        Benji(self._config, migrate_database=True).close()

    @register_as_task
    def core_v1_enforce(self,
                        *,
                        rules_spec: str,
                        filter_expression: str = None,
                        dry_run: bool = False,
                        keep_metadata_backup: bool = False,
                        group_label: str = None) -> StringIO:
        with Benji(self._config) as benji_obj:
            dismissed_versions = benji_obj.enforce_retention_policy(filter_expression=filter_expression,
                                                                    rules_spec=rules_spec,
                                                                    dry_run=dry_run,
                                                                    keep_metadata_backup=keep_metadata_backup,
                                                                    group_label=group_label)

            result = StringIO()
            benji_obj.export_any(
                {'versions': dismissed_versions},
                result,
                ignore_relationships=[((Version,), ('blocks',))],
            )

            return result

    @register_as_task
    def core_v1_version_info(self) -> Dict[str, Any]:
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

        return result

    @register_as_task
    def core_v1_storage_stats(self, *, storage_name: str) -> Dict[str, int]:
        with Benji(self._config) as benji_obj:
            objects_count, objects_size = benji_obj.storage_stats(storage_name)

            result = {
                'objects_count': objects_count,
                'objects_size': objects_size,
            }

            return result
