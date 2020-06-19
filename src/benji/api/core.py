from io import StringIO
from typing import Sequence, Optional, Dict, Any, Tuple, List, Union

from benji import __version__
from benji.api import TasksBase
from benji.api.base import register_as_task
from benji.benji import Benji
from benji.database import VersionUid, Version
from benji.versions import VERSIONS

API_GROUP = 'core'
API_VERSION = 'v1'


class Tasks(TasksBase):

    @staticmethod
    def _export_versions(benji_obj: Benji, versions: Union[Version, Sequence[Version]]) -> StringIO:
        result = StringIO()
        benji_obj.export_any(versions,
                             result,
                             ignore_relationships=(((Version,), ('blocks',)),),
                             embedded_metadata_version=True)
        return result

    @register_as_task(API_GROUP, API_VERSION)
    def backup(self,
               *,
               version_uid: str,
               volume: str,
               snapshot: str,
               source: str,
               hints: Sequence[Tuple[int, int, bool]] = None,
               base_version_uid: str = None,
               block_size: int = None,
               storage_name: str = None,
               labels: Dict[str, str] = None) -> StringIO:
        version_uid_obj = VersionUid(version_uid)
        base_version_uid_obj = VersionUid(base_version_uid) if base_version_uid else None

        with Benji(self._config) as benji_obj:
            version = benji_obj.backup(version_uid=version_uid_obj,
                                       volume=volume,
                                       snapshot=snapshot,
                                       source=source,
                                       hints=hints,
                                       base_version_uid=base_version_uid_obj,
                                       storage_name=storage_name,
                                       block_size=block_size)
            if labels:
                for name, value in labels.items():
                    version.add_label(name, value)
            return self._export_versions(benji_obj, version)

    @register_as_task(API_GROUP, API_VERSION)
    def restore(self,
                *,
                version_uid: str,
                target: str,
                sparse: bool = False,
                force: bool = False,
                database_backend_less: bool = False) -> None:
        version_uid_obj = VersionUid(version_uid)
        with Benji(self._config, in_memory_database=database_backend_less) as benji_obj:
            if database_backend_less:
                benji_obj.metadata_restore([version_uid_obj])
            benji_obj.restore(version_uid=version_uid_obj, target=target, sparse=sparse, force=force)

    @register_as_task(API_GROUP, API_VERSION)
    def get_version_by_uid(self, *, version_uid: str) -> StringIO:
        with Benji(self._config) as benji_obj:
            return self._export_versions(benji_obj.get_version_by_uid(version_uid=VersionUid(version_uid)))

    @register_as_task(API_GROUP, API_VERSION)
    def protect(self, *, version_uid: str, protected: bool = True) -> None:
        with Benji(self._config) as benji_obj:
            benji_obj.protect(VersionUid(version_uid), protected=protected)

    @register_as_task(API_GROUP, API_VERSION)
    def add_label(self, *, version_uid: str, name: str, value: str) -> None:
        with Benji(self._config) as benji_obj:
            benji_obj.add_label(VersionUid(version_uid), name, value)

    @register_as_task(API_GROUP, API_VERSION)
    def rm_label(self, *, version_uid: str, name: str) -> None:
        with Benji(self._config) as benji_obj:
            benji_obj.rm_label(VersionUid(version_uid), name)

    @register_as_task(API_GROUP, API_VERSION)
    def rm(self,
           *,
           version_uid: str,
           force: bool = False,
           keep_metadata_backup: bool = False,
           override_lock: bool = False) -> None:
        with Benji(self._config) as benji_obj:
            benji_obj.rm(VersionUid(version_uid),
                         force=force,
                         keep_metadata_backup=keep_metadata_backup,
                         override_lock=override_lock)

    @register_as_task(API_GROUP, API_VERSION)
    def scrub(self, *, version_uid: str, block_percentage: int = 100) -> None:
        with Benji(self._config) as benji_obj:
            benji_obj.scrub(VersionUid(version_uid), block_percentage=block_percentage)

    @register_as_task(API_GROUP, API_VERSION)
    def deep_scrub(self, *, version_uid: str, source: str = None, block_percentage: int = 100) -> None:
        with Benji(self._config) as benji_obj:
            benji_obj.deep_scrub(VersionUid(version_uid), source=source, block_percentage=block_percentage)

    def _batch_scrub(self, method: str, filter_expression: Optional[str], version_percentage: int,
                     block_percentage: int, group_label: Optional[str]) -> StringIO:
        with Benji(self._config) as benji_obj:
            versions, errors = getattr(benji_obj, method)(filter_expression, version_percentage, block_percentage,
                                                          group_label)

            return self._export_versions(benji_obj, versions), self._export_versions(benji_obj, errors)

    @register_as_task(API_GROUP, API_VERSION)
    def batch_scrub(self,
                    *,
                    filter_expression: str = None,
                    version_percentage: int = 100,
                    block_percentage: int = 100,
                    group_label: str = None) -> StringIO:
        return self._batch_scrub('batch_scrub', filter_expression, version_percentage, block_percentage, group_label)

    @register_as_task(API_GROUP, API_VERSION)
    def batch_deep_scrub(self,
                         *,
                         filter_expression: str = None,
                         version_percentage: int = 100,
                         block_percentage: int = 100,
                         group_label: str = None) -> StringIO:
        return self._batch_scrub('batch_deep_scrub', filter_expression, version_percentage, block_percentage,
                                 group_label)

    @register_as_task(API_GROUP, API_VERSION)
    def find_versions_with_filter(self, *, filter_expression: str = None) -> StringIO:
        with Benji(self._config) as benji_obj:
            return self._export_versions(benji_obj, benji_obj.find_versions_with_filter(filter_expression))

    @register_as_task(API_GROUP, API_VERSION)
    def cleanup(self, *, override_lock: bool = False) -> None:
        with Benji(self._config) as benji_obj:
            benji_obj.cleanup(override_lock=override_lock)

    @register_as_task(API_GROUP, API_VERSION)
    def metadata_backup(self, *, filter_expression: str = None, force: bool = False) -> None:
        with Benji(self._config) as benji_obj:
            version_uid_objs = [version.uid for version in benji_obj.find_versions_with_filter(filter_expression)]
            benji_obj.metadata_backup(version_uid_objs, overwrite=force)

    @register_as_task(API_GROUP, API_VERSION)
    def metadata_import(self, data: str) -> None:
        with Benji(self._config) as benji_obj:
            benji_obj.metadata_import(data)

    @register_as_task(API_GROUP, API_VERSION)
    def metadata_restore(self, *, version_uids: Sequence[str], storage_name: str = None) -> None:
        version_uid_objs = [VersionUid(version_uid) for version_uid in version_uids]
        with Benji(self._config) as benji_obj:
            benji_obj.metadata_restore(version_uid_objs, storage_name)

    @register_as_task(API_GROUP, API_VERSION)
    def list_storages(self) -> List[str]:
        with Benji(self._config) as benji_obj:
            return benji_obj.list_storages()

    @register_as_task(API_GROUP, API_VERSION)
    def database_init(self) -> None:
        Benji(self._config, init_database=True).close()

    @register_as_task(API_GROUP, API_VERSION)
    def database_migrate(self) -> None:
        Benji(self._config, migrate_database=True).close()

    @register_as_task(API_GROUP, API_VERSION)
    def enforce(self,
                *,
                rules_spec: str,
                filter_expression: str = None,
                dry_run: bool = False,
                keep_metadata_backup: bool = False,
                group_label: str = None) -> StringIO:
        with Benji(self._config) as benji_obj:
            return self._export_versions(
                benji_obj,
                benji_obj.enforce_retention_policy(filter_expression=filter_expression,
                                                   rules_spec=rules_spec,
                                                   dry_run=dry_run,
                                                   keep_metadata_backup=keep_metadata_backup,
                                                   group_label=group_label))

    @register_as_task(API_GROUP, API_VERSION)
    def storage_stats(self, *, storage_name: str) -> Tuple[int, int]:
        with Benji(self._config) as benji_obj:
            return benji_obj.storage_stats(storage_name)

    @register_as_task(API_GROUP, API_VERSION)
    def version_info(self) -> Dict[str, Any]:
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
