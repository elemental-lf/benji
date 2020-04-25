import inspect
import json
import re
import types
from io import StringIO
from typing import Optional, Dict, Any

from webargs import fields

import benji.exception
from benji import __version__
from benji.amqp import AMQPRPCServer, AMQPRPCClient
from benji.benji import Benji
from benji.config import Config
from benji.database import Version, VersionUid
from benji.utils import hints_from_rbd_diff, InputValidation, random_string
from benji.versions import VERSIONS

DEFAULT_RPC_QUEUE = 'benji-rpc'


def register_as_task(func):
    parameters = inspect.Signature.from_callable(func, follow_wrapped=False).parameters
    func.rpc_task = {
        'webargs_argmap': {
            name: value.annotation for name, value in parameters.items() if isinstance(value.annotation, fields.Field)
        }
    }

    return func


class APIServer:

    def __init__(self, *, config: Config, queue: str, threads: int, inactivity_timeout: int) -> None:
        self._rpc_server = AMQPRPCServer(queue=queue, threads=threads, inactivity_timeout=inactivity_timeout)
        self._config = config
        self._install_tasks()

    def _install_tasks(self) -> None:
        for kw in dir(self):
            attr = getattr(self, kw)
            if hasattr(attr, 'rpc_task'):
                self._rpc_server.register_as_task(attr.rpc_task['webargs_argmap'])(attr)

    def serve(self) -> None:
        self._rpc_server.serve()

    @register_as_task
    def core_v1_backup(
        self, version_uid: fields.Str(missing=None), volume: fields.Str(required=True),
        snapshot: fields.Str(required=True), source: fields.Str(required=True), rbd_hints: fields.Str(missing=None),
        base_version_uid: fields.Str(missing=None), block_size: fields.Int(missing=None),
        storage_name: fields.Str(missing=None)
    ) -> StringIO:
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
    def core_v1_restore(
        self, version_uid: fields.Str(required=True), destination: fields.Str(required=True),
        sparse: fields.Bool(missing=False), force: fields.Bool(missing=False),
        database_backend_less: fields.Bool(missing=False)
    ) -> StringIO:
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
    def core_v1_get(self, version_uid: fields.Str(required=True)) -> StringIO:
        version_uid_obj = VersionUid(version_uid)
        result = StringIO()
        with Benji(self._config) as benji_obj:
            benji_obj.export_any({'versions': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)]},
                                 result,
                                 ignore_relationships=[((Version,), ('blocks',))])

        return result

    @register_as_task
    def core_v1_protect(
            self, version_uid: fields.Str(required=True), protected: fields.Bool(required=True)) -> StringIO:
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
    def core_v1_label(
            self, version_uid: fields.Str(required=True), labels: fields.List(fields.Str(), required=True)) -> StringIO:
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
    def core_v1_rm(
        self, version_uid: fields.Str(required=True), force: fields.Bool(missing=False),
        keep_metadata_backup: fields.Bool(missing=False), override_lock: fields.Bool(missing=False)
    ) -> StringIO:
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
    def core_v1_scrub(
            self, version_uid: fields.Str(required=True), block_percentage: fields.Int(missing=100)) -> StringIO:
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
    def core_v1_deep_scrub(
        self, version_uid: fields.Str(required=True), source: fields.Str(missing=None),
        block_percentage: fields.Int(missing=100)
    ) -> StringIO:
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
    def core_v1_batch_scrub(
        self, filter_expression: fields.Str(missing=None), version_percentage: fields.Int(missing=100),
        block_percentage: fields.Int(missing=100), group_label: fields.Str(missing=None)
    ) -> StringIO:
        return self._batch_scrub('batch_scrub', filter_expression, version_percentage, block_percentage, group_label)

    @register_as_task
    def core_v1_batch_deep_scrub(
        self, filter_expression: fields.Str(missing=None), version_percentage: fields.Int(missing=100),
        block_percentage: fields.Int(missing=100), group_label: fields.Str(missing=None)
    ) -> StringIO:
        return self._batch_scrub('batch_deep_scrub', filter_expression, version_percentage, block_percentage,
                                 group_label)

    @register_as_task
    def core_v1_ls(
            self, filter_expression: fields.Str(missing=None), include_blocks: fields.Bool(missing=False)) -> StringIO:
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
    def core_v1_cleanup(self, override_lock: fields.Bool(missing=False)) -> None:
        with Benji(self._config) as benji_obj:
            benji_obj.cleanup(override_lock=override_lock)

    @register_as_task
    def core_v1_metadata_backup(
            self, filter_expression: fields.Str(missing=None), force: fields.Bool(missing=False)) -> None:
        with Benji(self._config) as benji_obj:
            version_uid_objs = [version.uid for version in benji_obj.find_versions_with_filter(filter_expression)]
            benji_obj.metadata_backup(version_uid_objs, overwrite=force)

    @register_as_task
    def core_v1_metadata_import(self, data: fields.Str(required=True)) -> None:
        with Benji(self._config) as benji_obj:
            benji_obj.metadata_import(data)

    @register_as_task
    def core_v1_metadata_restore(
        self, version_uids: fields.DelimitedList(fields.Str, required=True), storage_name: fields.Str(missing=None)
    ) -> None:
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
    def core_v1_enforce(
        self, rules_spec: fields.Str(required=True), filter_expression: fields.Str(missing=None),
        dry_run: fields.Bool(missing=False), keep_metadata_backup: fields.Bool(missing=False),
        group_label: fields.Str(missing=None)
    ) -> StringIO:
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
    def core_v1_storage_stats(self, storage_name: str) -> Dict[str, int]:
        with Benji(self._config) as benji_obj:
            objects_count, objects_size = benji_obj.storage_stats(storage_name)

            result = {
                'objects_count': objects_count,
                'objects_size': objects_size,
            }

            return result


class APIClient:

    def __init__(self, queue: str = DEFAULT_RPC_QUEUE) -> None:
        self._rpc_client = AMQPRPCClient(queue=queue)
        self._create_tasks()

    def _create_task(self, name: str, argmap: Dict[str, fields.Field]) -> None:
        method_name = re.sub(r'[^\d\w_]', '_', name)
        parameters = arguments = ''
        kwdefaults = {}
        if argmap:
            parameters = ', *, ' + ', '.join(argmap.keys())
            arguments = ', ' + ', '.join([f'{name}={name}' for name in argmap.keys()])
            for arg_name, arg_type in argmap.items():
                if isinstance(arg_type, fields.Field) and arg_type.required == False:
                    kwdefaults[arg_name] = arg_type.missing

        # This is ugly as hell...
        func_source = f'def {method_name}(self{parameters}):\n  return json.loads(self._rpc_client.call(\'{name}\'{arguments}).decode("utf-8"))'
        module_code = compile(func_source, '<string>', 'exec')
        func_code = [c for c in module_code.co_consts if isinstance(c, types.CodeType)][0]
        func = types.FunctionType(func_code, globals(), method_name)
        func.__kwdefaults__ = kwdefaults
        bound_func = types.MethodType(func, self)
        # End of ugly as hell

        setattr(self, method_name, bound_func)

    def _create_tasks(self) -> None:
        for attr in [t[1] for t in inspect.getmembers(APIServer)]:
            if hasattr(attr, 'rpc_task'):
                self._create_task(attr.__name__, attr.rpc_task['webargs_argmap'])

    def close(self) -> None:
        self._rpc_client.close()
