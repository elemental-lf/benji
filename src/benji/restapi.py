import functools
import json
from io import StringIO
from typing import List, Optional, Dict, Any

from bottle import Bottle, response, HTTPError, request
from webargs import fields
from webargs.bottleparser import use_kwargs

import benji.exception
from benji import __version__
from benji.benji import Benji
from benji.config import Config
from benji.database import Version, VersionUid
from benji.utils import hints_from_rbd_diff, InputValidation, random_string
from benji.versions import VERSIONS


def error(status: int):

    def decorator(func):
        func.bottle_error = status

        @functools.wraps(func)
        def wrapped_func(*args, **kwargs):
            body = json.dumps(func(*args, **kwargs))
            response.content_type = 'application/json; charset=utf-8'
            return body

        return wrapped_func()

    return decorator


def route(path: str, **decorator_kwargs):

    def decorator(func):

        func.bottle_route = decorator_kwargs
        func.bottle_route['path'] = path

        annotations = getattr(func, "__annotations__", {})
        request_args = {
            name: value for name, value in annotations.items() if isinstance(value, fields.Field) and name != "return"
        }
        func.bottle_route['apply'] = use_kwargs(request_args)

        @functools.wraps(func)
        def wrapped_func(*args, **kwargs):
            body = func(*args, **kwargs)

            if response.status_code == 200 and body is None:
                response.status = 204

            if isinstance(body, StringIO):
                response.content_type = 'application/json; charset=utf-8'
                return body.getvalue()
            else:
                response.content_type = 'application/json; charset=utf-8'
                return json.dumps(body) if body is not None else ''

        return wrapped_func

    return decorator


class RestAPI:
    """Proxy between REST calls and actual backup code."""

    CORE_API_VERSION_V1 = 'v1'
    CORE_API_GROUP = 'core'

    def __init__(self, config: Config):
        self._app = Bottle()

        def default_error_handler(error: HTTPError):
            return self._default_error_handler(error)

        self._app.default_error_handler = default_error_handler
        self._config = config
        self._install_routes()

    def _install_routes(self):
        for kw in dir(self):
            attr = getattr(self, kw)
            if hasattr(attr, 'bottle_route'):
                self._app.route(**attr.bottle_route)(attr)
            if hasattr(attr, 'bottle_error'):
                self._app.error(attr.bottle_error)(attr)

    def run(self, bind_address: str, bind_port: int, threads: int, debug: bool = False):
        self._app.run(
            server='gunicorn',
            host=bind_address,
            port=bind_port,
            reuse_port=True,
            worker_class='gthread',
            threads=threads,
            worker_connections=threads * 10,
            debug=debug,
        )

    @staticmethod
    def _default_error_handler(error: HTTPError):

        result = {
            'url': repr(request.url),
            'status': error.status,
            'body': error.body,
        }

        if error.exception:
            result['exception'] = {'name': error.exception.__class__.__name__, 'message': str(error.exception)}

        if error.traceback:
            result['traceback'] = error.traceback

        response.content_type = 'application/json; charset=utf-8'
        return json.dumps(result)

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/versions', method='POST')
    def _api_v1_versions_create(
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

            response.status = 201
            response.set_header('Location', f'{request.path}/{backup_version.uid}')
        return result

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/versions/<version_uid>/restore', method='POST')
    def _api_v1_versions_restore_create(
        self, version_uid: str, destination: fields.Str(required=True), sparse: fields.Bool(missing=False),
        force: fields.Bool(missing=False), database_backend_less: fields.Bool(missing=False)
    ) -> StringIO:
        version_uid_obj = VersionUid(version_uid)
        result = StringIO()
        with Benji(self._config, in_memory_database=database_backend_less) as benji_obj:
            try:
                if database_backend_less:
                    benji_obj.metadata_restore([version_uid_obj])
                benji_obj.restore(version_uid_obj, destination, sparse, force)

                benji_obj.export_any({'versions': [version_uid_obj]},
                                     result,
                                     ignore_relationships=[((Version,), ('blocks',))])
            except KeyError:
                response.status = f'410 Version {version_uid} not found.'

        return result

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/versions/<version_uid>', method='GET')
    def _api_v1_versions_read(self, version_uid: str) -> StringIO:
        version_uid_obj = VersionUid(version_uid)
        result = StringIO()
        with Benji(self._config) as benji_obj:
            try:
                benji_obj.export_any({'versions': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)]},
                                     result,
                                     ignore_relationships=[((Version,), ('blocks',))])
            except KeyError:
                response.status = f'410 Version {version_uid} not found.'

        return result

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/versions/<version_uid>', method='PATCH')
    def _api_v1_versions_patch(
        self, version_uid: str, protected: fields.Bool(missing=None), labels: fields.DelimitedList(fields.Str(),
                                                                                                   missing=None)
    ) -> StringIO:
        version_uid_obj = VersionUid(version_uid)
        if labels is not None:
            label_add, label_remove = InputValidation.parse_and_validate_labels(labels)
        else:
            label_add, label_remove = [], []
        result = StringIO()
        with Benji(self._config) as benji_obj:
            try:
                if protected is not None:
                    benji_obj.protect(version_uid_obj, protected=protected)

                for name, value in label_add:
                    benji_obj.add_label(version_uid_obj, name, value)
                for name in label_remove:
                    benji_obj.rm_label(version_uid_obj, name)

                benji_obj.export_any({'versions': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)]},
                                     result,
                                     ignore_relationships=[((Version,), ('blocks',))])
            except KeyError:
                response.status = f'410 Version {version_uid} not found.'

        return result

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/versions/<version_uid>', method='DELETE')
    def _api_v1_versions_delete(
        self, version_uid: str, force: fields.Bool(missing=False), keep_metadata_backup: fields.Bool(missing=False),
        override_lock: fields.Bool(missing=False)
    ) -> StringIO:
        version_uid_obj = VersionUid(version_uid)
        result = StringIO()
        with Benji(self._config) as benji_obj:
            try:
                # Do this before deleting the version
                benji_obj.export_any({'versions': [benji_obj.get_version_by_uid(version_uid=version_uid_obj)]},
                                     result,
                                     ignore_relationships=[((Version,), ('blocks',))])

                benji_obj.rm(version_uid_obj,
                             force=force,
                             keep_metadata_backup=keep_metadata_backup,
                             override_lock=override_lock)
            except KeyError:
                response.status = f'410 Version {version_uid} not found.'

        return result

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/versions/<version_uid>/scrub', method='POST')
    def _api_v1_versions_scrub_create(self, version_uid: str, block_percentage: fields.Int(missing=100)) -> StringIO:
        version_uid_obj = VersionUid(version_uid)
        result = StringIO()
        with Benji(self._config) as benji_obj:
            try:
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
            except KeyError:
                response.status = f'410 Version {version_uid} not found.'

        return result

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/versions/<version_uid>/deep-scrub', method='POST')
    def _api_v1_versions_deep_scrub_create(
            self, version_uid: str, source: fields.Str(missing=None),
            block_percentage: fields.Int(missing=100)) -> StringIO:
        version_uid_obj = VersionUid(version_uid)
        result = StringIO()
        with Benji(self._config) as benji_obj:
            try:
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
            except KeyError:
                response.status = f'410 Version {version_uid} not found.'

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

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/versions/scrub', method='POST')
    def _api_v1_versions_batch_scrub_create(
        self, filter_expression: fields.Str(missing=None), version_percentage: fields.Int(missing=100),
        block_percentage: fields.Int(missing=100), group_label: fields.Str(missing=None)
    ) -> StringIO:
        return self._batch_scrub('batch_scrub', filter_expression, version_percentage, block_percentage, group_label)

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/versions/deep-scrub', method='POST')
    def _api_v1_versions_batch_deep_scrub_create(
        self, filter_expression: fields.Str(missing=None), version_percentage: fields.Int(missing=100),
        block_percentage: fields.Int(missing=100), group_label: fields.Str(missing=None)
    ) -> StringIO:
        return self._batch_scrub('batch_deep_scrub', filter_expression, version_percentage, block_percentage,
                                 group_label)

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/versions', method='GET')
    def _api_v1_versions_list(
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

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/blocks', method='DELETE')
    def _api_v1_blocks_delete_collection(self, override_lock: fields.Bool(missing=False)) -> None:
        with Benji(self._config) as benji_obj:
            benji_obj.cleanup(override_lock=override_lock)

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/versions/metadata/backup', method='POST')
    def _api_v1_versions_metadata_backup_create(
            self, filter_expression: fields.Str(missing=None), force: fields.Bool(missing=False)) -> None:
        with Benji(self._config) as benji_obj:
            version_uid_objs = [version.uid for version in benji_obj.find_versions_with_filter(filter_expression)]
            benji_obj.metadata_backup(version_uid_objs, overwrite=force)

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/versions/metadata/import', method='POST')
    def _api_v1_versions_metadata_import_create(self) -> None:
        with Benji(self._config) as benji_obj:
            benji_obj.metadata_import(request.body.read())

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/versions/metadata/restore', method='POST')
    def _api_v1_versions_metadata_restore_create(
        self, version_uids: fields.DelimitedList(fields.Str, required=True), storage_name: fields.Str(missing=None)
    ) -> None:
        version_uid_objs = [VersionUid(version_uid) for version_uid in version_uids]
        with Benji(self._config) as benji_obj:
            benji_obj.metadata_restore(version_uid_objs, storage_name)

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/storages', method='GET')
    def _api_v1_storages_list(self) -> List[str]:
        with Benji(self._config) as benji_obj:
            return benji_obj.list_storages()

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/database', method='POST')
    def _api_v1_database_init_create(self) -> None:
        Benji(self._config, init_database=True).close()

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/database', method='PATCH')
    def _api_v1_database_migrate_create(self) -> None:
        Benji(self._config, migrate_database=True).close()

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/versions', method='DELETE')
    def _api_v1_versions_delete_collection(
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

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/version-info', method='GET')
    def _api_v1_version_info_read(self) -> Dict[str, Any]:
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

    @route(f'/apis/{CORE_API_GROUP}/{CORE_API_VERSION_V1}/storages/<storage_name>', method='GET')
    def _api_v1_storages_read(self, storage_name: str) -> Dict[str, int]:
        with Benji(self._config) as benji_obj:
            objects_count, objects_size = benji_obj.storage_stats(storage_name)

            result = {
                'objects_count': objects_count,
                'objects_size': objects_size,
            }

            return result
