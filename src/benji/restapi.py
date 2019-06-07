import fileinput
import json
import os
import sys
from typing import List, Optional, Tuple

from io import BytesIO
from prettytable import PrettyTable
from bottle import Bottle, response
from webargs import fields
from webargs.bottleparser import use_args, use_kwargs

import benji.exception
from benji import __version__
from benji.benji import Benji
from benji.database import Version, VersionUid
from benji.logging import logger
from benji.utils import hints_from_rbd_diff, InputValidation
from benji.versions import VERSIONS


def route(path, **kwargs):

    def decorator(f):
        f.bottle_route = kwargs
        f.bottle_route['path'] = path
        return f

    return decorator


def error(status: int):

    def decorator(f):
        f.bottle_error = status
        return f

    return decorator


class RestAPI:
    """Proxy between REST calls and actual backup code."""

    def __init__(self, config):
        self._app = Bottle()
        self._config = config
        self._install_routes()

    def _install_routes(self):

        def handle_error(err):
            response.content_type = "application/json"
            return err.body

        self._app.error(400, callback=handle_error)
        self._app.error(422, callback=handle_error)

        for kw in dir(self):
            attr = getattr(self, kw)
            if hasattr(attr, 'bottle_route'):
                self._app.route(**attr.bottle_route)(attr)
            if hasattr(attr, 'bottle_error'):
                self._app.error(attr.bottle_error.status)(attr)

    def run(self):
        self._app.run()

    backup_args = {
        'version_name': fields.Str(required=True),
        'snapshot_name': fields.Str(required=True),
        'source': fields.Str(required=True),
        'rbd_hints': fields.Str(default=None),
        'base_version_uid': fields.Str(default=None),
        'block_size': fields.Int(default=None),
        'storage_name': fields.Int(default=None),
    }

    @route('/api/v1/versions', method='POST', apply=use_kwargs(backup_args))
    def backup(self, version_name: str, snapshot_name: str, source: str, rbd_hints: str, base_version_uid: str,
               block_size: int, labels: List[str], storage_name) -> None:
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
            benji_obj = Benji(self._config, block_size=block_size)
            hints = None
            if rbd_hints:
                data = ''.join([line for line in fileinput.input(rbd_hints).readline()])
                hints = hints_from_rbd_diff(data)
            backup_version = benji_obj.backup(version_name, snapshot_name, source, hints, base_version_uid_obj,
                                              storage_name)

            benji_obj.export_any({'versions': [backup_version]},
                                 sys.stdout,
                                 ignore_relationships=[((Version,), ('blocks',))])
        finally:
            if benji_obj:
                benji_obj.close()

    @route('/api/v1/versions/<version_uid>/restore', method='GET')
    def restore(self, version_uid: str, destination: str, sparse: bool, force: bool,
                database_backend_less: bool) -> None:
        version_uid_obj = VersionUid(version_uid)
        benji_obj = None
        try:
            benji_obj = Benji(self._config, in_memory_database=database_backend_less)
            if database_backend_less:
                benji_obj.metadata_restore([version_uid_obj])
            benji_obj.restore(version_uid_obj, destination, sparse, force)
        finally:
            if benji_obj:
                benji_obj.close()

    update_args = {
        'protected': fields.Bool(default=False),
    }

    @route('/apis/benji/v1/versions/<version_uid>', method='PATCH', apply=use_kwargs(update_args))
    def update(self, version_uid: str, protected: bool) -> None:
        version_uid = VersionUid(version_uid)
        benji_obj = None
        try:
            benji_obj = Benji(self._config)
            benji_obj.protect(version_uid, protected)
        finally:
            if benji_obj:
                benji_obj.close()

    rm_args = {
        'force': fields.Bool(default=False),
        'keep_metadata_backup': fields.Bool(default=False),
        'override_lock': fields.Bool(default=False),
    }

    @route('/api/v1/versions/<version_uid>', method='DELETE', apply=use_kwargs(rm_args))
    def rm(self, version_uid: str, force: bool, keep_metadata_backup: bool, override_lock: bool) -> None:
        version_uid = VersionUid(version_uid)
        disallow_rm_when_younger_than_days = self._config.get('disallowRemoveWhenYounger', types=int)
        benji_obj = None
        try:
            benji_obj = Benji(self._config)
            benji_obj.rm(version_uid,
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
            benji_obj = Benji(self._config)
            benji_obj.scrub(version_uid_obj, block_percentage=block_percentage)
        except benji.exception.ScrubbingError:
            assert benji_obj is not None
            if self.machine_output:
                benji_obj.export_any(
                    {
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
            benji_obj = Benji(self._config)
            benji_obj.deep_scrub(version_uid_obj, source=source, block_percentage=block_percentage)
        except benji.exception.ScrubbingError:
            assert benji_obj is not None
            if self.machine_output:
                benji_obj.export_any(
                    {
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
            benji_obj = Benji(self._config)
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

    ls_args = {
        'filter_expression': fields.Str(required=True),
        'include_labels': fields.Bool(default=False),
        'include_stats': fields.Bool(default=False),
    }

    @route('/api/v1/versions', method='GET', apply=use_kwargs(ls_args))
    def ls(self, filter_expression: Optional[str], include_labels: bool, include_stats: bool) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self._config)
            versions = benji_obj.ls_with_filter(filter_expression)

            if self.machine_output:
                benji_obj.export_any(
                    {'versions': versions},
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
            benji_obj = Benji(self._config)
            benji_obj.cleanup(override_lock=override_lock)
        finally:
            if benji_obj:
                benji_obj.close()

    @route('/api/v1/versions')
    def metadata_export(self, filter_expression: Optional[str], output_file: Optional[str], force: bool) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self._config)
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
            benji_obj = Benji(self._config)
            version_uid_objs = [version.uid for version in benji_obj.ls_with_filter(filter_expression)]
            benji_obj.metadata_backup(version_uid_objs, overwrite=force)
        finally:
            if benji_obj:
                benji_obj.close()

    def metadata_import(self, input_file: str = None) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self._config)
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
            benji_obj = Benji(self._config)
            benji_obj.metadata_restore(version_uid_objs, storage)
        finally:
            if benji_obj:
                benji_obj.close()

    def metadata_ls(self, storage: str = None) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self._config)
            version_uids = benji_obj.metadata_ls(storage)
            content = json.dumps([version_uid.v_string for version_uid in version_uids])

            response.content_type = 'application/json'
            return content
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
            benji_obj = Benji(self._config)
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

    @route('/api/v1/database', method='POST')
    def database_init(self) -> None:
        benji_obj = Benji(self._config, init_database=True)
        benji_obj.close()

    @route('/api/v1/database', method='PATCH')
    def database_migrate(self) -> None:
        benji_obj = Benji(self._config, migrate_database=True)
        benji_obj.close()

    def enforce_retention_policy(self, rules_spec: str, filter_expression: str, dry_run: bool,
                                 keep_metadata_backup: bool, group_label: Optional[str]) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self._config)
            dismissed_versions = benji_obj.enforce_retention_policy(filter_expression=filter_expression,
                                                                    rules_spec=rules_spec,
                                                                    dry_run=dry_run,
                                                                    keep_metadata_backup=keep_metadata_backup,
                                                                    group_label=group_label)

            content = BytesIO()
            benji_obj.export_any({
                'versions': dismissed_versions,
            },
                                 content,
                                 ignore_relationships=[((Version,), ('blocks',))])

            response.content_type = 'application/json'
            return content
        finally:
            if benji_obj:
                benji_obj.close()

    @route('/api/v1/version-info', method='GET')
    def version_info(self) -> None:
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

        response.content_type = 'application/json'
        return json.dumps(versions)

    def storage_stats(self, storage_name: str = None) -> None:
        benji_obj = None
        try:
            benji_obj = Benji(self._config)
            objects_count, objects_size = benji_obj.storage_stats(storage_name)

            content = BytesIO()
            benji_obj.export_any({
                'objects_count': objects_count,
                'objects_size': objects_size,
            }, content)

            response.content_type = 'application/json'
            return content
        finally:
            if benji_obj:
                benji_obj.close()
