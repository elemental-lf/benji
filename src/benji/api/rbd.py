import urllib
from typing import Sequence, List, Tuple, Dict

import structlog

from benji.rpc.server import RPCServer, APIBase
from benji.benji import Benji
from benji.database import VersionUid
from benji.utils import hints_from_rbd_diff, subprocess_run

API_GROUP = 'rbd'
API_VERSION = 'v1'

RBD_SNAP_CREATE_TIMEOUT = 30
RBD_SNAP_RM_TIMEOUT = 30
RBD_SNAP_NAME_PREFIX = 'b-'
CEPH_DEFAULT_USER = 'admin'
IO_MODULE_NAME = 'rbd'

logger = structlog.get_logger(__name__)


@RPCServer.register_api()
class RBDAPI(APIBase):

    @RPCServer.register_task(API_GROUP, API_VERSION)
    def restore(
        self,
        version_uid: str,
        pool: str,
        image: str,
        sparse: bool = False,
        force: bool = False,
        database_backend_less: bool = False,
        monitors: Sequence[str] = None,
        user: str = None,
        keyring: str = None,
        key: str = None,
    ) -> None:
        ceph_credentials_qs = self._build_ceph_credential_query_string(monitors=monitors,
                                                                       user=user,
                                                                       keyring=keyring,
                                                                       key=key)

        target = f'{IO_MODULE_NAME}:{pool}/{image}?{ceph_credentials_qs}'

        with Benji(self._config, in_memory_database=database_backend_less) as benji_obj:
            benji_obj.restore(version_uid=VersionUid(version_uid), target=target, sparse=sparse, force=force)

    @RPCServer.register_task(API_GROUP, API_VERSION)
    def snapshot_create(self,
                        *,
                        pool: str,
                        image: str,
                        snapshot: str,
                        monitors: Sequence[str] = None,
                        user: str = None,
                        keyring: str = None,
                        key: str = None) -> None:
        ceph_credential_args = self._build_ceph_credential_arguments(monitors=monitors,
                                                                     user=user,
                                                                     keyring=keyring,
                                                                     key=key)
        rbd_snap_create_args = ['rbd', 'snap', 'create', f'{pool}/{image}@{snapshot}']
        args_repr = ' '.join(rbd_snap_create_args)
        rbd_snap_create_args.extend(ceph_credential_args)
        subprocess_run(rbd_snap_create_args, timeout=RBD_SNAP_CREATE_TIMEOUT, args_repr=args_repr)

    @RPCServer.register_task(API_GROUP, API_VERSION)
    def snapshot_rm(self,
                    *,
                    pool: str,
                    image: str,
                    snapshot: str,
                    monitors: Sequence[str] = None,
                    user: str = None,
                    keyring: str = None,
                    key: str = None):
        ceph_credential_args = self._build_ceph_credential_arguments(monitors=monitors,
                                                                     user=user,
                                                                     keyring=keyring,
                                                                     key=key)
        rbd_snap_rm_args = ['rbd', 'snap', 'rm', f'{pool}/{image}@{snapshot}']
        args_repr = ' '.join(rbd_snap_rm_args)
        rbd_snap_rm_args.extend(ceph_credential_args)
        subprocess_run(rbd_snap_rm_args, timeout=RBD_SNAP_RM_TIMEOUT, args_repr=args_repr)

    @RPCServer.register_task(API_GROUP, API_VERSION)
    def snapshot_diff(self,
                      *,
                      pool: str,
                      image: str,
                      snapshot: str,
                      last_snapshot: str = None,
                      monitors: Sequence[str] = None,
                      user: str = None,
                      keyring: str = None,
                      key: str = None):
        ceph_credential_args = self._build_ceph_credential_arguments(monitors=monitors,
                                                                     user=user,
                                                                     keyring=keyring,
                                                                     key=key)
        rbd_diff_args = ['rbd', 'diff', '--whole-object', '--format=json']
        if last_snapshot:
            rbd_diff_args.extend(['--from-snap', last_snapshot])
        rbd_diff_args.append(f'{pool}/{image}@{snapshot}')
        args_repr = ' '.join(rbd_diff_args)
        rbd_diff_args.extend(ceph_credential_args)

        return hints_from_rbd_diff(subprocess_run(rbd_diff_args, args_repr=args_repr))

    @RPCServer.register_task(API_GROUP, API_VERSION)
    def snapshot_ls(self,
                    *,
                    pool: str,
                    image: str,
                    monitors: Sequence[str] = None,
                    user: str = None,
                    keyring: str = None,
                    key: str = None):
        ceph_credential_args = self._build_ceph_credential_arguments(monitors=monitors,
                                                                     user=user,
                                                                     keyring=keyring,
                                                                     key=key)
        rbd_snap_ls_args = ['rbd', 'snap', 'ls', '--format=json', f'{pool}/{image}']
        args_repr = ' '.join(rbd_snap_ls_args)
        rbd_snap_ls_args.extend(ceph_credential_args)
        return subprocess_run(rbd_snap_ls_args, decode_json=True, args_repr=args_repr)

    @RPCServer.register_task(API_GROUP, API_VERSION)
    def backup(self,
               *,
               version_uid: str,
               volume: str,
               pool: str,
               image: str,
               snapshot: str,
               hints: Sequence[Tuple[int, int, bool]] = None,
               base_version_uid: str = None,
               block_size: int = None,
               storage_name: str = None,
               labels: Dict[str, str] = None,
               monitors: Sequence[str] = None,
               user: str = None,
               keyring: str = None,
               key: str = None) -> None:

        ceph_credentials_qs = self._build_ceph_credential_query_string(monitors=monitors,
                                                                       user=user,
                                                                       keyring=keyring,
                                                                       key=key)

        source = f'{IO_MODULE_NAME}:{pool}/{image}@{snapshot}?{ceph_credentials_qs}'
        logger.info(f'{source}')
        with Benji(self._config) as benji_obj:
            version = benji_obj.backup(version_uid=VersionUid(version_uid),
                                       volume=volume,
                                       snapshot=snapshot,
                                       source=source,
                                       hints=hints,
                                       base_version_uid=VersionUid(base_version_uid) if base_version_uid else None,
                                       storage_name=storage_name,
                                       block_size=block_size)
            if labels:
                for name, value in labels.items():
                    version.add_label(name, value)

    @staticmethod
    def _build_ceph_credential_arguments(*, monitors: Sequence[str], user: str, keyring: str, key: str) -> List[str]:
        arguments = []
        if monitors:
            arguments += ['-m']
            arguments += [','.join(monitors)]
        arguments += ['--id', user or CEPH_DEFAULT_USER]
        if key:
            arguments += [f'--key={key}']
        elif keyring:
            arguments += ['-k', keyring]
        return arguments

    def _build_ceph_credential_query_string(self, *, monitors: Sequence[str], user: str, keyring: str,
                                            key: str) -> List[str]:
        query_string = []
        if monitors:
            query_string.append(('mon_host', ','.join(monitors)))
        query_string.append(('client_identifier', user or CEPH_DEFAULT_USER))
        if key:
            query_string.append(('key', key))
        elif keyring:
            query_string.append(('keyring', keyring))
        return urllib.parse.urlencode(query_string)
