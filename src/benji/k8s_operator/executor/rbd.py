from datetime import datetime
from typing import Sequence, Optional, List

import attr

from benji.k8s_operator.executor.executor import VolumeBase, ExecutorInterface

from benji.celery import RPCClient
from benji.k8s_operator import OperatorContext
from benji.k8s_operator.resources import BenjiJob
from benji.k8s_operator.utils import random_string

TASK_FIND_VERSIONS = 'core.v1.find_versions_with_filter'
TASK_RBD_SNAPSHOT_LS = 'rbd.v1.snapshot_ls'
TASK_RBD_SNAPSHOT_CREATE = 'rbd.v1.snapshot_create'
TASK_RBD_SNAPSHOT_RM = 'rbd.v1.snapshot_rm'
TASK_RBD_SNAPSHOT_DIFF = 'rbd.v1.snapshot_diff'
TASK_RBD_BACKUP = 'rbd.v1.backup'

RBD_SNAP_NAME_PREFIX = 'b-'


@attr.s(auto_attribs=True, kw_only=True)
class Volume(VolumeBase):
    pool: str
    image: str
    monitors: Sequence[str] = attr.ib(default=[])
    user: Optional[str] = attr.ib(default=None)
    keyring: Optional[str] = attr.ib(default=None)
    key: Optional[str] = attr.ib(default=None)


class Backup(ExecutorInterface):

    def __init__(self, *, logger):
        self.volumes: List[Volume] = []
        self.job: Optional[BenjiJob] = None

        self._logger = logger
        self._rpc_client = RPCClient()
        self._rpc_client_job = RPCClient(auto_queue=True)

    def add_volume(self, volume: Volume):
        if not isinstance(volume, Volume):
            raise TypeError(f'Object has invalid type {type(volume)}.')
        self.volumes.append(volume)

    def start(self):
        if self.volumes:
            for volume in self.volumes:
                self._queue_one_backup(volume)
            self._rpc_client_job.call_async('rpc.v1.terminate', ignore_result=True)

            command = ['benji', 'api-server', self._rpc_client_job.queue]
            self.job = BenjiJob(OperatorContext.kubernetes_client, command=command, parent_body=volume.parent_body)
            self.job.create()

    def _queue_one_backup(self, volume: Volume):
        volume_name = '{}/{}'.format(volume.pvc.namespace, volume.pvc.name)
        version_uid = '{}-{}'.format(volume_name[:246], random_string(6))
        now = datetime.utcnow()
        new_snapshot = RBD_SNAP_NAME_PREFIX + now.strftime('%Y-%m-%dT%H:%M:%SZ')

        benjis_snapshots = self._rpc_client.call(TASK_RBD_SNAPSHOT_LS,
                                                 pool=volume.pool,
                                                 image=volume.image,
                                                 monitors=volume.monitors,
                                                 user=volume.user,
                                                 keyring=volume.keyring,
                                                 key=volume.key)

        benjis_snapshots = [
            snapshot['name'] for snapshot in benjis_snapshots if snapshot['name'].startswith(RBD_SNAP_NAME_PREFIX)
        ]

        if len(benjis_snapshots) == 0:
            self._logger.info(f'{volume_name}: No previous RBD snapshot found, performing initial backup.')

            self._rpc_client.call(TASK_RBD_SNAPSHOT_CREATE,
                                  pool=volume.pool,
                                  image=volume.image,
                                  monitors=volume.monitors,
                                  user=volume.user,
                                  keyring=volume.keyring,
                                  key=volume.key,
                                  snapshot=new_snapshot)
            self._logger.info(f'{volume_name}: Newest RBD snapshot is {volume.pool}/{volume.image}@{new_snapshot}.')

            self._rpc_client_job.call_async(TASK_RBD_BACKUP,
                                            version_uid=version_uid,
                                            volume=volume_name,
                                            pool=volume.pool,
                                            image=volume.image,
                                            snapshot=new_snapshot,
                                            monitors=volume.monitors,
                                            user=volume.user,
                                            keyring=volume.keyring,
                                            key=volume.key,
                                            ignore_result=True)
        else:
            # Delete all snapshots except the newest
            for snapshot in benjis_snapshots[:-1]:
                self._logger.info(f'{volume_name}: Deleting older RBD snapshot {volume.pool}/{volume.image}@{snapshot}.')
                self._rpc_client.call(TASK_RBD_SNAPSHOT_RM,
                                      pool=volume.pool,
                                      image=volume.image,
                                      monitors=volume.monitors,
                                      user=volume.user,
                                      keyring=volume.keyring,
                                      key=volume.key,
                                      snapshot=snapshot)

            last_snapshot = benjis_snapshots[-1]
            self._logger.info(f'{volume_name}: Last RBD snapshot is {volume.pool}/{volume.image}@{last_snapshot}.')

            versions = self._rpc_client.call(
                TASK_FIND_VERSIONS,
                filter_expression=f'volume == "{volume_name}" and snapshot == "{last_snapshot}" and status == "valid"')
            if versions:
                base_version_uid = versions[0]['uid']

                self._rpc_client.call(TASK_RBD_SNAPSHOT_CREATE,
                                      pool=volume.pool,
                                      image=volume.image,
                                      monitors=volume.monitors,
                                      user=volume.user,
                                      keyring=volume.keyring,
                                      key=volume.key,
                                      snapshot=new_snapshot)
                self._logger.info(f'{volume_name}: Newest RBD snapshot is {volume.pool}/{volume.image}@{new_snapshot}.')

                hints = self._rpc_client.call(TASK_RBD_SNAPSHOT_DIFF,
                                              pool=volume.pool,
                                              image=volume.image,
                                              monitors=volume.monitors,
                                              user=volume.user,
                                              keyring=volume.keyring,
                                              key=volume.key,
                                              snapshot=new_snapshot,
                                              last_snapshot=last_snapshot)

                self._logger.info(f'{volume_name}: Deleting last RBD snapshot {volume.pool}/{volume.image}@{last_snapshot}.')
                self._rpc_client.call(TASK_RBD_SNAPSHOT_RM,
                                      pool=volume.pool,
                                      image=volume.image,
                                      monitors=volume.monitors,
                                      user=volume.user,
                                      keyring=volume.keyring,
                                      key=volume.key,
                                      snapshot=last_snapshot)

                self._rpc_client_job.call_async(TASK_RBD_BACKUP,
                                                version_uid=version_uid,
                                                volume=volume_name,
                                                pool=volume.pool,
                                                image=volume.image,
                                                snapshot=new_snapshot,
                                                monitors=volume.monitors,
                                                user=volume.user,
                                                keyring=volume.keyring,
                                                key=volume.key,
                                                base_version_uid=base_version_uid,
                                                hints=hints,
                                                ignore_result=True)
            else:
                self._logger.info(f'{volume_name}: Existing RBD snapshot {volume.pool}/{volume.image}@{last_snapshot} not found in Benji, deleting it and reverting to initial backup.')
                self._rpc_client.call(TASK_RBD_SNAPSHOT_RM,
                                      pool=volume.pool,
                                      image=volume.image,
                                      monitors=volume.monitors,
                                      user=volume.user,
                                      keyring=volume.keyring,
                                      key=volume.key,
                                      snapshot=last_snapshot)

                self._rpc_client.call(TASK_RBD_SNAPSHOT_CREATE,
                                      pool=volume.pool,
                                      image=volume.image,
                                      monitors=volume.monitors,
                                      user=volume.user,
                                      keyring=volume.keyring,
                                      key=volume.key,
                                      snapshot=new_snapshot)
                self._logger.info(f'{volume_name}: Newest RBD snapshot is {volume.pool}/{volume.image}@{new_snapshot}.')

                self._rpc_client_job.call_async(TASK_RBD_BACKUP,
                                                version_uid=version_uid,
                                                volume=volume_name,
                                                pool=volume.pool,
                                                image=volume.image,
                                                snapshot=new_snapshot,
                                                monitors=volume.monitors,
                                                user=volume.user,
                                                keyring=volume.keyring,
                                                key=volume.key,
                                                ignore_result=True)
