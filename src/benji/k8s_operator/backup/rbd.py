from datetime import datetime
from typing import Sequence, Dict, Any

import pykube
from benji.k8s_operator import OperatorContext

from benji.celery import RPCClient
from benji.k8s_operator.resources import BenjiJob, NamespacedAPIObject
from benji.k8s_operator.utils import random_string

from benji.k8s_operator.backup.interface import BackupInterface

TASK_FIND_VERSIONS = 'core.v1.find_versions_with_filter'
TASK_RBD_SNAPSHOT_LS = 'rbd.v1.snapshot_ls'
TASK_RBD_SNAPSHOT_CREATE = 'rbd.v1.snapshot_create'
TASK_RBD_SNAPSHOT_RM = 'rbd.v1.snapshot_rm'
TASK_RBD_SNAPSHOT_DIFF = 'rbd.v1.snapshot_diff'
TASK_RBD_BACKUP = 'rbd.v1.backup'

RBD_SNAP_NAME_PREFIX = 'b-'


class Backup(BackupInterface):

    def __init__(self, *, parent_body: Dict[str, Any], pvc: pykube.PersistentVolumeClaim, pv: pykube.PersistentVolume,
                 logger, pool: str, image: str, monitors: Sequence[str], user: str, keyring: str, key: str):
        self.parent_body = parent_body
        self.pvc = pvc
        self.pv = pv
        self.logger = logger
        self.pool = pool
        self.image = image
        self.monitors = monitors
        self.user = user
        self.keyring = keyring
        self.key = key

        self._rpc_client = RPCClient()
        self._rpc_client_job = RPCClient(auto_queue=True)

    def backup(self):
        volume = '{}/{}'.format(self.pvc.namespace, self.pvc.name)
        version_uid = '{}-{}'.format(volume[:246], random_string(6))
        now = datetime.utcnow()
        new_snapshot = RBD_SNAP_NAME_PREFIX + now.strftime('%Y-%m-%dT%H:%M:%SZ')

        benjis_snapshots = self._rpc_client.call(TASK_RBD_SNAPSHOT_LS,
                                                 pool=self.pool,
                                                 image=self.image,
                                                 monitors=self.monitors,
                                                 user=self.user,
                                                 keyring=self.keyring,
                                                 key=self.key)

        benjis_snapshots = [
            snapshot['name'] for snapshot in benjis_snapshots if snapshot['name'].startswith(RBD_SNAP_NAME_PREFIX)
        ]

        if len(benjis_snapshots) == 0:
            self.logger.info(f'{volume}: No previous RBD snapshot found, performing initial backup.')

            self._rpc_client.call(TASK_RBD_SNAPSHOT_CREATE,
                                  pool=self.pool,
                                  image=self.image,
                                  monitors=self.monitors,
                                  user=self.user,
                                  keyring=self.keyring,
                                  key=self.key,
                                  snapshot=new_snapshot)
            self.logger.info(f'{volume}: Newest RBD snapshot is {self.pool}/{self.image}@{new_snapshot}.')

            self._rpc_client_job.call_async(TASK_RBD_BACKUP,
                                            version_uid=version_uid,
                                            volume=volume,
                                            pool=self.pool,
                                            image=self.image,
                                            snapshot=new_snapshot,
                                            monitors=self.monitors,
                                            user=self.user,
                                            keyring=self.keyring,
                                            key=self.key,
                                            ignore_result=True)
        else:
            # Delete all snapshots except the newest
            for snapshot in benjis_snapshots[:-1]:
                self.logger.info(f'{volume}: Deleting older RBD snapshot {self.pool}/{self.image}@{snapshot}.')
                self._rpc_client.call(TASK_RBD_SNAPSHOT_RM,
                                      pool=self.pool,
                                      image=self.image,
                                      monitors=self.monitors,
                                      user=self.user,
                                      keyring=self.keyring,
                                      key=self.key,
                                      snapshot=snapshot)

            last_snapshot = benjis_snapshots[-1]
            self.logger.info(f'{volume}: Last RBD snapshot is {self.pool}/{self.image}@{last_snapshot}.')

            versions = self._rpc_client.call(
                TASK_FIND_VERSIONS,
                filter_expression=f'volume == "{volume}" and snapshot == "{last_snapshot}" and status == "valid"')
            if versions:
                base_version_uid = versions[0].uid

                self._rpc_client.call(TASK_RBD_SNAPSHOT_CREATE,
                                      pool=self.pool,
                                      image=self.image,
                                      monitors=self.monitors,
                                      user=self.user,
                                      keyring=self.keyring,
                                      key=self.key,
                                      snapshot=new_snapshot)
                self.logger.info(f'{volume}: Newest RBD snapshot is {self.pool}/{self.image}@{new_snapshot}.')

                hints = self._rpc_client.call(TASK_RBD_SNAPSHOT_DIFF,
                                              pool=self.pool,
                                              image=self.image,
                                              monitors=self.monitors,
                                              user=self.user,
                                              keyring=self.keyring,
                                              key=self.key,
                                              snapshot=new_snapshot,
                                              last_snapshot=last_snapshot)

                self.logger.info(f'{volume}: Deleting last RBD snapshot {self.pool}/{self.image}@{last_snapshot}.')
                self._rpc_client.call(TASK_RBD_SNAPSHOT_RM,
                                      pool=self.pool,
                                      image=self.image,
                                      monitors=self.monitors,
                                      user=self.user,
                                      keyring=self.keyring,
                                      key=self.key,
                                      snapshot=last_snapshot)

                self._rpc_client_job.call_async(TASK_RBD_BACKUP,
                                                version_uid=version_uid,
                                                volume=volume,
                                                pool=self.pool,
                                                image=self.image,
                                                snapshot=new_snapshot,
                                                monitors=self.monitors,
                                                user=self.user,
                                                keyring=self.keyring,
                                                key=self.key,
                                                base_version_uid=base_version_uid,
                                                hints=hints,
                                                ignore_result=True)
            else:
                self.logger.info(f'{volume}: Existing RBD snapshot {self.pool}/{self.image}@{last_snapshot} not found in Benji, deleting it and reverting to initial backup.')
                self._rpc_client.call(TASK_RBD_SNAPSHOT_RM,
                                      pool=self.pool,
                                      image=self.image,
                                      monitors=self.monitors,
                                      user=self.user,
                                      keyring=self.keyring,
                                      key=self.key,
                                      snapshot=last_snapshot)

                self._rpc_client.call(TASK_RBD_SNAPSHOT_CREATE,
                                      pool=self.pool,
                                      image=self.image,
                                      monitors=self.monitors,
                                      user=self.user,
                                      keyring=self.keyring,
                                      key=self.key,
                                      snapshot=new_snapshot)
                self.logger.info(f'{volume}: Newest RBD snapshot is {self.pool}/{self.image}@{new_snapshot}.')

                self._rpc_client_job.call_async(TASK_RBD_BACKUP,
                                                version_uid=version_uid,
                                                volume=volume,
                                                pool=self.pool,
                                                image=self.image,
                                                snapshot=new_snapshot,
                                                monitors=self.monitors,
                                                user=self.user,
                                                keyring=self.keyring,
                                                key=self.key,
                                                ignore_result=True)

            self._rpc_client_job.call_async('rpc.v1.terminate', ignore_result=True)

            command = ['benji', 'api-server', self._rpc_client_job.queue]
            job = BenjiJob(OperatorContext.kubernetes_client, command=command, parent_body=self.parent_body)
            job.create()
