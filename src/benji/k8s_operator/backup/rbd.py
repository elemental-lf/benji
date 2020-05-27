from typing import Sequence

import pykube

from benji.k8s_operator.backup.interface import BackupInterface


class Backup(BackupInterface):

    def __init__(self, *, pvc: pykube.PersistentVolumeClaim, pv: pykube.PersistentVolume, logger, pool: str, image: str,
                 monitors: Sequence[str], user: str, keyring: str, key: str):
        self.pvc = pvc
        self.pv = pv
        self.logger = logger
        self.pool = pool
        self.image = image
        self.monitors = monitors
        self.user = user
        self.keyring = keyring
        self.key = key

    def backup(self):
        self.logger.info(f'Backup called for {self.pvc}.')
        pass
        # with RPCClient() as rpc_client:
        #     pass
        #
        # with RPCClient(auto_queue=True) as rpc_client:
        #     rpc_calls = 0
        #     for pvc, pv in pvc_pv_pairs:
        #         pvc_obj = pvc.obj
        #
        #         pvc_name = '{}/{}'.format(pvc_obj['metadata']['namespace'], pvc_obj['metadata']['name'])
        #         version_uid = '{}-{}'.format(pvc_name[:246], random_string(6))
        #
        #         rbd_materials = determine_rbd_materials(pvc, pv, logger=logger)
        #         if rbd_materials is None:
        #             continue
        #
        #         version_labels = build_version_labels_rbd(pvc=pvc, pv=pv)
        #
        #         rpc_client.call_async('rbd.v1.backup',
        #                               version_uid=version_uid,
        #                               volume=pvc_name,
        #                               pool=rbd_materials.pool,
        #                               image=rbd_materials.image,
        #                               monitors=rbd_materials.monitors,
        #                               user=rbd_materials.user,
        #                               keyring=rbd_materials.keyring,
        #                               key=rbd_materials.key,
        #                               version_labels=version_labels,
        #                               ignore_result=True)
        #         rpc_calls += 1
        #
        #     if rpc_calls > 0:
        #         rpc_client.call_async('rpc.v1.terminate', ignore_result=True)
        #
        #         command = ['benji', 'api-server', rpc_client.queue]
        #         job = BenjiJob(OperatorContext.kubernetes_client, command=command, parent_body=parent_body)
        #         job.create()
