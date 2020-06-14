from typing import Dict, Any

import pykube

import benji.k8s_operator.executor.rbd
from benji.k8s_operator.executor.executor import BatchExecutor
from benji.k8s_operator.utils import keys_exist
from benji.k8s_operator.volume_driver.interface import VolumeDriverInterface
from benji.k8s_operator.volume_driver.registry import VolumeDriverRegistry


@VolumeDriverRegistry.register(order=30)
class RookFlexVolume(VolumeDriverInterface):

    @classmethod
    def handle(cls, *, batch_executor: BatchExecutor, parent_body: Dict[str, Any], pvc: pykube.PersistentVolumeClaim,
               pv: pykube.PersistentVolume) -> bool:
        logger = batch_executor.logger
        pv_obj = pv.obj
        pool, image, monitors, user, keyring, key = None, None, None, None, None, None

        if keys_exist(pv_obj, ('spec.flexVolume.options', 'spec.flexVolume.driver')):
            options = pv_obj['spec']['flexVolume']['options']
            driver = pv_obj['spec']['flexVolume']['driver']
            if driver.startswith('ceph.rook.io/') and options.get('pool') and options.get('image'):
                pool, image = options['pool'], options['image']
            else:
                return False
        else:
            return False

        logger.info(f'PVC {pvc.namespace}/{pvc.name}, PV {pv.name}: image = {image}, pool = {pool}, monitors = {monitors}, keyring set = {keyring is not None}, key set = {key is not None}.')
        volume = benji.k8s_operator.executor.rbd.Backup(parent_body=parent_body,
                                                        pvc=pvc,
                                                        pv=pv,
                                                        pool=pool,
                                                        image=image,
                                                        monitors=monitors,
                                                        user=user,
                                                        keyring=keyring,
                                                        key=key,
                                                        logger=logger)
        batch_executor.get_executor(benji.k8s_operator.executor.rbd.Backup).add_volume(volume)
        return True
