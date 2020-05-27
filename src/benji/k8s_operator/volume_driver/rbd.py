from base64 import b64decode

import pykube
from benji.k8s_operator.volume_driver.registry import VolumeDriverRegistry

from benji.k8s_operator import OperatorContext
from benji.k8s_operator.resources import StorageClass
from benji.k8s_operator.utils import keys_exist
from benji.k8s_operator.volume_driver.base import VolumeDriverInterface
import benji.k8s_operator.backup.rbd


@VolumeDriverRegistry.register(order=10)
class RBD(VolumeDriverInterface):

    @classmethod
    def handle(cls, *, pvc: pykube.PersistentVolumeClaim, pv: pykube.PersistentVolume, logger):
        pvc_obj = pvc.obj
        pv_obj = pv.obj
        pool, image, monitors, user, keyring, key = None, None, None, None, None, None

        if keys_exist(pv_obj, ('spec.rbd.pool', 'spec.rbd.image')):
            pool, image = pv_obj['spec']['rbd']['pool'], pv_obj['spec']['rbd']['image']

            if keys_exist(pvc_obj, ('spec.storageClassName',)):
                storage_class_name = pvc_obj['spec']['storageClassName']
                try:
                    storage_class = StorageClass.objects(OperatorContext.kubernetes_client).get_by_name(storage_class_name)
                except pykube.exceptions.ObjectDoesNotExist:
                    logger.error(f'Unable to determine Ceph credentials for PVC {pvc.namespace}/{pvc.name}/'
                                 f'PV {pv.name}, storage class {storage_class_name} does not exist anymore.')
                    return None
                else:
                    storage_class_obj = storage_class.obj
                    if keys_exist(storage_class_obj, ('parameters.adminId', 'parameters.adminSecretName',
                                                      'parameters.adminSecretNamespace', 'parameters.monitors')):
                        admin_secret_name = storage_class_obj['parameters']['adminSecretName']
                        admin_secret_namespace = storage_class_obj['parameters']['adminSecretNamespace']

                        try:
                            admin_secret = pykube.Secret.objects(OperatorContext.kubernetes_client).filter(
                                namespace=admin_secret_namespace).get_by_name(admin_secret_name)
                        except pykube.exceptions.ObjectDoesNotExist:
                            logger.error(f'Unable to determine Ceph credentials for PVC {pvc.namespace}/{pvc.name}/'
                                         f'PV {pv.name}, admin secret referenced in storage class {storage_class_name} '
                                         'does not exist')
                            return None
                        else:
                            admin_secret_obj = admin_secret.obj
                            if keys_exist(admin_secret_obj, ('data.key',)):
                                user = storage_class_obj['parameters']['adminId']
                                monitors = storage_class_obj['parameters']['monitors']
                                key = b64decode(admin_secret_obj['data']['key']).decode('ascii')
                            else:
                                logger.error(f'Unable to determine Ceph credentials for PVC {pvc.namespace}/{pvc.name}/'
                                             f'PV {pv.name}, admin secret is missing required field data.key.')
                                return None
                    else:
                        logger.error(f'Unable to determine Ceph credentials for PVC {pvc.namespace}/{pvc.name}/'
                                     f'PV {pv.name}, storage class {storage_class_name} does not look like an RBD backed '
                                     'class.')
                        return None
        else:
            return None

        logger.info(f'PVC {pvc.namespace}/{pvc.name}, PV {pv.name}: image = {image}, pool = {pool}, monitors = {monitors}, keyring set = {keyring is not None}, key set = {key is not None}.')
        return benji.k8s_operator.backup.rbd.Backup(pvc=pvc,
                                                    pv=pv,
                                                    pool=pool,
                                                    image=image,
                                                    monitors=monitors,
                                                    user=user,
                                                    keyring=keyring,
                                                    key=key)
