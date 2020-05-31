import re
from base64 import b64decode
from typing import Dict, Any

import pykube
from benji.k8s_operator.volume_driver.registry import VolumeDriverRegistry

from benji.k8s_operator import OperatorContext
from benji.k8s_operator.utils import keys_exist
from benji.k8s_operator.volume_driver.interface import VolumeDriverInterface
import benji.k8s_operator.backup.rbd

ROOK_CEPH_MON_ENDPOINTS_CONFIGMAP = 'rook-ceph-mon-endpoints'
ROOK_CEPH_MON_SECRET = 'rook-ceph-mon'


@VolumeDriverRegistry.register(order=20)
class VolumeDriver(VolumeDriverInterface):

    @classmethod
    def handle(cls, *, parent_body: Dict[str, Any], pvc: pykube.PersistentVolumeClaim, pv: pykube.PersistentVolume,
               logger):
        pv_obj = pv.obj
        pool, image, monitors, user, keyring, key = None, None, None, None, None, None

        if keys_exist(pv_obj, ('spec.csi.driver', 'spec.csi.volumeHandle', 'spec.csi.volumeAttributes')):
            driver = pv_obj['spec']['csi']['driver']
            if driver.endswith('.rbd.csi.ceph.com') and 'pool' in pv_obj['spec']['csi']['volumeAttributes']:
                logger.debug(f'Considering PV {pv.name} as a Rook Ceph CSI volume.')

                user = 'admin'
                volume_handle = pv_obj['spec']['csi']['volumeHandle']
                pool = pv_obj['spec']['csi']['volumeAttributes']['pool']
                image_ids = volume_handle.split('-')
                if len(image_ids) >= 9:
                    image = 'csi-vol-' + '-'.join(image_ids[len(image_ids) - 5:])
                else:
                    logger.error(f'PV {pv.name} was provisioned by Rook Ceph CSI, but we do not understand the volumeHandle format: {volume_handle}')
                    return None

                controller_namespace = driver.split('.')[0]
                try:
                    mon_endpoints = pykube.ConfigMap.objects(OperatorContext.kubernetes_client).filter(
                        namespace=controller_namespace).get_by_name(ROOK_CEPH_MON_ENDPOINTS_CONFIGMAP)
                    mon_secret = pykube.Secret.objects(OperatorContext.kubernetes_client).filter(
                        namespace=controller_namespace).get_by_name(ROOK_CEPH_MON_SECRET)
                except pykube.exceptions.ObjectDoesNotExist:
                    logger.error(f'PV {pv.name} was provisioned by Rook Ceph CSI, but the corresponding configmap {ROOK_CEPH_MON_ENDPOINTS_CONFIGMAP} and secret {ROOK_CEPH_MON_SECRET} could not be found namespace {controller_namespace}.')
                    return None

                if keys_exist(mon_endpoints.obj, ('data.data',)):
                    monitors = mon_endpoints.obj['data']['data']
                    monitors = re.sub(r'[a-z]=', '', monitors).split(',')
                else:
                    logger.error(f'PV {pv.name} was provisioned by Rook Ceph CSI, but the configmap {controller_namespace}/{ROOK_CEPH_MON_ENDPOINTS_CONFIGMAP} is missing field data.data.')
                    return None

                if keys_exist(mon_secret.obj, ('data.admin-secret',)):
                    key = b64decode(mon_secret.obj['data']['admin-secret']).decode('ascii')
                else:
                    logger.error(f'PV {pv.name} was provisioned by Rook Ceph CSI, but the secret {controller_namespace}/{ROOK_CEPH_MON_SECRET} is missing field data.admin-secret.')
                    return None
            else:
                logger.warning(f'PV {pv.name} was provisioned by an unknown driver {driver}.')
                return None
        else:
            return None

        logger.info(f'PVC {pvc.namespace}/{pvc.name}, PV {pv.name}: image = {image}, pool = {pool}, monitors = {monitors}, keyring set = {keyring is not None}, key set = {key is not None}.')
        return benji.k8s_operator.backup.rbd.Backup(parent_body=parent_body,
                                                    pvc=pvc,
                                                    pv=pv,
                                                    pool=pool,
                                                    image=image,
                                                    monitors=monitors,
                                                    user=user,
                                                    keyring=keyring,
                                                    key=key,
                                                    logger=logger)
