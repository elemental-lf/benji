import inspect
import string
import random
from typing import Dict, Any

import kopf

from benji.helpers import settings
from benji.helpers.constants import VERSION_LABELS, LABEL_K8S_PVC_NAMESPACE, LABEL_INSTANCE, LABEL_K8S_PVC_NAME, \
    LABEL_K8S_PV_NAME, LABEL_K8S_STORAGE_CLASS_NAME, LABEL_K8S_PV_TYPE, PV_TYPE_RBD, LABEL_RBD_CLUSTER_FSID, \
    LABEL_RBD_IMAGE_SPEC
from benji.api import APIClient
from benji.helpers.utils import keys_exist

SERVICE_NAMESPACE_FILENAME = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'


def get_caller_name() -> str:
    """Returns the name of the calling function"""
    return inspect.getouterframes(inspect.currentframe())[1].function


def check_version_access(benji: APIClient, version_uid: str, crd: Dict[Any, str]) -> None:
    try:
        version = benji.core_v1_get(version_uid=version_uid)
    except KeyError as exception:
        raise kopf.PermanentError(str(exception))

    crd_namespace = crd['metadata']['namespace']
    try:
        version_namespace = version[VERSION_LABELS][LABEL_K8S_PVC_NAMESPACE]
    except KeyError:
        raise kopf.PermanentError(f'Version is missing {LABEL_K8S_PVC_NAMESPACE} label, permission denied.')

    if crd_namespace != version_namespace:
        raise kopf.PermanentError('Version namespace label does not match resource namespace, permission denied')


def cr_to_job_name(body, suffix: str):
    if 'namespace' in body['metadata']:
        return f'crd:{body["kind"]}/{body["metadata"]["namespace"]}/{body["metadata"]["name"]}-{suffix}'
    else:
        return f'crd:{body["kind"]}/{body["metadata"]["name"]}-{suffix}'


def service_account_namespace() -> str:
    with open(SERVICE_NAMESPACE_FILENAME, 'r') as f:
        namespace = f.read()
        if namespace == '':
            raise RuntimeError(f'{SERVICE_NAMESPACE_FILENAME} is empty.')
    return namespace


def build_version_labels_rbd(*, pvc, pv, pool: str, image: str) -> Dict[str, str]:
    version_labels = {
        LABEL_INSTANCE: settings.benji_instance,
        LABEL_K8S_PVC_NAMESPACE: pvc.metadata.namespace,
        LABEL_K8S_PVC_NAME: pvc.metadata.name,
        LABEL_K8S_PV_NAME: pv.metadata.name,
        # RBD specific
        LABEL_K8S_PV_TYPE: PV_TYPE_RBD,
        LABEL_RBD_IMAGE_SPEC: f'{pool}/{image}',
    }

    return version_labels


def determine_rbd_image_location(pv: Dict[str, Any], logger) -> (str, str):
    pool, image = None, None

    pv_name = pv['metadata']['name']
    if keys_exist(pv['spec']['rbd.pool', 'rbd.image']):
        logger.debug(f'Considering PersistentVolume {pv_name} as a native Ceph RBD volume.')
        pool, image = pv['spec']['rbd']['pool'], pv['spec']['rbd']['image']
    elif keys_exist(pv['spec']['flexVolume.options', 'flexVolume.driver']):
        logger.debug(f'Considering PersistentVolume {pv_name} as a Rook Ceph FlexVolume volume.')
        options = pv['spec']['flexVolume']['options']
        driver = pv['spec']['flexVolume']['driver']
        if driver.startswith('ceph.rook.io/') and options.get('pool') and options.get('image'):
            pool, image = options['pool'], options['image']
        else:
            logger.debug(f'PersistentVolume {pv_name} was provisioned by unknown driver {driver}.')
    elif keys_exist(pv['spec']['csi.driver', 'csi.volume_handle', 'csi.volume_attributes']):
        logger.debug(f'Considering PersistentVolume {pv_name} as a Rook Ceph CSI volume.')
        driver = pv['spec']['csi']['driver']
        volume_handle = pv['spec']['csi']['volumeHandle']
        if driver.endswith('.rbd.csi.ceph.com') and 'pool' in pv['spec']['csi']['volumeAttributes']:
            pool = pv['spec']['csi']['volumeAttributes']['pool']
            image_ids = volume_handle.split('-')
            if len(image_ids) >= 9:
                image = 'csi-vol-' + '-'.join(image_ids[len(image_ids) - 5:])
            else:
                logger.warning(f'PersistentVolume {pv_name} was provisioned by Rook Ceph CSI, but we do not understand the volumeHandle format: {volume_handle}')

    return pool, image


def random_string(length: int, characters: str = string.ascii_lowercase + string.digits) -> str:
    return ''.join(random.choice(characters) for _ in range(length))
