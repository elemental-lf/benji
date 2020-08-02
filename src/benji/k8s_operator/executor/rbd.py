import logging
import re
from base64 import b64decode
from collections import defaultdict
from datetime import datetime
from typing import Sequence, Optional, List, Dict, Any

import attr
import pykube

from benji.api.client import RPCClient
from benji.k8s_operator import OperatorContext, settings
from benji.k8s_operator.constants import LABEL_INSTANCE, LABEL_K8S_PVC_NAMESPACE, LABEL_K8S_PVC_NAME
from benji.k8s_operator.executor.executor import ExecutorInterface, ActionType, \
    BACKUP_ACTION, BatchExecutor
from benji.k8s_operator.resources import BenjiJob, StorageClass
from benji.k8s_operator.utils import random_string, keys_exist

ROOK_CEPH_MON_ENDPOINTS_CONFIGMAP = 'rook-ceph-mon-endpoints'
ROOK_CEPH_MON_SECRET = 'rook-ceph-mon'

core_v1_find_versions_with_filter = RPCClient.signature('core.v1.find_versions_with_filter')
rbd_v1_snapshot_ls = RPCClient.signature('rbd.v1.snapshot_ls')
rbd_v1_snapshot_create = RPCClient.signature('rbd.v1.snapshot_create')
rbd_v1_snapshot_rm = RPCClient.signature('rbd.v1.snapshot_rm')
rbd_v1_snapshot_diff = RPCClient.signature('rbd.v1.snapshot_diff')
rbd_v1_backup = RPCClient.signature('rbd.v1.backup', options={'ignore_result': True})
rpc_v1_terminate = RPCClient.signature('rpc.v1.terminate', options={'ignore_result': True})

RBD_SNAP_NAME_PREFIX = 'b-'

logger = logging.getLogger(__name__)


@attr.s(auto_attribs=True, kw_only=True)
class _Volume:
    parent_body: Dict[str, Any]
    pvc: pykube.PersistentVolumeClaim
    pv: pykube.PersistentVolume
    pool: str
    image: str
    monitors: Sequence[str] = attr.ib(default=[])
    user: Optional[str] = attr.ib(default=None)
    keyring: Optional[str] = attr.ib(default=None)
    key: Optional[str] = attr.ib(default=None)


@BatchExecutor.register(order=10)
class RBDExecutor(ExecutorInterface):

    def __init__(self):
        self._volumes: Dict[object, List[_Volume]] = defaultdict(lambda: [])
        self._rpc_client = RPCClient()
        self._rbd_v1_backup = self._rpc_client.to_dedicated_queue(rbd_v1_backup)
        self._rpc_v1_terminate = self._rpc_client.to_dedicated_queue(rpc_v1_terminate)

    @BatchExecutor.register_as_volume_handler
    def handle_rook_csi(self, *, action: ActionType, parent_body: Dict[str, Any], pvc: pykube.PersistentVolumeClaim,
                        pv: pykube.PersistentVolume) -> bool:
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
                    return False

                controller_namespace = driver.split('.')[0]
                try:
                    mon_endpoints = pykube.ConfigMap.objects(OperatorContext.kubernetes_client).filter(
                        namespace=controller_namespace).get_by_name(ROOK_CEPH_MON_ENDPOINTS_CONFIGMAP)
                    mon_secret = pykube.Secret.objects(OperatorContext.kubernetes_client).filter(
                        namespace=controller_namespace).get_by_name(ROOK_CEPH_MON_SECRET)
                except pykube.exceptions.ObjectDoesNotExist:
                    logger.error(f'PV {pv.name} was provisioned by Rook Ceph CSI, but the corresponding configmap {ROOK_CEPH_MON_ENDPOINTS_CONFIGMAP} and secret {ROOK_CEPH_MON_SECRET} could not be found namespace {controller_namespace}.')
                    return False

                if keys_exist(mon_endpoints.obj, ('data.data',)):
                    monitors = mon_endpoints.obj['data']['data']
                    monitors = re.sub(r'[a-z]=', '', monitors).split(',')
                else:
                    logger.error(f'PV {pv.name} was provisioned by Rook Ceph CSI, but the configmap {controller_namespace}/{ROOK_CEPH_MON_ENDPOINTS_CONFIGMAP} is missing field data.data.')
                    return False

                if keys_exist(mon_secret.obj, ('data.admin-secret',)):
                    key = b64decode(mon_secret.obj['data']['admin-secret']).decode('ascii')
                else:
                    logger.error(f'PV {pv.name} was provisioned by Rook Ceph CSI, but the secret {controller_namespace}/{ROOK_CEPH_MON_SECRET} is missing field data.admin-secret.')
                    return False
            else:
                logger.warning(f'PV {pv.name} was provisioned by an unknown driver {driver}.')
                return False
        else:
            return False

        logger.info(f'PVC {pvc.namespace}/{pvc.name}, PV {pv.name}: image = {image}, pool = {pool}, monitors = {monitors}, keyring set = {keyring is not None}, key set = {key is not None}.')
        volume = _Volume(parent_body=parent_body,
                         pvc=pvc,
                         pv=pv,
                         pool=pool,
                         image=image,
                         monitors=monitors,
                         user=user,
                         keyring=keyring,
                         key=key)
        self._volumes[action].append(volume)
        return True

    @BatchExecutor.register_as_volume_handler
    def handle_rbd(self, *, action: ActionType, parent_body: Dict[str, Any], pvc: pykube.PersistentVolumeClaim,
                   pv: pykube.PersistentVolume) -> bool:
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
                    return False
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
                            return False
                        else:
                            admin_secret_obj = admin_secret.obj
                            if keys_exist(admin_secret_obj, ('data.key',)):
                                user = storage_class_obj['parameters']['adminId']
                                monitors = storage_class_obj['parameters']['monitors']
                                key = b64decode(admin_secret_obj['data']['key']).decode('ascii')
                            else:
                                logger.error(f'Unable to determine Ceph credentials for PVC {pvc.namespace}/{pvc.name}/'
                                             f'PV {pv.name}, admin secret is missing required field data.key.')
                                return False
                    else:
                        logger.error(f'Unable to determine Ceph credentials for PVC {pvc.namespace}/{pvc.name}/'
                                     f'PV {pv.name}, storage class {storage_class_name} does not look like an RBD backed '
                                     'class.')
                        return False
        else:
            return False

        logger.info(f'PVC {pvc.namespace}/{pvc.name}, PV {pv.name}: image = {image}, pool = {pool}, monitors = {monitors}, keyring set = {keyring is not None}, key set = {key is not None}.')
        volume = _Volume(parent_body=parent_body,
                         pvc=pvc,
                         pv=pv,
                         pool=pool,
                         image=image,
                         monitors=monitors,
                         user=user,
                         keyring=keyring,
                         key=key,
                         logger=logger)
        self._volumes[action].append(volume)
        return True

    @BatchExecutor.register_as_volume_handler
    def handle_flex_volume(self, *, action: ActionType, parent_body: Dict[str, Any], pvc: pykube.PersistentVolumeClaim,
                           pv: pykube.PersistentVolume) -> bool:
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
        volume = _Volume(parent_body=parent_body,
                         pvc=pvc,
                         pv=pv,
                         pool=pool,
                         image=image,
                         monitors=monitors,
                         user=user,
                         keyring=keyring,
                         key=key,
                         logger=logger)
        self._volumes[action].append(volume)
        return True

    def start(self):
        if self._volumes:
            for volume in self._volumes[BACKUP_ACTION]:
                self._queue_backup(volume)
            self._rpc_v1_terminate.delay()

            command = ['benji', 'api-server', self._rpc_client.dedicated_queue]
            BenjiJob(OperatorContext.kubernetes_client, command=command, parent_body=volume.parent_body).create()
        self._rpc_client.close()

    @staticmethod
    def _build_version_labels(pvc: pykube.PersistentVolumeClaim) -> Dict[str, str]:
        pvc_obj = pvc.obj
        labels = {
            LABEL_INSTANCE: settings.benji_instance,
            LABEL_K8S_PVC_NAMESPACE: pvc_obj['metadata']['namespace'],
            LABEL_K8S_PVC_NAME: pvc_obj['metadata']['name'],
        }

        return labels

    def _queue_backup(self, volume: _Volume):
        volume_name = '{}/{}'.format(volume.pvc.namespace, volume.pvc.name)
        version_uid = '{}-{}'.format(volume_name[:246], random_string(6))
        labels = self._build_version_labels(volume.pvc)
        now = datetime.utcnow()
        new_snapshot = RBD_SNAP_NAME_PREFIX + now.strftime('%Y-%m-%dT%H:%M:%SZ')

        benjis_snapshots = rbd_v1_snapshot_ls.delay(pool=volume.pool,
                                                    image=volume.image,
                                                    monitors=volume.monitors,
                                                    user=volume.user,
                                                    keyring=volume.keyring,
                                                    key=volume.key).get()

        benjis_snapshots = [
            snapshot['name'] for snapshot in benjis_snapshots if snapshot['name'].startswith(RBD_SNAP_NAME_PREFIX)
        ]

        if len(benjis_snapshots) == 0:
            logger.info(f'{volume_name}: No previous RBD snapshot found, performing initial backup.')

            # Don't ignore result, we want exceptions to be delivered
            rbd_v1_snapshot_create.delay(pool=volume.pool,
                                         image=volume.image,
                                         monitors=volume.monitors,
                                         user=volume.user,
                                         keyring=volume.keyring,
                                         key=volume.key,
                                         snapshot=new_snapshot).get()
            logger.info(f'{volume_name}: Newest RBD snapshot is {volume.pool}/{volume.image}@{new_snapshot}.')

            self._rbd_v1_backup.delay(version_uid=version_uid,
                                      volume=volume_name,
                                      labels=labels,
                                      pool=volume.pool,
                                      image=volume.image,
                                      snapshot=new_snapshot,
                                      monitors=volume.monitors,
                                      user=volume.user,
                                      keyring=volume.keyring,
                                      key=volume.key)
        else:
            # Delete all snapshots except the newest
            for snapshot in benjis_snapshots[:-1]:
                logger.info(f'{volume_name}: Deleting older RBD snapshot {volume.pool}/{volume.image}@{snapshot}.')
                # Don't ignore result, we want exceptions to be delivered
                rbd_v1_snapshot_rm.delay(pool=volume.pool,
                                         image=volume.image,
                                         monitors=volume.monitors,
                                         user=volume.user,
                                         keyring=volume.keyring,
                                         key=volume.key,
                                         snapshot=snapshot).get()

            last_snapshot = benjis_snapshots[-1]
            logger.info(f'{volume_name}: Last RBD snapshot is {volume.pool}/{volume.image}@{last_snapshot}.')

            versions = core_v1_find_versions_with_filter.delay(
                filter_expression=f'volume == "{volume_name}" and snapshot == "{last_snapshot}" and status == "valid"'
            ).get()
            if versions:
                base_version_uid = versions[0]['uid']

                # Don't ignore result, we want exceptions to be delivered
                rbd_v1_snapshot_create.delay(pool=volume.pool,
                                             image=volume.image,
                                             monitors=volume.monitors,
                                             user=volume.user,
                                             keyring=volume.keyring,
                                             key=volume.key,
                                             snapshot=new_snapshot).get()
                logger.info(f'{volume_name}: Newest RBD snapshot is {volume.pool}/{volume.image}@{new_snapshot}.')

                hints = rbd_v1_snapshot_diff.delay(pool=volume.pool,
                                                   image=volume.image,
                                                   monitors=volume.monitors,
                                                   user=volume.user,
                                                   keyring=volume.keyring,
                                                   key=volume.key,
                                                   snapshot=new_snapshot,
                                                   last_snapshot=last_snapshot).get()

                logger.info(f'{volume_name}: Deleting last RBD snapshot {volume.pool}/{volume.image}@{last_snapshot}.')
                # Don't ignore result, we want exceptions to be delivered
                rbd_v1_snapshot_rm.delay(pool=volume.pool,
                                         image=volume.image,
                                         monitors=volume.monitors,
                                         user=volume.user,
                                         keyring=volume.keyring,
                                         key=volume.key,
                                         snapshot=last_snapshot).get()

                self._rbd_v1_backup.delay(version_uid=version_uid,
                                          volume=volume_name,
                                          labels=labels,
                                          pool=volume.pool,
                                          image=volume.image,
                                          snapshot=new_snapshot,
                                          monitors=volume.monitors,
                                          user=volume.user,
                                          keyring=volume.keyring,
                                          key=volume.key,
                                          base_version_uid=base_version_uid,
                                          hints=hints)
            else:
                logger.info(f'{volume_name}: Existing RBD snapshot {volume.pool}/{volume.image}@{last_snapshot} not found in Benji, deleting it and reverting to initial backup.')
                # Don't ignore result, we want exceptions to be delivered
                rbd_v1_snapshot_rm.delay(pool=volume.pool,
                                         image=volume.image,
                                         monitors=volume.monitors,
                                         user=volume.user,
                                         keyring=volume.keyring,
                                         key=volume.key,
                                         snapshot=last_snapshot).get()

                # Don't ignore result, we want exceptions to be delivered
                rbd_v1_snapshot_create.delay(pool=volume.pool,
                                             image=volume.image,
                                             monitors=volume.monitors,
                                             user=volume.user,
                                             keyring=volume.keyring,
                                             key=volume.key,
                                             snapshot=new_snapshot).get()
                logger.info(f'{volume_name}: Newest RBD snapshot is {volume.pool}/{volume.image}@{new_snapshot}.')

                self._rbd_v1_backup.delay(version_uid=version_uid,
                                          volume=volume_name,
                                          labels=labels,
                                          pool=volume.pool,
                                          image=volume.image,
                                          snapshot=new_snapshot,
                                          monitors=volume.monitors,
                                          user=volume.user,
                                          keyring=volume.keyring,
                                          key=volume.key)
