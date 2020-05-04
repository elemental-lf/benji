import re
from base64 import b64decode
from functools import partial
from typing import Dict, Any, Optional, NamedTuple, Sequence

import kopf
import pykube
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger

from benji.celery import RPCClient
from benji.helpers import settings
from benji.k8s_operator import OperatorContext
from benji.k8s_operator.constants import LABEL_PARENT_KIND, API_VERSION, API_GROUP, LABEL_INSTANCE, \
    LABEL_K8S_PVC_NAMESPACE, LABEL_K8S_PVC_NAME, LABEL_K8S_PV_NAME
from benji.k8s_operator.resources import track_job_status, delete_all_dependant_jobs, BenjiJob, APIObject, \
    NamespacedAPIObject, StorageClass
from benji.k8s_operator.utils import cr_to_job_name, random_string, keys_exist

K8S_BACKUP_SCHEDULE_SPEC_SCHEDULE = 'schedule'
K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR = 'persistentVolumeClaimSelector'
K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR_MATCH_LABELS = 'matchLabels'
K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR_MATCH_NAMESPACE_LABELS = 'matchNamespaceLabels'

ROOK_CEPH_MON_ENDPOINTS_CONFIGMAP = 'rook-ceph-mon-endpoints'
ROOK_CEPH_MON_SECRET = 'rook-ceph-mon'


class BenjiBackupSchedule(NamespacedAPIObject):

    version = f'{API_GROUP}/{API_VERSION}'
    endpoint = 'benjibackupschedules'
    kind = 'BenjiBackupSchedule'


class ClusterBenjiBackupSchedule(APIObject):

    version = f'{API_GROUP}/{API_VERSION}'
    endpoint = 'clusterbenjibackupschedules'
    kind = 'ClusterBenjiBackupSchedule'


class _RBDMaterials(NamedTuple):
    pool: str
    image: str
    monitors: Sequence[str]
    user: str
    keyring: str
    key: str


def determine_rbd_materials(pvc: pykube.PersistentVolumeClaim, pv: pykube.PersistentVolume, *,
                            logger) -> Optional[_RBDMaterials]:
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
                logger.warning(f'Unable to determine Ceph credentials for PVC {pvc.namespace}/{pvc.name}/'
                               f'PV {pv.name}, storage class {storage_class_name} does not exist anymore.')
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
                        logger.warning(f'Unable to determine Ceph credentials for PVC {pvc.namespace}/{pvc.name}/'
                                       f'PV {pv.name}, admin secret referenced in storage class {storage_class_name} '
                                       'does not exist')
                    else:
                        admin_secret_obj = admin_secret.obj
                        if keys_exist(admin_secret_obj, ('data.key',)):
                            user = storage_class_obj['parameters']['adminId']
                            monitors = storage_class_obj['parameters']['monitors']
                            key = b64decode(admin_secret_obj['data']['key']).decode('ascii')
                        else:
                            logger.warning(f'Unable to determine Ceph credentials for PVC {pvc.namespace}/{pvc.name}/'
                                           f'PV {pv.name}, admin secret is missing required field data.key.')
                else:
                    logger.warning(f'Unable to determine Ceph credentials for PVC {pvc.namespace}/{pvc.name}/'
                                   f'PV {pv.name}, storage class {storage_class_name} does not look like an RBD backed '
                                   'class.')
    elif keys_exist(pv_obj, ('spec.flexVolume.options', 'spec.flexVolume.driver')):
        options = pv_obj['spec']['flexVolume']['options']
        driver = pv_obj['spec']['flexVolume']['driver']
        if driver.startswith('ceph.rook.io/') and options.get('pool') and options.get('image'):
            pool, image = options['pool'], options['image']
        else:
            logger.warning(f'PV {pv.name} was provisioned by an unknown driver {driver}.')
            return None
    elif keys_exist(pv_obj, ('spec.csi.driver', 'spec.csi.volumeHandle', 'spec.csi.volumeAttributes')):
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

    logger.info(f'PVC {pvc.namespace}/{pvc.name}, PV {pv.name}: image = {image}, pool = {pool}, monitors = {monitors}, keyring set = {keyring is not None}, key set = {key is not None}.')
    return _RBDMaterials(pool=pool, image=image, monitors=monitors, user=user, keyring=keyring, key=key)


def build_version_labels_rbd(*, pvc: pykube.PersistentVolumeClaim, pv: pykube.PersistentVolume, pool: str,
                             image: str) -> Dict[str, str]:
    pvc_obj = pvc.obj
    pv_obj = pv.obj
    version_labels = {
        LABEL_INSTANCE: settings.benji_instance,
        LABEL_K8S_PVC_NAMESPACE: pvc_obj['metadata']['namespace'],
        LABEL_K8S_PVC_NAME: pvc_obj['metadata']['name'],
        LABEL_K8S_PV_NAME: pv_obj['metadata']['name'],
    }

    return version_labels


def backup_scheduler_job(*,
                         namespace_label_selector: str = None,
                         namespace: str = None,
                         label_selector: str,
                         parent_body,
                         logger):
    if namespace_label_selector is not None:
        namespaces = [
            namespace.metadata.name for namespace in pykube.Namespace.objects(OperatorContext.kubernetes_client).filter(selector=namespace_label_selector)
        ]
    else:
        namespaces = [namespace]

    pvcs = []
    for ns in namespaces:
        pvcs.extend(
            pykube.PersistentVolumeClaim.objects(OperatorContext.kubernetes_client).filter(namespace=ns,
                                                                                           selector=label_selector))

    if not pvcs:
        logger.warning(f'No PVC matched the selector {label_selector} in namespace(s) {", ".join(namespaces)}.')
        return

    pvc_pv_pairs = []
    for pvc in pvcs:
        pvc_obj = pvc.obj
        if 'volumeName' not in pvc_obj['spec'] or pvc_obj['spec']['volumeName'] in (None, ''):
            continue

        try:
            pv = pykube.PersistentVolume.objects(OperatorContext.kubernetes_client).get_by_name(
                pvc_obj['spec']['volumeName'])
        except pykube.exceptions.ObjectDoesNotExist:
            pvc_name = '{}/{}'.format(pvc_obj['metadata']['namespace'], pvc_obj['metadata']['name'])
            logger.warning(f'PVC {pvc_name} is currently not bound to any PV, skipping.')

        pvc_pv_pairs.append((pvc, pv))

    if not pvc_pv_pairs:
        logger.warning(f'All PVCs matched by the selector {label_selector} in namespace(s) {", ".join(namespaces)} are currently unbound.')
        return

    with RPCClient(auto_queue=True) as rpc_client:
        rpc_calls = 0
        for pvc, pv in pvc_pv_pairs:
            pvc_obj = pvc.obj

            pvc_name = '{}/{}'.format(pvc_obj['metadata']['namespace'], pvc_obj['metadata']['name'])
            version_uid = '{}-{}'.format(pvc_name[:246], random_string(6))

            rbd_materials = determine_rbd_materials(pvc, pv, logger=logger)
            if rbd_materials is None:
                continue

            version_labels = build_version_labels_rbd(pvc=pvc,
                                                      pv=pv,
                                                      pool=rbd_materials.pool,
                                                      image=rbd_materials.image)

            rpc_client.call_async('ceph_v1_backup',
                                  version_uid=version_uid,
                                  volume=pvc_name,
                                  pool=rbd_materials.pool,
                                  image=rbd_materials.image,
                                  monitors=rbd_materials.monitors,
                                  user=rbd_materials.user,
                                  keyring=rbd_materials.keyring,
                                  key=rbd_materials.key,
                                  version_labels=version_labels,
                                  ignore_result=True)
            rpc_calls += 1

        if rpc_calls > 0:
            rpc_client.call_async('terminate', ignore_result=True)

            command = ['benji-api-server', rpc_client.queue]
            job = BenjiJob(OperatorContext.kubernetes_client, command=command, parent_body=parent_body)
            job.create()


@kopf.on.resume(*BenjiBackupSchedule.group_version_plural())
@kopf.on.create(*BenjiBackupSchedule.group_version_plural())
@kopf.on.update(*BenjiBackupSchedule.group_version_plural())
@kopf.on.resume(*ClusterBenjiBackupSchedule.group_version_plural())
@kopf.on.create(*ClusterBenjiBackupSchedule.group_version_plural())
@kopf.on.update(*ClusterBenjiBackupSchedule.group_version_plural())
def benji_backup_schedule(namespace: str, spec: Dict[str, Any], body: Dict[str, Any], logger,
                          **_) -> Optional[Dict[str, Any]]:
    schedule = spec[K8S_BACKUP_SCHEDULE_SPEC_SCHEDULE]
    label_selector = spec[K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR].get(
        K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR_MATCH_LABELS, None)
    namespace_label_selector = None
    if body['kind'] == BenjiBackupSchedule.kind:
        namespace_label_selector = spec[K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR].get(
            K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR_MATCH_NAMESPACE_LABELS, None)

    job_name = cr_to_job_name(body, 'scheduler')
    OperatorContext.apscheduler.add_job(partial(backup_scheduler_job,
                                                namespace_label_selector=namespace_label_selector,
                                                namespace=namespace,
                                                label_selector=label_selector,
                                                parent_body=body,
                                                logger=logger),
                                        CronTrigger.from_crontab(schedule),
                                        name=job_name,
                                        id=job_name,
                                        replace_existing=True)


@kopf.on.delete(*BenjiBackupSchedule.group_version_plural())
@kopf.on.delete(*ClusterBenjiBackupSchedule.group_version_plural())
def benji_backup_schedule_delete(name: str, namespace: str, body: Dict[str, Any], logger,
                                 **_) -> Optional[Dict[str, Any]]:
    try:
        OperatorContext.apscheduler.remove_job(job_id=cr_to_job_name(body, 'scheduler'))
    except JobLookupError:
        pass
    delete_all_dependant_jobs(name=name, namespace=namespace, kind=body['kind'], logger=logger)


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiBackupSchedule.kind})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiBackupSchedule.kind})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiBackupSchedule.kind})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: BenjiBackupSchedule.kind})
def benji_track_job_status_backup_schedule(**kwargs) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=BenjiBackupSchedule, **kwargs)


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: ClusterBenjiBackupSchedule.kind})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: ClusterBenjiBackupSchedule.kind})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: ClusterBenjiBackupSchedule.kind})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: ClusterBenjiBackupSchedule.kind})
def benji_track_job_status_cluster_backup_schedule(**kwargs) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=ClusterBenjiBackupSchedule, **kwargs)


@kopf.timer(*BenjiBackupSchedule.group_version_plural(), initial_delay=60, interval=60)
@kopf.timer(*ClusterBenjiBackupSchedule.group_version_plural(), initial_delay=60, interval=60)
def benji_backup_schedule_job_gc(name: str, namespace: str, **_):
    pass
