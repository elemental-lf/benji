from functools import partial
from typing import Dict, Any, Optional

import kopf
import pykube
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger

from benji.amqp import AMQPRPCClient
from benji.helpers import settings
from benji.k8s_operator import OperatorContext
from benji.k8s_operator.constants import LABEL_PARENT_KIND, API_VERSION, API_GROUP, LABEL_INSTANCE, \
    LABEL_K8S_PVC_NAMESPACE, LABEL_K8S_PVC_NAME, LABEL_K8S_PV_NAME, LABEL_K8S_PV_TYPE, PV_TYPE_RBD, LABEL_RBD_IMAGE_SPEC
from benji.k8s_operator.resources import track_job_status, delete_all_dependant_jobs, BenjiJob, APIObject, \
    NamespacedAPIObject
from benji.k8s_operator.utils import cr_to_job_name, random_string, keys_exist

K8S_BACKUP_SCHEDULE_SPEC_SCHEDULE = 'schedule'
K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR = 'persistentVolumeClaimSelector'
K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR_MATCH_LABELS = 'matchLabels'
K8S_BACKUP_SCHEDULE_SPEC_PERSISTENT_VOLUME_CLAIM_SELECTOR_MATCH_NAMESPACE_LABELS = 'matchNamespaceLabels'


class BenjiBackupSchedule(NamespacedAPIObject):

    version = f'{API_GROUP}/{API_VERSION}'
    endpoint = 'benjibackupschedules'
    kind = 'BenjiBackupSchedule'


class ClusterBenjiBackupSchedule(APIObject):

    version = f'{API_GROUP}/{API_VERSION}'
    endpoint = 'clusterbenjibackupschedules'
    kind = 'ClusterBenjiBackupSchedule'


def determine_rbd_image_location(pv: pykube.PersistentVolume, *, logger) -> (str, str):
    pv_obj = pv.obj
    pool, image = None, None

    pv_name = pv_obj['metadata']['name']
    if keys_exist(pv_obj['spec'], ('rbd.pool', 'rbd.image')):
        logger.debug(f'Considering PersistentVolume {pv_name} as a native Ceph RBD volume.')
        pool, image = pv_obj['spec']['rbd']['pool'], pv_obj['spec']['rbd']['image']
    elif keys_exist(pv_obj['spec'], ('flexVolume.options', 'flexVolume.driver')):
        logger.debug(f'Considering PersistentVolume {pv_name} as a Rook Ceph FlexVolume volume.')
        options = pv_obj['spec']['flexVolume']['options']
        driver = pv_obj['spec']['flexVolume']['driver']
        if driver.startswith('ceph.rook.io/') and options.get('pool') and options.get('image'):
            pool, image = options['pool'], options['image']
        else:
            logger.debug(f'PersistentVolume {pv_name} was provisioned by unknown driver {driver}.')
    elif keys_exist(pv_obj['spec'], ('csi.driver', 'csi.volumeHandle', 'csi.volumeAttributes')):
        logger.debug(f'Considering PersistentVolume {pv_name} as a Rook Ceph CSI volume.')
        driver = pv_obj['spec']['csi']['driver']
        volume_handle = pv_obj['spec']['csi']['volumeHandle']
        if driver.endswith('.rbd.csi.ceph.com') and 'pool' in pv_obj['spec']['csi']['volumeAttributes']:
            pool = pv_obj['spec']['csi']['volumeAttributes']['pool']
            image_ids = volume_handle.split('-')
            if len(image_ids) >= 9:
                image = 'csi-vol-' + '-'.join(image_ids[len(image_ids) - 5:])
            else:
                logger.warning(f'PersistentVolume {pv_name} was provisioned by Rook Ceph CSI, but we do not understand the volumeHandle format: {volume_handle}')

    return pool, image


def build_version_labels_rbd(*, pvc: pykube.PersistentVolumeClaim, pv: pykube.PersistentVolume, pool: str,
                             image: str) -> Dict[str, str]:
    pvc_obj = pvc.obj
    pv_obj = pv.obj
    version_labels = {
        LABEL_INSTANCE: settings.benji_instance,
        LABEL_K8S_PVC_NAMESPACE: pvc_obj['metadata']['namespace'],
        LABEL_K8S_PVC_NAME: pvc_obj['metadata']['name'],
        LABEL_K8S_PV_NAME: pv_obj['metadata']['name'],
        # RBD specific
        LABEL_K8S_PV_TYPE: PV_TYPE_RBD,
        LABEL_RBD_IMAGE_SPEC: f'{pool}/{image}',
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

    with AMQPRPCClient(queue='') as rpc_client:
        rpc_calls = 0
        for pvc, pv in pvc_pv_pairs:
            pvc_obj = pvc.obj

            pvc_name = '{}/{}'.format(pvc_obj['metadata']['namespace'], pvc_obj['metadata']['name'])
            version_uid = '{}-{}'.format(pvc_name[:246], random_string(6))

            pool, image = determine_rbd_image_location(pv, logger=logger)
            if pool is None or image is None:
                logger.warning(f'Unable to determine RBD pool and image location for PVC {pvc_name}, maybe it is not backed by RBD.')
                continue

            version_labels = build_version_labels_rbd(pvc=pvc, pv=pv, pool=pool, image=image)

            rpc_client.call_async('ceph_v1_backup',
                                  version_uid=version_uid,
                                  volume=pvc_name,
                                  pool=pool,
                                  image=image,
                                  version_labels=version_labels,
                                  one_way=True)
            rpc_calls += 1

        if rpc_calls > 0:
            rpc_client.call_async('terminate', one_way=True)

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
