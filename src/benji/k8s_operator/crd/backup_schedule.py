from functools import partial
from typing import Dict, Any, Optional

import kopf
import pykube
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger

from benji.amqp import AMQPRPCClient
from benji.helpers import settings
from benji.k8s_operator import apscheduler, kubernetes_client
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


def backup_scheduler_job(*,
                         namespace_label_selector: str = None,
                         namespace: str = None,
                         label_selector: str,
                         parent_body,
                         logger):
    if namespace_label_selector is not None:
        namespaces = [
            namespace.metadata.name for namespace in pykube.Namespace.objects(kubernetes_client).filter(label_selector=namespace_label_selector)
        ]
    else:
        namespaces = [namespace]

    pvcs = []
    for ns in namespaces:
        pvcs.extend([
            o.obj
            for o in pykube.PersistentVolumeClaim().objects(kubernetes_client).filter(namespace=ns,
                                                                                      label_selector=label_selector)
        ])

    if len(pvcs) == 0:
        logger.warning(f'No PVC matched the selector {label_selector} in namespace(s) {", ".join(namespaces)}.')
        return

    rpc_client = AMQPRPCClient(queue='')
    for pvc in pvcs:
        if 'volumeName' not in pvc['spec'] or pvc['spec']['volumeName'] in (None, ''):
            continue

        version_uid = '{}-{}'.format(f'{pvc.metadata.namespace}-{pvc.metadata.name}'[:246], random_string(6))
        volume = '{}/{}'.format(pvc['metadata']['namespace'], pvc['metadata']['name'])
        pv = pykube.PersistentVolume().objects(kubernetes_client).get_by_name(pvc['spec']['volumeName'])
        pool, image = determine_rbd_image_location(pv)
        version_labels = build_version_labels_rbd(pvc, pv, pool, image)

        rpc_client.call_async('ceph_v1_backup',
                              version_uid=version_uid,
                              volume=volume,
                              pool=pool,
                              image=image,
                              version_labels=version_labels)
    rpc_client.call_async('terminate')

    command = ['benji-api-server', '--queue', rpc_client.queue]
    job = BenjiJob(kubernetes_client, command=command, parent_body=parent_body)
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
    apscheduler.add_job(partial(backup_scheduler_job,
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
        apscheduler.remove_job(job_id=cr_to_job_name(body, 'scheduler'))
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
def benji_backup_schedule_job_gc(name: str, namespace: str):
    pass
