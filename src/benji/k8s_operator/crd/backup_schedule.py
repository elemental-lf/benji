import re
from base64 import b64decode
from functools import partial
from typing import Dict, Any, Optional, NamedTuple, Sequence

import kopf
import pykube
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger
from benji.k8s_operator.volume_driver.registry import VolumeDriverRegistry

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


def build_version_labels_rbd(*, pvc: pykube.PersistentVolumeClaim, pv: pykube.PersistentVolume) -> Dict[str, str]:
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

    for pvc, pv in pvc_pv_pairs:
        backup_handler = VolumeDriverRegistry.handle(pvc=pvc, pv=pv, logger=logger)
        if backup_handler is not None:
            backup_handler.backup()
        else:
            logger.error(f'Backup requested for PVC {pvc.namespace}/{pvc.name} but the kind of volume is unknown, '
                         'no backup will be performed.')


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
