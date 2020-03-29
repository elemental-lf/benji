from functools import partial
from typing import Dict, Any, Optional

import kopf
import kubernetes
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger

import benji.k8s_operator
from benji.helpers.kubernetes import list_namespaces
from benji.k8s_operator.constants import CRD_BACKUP_SCHEDULE, CRD_CLUSTER_BACKUP_SCHEDULE, LABEL_PARENT_KIND, \
    RESOURCE_STATUS_CHILD_CHANGED
from benji.k8s_operator.resources import create_job, track_job_status, delete_dependant_jobs
from benji.k8s_operator.utils import cr_to_job_name


def backup_scheduler_job(*,
                         namespace_label_selector: str = None,
                         namespace: str = None,
                         label_selector: str,
                         parent_body,
                         logger):
    if namespace_label_selector is not None:
        namespaces = [namespace.metadata.name for namespace in list_namespaces(label_selector=namespace_label_selector)]
    else:
        namespaces = [namespace]

    core_v1_api = kubernetes.client.CoreV1Api()
    pvcs = []
    for ns in namespaces:
        pvcs.extend(
            core_v1_api.list_namespaced_persistent_volume_claim(namespace=ns, label_selector=label_selector).items)

    if len(pvcs) == 0:
        logger.warning(f'No PVC matched the selector {label_selector} in namespace(s) {", ".join(namespaces)}.')
        return

    for pvc in pvcs:
        if not hasattr(pvc.spec, 'volume_name') or pvc.spec.volume_name in (None, ''):
            continue

        command = ['benji-backup-pvc', namespace, pvc.metadata.name]
        create_job(command, parent_body=parent_body, logger=logger)


@kopf.on.resume(CRD_BACKUP_SCHEDULE.api_group, CRD_BACKUP_SCHEDULE.api_version, CRD_BACKUP_SCHEDULE.plural)
@kopf.on.create(CRD_BACKUP_SCHEDULE.api_group, CRD_BACKUP_SCHEDULE.api_version, CRD_BACKUP_SCHEDULE.plural)
@kopf.on.update(CRD_BACKUP_SCHEDULE.api_group, CRD_BACKUP_SCHEDULE.api_version, CRD_BACKUP_SCHEDULE.plural)
@kopf.on.field(CRD_BACKUP_SCHEDULE.api_group,
               CRD_BACKUP_SCHEDULE.api_version,
               CRD_BACKUP_SCHEDULE.plural,
               field=f'status.{RESOURCE_STATUS_CHILD_CHANGED}')
@kopf.on.resume(CRD_CLUSTER_BACKUP_SCHEDULE.api_group, CRD_CLUSTER_BACKUP_SCHEDULE.api_version,
                CRD_CLUSTER_BACKUP_SCHEDULE.plural)
@kopf.on.create(CRD_CLUSTER_BACKUP_SCHEDULE.api_group, CRD_CLUSTER_BACKUP_SCHEDULE.api_version,
                CRD_CLUSTER_BACKUP_SCHEDULE.plural)
@kopf.on.update(CRD_CLUSTER_BACKUP_SCHEDULE.api_group, CRD_CLUSTER_BACKUP_SCHEDULE.api_version,
                CRD_CLUSTER_BACKUP_SCHEDULE.plural)
@kopf.on.field(CRD_CLUSTER_BACKUP_SCHEDULE.api_group,
               CRD_CLUSTER_BACKUP_SCHEDULE.api_version,
               CRD_CLUSTER_BACKUP_SCHEDULE.plural,
               field=f'status.{RESOURCE_STATUS_CHILD_CHANGED}')
def benji_backup_schedule(namespace: str, spec: Dict[str, Any], body: Dict[str, Any], logger,
                          **_) -> Optional[Dict[str, Any]]:
    schedule = spec['schedule']
    label_selector = spec['persistentVolumeClaimSelector'].get('matchLabels', None)
    namespace_label_selector = None
    if body['kind'] == CRD_BACKUP_SCHEDULE.name:
        namespace_label_selector = spec['persistentVolumeClaimSelector'].get('matchNamespaceLabels', None)

    job_name = cr_to_job_name(body, 'scheduler')
    benji.k8s_operator.scheduler.add_job(partial(backup_scheduler_job,
                                                 namespace_label_selector=namespace_label_selector,
                                                 namespace=namespace,
                                                 label_selector=label_selector,
                                                 parent_body=body,
                                                 logger=logger),
                                         CronTrigger.from_crontab(schedule),
                                         name=job_name,
                                         id=job_name,
                                         replace_existing=True)


@kopf.on.delete(CRD_BACKUP_SCHEDULE.api_group, CRD_BACKUP_SCHEDULE.api_version, CRD_BACKUP_SCHEDULE.plural)
@kopf.on.delete(CRD_CLUSTER_BACKUP_SCHEDULE.api_group, CRD_CLUSTER_BACKUP_SCHEDULE.api_version,
                CRD_CLUSTER_BACKUP_SCHEDULE.plural)
def benji_backup_schedule_delete(name: str, namespace: str, body: Dict[str, Any], logger,
                                 **_) -> Optional[Dict[str, Any]]:
    try:
        benji.k8s_operator.scheduler.remove_job(job_id=cr_to_job_name(body, 'scheduler'))
    except JobLookupError:
        pass
    delete_dependant_jobs(name=name, namespace=namespace, kind=body['kind'], logger=logger)


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_BACKUP_SCHEDULE.name})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_BACKUP_SCHEDULE.name})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_BACKUP_SCHEDULE.name})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: CRD_BACKUP_SCHEDULE.name})
def benji_track_job_status_backup_schedule(**_) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=CRD_BACKUP_SCHEDULE, **_)


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_CLUSTER_BACKUP_SCHEDULE.name})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_CLUSTER_BACKUP_SCHEDULE.name})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_CLUSTER_BACKUP_SCHEDULE.name})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: CRD_CLUSTER_BACKUP_SCHEDULE.name})
def benji_track_job_status_cluster_backup_schedule(**_) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=CRD_CLUSTER_BACKUP_SCHEDULE, **_)
