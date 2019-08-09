from typing import Dict, Any, Optional

import kopf

from benji.k8s_operator.constants import CRD_BACKUP_SCHEDULE, CRD_CLUSTER_BACKUP_SCHEDULE, LABEL_PARENT_KIND, \
    RESOURCE_STATUS_CHILD_CHANGED
from benji.k8s_operator.resources import get_cron_jobs, delete_cron_job, create_cron_job
from benji.k8s_operator.status import build_resource_status_children, track_job_status, \
    track_cron_job_status
from benji.k8s_operator.utils import get_caller_name


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
def benji_backup_schedule(reason: str, name: str, namespace: str, spec: Dict[str, Any], status: Dict[str, Any],
                          body: Dict[str, Any], patch: Dict[str, Any], logger, **kwargs) -> Optional[Dict[str, Any]]:
    # We recreate all CronJobs on resume because we don't know if the resource or the CronJob template has been changed
    # in the meantime.
    cron_jobs = get_cron_jobs(body)
    if cron_jobs:
        for cron_job in cron_jobs:
            delete_cron_job(cron_job.metadata.name, cron_job.metadata.namespace, logger=logger)
    elif reason != 'create':
        if namespace:
            logger.warning(f'{body["kind"]} {namespace}/{name} had no corresponding CronJob on {reason}.')
        else:
            logger.warning(f'{body["kind"]} {name} had no corresponding CronJob on {reason}.')

    schedule = spec['schedule']

    command = ['benji-backup-pvc']

    label_selector = spec['persistentVolumeClaimSelector'].get('matchLabels', None)
    if label_selector is not None:
        command.extend(['--selector', label_selector])

    if body['kind'] == CRD_BACKUP_SCHEDULE.name:
        namespace_label_selector = spec['persistentVolumeClaimSelector'].get('matchNamespaceLabels', None)
        if namespace_label_selector is not None:
            command.extend(['--namespace-selector', namespace_label_selector])
    else:
        command.extend(['--namespace'], namespace)

    cron_job = create_cron_job(command, schedule, parent_body=body, logger=logger)
    patch['status'] = build_resource_status_children(status, cron_job, get_caller_name())


@kopf.on.delete(CRD_BACKUP_SCHEDULE.api_group, CRD_BACKUP_SCHEDULE.api_version, CRD_BACKUP_SCHEDULE.plural)
@kopf.on.delete(CRD_CLUSTER_BACKUP_SCHEDULE.api_group, CRD_CLUSTER_BACKUP_SCHEDULE.api_version,
                CRD_CLUSTER_BACKUP_SCHEDULE.plural)
def benji_backup_schedule_delete(reason: str, name: str, namespace: str, body: Dict[str, Any], logger,
                                 **kwargs) -> Optional[Dict[str, Any]]:
    cron_jobs = get_cron_jobs(body)
    if cron_jobs:
        for cron_job in cron_jobs:
            delete_cron_job(cron_job.metadata.name, cron_job.metadata.namespace, logger=logger)
    else:
        if namespace:
            logger.warning(f'{body["kind"]} {namespace}/{name} had no corresponding CronJob on {reason}.')
        else:
            logger.warning(f'{body["kind"]} {name} had no corresponding CronJob on {reason}.')


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_BACKUP_SCHEDULE.name})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_BACKUP_SCHEDULE.name})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_BACKUP_SCHEDULE.name})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: CRD_BACKUP_SCHEDULE.name})
def benji_track_job_status_backup_schedule(**kwargs) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=CRD_BACKUP_SCHEDULE, **kwargs)


@kopf.on.update('batch', 'v1beta1', 'cronjobs', labels={LABEL_PARENT_KIND: CRD_BACKUP_SCHEDULE.name})
@kopf.on.delete('batch', 'v1beta1', 'cronjobs', labels={LABEL_PARENT_KIND: CRD_BACKUP_SCHEDULE.name})
def benji_track_cronjob_status_backup_schedule(**kwargs) -> Optional[Dict[str, Any]]:
    return track_cron_job_status(crd=CRD_BACKUP_SCHEDULE, **kwargs)


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_CLUSTER_BACKUP_SCHEDULE.name})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_CLUSTER_BACKUP_SCHEDULE.name})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_CLUSTER_BACKUP_SCHEDULE.name})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: CRD_CLUSTER_BACKUP_SCHEDULE.name})
def benji_track_job_status_cluster_backup_schedule(**kwargs) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=CRD_CLUSTER_BACKUP_SCHEDULE, **kwargs)


@kopf.on.update('batch', 'v1beta1', 'cronjobs', labels={LABEL_PARENT_KIND: CRD_CLUSTER_BACKUP_SCHEDULE.name})
@kopf.on.delete('batch', 'v1beta1', 'cronjobs', labels={LABEL_PARENT_KIND: CRD_CLUSTER_BACKUP_SCHEDULE.name})
def benji_track_cronjob_status_cluster_backup_schedule(**kwargs) -> Optional[Dict[str, Any]]:
    return track_cron_job_status(crd=CRD_CLUSTER_BACKUP_SCHEDULE, **kwargs)
