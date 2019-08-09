from typing import Dict, Any, Optional

import kopf

from benji.helpers.constants import LABEL_INSTANCE, LABEL_K8S_PVC_NAMESPACE
from benji.helpers.settings import benji_instance
from benji.k8s_operator.constants import CRD_RETENTION_SCHEDULE, CRD_CLUSTER_RETENTION_SCHEDULE, LABEL_PARENT_KIND
from benji.k8s_operator.resources import get_cron_jobs, delete_cron_job, create_cron_job
from benji.k8s_operator.status import build_resource_status_children, track_job_status
from benji.k8s_operator.utils import get_caller_name


@kopf.on.resume(CRD_RETENTION_SCHEDULE.api_group, CRD_RETENTION_SCHEDULE.api_version, CRD_RETENTION_SCHEDULE.plural)
@kopf.on.create(CRD_RETENTION_SCHEDULE.api_group, CRD_RETENTION_SCHEDULE.api_version, CRD_RETENTION_SCHEDULE.plural)
@kopf.on.update(CRD_RETENTION_SCHEDULE.api_group, CRD_RETENTION_SCHEDULE.api_version, CRD_RETENTION_SCHEDULE.plural)
@kopf.on.resume(CRD_CLUSTER_RETENTION_SCHEDULE.api_group, CRD_CLUSTER_RETENTION_SCHEDULE.api_version,
                CRD_CLUSTER_RETENTION_SCHEDULE.plural)
@kopf.on.create(CRD_CLUSTER_RETENTION_SCHEDULE.api_group, CRD_CLUSTER_RETENTION_SCHEDULE.api_version,
                CRD_CLUSTER_RETENTION_SCHEDULE.plural)
@kopf.on.update(CRD_CLUSTER_RETENTION_SCHEDULE.api_group, CRD_CLUSTER_RETENTION_SCHEDULE.api_version,
                CRD_CLUSTER_RETENTION_SCHEDULE.plural)
def benji_retention_schedule(reason: str, name: str, namespace: str, spec: Dict[str, Any], status: Dict[str, Any],
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

    command = ['benji-command', 'enforce']

    retention_rule = spec['retentionRule']
    command.append(retention_rule)

    instance_filter = f'labels["{LABEL_INSTANCE}"] == "{benji_instance}"'
    match_versions = spec.get('matchVersions', None)
    if match_versions is not None:
        match_versions = f'({match_versions}) and {instance_filter}'
    else:
        match_versions = instance_filter

    if body['kind'] == CRD_RETENTION_SCHEDULE.name:
        match_versions = f'{match_versions} and labels["{LABEL_K8S_PVC_NAMESPACE}"] == "{namespace}"'

    command.append(match_versions)

    cron_job = create_cron_job(command, schedule, parent_body=body, logger=logger)
    patch['status'] = build_resource_status_children(status, cron_job, get_caller_name())


@kopf.on.delete(CRD_RETENTION_SCHEDULE.api_group, CRD_RETENTION_SCHEDULE.api_version, CRD_RETENTION_SCHEDULE.plural)
@kopf.on.delete(CRD_CLUSTER_RETENTION_SCHEDULE.api_group, CRD_CLUSTER_RETENTION_SCHEDULE.api_version,
                CRD_CLUSTER_RETENTION_SCHEDULE.plural)
def benji_retention_schedule_delete(reason: str, name: str, namespace: str, body: Dict[str, Any], logger,
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


# dd
@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_RETENTION_SCHEDULE.name})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_RETENTION_SCHEDULE.name})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_RETENTION_SCHEDULE.name})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: CRD_RETENTION_SCHEDULE.name})
def benji_track_job_status_retention_schedule(**kwargs) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=CRD_RETENTION_SCHEDULE, **kwargs)


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_CLUSTER_RETENTION_SCHEDULE.name})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_CLUSTER_RETENTION_SCHEDULE.name})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_CLUSTER_RETENTION_SCHEDULE.name})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: CRD_CLUSTER_RETENTION_SCHEDULE.name})
def benji_track_job_status_cluster_retention_schedule(**kwargs) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=CRD_CLUSTER_RETENTION_SCHEDULE, **kwargs)
