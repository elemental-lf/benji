from functools import partial
from typing import Dict, Any, Optional

import kopf
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger

import benji.k8s_operator
from benji.helpers.constants import LABEL_INSTANCE, LABEL_K8S_PVC_NAMESPACE
from benji.helpers.settings import benji_instance
from benji.k8s_operator.constants import CRD_RETENTION_SCHEDULE, CRD_CLUSTER_RETENTION_SCHEDULE, LABEL_PARENT_KIND
from benji.k8s_operator.resources import create_job, track_job_status, delete_dependant_jobs
from benji.k8s_operator.utils import cr_to_job_name


def enforce_scheduler_job(*, retention_rule: str, match_versions: str, parent_body, logger):
    command = ['benji-command', 'enforce', retention_rule, match_versions]
    create_job(command, parent_body=parent_body, logger=logger)


@kopf.on.resume(CRD_RETENTION_SCHEDULE.api_group, CRD_RETENTION_SCHEDULE.api_version, CRD_RETENTION_SCHEDULE.plural)
@kopf.on.create(CRD_RETENTION_SCHEDULE.api_group, CRD_RETENTION_SCHEDULE.api_version, CRD_RETENTION_SCHEDULE.plural)
@kopf.on.update(CRD_RETENTION_SCHEDULE.api_group, CRD_RETENTION_SCHEDULE.api_version, CRD_RETENTION_SCHEDULE.plural)
@kopf.on.resume(CRD_CLUSTER_RETENTION_SCHEDULE.api_group, CRD_CLUSTER_RETENTION_SCHEDULE.api_version,
                CRD_CLUSTER_RETENTION_SCHEDULE.plural)
@kopf.on.create(CRD_CLUSTER_RETENTION_SCHEDULE.api_group, CRD_CLUSTER_RETENTION_SCHEDULE.api_version,
                CRD_CLUSTER_RETENTION_SCHEDULE.plural)
@kopf.on.update(CRD_CLUSTER_RETENTION_SCHEDULE.api_group, CRD_CLUSTER_RETENTION_SCHEDULE.api_version,
                CRD_CLUSTER_RETENTION_SCHEDULE.plural)
def benji_retention_schedule(namespace: str, spec: Dict[str, Any], body: Dict[str, Any], logger,
                             **_) -> Optional[Dict[str, Any]]:
    schedule = spec['schedule']
    retention_rule = spec['retentionRule']

    instance_filter = f'labels["{LABEL_INSTANCE}"] == "{benji_instance}"'
    match_versions = spec.get('matchVersions', None)
    if match_versions is not None:
        match_versions = f'({match_versions}) and {instance_filter}'
    else:
        match_versions = instance_filter

    if body['kind'] == CRD_RETENTION_SCHEDULE.name:
        match_versions = f'{match_versions} and labels["{LABEL_K8S_PVC_NAMESPACE}"] == "{namespace}"'

    job_name = cr_to_job_name(body, 'scheduler')
    benji.k8s_operator.scheduler.add_job(partial(enforce_scheduler_job,
                                                 retention_rule=retention_rule,
                                                 match_versions=match_versions,
                                                 parent_body=body,
                                                 logger=logger),
                                         CronTrigger.from_crontab(schedule),
                                         name=job_name,
                                         id=job_name,
                                         replace_existing=True)


@kopf.on.delete(CRD_RETENTION_SCHEDULE.api_group, CRD_RETENTION_SCHEDULE.api_version, CRD_RETENTION_SCHEDULE.plural)
@kopf.on.delete(CRD_CLUSTER_RETENTION_SCHEDULE.api_group, CRD_CLUSTER_RETENTION_SCHEDULE.api_version,
                CRD_CLUSTER_RETENTION_SCHEDULE.plural)
def benji_retention_schedule_delete(name: str, namespace: str, body: Dict[str, Any], logger,
                                    **_) -> Optional[Dict[str, Any]]:
    try:
        benji.k8s_operator.scheduler.remove_job(job_id=cr_to_job_name(body, 'scheduler'))
    except JobLookupError:
        pass
    delete_dependant_jobs(name=name, namespace=namespace, kind=body['kind'], logger=logger)


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_RETENTION_SCHEDULE.name})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_RETENTION_SCHEDULE.name})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_RETENTION_SCHEDULE.name})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: CRD_RETENTION_SCHEDULE.name})
def benji_track_job_status_retention_schedule(**_) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=CRD_RETENTION_SCHEDULE, **_)


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_CLUSTER_RETENTION_SCHEDULE.name})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_CLUSTER_RETENTION_SCHEDULE.name})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_CLUSTER_RETENTION_SCHEDULE.name})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: CRD_CLUSTER_RETENTION_SCHEDULE.name})
def benji_track_job_status_cluster_retention_schedule(**_) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=CRD_CLUSTER_RETENTION_SCHEDULE, **_)
