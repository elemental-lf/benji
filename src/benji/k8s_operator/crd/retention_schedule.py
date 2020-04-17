from functools import partial
from typing import Dict, Any, Optional

import kopf
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger

from benji.helpers.settings import benji_instance
from benji.k8s_operator import kubernetes_client, apscheduler
from benji.k8s_operator.constants import LABEL_PARENT_KIND, API_GROUP, API_VERSION, LABEL_INSTANCE, \
    LABEL_K8S_PVC_NAMESPACE
from benji.k8s_operator.resources import track_job_status, delete_all_dependant_jobs, BenjiJob, NamespacedAPIObject
from benji.k8s_operator.utils import cr_to_job_name

K8S_RESTORE_SPEC_SCHEDULE = 'schedule'
K8S_RESTORE_SPEC_RETENTION_RULE = 'retentionRule'
K8S_RESTORE_SPEC_MATCH_VERSIONS = 'matchVersions'


class BenjiRetentionSchedule(NamespacedAPIObject):

    version = f'{API_GROUP}/{API_VERSION}'
    endpoint = 'benjiretentionschedules'
    kind = 'BenjiRetentionSchedule'


class ClusterBenjiRetentionSchedule(NamespacedAPIObject):

    version = f'{API_GROUP}/{API_VERSION}'
    endpoint = 'clusterbenjiretentionschedules'
    kind = 'ClusterBenjiRetentionSchedule'


def enforce_scheduler_job(*, retention_rule: str, match_versions: str, parent_body, logger):
    command = ['benji-command', 'enforce', retention_rule, match_versions]
    job = BenjiJob(kubernetes_client, command=command, parent_body=parent_body)
    job.create()


@kopf.on.resume(*BenjiRetentionSchedule.group_version_plural())
@kopf.on.create(*BenjiRetentionSchedule.group_version_plural())
@kopf.on.update(*BenjiRetentionSchedule.group_version_plural())
@kopf.on.resume(*ClusterBenjiRetentionSchedule.group_version_plural())
@kopf.on.create(*ClusterBenjiRetentionSchedule.group_version_plural())
@kopf.on.update(*ClusterBenjiRetentionSchedule.group_version_plural())
def benji_retention_schedule(namespace: str, spec: Dict[str, Any], body: Dict[str, Any], logger,
                             **_) -> Optional[Dict[str, Any]]:
    schedule = spec[K8S_RESTORE_SPEC_SCHEDULE]
    retention_rule = spec[K8S_RESTORE_SPEC_RETENTION_RULE]

    instance_filter = f'labels["{LABEL_INSTANCE}"] == "{benji_instance}"'
    match_versions = spec.get(K8S_RESTORE_SPEC_MATCH_VERSIONS, None)
    if match_versions is not None:
        match_versions = f'({match_versions}) and {instance_filter}'
    else:
        match_versions = instance_filter

    if body['kind'] == BenjiRetentionSchedule.kind:
        match_versions = f'{match_versions} and labels["{LABEL_K8S_PVC_NAMESPACE}"] == "{namespace}"'

    job_name = cr_to_job_name(body, 'scheduler')
    apscheduler.add_job(partial(enforce_scheduler_job,
                                retention_rule=retention_rule,
                                match_versions=match_versions,
                                parent_body=body,
                                logger=logger),
                        CronTrigger.from_crontab(schedule),
                        name=job_name,
                        id=job_name,
                        replace_existing=True)


@kopf.on.delete(*BenjiRetentionSchedule.group_version_plural())
@kopf.on.delete(*ClusterBenjiRetentionSchedule.group_version_plural())
def benji_retention_schedule_delete(name: str, namespace: str, body: Dict[str, Any], logger,
                                    **_) -> Optional[Dict[str, Any]]:
    try:
        apscheduler.remove_job(job_id=cr_to_job_name(body, 'scheduler'))
    except JobLookupError:
        pass
    delete_all_dependant_jobs(name=name, namespace=namespace, kind=body['kind'], logger=logger)


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiRetentionSchedule.kind})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiRetentionSchedule.kind})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiRetentionSchedule.kind})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: BenjiRetentionSchedule.kind})
def benji_track_job_status_retention_schedule(**kwargs) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=BenjiRetentionSchedule, **kwargs)


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: ClusterBenjiRetentionSchedule.kind})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: ClusterBenjiRetentionSchedule.kind})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: ClusterBenjiRetentionSchedule.kind})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: ClusterBenjiRetentionSchedule.kind})
def benji_track_job_status_cluster_retention_schedule(**kwargs) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=ClusterBenjiRetentionSchedule, **kwargs)
