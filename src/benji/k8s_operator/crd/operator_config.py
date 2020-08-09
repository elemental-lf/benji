import logging
from contextlib import suppress
from typing import Optional, Dict, Any

import kopf
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger

from benji.rpc.client import RPCClient
from benji.k8s_operator import OperatorContext
from benji.k8s_operator.constants import LABEL_PARENT_KIND, API_VERSION, API_GROUP, LABEL_INSTANCE
from benji.k8s_operator.crd.version import BenjiVersion
from benji.k8s_operator.resources import track_job_status, BenjiJob, NamespacedAPIObject
from benji.k8s_operator.settings import benji_instance
from benji.k8s_operator.utils import service_account_namespace

SCHED_VERSION_RECONCILIATION_JOB = 'version-reconciliation'
SCHED_CLEANUP_JOB = 'cleanup'

core_v1_find_versions_with_filter = RPCClient.signature('core.v1.find_versions_with_filter')

module_logger = logging.getLogger(__name__)


class BenjiOperatorConfig(NamespacedAPIObject):

    version = f'{API_GROUP}/{API_VERSION}'
    endpoint = 'benjioperatorconfigs'
    kind = 'BenjiOperatorConfig'


def set_operator_config() -> None:
    OperatorContext.operator_config = BenjiOperatorConfig.objects(OperatorContext.kubernetes_client).filter(
        namespace=service_account_namespace()).get_by_name(OperatorContext.operator_config_name)


def reconciliate_versions_job():
    module_logger.debug(f'Finding versions with filter labels["{LABEL_INSTANCE}"] == "{benji_instance}".')
    with RPCClient() as rpc_client:
        versions = core_v1_find_versions_with_filter.delay(
            filter_expression=f'labels["{LABEL_INSTANCE}"] == "{benji_instance}"').get()
    module_logger.debug(f"Number of matching versions in the database: {len(versions)}.")

    versions_seen = set()
    for version in versions:
        try:
            version_resource = BenjiVersion.create_or_update_from_version(version=version)
        except KeyError as exception:
            module_logger.warning(str(exception))
            continue

        versions_seen.add(version_resource)

    module_logger.debug(f'Listing all version resources with label {LABEL_INSTANCE}={benji_instance}.')
    for version_resource in BenjiVersion.objects(
            OperatorContext.kubernetes_client).filter(selector=f'{LABEL_INSTANCE}={benji_instance}'):
        if version_resource not in versions_seen:
            version_resource.delete()


def cleanup_job(*, parent_body: Dict[str, Any]):
    command = ['benji-command', 'cleanup']
    job = BenjiJob(OperatorContext.kubernetes_client, command=command, parent_body=parent_body)
    job.create()


def install_maintenance_jobs(*, parent_body: Dict[str, Any]) -> None:
    reconciliation_schedule: Optional[str] = OperatorContext.operator_config.obj['spec']['reconciliationSchedule']

    OperatorContext.apscheduler.add_job(reconciliate_versions_job,
                                        CronTrigger().from_crontab(reconciliation_schedule),
                                        name=SCHED_VERSION_RECONCILIATION_JOB,
                                        id=SCHED_VERSION_RECONCILIATION_JOB,
                                        replace_existing=True)

    cleanup_schedule: Optional[str] = OperatorContext.operator_config.obj['spec'].get('cleanupSchedule', None)
    if cleanup_schedule is not None and cleanup_schedule:
        OperatorContext.apscheduler.add_job(lambda: cleanup_job(parent_body=parent_body),
                                            CronTrigger().from_crontab(cleanup_schedule),
                                            name=SCHED_CLEANUP_JOB,
                                            id=SCHED_CLEANUP_JOB,
                                            replace_existing=True)


def remove_maintenance_jobs() -> None:
    with suppress(JobLookupError):
        OperatorContext.apscheduler.remove_job(SCHED_VERSION_RECONCILIATION_JOB)
    with suppress(JobLookupError):
        OperatorContext.apscheduler.remove_job(SCHED_CLEANUP_JOB)


@kopf.on.resume(*BenjiOperatorConfig.group_version_plural())
@kopf.on.create(*BenjiOperatorConfig.group_version_plural())
@kopf.on.update(*BenjiOperatorConfig.group_version_plural())
def startup_operator(name: str, namespace: str, logger, **_) -> Optional[Dict[str, Any]]:
    if namespace != service_account_namespace() or name != OperatorContext.operator_config_name:
        return

    set_operator_config()

    if OperatorContext.operator_config is None:
        raise RuntimeError('Operator configuration has not been loaded.')

    if not OperatorContext.apscheduler.running:
        OperatorContext.apscheduler.start()
    install_maintenance_jobs(parent_body=OperatorContext.operator_config.obj)


def shutdown_operator():
    if OperatorContext.operator_config is None:
        return

    remove_maintenance_jobs()
    if OperatorContext.apscheduler.running:
        OperatorContext.apscheduler.shutdown()
    OperatorContext.operator_config = None


@kopf.on.cleanup()
def shutdown_operator_on_termination(**_) -> Optional[Dict[str, Any]]:
    shutdown_operator()


@kopf.on.delete(*BenjiOperatorConfig.group_version_plural())
def shutdown_operator_on_delete(name: str, namespace: str, **_) -> Optional[Dict[str, Any]]:
    if namespace != service_account_namespace() or name != OperatorContext.operator_config_name:
        return

    shutdown_operator()


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiOperatorConfig.kind})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiOperatorConfig.kind})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiOperatorConfig.kind})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: BenjiOperatorConfig.kind})
def benji_track_job_status_maintenance(**kwargs) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=BenjiOperatorConfig, **kwargs)
