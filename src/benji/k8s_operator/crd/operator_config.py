from contextlib import suppress
from typing import Optional, Dict, Any

import kopf
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import benji.k8s_operator
from benji.api import APIClient
from benji.helpers.settings import benji_instance
from benji.k8s_operator import kubernetes_client
from benji.k8s_operator.constants import LABEL_PARENT_KIND, SCHED_VERSION_RECONCILIATION_JOB, \
    SCHED_CLEANUP_JOB, API_VERSION, API_GROUP, LABEL_INSTANCE
from benji.k8s_operator.crd.version import BenjiVersion
from benji.k8s_operator.resources import track_job_status, BenjiJob, NamespacedAPIObject
from benji.k8s_operator.utils import service_account_namespace


class BenjiOperatorConfig(NamespacedAPIObject):

    version = f'{API_GROUP}/{API_VERSION}'
    endpoint = 'benjioperatorconfigs'
    kind = 'BenjiOperatorConfig'


def set_operator_config() -> None:
    benji.k8s_operator.operator_config = BenjiOperatorConfig.objects(kubernetes_client).filter(
        namespace=service_account_namespace()).get_by_name(benji.k8s_operator.operator_config_name)


def reconciliate_versions_job(*, logger):
    benji = APIClient()
    logger.debug(f'Finding versions with filter labels["{LABEL_INSTANCE}"] == "{benji_instance}".')
    versions = benji.core_v1_ls(filter_expression=f'labels["{LABEL_INSTANCE}"] == "{benji_instance}"')['versions']
    logger.debug(f"Number of matching versions in the database: {len(versions)}.")

    versions_seen = set()
    for version in versions:
        try:
            version_resource = BenjiVersion.create_or_update_from_version(version=version, logger=logger)
        except KeyError as exception:
            logger.warning(str(exception))
            continue

        versions_seen.add(version_resource)

    logger.debug(f'Listing all version resources with label {LABEL_INSTANCE}={benji_instance}.')
    for version_resource in BenjiVersion.objects(kubernetes_client).filter(selector=f'{LABEL_INSTANCE}={benji_instance}'):
        if version_resource not in versions_seen:
            version_resource.delete()


def cleanup_job(*, parent_body: Dict[str, Any], logger):
    command = ['benji-command', 'cleanup']
    job = BenjiJob(kubernetes_client, command=command, parent_body=parent_body)
    job.create()


def install_maintenance_jobs(*, parent_body: Dict[str, Any], logger) -> None:
    reconciliation_schedule: Optional[str] = benji.k8s_operator.operator_config.obj['spec']['reconciliationSchedule']

    benji.k8s_operator.apscheduler.add_job(lambda: reconciliate_versions_job(logger=logger),
                                           CronTrigger().from_crontab(reconciliation_schedule),
                                           name=SCHED_VERSION_RECONCILIATION_JOB,
                                           id=SCHED_VERSION_RECONCILIATION_JOB)

    cleanup_schedule: Optional[str] = benji.k8s_operator.operator_config.obj['spec'].get('cleanupSchedule', None)
    if cleanup_schedule is not None and cleanup_schedule:
        benji.k8s_operator.apscheduler.add_job(lambda: cleanup_job(parent_body=parent_body, logger=logger),
                                               CronTrigger().from_crontab(cleanup_schedule),
                                               name=SCHED_CLEANUP_JOB,
                                               id=SCHED_CLEANUP_JOB)


def remove_maintenance_jobs() -> None:
    with suppress(JobLookupError):
        benji.k8s_operator.apscheduler.remove_job(SCHED_VERSION_RECONCILIATION_JOB)
    with suppress(JobLookupError):
        benji.k8s_operator.apscheduler.remove_job(SCHED_CLEANUP_JOB)


@kopf.on.startup()
def startup(logger, **_) -> None:
    set_operator_config()

    if benji.k8s_operator.operator_config is None:
        raise RuntimeError('Operator configuration has not been loaded.')

    # See https://apscheduler.readthedocs.io/en/stable/userguide.html#missed-job-executions
    job_defaults = {'coalesce': True, 'max_instances': 1, 'misfire_grace_time': 60}
    benji.k8s_operator.apscheduler = scheduler = BackgroundScheduler(job_defaults=job_defaults, timezone='UTC')
    scheduler.start()

    remove_maintenance_jobs()
    install_maintenance_jobs(parent_body=benji.k8s_operator.operator_config, logger=logger)


@kopf.on.cleanup()
def cleanup(**_) -> None:
    if benji.k8s_operator.operator_config is None:
        return

    remove_maintenance_jobs()
    benji.k8s_operator.apscheduler.shutdown()


@kopf.on.update(*BenjiOperatorConfig.group_version_plural())
def reload_operator_config(name: str, namespace: str, logger, **_) -> Optional[Dict[str, Any]]:
    if namespace != service_account_namespace() or name != benji.k8s_operator.operator_config_name:
        return

    set_operator_config()
    remove_maintenance_jobs()
    install_maintenance_jobs(parent_body=benji.k8s_operator.operator_config, logger=logger)


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiOperatorConfig.kind})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiOperatorConfig.kind})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiOperatorConfig.kind})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: BenjiOperatorConfig.kind})
def benji_track_job_status_maintenance(**kwargs) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=BenjiOperatorConfig, **kwargs)
