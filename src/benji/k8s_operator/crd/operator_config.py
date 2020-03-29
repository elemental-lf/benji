from contextlib import suppress
from typing import Optional, Dict, Any

import kopf
import kubernetes
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import benji.k8s_operator
from benji.helpers.constants import LABEL_INSTANCE
from benji.helpers.kubernetes import service_account_namespace, BenjiVersionResource
from benji.helpers.prometheus import version_status_older_incomplete, version_status_invalid, push, \
    version_status_registry
from benji.helpers.restapi import BenjiRESTClient
from benji.helpers.settings import benji_instance
from benji.k8s_operator import api_endpoint
from benji.k8s_operator.constants import CRD_OPERATOR_CONFIG, LABEL_PARENT_KIND, SCHED_VERSION_RECONCILIATION_JOB, \
    SCHED_CLEANUP_JOB, SCHED_VERSION_STATUS_JOB
from benji.k8s_operator.resources import track_job_status, JobResource


def set_operator_config() -> None:
    custom_objects_api = kubernetes.client.CustomObjectsApi()
    benji.k8s_operator.operator_config = custom_objects_api.get_namespaced_custom_object(
        group=CRD_OPERATOR_CONFIG.api_group,
        version=CRD_OPERATOR_CONFIG.api_version,
        plural=CRD_OPERATOR_CONFIG.plural,
        name=benji.k8s_operator.operator_config_name,
        namespace=service_account_namespace())


def reconciliate_versions_job(*, logger):
    benji = BenjiRESTClient(api_endpoint)
    logger.debug(f'Finding versions with filter labels["{LABEL_INSTANCE}"] == "{benji_instance}".')
    versions = benji.find_versions_with_filter(filter_expression=f'labels["{LABEL_INSTANCE}"] == "{benji_instance}"')
    logger.debug(f"Number of matching versions in the database: {len(versions)}.")

    versions_seen = set()
    for version in versions:
        try:
            version_resource = BenjiVersionResource.create_or_replace(version=version, logger=logger)
        except KeyError as exception:
            logger.warning(str(exception))
            continue

        versions_seen.add(version_resource)

    logger.debug(f'Listing all version resources with label {LABEL_INSTANCE}={benji_instance}.')
    for version_resource in BenjiVersionResource.list(label_selector=f'{LABEL_INSTANCE}={benji_instance}',
                                                      logger=logger):
        if version_resource not in versions_seen:
            version_resource.delete()


def cleanup_job(*, parent_body: Dict[str, Any], logger):
    command = ['benji-command', 'cleanup']
    JobResource(command, parent_body=parent_body, logger=logger)


def versions_status_job():
    benji = BenjiRESTClient(api_endpoint)

    incomplete_versions_count = len(
        benji.find_versions_with_filter(f'labels["{LABEL_INSTANCE}"] == "{benji_instance}" and status == "incomplete" and date < "1 day ago"'))
    invalid_versions_count = len(
        benji.find_versions_with_filter(f'labels["{LABEL_INSTANCE}"] == "{benji_instance}" and status == "invalid"'))

    version_status_older_incomplete.set(incomplete_versions_count)
    version_status_invalid.set(invalid_versions_count)
    push(version_status_registry)


def install_maintenance_jobs(*, parent_body: Dict[str, Any], logger) -> None:
    reconciliation_schedule: Optional[str] = benji.k8s_operator.operator_config['spec']['reconciliationSchedule']

    benji.k8s_operator.scheduler.add_job(lambda: reconciliate_versions_job(logger=logger),
                                         CronTrigger().from_crontab(reconciliation_schedule),
                                         name=SCHED_VERSION_RECONCILIATION_JOB,
                                         id=SCHED_VERSION_RECONCILIATION_JOB)

    cleanup_schedule: Optional[str] = benji.k8s_operator.operator_config['spec'].get('cleanupSchedule', None)
    if cleanup_schedule is not None and cleanup_schedule:
        benji.k8s_operator.scheduler.add_job(lambda: cleanup_job(parent_body=parent_body, logger=logger),
                                             CronTrigger().from_crontab(cleanup_schedule),
                                             name=SCHED_CLEANUP_JOB,
                                             id=SCHED_CLEANUP_JOB)


def remove_maintenance_jobs() -> None:
    try:
        benji.k8s_operator.scheduler.remove_job(SCHED_VERSION_RECONCILIATION_JOB)
        benji.k8s_operator.scheduler.remove_job(SCHED_CLEANUP_JOB)
    except JobLookupError:
        pass


@kopf.on.startup()
def startup(logger, **_) -> None:
    set_operator_config()

    if benji.k8s_operator.operator_config is None:
        raise RuntimeError('Operator configuration has not been loaded.')

    # See https://apscheduler.readthedocs.io/en/stable/userguide.html#missed-job-executions
    job_defaults = {'coalesce': True, 'max_instances': 1, 'misfire_grace_time': 60}
    benji.k8s_operator.scheduler = scheduler = BackgroundScheduler(job_defaults=job_defaults, timezone='UTC')
    scheduler.start()

    remove_maintenance_jobs()
    install_maintenance_jobs(parent_body=benji.k8s_operator.operator_config, logger=logger)


@kopf.on.cleanup()
def cleanup(**_) -> None:
    if benji.k8s_operator.operator_config is None:
        return

    remove_maintenance_jobs()
    benji.k8s_operator.scheduler.shutdown()


@kopf.on.update(CRD_OPERATOR_CONFIG.api_group, CRD_OPERATOR_CONFIG.api_version, CRD_OPERATOR_CONFIG.plural)
def reload_operator_config(name: str, namespace: str, logger, **_) -> Optional[Dict[str, Any]]:
    if namespace != service_account_namespace() or name != benji.k8s_operator.operator_config_name:
        return

    set_operator_config()
    remove_maintenance_jobs()
    install_maintenance_jobs(parent_body=benji.k8s_operator.operator_config, logger=logger)


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_OPERATOR_CONFIG.name})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_OPERATOR_CONFIG.name})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_OPERATOR_CONFIG.name})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: CRD_OPERATOR_CONFIG.name})
def benji_track_job_status_maintenance(**_) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=CRD_OPERATOR_CONFIG, **_)
