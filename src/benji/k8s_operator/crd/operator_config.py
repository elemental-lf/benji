from typing import Optional, Dict, Any

import kopf
import kubernetes

import benji.k8s_operator
from benji.helpers.kubernetes import service_account_namespace
from benji.k8s_operator.constants import CRD_OPERATOR_CONFIG, LABEL_PARENT_KIND
from benji.k8s_operator.resources import get_cron_jobs, delete_cron_job, create_cron_job
from benji.k8s_operator.status import track_job_status


def set_operator_config() -> None:
    custom_objects_api = kubernetes.client.CustomObjectsApi()
    benji.k8s_operator.operator_config = custom_objects_api.get_namespaced_custom_object(
        group=CRD_OPERATOR_CONFIG.api_group,
        version=CRD_OPERATOR_CONFIG.api_version,
        plural=CRD_OPERATOR_CONFIG.plural,
        name=benji.k8s_operator.operator_config_name,
        namespace=service_account_namespace())


def install_maintenance_cron_jobs(*, logger) -> None:
    name_prefix = benji.k8s_operator.operator_config['metadata']['name']

    reconciliation_schedule: Optional[str] = benji.k8s_operator.operator_config['spec']['reconciliationSchedule']
    create_cron_job(['benji-versions-recon'],
                    reconciliation_schedule,
                    parent_body=benji.k8s_operator.operator_config,
                    name_override=f'{name_prefix}-reconciliation',
                    logger=logger)

    cleanup_schedule: Optional[str] = benji.k8s_operator.operator_config['spec'].get('cleanupSchedule', None)
    if cleanup_schedule is not None and cleanup_schedule:
        create_cron_job(['benji-command', 'cleanup'],
                        cleanup_schedule,
                        parent_body=benji.k8s_operator.operator_config,
                        name_override=f'{name_prefix}-cleanup',
                        logger=logger)


@kopf.on.startup()
def startup(logger, **kwargs) -> None:
    set_operator_config()

    if benji.k8s_operator.operator_config is None:
        raise RuntimeError('Operator configuration has not been loaded.')

    cron_jobs = get_cron_jobs(benji.k8s_operator.operator_config)
    for cron_job in cron_jobs:
        delete_cron_job(cron_job.metadata.name, cron_job.metadata.namespace, logger=logger)

    install_maintenance_cron_jobs(logger=logger)


@kopf.on.cleanup()
def cleanup(logger, **kwargs) -> None:
    if benji.k8s_operator.operator_config is None:
        return

    cron_jobs = get_cron_jobs(benji.k8s_operator.operator_config)
    for cron_job in cron_jobs:
        delete_cron_job(cron_job.metadata.name, cron_job.metadata.namespace, logger=logger)


@kopf.on.update(CRD_OPERATOR_CONFIG.api_group, CRD_OPERATOR_CONFIG.api_version, CRD_OPERATOR_CONFIG.plural)
def reload_operator_config(name: str, namespace: str, logger, **kwargs) -> Optional[Dict[str, Any]]:
    if namespace != service_account_namespace() or name != benji.k8s_operator.operator_config_name:
        return

    set_operator_config()
    install_maintenance_cron_jobs(logger=logger)


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_OPERATOR_CONFIG.name})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_OPERATOR_CONFIG.name})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_OPERATOR_CONFIG.name})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: CRD_OPERATOR_CONFIG.name})
def benji_track_job_status_maintenance(**kwargs) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=CRD_OPERATOR_CONFIG, **kwargs)
