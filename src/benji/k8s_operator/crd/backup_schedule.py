from functools import partial
from operator import attrgetter
from typing import Dict, Any, Optional

import kopf
import kubernetes
import pykube
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger

import benji.k8s_operator
from benji.amqp import AMQPRPCClient
from benji.k8s_operator.constants import CRD_BACKUP_SCHEDULE, CRD_CLUSTER_BACKUP_SCHEDULE, LABEL_PARENT_KIND
from benji.k8s_operator.resources import track_job_status, delete_all_dependant_jobs, JobResource
from benji.k8s_operator.utils import cr_to_job_name, build_version_labels_rbd, determine_rbd_image_location, \
    random_string
from benji.k8s_operator import kubernetes_client


def backup_scheduler_job(*,
                         namespace_label_selector: str = None,
                         namespace: str = None,
                         label_selector: str,
                         parent_body,
                         logger):
    if namespace_label_selector is not None:
        namespaces = [
            namespace.metadata.name for namespace in pykube.Namespace.objects(kubernetes_client).filter(label_selector=namespace_label_selector)
        ]
    else:
        namespaces = [namespace]

    pvcs = []
    for ns in namespaces:
        pvcs.extend([
            o.obj
            for o in pykube.PersistentVolumeClaim().objects(kubernetes_client).filter(namespace=ns,
                                                                                      label_selector=label_selector)
        ])

    if len(pvcs) == 0:
        logger.warning(f'No PVC matched the selector {label_selector} in namespace(s) {", ".join(namespaces)}.')
        return

    rpc_client = AMQPRPCClient(queue='')
    for pvc in pvcs:
        if 'volumeName' not in pvc['spec'] or pvc['spec']['volumeName'] in (None, ''):
            continue

        version_uid = '{}-{}'.format(f'{pvc.metadata.namespace}-{pvc.metadata.name}'[:246], random_string(6))
        volume = '{}/{}'.format(pvc['metadata']['namespace'], pvc['metadata']['name'])
        pv = pykube.PersistentVolume().objects(kubernetes_client).get_by_name(pvc['spec']['volumeName'])
        pool, image = determine_rbd_image_location(pv)
        version_labels = build_version_labels_rbd(pvc, pv, pool, image)

        rpc_client.call_async('ceph_v1_backup',
                              version_uid=version_uid,
                              volume=volume,
                              pool=pool,
                              image=image,
                              version_labels=version_labels)
    rpc_client.call_async('terminate')

    command = ['benji-api-server', '--queue', rpc_client.queue]
    JobResource(command, parent_body=parent_body, logger=logger)


@kopf.on.resume(CRD_BACKUP_SCHEDULE.api_group, CRD_BACKUP_SCHEDULE.api_version, CRD_BACKUP_SCHEDULE.plural)
@kopf.on.create(CRD_BACKUP_SCHEDULE.api_group, CRD_BACKUP_SCHEDULE.api_version, CRD_BACKUP_SCHEDULE.plural)
@kopf.on.update(CRD_BACKUP_SCHEDULE.api_group, CRD_BACKUP_SCHEDULE.api_version, CRD_BACKUP_SCHEDULE.plural)
@kopf.on.resume(CRD_CLUSTER_BACKUP_SCHEDULE.api_group, CRD_CLUSTER_BACKUP_SCHEDULE.api_version,
                CRD_CLUSTER_BACKUP_SCHEDULE.plural)
@kopf.on.create(CRD_CLUSTER_BACKUP_SCHEDULE.api_group, CRD_CLUSTER_BACKUP_SCHEDULE.api_version,
                CRD_CLUSTER_BACKUP_SCHEDULE.plural)
@kopf.on.update(CRD_CLUSTER_BACKUP_SCHEDULE.api_group, CRD_CLUSTER_BACKUP_SCHEDULE.api_version,
                CRD_CLUSTER_BACKUP_SCHEDULE.plural)
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
    delete_all_dependant_jobs(name=name, namespace=namespace, kind=body['kind'], logger=logger)


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


@kopf.timer(CRD_BACKUP_SCHEDULE.api_group,
            CRD_BACKUP_SCHEDULE.api_version,
            CRD_BACKUP_SCHEDULE.plural,
            initial_delay=60,
            interval=60)
@kopf.timer(CRD_CLUSTER_BACKUP_SCHEDULE.api_group,
            CRD_CLUSTER_BACKUP_SCHEDULE.api_version,
            CRD_CLUSTER_BACKUP_SCHEDULE.plural,
            initial_delay=60,
            interval=60)
def benji_backup_schedule_job_gc(name: str, namespace: str):
    pass
