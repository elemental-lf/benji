from typing import Dict, Any, Optional

import kopf

from benji.api import RPCClient
from benji.k8s_operator import OperatorContext
from benji.k8s_operator.constants import LABEL_PARENT_KIND, \
    RESOURCE_STATUS_CHILDREN, API_GROUP, API_VERSION
from benji.k8s_operator.crd.version import check_version_access
from benji.k8s_operator.resources import track_job_status, delete_all_dependant_jobs, BenjiJob, NamespacedAPIObject

K8S_RESTORE_SPEC_PERSISTENT_VOLUME_CLAIM_NAME = 'persistentVolumeClaimName'
K8S_RESTORE_SPEC_VERSION_NAME = 'versionName'
K8S_RESTORE_SPEC_OVERWRITE = 'overwrite'
K8S_RESTORE_SPEC_STORAGE_CLASS_NAME = 'storageClassName'


class BenjiRestore(NamespacedAPIObject):

    version = f'{API_GROUP}/{API_VERSION}'
    endpoint = 'benjirestores'
    kind = 'BenjiRestore'


@kopf.on.resume(*BenjiRestore.group_version_plural())
@kopf.on.create(*BenjiRestore.group_version_plural())
def benji_restore(namespace: str, spec: Dict[str, Any], status: Dict[str, Any], body: Dict[str, Any], logger,
                  **_) -> Optional[Dict[str, Any]]:
    if RESOURCE_STATUS_CHILDREN in status:
        # We've already seen this resource
        return

    pvc_name = spec[K8S_RESTORE_SPEC_PERSISTENT_VOLUME_CLAIM_NAME]
    version_name = spec[K8S_RESTORE_SPEC_VERSION_NAME]
    storage_class_name = spec[K8S_RESTORE_SPEC_STORAGE_CLASS_NAME]
    overwrite = spec.get(K8S_RESTORE_SPEC_OVERWRITE, False)

    with RPCClient():
        check_version_access(version_name, body)

    command = [
        'benji-restore-pvc',
        version_name,
        namespace,
        pvc_name,
        storage_class_name,
    ]

    if overwrite:
        command.append('--force')

    job = BenjiJob(OperatorContext.kubernetes_client, command, parent_body=body)
    job.create()


@kopf.on.delete(*BenjiRestore.group_version_plural())
def benji_backup_schedule_delete(name: str, namespace: str, body: Dict[str, Any], logger,
                                 **_) -> Optional[Dict[str, Any]]:
    delete_all_dependant_jobs(name=name, namespace=namespace, kind=body['kind'], logger=logger)


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiRestore.kind})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiRestore.kind})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: BenjiRestore.kind})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: BenjiRestore.kind})
def benji_track_job_status_restore(**kwargs) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=BenjiRestore, **kwargs)
