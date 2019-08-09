from typing import Dict, Any, Optional

import kopf

from benji.helpers.restapi import BenjiRESTClient
from benji.k8s_operator import api_endpoint
from benji.k8s_operator.constants import CRD_RESTORE, LABEL_PARENT_KIND, \
    RESOURCE_STATUS_CHILDREN, K8S_RESTORE_SPEC_PERSISTENT_VOLUME_CLAIM_NAME, K8S_RESTORE_SPEC_VERSION_NAME, \
    K8S_RESTORE_SPEC_OVERWRITE, K8S_RESTORE_SPEC_STORAGE_CLASS_NAME
from benji.k8s_operator.resources import create_job
from benji.k8s_operator.status import build_resource_status_children, track_job_status
from benji.k8s_operator.utils import get_caller_name, check_version_access


@kopf.on.resume(CRD_RESTORE.api_group, CRD_RESTORE.api_version, CRD_RESTORE.plural)
@kopf.on.create(CRD_RESTORE.api_group, CRD_RESTORE.api_version, CRD_RESTORE.plural)
def benji_restore(namespace: str, spec: Dict[str, Any], status: Dict[str, Any], body: Dict[str, Any],
                  patch: Dict[str, Any], logger, **kwargs) -> Optional[Dict[str, Any]]:
    if RESOURCE_STATUS_CHILDREN in status:
        # We've already seen this resource
        return

    pvc_name = spec[K8S_RESTORE_SPEC_PERSISTENT_VOLUME_CLAIM_NAME]
    version_name = spec[K8S_RESTORE_SPEC_VERSION_NAME]
    overwrite = spec.get(K8S_RESTORE_SPEC_OVERWRITE, False)
    storage_class_name = spec.get(K8S_RESTORE_SPEC_STORAGE_CLASS_NAME, None)

    benji = BenjiRESTClient(api_endpoint)
    check_version_access(benji, version_name, body)

    command = [
        'benji-restore-pvc',
        version_name,
        namespace,
        pvc_name,
    ]

    if overwrite:
        command.append('--force')

    if storage_class_name is not None:
        command.extend(['--storage-class', storage_class_name])

    job = create_job(command, parent_body=body, logger=logger)
    patch['status'] = build_resource_status_children(status, job, get_caller_name())


@kopf.on.create('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_RESTORE.name})
@kopf.on.resume('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_RESTORE.name})
@kopf.on.delete('batch', 'v1', 'jobs', labels={LABEL_PARENT_KIND: CRD_RESTORE.name})
@kopf.on.field('batch', 'v1', 'jobs', field='status', labels={LABEL_PARENT_KIND: CRD_RESTORE.name})
def benji_track_job_status_restore(**kwargs) -> Optional[Dict[str, Any]]:
    return track_job_status(crd=CRD_RESTORE, **kwargs)
