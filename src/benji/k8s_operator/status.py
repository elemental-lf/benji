import uuid
from typing import List, Any, Dict, Optional

import kubernetes
from kubernetes.client.rest import ApiException

from benji.helpers.kubernetes import service_account_namespace
from benji.k8s_operator.constants import RESOURCE_STATUS_LIST_OBJECT_REFERENCE, RESOURCE_STATUS_CHILDREN, \
    JOB_STATUS_START_TIME, JOB_STATUS_COMPLETION_TIME, RESOURCE_STATUS_CHILDREN_HANDLER_NAME, JOB_STATUS_FAILED, \
    RESOURCE_STATUS_DEPENDANT_JOBS_STATUS, RESOURCE_JOB_STATUS_SUCCEEDED, RESOURCE_JOB_STATUS_FAILED, \
    RESOURCE_JOB_STATUS_RUNNING, RESOURCE_JOB_STATUS_PENDING, RESOURCE_STATUS_DEPENDANT_JOBS, LABEL_PARENT_NAME, CRD, \
    LABEL_PARENT_NAMESPACE, RESOURCE_STATUS_CHILD_CHANGED
from benji.k8s_operator.resources import create_object_ref, get_parent, patch_parent


def update_status_list(lst: List,
                       resource: Any,
                       extra_data: Dict[str, Any],
                       *,
                       include_gvk: bool = True) -> List[Dict[str, any]]:
    if hasattr(resource, 'to_dict'):
        # Convert object of the official Kubernetes client to a dictionary.
        resource_dict = resource.to_dict()
    else:
        resource_dict = resource

    new_lst = list(lst)
    for i, value in enumerate(new_lst):
        if value[RESOURCE_STATUS_LIST_OBJECT_REFERENCE]['uid'] == resource_dict['metadata']['uid']:
            new_lst[i] = {RESOURCE_STATUS_LIST_OBJECT_REFERENCE: create_object_ref(resource_dict)}
            new_lst[i].update(extra_data)
            break
    else:
        new_lst.append(
            {RESOURCE_STATUS_LIST_OBJECT_REFERENCE: create_object_ref(resource_dict, include_gvk=include_gvk)})
        new_lst[-1].update(extra_data)

    return new_lst


def delete_status_list(lst: List, resource: Any) -> List[Dict[str, any]]:
    if hasattr(resource, 'to_dict'):
        # Convert object of the official Kubernetes client to a dictionary.
        resource_dict = resource.to_dict()
    else:
        resource_dict = resource

    return [
        value for value in lst if not value[RESOURCE_STATUS_LIST_OBJECT_REFERENCE]['uid'] == resource_dict['metadata']['uid']
    ]


def build_resource_status_children(status: Dict[str, Any], resource: Any, handler_name: str) -> Dict[str, Any]:
    return {
        RESOURCE_STATUS_CHILDREN:
            update_status_list(
                status.get(RESOURCE_STATUS_CHILDREN, []),
                resource,
                {RESOURCE_STATUS_CHILDREN_HANDLER_NAME: handler_name},
            )
    }


def build_dependant_job_status(job_status: Dict[str, Any]) -> Dict[str, Any]:
    dependant_job_status = {
        key: job_status[key] for key in (JOB_STATUS_START_TIME, JOB_STATUS_COMPLETION_TIME) if key in job_status
    }

    if JOB_STATUS_COMPLETION_TIME in job_status:
        dependant_job_status[RESOURCE_STATUS_DEPENDANT_JOBS_STATUS] = RESOURCE_JOB_STATUS_SUCCEEDED
    elif JOB_STATUS_START_TIME in job_status:
        if JOB_STATUS_FAILED in job_status and job_status[JOB_STATUS_FAILED] > 0:
            dependant_job_status[RESOURCE_STATUS_DEPENDANT_JOBS_STATUS] = RESOURCE_JOB_STATUS_FAILED
        else:
            dependant_job_status[RESOURCE_STATUS_DEPENDANT_JOBS_STATUS] = RESOURCE_JOB_STATUS_RUNNING
    else:
        dependant_job_status[RESOURCE_STATUS_DEPENDANT_JOBS_STATUS] = RESOURCE_JOB_STATUS_PENDING

    return dependant_job_status


def build_resource_status_dependant_jobs(status: Dict[str, Any],
                                         job: Dict[str, Any],
                                         delete: bool = False) -> Dict[str, Any]:
    if delete:
        dependant_jobs = delete_status_list(status.get(RESOURCE_STATUS_DEPENDANT_JOBS, []), job)
    else:
        dependant_jobs = update_status_list(status.get(RESOURCE_STATUS_DEPENDANT_JOBS, []),
                                            job,
                                            build_dependant_job_status(job['status']),
                                            include_gvk=False)

    return {RESOURCE_STATUS_DEPENDANT_JOBS: dependant_jobs}


def track_job_status(reason: str, name: str, namespace: str, meta: Dict[str, Any], body: Dict[str, Any], logger,
                     crd: CRD, **kwargs) -> Optional[Dict[str, Any]]:
    # Only look at events from our namespace
    if namespace != service_account_namespace():
        return

    batch_v1_api = kubernetes.client.BatchV1Api()

    if reason != 'delete' and 'labels' not in meta or LABEL_PARENT_NAME not in meta['labels']:
        # Stray jobs will be deleted
        logger.warning(f'Job {name} is one of ours but has no or incomplete parent labels, deleting it.')
        batch_v1_api.delete_namespaced_job(namespace=namespace, name=name)
        return

    if LABEL_PARENT_NAMESPACE in meta['labels']:
        parent_namespace = meta['labels'][LABEL_PARENT_NAMESPACE]
    else:
        parent_namespace = None

    parent_name = meta['labels'][LABEL_PARENT_NAME]

    try:
        parent = get_parent(parent_name=parent_name, parent_namespace=parent_namespace, logger=logger, crd=crd)
        parent_patch = {
            'status': build_resource_status_dependant_jobs(parent.get('status', {}), body, delete=(reason == 'delete'))
        }
        patch_parent(parent_name=parent_name,
                     parent_namespace=parent_namespace,
                     logger=logger,
                     crd=crd,
                     parent_patch=parent_patch)
    except ApiException as exception:
        if exception.status == 404:
            if reason != 'delete':
                # Jobs without parent will be deleted
                logger.warning(f'Parent {parent_name} of job {name} has gone away, deleting the job.')
                batch_v1_api.delete_namespaced_job(namespace=namespace, name=name)
        else:
            raise exception


def track_cron_job_status(reason: str, name: str, namespace: str, meta: Dict[str, Any], logger, crd: CRD,
                          **kwargs) -> Optional[Dict[str, Any]]:
    # Only look at events from our namespace
    if namespace != service_account_namespace():
        return

    batch_v1beta1_api = kubernetes.client.BatchV1beta1Api()

    if reason != 'delete' and 'labels' not in meta or LABEL_PARENT_NAME not in meta['labels']:
        # Stray jobs will be deleted
        logger.warning(f'CronJob {name} is one of ours but has no or incomplete parent labels, deleting it.')
        batch_v1beta1_api.delete_namespaced_cron_job(namespace=namespace, name=name)
        return

    if LABEL_PARENT_NAMESPACE in meta['labels']:
        parent_namespace = meta['labels'][LABEL_PARENT_NAMESPACE]
    else:
        parent_namespace = None

    parent_name = meta['labels'][LABEL_PARENT_NAME]

    try:
        parent = get_parent(parent_name=parent_name, parent_namespace=parent_namespace, logger=logger, crd=crd)
        parent_patch = {'status': parent.get('status', {}).update({RESOURCE_STATUS_CHILD_CHANGED: uuid.uuid4})}
        patch_parent(parent_name=parent_name,
                     parent_namespace=parent_namespace,
                     logger=logger,
                     crd=crd,
                     parent_patch=parent_patch)
    except ApiException as exception:
        if exception.status == 404:
            if reason != 'delete':
                # Cron jobs without parent will be deleted
                logger.warning(f'Parent {parent_name} of CronJob {name} has gone away, deleting the CronJob.')
                batch_v1beta1_api.delete_namespaced_cron_job(namespace=namespace, name=name)
        else:
            raise exception
