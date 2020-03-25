import copy
from typing import Dict, Any, List, Optional

import kubernetes
from kubernetes.client.rest import ApiException

import benji.k8s_operator
from benji.helpers.kubernetes import service_account_namespace
from benji.k8s_operator.constants import LABEL_PARENT_KIND, LABEL_PARENT_NAMESPACE, LABEL_PARENT_NAME, CRD, \
    RESOURCE_STATUS_LIST_OBJECT_REFERENCE, JOB_STATUS_START_TIME, JOB_STATUS_COMPLETION_TIME, \
    RESOURCE_STATUS_DEPENDANT_JOBS_STATUS, RESOURCE_JOB_STATUS_SUCCEEDED, JOB_STATUS_FAILED, RESOURCE_JOB_STATUS_FAILED, \
    RESOURCE_JOB_STATUS_RUNNING, RESOURCE_JOB_STATUS_PENDING, RESOURCE_STATUS_DEPENDANT_JOBS


def setup_manifest(*,
                   manifest: Dict[str, Any],
                   namespace: str,
                   parent_body: Dict[str, Any],
                   name_override: str = None) -> None:
    if manifest['kind'] != 'Job':
        raise RuntimeError(f'Unhandled kind: {manifest["kind"]}.')

    manifest['metadata']['namespace'] = namespace

    # Generate unique name with parent's metadata.name as prefix
    if 'name' in manifest['metadata']:
        del manifest['metadata']['name']
    if name_override is None:
        manifest['metadata']['generateName'] = '{}-'.format(parent_body['metadata']['name'])
    else:
        manifest['metadata']['generateName'] = '{}-'.format(name_override)

    # Label it so we can filter incoming events correctly
    labels = {
        LABEL_PARENT_KIND: parent_body['kind'],
        LABEL_PARENT_NAME: parent_body['metadata']['name'],
    }

    if 'namespace' in parent_body['metadata']:
        labels[LABEL_PARENT_NAMESPACE] = parent_body['metadata']['namespace']

    manifest['metadata']['labels'] = manifest['metadata'].get('labels', {})
    manifest['metadata']['labels'].update(labels)

    manifest['spec']['template']['metadata'] = manifest['spec']['template'].get('metadata', {})
    manifest['spec']['template']['metadata']['labels'] = manifest['spec']['template']['metadata'].get('labels', {})
    manifest['spec']['template']['metadata']['labels'].update(labels)


def create_job(command: List[str], *, parent_body: Dict[str, Any], logger) -> kubernetes.client.models.v1_job.V1Job:
    if benji.k8s_operator.operator_config is None:
        raise RuntimeError('Operator configuration has not been loaded.')

    job_manifest = copy.deepcopy(benji.k8s_operator.operator_config['spec']['jobTemplate'])
    setup_manifest(manifest=job_manifest, namespace=service_account_namespace(), parent_body=parent_body)

    job_manifest['spec']['template']['spec']['containers'][0]['command'] = command
    job_manifest['spec']['template']['spec']['containers'][0]['args'] = []

    # Actually create the job via the Kubernetes API.
    logger.debug(f'Creating Job: {job_manifest}')
    batch_v1_api = kubernetes.client.BatchV1Api()
    return batch_v1_api.create_namespaced_job(namespace=service_account_namespace(), body=job_manifest)


def create_cron_job(command: List[str],
                    schedule: str,
                    *,
                    parent_body: Dict[str, Any],
                    logger,
                    name_override: str = None) -> kubernetes.client.models.v1_job.V1Job:
    if benji.k8s_operator.operator_config is None:
        raise RuntimeError('Operator configuration has not been loaded.')

    cron_job_manifest = copy.deepcopy(benji.k8s_operator.operator_config['spec']['cronJobTemplate'])
    setup_manifest(manifest=cron_job_manifest,
                   namespace=service_account_namespace(),
                   parent_body=parent_body,
                   name_override=name_override)

    cron_job_manifest['spec']['schedule'] = schedule
    cron_job_manifest['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['command'] = command
    cron_job_manifest['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['args'] = []

    # Actually create the cron job via the Kubernetes API.
    logger.debug(f'Creating CronJob: {cron_job_manifest}')
    batch_v1beta1_api = kubernetes.client.BatchV1beta1Api()
    return batch_v1beta1_api.create_namespaced_cron_job(namespace=service_account_namespace(), body=cron_job_manifest)


def delete_cron_job(name: str, namespace: str, *, logger) -> None:
    batch_v1beta1_api = kubernetes.client.BatchV1beta1Api()

    try:
        logger.debug(f'Deleting CronJob {namespace}/{name}.')
        batch_v1beta1_api.delete_namespaced_cron_job(name=name, namespace=namespace)
    except ApiException as exception:
        if exception.status == 404:
            # CronJob went way in the meantime
            pass


def get_cron_jobs(
        parent_body: Dict[str, Any]) -> List[kubernetes.client.api_client.models.v1beta1_cron_job.V1beta1CronJob]:
    kind = parent_body['kind']
    namespace = parent_body['metadata'].get('namespace', None)
    name = parent_body['metadata']['name']
    if namespace is not None:
        label_selector = f'{LABEL_PARENT_KIND}={kind},{LABEL_PARENT_NAMESPACE}={namespace},{LABEL_PARENT_NAME}={name}'
    else:
        label_selector = f'{LABEL_PARENT_KIND}={kind},{LABEL_PARENT_NAME}={name}'

    batch_v1beta1_api = kubernetes.client.BatchV1beta1Api()
    cron_job_list = batch_v1beta1_api.list_namespaced_cron_job(namespace=service_account_namespace(),
                                                               label_selector=label_selector)

    return cron_job_list.items


def create_object_ref(resource_dict: Dict[str, Any], include_gvk: bool = True) -> Dict[str, Any]:
    reference = {
        'name': resource_dict['metadata']['name'],
        'namespace': resource_dict['metadata']['namespace'],
        'uid': resource_dict['metadata']['uid'],
    }

    if include_gvk:
        reference.update({
            # This is a difference between the dicts provided by Kopf and the official Kubernetes clients.
            'apiVersion':
                resource_dict['apiVersion'] if 'apiVersion' in resource_dict else resource_dict['api_version'],
            'kind':
                resource_dict['kind'],
        })

    return reference


def get_parent(*, parent_name: str, parent_namespace: str, logger, crd: CRD) -> Dict[str, Any]:
    custom_objects_api = kubernetes.client.CustomObjectsApi()
    if parent_namespace is not None:
        logger.debug(f'Getting resource {parent_namespace}/{parent_name}.')
        parent = custom_objects_api.get_namespaced_custom_object(group=crd.api_group,
                                                                 version=crd.api_version,
                                                                 plural=crd.plural,
                                                                 name=parent_name,
                                                                 namespace=parent_namespace)
    else:
        logger.debug(f'Getting resource {parent_name}.')
        parent = custom_objects_api.get_cluster_custom_object(group=crd.api_group,
                                                              version=crd.api_version,
                                                              plural=crd.plural,
                                                              name=parent_name)

    return parent


def patch_parent(*, parent_name: str, parent_namespace: Optional[str], logger, crd: CRD,
                 parent_patch: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    custom_objects_api = kubernetes.client.CustomObjectsApi()
    if parent_namespace is not None:
        logger.debug(f'Patching resource {parent_namespace}/{parent_name} with: {parent_patch}')
        custom_objects_api.patch_namespaced_custom_object(group=crd.api_group,
                                                          version=crd.api_version,
                                                          plural=crd.plural,
                                                          name=parent_name,
                                                          namespace=parent_namespace,
                                                          body=parent_patch)
    else:
        logger.debug(f'Patching resource {parent_name} with: {parent_patch}')
        custom_objects_api.patch_cluster_custom_object(group=crd.api_group,
                                                       version=crd.api_version,
                                                       plural=crd.plural,
                                                       name=parent_name,
                                                       body=parent_patch)


def update_status_list(lst: List, resource: Any, extra_data: Dict[str, Any]) -> List[Dict[str, any]]:
    if hasattr(resource, 'to_dict'):
        # Convert object of the official Kubernetes client to a dictionary.
        resource_dict = resource.to_dict()
    else:
        resource_dict = resource

    new_lst = list(lst)
    for i, value in enumerate(new_lst):
        if value[RESOURCE_STATUS_LIST_OBJECT_REFERENCE]['uid'] == resource_dict['metadata']['uid']:
            new_lst[i] = {RESOURCE_STATUS_LIST_OBJECT_REFERENCE: create_object_ref(resource_dict, include_gvk=False)}
            new_lst[i].update(extra_data)
            break
    else:
        new_lst.append({RESOURCE_STATUS_LIST_OBJECT_REFERENCE: create_object_ref(resource_dict, include_gvk=False)})
        new_lst[-1].update(extra_data)

    return new_lst


def delete_from_status_list(lst: List, resource: Any) -> List[Dict[str, any]]:
    if hasattr(resource, 'to_dict'):
        # Convert object of the official Kubernetes client to a dictionary.
        resource_dict = resource.to_dict()
    else:
        resource_dict = resource

    return [
        value for value in lst if not value[RESOURCE_STATUS_LIST_OBJECT_REFERENCE]['uid'] == resource_dict['metadata']['uid']
    ]


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
        dependant_jobs = delete_from_status_list(status.get(RESOURCE_STATUS_DEPENDANT_JOBS, []), job)
    else:
        dependant_jobs = update_status_list(status.get(RESOURCE_STATUS_DEPENDANT_JOBS, []), job,
                                            build_dependant_job_status(job['status']))

    return {RESOURCE_STATUS_DEPENDANT_JOBS: dependant_jobs}


def track_job_status(reason: str, name: str, namespace: str, meta: Dict[str, Any], body: Dict[str, Any], logger,
                     crd: CRD, **_) -> None:
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
                # Jobs without a parent will be deleted
                logger.warning(f'Parent {parent_name} of job {name} has gone away, deleting the job.')
                batch_v1_api.delete_namespaced_job(namespace=namespace, name=name)
        else:
            raise exception


def delete_dependant_jobs(*, name: str, namespace: str, logger, kind: str) -> None:
    batch_v1_api = kubernetes.client.BatchV1Api()
    jobs = batch_v1_api.list_namespaced_job(
        namespace=service_account_namespace(),
        label_selector=f'{LABEL_PARENT_KIND}={kind},{LABEL_PARENT_NAME}={name},{LABEL_PARENT_NAMESPACE}={namespace}')
    for job in jobs.items:
        try:
            logger.info(f'Deleting dependant job {job.metadata.namespace}/{job.metadata.name}.')
            batch_v1_api.delete_namespaced_job(namespace=job.metadata.namespace, name=job.metadata.name)
        except ApiException as exception:
            if exception.status == 404:
                logger.warning(f'Job {job.metadata.namespace}/{job.metadata.name} has gone away before it could be deleted.')
            else:
                raise exception
