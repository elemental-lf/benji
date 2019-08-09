import copy
from typing import Dict, Any, List, Optional

import kubernetes
from kubernetes.client.rest import ApiException

import benji.k8s_operator
from benji.helpers.kubernetes import service_account_namespace
from benji.k8s_operator.constants import LABEL_PARENT_KIND, LABEL_PARENT_NAMESPACE, LABEL_PARENT_NAME, CRD


def setup_manifest(*,
                   manifest: Dict[str, Any],
                   namespace: str,
                   parent_body: Dict[str, Any],
                   name_override: str = None) -> None:
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

    if manifest['kind'] == 'Job':
        manifest['spec']['template']['metadata'] = manifest['spec']['template'].get('metadata', {})
        manifest['spec']['template']['metadata']['labels'] = manifest['spec']['template']['metadata'].get('labels', {})
        manifest['spec']['template']['metadata']['labels'].update(labels)
    elif manifest['kind'] == 'CronJob':
        manifest['spec']['jobTemplate']['metadata'] = manifest['spec']['jobTemplate'].get('metadata', {})
        manifest['spec']['jobTemplate']['metadata']['labels'] = manifest['spec']['jobTemplate']['metadata'].get(
            'labels', {})
        manifest['spec']['jobTemplate']['metadata']['labels'].update(labels)

        manifest['spec']['jobTemplate']['spec']['template']['metadata'] = manifest['spec']['jobTemplate']['spec']['template'].get(
            'metadata', {})
        manifest['spec']['jobTemplate']['spec']['template']['metadata']['labels'] = manifest['spec']['jobTemplate'][
            'spec']['template']['metadata'].get('labels', {})
        manifest['spec']['jobTemplate']['spec']['template']['metadata']['labels'].update(labels)


def create_job(command: List[str], *, parent_body: Dict[str, Any], logger) -> kubernetes.client.models.v1_job.V1Job:
    if benji.k8s_operator.operator_config is None:
        raise RuntimeError('Operator configuration has not been loaded.')

    job_manifest = copy.deepcopy(benji.k8s_operator.operator_config['spec']['jobTemplate'])
    setup_manifest(manifest=job_manifest, namespace=service_account_namespace(), parent_body=parent_body)

    job_manifest['spec']['template']['spec']['containers'][0]['command'] = command

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
