import copy
import datetime
import uuid
from typing import Dict, Any, List, Optional, Sequence, NamedTuple, Tuple

import pykube
from pykube import HTTPClient
from pykube.objects import APIObject, APIObject as pykube_APIObject, NamespacedAPIObject as pykube_NamespacedAPIObject
from requests import HTTPError

from benji.helpers.settings import benji_instance, running_pod_name
from benji.k8s_operator import OperatorContext
from benji.k8s_operator.constants import LABEL_PARENT_KIND, LABEL_PARENT_NAMESPACE, LABEL_PARENT_NAME, \
    RESOURCE_STATUS_LIST_OBJECT_REFERENCE, JOB_STATUS_START_TIME, JOB_STATUS_COMPLETION_TIME, \
    RESOURCE_STATUS_DEPENDANT_JOBS_STATUS, RESOURCE_JOB_STATUS_SUCCEEDED, JOB_STATUS_FAILED, RESOURCE_JOB_STATUS_FAILED, \
    RESOURCE_JOB_STATUS_RUNNING, RESOURCE_JOB_STATUS_PENDING, RESOURCE_STATUS_DEPENDANT_JOBS, LABEL_INSTANCE
from benji.k8s_operator.utils import service_account_namespace, keys_exist


class BenjiJob(pykube.Job):

    @staticmethod
    def _setup_manifest(*,
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

    def __init__(self, api: HTTPClient, *, command: List[str], parent_body: Dict[str, Any]) -> None:
        if OperatorContext.operator_config is None:
            raise RuntimeError('Operator configuration has not been loaded.')

        job_manifest = copy.deepcopy(OperatorContext.operator_config.obj['spec']['jobTemplate'])
        self._setup_manifest(manifest=job_manifest, namespace=service_account_namespace(), parent_body=parent_body)

        job_manifest['spec']['template']['spec']['containers'][0]['command'] = command
        job_manifest['spec']['template']['spec']['containers'][0]['args'] = []

        super().__init__(api, job_manifest)


class StorageClass(pykube_APIObject):

    version = 'storage.k8s.io/v1'
    endpoint = 'storageclasses'
    kind = 'StorageClass'


def create_pvc(*, pvc_name: str, pvc_namespace: str, pvc_size: str,
               storage_class_name: str) -> pykube.PersistentVolumeClaim:
    manifest = {
        'kind': 'PersistentVolumeClaim',
        'apiVersion': 'v1',
        'metadata': {
            'namespace': pvc_namespace,
            'name': pvc_name,
        },
        'spec': {
            'storageClassName': storage_class_name,
            'accessModes': ['ReadWriteOnce'],
            'resources': {
                'requests': {
                    'storage': pvc_size
                }
            }
        }
    }

    pvc = pykube.PersistentVolumeClaim(OperatorContext.kubernetes_client, manifest)
    pvc.create()
    return pvc


def create_object_ref(resource_dict: Dict[str, Any], include_gvk: bool = True) -> Dict[str, Any]:
    reference = {
        'name': resource_dict['metadata']['name'],
        'namespace': resource_dict['metadata']['namespace'],
        'uid': resource_dict['metadata']['uid'],
    }

    if include_gvk:
        reference.update({'apiVersion': resource_dict['apiVersion'], 'kind': resource_dict['kind']})

    return reference


def get_parent(*, parent_name: str, parent_namespace: str, logger, crd: APIObject) -> APIObject:
    if parent_namespace is not None:
        logger.debug(f'Getting resource {parent_namespace}/{parent_name}.')
        parent = crd.objects(OperatorContext.kubernetes_client).filter(namespace=parent_namespace).get_by_name(parent_name)
    else:
        logger.debug(f'Getting resource {parent_name}.')
        parent = crd.objects(OperatorContext.kubernetes_client).get_by_name(parent_name)

    return parent


def patch_parent(*, parent_name: str, parent_namespace: Optional[str], logger, crd: APIObject,
                 parent_patch: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        if parent_namespace is not None:
            logger.debug(f'Patching resource {parent_namespace}/{parent_name} with: {parent_patch}')
            api_object: APIObject = crd.objects(
                OperatorContext.kubernetes_client).filter(namespace=parent_namespace).get_by_name(parent_name)
            api_object.patch(strategic_merge_patch=parent_patch)
        else:
            logger.debug(f'Patching resource {parent_name} with: {parent_patch}')
            api_object: APIObject = crd.objects(OperatorContext.kubernetes_client).get_by_name(parent_name)
            api_object.patch(strategic_merge_patch=parent_patch)
    except HTTPError as exception:
        if exception.response.status_code == 404:
            logger.warning(f'{crd.kind}/{parent_namespace}/{parent_name} has gone away before it could be patched.')
        elif exception.response.status_code == 422:
            logger.warning(f'{crd.kind}/{parent_namespace}/{parent_name} could not be patched because it is being deleted.')
        else:
            raise


def update_status_list(lst: List, resource: Dict[str, Any], extra_data: Dict[str, Any]) -> List[Dict[str, any]]:
    new_lst = list(lst)
    for i, value in enumerate(new_lst):
        if value[RESOURCE_STATUS_LIST_OBJECT_REFERENCE]['uid'] == resource['metadata']['uid']:
            new_lst[i] = {RESOURCE_STATUS_LIST_OBJECT_REFERENCE: create_object_ref(resource, include_gvk=False)}
            new_lst[i].update(extra_data)
            break
    else:
        new_lst.append({RESOURCE_STATUS_LIST_OBJECT_REFERENCE: create_object_ref(resource, include_gvk=False)})
        new_lst[-1].update(extra_data)

    return new_lst


def delete_from_status_list(lst: List, resource: Any) -> List[Dict[str, any]]:
    return [
        value for value in lst if not value[RESOURCE_STATUS_LIST_OBJECT_REFERENCE]['uid'] == resource['metadata']['uid']
    ]


class _DependantJobStatus(NamedTuple):
    start_time: str
    completion_time: str
    status: str


def derive_job_status(job_status: Dict[str, Any]) -> _DependantJobStatus:
    if JOB_STATUS_COMPLETION_TIME in job_status:
        status = RESOURCE_JOB_STATUS_SUCCEEDED
    elif JOB_STATUS_START_TIME in job_status:
        if JOB_STATUS_FAILED in job_status and job_status[JOB_STATUS_FAILED] > 0:
            status = RESOURCE_JOB_STATUS_FAILED
        else:
            status = RESOURCE_JOB_STATUS_RUNNING
    else:
        status = RESOURCE_JOB_STATUS_PENDING

    start_time = job_status.get(JOB_STATUS_START_TIME, None)
    completion_time = job_status.get(JOB_STATUS_COMPLETION_TIME, None)

    return _DependantJobStatus(start_time=start_time, completion_time=completion_time, status=status)


def build_dependant_job_status(job_status: Dict[str, Any]) -> Dict[str, Any]:
    derived_status = derive_job_status(job_status)

    dependant_job_status = {}
    if derived_status.start_time is not None:
        dependant_job_status[JOB_STATUS_START_TIME] = derived_status.start_time
    if derived_status.completion_time is not None:
        dependant_job_status[JOB_STATUS_COMPLETION_TIME] = derived_status.completion_time

    dependant_job_status[RESOURCE_STATUS_DEPENDANT_JOBS_STATUS] = derived_status.status

    return dependant_job_status


def build_resource_status_dependant_jobs(status: Dict[str, Any],
                                         job: Dict[str, Any],
                                         delete: bool = False) -> Dict[str, Any]:
    if delete:
        dependant_jobs = delete_from_status_list(status.get(RESOURCE_STATUS_DEPENDANT_JOBS, []), job)
    else:
        dependant_jobs = update_status_list(status.get(RESOURCE_STATUS_DEPENDANT_JOBS, []), job,
                                            build_dependant_job_status(job))

    return {RESOURCE_STATUS_DEPENDANT_JOBS: dependant_jobs}


def track_job_status(reason: str, name: str, namespace: str, meta: Dict[str, Any], body: Dict[str, Any], logger,
                     crd: APIObject, **_) -> None:
    # Only look at events from our namespace
    if namespace != service_account_namespace():
        return

    if reason != 'delete' and 'labels' not in meta or LABEL_PARENT_NAME not in meta['labels']:
        # Stray jobs will be deleted
        logger.warning(f'Job {name} is one of ours but has no or incomplete parent labels, deleting it.')
        pykube.Job(OperatorContext.kubernetes_client, body).delete()
        return

    if LABEL_PARENT_NAMESPACE in meta['labels']:
        parent_namespace = meta['labels'][LABEL_PARENT_NAMESPACE]
    else:
        parent_namespace = None

    parent_name = meta['labels'][LABEL_PARENT_NAME]

    try:
        parent = get_parent(parent_name=parent_name, parent_namespace=parent_namespace, logger=logger, crd=crd)
    except pykube.exceptions.ObjectDoesNotExist:
        logger.warning(f'The parent of job {name} has gone away, skipping status update.')
        return

    if keys_exist(parent.obj, ('metadata.deletionTimestamp')):
        logger.warning(f'The parent of job {name} is in deletion, skipping status update.')

    parent_patch = {
        'status': build_resource_status_dependant_jobs(parent.obj.get('status', {}), body, delete=(reason == 'delete'))
    }
    patch_parent(parent_name=parent_name,
                 parent_namespace=parent_namespace,
                 logger=logger,
                 crd=crd,
                 parent_patch=parent_patch)


def _delete_dependant_jobs(*, jobs: Sequence[pykube.Job], logger) -> None:
    for job in jobs:
        logger.info(f'Deleting dependant job {job.obj["metadata"]["namespace"]}/{job.obj["metadata"]["name"]}.')
        job.delete()


def delete_all_dependant_jobs(*, name: str, namespace: str, kind: str, logger) -> None:
    jobs = pykube.Job.objects(OperatorContext.kubernetes_client).filter(
        namespace=service_account_namespace(),
        selector=f'{LABEL_PARENT_KIND}={kind},{LABEL_PARENT_NAME}={name},{LABEL_PARENT_NAMESPACE}={namespace}')
    if jobs:
        _delete_dependant_jobs(jobs=jobs, logger=logger)


# def delete_old_dependant_jobs(*, name: str, namespace: str, kind: str, logger) -> None:
#     batch_v1_api = kubernetes.client.BatchV1Api()
#     jobs = batch_v1_api.list_namespaced_job(
#             namespace=service_account_namespace(),
#             label_selector=f'{LABEL_PARENT_KIND}={kind},{LABEL_PARENT_NAME}={name},{LABEL_PARENT_NAMESPACE}={namespace}').items
#     failed_jobs = [job for job in jobs if build_dependant_job_status(job['status'])[]]
EVENT_REPORTING_COMPONENT = 'benji'


def create_pvc_event(*, type: str, reason: str, message: str, pvc_namespace: str, pvc_name: str, pvc_uid: str) -> None:
    event_name = '{}-{}'.format(benji_instance, str(uuid.uuid4()))
    # Kubernetes requires a time including microseconds
    event_time = datetime.datetime.utcnow().isoformat(timespec='microseconds') + 'Z'

    # Setting uid is required so that kubectl describe finds the event.
    # And setting firstTimestamp is required so that kubectl shows a proper age for it.
    # See: https://github.com/kubernetes/kubernetes/blob/
    manifest = {
        'apiVersion': 'v1',
        'kind': 'Event',
        'metadata': {
            'name': event_name,
            'namespace': pvc_namespace,
            'labels': {
                LABEL_INSTANCE: benji_instance
            }
        },
        'involvedObject': {
            'apiVersion': 'v1',
            'kind': 'PersistentVolumeClaim',
            'name': pvc_name,
            'namespace': pvc_namespace,
            'uid': pvc_uid
        },
        'eventTime': event_time,
        'firstTimestamp': event_time,
        'lastTimestamp': event_time,
        'type': type,
        'reason': reason,
        # Message can be at most 1024 characters long
        'message': message[:1024],
        'action': 'None',
        'reportingComponent': EVENT_REPORTING_COMPONENT,
        'reportingInstance': running_pod_name,
        'source': {
            'component': 'benji'
        }
    }

    pykube.Event(OperatorContext.kubernetes_client, manifest).create()


class APIObject(pykube_APIObject):

    @classmethod
    def group_version_plural(cls) -> Tuple[str, str, str]:
        group_version = cls.version.split('/')
        return group_version[0], group_version[1], cls.endpoint

    def __hash__(self):
        return hash(self.name)


class NamespacedAPIObject(pykube_NamespacedAPIObject):

    @classmethod
    def group_version_plural(cls) -> Tuple[str, str, str]:
        group_version = cls.version.split('/')
        return group_version[0], group_version[1], cls.endpoint

    def __hash__(self):
        return hash((self.namespace, self.name))
