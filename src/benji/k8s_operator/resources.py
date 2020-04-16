import copy
import datetime
import logging
import uuid
from typing import Dict, Any, List, Optional, Mapping, Sequence, NamedTuple, MutableMapping, Generator

import pykube
from benji.k8s_operator import kubernetes_client

import benji.k8s_operator
from benji.helpers.constants import LABEL_INSTANCE, VERSION_LABELS, LABEL_K8S_PVC_NAME, LABEL_K8S_PVC_NAMESPACE, \
    LABEL_K8S_PV_NAME, LABEL_K8S_PV_TYPE, LABEL_K8S_STORAGE_CLASS_NAME, K8S_VERSION_SPEC_DATE, VERSION_DATE, \
    K8S_VERSION_SPEC_VOLUME, VERSION_VOLUME, K8S_VERSION_SPEC_SNAPSHOT, VERSION_SNAPSHOT, K8S_VERSION_SPEC_SIZE, \
    VERSION_SIZE, K8S_VERSION_SPEC_STORAGE, VERSION_STORAGE, K8S_VERSION_SPEC_BYTES_READ, VERSION_BYTES_READ, \
    K8S_VERSION_SPEC_BYTES_WRITTEN, VERSION_BYTES_WRITTEN, K8S_VERSION_SPEC_BYTES_DEDUPLICATED, \
    VERSION_BYTES_DEDUPLICATED, K8S_VERSION_SPEC_BYTES_SPARSE, VERSION_BYTES_SPARSE, K8S_VERSION_SPEC_DURATION, \
    VERSION_DURATION, K8S_VERSION_SPEC_PERSISTENT_VOLUME_CLAIM_NAME, K8S_VERSION_STATUS_PROTECTED, VERSION_PROTECTED, \
    K8S_VERSION_STATUS_STATUS, VERSION_STATUS
from benji.helpers.settings import benji_instance, running_pod_name
from benji.k8s_operator.utils import service_account_namespace
from benji.k8s_operator.constants import LABEL_PARENT_KIND, LABEL_PARENT_NAMESPACE, LABEL_PARENT_NAME, CRD, \
    RESOURCE_STATUS_LIST_OBJECT_REFERENCE, JOB_STATUS_START_TIME, JOB_STATUS_COMPLETION_TIME, \
    RESOURCE_STATUS_DEPENDANT_JOBS_STATUS, RESOURCE_JOB_STATUS_SUCCEEDED, JOB_STATUS_FAILED, RESOURCE_JOB_STATUS_FAILED, \
    RESOURCE_JOB_STATUS_RUNNING, RESOURCE_JOB_STATUS_PENDING, RESOURCE_STATUS_DEPENDANT_JOBS, CRD_VERSION


class JobResource(Mapping):

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

    def __init__(self, command: List[str], *, parent_body: Dict[str, Any], logger) -> None:
        if benji.k8s_operator.operator_config is None:
            raise RuntimeError('Operator configuration has not been loaded.')

        job_manifest = copy.deepcopy(benji.k8s_operator.operator_config['spec']['jobTemplate'])
        self._setup_manifest(manifest=job_manifest, namespace=service_account_namespace(), parent_body=parent_body)

        job_manifest['spec']['template']['spec']['containers'][0]['command'] = command
        job_manifest['spec']['template']['spec']['containers'][0]['args'] = []

        # Actually create the job via the Kubernetes API.
        logger.debug(f'Creating Job: {job_manifest}')
        self._k8s_resource = pykube.Job(kubernetes_client, job_manifest).create().obj

    def __getitem__(self, item):
        return self._k8s_resource[item]

    def __len__(self):
        return len(self._k8s_resource)

    def __iter__(self):
        return iter(self._k8s_resource)

    def __hash__(self):
        return hash((self._k8s_resource['metadata']['name'], self._k8s_resource['metadata']['namespace']))


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

    pvc = pykube.PersistentVolumeClaim(kubernetes_client, manifest)
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
    try:
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
    except ApiException as exception:
        if exception.status == 404:
            logger.warning(f'{crd.kind}/{parent_namespace}/{parent_name} has gone away before it could be patched.')
        elif exception.status == 422:
            logger.warning(f'{crd.kind}/{parent_namespace}/{parent_name} could not be patched because it is being deleted.')
        else:
            raise exception


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


def _delete_dependant_jobs(*, jobs: Sequence[kubernetes.client.V1Job], logger) -> None:
    batch_v1_api = kubernetes.client.BatchV1Api()
    for job in jobs:
        try:
            logger.info(f'Deleting dependant job {job.metadata.namespace}/{job.metadata.name}.')
            batch_v1_api.delete_namespaced_job(namespace=job.metadata.namespace, name=job.metadata.name)
        except ApiException as exception:
            if exception.status == 404:
                logger.warning(f'Job {job.metadata.namespace}/{job.metadata.name} has gone away before it could be deleted.')
            else:
                raise exception


def delete_all_dependant_jobs(*, name: str, namespace: str, kind: str, logger) -> None:
    batch_v1_api = kubernetes.client.BatchV1Api()
    jobs = batch_v1_api.list_namespaced_job(
        namespace=service_account_namespace(),
        label_selector=f'{LABEL_PARENT_KIND}={kind},{LABEL_PARENT_NAME}={name},{LABEL_PARENT_NAMESPACE}={namespace}').items
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

    pykube.Event(kubernetes_client, manifest).create()


class BenjiVersionResource(MutableMapping):

    def __init__(self, *, name: str, namespace: str, logger=None):
        self.name = name
        self.namespace = namespace
        self.logger = logger if logger else logging.getLogger()
        self._k8s_resource: Dict[str, Any] = {}

    def populate(self) -> 'BenjiVersionResource':
        custom_objects_api = kubernetes.client.CustomObjectsApi()
        self._k8s_resource = custom_objects_api.get_namespaced_custom_object(group=CRD_VERSION.api_group,
                                                                             version=CRD_VERSION.api_version,
                                                                             plural=CRD_VERSION.plural,
                                                                             namespace=self.namespace,
                                                                             name=self.name)
        return self

    @classmethod
    def create_or_replace(cls, *, version: Dict[str, Any], logger=None) -> 'BenjiVersionResource':
        labels = version[VERSION_LABELS]

        required_label_names = [
            LABEL_INSTANCE, LABEL_K8S_PVC_NAME, LABEL_K8S_PVC_NAMESPACE, LABEL_K8S_PV_NAME, LABEL_K8S_PV_TYPE,
            LABEL_K8S_STORAGE_CLASS_NAME
        ]

        for label_name in required_label_names:
            if label_name not in labels:
                raise KeyError(f'Version {version["uid"]} is missing label {label_name}, skipping update.')

        namespace = labels[LABEL_K8S_PVC_NAMESPACE]

        body: Dict[str, Any] = {
            'apiVersion': 'benji-backup.me/v1alpha1',
            'kind': 'BenjiVersion',
            'metadata': {
                'name': version['uid'],
                'namespace': namespace,
                'annotations': {},
                'labels': {
                    LABEL_INSTANCE: labels[LABEL_INSTANCE],
                },
            },
            'spec': {
                K8S_VERSION_SPEC_DATE: version[VERSION_DATE],
                K8S_VERSION_SPEC_VOLUME: version[VERSION_VOLUME],
                K8S_VERSION_SPEC_SNAPSHOT: version[VERSION_SNAPSHOT],
                K8S_VERSION_SPEC_SIZE: str(version[VERSION_SIZE]),
                K8S_VERSION_SPEC_STORAGE: version[VERSION_STORAGE],
                K8S_VERSION_SPEC_BYTES_READ: str(version[VERSION_BYTES_READ]),
                K8S_VERSION_SPEC_BYTES_WRITTEN: str(version[VERSION_BYTES_WRITTEN]),
                K8S_VERSION_SPEC_BYTES_DEDUPLICATED: str(version[VERSION_BYTES_DEDUPLICATED]),
                K8S_VERSION_SPEC_BYTES_SPARSE: str(version[VERSION_BYTES_SPARSE]),
                K8S_VERSION_SPEC_DURATION: version[VERSION_DURATION],
                K8S_VERSION_SPEC_PERSISTENT_VOLUME_CLAIM_NAME: labels[LABEL_K8S_PVC_NAME],
            },
            'status': {
                K8S_VERSION_STATUS_PROTECTED: version[VERSION_PROTECTED],
                K8S_VERSION_STATUS_STATUS: version[VERSION_STATUS].capitalize(),
            }
        }

        self = cls(name=version['uid'], namespace=namespace, logger=logger)
        self.logger.debug(f'Creating or replacing version resource {namespace}/{version["uid"]}.')
        custom_objects_api = kubernetes.client.CustomObjectsApi()
        try:
            self.populate()

            body['metadata']['resourceVersion'] = self['metadata']['resourceVersion']

            # Keep other labels and annotations but overwrite our own
            self['metadata']['labels'] = self['metadata'].get('labels', {})
            self['metadata']['labels'].update(body['metadata']['labels'])
            body['metadata']['labels'] = self['metadata']['labels']

            self['metadata']['annotations'] = self['metadata'].get('annotations', {})
            self['metadata']['annotations'].update(body['metadata']['annotations'])
            body['metadata']['annotations'] = self['metadata']['annotations']

            # Keep other status field but overwrite protected and status
            self['status'] = self.get('status', {})
            self['status'].update(body['status'])
            body['status'] = self['status']

            self = custom_objects_api.replace_namespaced_custom_object(group=CRD_VERSION.api_group,
                                                                       version=CRD_VERSION.api_version,
                                                                       plural=CRD_VERSION.plural,
                                                                       name=version['uid'],
                                                                       namespace=namespace,
                                                                       body=body)
        except ApiException as exception:
            if exception.status == 404:
                self._k8s_resource = custom_objects_api.create_namespaced_custom_object(group=CRD_VERSION.api_group,
                                                                                        version=CRD_VERSION.api_version,
                                                                                        plural=CRD_VERSION.plural,
                                                                                        namespace=namespace,
                                                                                        body=body)
            else:
                raise exception

        return self

    def delete(self) -> None:
        custom_objects_api = kubernetes.client.CustomObjectsApi()
        try:
            self.logger.debug(f'Deleting version resource {self.namespace}/{self.name}.')
            custom_objects_api.delete_namespaced_custom_object(group=CRD_VERSION.api_group,
                                                               version=CRD_VERSION.api_version,
                                                               plural=CRD_VERSION.plural,
                                                               name=self.name,
                                                               namespace=self.namespace,
                                                               body=kubernetes.client.V1DeleteOptions())
        except ApiException as exception:
            if exception.status == 404:
                self.logger.warning(f'Tried to delete non-existing version resource {self.name} in namespace {self.namespace}.')
            else:
                raise exception

    @classmethod
    def list(cls,
             *,
             namespace_label_selector: str = '',
             label_selector: str = '',
             logger=None) -> Generator['BenjiVersionResource', None, None]:
        custom_objects_api = kubernetes.client.CustomObjectsApi()

        for namespace in pykube.Namespace.objects(kubernetes_client).filter(label_selector=namespace_label_selector):
            list_result = custom_objects_api.list_namespaced_custom_object(group=CRD_VERSION.api_group,
                                                                           version=CRD_VERSION.api_version,
                                                                           plural=CRD_VERSION.plural,
                                                                           namespace=namespace.metadata.name,
                                                                           label_selector=label_selector)

            for k8s_resource in list_result['items']:
                self = cls(name=k8s_resource['metadata']['name'],
                           namespace=k8s_resource['metadata']['namespace'],
                           logger=logger)
                self._k8s_resource = k8s_resource
                yield self

    def __getitem__(self, item):
        return self._k8s_resource[item]

    def __setitem__(self, key, value):
        self._k8s_resource[key] = value

    def __delitem__(self, key):
        del self._k8s_resource[key]

    def __len__(self):
        return len(self._k8s_resource)

    def __iter__(self):
        return iter(self._k8s_resource)

    def __hash__(self):
        return hash((self.name, self.namespace))
