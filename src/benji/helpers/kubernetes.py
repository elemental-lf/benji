import datetime
import logging
import uuid
from collections import MutableMapping
from typing import Dict, Any, Generator

import pykube

from benji.helpers import settings
from benji.helpers.constants import LABEL_INSTANCE, LABEL_K8S_PVC_NAMESPACE, LABEL_K8S_PVC_NAME, LABEL_K8S_PV_NAME, \
    LABEL_K8S_STORAGE_CLASS_NAME, LABEL_K8S_PV_TYPE, LABEL_RBD_CLUSTER_FSID, \
    LABEL_RBD_IMAGE_SPEC, VERSION_DATE, VERSION_VOLUME, VERSION_SNAPSHOT, VERSION_SIZE, VERSION_STORAGE, \
    VERSION_BYTES_READ, VERSION_BYTES_WRITTEN, VERSION_BYTES_DEDUPLICATED, VERSION_BYTES_SPARSE, VERSION_DURATION, \
    K8S_VERSION_SPEC_DATE, K8S_VERSION_SPEC_VOLUME, \
    K8S_VERSION_SPEC_SNAPSHOT, K8S_VERSION_SPEC_SIZE, K8S_VERSION_SPEC_STORAGE, K8S_VERSION_SPEC_BYTES_READ, \
    K8S_VERSION_SPEC_BYTES_WRITTEN, K8S_VERSION_SPEC_BYTES_DEDUPLICATED, K8S_VERSION_SPEC_BYTES_SPARSE, \
    K8S_VERSION_SPEC_DURATION, K8S_VERSION_STATUS_PROTECTED, K8S_VERSION_STATUS_STATUS, VERSION_PROTECTED, \
    VERSION_STATUS, VERSION_LABELS, PV_TYPE_RBD, K8S_VERSION_SPEC_PERSISTENT_VOLUME_CLAIM_NAME
from benji.helpers.settings import running_pod_name, benji_instance
from benji.helpers.utils import attrs_exist

SERVICE_NAMESPACE_FILENAME = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'

BENJI_VERSIONS_API_VERSION = 'v1alpha1'
BENJI_VERSIONS_API_GROUP = 'benji-backup.me'
BENJI_VERSIONS_API_PLURAL = 'benjiversions'

EVENT_REPORTING_COMPONENT = 'benji'

logger = logging.getLogger()


def service_account_namespace() -> str:
    with open(SERVICE_NAMESPACE_FILENAME, 'r') as f:
        namespace = f.read()
        if namespace == '':
            raise RuntimeError(f'{SERVICE_NAMESPACE_FILENAME} is empty.')
    return namespace


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

    api = pykube.HTTPClient(pykube.KubeConfig.from_env())
    pykube.Event(api, manifest).create()


class BenjiVersionResource(MutableMapping):

    def __init__(self, *, name: str, namespace: str, logger=None):
        self.name = name
        self.namespace = namespace
        self.logger = logger if logger else logging.getLogger()
        self._k8s_resource: Dict[str, Any] = {}

    def populate(self) -> 'BenjiVersionResource':
        custom_objects_api = kubernetes.client.CustomObjectsApi()
        self._k8s_resource = custom_objects_api.get_namespaced_custom_object(group=BENJI_VERSIONS_API_GROUP,
                                                                             version=BENJI_VERSIONS_API_VERSION,
                                                                             plural=BENJI_VERSIONS_API_PLURAL,
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

            self = custom_objects_api.replace_namespaced_custom_object(group=BENJI_VERSIONS_API_GROUP,
                                                                       version=BENJI_VERSIONS_API_VERSION,
                                                                       plural=BENJI_VERSIONS_API_PLURAL,
                                                                       name=version['uid'],
                                                                       namespace=namespace,
                                                                       body=body)
        except ApiException as exception:
            if exception.status == 404:
                self._k8s_resource = custom_objects_api.create_namespaced_custom_object(group=BENJI_VERSIONS_API_GROUP,
                                                                                        version=BENJI_VERSIONS_API_VERSION,
                                                                                        plural=BENJI_VERSIONS_API_PLURAL,
                                                                                        namespace=namespace,
                                                                                        body=body)
            else:
                raise exception

        return self

    def delete(self) -> None:
        custom_objects_api = kubernetes.client.CustomObjectsApi()
        try:
            self.logger.debug(f'Deleting version resource {self.namespace}/{self.name}.')
            custom_objects_api.delete_namespaced_custom_object(group=BENJI_VERSIONS_API_GROUP,
                                                               version=BENJI_VERSIONS_API_VERSION,
                                                               plural=BENJI_VERSIONS_API_PLURAL,
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

        for namespace in list_namespaces(label_selector=namespace_label_selector):
            list_result = custom_objects_api.list_namespaced_custom_object(group=BENJI_VERSIONS_API_GROUP,
                                                                           version=BENJI_VERSIONS_API_VERSION,
                                                                           plural=BENJI_VERSIONS_API_PLURAL,
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


def list_namespaces(label_selector: str = '') -> Generator[pykube.Namespace, None, None]:

    api = pykube.HTTPClient(pykube.KubeConfig.from_env())
    list_namespace_result = core_v1_api.list_namespace(label_selector=label_selector)
    for namespace in list_namespace_result.items:
        yield namespace


def build_version_labels_rbd(*, pvc, pv, pool: str, image: str, cluster_fsid: str) -> Dict[str, str]:
    version_labels = {
        LABEL_INSTANCE: settings.benji_instance,
        LABEL_K8S_PVC_NAMESPACE: pvc.metadata.namespace,
        LABEL_K8S_PVC_NAME: pvc.metadata.name,
        LABEL_K8S_PV_NAME: pv.metadata.name,
        LABEL_K8S_STORAGE_CLASS_NAME: pv.spec.storage_class_name,
        # RBD specific
        LABEL_K8S_PV_TYPE: PV_TYPE_RBD,
        LABEL_RBD_CLUSTER_FSID: cluster_fsid,
        LABEL_RBD_IMAGE_SPEC: f'{pool}/{image}',
    }

    return version_labels


def determine_rbd_image_location(pv: kubernetes.client.models.V1PersistentVolume) -> (str, str):
    pool, image = None, None

    if attrs_exist(pv.spec, ['rbd.pool', 'rbd.image']):
        logger.debug(f'Considering PersistentVolume {pv.metadata.name} as a native Ceph RBD volume.')
        pool, image = pv.spec.rbd.pool, pv.spec.rbd.image
    elif attrs_exist(pv.spec, ['flex_volume.options', 'flex_volume.driver']):
        logger.debug(f'Considering PersistentVolume {pv.metadata.name} as a Rook Ceph FlexVolume volume.')
        options = pv.spec.flex_volume.options
        driver = pv.spec.flex_volume.driver
        if driver.startswith('ceph.rook.io/') and options.get('pool') and options.get('image'):
            pool, image = options['pool'], options['image']
        else:
            logger.debug(f'PersistentVolume {pv.metadata.name} was provisioned by unknown driver {driver}.')
    elif attrs_exist(pv.spec, ['csi.driver', 'csi.volume_handle', 'csi.volume_attributes']):
        logger.debug(f'Considering PersistentVolume {pv.metadata.name} as a Rook Ceph CSI volume.')
        driver = pv.spec.csi.driver
        volume_handle = pv.spec.csi.volume_handle
        if driver.endswith('.rbd.csi.ceph.com') and pv.spec.csi.volume_attributes.get('pool'):
            pool = pv.spec.csi.volume_attributes['pool']
            image_ids = volume_handle.split('-')
            if len(image_ids) >= 9:
                image = 'csi-vol-' + '-'.join(image_ids[len(image_ids) - 5:])
            else:
                logger.warning(f'PersistentVolume {pv.metadata.name} was provisioned by Rook Ceph CSI, but we do not understand the volumeHandle format: {volume_handle}')

    return pool, image
