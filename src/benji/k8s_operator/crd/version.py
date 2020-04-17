from typing import Dict, Any, Optional

import kopf
from requests import HTTPError

from benji.api import APIClient
from benji.k8s_operator import kubernetes_client
from benji.k8s_operator.constants import API_VERSION, API_GROUP, LABEL_INSTANCE, LABEL_K8S_PVC_NAMESPACE, \
    LABEL_K8S_PVC_NAME, LABEL_K8S_PV_NAME, LABEL_K8S_STORAGE_CLASS_NAME, LABEL_K8S_PV_TYPE
from benji.k8s_operator.resources import NamespacedAPIObject

# Key names in version
VERSION_UID = 'uid'
VERSION_DATE = 'date'
VERSION_VOLUME = 'volume'
VERSION_SNAPSHOT = 'snapshot'
VERSION_SIZE = 'size'
VERSION_STORAGE = 'storage'
VERSION_BYTES_READ = 'bytes_read'
VERSION_BYTES_WRITTEN = 'bytes_written'
VERSION_BYTES_DEDUPLICATED = 'bytes_deduplicated'
VERSION_BYTES_SPARSE = 'bytes_sparse'
VERSION_DURATION = 'duration'
VERSION_PROTECTED = 'protected'
VERSION_STATUS = 'status'
VERSION_LABELS = 'labels'

# Names used in version resource
K8S_VERSION_SPEC_DATE = 'date'
K8S_VERSION_SPEC_VOLUME = 'volume'
K8S_VERSION_SPEC_SNAPSHOT = 'snapshot'
K8S_VERSION_SPEC_SIZE = 'size'
K8S_VERSION_SPEC_STORAGE = 'storage'
K8S_VERSION_SPEC_BYTES_READ = 'bytesRead'
K8S_VERSION_SPEC_BYTES_WRITTEN = 'bytesWritten'
K8S_VERSION_SPEC_BYTES_DEDUPLICATED = 'bytesDeduplicated'
K8S_VERSION_SPEC_BYTES_SPARSE = 'bytesSparse'
K8S_VERSION_SPEC_DURATION = 'duration'
K8S_VERSION_SPEC_PERSISTENT_VOLUME_CLAIM_NAME = 'persistentVolumeClaimName'

K8S_VERSION_STATUS_PROTECTED = 'protected'
K8S_VERSION_STATUS_STATUS = 'status'


class BenjiVersion(NamespacedAPIObject):

    version = f'{API_GROUP}/{API_VERSION}'
    endpoint = 'benjiversions'
    kind = 'BenjiVersion'

    @classmethod
    def create_or_update_from_version(cls, *, version: Dict[str, Any], logger=None) -> 'BenjiVersion':
        labels = version[VERSION_LABELS]

        required_label_names = [
            LABEL_INSTANCE, LABEL_K8S_PVC_NAME, LABEL_K8S_PVC_NAMESPACE, LABEL_K8S_PV_NAME, LABEL_K8S_PV_TYPE,
            LABEL_K8S_STORAGE_CLASS_NAME
        ]

        for label_name in required_label_names:
            if label_name not in labels:
                raise KeyError(f'Version {version["uid"]} is missing label {label_name}, skipping update.')

        namespace = labels[LABEL_K8S_PVC_NAMESPACE]

        target: Dict[str, Any] = {
            'apiVersion': cls.version,
            'kind': cls.kind,
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

        logger.debug(f'Creating or updating version resource {namespace}/{version["uid"]}.')
        try:
            version_obj: NamespacedAPIObject = cls(kubernetes_client).filter(namespace=namespace).get_by_name(
                version["uid"])
            actual = version_obj.obj

            # Keep other labels and annotations but overwrite our own
            actual['metadata']['labels'] = actual['metadata'].get('labels', {})
            actual['metadata']['labels'].update(target['metadata']['labels'])
            target['metadata']['labels'] = actual['metadata']['labels']

            actual['metadata']['annotations'] = actual['metadata'].get('annotations', {})
            actual['metadata']['annotations'].update(target['metadata']['annotations'])
            target['metadata']['annotations'] = actual['metadata']['annotations']

            # Keep other status field but overwrite protected and status
            actual['status'] = actual.get('status', {})
            actual['status'].update(target['status'])
            target['status'] = actual['status']

            version_obj.set_obj(target)
            version_obj.update(is_strategic=False)
        except HTTPError as exception:
            if exception.response.status_code == 404:
                version_obj = cls(kubernetes_client, target)
                version_obj.create()
            else:
                raise

        return version_obj


def check_version_access(benji: APIClient, version_uid: str, crd: Dict[Any, str]) -> None:
    try:
        version = benji.core_v1_get(version_uid=version_uid)
    except KeyError as exception:
        raise kopf.PermanentError(str(exception))

    crd_namespace = crd['metadata']['namespace']
    try:
        version_namespace = version[VERSION_LABELS][LABEL_K8S_PVC_NAMESPACE]
    except KeyError:
        raise kopf.PermanentError(f'Version is missing {LABEL_K8S_PVC_NAMESPACE} label, permission denied.')

    if crd_namespace != version_namespace:
        raise kopf.PermanentError('Version namespace label does not match resource namespace, permission denied')


@kopf.on.field(*BenjiVersion.group_version_plural(), field='status.protected')
def benji_protect(name: str, status: Dict[str, Any], body: Dict[str, Any], **_) -> Optional[Dict[str, Any]]:
    benji = APIClient()
    check_version_access(benji, name, body)
    protected = status.get('protected', False)
    benji.protect(name, protected)


@kopf.on.delete(*BenjiVersion.group_version_plural())
def benji_remove(name: str, body: Dict[str, Any], **_) -> Optional[Dict[str, Any]]:
    benji = APIClient()
    try:
        benji.core_v1_get(name)
    except KeyError:
        return
    check_version_access(benji, name, body)
    benji.rm(name)
