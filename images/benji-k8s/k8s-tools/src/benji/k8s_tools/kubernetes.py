import datetime
import json
import logging
import re
import time
import uuid
from subprocess import TimeoutExpired, CalledProcessError
from typing import List, Union, Tuple, Optional

import attrs
import kubernetes
from kubernetes.stream import stream
from kubernetes.stream.ws_client import ERROR_CHANNEL, STDOUT_CHANNEL, STDERR_CHANNEL

from benji.helpers.settings import running_pod_name, benji_instance
from benji.helpers.utils import keys_exist, key_get

SERVICE_NAMESPACE_FILENAME = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'

NODE_RBD_MOUNT_PATH_FORMAT = '/var/lib/kubelet/plugins/kubernetes.io/rbd/mounts/rbd-image-{image}'
NODE_CSI_MOUNT_PATH_FORMAT = '/var/lib/kubelet/plugins/kubernetes.io/csi/pv/{pv}/globalmount/{volume_handle}'

logger = logging.getLogger()


def load_config() -> None:
    try:
        kubernetes.config.load_incluster_config()
        logger.debug('Configured in cluster with service account.')
    except Exception:
        try:
            kubernetes.config.load_kube_config()
            logger.debug('Configured via kubeconfig file.')
        except Exception:
            raise RuntimeError('No Kubernetes configuration found.')


def service_account_namespace() -> str:
    with open(SERVICE_NAMESPACE_FILENAME, 'r') as f:
        namespace = f.read()
        if namespace == '':
            raise RuntimeError(f'{SERVICE_NAMESPACE_FILENAME} is empty.')
    return namespace


# This was implemented with version 10.0.0 of the Python kubernetes client in mind. But there are several open issues
# and PRs regarding encoding and timeout with might affect us in the future:
#
#  https://github.com/kubernetes-client/python-base/issues/106
#  https://github.com/kubernetes-client/python-base/pull/143
#  https://github.com/kubernetes-client/python-base/pull/78
#
# kubectl uses a POST request to establish the pod connection. We mimic this here by using
# connect_post_namespaced_pod_exec. The examples from the kubernetes client use connect_get_namespaced_pod_exec instead.
# There shouldn't be any differences in functionality but the settings in the RBAC role are different (create vs. get)
# which is why we follow the kubectl implementation here.
def pod_exec(args: List[str],
             *,
             name: str,
             namespace: str,
             container: str = None,
             timeout: float = float("inf")) -> Tuple[str, str]:
    core_v1_api = kubernetes.client.CoreV1Api()
    logger.debug('Running command in pod {}/{}: {}.'.format(namespace, name, ' '.join(args)))
    ws_client = stream(core_v1_api.connect_post_namespaced_pod_exec,
                       name,
                       namespace,
                       command=args,
                       container=container,
                       stderr=True,
                       stdin=False,
                       stdout=True,
                       tty=False,
                       _preload_content=False)

    start = time.time()
    while ws_client.is_open() and time.time() - start < timeout:
        ws_client.update(timeout=(timeout - time.time() + start))

    stdout_channel = ws_client.read_channel(STDOUT_CHANNEL, timeout=0)
    stderr_channel = ws_client.read_channel(STDERR_CHANNEL, timeout=0)
    error_channel = ws_client.read_channel(ERROR_CHANNEL, timeout=0)
    ws_client.close()
    if error_channel == '':
        raise TimeoutExpired(cmd=args, timeout=timeout, output=stdout_channel, stderr=stderr_channel)
    else:
        error_channel_object = json.loads(error_channel)

        # Failure example:
        # {
        #   "metadata": {},
        #   "status": "Failure",
        #   "message": "command terminated with non-zero exit code: Error executing in Docker Container: 126",
        #   "reason": "NonZeroExitCode",
        #   "details": {
        #     "causes": [
        #       {
        #         "reason": "ExitCode",
        #         "message": "126"
        #       }
        #     ]
        #   }
        # }
        #
        # Non-zero exit codes from the command ran are also returned this way.
        #
        # Success example:
        # {"metadata":{},"status":"Success"}
        #
        # See: https://github.com/kubernetes/kubernetes/blob/87b744715ec6952c45d04253dc7b63fc3cfe1ddc/staging/src/k8s.io/client-go/tools/remotecommand/v4.go#L82
        #      https://github.com/kubernetes-client/python-base/blob/master/stream/ws_client.py

        assert isinstance(error_channel_object, dict)
        assert 'status' in error_channel_object
        if error_channel_object['status'] == 'Success':
            pass
        elif error_channel_object['status'] == 'Failure' and 'reason' in error_channel_object and error_channel_object['reason'] == 'NonZeroExitCode':
            assert 'details' in error_channel_object
            assert 'causes' in error_channel_object['details']
            assert isinstance(error_channel_object['details']['causes'], list)
            for cause in error_channel_object['details']['causes']:
                assert 'reason' in cause
                if cause['reason'] != 'ExitCode':
                    continue
                assert 'message' in cause
                raise CalledProcessError(returncode=int(cause["message"]),
                                         cmd=args,
                                         output=stdout_channel,
                                         stderr=stderr_channel)
        else:
            raise RuntimeError(f'Unknown stream status: {error_channel_object["status"]}/{error_channel_object.get("reason", "mot-set")}.')

    return stdout_channel, stderr_channel


def create_pvc_event(*, type: str, reason: str, message: str, pvc_namespace: str, pvc_name: str,
                     pvc_uid: str) -> kubernetes.client.models.v1_event.V1Event:
    event_name = '{}-{}'.format(benji_instance, str(uuid.uuid4()))
    # Kubernetes requires a time including microseconds
    event_time = datetime.datetime.utcnow().isoformat(timespec='microseconds') + 'Z'

    # Setting uid is required so that kubectl describe finds the event.
    # And setting firstTimestamp is required so that kubectl shows a proper age for it.
    # See: https://github.com/kubernetes/kubernetes/blob/
    event = {
        'apiVersion': 'v1',
        'kind': 'Event',
        'metadata': {
            'name': event_name,
            'namespace': pvc_namespace,
            'labels': {
                'reporting-component': 'benji',
                'reporting-instance': running_pod_name,
                'benji-backup.me/instance': benji_instance
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
        'reportingComponent': 'benji',
        'reportingInstance': running_pod_name,
        'source': {
            'component': 'benji'
        }
    }

    core_v1_api = kubernetes.client.CoreV1Api()
    return core_v1_api.create_namespaced_event(namespace=pvc_namespace, body=event)


def create_pvc(pvc_name: str, pvc_namespace: int, pvc_size: str,
               pvc_storage_class: str) -> kubernetes.client.models.v1_persistent_volume_claim.V1PersistentVolumeClaim:
    pvc = {
        'kind': 'PersistentVolumeClaim',
        'apiVersion': 'v1',
        'metadata': {
            'namespace': pvc_namespace,
            'name': pvc_name,
        },
        'spec': {
            'storageClassName': pvc_storage_class,
            'accessModes': ['ReadWriteOnce'],
            'resources': {
                'requests': {
                    'storage': pvc_size
                }
            }
        }
    }

    core_v1_api = kubernetes.client.CoreV1Api()
    return core_v1_api.create_namespaced_persistent_volume_claim(namespace=pvc_namespace, body=pvc)


# Mark this as private because the class should only be instantiated by determine_rbd_info_from_pv.
@attrs.define(kw_only=True)
class _RBDInfo:
    pool: str
    namespace: str = attrs.field(default='')
    image: str
    mount_point: str = attrs.field(default=None)


def determine_rbd_info_from_pv(
        pv: kubernetes.client.models.v1_persistent_volume.V1PersistentVolume) -> Optional[_RBDInfo]:
    rbd_info = None

    if keys_exist(pv.spec, ['rbd.pool', 'rbd.image']):
        # Native Kubernetes RBD PV
        rbd_info = _RBDInfo(pool=key_get(pv.spec, 'rbd.pool'),
                            image=key_get(pv.spec, 'rbd.image'),
                            mount_point=NODE_RBD_MOUNT_PATH_FORMAT.format(image=key_get(pv.spec, 'rbd.image')))
    elif keys_exist(pv.spec, ['flex_volume.options.pool', 'flex_volume.options.image',  'flex_volume.driver']) \
            and key_get(pv.spec, 'flex_volume.driver').startswith('ceph.rook.io/'):
        # Don't know how mount paths are structured for flex volumes, but as flex volumes are obsolete anyway we don't bother.
        rbd_info = _RBDInfo(pool=key_get(pv.spec, 'flex_volume.options.pool'),
                            image=key_get(pv.spec, 'flex_volume.options.image'))
    # rbd.csi.ceph.com is for the stock CSI driver, the second one is for Rook which adds the namespace to the driver
    # name.
    elif keys_exist(pv.spec, ['csi.driver', 'csi.volume_handle', 'csi.volume_attributes.pool', 'csi.volume_attributes.imageName']) \
        and (key_get(pv.spec, 'csi.driver') == 'rbd.csi.ceph.com' or key_get(pv.spec, 'csi.driver').endswith('.rbd.csi.ceph.com')):
        rbd_info = _RBDInfo(pool=key_get(pv.spec, 'csi.volume_attributes.pool'),
                            namespace=key_get(pv.spec, 'csi.volume_attributes.radosNamespace', ''),
                            image=key_get(pv.spec, 'csi.volume_attributes.imageName'),
                            mount_point=NODE_CSI_MOUNT_PATH_FORMAT.format(pv=pv.metadata.name,
                                                                          volume_handle=key_get(
                                                                              pv.spec, 'csi.volume_handle')))

    return rbd_info


# This is taken from https://github.com/kubernetes-client/python/pull/855 with minimal changes.
#
# Copyright 2019 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


def parse_quantity(quantity: Union[str, int, float]) -> float:
    """
    Parse kubernetes canonical form quantity like 200Mi to an float number.
    Supported SI suffixes:
    base1024: Ki | Mi | Gi | Ti | Pi | Ei
    base1000: m | "" | k | M | G | T | P | E

    Input:
    quanity: string. kubernetes canonical form quantity

    Returns:
    float

    Raises:
    ValueError on invalid or unknown input
    """
    exponents = {"m": -1, "K": 1, "k": 1, "M": 2, "G": 3, "T": 4, "P": 5, "E": 6}
    pattern = r"^(\d+)([^\d]{1,2})?$"

    if isinstance(quantity, (int, float)):
        return float(quantity)

    quantity = str(quantity)

    res = re.match(pattern, quantity)
    if not res:
        raise ValueError("{} did not match pattern {}".format(quantity, pattern))
    number, suffix = res.groups()
    number_float = float(number)

    if suffix is None:
        return number_float

    suffix = res.groups()[1]

    if suffix.endswith("i"):
        base = 1024
    elif len(suffix) == 1:
        base = 1000
    else:
        raise ValueError("{} has unknown suffix".format(quantity))

    # handle SI inconsistency
    if suffix == "ki":
        raise ValueError("{} has unknown suffix".format(quantity))

    if suffix[0] not in exponents:
        raise ValueError("{} has unknown suffix".format(quantity))

    exponent = exponents[suffix[0]]
    return number_float * (base**exponent)


# End of included content
