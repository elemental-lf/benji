from typing import Any, Dict, List

import uuid

import datetime

import logging

from benji.helpers.settings import pod_name, benji_instance
from benji.helpers.utils import subprocess_run

logger = logging.getLogger()


def get_list(result: Dict[str, Any]) -> List[Any]:
    if 'items' in result:
        assert isinstance(result['items'], list)
        return result['items']
    else:
        return [result]


def create_pvc_event(*, type: str, reason: str, message: str, pvc_namespace: str, pvc_name: str, pvc_uid: str):
    event_name = '{}-{}'.format(benji_instance, str(uuid.uuid4()))
    # Kubernetes requires a time including microseconds
    event_time = datetime.datetime.utcnow().isoformat(timespec='microseconds') + 'Z'

    # Setting uid is required so that kubectl describe finds the event.
    # And setting firstTimestamp is required so that kubectl shows a proper age for it.
    # See: https://github.com/kubernetes/kubernetes/blob/
    event_manifest = f'''
apiVersion: v1
kind: Event
metadata:
  name: "{event_name}"
  namespace: "{pvc_namespace}"
  labels:
    reporting-component: benji
    reporting-instance: "{pod_name}"
    benji-backup.me/instance: "{benji_instance}"
involvedObject:
  apiVersion: v1
  kind: PersistentVolumeClaim
  name: "{pvc_name}"
  namespace: "{pvc_namespace}"
  uid: "{pvc_uid}"
eventTime: "{event_time}"
firstTimestamp: "{event_time}"
lastTimestamp: "{event_time}"
type: "{type}"
reason: "{reason}"
message: "{message}"
action: None
reportingComponent: benji
reportingInstance: "{pod_name}"
source:
  component: benji
'''

    try:
        subprocess_run(['kubectl', 'create', '-f', '-'], input=event_manifest)
    except Exception as exception:
        logger.error(f'Creating Kubernetes event for {pvc_namespace}/{pvc_name} failed with a {exception.__class__.__name__} exception: {str(exception)}')
        pass
