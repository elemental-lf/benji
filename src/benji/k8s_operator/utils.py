import inspect
from typing import Dict, Any

import kopf

from benji.helpers.constants import VERSION_LABELS, LABEL_K8S_PVC_NAMESPACE
from benji.helpers.restapi import BenjiRESTClient


def get_caller_name() -> str:
    """Returns the name of the calling function"""
    return inspect.getouterframes(inspect.currentframe())[1].function


def check_version_access(benji: BenjiRESTClient, version_uid: str, crd: Dict[Any, str]) -> None:
    try:
        version = benji.get_version_by_uid(version_uid)
    except KeyError as exception:
        raise kopf.PermanentError(str(exception))

    crd_namespace = crd['metadata']['namespace']
    try:
        version_namespace = version[VERSION_LABELS][LABEL_K8S_PVC_NAMESPACE]
    except KeyError:
        raise kopf.PermanentError(f'Version is missing {LABEL_K8S_PVC_NAMESPACE} label, permission denied.')

    if crd_namespace != version_namespace:
        raise kopf.PermanentError('Version namespace label does not match resource namespace, permission denied')
