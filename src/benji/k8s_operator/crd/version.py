from typing import Dict, Any, Optional

import kopf

from benji.api import APIClient
from benji.k8s_operator.constants import CRD_VERSION
from benji.k8s_operator.utils import check_version_access


@kopf.on.field(CRD_VERSION.api_group, CRD_VERSION.api_version, CRD_VERSION.plural, field='status.protected')
def benji_protect(name: str, status: Dict[str, Any], body: Dict[str, Any], **_) -> Optional[Dict[str, Any]]:
    benji = APIClient()
    check_version_access(benji, name, body)
    protected = status.get('protected', False)
    benji.protect(name, protected)


@kopf.on.delete(CRD_VERSION.api_group, CRD_VERSION.api_version, CRD_VERSION.plural)
def benji_remove(name: str, body: Dict[str, Any], **_) -> Optional[Dict[str, Any]]:
    benji = APIClient()
    try:
        benji.core_v1_get(name)
    except KeyError:
        return
    check_version_access(benji, name, body)
    benji.rm(name)
