import os
from typing import Optional, Dict, Any

import pykube
from apscheduler.schedulers.background import BaseScheduler

from .constants import OPERATOR_CONFIG_ENV_NAME, \
    DEFAULT_OPERATOR_CONFIG_NAME

operator_config_name = os.getenv(OPERATOR_CONFIG_ENV_NAME, DEFAULT_OPERATOR_CONFIG_NAME)

operator_config: Optional[Dict[str, Any]] = None

scheduler: Optional[BaseScheduler] = None

kubernetes_client = pykube.HTTPClient(pykube.KubeConfig.from_env())

# These ensure that our handlers are registered with Kopf
from .crd import operator_config, version, retention_schedule, backup_schedule, restore
