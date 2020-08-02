import os
from typing import Optional, Dict, Any

import pykube
from apscheduler.schedulers.background import BaseScheduler, BackgroundScheduler

OPERATOR_CONFIG_ENV_NAME = 'BENJI_K8S_OPERATOR_CONFIG_NAME'
DEFAULT_OPERATOR_CONFIG_NAME = 'benji'


class _OperatorContext:

    def __init__(self):
        self.operator_config_name = os.getenv(OPERATOR_CONFIG_ENV_NAME, DEFAULT_OPERATOR_CONFIG_NAME)
        self.operator_config: Optional[Dict[str, Any]] = None

        job_defaults = {'coalesce': True, 'max_instances': 1, 'misfire_grace_time': 60}
        self.apscheduler: Optional[BaseScheduler] = BackgroundScheduler(job_defaults=job_defaults, timezone='UTC')

        self.kubernetes_client = pykube.HTTPClient(pykube.KubeConfig.from_env())


OperatorContext = _OperatorContext()

# These ensure that our handlers are registered with Kopf
from .crd import operator_config, version, retention_schedule, backup_schedule, restore
