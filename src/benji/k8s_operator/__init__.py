import os
from typing import Optional, Dict, Any

import kubernetes

from .constants import API_ENDPOINT_ENV_NAME, DEFAULT_API_ENDPOINT, OPERATOR_CONFIG_ENV_NAME, \
    DEFAULT_OPERATOR_CONFIG_NAME

api_endpoint = os.getenv(API_ENDPOINT_ENV_NAME, DEFAULT_API_ENDPOINT)
operator_config_name = os.getenv(OPERATOR_CONFIG_ENV_NAME, DEFAULT_OPERATOR_CONFIG_NAME)

operator_config: Optional[Dict[str, Any]] = None

kubernetes.config.load_incluster_config()

# These ensure that our handlers are registered with Kopf
from .crd import operator_config, version, retention_schedule, backup_schedule, restore
