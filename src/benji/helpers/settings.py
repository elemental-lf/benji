import os

running_pod_name = os.getenv('POD_NAME', 'unknown-pod-name')

benji_instance = os.getenv('BENJI_INSTANCE', 'benji-k8s')
benji_log_level = os.getenv('BENJI_LOG_LEVEL', 'INFO')
prom_push_gateway = os.getenv('PROM_PUSH_GATEWAY', None)
