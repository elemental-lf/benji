import logging
import urllib.error

from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway, generate_latest

from benji.helpers.settings import prom_push_gateway, benji_instance

logger = logging.getLogger()
command_registry = CollectorRegistry()
backup_registry = CollectorRegistry()
version_status_registry = CollectorRegistry()


def push(registry: CollectorRegistry):
    if prom_push_gateway is not None and benji_instance is not None:
        logger.info(f'Pushing Prometheus metrics to gateway {prom_push_gateway}.')
        logger.debug(generate_latest(registry).decode('utf-8'))
        try:
            pushadd_to_gateway(prom_push_gateway, job=benji_instance, registry=registry)
        except urllib.error.URLError as exception:
            logger.error(f'Pushing Prometheus metrics failed with a {type(exception).__name__} exception: {str(exception)}')
            logger.error('Ignoring.')


# yapf: disable
command_start_time = Gauge('benji_command_start_time', labelnames=['command'], documentation='Start time of Benji command (time_t)', registry=command_registry)
command_completion_time = Gauge('benji_command_completion_time', labelnames=['command'], documentation='Completion time of Benji command (time_t)', registry=command_registry)
command_runtime_seconds = Gauge('benji_command_runtime_seconds', labelnames=['command'], documentation='Runtime of Benji command (seconds)', registry=command_registry)
command_status_succeeded = Gauge('benji_command_status_succeeded', labelnames=['command'], documentation='Benji command succeeded', registry=command_registry)
command_status_failed = Gauge('benji_command_status_failed', labelnames=['command'], documentation='Benji command failed', registry=command_registry)

backup_start_time = Gauge('benji_backup_start_time', labelnames=['volume'], documentation='Start time of Benji backup command (time_t)', registry=backup_registry)
backup_completion_time = Gauge('benji_backup_completion_time', labelnames=['volume'], documentation='Completion time of Benji backup command (time_t)', registry=backup_registry)
backup_runtime_seconds = Gauge('benji_backup_runtime_seconds', labelnames=['volume'], documentation='Runtime of Benji backup command (seconds)', registry=backup_registry)
backup_status_succeeded = Gauge('benji_backup_status_succeeded', labelnames=['volume'], documentation='Benji backup command succeeded', registry=backup_registry)
backup_status_failed = Gauge('benji_backup_status_failed', labelnames=['volume'], documentation='Benji backup command failed', registry=backup_registry)

invalid_versions = Gauge('benji_invalid_versions', documentation='Number of invalid backup versions', registry=version_status_registry)
older_incomplete_versions = Gauge('benji_older_incomplete_versions', documentation='Number of older incomplete versions', registry=version_status_registry)
# yapf: enable
