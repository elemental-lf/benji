#!/usr/bin/env python3
import sys
import time

import benji.helpers.prometheus as prometheus
import benji.helpers.settings as settings
from benji.helpers.utils import setup_logging, subprocess_run

setup_logging()


def main():
    command = ' '.join(sys.argv[1:])
    start_time = time.time()

    prometheus.command_start_time.labels(command=command).set(start_time)
    try:
        print(subprocess_run(['benji', '--machine-output', '--log-level', settings.benji_log_level] + sys.argv[1:]))
    except Exception as exception:
        prometheus.command_status_failed.labels(command=command).set(1)
        completion_time = time.time()
        prometheus.command_completion_time.labels(command=command).set(completion_time)
        prometheus.command_runtime_seconds.labels(command=command).set(completion_time - start_time)
        prometheus.push(prometheus.command_registry, grouping_key={"command": command})
        raise exception
    else:
        prometheus.command_status_succeeded.labels(command=command).set(1)
        completion_time = time.time()
        prometheus.command_completion_time.labels(command=command).set(completion_time)
        prometheus.command_runtime_seconds.labels(command=command).set(completion_time - start_time)
        prometheus.push(prometheus.command_registry, grouping_key={"command": command})
        sys.exit(0)
