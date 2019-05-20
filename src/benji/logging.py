#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import logging
import logging.config
import os
import sys
import threading
import warnings
from typing import Optional, Dict

import structlog
from structlog._frames import _find_first_app_frame_and_name

from benji.exception import UsageError
from benji.formatrenderer import FormatRenderer

logger = structlog.get_logger()


def _sl_processor_add_source_context(_, __, event_dict: Dict) -> Dict:
    frame, name = _find_first_app_frame_and_name([__name__, 'logging'])
    event_dict['file'] = frame.f_code.co_filename
    event_dict['line'] = frame.f_lineno
    event_dict['function'] = frame.f_code.co_name
    return event_dict


def _sl_processor_add_process_context(_, __, event_dict: Dict) -> Dict:
    event_dict['process'] = os.getpid()
    event_dict['thread_name'] = threading.current_thread().name
    event_dict['thread_id'] = threading.get_ident()
    return event_dict


_sl_processor_timestamper = structlog.processors.TimeStamper(utc=True)

_sl_foreign_pre_chain = [
    structlog.stdlib.add_log_level,
    _sl_processor_timestamper,
    _sl_processor_add_source_context,
    _sl_processor_add_process_context,
]

_sl_processors = [
    structlog.stdlib.add_log_level,
    structlog.stdlib.PositionalArgumentsFormatter(),
    _sl_processor_timestamper,
    _sl_processor_add_source_context,
    _sl_processor_add_process_context,
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
    structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
]

structlog.configure(
    processors=_sl_processors,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)


def init_logging(logfile: Optional[str],
                 console_level: str,
                 console_formatter: str = "console-plain",
                 logfile_formatter: str = 'legacy') -> None:

    logging_config: Dict = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "console-plain": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processor": FormatRenderer(colors=False, fmt='{log_color}{level_uc:>8s}: {event:s}'),
                "foreign_pre_chain": _sl_foreign_pre_chain,
            },
            "console-colored": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processor": FormatRenderer(colors=True, fmt='{log_color}{level_uc:>8s}: {event:s}'),
                "foreign_pre_chain": _sl_foreign_pre_chain,
            },
            "legacy": {
                "()":
                structlog.stdlib.ProcessorFormatter,
                "processor":
                FormatRenderer(
                    colors=False,
                    fmt='{timestamp_local_ctime} {process:d}/{thread_name:s} {file:s}:{line:d} {level_uc:s} {event:s}'),
                "foreign_pre_chain":
                _sl_foreign_pre_chain,
            },
            "json": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processor": structlog.processors.JSONRenderer(),
                "foreign_pre_chain": _sl_foreign_pre_chain,
            },
        },
        "handlers": {
            "console": {
                "level": None,  # Filled in
                "class": "logging.StreamHandler",
                "formatter": None,  # Filled in
                "stream": "ext://sys.stderr",
            },
            "file": {
                "level": None,  # Filled in
                "class": "logging.handlers.WatchedFileHandler",
                "filename": None,  # Filled in
                "formatter": None,  # Filled in
            },
        },
        "loggers": {
            "": {
                "handlers": None,  # Filled in
                "level": "DEBUG",
                "propagate": True,
            },
        }
    }

    if console_formatter not in logging_config['formatters'].keys():
        raise UsageError('Event formatter {} is unknown.'.format(console_formatter))

    if logfile_formatter not in logging_config['formatters'].keys():
        raise UsageError('Event formatter {} is unknown.'.format(logfile_formatter))

    logging_config['handlers']['console']['formatter'] = console_formatter
    logging_config['handlers']['console']['level'] = console_level

    if logfile is not None:
        logging_config['handlers']['file']['filename'] = logfile
        logging_config['handlers']['file']['level'] = min(logging.getLevelName(console_level), logging.INFO)
        logging_config['handlers']['file']['formatter'] = logfile_formatter
    else:
        del (logging_config['handlers']['file'])

    logging_config['loggers']['']['handlers'] = logging_config['handlers'].keys()

    logging.config.dictConfig(logging_config)

    # silence alembic
    logging.getLogger('alembic').setLevel(logging.WARN)
    # silence boto3
    # See https://github.com/boto/boto3/issues/521
    logging.getLogger('boto3').setLevel(logging.WARN)
    logging.getLogger('botocore').setLevel(logging.WARN)
    logging.getLogger('nose').setLevel(logging.WARN)
    # This disables ResourceWarnings from boto3 which are normal
    # See: https://github.com/boto/boto3/issues/454
    warnings.filterwarnings("ignore",
                            category=ResourceWarning,
                            message=r'unclosed.*<(?:ssl.SSLSocket|socket\.socket).*>')
    # silence b2
    logging.getLogger('b2').setLevel(logging.WARN)

    if os.getenv('BENJI_DEBUG_SQL') == '1':
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    logger.info('$ ' + ' '.join(sys.argv))


# Source: https://stackoverflow.com/questions/6234405/logging-uncaught-exceptions-in-python/16993115#16993115
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = handle_exception
