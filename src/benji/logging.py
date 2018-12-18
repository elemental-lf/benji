#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import logging
import sys
from logging.handlers import WatchedFileHandler
from typing import Optional

import colorlog

logger = logging.getLogger(__name__)


def init_logging(logfile: Optional[str], console_level: str, no_color: bool = False):
    handlers = []

    if no_color:
        console = logging.StreamHandler(stream=sys.stderr)
        console.setFormatter(logging.Formatter('%(levelname)8s: %(message)s'))
    else:
        console = colorlog.StreamHandler(stream=sys.stderr)
        console.setFormatter(
            colorlog.TTYColoredFormatter('%(log_color)s%(levelname)8s: %(message)s', stream=sys.stderr))
    console.setLevel(console_level)
    handlers.append(console)

    if logfile is not None:
        logfile_handler = WatchedFileHandler(logfile)
        logfile_handler.setLevel(logging.INFO)
        logfile_handler.setFormatter(logging.Formatter('%(asctime)s [%(process)d] %(message)s'))
        handlers.append(logfile_handler)  # type: ignore # Expects StreamHandler and not WatchedFileHandler, but works...

    logging.basicConfig(handlers=handlers, level=logging.DEBUG)

    # silence alembic
    logging.getLogger('alembic').setLevel(logging.WARN)
    # silence filelock
    logging.getLogger('filelock').setLevel(logging.WARN)
    # silence boto3
    # See https://github.com/boto/boto3/issues/521
    logging.getLogger('boto3').setLevel(logging.WARN)
    logging.getLogger('botocore').setLevel(logging.WARN)
    logging.getLogger('nose').setLevel(logging.WARN)
    # silence b2
    logging.getLogger('b2').setLevel(logging.WARN)

    # To enable query logging
    # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    logger.info('$ ' + ' '.join(sys.argv))


# Source: https://stackoverflow.com/questions/6234405/logging-uncaught-exceptions-in-python/16993115#16993115
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = handle_exception
