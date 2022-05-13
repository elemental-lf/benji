import json
import logging
import os
import subprocess
import sys
import threading
from json import JSONDecodeError
from typing import Dict, List, Union, Any, Sequence

import structlog
from structlog._frames import _find_first_app_frame_and_name

from benji.helpers.settings import benji_log_level

logger = structlog.get_logger()


def setup_logging() -> None:

    def sl_processor_add_source_context(_, __, event_dict: Dict) -> Dict:
        frame, name = _find_first_app_frame_and_name([__name__, 'logging'])
        event_dict['file'] = frame.f_code.co_filename
        event_dict['line'] = frame.f_lineno
        event_dict['function'] = frame.f_code.co_name
        return event_dict

    def sl_processor_add_process_context(_, __, event_dict: Dict) -> Dict:
        event_dict['process'] = os.getpid()
        event_dict['thread_name'] = threading.current_thread().name
        event_dict['thread_id'] = threading.get_ident()
        return event_dict

    sl_processor_timestamper = structlog.processors.TimeStamper(utc=True)

    sl_foreign_pre_chain = [
        structlog.stdlib.add_log_level,
        sl_processor_timestamper,
        sl_processor_add_source_context,
        sl_processor_add_process_context,
    ]

    sl_processors = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        sl_processor_timestamper,
        sl_processor_add_source_context,
        sl_processor_add_process_context,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ]

    structlog.configure(
        processors=sl_processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(foreign_pre_chain=sl_foreign_pre_chain,
                                                    processors=[
                                                        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                                                        structlog.processors.JSONRenderer(),
                                                    ])

    # StreamHandler() will log to sys.stderr by default.
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    try:
        benji_log_level_int = int(benji_log_level)
    except ValueError:
        try:
            benji_log_level_int = int(logging.getLevelName(benji_log_level.upper()))
        except ValueError:
            logger.warning('Unknown logging level %s, falling back to INFO.', benji_log_level)
            benji_log_level_int = logging.INFO
    root_logger.setLevel(logging.getLevelName(benji_log_level_int))

    # Source: https://stackoverflow.com/questions/6234405/logging-uncaught-exceptions-in-python/16993115#16993115
    def _handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = _handle_exception


def log_jsonl(line_json: Any, default_level: int = logging.INFO) -> None:
    try:
        level = line_json['level'].upper()
    except (NameError, TypeError):
        level = default_level
    else:
        try:
            level = int(logging.getLevelName(level))
        except ValueError:
            level = default_level

    if logger.isEnabledFor(level):
        print(json.dumps(line_json, sort_keys=True), file=sys.stderr)


def subprocess_run(args: List[str],
                   input: str = None,
                   timeout: int = None,
                   decode_json: bool = False,
                   jsonl_passthru: bool = True) -> Union[Dict, List, str]:
    logger.debug('Running process: {}'.format(' '.join(args)))
    try:

        result = subprocess.run(args=args,
                                input=input,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                encoding='utf-8',
                                errors='ignore',
                                timeout=timeout)
    except subprocess.TimeoutExpired as exception:
        raise RuntimeError(f'{args[0]} invocation failed due to timeout with output.') from None
    except Exception as exception:
        raise RuntimeError(f'{args[0]} invocation failed with a {type(exception).__name__} exception: {str(exception)}') from None

    if result.stderr != '':
        for line in result.stderr.splitlines(keepends=False):
            if jsonl_passthru:
                try:
                    line_json = json.loads(line)
                except JSONDecodeError:
                    logger.info(line)
                else:
                    log_jsonl(line_json)
            else:
                logger.info(line)

    if result.returncode == 0:
        logger.debug('Process finished successfully.')
        if decode_json:
            try:
                stdout_json = json.loads(result.stdout)
            except JSONDecodeError:
                raise RuntimeError(f'{args[0]} invocation was successful but did not return valid JSON.')

            if stdout_json is None or not isinstance(stdout_json, (dict, list)):
                raise RuntimeError(f'{args[0]} invocation was successful but did return null or neither a JSON list nor'
                                   'a dictionary.')

            return stdout_json
        else:
            return result.stdout
    else:
        raise RuntimeError(f'{args[0]} invocation failed with return code {result.returncode}.')


# A copy of this function is in benji.utils.
# This works with dictionary keys and object attributes and a mixture of both.
def keys_exist(obj: Dict[str, Any], keys: Sequence[str]) -> bool:
    split_keys = [key.split('.') for key in keys]

    KeyDoesNotExist = object()
    for split_key in split_keys:
        position = obj
        for component in split_key:
            try:
                position = position.get(component, KeyDoesNotExist)
            except AttributeError:
                # We get here if the get() method is not supported.
                try:
                    position = getattr(position, component, KeyDoesNotExist)
                except AttributeError:
                    # We get here if the getattr() method is not supported.
                    return False
            if position == KeyDoesNotExist:
                return False

    return True


_KeyGetNoDefault = object()


# A copy of this function is in benji.utils.
def key_get(obj: Dict[str, Any], key: str, default: Any = _KeyGetNoDefault) -> Any:
    split_key = key.split('.')

    KeyDoesNotExist = object()
    position = obj
    for component in split_key:
        try:
            position = position.get(component, KeyDoesNotExist)
        except AttributeError:
            # We get here if the get() method is not supported.
            try:
                position = getattr(position, component, KeyDoesNotExist)
            except AttributeError:
                # We get here if the getattr() method is not supported.
                if default is not _KeyGetNoDefault:
                    return default
                else:
                    raise AttributeError(f'{key} does not exist.')
        if position == KeyDoesNotExist:
            if default is not _KeyGetNoDefault:
                return default
            else:
                raise AttributeError(f'{key} does not exist.')

    return position
