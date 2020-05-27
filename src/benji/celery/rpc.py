import functools
import json
from io import StringIO
from json import JSONDecodeError

import attr

from contextlib import AbstractContextManager
from datetime import datetime
from typing import Any, ByteString

from celery import Celery
from celery.result import AsyncResult

from benji.celery.message import RPCError, RPCResult, Message
from benji.celery.utils import random_string

CELERY_SETTINGS = 'benji.celery.settings'
WORKER_DEFAULT_THREADS = 1
TERMINATE_TASK_API_GROUP = 'rpc'
TERMINATE_TASK_API_VERSION = 'v1'
TERMINATE_TASK_NAME = 'terminate'
WORKER_QUEUE_PREFIX = 'benji-rpc-'
CHARSET = 'utf-8'


class RPCCallFailed(Exception):

    def __init__(self, message, reason):
        super().__init__(message)
        self.reason = reason


class RPCServer:

    def __init__(self, *, queue: str = None, threads: int = None) -> None:
        self._queue = queue
        self._app = Celery()
        self._app.config_from_object(CELERY_SETTINGS)
        if threads:
            self._app.conf.update({'worker_concurrency': threads})
        self._register_terminate_task()

    @property
    def queue(self):
        return self._queue

    def _register_terminate_task(self):

        def terminate():
            nonlocal self
            self._app.control.shutdown(destination=[self._app.current_worker_task.request.hostname])

        self.register_as_task(api_group=TERMINATE_TASK_API_GROUP,
                              api_version=TERMINATE_TASK_API_VERSION,
                              name=TERMINATE_TASK_NAME)(terminate)

    @staticmethod
    def _encode_result(result: Any) -> str:
        # This assumes that a result of type StringIO already contains JSON formatted data and only
        # needs to be encoded.
        if isinstance(result, StringIO):
            encoded_result = result.getvalue().encode(CHARSET)
        else:
            encoded_result = json.dumps(result, check_circular=True, separators=(',', ': '), indent=2).encode(CHARSET)
        return encoded_result

    def register_as_task(self, api_group, api_version, name: str = None):

        def decorator(func):
            task_name = f'{api_group}.{api_version}.{name or func.__name__}'

            @functools.wraps(func)
            def call_task(*task_args, **task_kwargs):
                nonlocal func, self
                start_time = datetime.utcnow().isoformat(timespec='microseconds') + 'Z'
                try:
                    result = func(*task_args, **task_kwargs)
                except Exception as exception:
                    completion_time = datetime.utcnow().isoformat(timespec='microseconds') + 'Z'
                    return attr.asdict(
                        RPCError(task_id=self._app.current_task.request.id,
                                 task_name=task_name,
                                 args=task_args,
                                 kwargs=task_kwargs,
                                 start_time=start_time,
                                 completion_time=completion_time,
                                 reason=type(exception).__name__,
                                 message=str(exception)))
                completion_time = datetime.utcnow().isoformat(timespec='microseconds') + 'Z'
                return attr.asdict(
                    RPCResult(task_id=self._app.current_task.request.id,
                              task_name=task_name,
                              args=task_args,
                              kwargs=task_kwargs,
                              start_time=start_time,
                              completion_time=completion_time,
                              result=self._encode_result(result)))

            self._app.task(call_task, name=task_name)
            return func

        return decorator

    def serve(self) -> None:
        worker_args = ['', 'worker', '-O', 'fair', '-q', '-l', 'info', '--without-mingle', '--without-gossip']
        if self._queue is not None:
            worker_args.extend(['-Q', self._queue])
        self._app.start(worker_args)
        self._app.close()


class RPCClient(AbstractContextManager):

    def __init__(self, *, auto_queue: bool = False) -> None:
        if auto_queue:
            self._queue = f'{WORKER_QUEUE_PREFIX}{random_string(12)}'
        else:
            self._queue = None

        self._app = Celery()
        self._app.config_from_object(CELERY_SETTINGS)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def queue(self):
        return self._queue

    def call_async(self, task: str, *args, ignore_result: bool = False, **kwargs) -> AsyncResult:
        return self._app.send_task(task, args=args, kwargs=kwargs, ignore_result=ignore_result, queue=self._queue)

    @staticmethod
    def _decode_result(result: ByteString) -> Any:
        return json.loads(result.decode(CHARSET))

    def get_result(self, async_result: AsyncResult):
        result = async_result.get()
        print(f'{result}')
        result = Message.from_dict(result)
        try:
            return self._decode_result(result.result)
        except AttributeError:
            raise RPCCallFailed(result.message, result.reason)
        except JSONDecodeError as exception:
            raise RPCCallFailed(str(exception), type(exception).__name__)

    def call(self, task: str, *args, **kwargs) -> Any:
        return self.get_result(self.call_async(task, *args, **kwargs))

    def close(self) -> None:
        self._app.close()
