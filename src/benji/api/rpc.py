import logging
import string
from contextlib import AbstractContextManager
import random
from typing import List, Callable, NamedTuple

from celery.canvas import Signature

from benji.exception import InternalError

from benji.config import Config
from celery import Celery, signature

from benji.api.base import APIBase

CELERY_SETTINGS = 'benji.api.settings'
WORKER_DEFAULT_THREADS = 1
WORKER_API_QUEUE_PREFIX = 'benji-api-'
WORKER_DEDICATED_QUEUE_PREFIX = 'benji-api-dedicated-'

TERMINATE_TASK_API_GROUP = 'rpc'
TERMINATE_TASK_API_VERSION = 'v1'
TERMINATE_TASK_NAME = 'terminate'

logger = logging.getLogger(__name__)


def random_string(length: int, characters: str = string.ascii_lowercase + string.digits) -> str:
    return ''.join(random.choice(characters) for _ in range(length))


class RPCServer:

    _api_registry: List[APIBase] = []

    @classmethod
    def register_api(cls) -> Callable:

        def func(wrapped_class) -> Callable:
            cls._api_registry.append(wrapped_class)
            return wrapped_class

        return func

    def __init__(self, *, config: Config, queue: str = None, threads: int = None):
        self._app = Celery(set_as_current=False)
        self._app.config_from_object(CELERY_SETTINGS)
        if threads:
            self._app.conf.update({'worker_concurrency': threads})

        if queue is not None:
            self._dedicated_queue = True
            self._queues = set((queue,))
            self._register_terminate_task()
        else:
            self._dedicated_queue = False
            self._queues = set()

        api_objs = [api_cls(config=config) for api_cls in self._api_registry]
        api_groups = set()
        for api_obj in api_objs:
            api_groups
            self._install_tasks(api_obj)

    def _register_terminate_task(self):

        def terminate():
            nonlocal self
            self._app.control.shutdown(destination=[self._app.current_worker_task.request.hostname])

        task_name = f'{TERMINATE_TASK_API_GROUP}.{TERMINATE_TASK_API_VERSION}.{TERMINATE_TASK_NAME}'
        logger.info(f'Installing task {task_name}.')
        self._app.task(terminate, name=task_name)

    @classmethod
    def register_task(cls, api_group: str, api_version: str, name: str = None) -> Callable:

        class _APIGroupVersion(NamedTuple):
            group: str
            version: str
            name: str

        def decorator(func):
            func.api = _APIGroupVersion(api_group, api_version, name)
            return func

        return decorator

    def _install_tasks(self, api_obj: APIBase) -> None:
        for kw in dir(api_obj):
            attr = getattr(api_obj, kw)
            if hasattr(attr, 'api'):
                if not self._dedicated_queue:
                    self._queues.add(f'{WORKER_API_QUEUE_PREFIX}{attr.api.group}')
                task_name = f'{attr.api.group}.{attr.api.version}.{attr.api.name or attr.__name__}'
                logger.info(f'Installing task {task_name}.')
                self._app.task(attr, name=task_name)

    def serve(self) -> None:
        worker_args = ['', 'worker', '-O', 'fair', '-q', '-l', 'debug', '--without-mingle', '--without-gossip']
        if not self._queues:
            raise InternalError('No queues discovered from tasks and no dedicated queue specified.')
        logger.info(f'Subscribing to queue{"s" if len(self._queues) > 1 else ""} {", ".join(self._queues)}.')
        worker_args.extend(['-Q', ','.join(self._queues)])
        self._app.start(worker_args)
        self._app.close()


class RPCClient(AbstractContextManager):

    def __init__(self) -> None:
        self._app = Celery(set_as_current=True)
        self._app.config_from_object(CELERY_SETTINGS)
        self._dedicated_queue = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def dedicated_queue(self):
        if self._dedicated_queue is None:
            self._dedicated_queue = f'{WORKER_DEDICATED_QUEUE_PREFIX}{random_string(24)}'
        return self._dedicated_queue

    def to_dedicated_queue(self, sig: Signature) -> Signature:
        return sig.clone(queue=self.dedicated_queue)

    @classmethod
    def signature(cls, task: str, *args, **kwargs):
        queue = WORKER_API_QUEUE_PREFIX + task.split('.')[0]
        return signature(task, *args, queue=queue, **kwargs)

    def close(self) -> None:
        self._app.close()
