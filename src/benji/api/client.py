import random
import string
from contextlib import AbstractContextManager

from celery import Celery, signature
from celery.canvas import Signature

# Also adjust in server.py
CELERY_SETTINGS = 'benji.api.settings'
WORKER_API_QUEUE_PREFIX = 'benji-api-'

WORKER_DEDICATED_QUEUE_PREFIX = 'benji-api-dedicated-'


def _random_string(length: int, characters: str = string.ascii_lowercase + string.digits) -> str:
    return ''.join(random.choice(characters) for _ in range(length))


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
            self._dedicated_queue = f'{WORKER_DEDICATED_QUEUE_PREFIX}{_random_string(24)}'
        return self._dedicated_queue

    def to_dedicated_queue(self, sig: Signature) -> Signature:
        return sig.clone(queue=self.dedicated_queue)

    @classmethod
    def signature(cls, task: str, *args, **kwargs):
        queue = WORKER_API_QUEUE_PREFIX + task.split('.')[0]
        return signature(task, *args, queue=queue, **kwargs)

    def close(self) -> None:
        self._app.close()
