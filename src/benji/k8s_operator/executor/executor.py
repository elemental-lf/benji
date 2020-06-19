import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Type, NewType, List, NamedTuple, Callable

import pykube

ActionType = NewType('Action', object)

BACKUP_ACTION = ActionType(object())
RESTORE_ACTION = ActionType(object())

ACTIONS = (BACKUP_ACTION, RESTORE_ACTION)

logger = logging.getLogger(__name__)


class ExecutorInterface(ABC):

    @abstractmethod
    def __init__(self):
        raise NotImplementedError

    @abstractmethod
    def start(self):
        raise NotImplementedError


class _RegistryEntry(NamedTuple):
    order: int
    cls: ExecutorInterface


class BatchExecutor(ExecutorInterface):

    _registry: List[_RegistryEntry] = []

    @classmethod
    def register(cls, order: int) -> Callable:

        def func(wrapped_class) -> Callable:
            nonlocal order
            cls._registry.append(_RegistryEntry(order=order, cls=wrapped_class))
            return wrapped_class

        return func

    @classmethod
    def register_as_volume_handler(cls, func):
        func.volume_handler = True
        return func

    def __init__(self) -> None:
        self._executors: List[Type[ExecutorInterface], Any] = []
        self._handlers: List[Callable] = []

        sorted_registry = sorted(self._registry, key=lambda entry: entry.order)
        for entry in sorted_registry:
            logger.info(f'Instantiating executor {entry.cls.__name__} (order = {entry.order}).')
            executor = entry.cls()
            self._executors.append(executor)

            for attr_name in dir(executor):
                attr = getattr(executor, attr_name)
                if getattr(attr, 'volume_handler', False):
                    logger.info(f'Registering handler {entry.cls.__name__}.{attr.__name__}.')
                    self._handlers.append(attr)

    def start(self) -> None:
        for executor in self._executors:
            executor.start()

    def handle(self, *, action: ActionType, parent_body: Dict[str, Any], pvc: pykube.PersistentVolumeClaim,
               pv: pykube.PersistentVolume) -> bool:

        for handler in self._handlers:
            if handler(action=action, parent_body=parent_body, pvc=pvc, pv=pv):
                return True
        else:
            return False
