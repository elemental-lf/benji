import importlib
from abc import ABC, abstractmethod
from typing import Any, Dict, Type

import attr
import pykube


@attr.s(auto_attribs=True, kw_only=True)
class VolumeBase(ABC):
    parent_body: Dict[str, Any]
    pvc: pykube.PersistentVolumeClaim
    pv: pykube.PersistentVolume


class ExecutorInterface(ABC):

    @abstractmethod
    def __init__(self, *, logger):
        raise NotImplementedError

    @abstractmethod
    def add_volume(self, volume: VolumeBase):
        raise NotImplementedError

    @abstractmethod
    def start(self):
        raise NotImplementedError


class BatchExecutor:

    def __init__(self, *, logger) -> None:
        self.logger = logger

        self._executors: Dict[Type[ExecutorInterface], Any] = {}

    def start(self) -> None:
        for executor in self._executors.values():
            executor.start()

    def get_executor(self, executor_cls: Type[ExecutorInterface]) -> ExecutorInterface:
        if executor_cls not in self._executors:
            self._executors[executor_cls] = executor_cls(logger=self.logger)

        return self._executors[executor_cls]
