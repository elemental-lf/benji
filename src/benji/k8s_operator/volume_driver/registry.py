from collections import Callable
from typing import NamedTuple, List, Any, Dict

import pykube

from benji.k8s_operator.executor.executor import BatchExecutor
from benji.k8s_operator.volume_driver.interface import VolumeDriverInterface


class _RegistryEntry(NamedTuple):
    order: int
    cls: VolumeDriverInterface


class VolumeDriverRegistry:

    _registry: List[_RegistryEntry] = []

    @classmethod
    def register(cls, order: int) -> Callable:

        def func(wrapped_class) -> Callable:
            nonlocal order
            cls._registry.append(_RegistryEntry(order=order, cls=wrapped_class))
            return wrapped_class

        return func

    @classmethod
    def handle(cls, *, batch_executor: BatchExecutor, parent_body: Dict[str, Any], pvc: pykube.PersistentVolumeClaim,
               pv: pykube.PersistentVolume) -> bool:
        sorted_registry = sorted(cls._registry, key=lambda entry: entry.order)

        for entry in sorted_registry:
            if entry.cls.handle(batch_executor=batch_executor, parent_body=parent_body, pvc=pvc, pv=pv):
                return True
        else:
            return False
