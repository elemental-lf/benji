from collections import Callable
from typing import NamedTuple, List

import pykube

from benji.k8s_operator.backup.interface import BackupInterface
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
    def handle(cls, *, pvc: pykube.PersistentVolumeClaim, pv: pykube.PersistentVolume, logger) -> BackupInterface:
        sorted_registry = sorted(cls._registry, key=lambda entry: entry.order)

        for entry in sorted_registry:
            backup_handler = entry.handle(pvc=pvc, pv=pv, logger=logger)
            if backup_handler is not None:
                return backup_handler
