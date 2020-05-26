from collections import Callable
from typing import NamedTuple, List

from benji.k8s_operator.volume_driver.base import VolumeDriverBase


class _RegistryEntry(NamedTuple):
    order: int
    cls: VolumeDriverBase


class VolumeDriverRegistry:

    def __init__(self) -> None:
        self._registry: List[_RegistryEntry] = []

    def register(self, order: int) -> Callable:

        def func(wrapped_class) -> Callable:
            self._registry.append(_RegistryEntry(order=order, cls=wrapped_class))
            return wrapped_class

        return func

    def handle(self, name: str, *args, **kwargs):
        sorted_registry = sorted(self._registry, key=lambda entry: entry.order)

        for entry in sorted_registry:

            self._registry[name](*args, **kwargs)
