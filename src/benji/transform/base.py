#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from abc import abstractmethod, ABCMeta
from typing import Dict, Tuple, Optional

from benji.config import Config, ConfigDict
from benji.repr import ReprMixIn


class TransformBase(ReprMixIn, metaclass=ABCMeta):

    def __init__(self, *, config: Config, name: str, module_configuration: ConfigDict) -> None:
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    def module(self) -> str:
        return self.__class__.__module__.split('.')[-1]

    @abstractmethod
    def encapsulate(self, *, data: bytes) -> Tuple[Optional[bytes], Optional[Dict]]:
        pass

    @abstractmethod
    def decapsulate(self, *, data: bytes, materials: Dict) -> bytes:
        pass
