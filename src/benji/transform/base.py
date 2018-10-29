#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from abc import abstractmethod, ABCMeta


class TransformBase(metaclass=ABCMeta):

    def __init__(self, *, config, name, module_configuration):
        self._name = name

    @property
    def name(self):
        return self._name

    @property
    def module(self):
        return self.__class__.__module__.split('.')[-1]

    @abstractmethod
    def encapsulate(self, *, data):
        pass

    @abstractmethod
    def decapsulate(self, *, data, materials):
        pass
