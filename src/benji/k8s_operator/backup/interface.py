from abc import ABC, abstractclassmethod, abstractmethod

import pykube


class BackupInterface(ABC):

    @abstractmethod
    def backup(self):
        raise NotImplementedError


class RestoreInterface(ABC):

    @abstractmethod
    def restore(self):
        raise NotImplementedError
