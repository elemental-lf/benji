from abc import ABC, abstractclassmethod, abstractmethod

import pykube


class VolumeDriverInterface(ABC):

    @classmethod
    @abstractmethod
    def handle(cls, *, pvc: pykube.PersistentVolumeClaim, pv: pykube.PersistentVolume, logger):
        raise NotImplementedError
