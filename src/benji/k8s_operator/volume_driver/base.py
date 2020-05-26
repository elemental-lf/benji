from abc import ABC, abstractclassmethod, abstractmethod

import pykube


class VolumeDriverBase(ABC):

    @abstractmethod
    @classmethod
    def handle(cls, pvc: pykube.PersistentVolumeClaim, pv: pykube.PersistentVolume, *, logger):
        pass
