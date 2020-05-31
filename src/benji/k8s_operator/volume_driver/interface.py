from abc import ABC, abstractclassmethod, abstractmethod
from typing import Dict, Any

import pykube

from benji.k8s_operator.resources import NamespacedAPIObject


class VolumeDriverInterface(ABC):

    @classmethod
    @abstractmethod
    def handle(cls, *, parent_body: Dict[str, Any], pvc: pykube.PersistentVolumeClaim, pv: pykube.PersistentVolume,
               logger):
        raise NotImplementedError
