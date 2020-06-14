from abc import ABC, abstractmethod
from typing import Dict, Any

import pykube

from benji.k8s_operator.executor.executor import BatchExecutor


class VolumeDriverInterface(ABC):

    @classmethod
    @abstractmethod
    def handle(cls, *, batch_executor: BatchExecutor, parent_body: Dict[str, Any], pvc: pykube.PersistentVolumeClaim,
               pv: pykube.PersistentVolume) -> bool:
        raise NotImplementedError
