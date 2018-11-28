from sparsebitfield import SparseBitfield
from typing import Union, Dict

from benji.metadata import BlockUid, DereferencedBlockUid


class BlockUidHistory:

    def __init__(self) -> None:
        self._history: Dict[int, Dict[int, SparseBitfield]] = {}

    def add(self, storage_id: int, block_uid: Union[BlockUid, DereferencedBlockUid]) -> None:
        history = self._history
        if storage_id not in history:
            history[storage_id] = {}
        if block_uid.left not in history[storage_id]:
            history[storage_id][block_uid.left] = SparseBitfield()
        history[storage_id][block_uid.left].add(block_uid.right)

    def seen(self, storage_id: int, block_uid: Union[BlockUid, DereferencedBlockUid]) -> bool:
        history = self._history
        if storage_id not in history:
            return False
        if block_uid.left not in history[storage_id]:
            return False
        return block_uid.right in history[storage_id][block_uid.left]
