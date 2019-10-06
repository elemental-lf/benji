import hashlib
from abc import ABCMeta, abstractmethod
from typing import TypeVar, Generic

StorageObject = TypeVar('StorageObject')


class StorageKeyMixIn(Generic[StorageObject], metaclass=ABCMeta):

    @classmethod
    @abstractmethod
    def storage_prefix(cls) -> str:
        raise NotImplementedError

    @abstractmethod
    def _storage_object_to_key(self) -> str:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def _storage_key_to_object(cls, key: str) -> StorageObject:
        raise NotImplementedError

    @staticmethod
    def _to_path(prefix: str, key: str) -> str:
        digest = hashlib.md5(key.encode('ascii')).hexdigest()
        return '{}{}/{}/{}'.format(prefix, digest[0:2], digest[2:4], key)

    @staticmethod
    def _from_path(prefix: str, key: str) -> str:
        if not key.startswith(prefix):
            raise RuntimeError('Invalid key name {}, it doesn\'t start with "{}".'.format(key, prefix))
        pl = len(prefix)
        if len(key) <= (pl + 6):
            raise RuntimeError('Key {} has an invalid length, expected at least {} characters.'.format(key, pl + 6))
        return key[pl + 6:]

    def storage_object_to_path(self) -> str:
        return self._to_path(self.storage_prefix(), self._storage_object_to_key())

    @classmethod
    def storage_path_to_object(cls, path: str) -> StorageObject:
        return cls._storage_key_to_object(cls._from_path(cls.storage_prefix(), path))
