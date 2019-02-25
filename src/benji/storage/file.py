#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import os
from typing import List

from benji.config import Config, ConfigDict
from benji.storage.base import StorageBase


class Storage(StorageBase):

    WRITE_QUEUE_LENGTH = 10
    READ_QUEUE_LENGTH = 20

    def __init__(self, *, config: Config, name: str, storage_id: int, module_configuration: ConfigDict):
        super().__init__(config=config, name=name, storage_id=storage_id, module_configuration=module_configuration)

        if os.sep != '/':
            raise RuntimeError('This module only works with / as a path separator.')

        self.path = Config.get_from_dict(module_configuration, 'path', types=str)

        # Ensure that self.path ends in a slash
        if not self.path.endswith('/'):
            self.path = self.path + '/'

    def _write_object(self, key: str, data: bytes) -> None:
        filename = os.path.join(self.path, key)

        try:
            with open(filename, 'wb') as f:
                f.write(data)
        except FileNotFoundError:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'wb') as f:
                f.write(data)

    def _read_object(self, key: str) -> bytes:
        filename = os.path.join(self.path, key)

        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))

        with open(filename, 'rb') as f:
            data = f.read()

        return data

    def _read_object_length(self, key: str) -> int:
        filename = os.path.join(self.path, key)

        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))

        return os.path.getsize(filename)

    def _rm_object(self, key: str) -> None:
        filename = os.path.join(self.path, key)

        if not os.path.exists(filename):
            raise FileNotFoundError('File {} not found.'.format(filename))
        os.unlink(filename)

    # def _rm_many_objects(self, keys: Sequence[str]) -> List[str]:
    #     errors = []
    #     for key in keys:
    #         try:
    #             self._rm_object(key)
    #         except FileNotFoundError:
    #             errors.append(key)
    #     return errors

    def _list_objects(self, prefix: str) -> List[str]:
        matches = []
        for root, dirnames, filenames in os.walk(os.path.join(self.path, prefix)):
            for filename in filenames:
                key = (os.path.join(root, filename))[len(self.path):]
                matches.append(key)
        return matches
