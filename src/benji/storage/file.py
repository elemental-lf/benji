#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import os
from os.path import getsize
from typing import Union, Iterable, Tuple

from benji.config import Config, ConfigDict
from benji.storage.base import StorageBase


class Storage(StorageBase):

    WRITE_QUEUE_LENGTH = 10
    READ_QUEUE_LENGTH = 20

    def __init__(self, *, config: Config, name: str, module_configuration: ConfigDict):
        super().__init__(config=config, name=name, module_configuration=module_configuration)

        if os.sep != '/':
            raise RuntimeError('This module only works with / as a path separator.')

        self.path = Config.get_from_dict(module_configuration, 'path', types=str)

        # Ensure that self.path ends in os.path.sep
        if not self.path.endswith(os.path.sep):
            self.path = os.path.join(self.path, '')

    def _write_object(self, key: str, data: bytes) -> None:
        filename = os.path.join(self.path, key)

        try:
            with open(filename, 'wb', buffering=0) as f:
                f.write(data)
                os.fdatasync(f.fileno())
        except FileNotFoundError:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'wb', buffering=0) as f:
                f.write(data)
                os.fdatasync(f.fileno())

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

    def _list_objects(self, prefix: str = None,
                      include_size: bool = False) -> Union[Iterable[str], Iterable[Tuple[str, int]]]:
        for root, dirnames, filenames in os.walk(os.path.join(self.path, prefix) if prefix is not None else self.path):
            for filename in filenames:
                key = (os.path.join(root, filename))[len(self.path):]
                if include_size:
                    try:
                        size = getsize(os.path.join(root, filename))
                    except OSError:
                        # The file might be gone (due to benji cleanup for example) when we get to the getsize().
                        # Just move on to the next filename...
                        continue
                    else:
                        yield key, size
                else:
                    yield key
