#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import os
import threading
import time
from typing import Optional, BinaryIO, Tuple

from benji.config import ConfigDict, Config
from benji.database import DereferencedBlock
from benji.io.base import IOBase
from benji.logging import logger


class IO(IOBase):

    _writer: Optional[BinaryIO]

    def __init__(self, *, config: Config, name: str, module_configuration: ConfigDict, path: str,
                 block_size: int) -> None:
        super().__init__(
            config=config, name=name, module_configuration=module_configuration, path=path, block_size=block_size)

        self._writer = None

    def open_w(self, size: int, force: bool = False, sparse: bool = False) -> None:
        if os.path.exists(self._path):
            if not force:
                raise FileExistsError('{} already exists. Force the restore if you want to overwrite it.'.format(
                    self.url))
            else:
                if size > self.size():
                    raise IOError('{} is too small. Its size is {} bytes, but we need {} bytes for the restore.'.format(
                        self.url, self.size(), size))
        else:
            with open(self._path, 'wb') as f:
                f.seek(size - 1)
                f.write(b'\0')

        self._writer = open(self._path, 'rb+')

    def size(self) -> int:
        with open(self._path, 'rb') as f:
            f.seek(0, 2)  # to the end
            size = f.tell()
        return size

    def _read(self, block: DereferencedBlock) -> Tuple[DereferencedBlock, bytes]:
        with open(self._path, 'rb') as source_file:
            offset = block.id * self._block_size
            t1 = time.time()
            source_file.seek(offset)
            data = source_file.read(block.size)
            t2 = time.time()
            # throw away cache
            os.posix_fadvise(source_file.fileno(), offset, block.size, os.POSIX_FADV_DONTNEED)

        if not data:
            raise EOFError('End of file reached on {} when there should be data.'.format(self.url))

        logger.debug('{} read block {} in {:.2f}s'.format(
            threading.current_thread().name,
            block.id,
            t2 - t1,
        ))

        return block, data

    def write(self, block: DereferencedBlock, data: bytes) -> None:
        assert self._writer is not None
        offset = block.id * self._block_size
        self._writer.seek(offset)
        written = self._writer.write(data)
        os.posix_fadvise(self._writer.fileno(), offset, len(data), os.POSIX_FADV_DONTNEED)
        assert written == len(data)

    def close(self) -> None:
        super().close()
        if self._writer:
            self._writer.close()
            self._writer = None
