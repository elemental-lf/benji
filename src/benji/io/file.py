#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import os
import threading
import time
from typing import Tuple

from benji.config import ConfigDict, Config
from benji.database import DereferencedBlock
from benji.exception import UsageError
from benji.io.base import ThreadedIOBase
from benji.logging import logger


class IO(ThreadedIOBase):

    def __init__(self, *, config: Config, name: str, module_configuration: ConfigDict, url: str,
                 block_size: int) -> None:
        super().__init__(
            config=config, name=name, module_configuration=module_configuration, url=url, block_size=block_size)

        if self.parsed_url.username or self.parsed_url.password or self.parsed_url.hostname or self.parsed_url.port \
                or self.parsed_url.params or self.parsed_url.fragment or self.parsed_url.query:
            raise UsageError('The supplied URL {} is invalid.'.format(self.url))

    def open_w(self, size: int, force: bool = False, sparse: bool = False) -> None:
        super().open_w(size, force, sparse)
        if os.path.exists(self.parsed_url.path):
            if not force:
                raise FileExistsError('{} already exists. Force the restore if you want to overwrite it.'.format(
                    self.url))
            else:
                if size > self.size():
                    raise IOError('{} is too small. Its size is {} bytes, but we need {} bytes for the restore.'.format(
                        self.url, self.size(), size))
        else:
            with open(self.parsed_url.path, 'wb') as f:
                f.seek(size - 1)
                f.write(b'\0')

    def size(self) -> int:
        with open(self.parsed_url.path, 'rb') as f:
            f.seek(0, 2)  # to the end
            size = f.tell()
        return size

    def _read(self, block: DereferencedBlock) -> Tuple[DereferencedBlock, bytes]:
        offset = block.id * self.block_size
        t1 = time.time()
        with open(self.parsed_url.path, 'rb') as f:
            f.seek(offset)
            data = f.read(block.size)
            os.posix_fadvise(f.fileno(), offset, block.size, os.POSIX_FADV_DONTNEED)
        t2 = time.time()

        if not data:
            raise EOFError('End of file reached on {} when there should be data.'.format(self.url))

        logger.debug('{} read block {} in {:.2f}s'.format(
            threading.current_thread().name,
            block.id,
            t2 - t1,
        ))

        return block, data

    def _write(self, block: DereferencedBlock, data: bytes) -> DereferencedBlock:
        offset = block.id * self.block_size
        t1 = time.time()
        with open(self.parsed_url.path, 'rb+') as f:
            f.seek(offset)
            written = f.write(data)
            os.posix_fadvise(f.fileno(), offset, len(data), os.POSIX_FADV_DONTNEED)
        t2 = time.time()

        logger.debug('{} wrote block {} in {:.2f}s'.format(
            threading.current_thread().name,
            block.id,
            t2 - t1,
        ))

        assert written == len(data)
        return block
