#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import os
import threading
import time
from typing import Tuple, Optional, Union, Iterator

from benji.config import ConfigDict, Config
from benji.database import DereferencedBlock, Block
from benji.exception import UsageError
from benji.io.base import IOBase
from benji.jobexecutor import JobExecutor
from benji.logging import logger


class IO(IOBase):

    def __init__(self, *, config: Config, name: str, module_configuration: ConfigDict, url: str,
                 block_size: int) -> None:
        super().__init__(config=config,
                         name=name,
                         module_configuration=module_configuration,
                         url=url,
                         block_size=block_size)

        if self.parsed_url.username or self.parsed_url.password or self.parsed_url.hostname or self.parsed_url.port \
                or self.parsed_url.params or self.parsed_url.fragment or self.parsed_url.query:
            raise UsageError('The supplied URL {} is invalid.'.format(self.url))

        self._simultaneous_reads = config.get_from_dict(module_configuration, 'simultaneousReads', types=int)
        self._simultaneous_writes = config.get_from_dict(module_configuration, 'simultaneousWrites', types=int)
        self._read_executor: Optional[JobExecutor] = None
        self._write_executor: Optional[JobExecutor] = None

    def open_r(self) -> None:
        self._read_executor = JobExecutor(name='IO-Read', workers=self._simultaneous_reads, blocking_submit=False)

    def open_w(self, size: int, force: bool = False, sparse: bool = False) -> None:
        self._write_executor = JobExecutor(name='IO-Write', workers=self._simultaneous_writes, blocking_submit=True)

        if os.path.exists(self.parsed_url.path):
            if not force:
                raise FileExistsError('{} already exists. Force the restore if you want to overwrite it.'.format(self.url))
            else:
                if size > self.size():
                    raise IOError('{} is too small. Its size is {} bytes, but we need {} bytes for the restore.'.format(
                        self.url, self.size(), size))
        else:
            with open(self.parsed_url.path, 'wb') as f:
                f.seek(size - 1)
                f.write(b'\0')

    def close(self) -> None:
        if self._read_executor:
            self._read_executor.shutdown()
        if self._write_executor:
            self._write_executor.shutdown()

    def size(self) -> int:
        with open(self.parsed_url.path, 'rb') as f:
            f.seek(0, 2)  # to the end
            size = f.tell()
        return size

    def _read(self, block: DereferencedBlock) -> Tuple[DereferencedBlock, bytes]:
        offset = block.idx * self.block_size
        t1 = time.time()
        with open(self.parsed_url.path, 'rb') as f:
            f.seek(offset)
            data = f.read(block.size)
            os.posix_fadvise(f.fileno(), offset, block.size, os.POSIX_FADV_DONTNEED)
        t2 = time.time()

        if not data:
            raise EOFError('End of file reached on {} when there should be data.'.format(self.url))

        logger.debug('{} read block {} in {:.3f}s'.format(
            threading.current_thread().name,
            block.idx,
            t2 - t1,
        ))

        return block, data

    def read(self, block: Union[DereferencedBlock, Block]) -> None:
        # We do need to dereference the block outside of the closure otherwise a reference to the block will be held
        # inside of the closure leading to database troubles.
        # See https://github.com/elemental-lf/benji/issues/61.
        block_deref = block.deref()

        def job():
            return self._read(block_deref)

        assert self._read_executor is not None
        self._read_executor.submit(job)

    def read_sync(self, block: Union[DereferencedBlock, Block]) -> bytes:
        return self._read(block.deref())[1]

    def read_get_completed(self, timeout: Optional[int] = None
                          ) -> Iterator[Union[Tuple[DereferencedBlock, bytes], BaseException]]:
        assert self._read_executor is not None
        return self._read_executor.get_completed(timeout=timeout)

    def _write(self, block: DereferencedBlock, data: bytes) -> DereferencedBlock:
        offset = block.idx * self.block_size
        t1 = time.time()
        with open(self.parsed_url.path, 'rb+') as f:
            f.seek(offset)
            written = f.write(data)
            os.posix_fadvise(f.fileno(), offset, len(data), os.POSIX_FADV_DONTNEED)
        t2 = time.time()

        logger.debug('{} wrote block {} in {:.3f}s'.format(
            threading.current_thread().name,
            block.idx,
            t2 - t1,
        ))

        assert written == len(data)
        return block

    def write(self, block: Union[DereferencedBlock, Block], data: bytes) -> None:
        # We do need to dereference the block outside of the closure otherwise a reference to the block will be held
        # inside of the closure leading to database troubles.
        # See https://github.com/elemental-lf/benji/issues/61.
        block_deref = block.deref()

        def job():
            return self._write(block_deref, data)

        assert self._write_executor is not None
        self._write_executor.submit(job)

    def write_sync(self, block: Union[DereferencedBlock, Block], data: bytes) -> None:
        self._write(block.deref(), data)

    def write_get_completed(self, timeout: Optional[int] = None) -> Iterator[Union[DereferencedBlock, BaseException]]:
        assert self._write_executor is not None
        return self._write_executor.get_completed(timeout=timeout)
