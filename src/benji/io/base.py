#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from abc import ABCMeta, abstractmethod
from concurrent.futures import ThreadPoolExecutor, Future
from threading import BoundedSemaphore
from typing import Tuple, Union, Optional, List, cast, Iterator

from benji.config import ConfigDict, Config
from benji.database import Block, DereferencedBlock
from benji.jobexecutor import JobExecutor
from benji.logging import logger
from benji.repr import ReprMixIn
from benji.utils import future_results_as_completed


class IOBase(ReprMixIn, metaclass=ABCMeta):

    def __init__(self, *, config: Config, name: str, module_configuration: ConfigDict, path: str,
                 block_size: int) -> None:
        self._name = name
        self._path = path
        self._block_size = block_size
        self._simultaneous_reads = config.get_from_dict(module_configuration, 'simultaneousReads', types=int)
        self._simultaneous_writes = config.get_from_dict(module_configuration, 'simultaneousWrites', types=int)
        self._read_executor: Optional[JobExecutor] = None
        self._write_executor: Optional[JobExecutor] = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def url(self) -> str:
        return '{}://{}.'.format(self._name, self._path)

    @abstractmethod
    def size(self) -> int:
        raise NotImplementedError()

    def open_r(self) -> None:
        self._read_executor = JobExecutor(name='IO-Read', workers=self._simultaneous_reads, blocking_submit=False)

    @abstractmethod
    def _read(self, block: DereferencedBlock) -> Tuple[DereferencedBlock, bytes]:
        raise NotImplementedError()

    def read(self, block: Union[DereferencedBlock, Block]) -> None:
        block_deref = block.deref() if isinstance(block, Block) else block

        def job():
            return self._read(block_deref)

        assert self._read_executor is not None
        self._read_executor.submit(job)

    def read_sync(self, block: Union[DereferencedBlock, Block]) -> bytes:
        block_deref = block.deref() if isinstance(block, Block) else block
        return self._read(block_deref)[1]

    def read_get_completed(
            self, timeout: Optional[int] = None) -> Iterator[Union[Tuple[DereferencedBlock, bytes], BaseException]]:
        assert self._read_executor is not None
        return self._read_executor.get_completed(timeout=timeout)

    def open_w(self, size: int, force: bool = False, sparse: bool = False) -> None:
        self._write_executor = JobExecutor(name='IO-Write', workers=self._simultaneous_writes, blocking_submit=True)

    def write(self, block: DereferencedBlock, data: bytes) -> None:

        def job():
            return self._write(block, data)

        assert self._write_executor is not None
        self._write_executor.submit(job)

    def write_sync(self, block: DereferencedBlock, data: bytes) -> None:
        self._write(block, data)

    def write_get_completed(
            self, timeout: Optional[int] = None) -> Iterator[Union[Tuple[DereferencedBlock, bytes], BaseException]]:
        assert self._write_executor is not None
        return self._write_executor.get_completed(timeout=timeout)

    @abstractmethod
    def _write(self, block: DereferencedBlock, data: bytes) -> DereferencedBlock:
        raise NotImplementedError()

    def close(self) -> None:
        if self._read_executor:
            self._read_executor.shutdown()
        if self._write_executor:
            self._write_executor.shutdown()
