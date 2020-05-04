#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from abc import ABCMeta, abstractmethod
from typing import Tuple, Union, Optional, Iterator
from urllib import parse

from benji.config import ConfigDict, Config
from benji.database import Block, DereferencedBlock
from benji.repr import ReprMixIn


class IOBase(ReprMixIn, metaclass=ABCMeta):

    def __init__(self, *, config: Config, name: str, module_configuration: ConfigDict, url: str,
                 block_size: int) -> None:
        self._name = name
        self._url = url
        self._parsed_url = parse.urlparse(url, allow_fragments=False)
        self._block_size = block_size

    @property
    def name(self) -> str:
        return self._name

    @property
    def url(self) -> str:
        return self._url

    @property
    def parsed_url(self) -> parse.ParseResult:
        return self._parsed_url

    @property
    def block_size(self):
        return self._block_size

    @abstractmethod
    def open_r(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def open_w(self, size: int, force: bool = False, sparse: bool = False) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def size(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def read(self, block: Union[DereferencedBlock, Block]) -> None:
        raise NotImplementedError

    @abstractmethod
    def read_sync(self, block: Union[DereferencedBlock, Block]) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def read_get_completed(self,
                           timeout: Optional[int] = None
                          ) -> Iterator[Union[Tuple[DereferencedBlock, bytes], BaseException]]:
        raise NotImplementedError

    @abstractmethod
    def write(self, block: Union[DereferencedBlock, Block], data: bytes) -> None:
        raise NotImplementedError

    @abstractmethod
    def write_sync(self, block: Union[DereferencedBlock, Block], data: bytes) -> None:
        raise NotImplementedError

    @abstractmethod
    def write_get_completed(self, timeout: Optional[int] = None) -> Iterator[Union[DereferencedBlock, BaseException]]:
        raise NotImplementedError
