#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import queue
import re
import threading
import time
from collections import deque
from typing import Tuple, Optional, Union, Iterator, Deque
from urllib.parse import parse_qs

import rados
# noinspection PyUnresolvedReferences
import rbd

from benji.config import ConfigDict, Config
from benji.database import DereferencedBlock, Block
from benji.exception import UsageError, ConfigurationError
from benji.io.base import IOBase
from benji.logging import logger


class IO(IOBase):

    _pool_name: Optional[str]
    _image_name: Optional[str]
    _snapshot_name: Optional[str]

    def __init__(self, *, config: Config, name: str, module_configuration: ConfigDict, url: str,
                 block_size: int) -> None:
        super().__init__(config=config,
                         name=name,
                         module_configuration=module_configuration,
                         url=url,
                         block_size=block_size)

        if self.parsed_url.username or self.parsed_url.password or self.parsed_url.hostname or self.parsed_url.port \
                    or self.parsed_url.params or self.parsed_url.fragment:
            raise UsageError('The supplied URL {} is invalid.'.format(self.url))
        if self.parsed_url.query:
            try:
                extra_ceph_conf = parse_qs(self.parsed_url.query,
                                           keep_blank_values=True,
                                           strict_parsing=True,
                                           errors='strict')
            except (ValueError, UnicodeError) as exception:
                raise UsageError('The supplied URL {} is invalid.'.format(self.url)) from exception

            # parse_qs returns the values as lists, only consider the first appearance of each key in the query string.
            extra_ceph_conf = {key: value[0] for key, value in extra_ceph_conf.items()}
        else:
            extra_ceph_conf = {}

        ceph_config_file = config.get_from_dict(module_configuration, 'cephConfigFile', types=str)
        if 'client_identifier' in extra_ceph_conf:
            client_identifier = extra_ceph_conf['client_identifier']
            del extra_ceph_conf['client_identifier']
        else:
            client_identifier = config.get_from_dict(module_configuration, 'clientIdentifier', types=str)

        self._cluster = rados.Rados(conffile=ceph_config_file, rados_id=client_identifier, conf=extra_ceph_conf)
        self._cluster.connect()
        # create a bitwise or'd list of the configured features
        self._new_image_features = 0
        for feature in config.get_from_dict(module_configuration, 'newImageFeatures', types=list):
            try:
                self._new_image_features = self._new_image_features | getattr(rbd, feature)
            except AttributeError:
                raise ConfigurationError('{}: Unknown image feature {}.'.format(module_configuration.full_name, feature))

        self._pool_name = None
        self._image_name = None
        self._snapshot_name = None
        self._rbd_image = None

        self._simultaneous_reads = config.get_from_dict(module_configuration, 'simultaneousReads', types=int)
        self._simultaneous_writes = config.get_from_dict(module_configuration, 'simultaneousWrites', types=int)
        self._read_queue: Deque[DereferencedBlock] = deque()
        self._write_queue: Deque[Tuple[DereferencedBlock, bytes]] = deque()
        self._outstanding_aio_reads = 0
        self._outstanding_aio_writes = 0
        self._submitted_aio_writes = threading.BoundedSemaphore(self._simultaneous_writes)
        self._read_completion_queue: queue.Queue[Tuple[rbd.Completion, float, float, DereferencedBlock,
                                                       bytes]] = queue.Queue()
        self._write_completion_queue: queue.Queue[Tuple[rbd.Completion, float, float,
                                                        DereferencedBlock]] = queue.Queue()

    def open_r(self) -> None:
        re_match = re.match('^([^/]+)/([^@]+)(?:@(.+))?$', self.parsed_url.path)
        if not re_match:
            raise UsageError('URL {} is invalid . Need {}:<pool>/<imagename> or {}:<pool>/<imagename>@<snapshotname>.'.format(
                self.url, self.name, self.name))
        self._pool_name, self._image_name, self._snapshot_name = re_match.groups()

        # try opening it and quit if that's not possible.
        try:
            ioctx = self._cluster.open_ioctx(self._pool_name)
        except rados.ObjectNotFound:
            raise FileNotFoundError('Ceph pool {} not found.'.format(self._pool_name)) from None

        try:
            self._rbd_image = rbd.Image(ioctx, self._image_name, self._snapshot_name, read_only=True)
        except rbd.ImageNotFound:
            raise FileNotFoundError('RBD image or snapshot {} not found.'.format(self.url)) from None

    def open_w(self, size: int, force: bool = False, sparse: bool = False) -> None:
        re_match = re.match('^([^/]+)/([^@]+)$', self.parsed_url.path)
        if not re_match:
            raise UsageError('URL {} is invalid . Need {}:<pool>/<imagename>.'.format(self.url, self.name))
        self._pool_name, self._image_name = re_match.groups()

        # try opening it and quit if that's not possible.
        try:
            ioctx = self._cluster.open_ioctx(self._pool_name)
        except rados.ObjectNotFound:
            raise FileNotFoundError('Ceph pool {} not found.'.format(self._pool_name)) from None

        try:
            self._rbd_image = rbd.Image(ioctx, self._image_name)
        except rbd.ImageNotFound:
            rbd.RBD().create(ioctx, self._image_name, size, old_format=False, features=self._new_image_features)
            self._rbd_image = rbd.Image(ioctx, self._image_name)
        else:
            assert self._rbd_image is not None
            if not force:
                raise FileExistsError(
                    'RBD image {} already exists. Force the restore if you want to overwrite it.'.format(self.url))
            else:
                image_size = self._rbd_image.size()
                if size > image_size:
                    raise IOError(
                        'RBD image {} is too small. Its size is {} bytes, but we need {} bytes for the restore.'.format(
                            self.url, image_size, size))

                # If this is an existing image and sparse is true discard all objects from this image
                # RBD discard only supports a maximum region length of 0x7fffffff.
                if sparse:
                    logger.debug('Discarding all objects of RBD image {}.'.format(self.url))
                    region_start = 0
                    bytes_to_end = image_size
                    while bytes_to_end > 0:
                        region_length = min(0x7fffffff, bytes_to_end)
                        self._rbd_image.discard(region_start, region_length)
                        region_start += region_length
                        bytes_to_end -= region_length

    def close(self) -> None:
        assert self._rbd_image is not None
        self._rbd_image.close()

    def size(self) -> int:
        assert self._rbd_image is not None
        return self._rbd_image.size()

    def _submit_aio_reads(self):
        assert self._rbd_image is not None
        while len(self._read_queue) > 0 and self._outstanding_aio_reads < self._simultaneous_reads:
            block = self._read_queue.pop()
            t1 = time.time()

            def aio_callback(completion, data):
                t2 = time.time()
                self._read_completion_queue.put((completion, t1, t2, block, data))

            offset = block.idx * self.block_size
            self._rbd_image.aio_read(offset, block.size, aio_callback, rados.LIBRADOS_OP_FLAG_FADVISE_DONTNEED)
            self._outstanding_aio_reads += 1

    def read(self, block: Union[DereferencedBlock, Block]) -> None:
        self._read_queue.appendleft(block.deref())
        self._submit_aio_reads()

    def read_sync(self, block: Union[DereferencedBlock, Block]) -> bytes:
        assert self._rbd_image is not None
        offset = block.idx * self.block_size
        t1 = time.time()
        data = self._rbd_image.read(offset, block.size, rados.LIBRADOS_OP_FLAG_FADVISE_DONTNEED)
        t2 = time.time()

        if not data:
            raise EOFError('End of file reached on {} when there should be data.'.format(self.url))

        logger.debug('Read block {} in {:.3f}s'.format(block.idx, t2 - t1))

        return data

    def _reads_finished(self) -> bool:
        return len(self._read_queue) == 0 and self._outstanding_aio_reads == 0

    def read_get_completed(self,
                           timeout: Optional[int] = None
                          ) -> Iterator[Union[Tuple[DereferencedBlock, bytes], BaseException]]:
        try:
            while not self._reads_finished():
                logger.debug('Read queue length, outstanding reads, completion queue length: {}, {}, {}.'.format(
                    len(self._read_queue), self._outstanding_aio_reads, self._read_completion_queue.qsize()))
                self._submit_aio_reads()

                completion, t1, t2, block, data = self._read_completion_queue.get(block=(timeout is None or
                                                                                         timeout != 0),
                                                                                  timeout=timeout)
                assert self._outstanding_aio_reads > 0
                self._outstanding_aio_reads -= 1

                try:
                    completion.wait_for_complete_and_cb()
                except Exception as exception:
                    yield exception
                else:
                    read_return_value = completion.get_return_value()

                    if read_return_value < 0:
                        raise IOError('Read of block {} failed.'.format(block.idx))

                    if read_return_value != block.size:
                        raise IOError('Short read of block {}. Wanted {} bytes but got {}.'.format(
                            block.idx, block.size, read_return_value))

                    if not data:
                        # We shouldn't get here because a failed read should be caught by the "read_return_value < 0"
                        # check above. See: https://github.com/ceph/ceph/blob/880468b4bf6f0a1995de5bd98c09007a00222cbf/src/pybind/rbd/rbd.pyx#L4145.
                        raise IOError('Read of block {} failed.'.format(block.idx))

                    logger.debug('Read block {} in {:.3f}s'.format(block.idx, t2 - t1))

                    yield block, data

                self._read_completion_queue.task_done()
        except queue.Empty:
            return
        else:
            return

    def _submit_aio_writes(self):
        assert self._rbd_image is not None
        while len(self._write_queue) > 0 and self._outstanding_aio_writes < self._simultaneous_writes:
            block, data = self._write_queue.pop()
            t1 = time.time()

            def aio_callback(completion):
                t2 = time.time()
                self._write_completion_queue.put((completion, t1, t2, block))
                self._submitted_aio_writes.release()

            self._submitted_aio_writes.acquire()
            offset = block.idx * self.block_size
            self._rbd_image.aio_write(data, offset, aio_callback, rados.LIBRADOS_OP_FLAG_FADVISE_DONTNEED)
            self._outstanding_aio_writes += 1

    def write(self, block: Union[DereferencedBlock, Block], data: bytes) -> None:
        assert self._rbd_image is not None
        self._write_queue.appendleft((block.deref(), data))
        self._submit_aio_writes()

    def write_sync(self, block: Union[DereferencedBlock, Block], data: bytes) -> None:
        assert self._rbd_image is not None
        offset = block.idx * self.block_size
        t1 = time.time()
        written = self._rbd_image.write(data, offset, rados.LIBRADOS_OP_FLAG_FADVISE_DONTNEED)
        t2 = time.time()

        logger.debug('Wrote block {} in {:.3f}s'.format(block.idx, t2 - t1))

        assert written == block.size

    def _writes_finished(self) -> bool:
        return len(self._write_queue) == 0 and self._outstanding_aio_writes == 0

    def write_get_completed(self, timeout: Optional[int] = None) -> Iterator[Union[DereferencedBlock, BaseException]]:
        try:
            while not self._writes_finished():
                logger.debug('Write queue length, outstanding writes, completion queue length: {}, {}, {}.'.format(
                    len(self._write_queue), self._outstanding_aio_writes, self._write_completion_queue.qsize()))
                self._submit_aio_writes()

                completion, t1, t2, block = self._write_completion_queue.get(block=(timeout is None or timeout != 0),
                                                                             timeout=timeout)
                assert self._outstanding_aio_writes > 0
                self._outstanding_aio_writes -= 1

                try:
                    completion.wait_for_complete_and_cb()
                except Exception as exception:
                    yield exception
                else:
                    write_return_value = completion.get_return_value()
                    if write_return_value != 0:
                        raise IOError('Write of block {} failed.'.format(block.idx))

                    logger.debug('Wrote block {} in {:.3f}s'.format(block.idx, t2 - t1))

                    yield block

                self._write_completion_queue.task_done()
        except queue.Empty:
            return
        else:
            return
