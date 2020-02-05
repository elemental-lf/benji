#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import threading
import time
from typing import Tuple, Optional, Callable, Any, List, Union, Iterator

# noinspection PyUnresolvedReferences
import libiscsi.libiscsi as libiscsi

from benji.config import ConfigDict, Config
from benji.database import DereferencedBlock, Block
from benji.exception import ConfigurationError, UsageError
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

        if self.parsed_url.params or self.parsed_url.fragment:
            raise UsageError('The supplied URL {} is invalid.'.format(self.url))

        self._read_queue: List[DereferencedBlock] = []
        self._outstanding_write: Optional[Tuple[DereferencedBlock, bytes]] = None

        self._username = config.get_from_dict(module_configuration, 'username', None, types=str)
        self._password = config.get_from_dict(module_configuration, 'password', None, types=str)
        self._target_username = config.get_from_dict(module_configuration, 'targetUsername', None, types=str)
        self._target_password = config.get_from_dict(module_configuration, 'targetPassword', None, types=str)
        header_digest = config.get_from_dict(module_configuration, 'headerDigest', types=str)
        header_digest_attr_name = 'ISCSI_HEADER_DIGEST_{}'.format(header_digest)
        if hasattr(libiscsi, header_digest_attr_name):
            self._header_digest = getattr(libiscsi, header_digest_attr_name)
        else:
            raise ConfigurationError('Unknown header digest setting {}.'.format(header_digest))
        self._initiator_name = config.get_from_dict(module_configuration, 'initiatorName', types=str)
        self._timeout = config.get_from_dict(module_configuration, 'timeout', None, types=int)

        self._iscsi_context: Any = None

    @staticmethod
    def _iscsi_execute_sync(operation: str, function: Callable, iscsi_context, *args, **kwargs) -> Any:
        task = function(iscsi_context, *args, **kwargs)
        if task is not None:
            status, sense = libiscsi.scsi_task_get_status(task)
            if status != libiscsi.SCSI_STATUS_GOOD:
                raise RuntimeError('{} failed with {}, ASCQ {}.'.format(operation,
                                                                        libiscsi.scsi_sense_key_str(sense.key),
                                                                        libiscsi.scsi_sense_ascq_str(sense.ascq)))
            else:
                return task
        else:
            raise RuntimeError('{} failed: {}'.format(operation, libiscsi.iscsi_get_error(iscsi_context)))

    @staticmethod
    def _iscsi_call_sync(operation: str, function: Callable, iscsi_context, *args, **kwargs) -> Any:
        result = function(iscsi_context, *args, **kwargs)
        if result is None or isinstance(result, int) and result < 0:
            raise RuntimeError('{} failed: {}'.format(operation, libiscsi.iscsi_get_error(iscsi_context).rstrip()))
        return result

    def _open(self) -> None:
        if self._iscsi_context is not None:
            return

        # netloc includes username, password and port
        url = 'iscsi://{}{}?{}'.format(self.parsed_url.netloc, self.parsed_url.path, self.parsed_url.query)
        iscsi_context = libiscsi.iscsi_create_context(self._initiator_name)
        iscsi_url = self._iscsi_call_sync('URL parsing', libiscsi.iscsi_parse_full_url, self._iscsi_context, url)

        libiscsi.iscsi_set_targetname(iscsi_context, iscsi_url.target)
        libiscsi.iscsi_set_session_type(iscsi_context, libiscsi.ISCSI_SESSION_NORMAL)
        libiscsi.iscsi_set_header_digest(iscsi_context, self._header_digest)
        libiscsi.iscsi_set_timeout(iscsi_context, self._timeout)

        if len(iscsi_url.user) > 0:
            libiscsi.iscsi_set_initiator_username_pwd(iscsi_context, iscsi_url.user, iscsi_url.passwd)
        elif self._username is not None:
            libiscsi.iscsi_set_initiator_username_pwd(iscsi_context, self._username, self._password)
        # iscsi_set_target_username_pwd needs to come after iscsi_set_initiator_username_pwd
        if len(iscsi_url.target_user) > 0:
            libiscsi.iscsi_set_target_username_pwd(iscsi_context, iscsi_url.target_user, iscsi_url.target_passwd)
        elif self._target_username is not None:
            libiscsi.iscsi_set_target_username_pwd(iscsi_context, self._target_username, self._target_password)

        self._iscsi_call_sync('iSCSI connect', libiscsi.iscsi_full_connect_sync, iscsi_context, iscsi_url.portal,
                              iscsi_url.lun)

        task = self._iscsi_execute_sync('READ CAPACITY(16)', libiscsi.iscsi_readcapacity16_sync, iscsi_context,
                                        iscsi_url.lun)
        task_data = libiscsi.scsi_datain_unmarshall(task)
        if not task_data:
            raise RuntimeError('Failed to unmarshall READ CAPACITY(16) data.')

        self._iscsi_block_size = task_data.readcapacity16.block_length
        self._iscsi_num_blocks = task_data.readcapacity16.returned_lba + 1
        self._fully_provisioned = task_data.readcapacity16.lbpme == 0
        self._unmapped_is_zero = task_data.readcapacity16.lbprz != 0
        self._iscsi_lun = iscsi_url.lun

        if self.block_size < self._iscsi_block_size:
            raise RuntimeError('Block size of version is smaller than block size of iSCSI target ({} < {}).'.format(
                self.block_size, self._iscsi_block_size))

        if self.block_size % self._iscsi_block_size != 0:
            raise RuntimeError('Block size of version is not aligned to block size of iSCSI target (remainder {}).'.format(
                self.block_size % self._iscsi_block_size))

        self._iscsi_context = iscsi_context

        logger.debug('Opened iSCSI device {} with block size {} and {} blocks.'.format(
            self.url, self._iscsi_block_size, self._iscsi_num_blocks))

    def open_r(self) -> None:
        self._open()

    def open_w(self, size: int, force: bool = False, sparse: bool = False) -> None:
        self._open()

    def close(self) -> None:
        if len(self._read_queue) > 0:
            logger.warning('Closing IO module with {} outstanding read jobs.'.format(len(self._read_queue)))
            self._read_queue = []

        if self._outstanding_write is not None:
            logger.warning('Closing IO module with one outstanding write.')
            self._outstanding_write = None

        self._iscsi_context = None

    def size(self) -> int:
        self._open()
        return self._iscsi_block_size * self._iscsi_num_blocks

    def _read(self, block: DereferencedBlock) -> Tuple[DereferencedBlock, bytes]:
        assert block.size == self.block_size
        lba = (block.idx * self.block_size) // self._iscsi_block_size
        num_blocks = self.block_size // self._iscsi_block_size

        if lba >= self._iscsi_num_blocks:
            raise RuntimeError(
                'Attempt to read outside of the device. Requested LBA is {}, but device has only {} blocks. (1)'.format(
                    lba, self._iscsi_num_blocks))

        if lba + num_blocks > self._iscsi_num_blocks:
            raise RuntimeError(
                'Attempt to read outside of the device. Requested LBA is {}, but device has only {} blocks. (2)'.format(
                    lba + num_blocks, self._iscsi_num_blocks))

        t1 = time.time()
        task = self._iscsi_execute_sync('READ(16)', libiscsi.iscsi_read16_sync, self._iscsi_context, self._iscsi_lun,
                                        lba, self.block_size, self._iscsi_block_size, 0, 0, 0, 0, 0)

        data = task.datain
        assert len(data) == self.block_size
        t2 = time.time()

        logger.debug('{} read block {} in {:.3f}s'.format(
            threading.current_thread().name,
            block.idx,
            t2 - t1,
        ))

        return block, data

    def read(self, block: Union[DereferencedBlock, Block]) -> None:
        self._read_queue.append(block.deref())

    def read_sync(self, block: Union[DereferencedBlock, Block]) -> bytes:
        return self._read(block.deref())[1]

    def read_get_completed(self, timeout: Optional[int] = None
                          ) -> Iterator[Union[Tuple[DereferencedBlock, bytes], BaseException]]:
        while len(self._read_queue) > 0:
            yield self._read(self._read_queue.pop())

    def _write(self, block: DereferencedBlock, data: bytes) -> DereferencedBlock:
        assert block.size == self.block_size
        lba = (block.idx * self.block_size) // self._iscsi_block_size
        num_blocks = self.block_size // self._iscsi_block_size

        if lba >= self._iscsi_num_blocks:
            raise RuntimeError(
                'Attempt to write outside of the device. Requested LBA is {}, but device has only {} blocks. (1)'.format(
                    lba, self._iscsi_num_blocks))

        if lba + num_blocks > self._iscsi_num_blocks:
            raise RuntimeError(
                'Attempt to write outside of the device. Requested LBA is {}, but device has only {} blocks. (2)'.format(
                    lba + num_blocks, self._iscsi_num_blocks))

        t1 = time.time()
        self._iscsi_execute_sync('WRITE(16)', libiscsi.iscsi_write16_sync, self._iscsi_context, self._iscsi_lun, lba,
                                 data, self._iscsi_block_size, 0, 0, 0, 0, 0)
        t2 = time.time()

        logger.debug('{} wrote block {} in {:.3f}s'.format(
            threading.current_thread().name,
            block.idx,
            t2 - t1,
        ))

        return block

    def write(self, block: Union[DereferencedBlock, Block], data: bytes) -> None:
        assert self._outstanding_write is None
        self._outstanding_write = (block.deref(), data)

    def write_sync(self, block: Union[DereferencedBlock, Block], data: bytes) -> None:
        self._write(block.deref(), data)

    def write_get_completed(self, timeout: Optional[int] = None) -> Iterator[Union[DereferencedBlock, BaseException]]:
        if self._outstanding_write is not None:
            yield self._write(*self._outstanding_write)
            self._outstanding_write = None
