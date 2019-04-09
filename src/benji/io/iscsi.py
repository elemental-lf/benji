#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import threading
import time
from typing import Tuple, Optional

import libiscsi.libiscsi as libiscsi

from benji.config import ConfigDict, Config
from benji.database import DereferencedBlock
from benji.exception import ConfigurationError, UsageError
from benji.io.base import SimpleIOBase
from benji.logging import logger


class IO(SimpleIOBase):

    _pool_name: Optional[str]
    _image_name: Optional[str]
    _snapshot_name: Optional[str]

    def __init__(self, *, config: Config, name: str, module_configuration: ConfigDict, url: str,
                 block_size: int) -> None:
        super().__init__(
            config=config, name=name, module_configuration=module_configuration, url=url, block_size=block_size)

        if self.parsed_url.params or self.parsed_url.fragment:
            raise UsageError('The supplied URL {} is invalid.'.format(self.url))

        self._user = config.get_from_dict(module_configuration, 'user', None, types=str)
        self._password = config.get_from_dict(module_configuration, 'password', None, types=str)
        header_digest = config.get_from_dict(module_configuration, 'headerDigest', types=str)
        header_digest_attr_name = 'ISCSI_HEADER_DIGEST_{}'.format(header_digest)
        if hasattr(libiscsi, header_digest_attr_name):
            self._header_digest = getattr(libiscsi, header_digest_attr_name)
        else:
            raise ConfigurationError('Unknown header digest setting {}.'.format(header_digest))
        self._initiator_name = config.get_from_dict(module_configuration, 'initiatorName', types=str)
        self._timeout = config.get_from_dict(module_configuration, 'timeout', None, types=int)

        self._iscsi_context = None

    @staticmethod
    def _iscsi_check_status(task: 'struct scsi_task *', operation: str):
        if task is not None:
            sense = libiscsi.scsi_sense()
            status = libiscsi.scsi_task_get_status(task, sense)
            if status != libiscsi.SCSI_STATUS_GOOD:
                raise RuntimeError('{} failed with status {} and ASCQ {}.'.format(
                    operation, status.name, libiscsi.scsi_sense_key_str(sense.ascq)))
        else:
            raise RuntimeError('No task was returned for {}.'.format(operation))

    def _open(self) -> None:
        if self._iscsi_context is not None:
            return

        # netloc includes username, password and port
        url = 'iscsi://{}{}?{}'.format(self.parsed_url.netloc, self.parsed_url.path, self.parsed_url.query)
        iscsi_context = None
        iscsi_url = None
        task = None
        try:
            iscsi_context = libiscsi.iscsi_create_context(self._initiator_name)
            iscsi_url = libiscsi.iscsi_parse_full_url(self._iscsi_context, url)
            if not iscsi_url:
                raise RuntimeError('iSCSI URL {} is invalid.'.format(url))

            libiscsi.iscsi_set_targetname(iscsi_context, iscsi_url.target)
            libiscsi.iscsi_set_session_type(iscsi_context, libiscsi.ISCSI_SESSION_NORMAL)
            libiscsi.iscsi_set_header_digest(iscsi_context, self._header_digest)
            libiscsi.iscsi_set_timeout(iscsi_context, self._timeout)
            if self._user is not None:
                libiscsi.iscsi_set_initiator_username_pwd(iscsi_context, self._user.encode('ascii'),
                                                          self._password.encode('ascii'))
            libiscsi.iscsi_full_connect_sync(iscsi_context, iscsi_url.portal, iscsi_url.lun)

            task = libiscsi.iscsi_readcapacity16_sync(iscsi_context, iscsi_url.lun)
            self._iscsi_check_status(task, 'READ CAPACITY(16)')

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
                raise RuntimeError(
                    'Block size of version is not aligned to block size of iSCSI target (remainder {}).'.format(
                        self.block_size % self._iscsi_block_size))
        except Exception:
            if task is not None:
                libiscsi.scsi_free_scsi_task(task)
            if iscsi_url is not None:
                libiscsi.iscsi_destroy_url(iscsi_url)
            if iscsi_context is not None:
                libiscsi.iscsi_destroy_context(iscsi_context)
            raise
        else:
            libiscsi.scsi_free_scsi_task(task)
            libiscsi.iscsi_destroy_url(iscsi_url)

        self._iscsi_context = iscsi_context

        logger.debug('Opened iSCSI device {} with block size {} and {} blocks.'.format(
            self.url, self._iscsi_block_size, self._iscsi_num_blocks))

    def open_r(self) -> None:
        self._open()

    def open_w(self, size: int, force: bool = False, sparse: bool = False) -> None:
        self._open()

    def size(self) -> int:
        self._open()
        return self._iscsi_block_size * self._iscsi_num_blocks

    def _read(self, block: DereferencedBlock) -> Tuple[DereferencedBlock, bytes]:
        assert block.size == self.block_size
        lba = (block.id * self.block_size) // self._iscsi_block_size
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
        task = None
        try:
            task = libiscsi.iscsi_read16_sync(self._iscsi_context, self._iscsi_lun, lba, self.block_size,
                                              self._iscsi_block_size, 0, 0, 0, 0, 0)
            self._iscsi_check_status(task, 'READ(16)')

            assert task.datain.size == self.block_size
            data = libiscsi.bytes(task.datain.data, self.block_size)
        finally:
            if task is not None:
                libiscsi.scsi_free_scsi_task(task)
        t2 = time.time()

        logger.debug('{} read block {} in {:.2f}s'.format(
            threading.current_thread().name,
            block.id,
            t2 - t1,
        ))

        return block, data

    def _write(self, block: DereferencedBlock, data: bytes) -> DereferencedBlock:
        assert block.size == self.block_size
        lba = (block.id * self.block_size) // self._iscsi_block_size
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
        task = None
        try:
            task = libiscsi.iscsi_write16_sync(self._iscsi_context, self._iscsi_lun, lba, data, self._iscsi_block_size,
                                               0, 0, 0, 0, 0)
            self._iscsi_check_status(task, 'WRITE(16)')
        finally:
            if task is not None:
                libiscsi.scsi_free_scsi_task(task)
        t2 = time.time()

        logger.debug('{} wrote block {} in {:.2f}s'.format(
            threading.current_thread().name,
            block.id,
            t2 - t1,
        ))

        return block

    def close(self) -> None:
        super().close()
        if self._iscsi_context is not None:
            libiscsi.iscsi_destroy_context(self._iscsi_context)
