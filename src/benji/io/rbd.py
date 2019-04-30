#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import re
import threading
import time
from typing import Tuple, Optional

import rados
import rbd

from benji.config import ConfigDict, Config
from benji.database import DereferencedBlock
from benji.exception import UsageError, ConfigurationError
from benji.io.base import ThreadedIOBase
from benji.logging import logger


class IO(ThreadedIOBase):

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
                    or self.parsed_url.params or self.parsed_url.fragment or self.parsed_url.query:
            raise UsageError('The supplied URL {} is invalid.'.format(self.url))

        ceph_config_file = config.get_from_dict(module_configuration, 'cephConfigFile', types=str)
        client_identifier = config.get_from_dict(module_configuration, 'clientIdentifier', types=str)
        self._cluster = rados.Rados(conffile=ceph_config_file, rados_id=client_identifier)
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

    def open_r(self) -> None:
        super().open_r()

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
            rbd.Image(ioctx, self._image_name, self._snapshot_name, read_only=True)
        except rbd.ImageNotFound:
            raise FileNotFoundError('RBD image or snapshot {} not found.'.format(self.url)) from None

    def open_w(self, size: int, force: bool = False, sparse: bool = False) -> None:
        super().open_w(size, force, sparse)

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
            image = rbd.Image(ioctx, self._image_name)
        except rbd.ImageNotFound:
            rbd.RBD().create(ioctx, self._image_name, size, old_format=False, features=self._new_image_features)
            rbd.Image(ioctx, self._image_name)
        else:
            try:
                if not force:
                    raise FileExistsError(
                        'RBD image {} already exists. Force the restore if you want to overwrite it.'.format(self.url))
                else:
                    image_size = image.size()
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
                            image.discard(region_start, region_length)
                            region_start += region_length
                            bytes_to_end -= region_length
            finally:
                image.close()

    def size(self) -> int:
        assert self._pool_name is not None and self._image_name is not None
        ioctx = self._cluster.open_ioctx(self._pool_name)
        with rbd.Image(ioctx, self._image_name, self._snapshot_name, read_only=True) as image:
            size = image.size()
        return size

    def _read(self, block: DereferencedBlock) -> Tuple[DereferencedBlock, bytes]:
        offset = block.id * self.block_size
        t1 = time.time()
        ioctx = self._cluster.open_ioctx(self._pool_name)
        with rbd.Image(ioctx, self._image_name, self._snapshot_name, read_only=True) as image:
            data = image.read(offset, block.size, rados.LIBRADOS_OP_FLAG_FADVISE_DONTNEED)
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
        ioctx = self._cluster.open_ioctx(self._pool_name)
        with rbd.Image(ioctx, self._image_name, self._snapshot_name) as image:
            written = image.write(data, offset, rados.LIBRADOS_OP_FLAG_FADVISE_DONTNEED)
        t2 = time.time()

        logger.debug('{} wrote block {} in {:.2f}s'.format(
            threading.current_thread().name,
            block.id,
            t2 - t1,
        ))

        assert written == len(data)
        return block
