#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import re
import threading
import time
from typing import Tuple

import rados
import rbd

from benji.config import ConfigDict, Config
from benji.exception import UsageError, ConfigurationError
from benji.io.base import IOBase
from benji.logging import logger
from benji.database import DereferencedBlock


class IO(IOBase):

    def __init__(self, *, config: Config, name: str, module_configuration: ConfigDict, path: str,
                 block_size: int) -> None:
        super().__init__(
            config=config, name=name, module_configuration=module_configuration, path=path, block_size=block_size)

        ceph_conffile = config.get_from_dict(module_configuration, 'cephConfigFile', types=str)
        client_identifier = config.get_from_dict(module_configuration, 'clientIdentifier', types=str)
        self._cluster = rados.Rados(conffile=ceph_conffile, rados_id=client_identifier)
        self._cluster.connect()
        # create a bitwise or'd list of the configured features
        self._new_image_features = 0
        for feature in config.get_from_dict(module_configuration, 'newImageFeatures', types=list):
            try:
                self._new_image_features = self._new_image_features | getattr(rbd, feature)
            except AttributeError:
                raise ConfigurationError('{}: Unknown image feature {}.'.format(module_configuration.full_name, feature))

        self._writer = None

    def open_r(self) -> None:
        super().open_r()

        re_match = re.match('^([^/]+)/([^@]+)@?(.+)?$', self._path)
        if not re_match:
            raise UsageError(
                'IO path {} is invalid . Need {}://<pool>/<imagename> or {}://<pool>/<imagename>@<snapshotname>.'.format(
                    self._path, self.name, self.name))
        self._pool_name, self._image_name, self._snapshot_name = re_match.groups()

        # try opening it and quit if that's not possible.
        try:
            ioctx = self._cluster.open_ioctx(self._pool_name)
        except rados.ObjectNotFound:
            raise FileNotFoundError('RBD pool {} not found.'.format(self._pool_name)) from None

        try:
            rbd.Image(ioctx, self._image_name, self._snapshot_name, read_only=True)
        except rbd.ImageNotFound:
            raise FileNotFoundError('Image or snapshot not found for IO path {}.'.format(self._path)) from None

    def open_w(self, size: int, force: bool = False) -> None:
        re_match = re.match('^rbd://([^/]+)/([^@]+)$', self._path)
        if not re_match:
            raise UsageError('IO path {} is invalid . Need {}://<pool>/<imagename>.'.format(self._path, self.name))
        self._pool_name, self._image_name = re_match.groups()

        # try opening it and quit if that's not possible.
        try:
            ioctx = self._cluster.open_ioctx(self._pool_name)
        except rados.ObjectNotFound:
            raise FileNotFoundError('RBD pool {} not found.'.format(self._pool_name)) from None

        try:
            rbd.Image(ioctx, self._image_name)
        except rbd.ImageNotFound:
            rbd.RBD().create(ioctx, self._image_name, size, old_format=False, features=self._new_image_features)
        else:
            if not force:
                raise FileExistsError(
                    'Restore target {}://{} already exists. Force the restore if you want to overwrite it.'.format(
                        self.name, self._path))
            else:
                if size < self.size():
                    raise IOError(
                        'Restore target {}://{} is too small. Its size is {} bytes, but we need {} bytes for the restore.'.format(
                            self._name, self._path, self.size(), size))

    def size(self) -> int:
        ioctx = self._cluster.open_ioctx(self._pool_name)
        with rbd.Image(ioctx, self._image_name, self._snapshot_name, read_only=True) as image:
            size = image.size()
        return size

    def _read(self, block: DereferencedBlock) -> Tuple[DereferencedBlock, bytes]:
        ioctx = self._cluster.open_ioctx(self._pool_name)
        with rbd.Image(ioctx, self._image_name, self._snapshot_name, read_only=True) as image:
            offset = block.id * self._block_size
            t1 = time.time()
            data = image.read(offset, block.size, rados.LIBRADOS_OP_FLAG_FADVISE_DONTNEED)
            t2 = time.time()

        if not data:
            raise EOFError('EOF reached on source when there should be data.')

        logger.debug('{} read block {} in {:.2f}s'.format(
            threading.current_thread().name,
            block.id,
            t2 - t1,
        ))

        return block, data

    def write(self, block: DereferencedBlock, data: bytes) -> None:
        if not self._writer:
            ioctx = self._cluster.open_ioctx(self._pool_name)
            self._writer = rbd.Image(ioctx, self._image_name)

        offset = block.id * self._block_size
        written = self._writer.write(data, offset, rados.LIBRADOS_OP_FLAG_FADVISE_DONTNEED)  # type: ignore
        assert written == len(data)
        if written != len(data):
            raise IOError('Wanted to write {} bytes to restore target {}://{}, but only {} bytes written.', len(data),
                          self.name, self._path, written)

    def close(self) -> None:
        super().close()
        if self._writer:
            self._writer.close()
