#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import logging
import random
import time
from typing import List, Any

import b2
import b2.api
import b2.file_version
from b2.account_info.exception import MissingAccountData
from b2.account_info.in_memory import InMemoryAccountInfo
from b2.account_info.sqlite_account_info import SqliteAccountInfo
from b2.download_dest import DownloadDestBytes
from b2.exception import B2Error, FileNotPresent, B2ConnectionError

from benji.config import Config, ConfigDict
from benji.logging import logger
from benji.storage.base import ReadCacheStorageBase


class Storage(ReadCacheStorageBase):

    WRITE_QUEUE_LENGTH = 20
    READ_QUEUE_LENGTH = 20

    def __init__(self, *, config: Config, name: str, storage_id: int, module_configuration: ConfigDict):
        super().__init__(config=config, name=name, storage_id=storage_id, module_configuration=module_configuration)

        account_id = Config.get_from_dict(module_configuration, 'accountId', None, types=str)
        if account_id is None:
            account_id_file = Config.get_from_dict(module_configuration, 'accountIdFile', types=str)
            with open(account_id_file, 'r') as f:
                account_id = f.read().rstrip()
        application_key = Config.get_from_dict(module_configuration, 'applicationKey', None, types=str)
        if application_key is None:
            application_key_file = Config.get_from_dict(module_configuration, 'applicationKeyFile', types=str)
            with open(application_key_file, 'r') as f:
                application_key = f.read().rstrip()

        bucket_name = Config.get_from_dict(module_configuration, 'bucketName', types=str)

        account_info_file = Config.get_from_dict(module_configuration, 'accountInfoFile', None, types=str)
        if account_info_file is not None:
            account_info = SqliteAccountInfo(file_name=account_info_file)
        else:
            account_info = InMemoryAccountInfo()

        b2.bucket.Bucket.MAX_UPLOAD_ATTEMPTS = Config.get_from_dict(module_configuration, 'uploadAttempts', types=int)

        self._write_object_attempts = Config.get_from_dict(module_configuration, 'writeObjectAttempts', types=int)

        self._read_object_attempts = Config.get_from_dict(module_configuration, 'readObjectAttempts', types=int)

        self.service = b2.api.B2Api(account_info)
        if account_info_file is not None:
            try:
                # This temporarily disables all logging as the b2 library does some very verbose logging
                # of the exception we're trying to catch here...
                logging.disable(logging.ERROR)
                _ = self.service.get_account_id()
                logging.disable(logging.NOTSET)
            except MissingAccountData:
                self.service.authorize_account('production', account_id, application_key)
        else:
            self.service.authorize_account('production', account_id, application_key)

        self.bucket = self.service.get_bucket_by_name(bucket_name)

    def _write_object(self, key: str, data: bytes) -> None:
        for i in range(self._write_object_attempts):
            try:
                self.bucket.upload_bytes(data, key)
            except (B2Error, B2ConnectionError):
                if i + 1 < self._write_object_attempts:
                    sleep_time = (2**(i + 1)) + (random.randint(0, 1000) / 1000)
                    logger.warning(
                        'Upload of object with key {} to B2 failed repeatedly, will try again in {:.2f} seconds.'.format(
                            key, sleep_time))
                    time.sleep(sleep_time)
                    continue
                raise
            else:
                break

    def _read_object(self, key: str) -> bytes:
        for i in range(self._read_object_attempts):
            data_io = DownloadDestBytes()
            try:
                self.bucket.download_file_by_name(key, data_io)
            except (B2Error, B2ConnectionError) as exception:
                if isinstance(exception, FileNotPresent):
                    raise FileNotFoundError('Object {} not found.'.format(key)) from None
                else:
                    if i + 1 < self._read_object_attempts:
                        sleep_time = (2**(i + 1)) + (random.randint(0, 1000) / 1000)
                        logger.warning(
                            'Download of object with key {} to B2 failed, will try again in {:.2f} seconds.'.format(
                                key, sleep_time))
                        time.sleep(sleep_time)
                        continue
                    raise
            else:
                break

        return data_io.get_bytes_written()

    def _file_info(self, key: str) -> Any:
        r = self.bucket.list_file_names(key, 1)
        for entry in r['files']:
            file_version_info = b2.file_version.FileVersionInfoFactory.from_api_response(entry)
            if file_version_info.file_name == key:
                return file_version_info

        raise FileNotFoundError('Object {} not found.'.format(key))

    def _read_object_length(self, key: str) -> int:
        for i in range(self._read_object_attempts):
            try:
                file_version_info = self._file_info(key)
            except (B2Error, B2ConnectionError) as exception:
                if isinstance(exception, FileNotPresent):
                    raise FileNotFoundError('Object {} not found.'.format(key)) from None
                else:
                    if i + 1 < self._read_object_attempts:
                        sleep_time = (2**(i + 1)) + (random.randint(0, 1000) / 1000)
                        logger.warning(
                            'Object length request for key {} to B2 failed, will try again in {:.2f} seconds.'.format(
                                key, sleep_time))
                        time.sleep(sleep_time)
                        continue
                    raise
            else:
                break

        return file_version_info.size

    def _rm_object(self, key: str) -> None:
        try:
            file_version_info = self._file_info(key)
            self.bucket.delete_file_version(file_version_info.id_, file_version_info.file_name)
        except B2Error as exception:
            if isinstance(exception, FileNotPresent):
                raise FileNotFoundError('Object {} not found.'.format(key)) from None
            else:
                raise

    # def _rm_many_objects(self, keys: Sequence[str]) -> List[str]:
    #     """ Deletes many keys from the storage and returns a list of keys that couldn't be deleted.
    #     """
    #     errors = []
    #     for key in keys:
    #         try:
    #             file_version_info = self._file_info(key)
    #             self.bucket.delete_file_version(file_version_info.id_, file_version_info.file_name)
    #         except (B2Error, FileNotFoundError):
    #             errors.append(key)
    #     return errors

    def _list_objects(self, prefix: str) -> List[str]:
        return [
            file_version_info.file_name
            for (file_version_info, folder_name) in self.bucket.ls(folder_to_list=prefix, recursive=True)
        ]
