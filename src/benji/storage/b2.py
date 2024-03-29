#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import logging
import random
import time
from io import BytesIO
from typing import Union, Iterable, Tuple

from b2sdk.v2 import B2Api, UploadManager
from b2sdk.v2 import InMemoryAccountInfo
from b2sdk.v2 import SqliteAccountInfo
from b2sdk.v2.exception import MissingAccountData, B2Error, FileNotPresent

from benji.config import Config, ConfigDict
from benji.logging import logger
from benji.storage.base import ReadCacheStorageBase


class Storage(ReadCacheStorageBase):

    WRITE_QUEUE_LENGTH = 20
    READ_QUEUE_LENGTH = 20

    def __init__(self, *, config: Config, name: str, module_configuration: ConfigDict):
        super().__init__(config=config, name=name, module_configuration=module_configuration)

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

        UploadManager.MAX_UPLOAD_ATTEMPTS = Config.get_from_dict(module_configuration, 'uploadAttempts', types=int)

        self._write_object_attempts = Config.get_from_dict(module_configuration, 'writeObjectAttempts', types=int)

        self._read_object_attempts = Config.get_from_dict(module_configuration, 'readObjectAttempts', types=int)

        self.service = B2Api(account_info)
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

        # Check bucket configuration
        bucket_type = self.bucket.type_
        if bucket_type != 'allPrivate':
            logger.warning(f'The type of bucket {bucket_name} is {bucket_type}. '
                           'It is strongly recommended to set it to allPrivate.')

    def _write_object(self, key: str, data: bytes) -> None:
        for i in range(self._write_object_attempts):
            try:
                self.bucket.upload_bytes(data, key)
            # This is overly broad!
            except B2Error as exception:
                if i + 1 < self._write_object_attempts:
                    sleep_time = (2**(i + 1)) + (random.randint(0, 1000) / 1000)
                    logger.warning(
                        'Upload of object with key {} to B2 failed repeatedly, will try again in {:.2f} seconds. Exception thrown was {}'.format(
                            key, sleep_time, str(exception)))
                    time.sleep(sleep_time)
                    continue
                raise
            else:
                break

    def _read_object(self, key: str) -> bytes:
        for i in range(self._read_object_attempts):
            data_io = BytesIO()
            try:
                self.bucket.download_file_by_name(key).save(data_io)
            # This is overly broad!
            except B2Error as exception:
                if isinstance(exception, FileNotPresent):
                    raise FileNotFoundError('Object {} not found.'.format(key)) from None
                else:
                    if i + 1 < self._read_object_attempts:
                        sleep_time = (2**(i + 1)) + (random.randint(0, 1000) / 1000)
                        logger.warning(
                            'Download of object with key {} to B2 failed, will try again in {:.2f} seconds. Exception thrown was {}'.format(
                                key, sleep_time, str(exception)))
                        time.sleep(sleep_time)
                        continue
                    raise
            else:
                break

        return data_io.getvalue()

    def _read_object_length(self, key: str) -> int:
        for i in range(self._read_object_attempts):
            try:
                file_version_info = self.bucket.get_file_info_by_name(key)
            # This is overly broad!
            except B2Error as exception:
                if isinstance(exception, FileNotPresent):
                    raise FileNotFoundError('Object {} not found.'.format(key)) from None
                else:
                    if i + 1 < self._read_object_attempts:
                        sleep_time = (2**(i + 1)) + (random.randint(0, 1000) / 1000)
                        logger.warning(
                            'Object length request for key {} to B2 failed, will try again in {:.2f} seconds. Exception thrown was {}'.format(
                                key, sleep_time, str(exception)))
                        time.sleep(sleep_time)
                        continue
                    raise
            else:
                break

        return int(file_version_info.size)

    def _rm_object(self, key: str) -> None:
        try:
            file_version_info = self.bucket.get_file_info_by_name(key)
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

    def _list_objects(self,
                      prefix: str = None,
                      include_size: bool = False) -> Union[Iterable[str], Iterable[Tuple[str, int]]]:
        for file_version_info, folder_name in self.bucket.ls(folder_to_list=prefix if prefix is not None else '',
                                                             recursive=True):
            if include_size:
                yield file_version_info.file_name, file_version_info.size
            else:
                yield file_version_info.file_name
