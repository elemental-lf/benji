#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import threading
from typing import List

import boto3
from botocore.client import Config as BotoCoreClientConfig
from botocore.exceptions import ClientError
from botocore.handlers import set_list_objects_encoding_type_url

from benji.config import Config, ConfigDict
from benji.logging import logger
from benji.storage.base import ReadCacheStorageBase


class Storage(ReadCacheStorageBase):

    WRITE_QUEUE_LENGTH = 20
    READ_QUEUE_LENGTH = 20

    def __init__(self, *, config: Config, name: str, storage_id: int, module_configuration: ConfigDict):
        aws_access_key_id = Config.get_from_dict(module_configuration, 'awsAccessKeyId', None, types=str)
        if aws_access_key_id is None:
            aws_access_key_id_file = Config.get_from_dict(module_configuration, 'awsAccessKeyIdFile', types=str)
            with open(aws_access_key_id_file, 'r') as f:
                aws_access_key_id = f.read().rstrip()
        aws_secret_access_key = Config.get_from_dict(module_configuration, 'awsSecretAccessKey', None, types=str)
        if aws_secret_access_key is None:
            aws_secret_access_key_file = Config.get_from_dict(module_configuration, 'awsSecretAccessKeyFile', types=str)
            with open(aws_secret_access_key_file, 'r') as f:
                aws_secret_access_key = f.read().rstrip()
        region_name = Config.get_from_dict(module_configuration, 'regionName', None, types=str)
        endpoint_url = Config.get_from_dict(module_configuration, 'endpointUrl', None, types=str)
        use_ssl = Config.get_from_dict(module_configuration, 'useSsl', None, types=bool)
        addressing_style = Config.get_from_dict(module_configuration, 'addressingStyle', None, types=str)
        signature_version = Config.get_from_dict(module_configuration, 'signatureVersion', None, types=str)

        self._bucket_name = Config.get_from_dict(module_configuration, 'bucketName', types=str)
        self._disable_encoding_type = Config.get_from_dict(module_configuration, 'disableEncodingType', types=bool)

        self._resource_config = {
            'aws_access_key_id': aws_access_key_id,
            'aws_secret_access_key': aws_secret_access_key,
        }

        if region_name:
            self._resource_config['region_name'] = region_name

        if endpoint_url:
            self._resource_config['endpoint_url'] = endpoint_url

        if use_ssl:
            self._resource_config['use_ssl'] = use_ssl

        resource_config = {}
        if addressing_style:
            resource_config['s3'] = {'addressing_style': addressing_style}

        if signature_version:
            resource_config['signature_version'] = signature_version

        self._resource_config['config'] = BotoCoreClientConfig(**resource_config)
        self._local = threading.local()
        self._init_connection()
        self._local.bucket = self._local.resource.Bucket(self._bucket_name)

        super().__init__(config=config, name=name, storage_id=storage_id, module_configuration=module_configuration)

    def _init_connection(self) -> None:
        if not hasattr(self._local, 'session'):
            logger.debug('Initializing S3 session and resource for {}'.format(threading.current_thread().name))
            self._local.session = boto3.session.Session()
            if self._disable_encoding_type:
                self._local.session.events.unregister('before-parameter-build.s3.ListObjects',
                                                      set_list_objects_encoding_type_url)
            self._local.resource = self._local.session.resource('s3', **self._resource_config)
            self._local.bucket = self._local.resource.Bucket(self._bucket_name)

    def _write_object(self, key: str, data: bytes) -> None:
        self._init_connection()
        object = self._local.bucket.Object(key)
        object.put(Body=data)

    def _read_object(self, key: str) -> bytes:
        self._init_connection()
        object = self._local.bucket.Object(key)
        try:
            data_dict = object.get()
            data = data_dict['Body'].read()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey' or e.response['Error']['Code'] == '404':
                raise FileNotFoundError('Key {} not found.'.format(key)) from None
            else:
                raise

        return data

    def _read_object_length(self, key: str) -> int:
        self._init_connection()
        object = self._local.bucket.Object(key)
        try:
            object.load()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey' or e.response['Error']['Code'] == '404':
                raise FileNotFoundError('Key {} not found.'.format(key)) from None
            else:
                raise

        return object.content_length

    def _rm_object(self, key):
        self._init_connection()
        # delete() always returns 204 even when key doesn't exist, so check for existence
        object = self._local.bucket.Object(key)
        try:
            object.load()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey' or e.response['Error']['Code'] == '404':
                raise FileNotFoundError('Key {} not found.'.format(key)) from None
            else:
                raise
        else:
            object.delete()

    # def _rm_many_objects(self, keys: Sequence[str]) -> List[str]:
    #     self._init_connection()
    #     errors: List[str] = []
    #     if self._multi_delete:
    #         # Amazon (at least) only handles 1000 deletes at a time
    #         # Split list into parts of at most 1000 elements
    #         keys_parts = [islice(keys, i, i + 1000) for i in range(0, len(keys), 1000)]
    #         for part in keys_parts:
    #             response = self._local.resource.meta.client.delete_objects(
    #                 Bucket=self._local.bucket.name, Delete={
    #                     'Objects': [{
    #                         'Key': key
    #                     } for key in part],
    #                 })
    #             if 'Errors' in response:
    #                 errors += list(map(lambda object: object['Key'], response['Errors']))
    #     else:
    #         for key in keys:
    #             try:
    #                 self._local.bucket.Object(key).delete()
    #             except ClientError:
    #                 errors.append(key)
    #     return errors

    def _list_objects(self, prefix: str) -> List[str]:
        self._init_connection()
        return [object.key for object in self._local.bucket.objects.filter(Prefix=prefix)]
