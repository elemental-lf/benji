#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import concurrent
import hashlib
import json
import os
import threading
import time
from abc import ABCMeta, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore

from diskcache import Cache

from benji.config import Config
from benji.storage.dicthmac import DictHMAC
from benji.exception import InternalError, ConfigurationError
from benji.factory import TransformFactory
from benji.logging import logger
from benji.metadata import BlockUid, VersionUid
from benji.utils import TokenBucket, future_results_as_completed, derive_key


class StorageBase(metaclass=ABCMeta):

    _TRANSFORMS_KEY = 'transforms'
    _SIZE_KEY = 'size'
    _OBJECT_SIZE_KEY = 'object_size'
    _CHECKSUM_KEY = 'checksum'
    _HMAC_KEY = 'hmac'

    _BLOCKS_PREFIX = 'blocks/'
    _VERSIONS_PREFIX = 'versions/'

    _META_SUFFIX = '.meta'

    def __init__(self, *, config, name, storage_id, module_configuration):
        self._name = name
        self._storage_id = storage_id
        self._active_transforms = []

        active_transforms = Config.get_from_dict(module_configuration, 'activeTransforms', None, types=list)
        if active_transforms is not None:
            for transform in active_transforms:
                self._active_transforms.append(TransformFactory.get_by_name(transform))

        simultaneous_writes = Config.get_from_dict(module_configuration, 'simultaneousWrites', types=int)
        simultaneous_reads = Config.get_from_dict(module_configuration, 'simultaneousReads', types=int)
        bandwidth_read = Config.get_from_dict(module_configuration, 'bandwidthRead', types=int)
        bandwidth_write = Config.get_from_dict(module_configuration, 'bandwidthWrite', types=int)

        self._consistency_check_writes = Config.get_from_dict(
            module_configuration, 'consistencyCheckWrites', False, types=bool)

        hmac_key = Config.get_from_dict(module_configuration, 'hmac.key', None, types=bytes)
        if hmac_key is None:
            hmac_kdf_salt = Config.get_from_dict(module_configuration, 'hmac.kdfSalt', None, types=bytes)
            hmac_kdf_iterations = Config.get_from_dict(module_configuration, 'hmac.kdfIterations', None, types=int)
            hmac_password = Config.get_from_dict(module_configuration, 'hmac.password', None, types=str)

            hmac_config_options_count = int(hmac_kdf_salt is not None) + int(hmac_kdf_iterations is not None) \
                                      + int(hmac_password is not None)
            if 0 < hmac_config_options_count < 3:
                raise ConfigurationError(
                    'Some but not all HMAC of the required configuration keys are set for storage {}, this is invalid.'.format(name))

            if hmac_config_options_count == 3:
                hmac_key = derive_key(
                    salt=hmac_kdf_salt, iterations=hmac_kdf_iterations, key_length=32, password=hmac_password)
        if hmac_key is not None:
            logger.info('Enabling HMAC metadata integrity protection for storage {}.'.format(name))
            self._dict_hmac = DictHMAC(dict_key=self._HMAC_KEY, key=hmac_key)
        else:
            self._dict_hmac = None

        self.read_throttling = TokenBucket()
        self.read_throttling.set_rate(bandwidth_read)  # 0 disables throttling
        self.write_throttling = TokenBucket()
        self.write_throttling.set_rate(bandwidth_write)  # 0 disables throttling

        self._read_executor = ThreadPoolExecutor(max_workers=simultaneous_reads, thread_name_prefix='Storage-Reader')
        self._read_futures = []
        self._read_semaphore = BoundedSemaphore(simultaneous_reads + self.READ_QUEUE_LENGTH)

        self._write_executor = ThreadPoolExecutor(max_workers=simultaneous_writes, thread_name_prefix='Storage-Writer')
        self._write_futures = []
        self._write_semaphore = BoundedSemaphore(simultaneous_writes + self.WRITE_QUEUE_LENGTH)

    @property
    def name(self):
        return self._name

    @property
    def storage_id(self):
        return self._storage_id

    def _build_metadata(self, *, size, object_size, transforms_metadata, checksum=None):
        metadata = {
            self._SIZE_KEY: size,
            self._OBJECT_SIZE_KEY: object_size,
        }

        if checksum:
            metadata[self._CHECKSUM_KEY] = checksum

        if transforms_metadata:
            metadata[self._TRANSFORMS_KEY] = transforms_metadata

        if self._dict_hmac:
            self._dict_hmac.add_hexdigest(metadata)

        return metadata, json.dumps(metadata, separators=(',', ':')).encode('utf-8')

    def _decode_metadata(self, *, metadata_json, key, data_length):
        metadata = json.loads(metadata_json.decode('utf-8'))

        if self._dict_hmac:
            self._dict_hmac.verify_hexdigest(metadata)

        for required_key in [self._OBJECT_SIZE_KEY, self._SIZE_KEY]:
            if required_key not in metadata:
                raise KeyError('Required metadata key {} is missing for object {}.'.format(required_key, key))

        if data_length != metadata[self._OBJECT_SIZE_KEY]:
            raise ValueError('Length mismatch for object {}. Expected: {}, got: {}.'.format(
                key, metadata[self._OBJECT_SIZE_KEY], data_length))

        return metadata

    def _check_write(self, *, key, metadata_key, data_expected):
        data_actual = self._read_object(key)
        metadata_actual_json = self._read_object(metadata_key)

        # Return value is ignored
        self._decode_metadata(metadata_json=metadata_actual_json, key=key, data_length=len(data_actual))

        # Comparing encapsulated data here
        if data_expected != data_actual:
            raise InternalError('Written and read data of {} differ.'.format(key))

    def _write(self, block, data):
        data, transforms_metadata = self._encapsulate(data)

        metadata, metadata_json = self._build_metadata(
            size=block.size, object_size=len(data), checksum=block.checksum, transforms_metadata=transforms_metadata)

        key = self._block_uid_to_key(block.uid)
        metadata_key = key + self._META_SUFFIX

        time.sleep(self.write_throttling.consume(len(data) + len(metadata_json)))
        t1 = time.time()
        try:
            self._write_object(key, data)
            self._write_object(metadata_key, metadata_json)
        except:
            try:
                self._rm_object(key)
                self._rm_object(metadata_key)
            except FileNotFoundError:
                pass
            raise
        t2 = time.time()

        logger.debug('{} wrote data of uid {} in {:.2f}s'.format(threading.current_thread().name, block.uid, t2 - t1))

        if self._consistency_check_writes:
            self._check_write(key=key, metadata_key=metadata_key, data_expected=data)

        return block

    def save(self, block, data, sync=False):
        if sync:
            self._write(block, data)
        else:
            self._write_semaphore.acquire()

            def write_with_release():
                try:
                    return self._write(block, data)
                except Exception:
                    raise
                finally:
                    self._write_semaphore.release()

            self._write_futures.append(self._write_executor.submit(write_with_release))

    def save_get_completed(self, timeout=None):
        """ Returns a generator for all completed read jobs
        """
        return future_results_as_completed(self._write_futures, timeout=timeout)

    def _read(self, block, metadata_only):
        key = self._block_uid_to_key(block.uid)
        metadata_key = key + self._META_SUFFIX
        t1 = time.time()
        if not metadata_only:
            data = self._read_object(key)
            data_length = len(data)
        else:
            data = None
            data_length = self._read_object_length(key)
        metadata_json = self._read_object(metadata_key)
        time.sleep(self.read_throttling.consume(len(data) if data else 0 + len(metadata_json)))
        t2 = time.time()

        metadata = self._decode_metadata(metadata_json=metadata_json, key=key, data_length=data_length)

        if self._CHECKSUM_KEY not in metadata:
            raise KeyError('Required metadata key {} is missing for block {} (UID {}).'.format(
                self._CHECKSUM_KEY, block.id, block.uid))

        if not metadata_only and self._TRANSFORMS_KEY in metadata:
            data = self._decapsulate(data, metadata[self._TRANSFORMS_KEY])

        logger.debug('{} read data of uid {} in {:.2f}s{}'.format(threading.current_thread().name, block.uid, t2 - t1,
                                                                  ' (metadata only)' if metadata_only else ''))

        return block, data, metadata

    def read(self, block, sync=False, metadata_only=False):
        if sync:
            return self._read(block, metadata_only)[1]
        else:

            def read_with_acquire():
                self._read_semaphore.acquire()
                return self._read(block, metadata_only)

            self._read_futures.append(self._read_executor.submit(read_with_acquire))

    def read_get_completed(self, timeout=None):
        """ Returns a generator for all completed read jobs
        """
        return future_results_as_completed(self._read_futures, semaphore=self._read_semaphore, timeout=timeout)

    def check_block_metadata(self, *, block, data_length, metadata):
        # Existence of keys has already been checked in _decode_metadata() and _read()
        if metadata[self._SIZE_KEY] != block.size:
            raise ValueError('Mismatch between recorded block size and data length in metadata for block {} (UID {}). '
                             'Expected: {}, got: {}.'.format(block.id, block.uid, block.size, metadata[self._SIZE_KEY]))

        if data_length and data_length != block.size:
            raise ValueError('Mismatch between recorded block size and actual data length for block {} (UID {}). '
                             'Expected: {}, got: {}.'.format(block.id, block.uid, block.size, data_length))

        if block.checksum != metadata[self._CHECKSUM_KEY]:
            raise ValueError('Mismatch between recorded block checksum and checksum in metadata for block {} (UID {}). '
                             'Expected: {}, got: {}.'.format(block.id, block.uid, block.checksum[:16],
                                                             metadata[self._CHECKSUM_KEY][:16]))

    def rm(self, uid):
        key = self._block_uid_to_key(uid)
        metadata_key = key + self._META_SUFFIX
        try:
            self._rm_object(key)
        finally:
            try:
                self._rm_object(metadata_key)
            except FileNotFoundError:
                pass

    def rm_many(self, uids):
        keys = [self._block_uid_to_key(uid) for uid in uids]
        metadata_keys = [key + self._META_SUFFIX for key in keys]

        errors = self._rm_many_objects(keys)
        self._rm_many_objects(metadata_keys)
        return [self._key_to_block_uid(error) for error in errors]

    def list_blocks(self):
        keys = self._list_objects(self._BLOCKS_PREFIX)
        block_uids = []
        for key in keys:
            if key.endswith(self._META_SUFFIX):
                continue
            try:
                block_uids.append(self._key_to_block_uid(key))
            except (RuntimeError, ValueError):
                # Ignore any keys which don't match our pattern to account for stray objects/files
                pass
        return block_uids

    def list_versions(self):
        keys = self._list_objects(self._VERSIONS_PREFIX)
        version_uids = []
        for key in keys:
            if key.endswith(self._META_SUFFIX):
                continue
            try:
                version_uids.append(self._key_to_version_uid(key))
            except (RuntimeError, ValueError):
                # Ignore any keys which don't match our pattern to account for stray objects/files
                pass
        return version_uids

    def read_version(self, version_uid):
        key = self._version_uid_to_key(version_uid)
        metadata_key = key + self._META_SUFFIX
        data = self._read_object(key)
        metadata_json = self._read_object(metadata_key)

        metadata = self._decode_metadata(metadata_json=metadata_json, key=key, data_length=len(data))

        if self._TRANSFORMS_KEY in metadata:
            data = self._decapsulate(data, metadata[self._TRANSFORMS_KEY])

        if len(data) != metadata[self._SIZE_KEY]:
            raise ValueError('Length mismatch of original data for object {}. Expected: {}, got: {}.'.format(
                key, metadata[self._SIZE_KEY], len(data)))

        data = data.decode('utf-8')
        return data

    def save_version(self, version_uid, data, overwrite=False):
        key = self._version_uid_to_key(version_uid)
        metadata_key = key + self._META_SUFFIX

        if not overwrite:
            try:
                self._read_object(key)
            except FileNotFoundError:
                pass
            else:
                raise FileExistsError('Version {} already exists in storage.'.format(version_uid.readable))

        data = data.encode('utf-8')
        size = len(data)

        data, transforms_metadata = self._encapsulate(data)
        metadata, metadata_json = self._build_metadata(
            size=size, object_size=len(data), transforms_metadata=transforms_metadata)

        try:
            self._write_object(key, data)
            self._write_object(metadata_key, metadata_json)
        except:
            try:
                self._rm_object(key)
                self._rm_object(metadata_key)
            except FileNotFoundError:
                pass
            raise

        if self._consistency_check_writes:
            self._check_write(key=key, metadata_key=metadata_key, data_expected=data)

    def rm_version(self, version_uid):
        key = self._version_uid_to_key(version_uid)
        metadata_key = key + self._META_SUFFIX
        try:
            self._rm_object(key)
        finally:
            try:
                self._rm_object(metadata_key)
            except FileNotFoundError:
                pass

    def _encapsulate(self, data):
        if self._active_transforms is not None:
            transforms_metadata = []
            for transform in self._active_transforms:
                data_encapsulated, materials = transform.encapsulate(data=data)
                if data_encapsulated:
                    transforms_metadata.append({
                        'name': transform.name,
                        'module': transform.module,
                        'materials': materials,
                    })
                    data = data_encapsulated
            return data, transforms_metadata
        else:
            return data, []

    def _decapsulate(self, data, transforms_metadata):
        for element in reversed(transforms_metadata):
            name = element['name']
            module = element['module']
            transform = TransformFactory.get_by_name(name)
            if transform:
                if module != transform.module:
                    raise ConfigurationError('Mismatch between object transform module and configured module for ' +
                                             '{} ({} != {})'.format(name, module, transform.module))

                data = transform.decapsulate(data=data, materials=element['materials'])
            else:
                raise IOError('Unknown transform {} in object metadata.'.format(name))
        return data

    def wait_reads_finished(self):
        concurrent.futures.wait(self._read_futures)

    def wait_saves_finished(self):
        concurrent.futures.wait(self._write_futures)

    def use_read_cache(self, enable):
        return False

    def close(self):
        if len(self._read_futures) > 0:
            logger.warning('Data backend closed with {} outstanding read jobs, cancelling them.'.format(
                len(self._read_futures)))
            for future in self._read_futures:
                future.cancel()
            logger.debug('Data backend cancelled all outstanding read jobs.')
            # Get all jobs so that the semaphore gets released and still waiting jobs can complete
            for future in self.read_get_completed():
                pass
            logger.debug('Data backend read results from all outstanding read jobs.')
        if len(self._write_futures) > 0:
            logger.warning('Data backend closed with {} outstanding write jobs, cancelling them.'.format(
                len(self._write_futures)))
            for future in self._write_futures:
                future.cancel()
            logger.debug('Data backend cancelled all outstanding write jobs.')
            # Write jobs release their semaphore at completion so we don't need to collect the results
            self._write_futures = []
        self._write_executor.shutdown()
        self._read_executor.shutdown()

    def _to_key(self, prefix, object_key):
        digest = hashlib.md5(object_key.encode('ascii')).hexdigest()
        return '{}{}/{}/{}'.format(prefix, digest[0:2], digest[2:4], object_key)

    def _from_key(self, prefix, key):
        if not key.startswith(prefix):
            raise RuntimeError('Invalid key name {}, it doesn\'t start with "{}".'.format(key, prefix))
        pl = len(prefix)
        if len(key) <= (pl + 6):
            raise RuntimeError('Key {} has an invalid length, expected at least {} characters.'.format(key, pl + 6))
        return key[pl + 6:]

    def _block_uid_to_key(self, block_uid):
        return self._to_key(self._BLOCKS_PREFIX, '{:016x}-{:016x}'.format(block_uid.left, block_uid.right))

    def _key_to_block_uid(self, key):
        object_key = self._from_key(self._BLOCKS_PREFIX, key)
        if len(object_key) != (16 + 1 + 16):
            raise RuntimeError('Object key {} has an invalid length, expected exactly {} characters.'.format(
                object_key, (16 + 1 + 16)))
        return BlockUid(int(object_key[0:16], 16), int(object_key[17:17 + 16], 16))

    def _version_uid_to_key(self, version_uid):
        return self._to_key(self._VERSIONS_PREFIX, version_uid.readable)

    def _key_to_version_uid(self, key):
        object_key = self._from_key(self._VERSIONS_PREFIX, key)
        vl = len(VersionUid(1).readable)
        if len(object_key) != vl:
            raise RuntimeError('Object key {} has an invalid length, expected exactly {} characters.'.format(
                object_key, vl))
        return VersionUid.create_from_readables(object_key)

    @abstractmethod
    def _write_object(self, key, data):
        raise NotImplementedError

    @abstractmethod
    def _read_object(self, key):
        raise NotImplementedError

    @abstractmethod
    def _read_object_length(self, key):
        raise NotImplementedError

    @abstractmethod
    def _rm_object(self):
        raise NotImplementedError

    @abstractmethod
    def _rm_many_objects(self):
        raise NotImplementedError

    @abstractmethod
    def _list_objects(self):
        raise NotImplementedError


class ReadCacheStorageBase(StorageBase):

    def __init__(self, *, config, name, storage_id, module_configuration):
        read_cache_directory = Config.get_from_dict(module_configuration, 'readCache.directory', None, types=str)
        read_cache_maximum_size = Config.get_from_dict(module_configuration, 'readCache.maximumSize', None, types=int)

        if read_cache_directory and not read_cache_maximum_size or not read_cache_directory and read_cache_maximum_size:
            raise ConfigurationError('Both readCache.directory and readCache.maximumSize need to be set ' + 'to enable disk based caching.')

        if read_cache_directory and read_cache_maximum_size:
            os.makedirs(read_cache_directory, exist_ok=True)
            try:
                self._read_cache = Cache(
                    read_cache_directory,
                    size_limit=read_cache_maximum_size,
                    eviction_policy='least-frequently-used',
                    statistics=1,
                )
            except Exception:
                logger.warning('Unable to enable disk based read caching. Continuing without it.')
                self._read_cache = None
            else:
                logger.debug('Disk based read caching instantiated (cache size {}).'.format(read_cache_maximum_size))
        else:
            self._read_cache = None
        self._use_read_cache = True

        # Start reader and write threads after the disk cached is created, so that they see it.
        super().__init__(config=config, name=name, storage_id=storage_id, module_configuration=module_configuration)

    def _read(self, block, metadata_only):
        key = self._block_uid_to_key(block.uid)
        metadata_key = key + self._META_SUFFIX
        if self._read_cache is not None and self._use_read_cache:
            metadata = self._read_cache.get(metadata_key)
            if metadata and metadata_only:
                return block, None, metadata
            elif metadata:
                data = self._read_cache.get(key)
                if data:
                    return block, data, metadata

        block, data, metadata = super()._read(block, metadata_only)

        # We always put blocks into the cache even when self._use_read_cache is False
        if self._read_cache is not None:
            self._read_cache.set(metadata_key, metadata)
            if not metadata_only:
                self._read_cache.set(key, data)

        return block, data, metadata

    def use_read_cache(self, enable):
        old_value = self._use_read_cache
        self._use_read_cache = enable
        return old_value

    def close(self):
        super().close()
        if self._read_cache is not None:
            (cache_hits, cache_misses) = self._read_cache.stats()
            logger.debug('Disk based cache statistics (since cache creation): {} hits, {} misses.'.format(
                cache_hits, cache_misses))
            self._read_cache.close()
