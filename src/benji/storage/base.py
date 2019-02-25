#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import base64
import datetime
import json
import os
import threading
import time
from abc import ABCMeta, abstractmethod
from typing import Union, Optional, Dict, Tuple, List, Sequence, cast, Iterator

import semantic_version
from diskcache import FanoutCache

from benji.config import Config, ConfigDict
from benji.database import VersionUid, DereferencedBlock, BlockUid, Block
from benji.exception import ConfigurationError, BenjiException
from benji.factory import TransformFactory
from benji.jobexecutor import JobExecutor
from benji.logging import logger
from benji.repr import ReprMixIn
from benji.storage.dicthmac import DictHMAC
from benji.transform.base import TransformBase
from benji.utils import TokenBucket, derive_key
from benji.versions import VERSIONS


class InvalidBlockException(BenjiException, IOError):

    def __init__(self, message: str, block: DereferencedBlock) -> None:
        super().__init__(message)

        self._block = block

    @property
    def block(self) -> DereferencedBlock:
        return self._block


class BlockNotFoundError(BenjiException, IOError):

    def __init__(self, message: str, uid: BlockUid) -> None:
        super().__init__(message)

        self._uid = uid

    @property
    def uid(self) -> BlockUid:
        return self._uid


class StorageBase(ReprMixIn, metaclass=ABCMeta):

    _CHECKSUM_KEY = 'checksum'
    _CREATED_KEY = 'created'
    _MODIFIED_KEY = 'modified'
    _HMAC_KEY = 'hmac'
    _METADATA_VERSION_KEY = 'metadata_version'
    _OBJECT_SIZE_KEY = 'object_size'
    _SIZE_KEY = 'size'
    _TRANSFORMS_KEY = 'transforms'

    _META_SUFFIX = '.meta'

    def __init__(self, *, config: Config, name: str, storage_id: int, module_configuration: ConfigDict) -> None:
        self._name = name
        self._storage_id = storage_id
        self._active_transforms: List[TransformBase] = []

        active_transforms = Config.get_from_dict(module_configuration, 'activeTransforms', None, types=list)
        if active_transforms is not None:
            for transform in active_transforms:
                self._active_transforms.append(TransformFactory.get_by_name(transform))
            logger.info('Active transforms for storage {}: {}.'.format(
                name, ', '.join(
                    ['{} ({})'.format(transform.name, transform.module) for transform in self._active_transforms])))

        simultaneous_writes = Config.get_from_dict(module_configuration, 'simultaneousWrites', types=int)
        simultaneous_reads = Config.get_from_dict(module_configuration, 'simultaneousReads', types=int)
        simultaneous_removals = Config.get_from_dict(module_configuration, 'simultaneousRemovals', types=int)
        bandwidth_read = Config.get_from_dict(module_configuration, 'bandwidthRead', types=int)
        bandwidth_write = Config.get_from_dict(module_configuration, 'bandwidthWrite', types=int)

        self._consistency_check_writes = Config.get_from_dict(
            module_configuration, 'consistencyCheckWrites', False, types=bool)

        hmac_key_encoded = Config.get_from_dict(module_configuration, 'hmac.key', None, types=str)
        hmac_key: Optional[bytes] = None
        if hmac_key_encoded is None:
            hmac_password = Config.get_from_dict(module_configuration, 'hmac.password', None, types=str)
            if hmac_password is not None:
                hmac_kdf_salt = base64.b64decode(Config.get_from_dict(module_configuration, 'hmac.kdfSalt', types=str))
                hmac_kdf_iterations = Config.get_from_dict(module_configuration, 'hmac.kdfIterations', types=int)
                hmac_key = derive_key(
                    salt=hmac_kdf_salt, iterations=hmac_kdf_iterations, key_length=32, password=hmac_password)
        else:
            hmac_key = base64.b64decode(hmac_key_encoded)
        self._dict_hmac: Optional[DictHMAC] = None
        if hmac_key is not None:
            logger.info('Enabling HMAC object metadata integrity protection for storage {}.'.format(name))
            self._dict_hmac = DictHMAC(hmac_key=self._HMAC_KEY, secret_key=hmac_key)

        self.read_throttling = TokenBucket()
        self.read_throttling.set_rate(bandwidth_read)  # 0 disables throttling
        self.write_throttling = TokenBucket()
        self.write_throttling.set_rate(bandwidth_write)  # 0 disables throttling

        self._read_executor = JobExecutor(name='Storage-Read', workers=simultaneous_reads, blocking_submit=False)
        self._write_executor = JobExecutor(name='Storage-Write', workers=simultaneous_writes, blocking_submit=True)
        self._remove_executor = JobExecutor(name='Storage-Remove', workers=simultaneous_removals, blocking_submit=True)

    @property
    def name(self) -> str:
        return self._name

    @property
    def storage_id(self) -> int:
        return self._storage_id

    def _build_metadata(self,
                        *,
                        size: int,
                        object_size: int,
                        transforms_metadata: List[Dict] = None,
                        checksum: str = None) -> Tuple[Dict, bytes]:

        timestamp = datetime.datetime.utcnow().isoformat(timespec='microseconds')
        metadata: Dict = {
            self._CREATED_KEY: timestamp,
            self._METADATA_VERSION_KEY: str(VERSIONS.object_metadata.current),
            self._MODIFIED_KEY: timestamp,
            self._OBJECT_SIZE_KEY: object_size,
            self._SIZE_KEY: size,
        }

        if checksum:
            metadata[self._CHECKSUM_KEY] = checksum

        if transforms_metadata:
            metadata[self._TRANSFORMS_KEY] = transforms_metadata

        if self._dict_hmac:
            self._dict_hmac.add_digest(metadata)

        return metadata, json.dumps(metadata, separators=(',', ':')).encode('utf-8')

    def _decode_metadata(self, *, metadata_json: bytes, key: str, data_length: int) -> Dict:
        metadata = json.loads(metadata_json.decode('utf-8'))

        if self._dict_hmac:
            self._dict_hmac.verify_digest(metadata)

        # We currently support only one object metadata version
        if self._METADATA_VERSION_KEY not in metadata:
            raise KeyError('Required object metadata key {} is missing for object {}.'.format(
                self._METADATA_VERSION_KEY, key))
        version_obj = semantic_version.Version(metadata[self._METADATA_VERSION_KEY])
        if version_obj not in VERSIONS.object_metadata.supported:
            raise ValueError('Unsupported object metadata version: "{}".'.format(str(version_obj)))

        for required_key in [self._CREATED_KEY, self._MODIFIED_KEY, self._OBJECT_SIZE_KEY, self._SIZE_KEY]:
            if required_key not in metadata:
                raise KeyError('Required object metadata key {} is missing for object {}.'.format(required_key, key))

        if data_length != metadata[self._OBJECT_SIZE_KEY]:
            raise ValueError('Length mismatch for object {}. Expected: {}, got: {}.'.format(
                key, metadata[self._OBJECT_SIZE_KEY], data_length))

        return metadata

    def _check_write(self, *, key: str, metadata_key: str, data_expected: bytes) -> None:
        data_actual = self._read_object(key)
        metadata_actual_json = self._read_object(metadata_key)

        # Return value is ignored
        self._decode_metadata(metadata_json=metadata_actual_json, key=key, data_length=len(data_actual))

        # Comparing encapsulated data here
        if data_expected != data_actual:
            raise ValueError('Written and read data of {} differ.'.format(key))

    def _write(self, block: DereferencedBlock, data: bytes) -> DereferencedBlock:
        data, transforms_metadata = self._encapsulate(data)

        metadata, metadata_json = self._build_metadata(
            size=block.size, object_size=len(data), checksum=block.checksum, transforms_metadata=transforms_metadata)

        key = block.uid.storage_object_to_path()
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
            try:
                self._check_write(key=key, metadata_key=metadata_key, data_expected=data)
            except (KeyError, ValueError) as exception:
                raise InvalidBlockException('Check write of block {} (UID {}) failed.'.format(block.id, block.uid),
                                            block) from exception

        return block

    def write_block_async(self, block: Union[DereferencedBlock, Block], data: bytes) -> None:
        block_deref = block.deref() if isinstance(block, Block) else block

        def job():
            return self._write(block_deref, data)

        self._write_executor.submit(job)

    def write_block(self, block: Union[DereferencedBlock, Block], data: bytes) -> None:
        block_deref = block.deref() if isinstance(block, Block) else block
        self._write(block_deref, data)

    def write_get_completed(self, timeout: int = None) -> Iterator[Union[DereferencedBlock, BaseException]]:
        return self._write_executor.get_completed(timeout=timeout)

    def _read(self, block: DereferencedBlock, metadata_only: bool) -> Tuple[DereferencedBlock, Optional[bytes], Dict]:
        key = block.uid.storage_object_to_path()
        metadata_key = key + self._META_SUFFIX
        data: Optional[bytes] = None
        try:
            t1 = time.time()
            if not metadata_only:
                data = self._read_object(key)
                data_length = len(data)
            else:
                data_length = self._read_object_length(key)
            metadata_json = self._read_object(metadata_key)
            time.sleep(self.read_throttling.consume(len(data) if data else 0 + len(metadata_json)))
            t2 = time.time()
        except FileNotFoundError as exception:
            raise InvalidBlockException(
                'Object metadata or data of block {} (UID{}) not found.'.format(block.id, block.uid),
                block) from exception

        try:
            metadata = self._decode_metadata(metadata_json=metadata_json, key=key, data_length=data_length)
        except (KeyError, ValueError) as exception:
            raise InvalidBlockException('Object metadata of block {} (UID{}) is invalid.'.format(block.id, block.uid),
                                        block) from exception

        if self._CHECKSUM_KEY not in metadata:
            raise InvalidBlockException(
                'Required object metadata key {} is missing for block {} (UID {}).'.format(
                    self._CHECKSUM_KEY, block.id, block.uid), block)

        if not metadata_only and self._TRANSFORMS_KEY in metadata:
            data = self._decapsulate(data, metadata[self._TRANSFORMS_KEY])  # type: ignore

        logger.debug('{} read data of uid {} in {:.2f}s{}'.format(threading.current_thread().name, block.uid, t2 - t1,
                                                                  ' (metadata only)' if metadata_only else ''))

        return block, data, metadata

    def read_block_async(self, block: Block, metadata_only: bool = False) -> None:

        def job():
            return self._read(block.deref(), metadata_only)

        self._read_executor.submit(job)

    def read_block(self, block: Block, metadata_only: bool = False) -> Optional[bytes]:
        return self._read(block.deref(), metadata_only)[1]

    def read_get_completed(self,
                           timeout: int = None) -> Iterator[Union[Tuple[DereferencedBlock, bytes, Dict], BaseException]]:
        return self._read_executor.get_completed(timeout=timeout)

    def check_block_metadata(self, *, block: DereferencedBlock, data_length: Optional[int], metadata: Dict) -> None:
        # Existence of keys has already been checked in _decode_metadata() and _read()
        if metadata[self._SIZE_KEY] != block.size:
            raise ValueError(
                'Mismatch between recorded block size and data length in object metadata for block {} (UID {}). '
                'Expected: {}, got: {}.'.format(block.id, block.uid, block.size, metadata[self._SIZE_KEY]))

        if data_length and data_length != block.size:
            raise ValueError('Mismatch between recorded block size and actual data length for block {} (UID {}). '
                             'Expected: {}, got: {}.'.format(block.id, block.uid, block.size, data_length))

        if block.checksum != metadata[self._CHECKSUM_KEY]:
            raise ValueError(
                'Mismatch between recorded block checksum and checksum in object metadata for block {} (UID {}). '
                'Expected: {}, got: {}.'.format(
                    block.id,
                    block.uid,
                    cast(str, block.checksum)[:16],  # We know that block.checksum is set
                    metadata[self._CHECKSUM_KEY][:16]))

    def _rm_block(self, uid: BlockUid) -> BlockUid:
        key = uid.storage_object_to_path()
        metadata_key = key + self._META_SUFFIX
        try:
            self._rm_object(key)
        except FileNotFoundError as exception:
            raise BlockNotFoundError('Block UID {} not found on storage.'.format(str(uid)), uid) from exception
        finally:
            try:
                self._rm_object(metadata_key)
            except FileNotFoundError:
                pass
        return uid

    def rm_block_async(self, uid: BlockUid) -> None:

        def job():
            return self._rm_block(uid)

        self._remove_executor.submit(job)

    def rm_block(self, uid: BlockUid) -> None:
        self._rm_block(uid)

    def rm_get_completed(self, timeout: int = None) -> Iterator[Union[BlockUid, BaseException]]:
        return self._remove_executor.get_completed(timeout=timeout)

    def wait_rms_finished(self):
        self._remove_executor.wait_for_all()

    # def rm_many_blocks(self, uids: Union[Sequence[BlockUid], AbstractSet[BlockUid]]) -> List[BlockUid]:
    #     keys = [uid.storage_object_to_path() for uid in uids]
    #     metadata_keys = [key + self._META_SUFFIX for key in keys]
    #
    #     errors = self._rm_many_objects(keys)
    #     self._rm_many_objects(metadata_keys)
    #     return [cast(BlockUid, BlockUid.storage_path_to_object(error)) for error in errors]

    def list_blocks(self) -> List[BlockUid]:
        keys = self._list_objects(BlockUid.storage_prefix())
        block_uids: List[BlockUid] = []
        for key in keys:
            if key.endswith(self._META_SUFFIX):
                continue
            try:
                block_uids.append(cast(BlockUid, BlockUid.storage_path_to_object(key)))
            except (RuntimeError, ValueError):
                # Ignore any keys which don't match our pattern to account for stray objects/files
                pass
        return block_uids

    def list_versions(self) -> List[VersionUid]:
        keys = self._list_objects(VersionUid.storage_prefix())
        version_uids: List[VersionUid] = []
        for key in keys:
            if key.endswith(self._META_SUFFIX):
                continue
            try:
                version_uids.append(cast(VersionUid, VersionUid.storage_path_to_object(key)))
            except (RuntimeError, ValueError):
                # Ignore any keys which don't match our pattern to account for stray objects/files
                pass
        return version_uids

    def read_version(self, version_uid: VersionUid) -> str:
        key = version_uid.storage_object_to_path()
        metadata_key = key + self._META_SUFFIX
        data = self._read_object(key)
        metadata_json = self._read_object(metadata_key)

        metadata = self._decode_metadata(metadata_json=metadata_json, key=key, data_length=len(data))

        if self._TRANSFORMS_KEY in metadata:
            data = self._decapsulate(data, metadata[self._TRANSFORMS_KEY])

        if len(data) != metadata[self._SIZE_KEY]:
            raise ValueError('Length mismatch of original data for object {}. Expected: {}, got: {}.'.format(
                key, metadata[self._SIZE_KEY], len(data)))

        return data.decode('utf-8')

    def write_version(self, version_uid: VersionUid, data: str, overwrite: Optional[bool] = False) -> None:
        key = version_uid.storage_object_to_path()
        metadata_key = key + self._META_SUFFIX

        if not overwrite:
            try:
                self._read_object(key)
            except FileNotFoundError:
                pass
            else:
                raise FileExistsError('Version {} already exists in storage.'.format(version_uid.v_string))

        data_bytes = data.encode('utf-8')
        size = len(data_bytes)

        data_bytes, transforms_metadata = self._encapsulate(data_bytes)
        metadata, metadata_json = self._build_metadata(
            size=size, object_size=len(data_bytes), transforms_metadata=transforms_metadata)

        try:
            self._write_object(key, data_bytes)
            self._write_object(metadata_key, metadata_json)
        except:
            try:
                self._rm_object(key)
                self._rm_object(metadata_key)
            except FileNotFoundError:
                pass
            raise

        if self._consistency_check_writes:
            self._check_write(key=key, metadata_key=metadata_key, data_expected=data_bytes)

    def rm_version(self, version_uid: VersionUid) -> None:
        key = version_uid.storage_object_to_path()
        metadata_key = key + self._META_SUFFIX
        try:
            self._rm_object(key)
        finally:
            try:
                self._rm_object(metadata_key)
            except FileNotFoundError:
                pass

    def _encapsulate(self, data: bytes) -> Tuple[bytes, List]:
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

    def _decapsulate(self, data: bytes, transforms_metadata: Sequence[Dict]) -> bytes:
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

    def wait_writes_finished(self) -> None:
        self._write_executor.wait_for_all()

    def use_read_cache(self, enable: bool) -> bool:
        return False

    def close(self) -> None:
        self._read_executor.shutdown()
        self._write_executor.shutdown()
        self._remove_executor.shutdown()

    @abstractmethod
    def _write_object(self, key: str, data: bytes):
        raise NotImplementedError

    @abstractmethod
    def _read_object(self, key: str) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def _read_object_length(self, key: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def _rm_object(self, key: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def _list_objects(self, prefix: str) -> List[str]:
        raise NotImplementedError


class ReadCacheStorageBase(StorageBase):

    def __init__(self, *, config: Config, name: str, storage_id: int, module_configuration: ConfigDict) -> None:
        read_cache_directory = Config.get_from_dict(module_configuration, 'readCache.directory', None, types=str)
        read_cache_maximum_size = Config.get_from_dict(module_configuration, 'readCache.maximumSize', None, types=int)
        read_cache_shards = Config.get_from_dict(module_configuration, 'readCache.shards', None, types=int)

        if read_cache_directory and read_cache_maximum_size:
            os.makedirs(read_cache_directory, exist_ok=True)
            try:
                self._read_cache = FanoutCache(
                    read_cache_directory,
                    size_limit=read_cache_maximum_size,
                    shards=read_cache_shards,
                    eviction_policy='least-frequently-used',
                    statistics=1,
                )
            except Exception:
                logger.warning('Unable to enable disk based read caching. Continuing without it.')
                self._read_cache = None
            else:
                logger.debug('Disk based read caching instantiated (cache size {}, shards {}).'.format(
                    read_cache_maximum_size, read_cache_shards))
        else:
            self._read_cache = None
        self._use_read_cache = True

        # Start reader and write threads after the disk cached is created, so that they see it.
        super().__init__(config=config, name=name, storage_id=storage_id, module_configuration=module_configuration)

    def _read(self, block: DereferencedBlock, metadata_only: bool) -> Tuple[DereferencedBlock, Optional[bytes], Dict]:
        key = block.uid.storage_object_to_path()
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

    def use_read_cache(self, enable: bool) -> bool:
        old_value = self._use_read_cache
        self._use_read_cache = enable
        return old_value

    def close(self) -> None:
        super().close()
        if self._read_cache is not None:
            (cache_hits, cache_misses) = self._read_cache.stats()
            logger.debug('Disk based cache statistics (since cache creation): {} hits, {} misses.'.format(
                cache_hits, cache_misses))
            self._read_cache.close()
