# -*- encoding: utf-8 -*-

import datetime
import errno
import hashlib
import os
import random
import time
from collections import defaultdict
from concurrent.futures import CancelledError, TimeoutError
from contextlib import AbstractContextManager
from io import StringIO, BytesIO
from typing import List, Tuple, TextIO, Optional, Set, Dict, cast, Union, \
    Sequence, Any, Iterator

from diskcache import Cache

from benji.blockuidhistory import BlockUidHistory
from benji.config import Config
from benji.database import Database, VersionUid, Version, Block, \
    BlockUid, DereferencedBlock, VersionStatus, Storage, Locking, DeletedBlock, SparseBlockUid
from benji.exception import InputDataError, InternalError, AlreadyLocked, UsageError, ScrubbingError, ConfigurationError
from benji.io.factory import IOFactory
from benji.logging import logger
from benji.repr import ReprMixIn
from benji.retentionfilter import RetentionFilter
from benji.storage.base import InvalidBlockException, BlockNotFoundError
from benji.storage.factory import StorageFactory
from benji.utils import notify, BlockHash, PrettyPrint, random_string, InputValidation


class Benji(ReprMixIn, AbstractContextManager):

    # This is in number of blocks (i.e. database rows in the blocks table)
    _BLOCKS_CREATE_WORK_PACKAGE = 10000

    def __init__(self,
                 config: Config,
                 init_database: bool = False,
                 migrate_database: bool = False,
                 in_memory_database: bool = False,
                 _destroy_database: bool = False) -> None:

        self.config = config

        self._block_size = config.get('blockSize', types=int)
        self._block_hash = BlockHash(config.get('hashFunction', types=str))
        self._process_name = config.get('processName', types=str)

        Database.configure(config, in_memory=in_memory_database)
        if init_database or in_memory_database:
            Database.init(_destroy=_destroy_database)
        if migrate_database:
            Database.migrate()
        Database.open()

        # Ensure that all defined storages are present
        storage_modules = StorageFactory.get_modules()
        for name, module in storage_modules.items():
            Storage.sync(name, storage_id=module.storage_id)
        default_storage_name = self.config.get('defaultStorage', types=str)
        if default_storage_name not in storage_modules.keys():
            raise ConfigurationError('Default storage {} is undefined.'.format(default_storage_name))
        self._default_storage_name = default_storage_name

        notify(self._process_name)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def create_version(self,
                       *,
                       version_uid: VersionUid,
                       volume: str,
                       snapshot: str,
                       block_size: int = None,
                       storage_name: str = None,
                       size: int = None,
                       base_version_uid: VersionUid = None,
                       base_version_locking: bool = True) -> Version:
        """ Prepares the metadata for a new version.
        If base_version_uid is given, this is taken as the base, otherwise
        a pure sparse version is created.
        """
        storage_id = Storage.get_by_name(storage_name).id if storage_name else None
        old_blocks_iter: Optional[Iterator[Block]] = None
        if base_version_uid:
            if not base_version_locking and not Locking.is_version_locked(base_version_uid):
                raise InternalError('Base version is not locked.')
            old_version = Version.get_by_uid(base_version_uid)  # raise if not exists
            if not old_version.status.is_valid():
                raise UsageError('You can only base a new version on a valid old version.')
            if block_size is not None and old_version.block_size != block_size:
                raise UsageError('You cannot base a new version on an old version with a different block size.')
            if storage_id is not None and old_version.storage_id != storage_id:
                raise UsageError('Base version and new version have to be in the same storage.')
            new_storage_id = old_version.storage_id

            base_version = Version.get_by_uid(base_version_uid)
            old_blocks_iter = base_version.blocks

            if size is not None:
                new_size = size
            else:
                new_size = old_version.size

            new_version_block_size = old_version.block_size
        else:
            new_storage_id = storage_id or Storage.get_by_name(self._default_storage_name).id
            if size is None:
                raise InternalError('Size needs to be specified if there is no base version.')
            new_size = size
            if block_size is None:
                raise InternalError('Block size needs to be specified if there is no base version.')
            new_version_block_size = block_size

        version = None
        try:
            if base_version_locking and base_version_uid:
                Locking.lock_version(base_version_uid, reason='Base version cloning')

            # We always start with invalid versions, then mark them valid after the backup succeeds.
            version = Version.create(version_uid=version_uid,
                                     volume=volume,
                                     snapshot=snapshot,
                                     size=new_size,
                                     block_size=new_version_block_size,
                                     storage_id=new_storage_id,
                                     status=VersionStatus.incomplete)
            Locking.lock_version(version.uid, reason='Preparing version')

            new_block_uid: Optional[BlockUid]
            new_block_checksum: Optional[str]
            new_block_block_size: int
            new_block_valid: bool
            blocks: List[Dict[str, Any]] = []
            old_version_exhausted = old_blocks_iter is None
            for idx in range(version.blocks_count):
                if not old_version_exhausted:
                    assert old_blocks_iter is not None
                    old_block = next(old_blocks_iter, None)
                    if old_block is not None:
                        assert old_block.idx == idx
                        new_block_uid = old_block.uid
                        new_block_checksum = old_block.checksum
                        new_block_block_size = old_block.size
                        new_block_valid = old_block.valid
                    else:
                        old_version_exhausted = True
                        new_block_uid = None
                        new_block_checksum = None
                        new_block_block_size = version.block_size
                        new_block_valid = True
                else:
                    new_block_uid = None
                    new_block_checksum = None
                    new_block_block_size = version.block_size
                    new_block_valid = True

                # This catches blocks which changed their size between the base version and the new version.
                # If the new version is bigger, this affects the last block of the old version and the last block of
                # the new version.
                # If the new version is smaller, this affects the last block of the new version.
                offset = idx * version.block_size
                new_block_block_size_tmp = min(version.block_size, new_size - offset)
                if new_block_block_size != new_block_block_size_tmp:
                    new_block_block_size = new_block_block_size_tmp
                    new_block_uid = None
                    new_block_checksum = None
                    # Forces reread.
                    new_block_valid = False

                blocks.append({
                    'idx': idx,
                    'uid_left': new_block_uid.left if new_block_uid is not None else None,
                    'uid_right': new_block_uid.right if new_block_uid is not None else None,
                    'checksum': new_block_checksum,
                    'size': new_block_block_size,
                    'valid': new_block_valid
                })

                notify(self._process_name,
                       'Preparing version {} ({:.1f}%)'.format(version.uid, (idx + 1) / version.blocks_count * 100))

                if len(blocks) == self._BLOCKS_CREATE_WORK_PACKAGE:
                    version.create_blocks(blocks=blocks)
                    blocks = []
            version.create_blocks(blocks=blocks)
        except:
            if version and Locking.is_version_locked(version.uid):
                Locking.unlock_version(version.uid)
            raise
        finally:
            if base_version_locking and base_version_uid and Locking.is_version_locked(base_version_uid):
                Locking.unlock_version(base_version_uid)
            notify(self._process_name)

        return version

    @staticmethod
    def get_version_by_uid(version_uid: VersionUid) -> Version:
        return Version.get_by_uid(version_uid)

    @staticmethod
    def find_versions_with_filter(filter_expression: str = None) -> List[Version]:
        return Version.find_with_filter(filter_expression)

    def _scrub_prepare(self,
                       *,
                       version: Version,
                       history: BlockUidHistory = None,
                       block_percentage: int,
                       deep_scrub: bool) -> int:
        storage = StorageFactory.get_by_name(version.storage.name)
        read_jobs = 0
        for i, block in enumerate(version.blocks):
            notify(
                self._process_name,
                'Preparing {} of version {} ({:.1f}%)'.format('deep-scrub' if deep_scrub else 'scrub', version.uid,
                                                              (i + 1) / version.blocks_count * 100))
            if not block.uid:
                logger.debug('{} of block {} (UID {}) skipped (sparse).'.format('Deep-scrub' if deep_scrub else 'Scrub',
                                                                                block.idx, block.uid))
                continue
            if history and history.seen(version.storage_id, block.uid):
                logger.debug('{} of block {} (UID {}) skipped (already seen).'.format(
                    'Deep-scrub' if deep_scrub else 'Scrub', block.idx, block.uid))
                continue
            # i != 0 makes sure that we always scrub at least one block (the first in this case)
            if i != 0 and block_percentage < 100 and random.randint(1, 100) > block_percentage:
                logger.debug('{} of block {} (UID {}) skipped (percentile is {}).'.format(
                    'Deep-scrub' if deep_scrub else 'Scrub', block.idx, block.uid, block_percentage))
            else:
                storage.read_block_async(block, metadata_only=(not deep_scrub))
                read_jobs += 1
        return read_jobs

    def _scrub_report_progress(self, *, version_uid: VersionUid, block: DereferencedBlock, read_jobs: int,
                               done_read_jobs: int, deep_scrub: bool) -> None:
        logger.debug('{} of block {} (UID {}) ok.'.format('Deep-scrub' if deep_scrub else 'Scrub', block.idx, block.uid))

        notify(
            self._process_name, '{} version {} ({:.1f}%)'.format('Deep-scrubbing' if deep_scrub else 'Scrubbing',
                                                                 version_uid, done_read_jobs / read_jobs * 100))
        log_every_jobs = read_jobs // 200 + 1  # about every half percent
        if done_read_jobs % log_every_jobs == 0 or done_read_jobs == read_jobs:
            logger.info('{} {}/{} blocks ({:.1f}%)'.format('Deep-scrubbed' if deep_scrub else 'Scrubbed',
                                                           done_read_jobs, read_jobs, done_read_jobs / read_jobs * 100))

    def scrub(self, version_uid: VersionUid, block_percentage: int = 100, history: BlockUidHistory = None) -> None:
        Locking.lock_version(version_uid, reason='Scrubbing version')
        try:
            version = Version.get_by_uid(version_uid)
            if not version.status.is_scrubbable():
                raise ScrubbingError('Version {} cannot be scrubbed, it has a status of {}.'.format(
                    version_uid, version.status.name))

        except:
            Locking.unlock_version(version_uid)
            raise

        valid = True
        affected_version_uids = []
        try:
            storage = StorageFactory.get_by_name(version.storage.name)
            read_jobs = self._scrub_prepare(version=version,
                                            history=history,
                                            block_percentage=block_percentage,
                                            deep_scrub=False)

            done_read_jobs = 0
            for entry in storage.read_get_completed():
                done_read_jobs += 1
                if isinstance(entry, Exception):
                    # If it really is a data inconsistency mark blocks invalid
                    if isinstance(entry, InvalidBlockException):
                        logger.error('Block {} (UID {}) is invalid: {}'.format(entry.block.idx, entry.block.uid, entry))
                        affected_version_uids.extend(Version.set_block_invalid(entry.block.uid))
                        valid = False
                        continue
                    else:
                        raise entry
                else:
                    block, data, metadata = cast(Tuple[DereferencedBlock, bytes, Dict], entry)

                try:
                    storage.check_block_metadata(block=block, data_length=None, metadata=metadata)
                except (KeyError, ValueError) as exception:
                    logger.error('Metadata check failed, block {} (UID {}) is invalid: {}'.format(
                        block.idx, block.uid, exception))
                    affected_version_uids.extend(Version.set_block_invalid(block.uid))
                    valid = False
                    continue

                if history:
                    history.add(version.storage_id, block.uid)

                self._scrub_report_progress(version_uid=version_uid,
                                            block=block,
                                            read_jobs=read_jobs,
                                            done_read_jobs=done_read_jobs,
                                            deep_scrub=False)
        finally:
            Locking.unlock_version(version_uid)
            notify(self._process_name)

        if read_jobs != done_read_jobs:
            raise InternalError(
                'Number of submitted and completed read jobs inconsistent (submitted: {}, completed {}).'.format(
                    read_jobs, done_read_jobs))

        # A scrub (in contrast to a deep-scrub) can only ever mark a version as invalid. To mark it as valid
        # there is not enough information.
        if valid:
            logger.info('Scrub of version {} successful.'.format(version.uid))
        else:
            logger.error('Marked version {} as invalid because it has errors.'.format(version_uid))
            affected_version_uids.remove(version_uid)
            if affected_version_uids:
                logger.error('Marked the following versions as invalid, too, because of invalid blocks: {}.'\
                             .format(', '.join(sorted(affected_version_uids))))
            raise ScrubbingError('Scrub of version {} failed.'.format(version_uid))

    def deep_scrub(self,
                   version_uid: VersionUid,
                   source: str = None,
                   block_percentage: int = 100,
                   history: BlockUidHistory = None) -> None:
        Locking.lock_version(version_uid, reason='Deep-scrubbing')
        try:
            version = Version.get_by_uid(version_uid)
            if not version.status.is_deep_scrubbable():
                raise ScrubbingError('Version {} cannot be deep-scrubbed, it has a status of {}.'.format(
                    version_uid, version.status.name))
            if not version.status.is_valid():
                logger.warn('Version {} has a status of {}.'.format(version_uid, version.status.name))

            if source:
                io = IOFactory.get(source, version.block_size)
                io.open_r()
        except:
            Locking.unlock_version(version_uid)
            raise

        valid = True
        source_mismatch = False
        affected_version_uids = []
        try:
            storage = StorageFactory.get_by_name(version.storage.name)
            old_use_read_cache = storage.use_read_cache(False)
            read_jobs = self._scrub_prepare(version=version,
                                            history=history,
                                            block_percentage=block_percentage,
                                            deep_scrub=True)

            done_read_jobs = 0
            for entry in storage.read_get_completed():
                done_read_jobs += 1
                if isinstance(entry, Exception):
                    # If it really is a data inconsistency mark blocks invalid
                    if isinstance(entry, InvalidBlockException):
                        logger.error('Block {} (UID {}) is invalid: {}'.format(entry.block.idx, entry.block.uid, entry))
                        affected_version_uids.extend(Version.set_block_invalid(entry.block.uid))
                        valid = False
                        continue
                    else:
                        raise entry
                else:
                    block, data, metadata = cast(Tuple[DereferencedBlock, bytes, Dict], entry)

                try:
                    storage.check_block_metadata(block=block, data_length=len(data), metadata=metadata)
                except (KeyError, ValueError) as exception:
                    logger.error('Metadata check failed, block {} (UID {}) is invalid: {}'.format(
                        block.idx, block.uid, exception))
                    Version.set_block_invalid(block.uid)
                    valid = False
                    continue

                data_checksum = self._block_hash.data_hexdigest(data)
                if data_checksum != block.checksum:
                    logger.error(
                        'Checksum mismatch during deep-scrub of block {} (UID {}) (is: {}... should-be: {}...).'.format(
                            block.idx, block.uid, data_checksum[:16],
                            cast(str, block.checksum)[:16]))  # We know that block.checksum is set
                    affected_version_uids.extend(Version.set_block_invalid(block.uid))
                    valid = False
                    continue

                if source:
                    source_data = io.read_sync(block)
                    if source_data != data:
                        logger.error('Source data has changed for block {} (UID {}) (is: {}... should-be: {}...). '
                                     'Won\'t set this block to invalid, because the source looks wrong.'.format(
                                         block.idx, block.uid,
                                         self._block_hash.data_hexdigest(source_data)[:16], data_checksum[:16]))
                        valid = False
                        # We are not setting the block invalid here because
                        # when the block is there AND the checksum is good,
                        # then the source is probably invalid.
                        source_mismatch = True

                if history:
                    history.add(version.storage_id, block.uid)

                self._scrub_report_progress(version_uid=version_uid,
                                            block=block,
                                            read_jobs=read_jobs,
                                            done_read_jobs=done_read_jobs,
                                            deep_scrub=True)
        except:
            Locking.unlock_version(version_uid)
            raise
        finally:
            if source:
                io.close()
            # Restore old read cache setting
            storage.use_read_cache(old_use_read_cache)
            notify(self._process_name)

        if read_jobs != done_read_jobs:
            raise InternalError(
                'Number of submitted and completed read jobs inconsistent (submitted: {}, completed {}).'.format(
                    read_jobs, done_read_jobs))

        if valid:
            if block_percentage == 100:
                try:
                    version.set(status=VersionStatus.valid)
                except:
                    Locking.unlock_version(version_uid)
                    raise
            logger.info('Deep-scrub of version {} successful.'.format(version.uid))
        else:
            if source_mismatch:
                logger.error('Version {} had source mismatches.'.format(version.uid))
            logger.error('Marked version {} as invalid because it has errors.'.format(version.uid))
            if version.uid in affected_version_uids:
                affected_version_uids.remove(version.uid)
            if affected_version_uids:
                logger.error('Marked the following versions as invalid, too, because of invalid blocks: {}.' \
                             .format(', '.join(sorted(affected_version_uids))))

        Locking.unlock_version(version_uid)

        if not valid:
            raise ScrubbingError('Deep-scrub of version {} failed.'.format(version_uid))

    def _batch_scrub(self, method: str, filter_expression: Optional[str], version_percentage: int,
                     block_percentage: int, group_label: Optional[str]) -> Tuple[List[Version], List[Version]]:
        history = BlockUidHistory()
        versions = set(Version.find_with_filter(filter_expression))
        errors = []

        if versions and group_label is not None:
            additional_versions: Set[Version] = set()
            for version in versions:
                label_match = list(filter(lambda label: label.name == group_label, version.labels))
                if not label_match:
                    continue
                assert len(label_match) == 1
                additional_versions |= set(Version.find(labels=[(group_label, label_match[0].value)]))
            versions |= additional_versions

        if version_percentage and versions:
            # Will always scrub at least one matching version
            versions = set(random.sample(versions, max(1, int(len(versions) * version_percentage / 100))))
        if not versions:
            logger.info('No matching versions found.')
            return [], []

        for version in versions:
            try:
                logger.info('{} {}% of version {} (volume {}).'.format(
                    'Scrubbing' if method == 'scrub' else 'Deep-scrubbing', block_percentage, version.uid,
                    version.volume))
                getattr(self, method)(version.uid, block_percentage=block_percentage, history=history)
            except ScrubbingError as exception:
                logger.error(exception)
                errors.append(version)
            except AlreadyLocked:
                logger.warning(f'Skipping version {version.uid}, it is locked. ')

        return sorted(versions), sorted(errors)

    def batch_scrub(self,
                    filter_expression: Optional[str],
                    version_percentage: int,
                    block_percentage: int,
                    group_label: Optional[str] = None) -> Tuple[List[Version], List[Version]]:
        return self._batch_scrub('scrub', filter_expression, version_percentage, block_percentage, group_label)

    def batch_deep_scrub(self,
                         filter_expression: Optional[str],
                         version_percentage: int,
                         block_percentage: int,
                         group_label: Optional[str] = None) -> Tuple[List[Version], List[Version]]:
        return self._batch_scrub('deep_scrub', filter_expression, version_percentage, block_percentage, group_label)

    def restore(self, version_uid: VersionUid, target: str, sparse: bool = False, force: bool = False) -> None:
        block: Union[DereferencedBlock, Block]

        Locking.lock_version(version_uid, reason='Restoring version')
        try:
            version = Version.get_by_uid(version_uid)  # raise if version not exists
            notify(self._process_name, 'Restoring version {} to {}: Getting blocks'.format(version_uid, target))

            self._storage = version.storage_id

            io = IOFactory.get(target, version.block_size)
            io.open_w(version.size, force=force, sparse=sparse)
        except:
            Locking.unlock_version(version_uid)
            raise

        try:
            t1 = time.time()
            storage = StorageFactory.get_by_name(version.storage.name)

            sparse_blocks_count = version.sparse_blocks_count
            read_blocks_count = version.blocks_count - sparse_blocks_count

            read_jobs = 0
            write_jobs = 0
            done_write_jobs = 0
            written = 0
            log_every_jobs = (sparse_blocks_count if not sparse else read_blocks_count) // 200 + 1  # about every half percent
            sparse_data_block = b'\0' * version.block_size

            def handle_sparse_write_completed(timeout: int = None):
                nonlocal done_write_jobs, written, sparse_blocks_count, log_every_jobs, version_uid, target
                try:
                    for written_block in io.write_get_completed(timeout=timeout):
                        if isinstance(written_block, Exception):
                            raise written_block
                        assert isinstance(written_block, DereferencedBlock)
                        done_write_jobs += 1
                        written += written_block.size

                        notify(
                            self._process_name, 'Restoring version {} to {}: Sparse writing ({:.1f}%)'.format(
                                version_uid, target, done_write_jobs / sparse_blocks_count * 100))
                        if done_write_jobs % log_every_jobs == 0 or done_write_jobs == sparse_blocks_count:
                            logger.info('Wrote sparse {}/{} blocks ({:.1f}%)'.format(
                                done_write_jobs, sparse_blocks_count, done_write_jobs / sparse_blocks_count * 100))
                except (TimeoutError, CancelledError):
                    pass

            for block in version.blocks:
                if block.uid:
                    storage.read_block_async(block)
                    read_jobs += 1
                    logger.debug('Queued read for block {} successfully ({} bytes).'.format(block.idx, block.size))
                elif not sparse:
                    io.write(block, sparse_data_block)
                    write_jobs += 1
                    logger.debug('Queued write for sparse block {} successfully ({} bytes).'.format(
                        block.idx, block.size))
                else:
                    logger.debug('Ignored sparse block {}.'.format(block.idx))

                if sparse:
                    # Version might be completely sparse
                    if read_blocks_count > 0:
                        notify(
                            self._process_name, 'Restoring version {} to {}: Queueing blocks to read ({:.1f}%)'.format(
                                version_uid, target, read_jobs / read_blocks_count * 100))
                else:
                    handle_sparse_write_completed(timeout=0)

            handle_sparse_write_completed()

            if write_jobs != done_write_jobs:
                raise InternalError(
                    'Number of submitted and completed write jobs inconsistent (submitted: {}, completed {}).'.format(
                        write_jobs, done_write_jobs))

            done_read_jobs = 0
            write_jobs = 0
            done_write_jobs = 0
            log_every_jobs = read_jobs // 200 + 1  # about every half percent

            def handle_write_completed(timeout: int = None):
                nonlocal done_write_jobs, written, read_jobs, log_every_jobs, version_uid, target
                try:
                    for written_block in io.write_get_completed(timeout=timeout):
                        if isinstance(written_block, Exception):
                            raise written_block
                        assert isinstance(written_block, DereferencedBlock)
                        done_write_jobs += 1
                        written += written_block.size

                        notify(
                            self._process_name,
                            'Restoring version {} to {} ({:.1f}%)'.format(version_uid, target,
                                                                          done_write_jobs / read_jobs * 100))
                        if done_write_jobs % log_every_jobs == 0 or done_write_jobs == read_jobs:
                            logger.info('Restored {}/{} blocks ({:.1f}%)'.format(done_write_jobs, read_jobs,
                                                                                 done_write_jobs / read_jobs * 100))
                except (TimeoutError, CancelledError):
                    pass

            for entry in storage.read_get_completed():
                done_read_jobs += 1
                if isinstance(entry, Exception):
                    logger.error('Storage backend read failed: {}'.format(entry))
                    # If it really is a data inconsistency mark blocks invalid
                    if isinstance(entry, (KeyError, ValueError)):
                        Version.set_block_invalid(block.uid)
                        continue
                    else:
                        raise entry
                else:
                    block, data, metadata = cast(Tuple[DereferencedBlock, bytes, Dict], entry)

                # Write what we have
                io.write(block, data)
                write_jobs += 1

                try:
                    storage.check_block_metadata(block=block, data_length=len(data), metadata=metadata)
                except (KeyError, ValueError) as exception:
                    logger.error('Metadata check failed, block is invalid: {}'.format(exception))
                    Version.set_block_invalid(block.uid)
                    continue

                data_checksum = self._block_hash.data_hexdigest(data)
                if data_checksum != block.checksum:
                    logger.error('Checksum mismatch during restore for block {} (UID {}) (is: {}... should-be: {}..., '
                                 'block.valid: {}). Block restored is invalid.'.format(
                                     block.idx, block.uid, data_checksum[:16],
                                     cast(str, block.checksum)[:16], block.valid))  # We know that block.checksum is set
                    Version.set_block_invalid(block.uid)
                else:
                    logger.debug('Restored block {} successfully ({} bytes).'.format(block.idx, block.size))

                handle_write_completed(timeout=0)

            handle_write_completed()
        finally:
            io.close()
            t2 = time.time()
            Locking.unlock_version(version_uid)
            notify(self._process_name)

        if read_jobs != done_read_jobs:
            raise InternalError(
                'Number of submitted and completed read jobs inconsistent (submitted: {}, completed {}).'.format(
                    read_jobs, done_read_jobs))

        if write_jobs != done_write_jobs:
            raise InternalError(
                'Number of submitted and completed write jobs inconsistent (submitted: {}, completed {}).'.format(
                    write_jobs, done_write_jobs))

        logger.info('Successfully restored version {} in {} with {}/s.'.format(
            version.uid, PrettyPrint.duration(max(int(t2 - t1), 1)), PrettyPrint.bytes(written / (t2 - t1))))

    @staticmethod
    def protect(version_uid: VersionUid, protected: bool) -> None:
        version = Version.get_by_uid(version_uid)
        version.set(protected=protected)

    @staticmethod
    def rm(version_uid: VersionUid,
           force: bool = True,
           disallow_rm_when_younger_than_days: int = 0,
           keep_metadata_backup: bool = False,
           override_lock: bool = False) -> None:
        with Locking.with_version_lock(version_uid, reason='Removing version', override_lock=override_lock):
            version = Version.get_by_uid(version_uid)

            if version.protected:
                raise PermissionError('Version {} is protected, will not delete it.'.format(version_uid))

            if not force:
                # check if disallow_rm_when_younger_than_days allows deletion
                age_days = (datetime.datetime.now() - version.date).days
                if disallow_rm_when_younger_than_days > age_days:
                    raise PermissionError('Version {} is too young. Will not delete.'.format(version_uid))
                if not version.status.is_removable():
                    raise PermissionError('Version {} cannot be removed without force, it has status {}.'.format(
                        version_uid, version.status.name))

            num_blocks = version.remove()

            if not keep_metadata_backup:
                try:
                    storage = StorageFactory.get_by_name(version.storage.name)
                    storage.rm_version(version_uid)
                    logger.info('Removed version {} metadata backup from storage.'.format(version_uid))
                except FileNotFoundError:
                    logger.warning(
                        'Unable to remove version {} metadata backup from storage, the object was not found.'.format(version_uid))
                    pass

            logger.info('Removed backup version {} with {} blocks.'.format(version_uid, num_blocks))

    @staticmethod
    def _blocks_from_hints(hints: Sequence[Tuple[int, int, bool]], block_size: int) -> Tuple[Set[int], Set[int]]:
        sparse_blocks = set()
        read_blocks = set()
        for offset, length, exists in hints:
            start_block = offset // block_size
            end_block = (offset + length - 1) // block_size
            if exists:
                for i in range(start_block, end_block + 1):
                    read_blocks.add(i)
            else:
                if offset % block_size > 0:
                    # Start block is only partially sparse, make sure it is read
                    read_blocks.add(start_block)

                if (offset + length) % block_size > 0:
                    # End block is only partially sparse, make sure it is read
                    read_blocks.add(end_block)

                for i in range(start_block, end_block + 1):
                    sparse_blocks.add(i)

        return sparse_blocks, read_blocks

    def backup(self,
               *,
               version_uid: VersionUid,
               volume: str,
               snapshot: str,
               source: str,
               hints: List[Tuple[int, int, bool]] = None,
               base_version_uid: VersionUid = None,
               storage_name: str = None,
               block_size: int = None) -> Version:
        """ Create a backup from source.
        If hints are given, they must be tuples of (offset, length, exists) where offset and length are integers and
        exists is a boolean. In this case only data within hints will be backed up.
        Otherwise, the backup reads source and looks if checksums match with the target.
        """
        if not InputValidation.is_volume_name(volume):
            raise UsageError('Version name {} is invalid.'.format(volume))
        if not InputValidation.is_snapshot_name(snapshot):
            raise UsageError('Snapshot name {} is invalid.'.format(snapshot))

        block: Union[DereferencedBlock, Block]

        stats: Dict[str, Any] = {
            'bytes_read': 0,
            'bytes_written': 0,
            'bytes_deduplicated': 0,
            'bytes_sparse': 0,
            'start_time': time.time(),
        }

        new_version_block_size = block_size if block_size else self._block_size

        io = IOFactory.get(source, block_size=new_version_block_size)
        io.open_r()

        source_size = io.size()

        version = self.create_version(version_uid=version_uid,
                                      volume=volume,
                                      snapshot=snapshot,
                                      size=source_size,
                                      block_size=new_version_block_size,
                                      base_version_uid=base_version_uid,
                                      storage_name=storage_name)
        Locking.update_version_lock(version.uid, reason='Backing up')

        if hints is not None:
            if len(hints) > 0:
                # Sanity check: check hints for validity, i.e. too high offsets, ...
                max_offset = max(h[0] + h[1] for h in hints)
                if max_offset > source_size:
                    raise InputDataError('Hints have higher offsets than source file.')

                sparse_blocks, read_blocks = self._blocks_from_hints(hints, version.block_size)
            else:
                # Two snapshots can be completely identical between one backup and next
                logger.warning('Hints are empty, assuming nothing has changed.')
                sparse_blocks = set()
                read_blocks = set()
        else:
            sparse_blocks = set()
            read_blocks = set(range(version.blocks_count))

        if base_version_uid and hints is not None:
            # SANITY CHECK:
            # Check some blocks outside of hints if they are the same in the
            # base_version backup and in the current backup. If they
            # aren't, either hints are wrong (e.g. from a wrong snapshot diff)
            # or source doesn't match. In any case, the resulting backup won't
            # be good.
            logger.info('Starting sanity check with 0.1% of the ignored blocks.')
            notify(self._process_name, 'Sanity checking hints of version {}'.format(version.uid))

            ignored_blocks = sorted(set(range(version.blocks_count)) - read_blocks - sparse_blocks)
            # 0.1% but at least ten. If there are less than ten blocks check them all.
            check_blocks_count = max(min(len(ignored_blocks), 10), len(ignored_blocks) // 1000)
            # 50% from the start
            check_blocks = set(ignored_blocks[:check_blocks_count // 2])
            # and 50% from random locations
            check_blocks = check_blocks.union(random.sample(ignored_blocks, check_blocks_count // 2))
            read_jobs = 0
            for block in [version.get_block_by_idx(idx) for idx in check_blocks]:
                if block.uid and block.valid:  # no uid = sparse block in backup. Can't check.
                    io.read(block)
                    read_jobs += 1

            for entry in io.read_get_completed():
                if isinstance(entry, Exception):
                    raise entry
                else:
                    source_block, source_data = cast(Tuple[DereferencedBlock, bytes], entry)

                # check metadata checksum with the newly read one
                source_data_checksum = self._block_hash.data_hexdigest(source_data)
                if source_block.checksum != source_data_checksum:
                    logger.error("Source and backup don't match in regions outside of the ones indicated by the hints.")
                    logger.error("Looks like the hints don't match or the source is different.")
                    logger.error("Found wrong source data at block {}: offset {}, length {}".format(
                        source_block.idx, source_block.idx * version.block_size, version.block_size))
                    # remove version
                    version.remove()
                    Locking.unlock_version(version.uid)
                    raise InputDataError('Source changed in regions outside of ones indicated by the hints.')
            logger.info('Finished sanity check. Checked {} blocks.'.format(read_jobs))

        try:
            storage = StorageFactory.get_by_name(version.storage.name)
            read_jobs = 0
            for block in version.blocks:
                if block.idx in read_blocks or not block.valid:
                    io.read(block)
                    read_jobs += 1
                elif block.idx in sparse_blocks:
                    # This "elif" is very important. Because if the block is in read_blocks AND sparse_blocks,
                    # it *must* be read.

                    # Only update the database when the block wasn't sparse to begin with
                    if block.uid:
                        version.set_block(idx=block.idx,
                                          block_uid=SparseBlockUid,
                                          checksum=None,
                                          size=block.size,
                                          valid=True)
                        logger.debug('Skipping block (had data, turned sparse) {}'.format(block.idx))
                    else:
                        assert block.checksum is None
                        logger.debug('Skipping block (sparse) {}'.format(block.idx))
                    stats['bytes_sparse'] += block.size

                else:
                    # Block is already in database, no need to update it
                    logger.debug('Keeping block {}'.format(block.idx))
                notify(
                    self._process_name, 'Backing up version {} from {}: Queueing blocks to read ({:.1f}%)'.format(
                        version.uid, source, (block.idx + 1) / version.blocks_count * 100))

            # Precompute checksum of a sparse block.
            sparse_block_checksum = self._block_hash.data_hexdigest(b'\0' * version.block_size)

            done_read_jobs = 0
            write_jobs = 0
            done_write_jobs = 0
            log_every_jobs = read_jobs // 200 + 1  # about every half percent

            def handle_write_completed(timeout: int = None):
                nonlocal done_write_jobs, stats, version
                try:
                    for written_block in storage.write_get_completed(timeout=timeout):
                        if isinstance(written_block, Exception):
                            raise written_block

                        written_block = cast(DereferencedBlock, written_block)

                        assert written_block.version_id == version.id
                        version.set_block(idx=written_block.idx,
                                          block_uid=written_block.uid,
                                          checksum=written_block.checksum,
                                          size=written_block.size,
                                          valid=True)
                        done_write_jobs += 1
                        stats['bytes_written'] += written_block.size
                except (TimeoutError, CancelledError):
                    pass

            for entry in io.read_get_completed():
                if isinstance(entry, Exception):
                    raise entry
                else:
                    block, data = cast(Tuple[DereferencedBlock, bytes], entry)

                stats['bytes_read'] += len(data)

                data_checksum = self._block_hash.data_hexdigest(data)
                if data_checksum == sparse_block_checksum and block.size == version.block_size:
                    # It's a sparse block.
                    stats['bytes_sparse'] += block.size
                    logger.debug('Skipping block (detected sparse) {}'.format(block.idx))
                    version.set_block(idx=block.idx,
                                      block_uid=SparseBlockUid,
                                      checksum=None,
                                      size=block.size,
                                      valid=True)
                else:
                    existing_block = version.get_block_by_checksum(data_checksum)
                    if existing_block and existing_block.size == block.size:
                        # It's a known block.
                        version.set_block(idx=block.idx,
                                          block_uid=existing_block.uid,
                                          checksum=existing_block.checksum,
                                          size=existing_block.size,
                                          valid=True)
                        stats['bytes_deduplicated'] += len(data)
                        logger.debug('Found existing block for id {} with UID {}'.format(block.idx, existing_block.uid))
                    else:
                        # It's a new block.
                        # Generate a unique block id by combining the version id and the block index.
                        block.uid = BlockUid(version.id, block.idx + 1)
                        block.checksum = data_checksum
                        storage.write_block_async(block, data)
                        write_jobs += 1
                        logger.debug('Queued block {} for write (checksum {}...)'.format(block.idx, data_checksum[:16]))

                done_read_jobs += 1

                handle_write_completed(timeout=0)

                notify(
                    self._process_name,
                    'Backing up version {} from {} ({:.1f}%)'.format(version.uid, source,
                                                                     done_read_jobs / read_jobs * 100))
                if done_read_jobs % log_every_jobs == 0 or done_read_jobs == read_jobs:
                    logger.info('Backed up {}/{} blocks ({:.1f}%)'.format(done_read_jobs, read_jobs,
                                                                          done_read_jobs / read_jobs * 100))

            handle_write_completed()
        except:
            Locking.unlock_version(version.uid)
            raise
        finally:
            # This will also cancel any outstanding read jobs
            io.close()
            version.commit()

        if read_jobs != done_read_jobs:
            raise InternalError(
                'Number of submitted and completed read jobs inconsistent (submitted: {}, completed {}).'.format(
                    read_jobs, done_read_jobs))

        if write_jobs != done_write_jobs:
            raise InternalError(
                'Number of submitted and completed write jobs inconsistent (submitted: {}, completed {}).'.format(
                    write_jobs, done_write_jobs))

        notify(self._process_name, 'Marking version {} as valid'.format(version.uid))
        version.set(status=VersionStatus.valid)

        notify(self._process_name, 'Backing up metadata of version {}'.format(version.uid))
        self.metadata_backup([version.uid], overwrite=True, locking=False)

        logger.debug('Stats: {}'.format(stats))
        version.set_stats(
            bytes_read=stats['bytes_read'],
            bytes_written=stats['bytes_written'],
            bytes_deduplicated=stats['bytes_deduplicated'],
            bytes_sparse=stats['bytes_sparse'],
            duration=int(time.time() - stats['start_time']),
        )

        Locking.unlock_version(version.uid)
        notify(self._process_name)
        logger.info('New version {} created, backup successful.'.format(version.uid))
        return version

    def cleanup(self, dt: int = 3600, override_lock: bool = False) -> None:
        with Locking.with_lock(lock_name='cleanup',
                               reason='Cleanup',
                               locked_msg='Another cleanup is already running.',
                               override_lock=override_lock):
            notify(self._process_name, 'Cleanup')
            for hit_list in DeletedBlock.get_unused_block_uids(dt):
                for storage_name, uids in hit_list.items():
                    storage = StorageFactory.get_by_name(storage_name)
                    logger.debug('Deleting UIDs from storage {}: {}'.format(storage_name,
                                                                            ', '.join(str(uid) for uid in uids)))

                    for uid in uids:
                        storage.rm_block_async(uid)

                    no_del_uids = []
                    for entry in storage.rm_get_completed():
                        if isinstance(entry, BlockNotFoundError):
                            no_del_uids.append(entry.uid)
                        elif isinstance(entry, Exception):
                            raise entry

                    if no_del_uids:
                        logger.info('Unable to delete these UIDs from storage {}: {}'.format(
                            storage_name, ', '.join(str(uid) for uid in no_del_uids)))
            notify(self._process_name)

    @staticmethod
    def add_label(version_uid: VersionUid, key: str, value: str) -> None:
        version = Version.get_by_uid(version_uid)
        version.add_label(key, value)

    @staticmethod
    def rm_label(version_uid: VersionUid, key: str) -> None:
        version = Version.get_by_uid(version_uid)
        version.rm_label(key)

    @staticmethod
    def close() -> None:
        StorageFactory.close()
        # Close database backend after storage so that any open locks are held until all storage jobs have
        # finished.
        Database.close()

    @staticmethod
    def metadata_export(version_uids: Sequence[VersionUid], f: TextIO) -> None:
        try:
            locked_version_uids = []
            for version_uid in version_uids:
                Locking.lock_version(version_uid, reason='Exporting version metadata')
                locked_version_uids.append(version_uid)

            Database.export(version_uids, f)
            logger.info('Exported metadata of version(s): {}.'.format(', '.join(version_uids)))
        finally:
            for version_uid in locked_version_uids:
                Locking.unlock_version(version_uid)

    @staticmethod
    def metadata_backup(version_uids: Sequence[VersionUid], overwrite: bool = False, locking: bool = True) -> None:
        versions = [Version.get_by_uid(version_uid) for version_uid in version_uids]
        try:
            locked_version_uids = []
            if locking:
                for version in versions:
                    Locking.lock_version(version.uid, reason='Backing up version metadata')
                    locked_version_uids.append(version.uid)

            for version in versions:
                with StringIO() as metadata_export:
                    Database.export([version.uid], metadata_export)
                    storage = StorageFactory.get_by_name(version.storage.name)
                    logger.debug(metadata_export.getvalue())
                    storage.write_version(version.uid, metadata_export.getvalue(), overwrite=overwrite)
                logger.info('Backed up metadata of version {}.'.format(version.uid))
        finally:
            for version_uid in locked_version_uids:
                Locking.unlock_version(version_uid)

    @staticmethod
    def export_any(*args, **kwargs) -> None:
        Database.export_any(*args, **kwargs)

    @staticmethod
    def metadata_import(f: TextIO) -> None:
        # TODO: Find a good way to lock here
        version_uids = Database.import_(f)
        logger.info('Imported metadata of version(s): {}.'.format(', '.join(version_uids)))

    def metadata_restore(self, version_uids: Sequence[VersionUid], storage_name: str = None) -> None:
        storage = StorageFactory.get_by_name(storage_name or self._default_storage_name)
        try:
            locked_version_uids = []
            for version_uid in version_uids:
                Locking.lock_version(version_uid, reason='Restoring version metadata')
                locked_version_uids.append(version_uid)

            for version_uid in version_uids:
                metadata_import_data = storage.read_version(version_uid)
                with StringIO(metadata_import_data) as metadata_import:
                    Database.import_(metadata_import)
                logger.info('Restored metadata of version {}.'.format(version_uid))
        finally:
            for version_uid in locked_version_uids:
                Locking.unlock_version(version_uid)

    def metadata_ls(self, storage_name: str = None) -> List[VersionUid]:
        storage = StorageFactory.get_by_name(storage_name or self._default_storage_name)
        return storage.list_versions()

    def enforce_retention_policy(self,
                                 filter_expression: str,
                                 rules_spec: str,
                                 dry_run: bool = False,
                                 keep_metadata_backup: bool = False,
                                 group_label: str = None) -> List[Version]:
        versions = Version.find_with_filter(filter_expression)

        versions_by_volume: Dict[str, List[Version]] = defaultdict(list)
        for version in versions:
            if version.protected:
                logger.info('Not considering version {}, it is protected.'.format(version.uid))
                continue

            if not version.status.is_removable():
                logger.info('Not considering version {}, it has a status of {}.'.format(
                    version.uid, version.status.name))
                continue

            versions_by_volume[version.volume].append(version)

        dismissed_versions: Set[Version] = set()
        for versions_slice in versions_by_volume.values():
            dismissed_versions |= set(RetentionFilter(rules_spec).filter(versions_slice))

        if dismissed_versions and group_label is not None:
            additional_versions: Set[Version] = set()
            for version in dismissed_versions:
                label_match = list(filter(lambda label: label.name == group_label, version.labels.values()))
                if not label_match:
                    continue
                assert len(label_match) == 1
                additional_versions |= set(Version.find(labels=[(group_label, label_match[0].value)]))
            dismissed_versions |= additional_versions

        if dismissed_versions:
            logger.info('Removing versions: {}.'.format(', '.join(
                map(lambda version: version.uid, sorted(dismissed_versions)))))
        else:
            logger.info('All versions are conforming to the retention policy.')

        if dry_run:
            logger.info('Dry run, will not remove anything.')
            return []

        # Iterate through copy of dismissed_versions
        for version in list(dismissed_versions):
            try:
                self.rm(version.uid, force=True, keep_metadata_backup=keep_metadata_backup)
            except KeyError:
                logger.warning(f'Version {version.uid} was removed in the meantime.')
                dismissed_versions.remove(version)
            except PermissionError as exception:
                logger.warning(str(exception))
                dismissed_versions.remove(version)
            except AlreadyLocked:
                logger.warning(f'Version {version.uid} could not be deleted, it is currently locked.')
                dismissed_versions.remove(version)

        return sorted(dismissed_versions)

    def storage_stats(self, storage_name: str = None) -> Tuple[int, int]:
        storage = StorageFactory.get_by_name(storage_name or self._default_storage_name)
        return storage.storage_stats()

    @staticmethod
    def list_storages() -> List[str]:
        return list(StorageFactory.get_modules().keys())

    @staticmethod
    def storage_usage(filter_expression: str = None) -> Dict[str, Dict[str, int]]:
        return Version.storage_usage(filter_expression)


class _BlockStore:

    _block_present: Set[BlockUid]

    def __init__(self, directory: str) -> None:
        self._directory = directory
        self._block_present = set()

    def _cache_filename(self, block_uid: BlockUid) -> str:
        assert block_uid.left is not None and block_uid.right is not None
        filename = '{:016x}-{:016x}'.format(block_uid.left, block_uid.right)
        digest = hashlib.md5(filename.encode('ascii')).hexdigest()
        return os.path.join(self._directory, '{}/{}/{}'.format(digest[0:2], digest[2:4], filename))

    def read(self, block_uid: BlockUid, offset: int = 0, length: int = None) -> bytes:
        filename = self._cache_filename(block_uid)
        with open(filename, 'rb', buffering=0) as f:
            f.seek(offset)
            if length is None:
                return f.read()
            else:
                return f.read(length)

    def write(self, block_uid: BlockUid, data) -> None:
        filename = self._cache_filename(block_uid)
        try:
            with open(filename, 'wb', buffering=0) as f:
                f.write(data)
        except FileNotFoundError:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'wb', buffering=0) as f:
                f.write(data)

        self._block_present.add(block_uid)

    def update(self, block_uid: BlockUid, offset: int, data: bytes) -> None:
        filename = self._cache_filename(block_uid)
        with open(filename, 'r+b', buffering=0) as f:
            f.seek(offset)
            f.write(data)

    def rm(self, block_uid: BlockUid) -> None:
        try:
            self._block_present.remove(block_uid)
        except KeyError:
            pass
        filename = self._cache_filename(block_uid)
        try:
            os.unlink(filename)
        except FileNotFoundError:
            pass

    def present(self, block_uid: BlockUid) -> bool:
        return block_uid in self._block_present


# The reason for this class being here is that it accesses private attributes of class Benji
# and I don't want to make them all generally publicly available.
# Maybe they could inherit from the same base class in the future, but currently their
# functionality seems very different. So we just define that BenjiStore objects may access
# private attributes of Benji objects.
class BenjiStore(ReprMixIn):

    _benji_obj: Benji
    _cow: Dict[VersionUid, Dict[int, DereferencedBlock]]

    def __init__(self, benji_obj: Benji) -> None:
        self._benji_obj = benji_obj
        self._cow = {}  # contains version_uid: dict() of block id -> block

        block_cache_directory = self._benji_obj.config.get('nbd.blockCache.directory', types=str)
        block_cache_maximum_size = self._benji_obj.config.get('nbd.blockCache.maximumSize', types=int)
        cow_store_directory = self._benji_obj.config.get('nbd.cowStore.directory', types=str)

        os.makedirs(block_cache_directory, exist_ok=True)
        self._block_cache = Cache(block_cache_directory,
                                  size_limit=block_cache_maximum_size,
                                  eviction_policy='least-frequently-used',
                                  disk_min_file_size=0)

        os.makedirs(cow_store_directory, exist_ok=True)
        self._cow_store = _BlockStore(cow_store_directory)

    @staticmethod
    def open(version) -> None:
        Locking.lock_version(version.uid, reason='NBD')

    @staticmethod
    def close(version) -> None:
        Locking.unlock_version(version.uid)

    @staticmethod
    def find_versions(version_uid: VersionUid = None) -> List[Version]:
        return Version.find(version_uid=version_uid)

    @staticmethod
    def _block_list(version: Version, offset: int, length: int) -> List[Tuple[Optional[Block], int, int]]:
        block_idx = offset // version.block_size
        block_offset = offset % version.block_size

        chunks: List[Tuple[Optional[Block], int, int]] = []
        while True:
            block = version.get_block_by_idx(block_idx)
            if block is None:
                # We round up the size reported by the NBD server to a multiple of 4096 which is the maximum
                # block size supported by NBD. So we might need to fake up to 4095 bytes (of zeros) here.
                if length > 4095:
                    # Don't throw one of our own exceptions here as we need an exception with an errno value
                    # to communicate it back in the NBD response.
                    raise OSError(errno.EIO)
                length_in_block = min(block.size - block_offset, length)
                chunks.append((None, 0, length_in_block))
            else:
                length_in_block = min(block.size - block_offset, length)
                chunks.append((block, block_offset, length_in_block))

            block_idx += 1
            block_offset = 0
            length -= length_in_block

            assert length >= 0
            if length == 0:
                break

        return chunks

    def read(self, version: Version, cow_version: Optional[Version], offset: int, length: int) -> bytes:
        if cow_version:
            cow: Optional[Dict[int, DereferencedBlock]] = self._cow[cow_version.uid]
        else:
            cow = None
        read_list = self._block_list(version, offset, length)
        data_chunks: List[bytes] = []
        block: Optional[Union[Block, DereferencedBlock]]
        for block, offset_in_block, length_in_block in read_list:
            # Access lies beyond end of version
            if block is None:
                logger.warning('Tried to read data beyond device (version {}, size {}, offset {}).'.format(
                    version.uid, version.size, offset_in_block))
                data_chunks.append(b'\0' * length_in_block)
                continue

            if cow is not None and block.idx in cow:
                # Read block from COW
                assert cow_version is not None

                block = cow[block.idx]
                logger.debug('Reading block from COW {}/{} {}:{}.'.format(cow_version.uid, block.idx, offset_in_block,
                                                                          length_in_block))

                assert self._cow_store.present(block.uid)
                data_chunks.append(self._cow_store.read(block.uid, offset_in_block, length_in_block))
            else:
                # Read block from original version (if not sparse)
                logger.debug('Reading {}block {}/{} {}:{}.'.format('sparse ' if not block.uid else '', version.uid,
                                                                   block.idx, offset_in_block, length_in_block))

                # Block is sparse
                if not block.uid:
                    data_chunks.append(b'\0' * length_in_block)
                    continue

                block_f = self._block_cache.get(str(block.uid), read=True)
                if block_f is not None:
                    # Cache hit
                    try:
                        block_f.seek(offset_in_block)
                        data_chunks.append(block_f.read(length_in_block))
                    finally:
                        block_f.close()
                else:
                    storage = StorageFactory.get_by_name(version.storage.name)
                    data = storage.read_block(block)
                    self._block_cache[str(block.uid)] = data
                    data_chunks.append(data[offset_in_block:offset_in_block + length_in_block])

        return b''.join(data_chunks)

    def create_cow_version(self, base_version: Version) -> Version:
        cow_version = self._benji_obj.create_version(
            version_uid=VersionUid('{}-{}'.format(f'nbd-cow-{base_version.uid}'[:248], random_string(6))),
            volume=base_version.volume,
            snapshot=datetime.datetime.utcnow().isoformat(timespec='microseconds') + 'Z',
            base_version_uid=base_version.uid,
            base_version_locking=False)
        Locking.update_version_lock(cow_version.uid, reason='NBD COW')
        self._cow[cow_version.uid] = {}  # contains version_uid: dict() of block id -> uid
        return cow_version

    def write(self, cow_version: Version, offset: int, data: bytes) -> None:
        """ Copy on write backup writer """
        cow = self._cow[cow_version.uid]
        write_list = self._block_list(cow_version, offset, len(data))
        position_in_data = 0
        for block, offset_in_block, length_in_block in write_list:
            if block is None:
                logger.warning('Tried to write data beyond device, it will be lost (version {}, size {}, offset {}).'.format(
                    cow_version.uid, cow_version.size, offset))
                break
            if block.idx in cow:
                # The block is already copied, so update in the cache
                update_block = cow[block.idx]
                self._cow_store.update(update_block.uid, offset_in_block,
                                       data[position_in_data:position_in_data + length_in_block])
                logger.debug('Updated block {}/{} {}:{}.'.format(cow_version.uid, block.idx, offset_in_block,
                                                                 length_in_block))
            else:
                # Read the block from the original
                if block.uid:
                    storage = StorageFactory.get_by_name(cow_version.storage.name)
                    write_data = BytesIO(storage.read_block(block))
                # Was a sparse block
                else:
                    write_data = BytesIO(b'\0' * cow_version.block_size)

                # Update the block
                write_data.seek(offset_in_block)
                write_data.write(data[position_in_data:position_in_data + length_in_block])
                write_data.seek(0)

                # Save a copy of the changed data and record the changed block UID
                new_block = block.deref()
                new_block.uid = BlockUid(cow_version.id, block.idx + 1)
                new_block.checksum = None
                self._cow_store.write(new_block.uid, write_data.read())
                cow[block.idx] = new_block
                logger.debug('COW: Wrote block {}/{} {}:{} into {}.'.format(cow_version.uid, block.idx, offset_in_block,
                                                                            length_in_block, new_block.uid))
            position_in_data += length_in_block

    def flush(self, cow_version: Version) -> None:
        pass

    def fixate(self, cow_version: Version) -> None:
        # save blocks into version
        logger.info('Fixating version {} with {} blocks, please wait.'.format(cow_version.uid,
                                                                              len(self._cow[cow_version.uid])))

        sparse_block_checksum = self._benji_obj._block_hash.data_hexdigest(b'\0' * cow_version.block_size)
        storage = StorageFactory.get_by_name(cow_version.storage.name)
        for block in self._cow[cow_version.uid].values():
            logger.debug('Fixating block {}/{} with UID {}'.format(cow_version.uid, block.idx, block.uid))
            data = self._cow_store.read(block.uid)

            block.checksum = self._benji_obj._block_hash.data_hexdigest(data)
            if block.checksum == sparse_block_checksum:
                logger.debug('Detected sparse block {}/{}.'.format(cow_version.uid, block.idx))
                # The remove assumes that each block UID appears only once in the list and is not shared in any way.
                self._cow_store.rm(block.uid)
                block.checksum = None
                block.uid = BlockUid(None, None)
            else:
                storage.write_block(block, data)
                # The remove assumes that each block UID appears only once in the list and is not shared in any way.
                self._cow_store.rm(block.uid)

            try:
                cow_version.set_block(idx=block.idx,
                                      block_uid=block.uid,
                                      checksum=block.checksum,
                                      size=len(data),
                                      valid=True)
            except:
                # Prevent orphaned blocks
                if block.uid:
                    storage.rm_block(block.uid)

        cow_version.commit()
        cow_version.set(status=VersionStatus.valid, protected=True)
        self._benji_obj.metadata_backup([cow_version.uid], overwrite=True, locking=False)
        Locking.unlock_version(cow_version.uid)
        del self._cow[cow_version.uid]
        logger.info('Finished.')
