# -*- encoding: utf-8 -*-

import datetime
import errno
import hashlib
import math
import os
import random
import time
from concurrent.futures import CancelledError, TimeoutError
from io import StringIO, BytesIO
from typing import List, Tuple, TextIO, Optional, Set, Dict, cast, Union, \
    Sequence, Any

from benji.blockuidhistory import BlockUidHistory
from benji.config import Config
from benji.database import DatabaseBackend, VersionUid, Version, Block, \
    BlockUid, DereferencedBlock, VersionStatus
from benji.exception import InputDataError, InternalError, AlreadyLocked, UsageError, ScrubbingError
from benji.factory import IOFactory, StorageFactory
from benji.logging import logger
from benji.repr import ReprMixIn
from benji.retentionfilter import RetentionFilter
from benji.storage.base import InvalidBlockException, BlockNotFoundError
from benji.utils import notify, BlockHash


class Benji(ReprMixIn):

    def __init__(self,
                 config: Config,
                 block_size: int = None,
                 init_database: bool = False,
                 migrate_database: bool = False,
                 in_memory_database: bool = False,
                 _destroy_database: bool = False) -> None:

        self.config = config

        if block_size is None:
            self._block_size = config.get('blockSize', types=int)
        else:
            self._block_size = block_size

        self._block_hash = BlockHash(config.get('hashFunction', types=str))
        self._process_name = config.get('processName', types=str)

        IOFactory.initialize(config)

        StorageFactory.initialize(self.config)
        default_storage = self.config.get('defaultStorage', types=str)
        self._default_storage_id = StorageFactory.name_to_storage_id(default_storage)

        database_backend = DatabaseBackend(config, in_memory=in_memory_database)
        if init_database or in_memory_database:
            database_backend.init(_destroy=_destroy_database)

        if migrate_database:
            database_backend.migrate()

        self._database_backend = database_backend.open()
        self._locking = self._database_backend.locking()

        notify(self._process_name)

    def _prepare_version(self,
                         version_name: str,
                         version_snapshot_name: str,
                         storage_name: str = None,
                         size: int = None,
                         base_version_uid: VersionUid = None,
                         base_version_locking: bool = True) -> Version:
        """ Prepares the metadata for a new version.
        If base_version_uid is given, this is taken as the base, otherwise
        a pure sparse version is created.
        """
        storage_id = StorageFactory.name_to_storage_id(storage_name) if storage_name else None
        old_blocks: Optional[List[Block]] = None
        if base_version_uid:
            if not base_version_locking and not self._locking.is_version_locked(base_version_uid):
                raise InternalError('Base version is not locked.')
            old_version = self._database_backend.get_version(base_version_uid)  # raise if not exists
            if not old_version.status.is_valid():
                raise UsageError('You can only base a new version on a valid old version.')
            if old_version.block_size != self._block_size:
                raise UsageError('You cannot base a new version on an old version with a different block size.')
            if storage_id is not None and old_version.storage_id != storage_id:
                raise UsageError('Base version and new version have to be in the same storage.')
            new_storage_id = old_version.storage_id
            old_blocks = self._database_backend.get_blocks_by_version(base_version_uid)
            if size is not None:
                new_size = size
            else:
                new_size = old_version.size
        else:
            new_storage_id = storage_id if storage_id is not None else self._default_storage_id
            if size is None:
                raise InternalError('Size needs to be specified if there is no base version.')
            new_size = size

        num_blocks = int(math.ceil(new_size / self._block_size))

        try:
            if base_version_locking and old_blocks:
                self._locking.lock_version(base_version_uid, reason='Base version cloning')

            # We always start with invalid versions, then mark them valid after the backup succeeds.
            version = self._database_backend.create_version(
                version_name=version_name,
                snapshot_name=version_snapshot_name,
                size=new_size,
                block_size=self._block_size,
                storage_id=new_storage_id,
                status=VersionStatus.incomplete)
            self._locking.lock_version(version.uid, reason='Preparing version')

            uid: Optional[BlockUid]
            checksum: Optional[str]
            block_size: int
            valid: bool
            for id in range(num_blocks):
                if old_blocks:
                    try:
                        old_block = old_blocks[id]
                    except IndexError:
                        uid = None
                        checksum = None
                        block_size = self._block_size
                        valid = True
                    else:
                        assert old_block.id == id
                        uid = old_block.uid
                        checksum = old_block.checksum
                        block_size = old_block.size
                        valid = old_block.valid
                else:
                    uid = None
                    checksum = None
                    block_size = self._block_size
                    valid = True

                # the last block can differ in size, so let's check
                _offset = id * self._block_size
                new_block_size = min(self._block_size, new_size - _offset)
                if new_block_size != block_size:
                    # last block changed, so set back all info
                    block_size = new_block_size
                    uid = None
                    checksum = None
                    valid = False

                self._database_backend.set_block(
                    id=id,
                    version_uid=version.uid,
                    block_uid=uid,
                    checksum=checksum,
                    size=block_size,
                    valid=valid,
                    upsert=False)
                notify(self._process_name, 'Preparing version {} ({:.1f}%)'.format(version.uid.v_string,
                                                                                   (id + 1) / num_blocks * 100))

            self._database_backend.commit()
        except:
            if self._locking.is_version_locked(version.uid):
                self._locking.unlock_version(version.uid)
            raise
        finally:
            if base_version_locking and old_blocks and self._locking.is_version_locked(base_version_uid):
                self._locking.unlock_version(base_version_uid)
            notify(self._process_name)

        return version

    def ls(self,
           version_uid: VersionUid = None,
           version_name: str = None,
           version_snapshot_name: str = None,
           version_labels: List[Tuple[str, str]] = None) -> List[Version]:
        return self._database_backend.get_versions(
            version_uid=version_uid,
            version_name=version_name,
            version_snapshot_name=version_snapshot_name,
            version_labels=version_labels)

    def ls_with_filter(self, filter_expression: str = None) -> List[Version]:
        return self._database_backend.get_versions_with_filter(filter_expression)

    def stats(self, filter_expression: str = None, limit: int = None):
        return self._database_backend.get_stats_with_filter(filter_expression, limit)

    def _scrub_prepare(self,
                       *,
                       version: Version,
                       blocks: List[Block],
                       history: BlockUidHistory = None,
                       block_percentage: int,
                       deep_scrub: bool) -> int:
        storage = StorageFactory.get_by_storage_id(version.storage_id)
        read_jobs = 0
        for i, block in enumerate(blocks):
            notify(
                self._process_name, 'Preparing {} of version {} ({:.1f}%)'.format(
                    'deep-scrub' if deep_scrub else 'scrub', version.uid.v_string, (i + 1) / len(blocks) * 100))
            if not block.uid:
                logger.debug('{} of block {} (UID {}) skipped (sparse).'.format('Deep-scrub' if deep_scrub else 'Scrub',
                                                                                block.id, block.uid))
                continue
            if history and history.seen(version.storage_id, block.uid):
                logger.debug('{} of block {} (UID {}) skipped (already seen).'.format(
                    'Deep-scrub' if deep_scrub else 'Scrub', block.id, block.uid))
                continue
            # i != 0 makes sure that we always scrub at least one block (the first in this case)
            if i != 0 and block_percentage < 100 and random.randint(1, 100) > block_percentage:
                logger.debug('{} of block {} (UID {}) skipped (percentile is {}).'.format(
                    'Deep-scrub' if deep_scrub else 'Scrub', block.id, block.uid, block_percentage))
            else:
                storage.read_block_async(block, metadata_only=(not deep_scrub))
                read_jobs += 1
        return read_jobs

    def _scrub_report_progress(self, *, version_uid: VersionUid, block: DereferencedBlock, read_jobs: int,
                               done_read_jobs: int, deep_scrub: bool) -> None:
        logger.debug('{} of block {} (UID {}) ok.'.format('Deep-scrub' if deep_scrub else 'Scrub', block.id, block.uid))

        notify(
            self._process_name, '{} version {} ({:.1f}%)'.format('Deep-scrubbing' if deep_scrub else 'Scrubbing',
                                                                 version_uid.v_string, done_read_jobs / read_jobs * 100))
        log_every_jobs = read_jobs // 200 + 1  # about every half percent
        if done_read_jobs % log_every_jobs == 0 or done_read_jobs == read_jobs:
            logger.info('{} {}/{} blocks ({:.1f}%)'.format('Deep-scrubbed' if deep_scrub else 'Scrubbed',
                                                           done_read_jobs, read_jobs, done_read_jobs / read_jobs * 100))

    def scrub(self, version_uid: VersionUid, block_percentage: int = 100, history: BlockUidHistory = None) -> None:
        self._locking.lock_version(version_uid, reason='Scrubbing version')
        try:
            version = self._database_backend.get_version(version_uid)
            if not version.status.is_scrubbable():
                raise ScrubbingError('Version {} cannot be scrubbed, it has a status of {}.'.format(
                    version_uid.v_string, version.status.name))
            blocks = self._database_backend.get_blocks_by_version(version_uid)
        except:
            self._locking.unlock_version(version_uid)
            raise

        valid = True
        affected_version_uids = []
        try:
            storage = StorageFactory.get_by_storage_id(version.storage_id)
            read_jobs = self._scrub_prepare(
                version=version, blocks=blocks, history=history, block_percentage=block_percentage, deep_scrub=False)

            done_read_jobs = 0
            for entry in storage.read_get_completed():
                done_read_jobs += 1
                if isinstance(entry, Exception):
                    # If it really is a data inconsistency mark blocks invalid
                    if isinstance(entry, InvalidBlockException):
                        logger.error('Block {} (UID {}) is invalid: {}'.format(entry.block.id, entry.block.uid, entry))
                        affected_version_uids.extend(self._database_backend.set_block_invalid(entry.block.uid))
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
                        block.id, block.uid, exception))
                    affected_version_uids.extend(self._database_backend.set_block_invalid(block.uid))
                    valid = False
                    continue
                except:
                    raise

                if history:
                    history.add(version.storage_id, block.uid)

                self._scrub_report_progress(
                    version_uid=version_uid,
                    block=block,
                    read_jobs=read_jobs,
                    done_read_jobs=done_read_jobs,
                    deep_scrub=False)
        except:
            raise
        finally:
            self._locking.unlock_version(version_uid)
            notify(self._process_name)

        if read_jobs != done_read_jobs:
            raise InternalError(
                'Number of submitted and completed read jobs inconsistent (submitted: {}, completed {}).'.format(
                    read_jobs, done_read_jobs))

        # A scrub (in contrast to a deep-scrub) can only ever mark a version as invalid. To mark it as valid
        # there is not enough information.
        if valid:
            logger.info('Scrub of version {} successful.'.format(version.uid.v_string))
        else:
            logger.error('Marked version {} as invalid because it has errors.'.format(version_uid.v_string))
            affected_version_uids.remove(version_uid)
            if affected_version_uids:
                logger.error('Marked the following versions as invalid, too, because of invalid blocks: {}.'\
                             .format(', '.join([affected_version.v_string for affected_version in sorted(affected_version_uids)])))
            raise ScrubbingError('Scrub of version {} failed.'.format(version_uid.v_string))

    def deep_scrub(self,
                   version_uid: VersionUid,
                   source: str = None,
                   block_percentage: int = 100,
                   history: BlockUidHistory = None) -> None:
        self._locking.lock_version(version_uid, reason='Deep-scrubbing')
        try:
            version = self._database_backend.get_version(version_uid)
            if not version.status.is_deep_scrubbable():
                raise ScrubbingError('Version {} cannot be deep-scrubbed, it has a status of {}.'.format(
                    version_uid.v_string, version.status.name))
            if not version.status.is_valid():
                logger.warn('Version {} has a status of {}.'.format(version_uid.v_string, version.status.name))
            blocks = self._database_backend.get_blocks_by_version(version_uid)

            if source:
                io = IOFactory.get(source, version.block_size)
                io.open_r()
        except:
            self._locking.unlock_version(version_uid)
            raise

        valid = True
        source_mismatch = False
        affected_version_uids = []
        try:
            storage = StorageFactory.get_by_storage_id(version.storage_id)
            old_use_read_cache = storage.use_read_cache(False)
            read_jobs = self._scrub_prepare(
                version=version, blocks=blocks, history=history, block_percentage=block_percentage, deep_scrub=True)

            done_read_jobs = 0
            for entry in storage.read_get_completed():
                done_read_jobs += 1
                if isinstance(entry, Exception):
                    # If it really is a data inconsistency mark blocks invalid
                    if isinstance(entry, InvalidBlockException):
                        logger.error('Block {} (UID {}) is invalid: {}'.format(entry.block.id, entry.block.uid, entry))
                        affected_version_uids.extend(self._database_backend.set_block_invalid(entry.block.uid))
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
                        block.id, block.uid, exception))
                    self._database_backend.set_block_invalid(block.uid)
                    valid = False
                    continue
                except:
                    raise

                data_checksum = self._block_hash.data_hexdigest(data)
                if data_checksum != block.checksum:
                    logger.error(
                        'Checksum mismatch during deep-scrub of block {} (UID {}) (is: {}... should-be: {}...).'.format(
                            block.id, block.uid, data_checksum[:16],
                            cast(str, block.checksum)[:16]))  # We know that block.checksum is set
                    affected_version_uids.extend(self._database_backend.set_block_invalid(block.uid))
                    valid = False
                    continue

                if source:
                    source_data = io.read_sync(block)
                    if source_data != data:
                        logger.error('Source data has changed for block {} (UID {}) (is: {}... should-be: {}...). '
                                     'Won\'t set this block to invalid, because the source looks wrong.'.format(
                                         block.id, block.uid,
                                         self._block_hash.data_hexdigest(source_data)[:16], data_checksum[:16]))
                        valid = False
                        # We are not setting the block invalid here because
                        # when the block is there AND the checksum is good,
                        # then the source is probably invalid.
                        source_mismatch = True

                if history:
                    history.add(version.storage_id, block.uid)

                self._scrub_report_progress(
                    version_uid=version_uid,
                    block=block,
                    read_jobs=read_jobs,
                    done_read_jobs=done_read_jobs,
                    deep_scrub=True)
        except:
            self._locking.unlock_version(version_uid)
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
                    self._database_backend.set_version(version_uid, status=VersionStatus.valid)
                except:
                    self._locking.unlock_version(version_uid)
                    raise
            logger.info('Deep-scrub of version {} successful.'.format(version.uid.v_string))
        else:
            if source_mismatch:
                logger.error('Version {} had source mismatches.'.format(version_uid.v_string))
            logger.error('Marked version {} as invalid because it has errors.'.format(version_uid.v_string))
            if version_uid in affected_version_uids:
                affected_version_uids.remove(version_uid)
            if affected_version_uids:
                logger.error('Marked the following versions as invalid, too, because of invalid blocks: {}.' \
                             .format(', '.join([affected_version.v_string for affected_version in sorted(affected_version_uids)])))

        self._locking.unlock_version(version_uid)

        if not valid:
            raise ScrubbingError('Deep-scrub of version {} failed.'.format(version_uid.v_string))

    def _batch_scrub(self, method: str, filter_expression: Optional[str], version_percentage: int,
                     block_percentage: int, group_label: Optional[str]) -> Tuple[List[Version], List[Version]]:
        history = BlockUidHistory()
        versions = set(self._database_backend.get_versions_with_filter(filter_expression))
        errors = []

        if versions and group_label is not None:
            additional_versions: Set[Version] = set()
            for version in versions:
                label_match = list(filter(lambda label: label.name == group_label, version.labels))
                if not label_match:
                    continue
                assert len(label_match) == 1
                additional_versions |= set(
                    self._database_backend.get_versions(version_labels=[(group_label, label_match[0].value)]))
            versions |= additional_versions

        if version_percentage and versions:
            # Will always scrub at least one matching version
            versions = set(random.sample(versions, max(1, int(len(versions) * version_percentage / 100))))
        if not versions:
            logger.info('No matching versions found.')
            return [], []

        for version in versions:
            try:
                logger.info('Scrubbing version {} with name {}.'.format(version.uid.v_string, version.name))
                getattr(self, method)(version.uid, block_percentage=block_percentage, history=history)
            except ScrubbingError as exception:
                logger.error(exception)
                errors.append(version)
            except:
                raise

        return sorted(versions), sorted(errors)

    def batch_scrub(self, filter_expression: Optional[str], version_percentage: int, block_percentage: int,
                    group_label: Optional[str]) -> Tuple[List[Version], List[Version]]:
        return self._batch_scrub('scrub', filter_expression, version_percentage, block_percentage, group_label)

    def batch_deep_scrub(self, filter_expression: Optional[str], version_percentage: int, block_percentage: int,
                         group_label: Optional[str]) -> Tuple[List[Version], List[Version]]:
        return self._batch_scrub('deep_scrub', filter_expression, version_percentage, block_percentage, group_label)

    def restore(self, version_uid: VersionUid, target: str, sparse: bool = False, force: bool = False) -> None:
        block: Union[DereferencedBlock, Block]

        self._locking.lock_version(version_uid, reason='Restoring version')
        try:
            version = self._database_backend.get_version(version_uid)  # raise if version not exists
            notify(self._process_name, 'Restoring version {} to {}: Getting blocks'.format(
                version_uid.v_string, target))
            blocks = self._database_backend.get_blocks_by_version(version_uid)

            self._storage = version.storage_id

            io = IOFactory.get(target, version.block_size)
            io.open_w(version.size, force=force, sparse=sparse)
        except:
            self._locking.unlock_version(version_uid)
            raise

        try:
            storage = StorageFactory.get_by_storage_id(version.storage_id)

            sparse_blocks = 0
            for block in blocks:
                if not block.uid:
                    sparse_blocks += 1

            read_jobs = 0
            write_jobs = 0
            done_write_jobs = 0
            log_every_jobs = read_jobs // 200 + 1  # about every half percent
            sparse_data_block = b'\0' * block.size
            for i, block in enumerate(blocks):
                if block.uid:
                    storage.read_block_async(block)
                    read_jobs += 1
                elif not sparse:
                    io.write(block, sparse_data_block)
                    write_jobs += 1
                    logger.debug('Queued write for sparse block {} successfully ({} bytes).'.format(
                        block.id, block.size))
                else:
                    logger.debug('Ignored sparse block {}.'.format(block.id))
                if sparse:
                    notify(
                        self._process_name, 'Restoring version {} to {}: Queueing blocks to read ({:.1f}%)'.format(
                            version_uid.v_string, target, (i + 1) / len(blocks) * 100))
                else:
                    try:
                        for written_block in io.write_get_completed(timeout=0):
                            if isinstance(written_block, Exception):
                                raise written_block
                            done_write_jobs += 1
                    except (TimeoutError, CancelledError):
                        pass

                    notify(
                        self._process_name, 'Restoring version {} to {}: Sparse writing ({:.1f}%)'.format(
                            version_uid.v_string, target, done_write_jobs / sparse_blocks * 100))
                    if i % log_every_jobs == 0 or done_write_jobs == write_jobs:
                        logger.info('Wrote sparse {}/{} blocks ({:.1f}%)'.format(done_write_jobs, sparse_blocks,
                                                                                 done_write_jobs / sparse_blocks * 100))

            try:
                for written_block in io.write_get_completed():
                    if isinstance(written_block, Exception):
                        raise written_block
                    done_write_jobs += 1

                    notify(
                        self._process_name, 'Restoring version {} to {}: Sparse writing ({:.1f}%)'.format(
                            version_uid.v_string, target, done_write_jobs / sparse_blocks * 100))
                    if i % log_every_jobs == 0 or done_write_jobs == write_jobs:
                        logger.info('Wrote sparse {}/{} blocks ({:.1f}%)'.format(done_write_jobs, sparse_blocks,
                                                                                 done_write_jobs / sparse_blocks * 100))
            except CancelledError:
                pass

            if write_jobs != done_write_jobs:
                raise InternalError(
                    'Number of submitted and completed write jobs inconsistent (submitted: {}, completed {}).'.format(
                        write_jobs, done_write_jobs))

            done_read_jobs = 0
            write_jobs = 0
            done_write_jobs = 0
            for entry in storage.read_get_completed():
                done_read_jobs += 1
                if isinstance(entry, Exception):
                    logger.error('Storage backend read failed: {}'.format(entry))
                    # If it really is a data inconsistency mark blocks invalid
                    if isinstance(entry, (KeyError, ValueError)):
                        self._database_backend.set_block_invalid(block.uid)
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
                    self._database_backend.set_block_invalid(block.uid)
                    continue
                except:
                    raise

                data_checksum = self._block_hash.data_hexdigest(data)
                if data_checksum != block.checksum:
                    logger.error('Checksum mismatch during restore for block {} (UID {}) (is: {}... should-be: {}..., '
                                 'block.valid: {}). Block restored is invalid.'.format(
                                     block.id, block.uid, data_checksum[:16],
                                     cast(str, block.checksum)[:16], block.valid))  # We know that block.checksum is set
                    self._database_backend.set_block_invalid(block.uid)
                else:
                    logger.debug('Restored block {} successfully ({} bytes).'.format(block.id, block.size))

                try:
                    for written_block in io.write_get_completed(timeout=0):
                        if isinstance(written_block, Exception):
                            raise written_block
                        done_write_jobs += 1
                except (TimeoutError, CancelledError):
                    pass

                notify(
                    self._process_name, 'Restoring version {} to {} ({:.1f}%)'.format(
                        version_uid.v_string, target, done_read_jobs / read_jobs * 100))
                if i % log_every_jobs == 0 or done_read_jobs == read_jobs:
                    logger.info('Restored {}/{} blocks ({:.1f}%)'.format(done_write_jobs, write_jobs,
                                                                         done_write_jobs / write_jobs * 100))

            try:
                for written_block in io.write_get_completed():
                    if isinstance(written_block, Exception):
                        raise written_block
                    done_write_jobs += 1

                    notify(
                        self._process_name, 'Restoring version {} to {} ({:.1f}%)'.format(
                            version_uid.v_string, target, done_read_jobs / read_jobs * 100))
                    if i % log_every_jobs == 0 or done_read_jobs == read_jobs:
                        logger.info('Restored {}/{} blocks ({:.1f}%)'.format(done_write_jobs, write_jobs,
                                                                             done_write_jobs / write_jobs * 100))
            except CancelledError:
                pass

        except:
            raise
        finally:
            io.close()
            self._locking.unlock_version(version_uid)
            notify(self._process_name)

        if read_jobs != done_read_jobs:
            raise InternalError(
                'Number of submitted and completed read jobs inconsistent (submitted: {}, completed {}).'.format(
                    read_jobs, done_read_jobs))

        if write_jobs != done_write_jobs:
            raise InternalError(
                'Number of submitted and completed write jobs inconsistent (submitted: {}, completed {}).'.format(
                    write_jobs, done_write_jobs))

        logger.info('Restore of version {} successful.'.format(version.uid.v_string))

    def protect(self, version_uid: VersionUid) -> None:
        self._database_backend.set_version(version_uid, protected=True)

    def unprotect(self, version_uid: VersionUid) -> None:
        self._database_backend.set_version(version_uid, protected=False)

    def rm(self,
           version_uid: VersionUid,
           force: bool = True,
           disallow_rm_when_younger_than_days: int = 0,
           keep_metadata_backup: bool = False,
           override_lock: bool = False) -> None:
        with self._locking.with_version_lock(version_uid, reason='Removing version', override_lock=override_lock):
            version = self._database_backend.get_version(version_uid)

            if version.protected:
                raise RuntimeError('Version {} is protected, will not delete it.'.format(version_uid.v_string))

            if not force:
                # check if disallow_rm_when_younger_than_days allows deletion
                age_days = (datetime.datetime.now() - version.date).days
                if disallow_rm_when_younger_than_days > age_days:
                    raise RuntimeError('Version {} is too young. Will not delete.'.format(version_uid.v_string))
                if not version.status.is_removable():
                    raise RuntimeError('Version {} cannot be removed without force, it has status {}.'.format(
                        version_uid.v_string, version.status.name))

            num_blocks = self._database_backend.rm_version(version_uid)

            if not keep_metadata_backup:
                try:
                    storage = StorageFactory.get_by_storage_id(version.storage_id)
                    storage.rm_version(version_uid)
                    logger.info('Removed version {} metadata backup from storage.'.format(version_uid.v_string))
                except FileNotFoundError:
                    logger.warning(
                        'Unable to remove version {} metadata backup from storage, the object wasn\'t found.'.format(
                            version_uid.v_string))
                    pass

            logger.info('Removed backup version {} with {} blocks.'.format(version_uid.v_string, num_blocks))

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
               version_name: str,
               version_snapshot_name: str,
               source: str,
               hints: List[Tuple[int, int, bool]] = None,
               base_version_uid: VersionUid = None,
               storage_name: str = None) -> Version:
        """ Create a backup from source.
        If hints are given, they must be tuples of (offset, length, exists) where offset and length are integers and
        exists is a boolean. In this case only data within hints will be backed up.
        Otherwise, the backup reads source and looks if checksums match with the target.
        """
        block: Union[DereferencedBlock, Block]

        stats: Dict[str, Any] = {
            'bytes_read': 0,
            'bytes_written': 0,
            'bytes_dedup': 0,
            'bytes_sparse': 0,
            'start_time': time.time(),
        }
        io = IOFactory.get(source, self._block_size)
        io.open_r()
        source_size = io.size()

        num_blocks = int(math.ceil(source_size / self._block_size))

        if hints is not None:
            if len(hints) > 0:
                # Sanity check: check hints for validity, i.e. too high offsets, ...
                max_offset = max([h[0] + h[1] for h in hints])
                if max_offset > source_size:
                    raise InputDataError('Hints have higher offsets than source file.')

                sparse_blocks, read_blocks = self._blocks_from_hints(hints, self._block_size)
            else:
                # Two snapshots can be completely identical between one backup and next
                logger.warning('Hints are empty, assuming nothing has changed.')
                sparse_blocks = set()
                read_blocks = set()
        else:
            sparse_blocks = set()
            read_blocks = set(range(num_blocks))

        version = self._prepare_version(
            version_name=version_name,
            version_snapshot_name=version_snapshot_name,
            size=source_size,
            base_version_uid=base_version_uid,
            storage_name=storage_name)
        self._locking.update_version_lock(version.uid, reason='Backing up')
        blocks = self._database_backend.get_blocks_by_version(version.uid)

        if base_version_uid and hints is not None:
            # SANITY CHECK:
            # Check some blocks outside of hints if they are the same in the
            # base_version backup and in the current backup. If they
            # aren't, either hints are wrong (e.g. from a wrong snapshot diff)
            # or source doesn't match. In any case, the resulting backup won't
            # be good.
            logger.info('Starting sanity check with 0.1% of the ignored blocks.')
            ignore_blocks = sorted(set(range(num_blocks)) - read_blocks - sparse_blocks)
            # 0.1% but at least ten. If there are less than ten blocks check them all.
            num_check_blocks = max(min(len(ignore_blocks), 10), len(ignore_blocks) // 1000)
            # 50% from the start
            check_block_ids = set(ignore_blocks[:num_check_blocks // 2])
            # and 50% from random locations
            check_block_ids = check_block_ids.union(random.sample(ignore_blocks, num_check_blocks // 2))
            num_reading = 0
            for block in blocks:
                if block.id in check_block_ids and block.uid and block.valid:  # no uid = sparse block in backup. Can't check.
                    io.read(block)
                    num_reading += 1
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
                    logger.error("Found wrong source data at block {}: offset {} with max. length {}".format(
                        source_block.id, source_block.id * self._block_size, self._block_size))
                    # remove version
                    self._database_backend.rm_version(version.uid)
                    raise InputDataError('Source changed in regions outside of ones indicated by the hints.')
            logger.info('Finished sanity check. Checked {} blocks: {}.'.format(num_reading, check_block_ids))

        try:
            storage = StorageFactory.get_by_storage_id(version.storage_id)
            read_jobs = 0
            for i, block in enumerate(blocks):
                if block.id in read_blocks or not block.valid:
                    io.read(block)
                    read_jobs += 1
                elif block.id in sparse_blocks:
                    # This "elif" is very important. Because if the block is in read_blocks AND sparse_blocks,
                    # it *must* be read.

                    # Only update the database when the block wasn't sparse to begin with
                    if block.uid is not None:
                        self._database_backend.set_block(
                            id=block.id,
                            version_uid=version.uid,
                            block_uid=None,
                            checksum=None,
                            size=block.size,
                            valid=True)
                        logger.debug('Skipping block (had data, turned sparse) {}'.format(block.id))
                    else:
                        assert block.checksum is None
                        logger.debug('Skipping block (sparse) {}'.format(block.id))
                    stats['bytes_sparse'] += block.size

                else:
                    # Block is already in database, no need to update it
                    logger.debug('Keeping block {}'.format(block.id))
                notify(
                    self._process_name, 'Backing up version {} from {}: Queueing blocks to read ({:.1f}%)'.format(
                        version.uid.v_string, source, (i + 1) / len(blocks) * 100))

            # precompute checksum of a sparse block
            sparse_block_checksum = self._block_hash.data_hexdigest(b'\0' * self._block_size)

            done_read_jobs = 0
            write_jobs = 0
            done_write_jobs = 0
            log_every_jobs = read_jobs // 200 + 1  # about every half percent
            for entry in io.read_get_completed():
                if isinstance(entry, Exception):
                    raise entry
                else:
                    block, data = cast(Tuple[DereferencedBlock, bytes], entry)

                stats['bytes_read'] += len(data)

                # dedup
                data_checksum = self._block_hash.data_hexdigest(data)
                existing_block = self._database_backend.get_block_by_checksum(data_checksum, version.storage_id)
                if data_checksum == sparse_block_checksum and block.size == self._block_size:
                    # if the block is only \0, set it as a sparse block.
                    stats['bytes_sparse'] += block.size
                    logger.debug('Skipping block (detected sparse) {}'.format(block.id))
                    self._database_backend.set_block(
                        id=block.id,
                        version_uid=version.uid,
                        block_uid=None,
                        checksum=None,
                        size=block.size,
                        valid=True)
                elif existing_block:
                    self._database_backend.set_block(
                        id=block.id,
                        version_uid=version.uid,
                        block_uid=existing_block.uid,
                        checksum=existing_block.checksum,
                        size=existing_block.size,
                        valid=True)
                    stats['bytes_dedup'] += len(data)
                    logger.debug('Found existing block for id {} with UID {}'.format(block.id, existing_block.uid))
                else:
                    block.uid = BlockUid(version.uid.integer, block.id + 1)
                    block.checksum = data_checksum
                    storage.write_block_async(block, data)
                    write_jobs += 1
                    logger.debug('Queued block {} for write (checksum {}...)'.format(block.id, data_checksum[:16]))

                done_read_jobs += 1

                try:
                    for written_block in storage.write_get_completed(timeout=0):
                        if isinstance(written_block, Exception):
                            raise written_block

                        written_block = cast(DereferencedBlock, written_block)

                        self._database_backend.set_block(
                            id=written_block.id,
                            version_uid=written_block.version_uid,
                            block_uid=written_block.uid,
                            checksum=written_block.checksum,
                            size=written_block.size,
                            valid=True)
                        done_write_jobs += 1
                        stats['bytes_written'] += written_block.size
                except (TimeoutError, CancelledError):
                    pass

                notify(
                    self._process_name, 'Backing up version {} from {} ({:.1f}%)'.format(
                        version.uid.v_string, source, done_read_jobs / read_jobs * 100))
                if done_read_jobs % log_every_jobs == 0 or done_read_jobs == read_jobs:
                    logger.info('Backed up {}/{} blocks ({:.1f}%)'.format(done_read_jobs, read_jobs,
                                                                          done_read_jobs / read_jobs * 100))

            try:
                for written_block in storage.write_get_completed():
                    if isinstance(written_block, Exception):
                        raise written_block

                    written_block = cast(DereferencedBlock, written_block)

                    self._database_backend.set_block(
                        id=written_block.id,
                        version_uid=written_block.version_uid,
                        block_uid=written_block.uid,
                        checksum=written_block.checksum,
                        size=written_block.size,
                        valid=True)
                    done_write_jobs += 1
                    stats['bytes_written'] += written_block.size
            except CancelledError:
                pass

        except:
            self._locking.unlock_version(version.uid)
            raise
        finally:
            # This will also cancel any outstanding read jobs
            io.close()
            self._database_backend.commit()

        if read_jobs != done_read_jobs:
            raise InternalError(
                'Number of submitted and completed read jobs inconsistent (submitted: {}, completed {}).'.format(
                    read_jobs, done_read_jobs))

        if write_jobs != done_write_jobs:
            raise InternalError(
                'Number of submitted and completed write jobs inconsistent (submitted: {}, completed {}).'.format(
                    write_jobs, done_write_jobs))

        self._database_backend.set_version(version.uid, status=VersionStatus.valid)

        self.metadata_backup([version.uid], overwrite=True, locking=False)

        logger.debug('Stats: {}'.format(stats))
        self._database_backend.set_stats(
            uid=version.uid,
            base_uid=base_version_uid,
            hints_supplied=hints is not None,
            date=version.date,
            name=version_name,
            snapshot_name=version_snapshot_name,
            size=source_size,
            storage_id=version.storage_id,
            block_size=self._block_size,
            bytes_read=stats['bytes_read'],
            bytes_written=stats['bytes_written'],
            bytes_dedup=stats['bytes_dedup'],
            bytes_sparse=stats['bytes_sparse'],
            duration=int(time.time() - stats['start_time']),
        )

        self._locking.unlock_version(version.uid)
        logger.info('New version {} created, backup successful.'.format(version.uid.v_string))
        return version

    def cleanup(self, dt: int = 3600, override_lock: bool = False) -> None:
        with self._locking.with_lock(
                lock_name='cleanup',
                reason='Cleanup',
                locked_msg='Another cleanup is already running.',
                override_lock=override_lock):
            notify(self._process_name, 'Cleanup')
            for hit_list in self._database_backend.get_delete_candidates(dt):
                for storage_id, uids in hit_list.items():
                    storage = StorageFactory.get_by_storage_id(storage_id)
                    logger.debug('Deleting UIDs from storage {}: {}'.format(storage.name,
                                                                            ', '.join([str(uid) for uid in uids])))

                    for uid in uids:
                        storage.rm_block_async(uid)

                    no_del_uids = []
                    for entry in storage.rm_get_completed():
                        if isinstance(entry, BlockNotFoundError):
                            no_del_uids.append(entry.uid)
                        elif isinstance(uid, Exception):
                            raise entry

                    if no_del_uids:
                        logger.info('Unable to delete these UIDs from storage {}: {}'.format(
                            storage.name, ', '.join([str(uid) for uid in no_del_uids])))
            notify(self._process_name)

    def add_label(self, version_uid: VersionUid, key: str, value: str) -> None:
        self._database_backend.add_label(version_uid, key, value)

    def rm_label(self, version_uid: VersionUid, key: str) -> None:
        self._database_backend.rm_label(version_uid, key)

    def close(self) -> None:
        StorageFactory.close()
        IOFactory.close()
        # Close database backend after storage so that any open locks are held until all storage jobs have
        # finished
        self._database_backend.close()

    def metadata_export(self, version_uids: Sequence[VersionUid], f: TextIO) -> None:
        try:
            locked_version_uids = []
            for version_uid in version_uids:
                self._locking.lock_version(version_uid, reason='Exporting version metadata')
                locked_version_uids.append(version_uid)

            self._database_backend.export(version_uids, f)
            logger.info('Exported metadata of version(s): {}.'.format(', '.join(
                [version_uid.v_string for version_uid in version_uids])))
        finally:
            for version_uid in locked_version_uids:
                self._locking.unlock_version(version_uid)

    def metadata_backup(self, version_uids: Sequence[VersionUid], overwrite: bool = False, locking: bool = True) -> None:
        versions = [self._database_backend.get_version(version_uid) for version_uid in version_uids]
        try:
            locked_version_uids = []
            if locking:
                for version in versions:
                    self._locking.lock_version(version.uid, reason='Backing up version metadata')
                    locked_version_uids.append(version.uid)

            for version in versions:
                with StringIO() as metadata_export:
                    self._database_backend.export([version.uid], metadata_export)
                    storage = StorageFactory.get_by_storage_id(version.storage_id)
                    storage.write_version(version.uid, metadata_export.getvalue(), overwrite=overwrite)
                logger.info('Backed up metadata of version {}.'.format(version.uid.v_string))
        finally:
            for version_uid in locked_version_uids:
                self._locking.unlock_version(version_uid)

    def export_any(self, *args, **kwargs) -> None:
        self._database_backend.export_any(*args, **kwargs)

    def metadata_import(self, f: TextIO) -> None:
        # TODO: Find a good way to lock here
        version_uids = self._database_backend.import_(f)
        logger.info('Imported metadata of version(s): {}.'.format(', '.join(
            [version_uid.v_string for version_uid in version_uids])))

    def metadata_restore(self, version_uids: Sequence[VersionUid], storage_name: str = None) -> None:
        if storage_name is not None:
            storage = StorageFactory.get_by_name(storage_name)
        else:
            storage = StorageFactory.get_by_storage_id(self._default_storage_id)
        try:
            locked_version_uids = []
            for version_uid in version_uids:
                self._locking.lock_version(version_uid, reason='Restoring version metadata')
                locked_version_uids.append(version_uid)

            for version_uid in version_uids:
                metadata_import_data = storage.read_version(version_uid)
                with StringIO(metadata_import_data) as metadata_import:
                    self._database_backend.import_(metadata_import)
                logger.info('Restored metadata of version {}.'.format(version_uid.v_string))
        finally:
            for version_uid in locked_version_uids:
                self._locking.unlock_version(version_uid)

    def metadata_ls(self, storage_name: str = None) -> List[VersionUid]:
        if storage_name is not None:
            storage = StorageFactory.get_by_name(storage_name)
        else:
            storage = StorageFactory.get_by_storage_id(self._default_storage_id)
        return storage.list_versions()

    def enforce_retention_policy(self,
                                 filter_expression: str,
                                 rules_spec: str,
                                 dry_run: bool = False,
                                 keep_metadata_backup: bool = False,
                                 group_label: str = None) -> List[Version]:
        versions = self._database_backend.get_versions_with_filter(filter_expression)

        versions_by_name: Dict[str, List[Version]] = {}
        for version in versions:
            if version.protected:
                logger.info('Not considering version {}, it is protected.'.format(version.uid.v_string))
                continue

            if not version.status.is_removable():
                logger.info('Not considering version {}, it has a status of {}.'.format(
                    version.uid.v_string, version.status.name))
                continue

            if version.name not in versions_by_name:
                versions_by_name[version.name] = []
            versions_by_name[version.name].append(version)

        dismissed_versions: Set[Version] = set()
        for versions_slice in versions_by_name.values():
            dismissed_versions |= set(RetentionFilter(rules_spec).filter(versions_slice))

        if dismissed_versions and group_label is not None:
            additional_versions: Set[Version] = set()
            for version in dismissed_versions:
                label_match = list(filter(lambda label: label.name == group_label, version.labels))
                if not label_match:
                    continue
                assert len(label_match) == 1
                additional_versions |= set(
                    self._database_backend.get_versions(version_labels=[(group_label, label_match[0].value)]))
            dismissed_versions |= additional_versions

        if dismissed_versions:
            logger.info('Removing versions: {}.'.format(', '.join(
                map(lambda version: version.uid.v_string, sorted(dismissed_versions)))))
        else:
            logger.info('All versions are conforming to the retention policy.')

        if dry_run:
            logger.info('Dry run, won\'t remove anything.')
            return []

        # Iterate through copy of dismissed_versions
        for version in list(dismissed_versions):
            try:
                self.rm(version.uid, force=True, keep_metadata_backup=keep_metadata_backup)
            except AlreadyLocked:
                logger.warning('Version {} couldn\'t be deleted, it\'s currently locked.')
                dismissed_versions.remove(version)

        return sorted(dismissed_versions)


class _BlockCache:

    _block_cache: Set[BlockUid]

    def __init__(self, cache_directory: str) -> None:
        self._cache_directory = cache_directory
        self._block_cache = set()

    def _cache_filename(self, block_uid: BlockUid) -> str:
        filename = '{:016x}-{:016x}'.format(block_uid.left, block_uid.right)
        digest = hashlib.md5(filename.encode('ascii')).hexdigest()
        return os.path.join(self._cache_directory, '{}/{}/{}'.format(digest[0:2], digest[2:4], filename))

    def read(self, block_uid: BlockUid, offset: int = 0, length: int = None) -> bytes:
        filename = self._cache_filename(block_uid)
        with open(filename, 'rb') as f:
            f.seek(offset)
            if length is None:
                return f.read()
            else:
                return f.read(length)

    def write(self, block_uid: BlockUid, data) -> None:
        filename = self._cache_filename(block_uid)
        try:
            with open(filename, 'wb') as f:
                f.write(data)
        except FileNotFoundError:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'wb') as f:
                f.write(data)

        self._block_cache.add(block_uid)

    def update(self, block_uid: BlockUid, offset: int, data: bytes) -> None:
        filename = self._cache_filename(block_uid)
        with open(filename, 'r+b') as f:
            f.seek(offset)
            f.write(data)

    def rm(self, block_uid: BlockUid) -> None:
        try:
            # Do this first, so that nobody tries to access this block anymore while we're trying to delete it
            self._block_cache.remove(block_uid)
        except KeyError:
            pass
        filename = self._cache_filename(block_uid)
        try:
            os.unlink(filename)
        except FileNotFoundError:
            pass

    def in_cache(self, block_uid: BlockUid) -> bool:
        return block_uid not in self._block_cache


# The reason for this class being here is that it accesses private attributes of class Benji
# and I don't want to make them all generally publicly available.
# Maybe they could inherit from the same base class in the future, but currently their
# functionality seems very different. So we just define that BenjiStore objects may access
# private attributes of Benji objects.
class BenjiStore(ReprMixIn):

    _benji_obj: Benji
    _cache_directory: str
    _blocks: Dict[VersionUid, List[Block]]
    _cow: Dict[VersionUid, Dict[int, DereferencedBlock]]

    def __init__(self, benji_obj: Benji) -> None:
        self._benji_obj = benji_obj
        self._blocks = {}  # block list cache by version
        self._cow = {}  # contains version_uid: dict() of block id -> block

        cache_directory = self._benji_obj.config.get('nbd.cacheDirectory', types=str)
        self._block_cache = _BlockCache(cache_directory)

    def open(self, version) -> None:
        self._benji_obj._locking.lock_version(version.uid, reason='NBD')

    def close(self, version) -> None:
        self._benji_obj._locking.unlock_version(version.uid)

    def get_versions(self, version_uid: VersionUid = None) -> List[Version]:
        return self._benji_obj._database_backend.get_versions(version_uid=version_uid)

    def _block_list(self, version: Version, offset: int, length: int) -> List[Tuple[Optional[Block], int, int]]:
        # Get version's blocks if they aren't in the cache already
        if version.uid not in self._blocks:
            self._blocks[version.uid] = self._benji_obj._database_backend.get_blocks_by_version(version.uid)
        blocks = self._blocks[version.uid]

        block_number = offset // version.block_size
        block_offset = offset % version.block_size

        chunks: List[Tuple[Optional[Block], int, int]] = []
        while True:
            try:
                block = blocks[block_number]
            except IndexError:
                # We round up the size reported by the NBD server to a multiple of 4096 which is the maximum
                # block size supported by NBD. So we might need to fake up to 4095 bytes (of zeros) here.
                if length > 4095:
                    # Don't throw one of our own exceptions here as we need an exception with an errno value
                    # to communicate it back in the NBD response.
                    raise OSError(errno.EIO)
                length_in_block = min(block.size - block_offset, length)
                chunks.append((None, 0, length_in_block))  # hint: return \0s
            else:
                assert block.id == block_number
                length_in_block = min(block.size - block_offset, length)
                chunks.append((block, block_offset, length_in_block))
            block_number += 1
            block_offset = 0
            length -= length_in_block
            assert length >= 0
            if length == 0:
                break

        return chunks

    def read(self, version: Version, cow_version: Optional[Version], offset: int, length: int) -> bytes:
        if cow_version:
            cow: Optional[Dict[int, DereferencedBlock]] = self._cow[cow_version.uid.integer]
        else:
            cow = None
        read_list = self._block_list(version, offset, length)
        data_chunks: List[bytes] = []
        block: Optional[Union[Block, DereferencedBlock]]
        for block, offset_in_block, length_in_block in read_list:
            # Read block from COW
            if block is not None and cow is not None and block.id in cow:
                logger.debug('Reading block from COW {}/{} {}:{}.'.format(block.version_uid.v_string, block.id,
                                                                          offset_in_block, length_in_block))
                block = cow[block.id]
            # Read block from original version
            elif block is not None:
                logger.debug('Reading {}block {}/{} {}:{}.'.format('sparse ' if not block.uid else '',
                                                                   block.version_uid.v_string, block.id,
                                                                   offset_in_block, length_in_block))

            # Block lies beyond end of device
            if block is None:
                logger.warning('Tried to read data beyond device (version {}, size {}, offset {}).'.format(
                    version.uid.v_string, version.size, offset_in_block))
                data_chunks.append(b'\0' * length_in_block)
            # Block is sparse
            elif not block.uid:
                data_chunks.append(b'\0' * length_in_block)
            else:
                # Block isn't cached already, fetch it
                if self._block_cache.in_cache(block.uid):
                    storage = StorageFactory.get_by_storage_id(version.storage_id)
                    data = storage.read_block(block)
                    self._block_cache.write(block.uid, data)

                data_chunks.append(self._block_cache.read(block.uid, offset_in_block, length_in_block))

        return b''.join(data_chunks)

    def get_cow_version(self, base_version: Version) -> Version:
        cow_version = self._benji_obj._prepare_version(
            version_name=base_version.name,
            version_snapshot_name='nbd-cow-{}-{}'.format(base_version.uid.v_string,
                                                         datetime.datetime.now().isoformat(timespec='seconds')),
            base_version_uid=base_version.uid,
            base_version_locking=False)
        self._benji_obj._locking.update_version_lock(cow_version.uid, reason='NBD COW')
        self._cow[cow_version.uid.integer] = {}  # contains version_uid: dict() of block id -> uid
        return cow_version

    def write(self, cow_version: Version, offset: int, data: bytes) -> None:
        """ Copy on write backup writer """
        cow = self._cow[cow_version.uid.integer]
        write_list = self._block_list(cow_version, offset, len(data))
        position_in_data = 0
        for block, offset_in_block, length_in_block in write_list:
            if block is None:
                logger.warning(
                    'COW: Tried to save data beyond device, it will be lost (version {}, size {}, offset {}).'.format(
                        cow_version.uid.v_string, cow_version.size, offset))
                break
            if block.id in cow:
                # The block is already copied, so update in the cache
                update_block = cow[block.id]
                self._block_cache.update(update_block.uid, offset_in_block,
                                         data[position_in_data:position_in_data + length_in_block])
                logger.debug('COW: Updated block {}/{} {}:{}.'.format(block.version_uid.v_string, block.id,
                                                                      offset_in_block, length_in_block))
            else:
                # Read the block from the original
                if block.uid:
                    storage = StorageFactory.get_by_storage_id(cow_version.storage_id)
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
                new_block.uid = BlockUid(cow_version.uid.integer, block.id + 1)
                new_block.checksum = None
                self._block_cache.write(new_block.uid, write_data.read())
                cow[block.id] = new_block
                logger.debug('COW: Wrote block {}/{} {}:{} into {}.'.format(
                    block.version_uid.v_string, block.id, offset_in_block, length_in_block, new_block.uid))
            position_in_data += length_in_block

    def flush(self, cow_version: Version) -> None:
        pass

    def fixate(self, cow_version: Version) -> None:
        # save blocks into version
        logger.info('Fixating version {} with {} blocks, please wait.'.format(cow_version.uid.v_string,
                                                                              len(self._cow[cow_version.uid.integer])))

        sparse_block_checksum = self._benji_obj._block_hash.data_hexdigest(b'\0' * cow_version.block_size)
        storage = StorageFactory.get_by_storage_id(cow_version.storage_id)
        for block in self._cow[cow_version.uid.integer].values():
            logger.debug('Fixating block {}/{} with UID {}'.format(cow_version.uid.v_string, block.id, block.uid))
            data = self._block_cache.read(block.uid)

            block.checksum = self._benji_obj._block_hash.data_hexdigest(data)
            if block.checksum == sparse_block_checksum:
                logger.debug('Detected sparse block {}/{}.'.format(cow_version.uid.v_string, block.id))
                self._block_cache.rm(block.uid)
                block.checksum = None
                block.uid = BlockUid(None, None)
            else:
                storage.write_block(block, data)
                self._block_cache.rm(block.uid)

            try:
                self._benji_obj._database_backend.set_block(
                    id=block.id,
                    version_uid=cow_version.uid,
                    block_uid=block.uid,
                    checksum=block.checksum,
                    size=len(data),
                    valid=True)
            except:
                # Prevent orphaned blocks
                if block.uid:
                    storage.rm_block(block.uid)

        self._benji_obj._database_backend.commit()
        self._benji_obj._database_backend.set_version(cow_version.uid, status=VersionStatus.valid, protected=True)
        self._benji_obj.metadata_backup([cow_version.uid], overwrite=True, locking=False)
        self._benji_obj._locking.unlock_version(cow_version.uid)
        del self._cow[cow_version.uid.integer]
        logger.info('Finished.')
