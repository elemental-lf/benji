#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import collections
import datetime
import enum
import inspect
import json
import math
import operator
import os
import platform
import re
import sqlite3
import threading
import time
import uuid
from abc import abstractmethod
from binascii import hexlify, unhexlify
from collections import OrderedDict, defaultdict
from contextlib import contextmanager
from functools import total_ordering
from itertools import chain
from typing import Union, List, Tuple, TextIO, Dict, cast, Iterator, Set, Any, Optional, Sequence, Callable

import pyparsing
import semantic_version
import sqlalchemy
import sqlalchemy.ext.declarative
import sqlalchemy.ext.mutable
import sqlalchemy.orm
from alembic import command as alembic_command
from alembic.config import Config as alembic_config_Config
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from sqlalchemy import func
from sqlalchemy.orm import object_session, sessionmaker, scoped_session, aliased
from sqlalchemy.orm.collections import attribute_mapped_collection

from benji.config import Config
from benji.exception import InputDataError, InternalError, AlreadyLocked, UsageError, ConfigurationError
from benji.logging import logger
from benji.repr import ReprMixIn
from benji.storage.key import StorageKeyMixIn
from benji.utils import InputValidation
from benji.versions import VERSIONS


# SQLite 3 supports checking of foreign keys but it needs to be enabled explicitly!
# See: http://docs.sqlalchemy.org/en/latest/dialects/sqlite.html#foreign-key-support
@sqlalchemy.event.listens_for(sqlalchemy.engine.Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, sqlite3.Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


class BenjiDateTime(sqlalchemy.types.TypeDecorator):

    impl = sqlalchemy.DateTime

    def process_bind_param(self, value: Optional[Union[datetime.datetime, str]], dialect) -> Optional[datetime.datetime]:
        if isinstance(value, datetime.datetime):
            if value.tzinfo is None:
                return value
            else:
                return value.astimezone(tz=datetime.timezone.utc).replace(tzinfo=None)
        elif isinstance(value, str):
            import dateparser
            date = dateparser.parse(date_string=value,
                                    date_formats=['%Y-%m-%dT%H:%M:%S'],
                                    locales=['en'],
                                    settings={
                                        'PREFER_DATES_FROM': 'past',
                                        'PREFER_DAY_OF_MONTH': 'first',
                                        'RETURN_AS_TIMEZONE_AWARE': True,
                                        'TO_TIMEZONE': 'UTC'
                                    })
            if date is None:
                raise ValueError('Invalid date and time specification: {}.'.format(value))
            return date.replace(tzinfo=None)
        else:
            raise InternalError('Unexpected type {} for value in BenjiDateTime.process_bind_param'.format(type(value)))


class VersionStatus(enum.Enum):
    incomplete = 1
    valid = 2
    invalid = 3

    min = incomplete
    max = invalid

    def __str__(self):
        return self.name

    def is_valid(self):
        return self == self.valid

    def is_deep_scrubbable(self):
        return self == self.invalid or self == self.valid

    def is_scrubbable(self):
        return self == self.valid

    def is_removable(self):
        return self != self.incomplete


class VersionStatusType(sqlalchemy.types.TypeDecorator):

    impl = sqlalchemy.Integer

    def process_bind_param(self, value: Optional[Union[int, str, VersionStatus]], dialect) -> Optional[int]:
        if value is None:
            return None
        elif isinstance(value, int):
            return value
        elif isinstance(value, str):
            return VersionStatus[value].value
        elif isinstance(value, VersionStatus):
            return value.value
        else:
            raise InternalError('Unexpected type {} for value in VersionStatusType.process_bind_param'.format(
                type(value)))

    def process_result_value(self, value: Optional[int], dialect) -> Optional[VersionStatus]:
        if value is not None:
            return VersionStatus(value)
        else:
            return None


class VersionUid(str, StorageKeyMixIn['VersionUid']):

    def __new__(cls, uid: str):
        if not isinstance(uid, str):
            raise InternalError(f'Unexpected type {type(uid)} in constructor.')
        if not InputValidation.is_version_uid(uid):
            raise InputDataError('Version name {} is invalid.'.format(uid))
        return str.__new__(cls, uid)  # type: ignore

    # Start: Implements StorageKeyMixIn
    _STORAGE_PREFIX = 'versions/'

    @classmethod
    def storage_prefix(cls) -> str:
        return cls._STORAGE_PREFIX

    def _storage_object_to_key(self) -> str:
        return str(self)

    @classmethod
    def _storage_key_to_object(cls, key: str) -> 'VersionUid':
        return VersionUid(key)

    # End: Implements StorageKeyMixIn


class VersionUidType(sqlalchemy.types.TypeDecorator):

    impl = sqlalchemy.String(255)

    def process_bind_param(self, value: Optional[str], dialect) -> Optional[str]:
        if value is None or isinstance(value, str):
            return value
        else:
            raise InternalError('Unexpected type {} for value in VersionUidType.process_bind_param'.format(type(value)))

    def process_result_value(self, value: Optional[str], dialect) -> Optional[VersionUid]:
        if value is not None:
            return VersionUid(value)
        else:
            return None


class ChecksumType(sqlalchemy.types.TypeDecorator):

    impl = sqlalchemy.LargeBinary

    def process_bind_param(self, value: Optional[str], dialect) -> Optional[bytes]:
        if value is not None:
            return unhexlify(value)
        else:
            return None

    def process_result_value(self, value: Optional[bytes], dialect) -> Optional[str]:
        if value is not None:
            return hexlify(value).decode('ascii')
        else:
            return None


class BlockUidComparator(sqlalchemy.orm.CompositeProperty.Comparator):

    def in_(self, other):
        clauses = self.__clause_element__().clauses
        other_tuples = [element.__composite_values__() for element in other]
        return sqlalchemy.sql.or_(
            *[sqlalchemy.sql.and_(*[clauses[0] == element[0], clauses[1] == element[1]]) for element in other_tuples])


@total_ordering
class BlockUid(sqlalchemy.ext.mutable.MutableComposite, StorageKeyMixIn['BlockUid']):

    left: Optional[int]
    right: Optional[int]

    def __init__(self, left: Optional[int], right: Optional[int]) -> None:
        assert (left is None and right is None) or (left is not None and right is not None)
        self.left = left
        self.right = right

    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)
        self.changed()

    def __composite_values__(self) -> Tuple[Optional[int], Optional[int]]:
        return self.left, self.right

    def __str__(self) -> str:
        return "{:x}-{:x}".format(self.left if self.left is not None else 0, self.right if self.right is not None else 0)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, BlockUid):
            return self.left == other.left and self.right == other.right
        else:
            return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, BlockUid):
            self_left = self.left if self.left is not None else 0
            self_right = self.right if self.right is not None else 0
            other_left = other.left if other.left is not None else 0
            other_right = other.right if other.right is not None else 0
            return self_left < other_left or self_left == other_left and self_right < other_right
        else:
            return NotImplemented

    def __bool__(self) -> bool:
        return self.left is not None and self.right is not None

    def __hash__(self) -> int:
        return hash((self.left, self.right))

    @classmethod
    def coerce(cls, key, value):
        if isinstance(value, cls):
            return value
        else:
            return super().coerce(key, value)

    # Start: Implements StorageKeyMixIn
    _STORAGE_PREFIX = 'blocks/'

    @classmethod
    def storage_prefix(cls) -> str:
        return cls._STORAGE_PREFIX

    def _storage_object_to_key(self) -> str:
        assert self.left is not None and self.right is not None
        return '{:016x}-{:016x}'.format(self.left, self.right)

    @classmethod
    def _storage_key_to_object(cls, key: str) -> 'BlockUid':
        if len(key) != (16 + 1 + 16):
            raise RuntimeError('Object key {} has an invalid length, expected exactly {} characters.'.format(
                key, (16 + 1 + 16)))
        return BlockUid(int(key[0:16], 16), int(key[17:17 + 16], 16))

    # End: Implements StorageKeyMixIn


SparseBlockUid = BlockUid(None, None)

# Explicit naming helps Alembic to auto-generate versions
metadata = sqlalchemy.MetaData(
    naming_convention={
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s"
    })
Base: Any = sqlalchemy.ext.declarative.declarative_base(metadata=metadata)

Session = scoped_session(sessionmaker())


@total_ordering
class Version(Base, ReprMixIn):
    __tablename__ = 'versions'

    TIMED_COMMIT_INTERVAL = 20  # in seconds
    BLOCKS_PER_CALL = 10000
    REPR_SQL_ATTR_SORT_FIRST = ['uid', 'volume', 'snapshot']

    _last_timed_commit = time.monotonic()

    # This makes sure that SQLite won't reuse UIDs
    __table_args__ = {'sqlite_autoincrement': True}

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True, nullable=False)
    uid = sqlalchemy.Column(VersionUidType, unique=True, nullable=False)
    date = sqlalchemy.Column(BenjiDateTime, nullable=False)
    volume = sqlalchemy.Column(sqlalchemy.String(255), nullable=False, index=True)
    snapshot = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    size = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
    block_size = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    storage_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('storages.id'), nullable=False)
    # Force loading of storage so that the attribute can be accessed even when there is no associated session anymore.
    storage = sqlalchemy.orm.relationship('Storage', lazy='joined')
    status = sqlalchemy.Column(VersionStatusType,
                               sqlalchemy.CheckConstraint('status >= {} AND status <= {}'.format(
                                   VersionStatus.min.value, VersionStatus.max.value),
                                                          name='status'),
                               nullable=False)
    protected = sqlalchemy.Column(sqlalchemy.Boolean(name='protected'), nullable=False)

    # Statistics
    bytes_read = sqlalchemy.Column(sqlalchemy.BigInteger)
    bytes_written = sqlalchemy.Column(sqlalchemy.BigInteger)
    bytes_deduplicated = sqlalchemy.Column(sqlalchemy.BigInteger)
    bytes_sparse = sqlalchemy.Column(sqlalchemy.BigInteger)
    duration = sqlalchemy.Column(sqlalchemy.BigInteger)

    # See https://docs.sqlalchemy.org/en/13/orm/collections.html#passive-deletes
    labels = sqlalchemy.orm.relationship('Label',
                                         backref='version',
                                         order_by='asc(Label.name)',
                                         passive_deletes=True,
                                         cascade='all, delete-orphan',
                                         collection_class=attribute_mapped_collection('name'))

    @classmethod
    def create(cls,
               version_uid: VersionUid,
               volume: str,
               snapshot: str,
               size: int,
               storage_id: int,
               block_size: int,
               status: VersionStatus = VersionStatus.incomplete,
               protected: bool = False) -> 'Version':
        version = cls(
            uid=version_uid,
            volume=volume,
            snapshot=snapshot,
            size=size,
            storage_id=storage_id,
            block_size=block_size,
            status=status,
            protected=protected,
            date=datetime.datetime.utcnow(),
        )
        try:
            Session.add(version)
            Session.commit()
        except:
            Session.rollback()
            raise

        return version

    def remove(self) -> int:
        try:
            affected_blocks = Session.query(Block).filter(Block.version_id == self.id)
            num_blocks = affected_blocks.count()
            for affected_block in affected_blocks:
                if affected_block.uid:
                    deleted_block = DeletedBlock(
                        storage_id=self.storage_id,
                        uid=affected_block.uid,
                        date=datetime.datetime.utcnow(),
                    )
                    Session.add(deleted_block)
            # The following delete statement will cascade this delete to the blocks table
            # and delete all blocks
            Session.delete(self)
            Session.commit()
        except:
            Session.rollback()
            raise

        return num_blocks

    @classmethod
    def _timed_commit(cls) -> None:
        current_clock = time.monotonic()
        if current_clock - cls._last_timed_commit > cls.TIMED_COMMIT_INTERVAL:
            t1 = time.time()
            Session.commit()
            t2 = time.time()
            caller = inspect.stack()[1].function
            logger.debug('Commited database transaction in {} in {:.2f}s'.format(caller, t2 - t1))
            cls._last_timed_commit = current_clock

    @staticmethod
    def commit() -> None:
        Session.commit()

    def create_blocks(self, *, blocks: List[Dict[str, Any]]) -> None:
        try:
            # Remove any fully sparse blocks
            blocks = [
                block for block in blocks if block['uid_left'] is not None or block['uid_right'] is not None or block['size'] != self.block_size
            ]

            for block in blocks:
                block['version_id'] = self.id

            Session.bulk_insert_mappings(Block, blocks)
            self._timed_commit()
        except:
            Session.rollback()
            raise

    def set_block(self, *, idx: int, block_uid: BlockUid, checksum: Optional[str], size: int, valid: bool) -> None:
        try:
            block = Session.query(Block).filter(Block.version_id == self.id, Block.idx == idx).one_or_none()

            if not block and not block_uid and size == self.block_size:
                # Block is not present and it should be fully sparse now -> Nothing to do.
                return
            elif block and not block_uid and size == self.block_size:
                # Block is present but it should be fully sparse now -> Delete it.
                Session.delete(block)
            elif not block:
                # Block is not present but should contain data now -> Create it.
                block = Block(version_id=self.id, idx=idx, uid=block_uid, checksum=checksum, size=size, valid=valid)
                Session.add(block)
            else:
                # Block is present and  should contains data -> Update it.
                block.uid = block_uid
                block.checksum = checksum
                block.size = size
                block.valid = valid

            self._timed_commit()
        except:
            Session.rollback()
            raise

    def set_stats(self, *, bytes_read: int, bytes_written: int, bytes_deduplicated: int, bytes_sparse: int,
                  duration: int) -> None:
        try:
            self.bytes_read = bytes_read
            self.bytes_written = bytes_written
            self.bytes_deduplicated = bytes_deduplicated
            self.bytes_sparse = bytes_sparse
            self.duration = duration
            Session.commit()
        except:
            Session.rollback()
            raise

    def set(self, *, status: VersionStatus = None, protected: bool = None):
        try:
            if status is not None:
                self.status = status
            if protected is not None:
                self.protected = protected
            Session.commit()
            if status is not None:
                logger_func = logger.info if self.status.is_valid() else logger.error
                logger_func('Set status of version {} to {}.'.format(self.uid, self.status.name))
            if protected is not None:
                logger.info('Marked version {} as {}.'.format(self.uid, 'protected' if protected else 'unprotected'))
        except:
            Session.rollback()
            raise

    @classmethod
    def set_block_invalid(cls, block_uid: BlockUid) -> List[VersionUid]:
        try:
            affected_version_uids_query = Session.query(sqlalchemy.distinct(
                Version.uid).label('uid')).join(Block).filter(Block.uid == block_uid)
            affected_version_uids = [row.uid for row in affected_version_uids_query]
            assert len(affected_version_uids) > 0

            Session.query(Block).filter(Block.uid == block_uid).update({'valid': False}, synchronize_session=False)
            Session.commit()

            logger.error('Marked block with UID {} as invalid. Affected versions: {}.'.format(
                block_uid, ', '.join(affected_version_uids)))

            for version_uid in affected_version_uids:
                version = Version.get_by_uid(version_uid)
                version.set(status=VersionStatus.invalid)
            Session.commit()
        except:
            Session.rollback()
            raise

        return affected_version_uids

    def _create_sparse_block(self, idx: int) -> 'Block':
        # This block isn't part if the database session and probably never will be.
        return Block(version_id=self.id, idx=idx, uid=SparseBlockUid, checksum=None, size=self.block_size, valid=True)

    # Our own version of yield_per without using a cursor
    # See: https://github.com/sqlalchemy/sqlalchemy/wiki/WindowedRangeQuery
    @property
    def blocks(self) -> Iterator['Block']:
        next_start_idx = 0
        while True:
            start_idx = next_start_idx
            next_start_idx = min(start_idx + self.BLOCKS_PER_CALL, self.blocks_count)

            blocks = object_session(self).query(Block).filter(Block.version_id == self.id, Block.idx >= start_idx,
                                                              Block.idx < next_start_idx).order_by(Block.idx)

            idx = start_idx
            for block in blocks:
                if idx < block.idx:
                    logger.debug(f'Synthesizing sparse blocks {idx} to {block.idx - 1}.')
                while idx < block.idx:
                    yield self._create_sparse_block(idx)
                    idx += 1
                yield block
                idx += 1

            if idx < next_start_idx:
                logger.debug(f'Synthesizing sparse blocks {idx} to {next_start_idx - 1} at end of slice.')
            while idx < next_start_idx:
                yield self._create_sparse_block(idx)
                idx += 1

            if next_start_idx == self.blocks_count:
                break

    @property
    def blocks_count(self) -> int:
        return math.ceil(self.size / self.block_size)

    @property
    def sparse_blocks_count(self) -> int:
        # noinspection PyComparisonWithNone
        non_sparse_blocks_query = object_session(self).query(Block.idx, Block.uid_left, Block.uid_right).filter(
            Block.version_id == self.id, Block.uid_left != None, Block.uid_right != None)
        non_sparse_blocks = {row.idx for row in non_sparse_blocks_query}

        return len([idx for idx in range(self.blocks_count) if idx not in non_sparse_blocks])

    @classmethod
    def get_by_uid(cls, version_uid: VersionUid) -> 'Version':
        version = Session.query(cls).filter(cls.uid == version_uid).one_or_none()

        if version is None:
            raise KeyError('Version {} not found.'.format(version_uid))

        return version

    def get_block_by_idx(self, idx: int) -> 'Block':
        block = Session.query(Block).filter(Block.version_id == self.id, Block.idx == idx).one_or_none()
        if not block:
            block = self._create_sparse_block(idx)

        return block

    def get_block_by_checksum(self, checksum):
        return Session.query(Block).join(Version).filter(Block.checksum == checksum, Block.valid == True,
                                                         Version.storage_id == self.storage_id).first()

    @classmethod
    def find(cls,
             version_uid: VersionUid = None,
             volume: str = None,
             snapshot: str = None,
             labels: List[Tuple[str, str]] = None) -> List['Version']:
        query = Session.query(Version)
        if version_uid:
            query = query.filter(Version.uid == version_uid)
        if volume:
            query = query.filter(Version.volume == volume)
        if snapshot:
            query = query.filterby(Version.snapshot == snapshot)
        if labels:
            for label in labels:
                label_query = Session.query(Label.version_uid).filter((Label.name == label[0]) & (Label.value == label[1]))
                query = query.filter(Version.uid.in_(label_query))

        return query.order_by(Version.volume, Version.date).all()

    @classmethod
    def find_with_filter(cls, filter_expression: str = None):
        builder = _QueryBuilder()
        return builder.build(filter_expression).order_by(Version.volume, Version.date).all()

    @classmethod
    def storage_usage(cls, filter_expression: str = None) -> Dict[str, Dict[str, int]]:
        if filter_expression is not None:
            builder = _QueryBuilder()
            version_ids = [id for id, in builder.build(filter_expression, columns=(Version.id,)).all()]
        else:
            version_ids = None

        usage = defaultdict(lambda: {
            'virtual': 0,
            'sparse': 0,
            'shared': 0,
            'exclusive': 0,
            'deduplicated_exclusive': 0,
        })

        virtual = Session.query(Storage.id, Storage.name,
                                func.sum(Version.size)).select_from(Version).join(Storage).group_by(
                                    Storage.id, Storage.name)
        if version_ids is not None:
            virtual = virtual.filter(Version.id.in_(version_ids))

        for _, storage_name, size in virtual.all():
            usage[storage_name]['virtual'] = int(size)  # func.sum()/SUM() returns type Decimal

        sq_block, sq_storage, sq_version = aliased(Block), aliased(Storage), aliased(Version)
        share_count_overall_sq = Session.query(func.count('*')).select_from(sq_block).join(sq_version).join(
            sq_storage).filter((Storage.id == sq_storage.id) & (Block.uid_left == sq_block.uid_left) &
                               (Block.uid_right == sq_block.uid_right)).as_scalar()

        # noinspection PyComparisonWithNone
        share_count_query = Session.query(
            Storage.id.label('storage_id'), Storage.name.label('storage_name'), Block.uid, Block.size.label('size'),
            func.count('*').label('share_count_subset'),
            share_count_overall_sq.label('share_count_overall')).select_from(Block).join(Version).join(Storage).filter(
                (Block.uid_left != None) & (Block.uid_right != None)).group_by(Storage.id, Storage.name, Block.uid,
                                                                               Block.size)

        if version_ids is not None:
            share_count_query = share_count_query.filter(Block.version_id.in_(version_ids))

        for row in share_count_query.all():
            if row.share_count_overall == row.share_count_subset:
                usage[row.storage_name]['exclusive'] += row.size * row.share_count_subset
                usage[row.storage_name]['deduplicated_exclusive'] += row.size
            else:
                usage[row.storage_name]['shared'] += row.size * row.share_count_subset

        for storage_name, entry in usage.items():
            usage[storage_name]['sparse'] = usage[storage_name]['virtual'] - usage[storage_name]['exclusive'] - usage[storage_name]['shared']

        return usage

    def add_label(self, name: str, value: str) -> None:
        try:
            label = Session.query(Label).join(Version).filter(Version.uid == self.uid, Label.name == name).one_or_none()
            if label:
                label.value = value
            else:
                version = Session.query(Version).filter(Version.uid == self.uid).one_or_none()
                if version is None:
                    raise KeyError('Version {} not found.'.format(self.uid))
                label = Label(version_id=version.id, name=name, value=value)
                Session.add(label)

            Session.commit()
        except:
            Session.rollback()
            raise

    def rm_label(self, name: str) -> None:
        try:
            version = Session.query(Version).filter(Version.uid == self.uid).one_or_none()
            Session.query(Label).filter(Label.version_id == version.id,
                                        Label.name == name).delete(synchronize_session=False)
            Session.commit()
        except:
            Session.rollback()
            raise

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Version):
            return self.uid == other.uid
        else:
            return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, Version):
            return self.uid < other.uid
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return self.id


class Label(Base, ReprMixIn):
    __tablename__ = 'labels'

    REPR_SQL_ATTR_SORT_FIRST = ['version_id', 'name', 'value']

    version_id = sqlalchemy.Column(sqlalchemy.Integer,
                                   sqlalchemy.ForeignKey('versions.id', ondelete='CASCADE'),
                                   primary_key=True,
                                   nullable=False)
    name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False, index=True, primary_key=True)
    value = sqlalchemy.Column(sqlalchemy.String(255), nullable=False, index=True)


class DereferencedBlock(ReprMixIn):

    def __init__(self, uid: Optional[BlockUid], version_id: int, idx: int, checksum: Optional[str], size: int,
                 valid: bool) -> None:
        self.uid = uid if uid is not None else BlockUid(None, None)
        self.version_id = version_id
        self.idx = idx
        self.checksum = checksum
        self.size = size
        self.valid = valid

    # Getter and setter need to directly follow each other
    # See https://github.com/python/mypy/issues/1465
    @property
    def uid(self) -> BlockUid:
        return self._uid

    @uid.setter
    def uid(self, uid: Optional[BlockUid]) -> None:
        if uid is None:
            self._uid = BlockUid(None, None)
        elif isinstance(uid, BlockUid):
            self._uid = uid
        else:
            raise InternalError('Unexpected type {} for uid.'.format(type(uid)))

    @property
    def uid_left(self) -> Optional[int]:
        return self._uid.left

    @property
    def uid_right(self) -> Optional[int]:
        return self._uid.right

    def deref(self) -> 'DereferencedBlock':
        return self


class Block(Base, ReprMixIn):
    __tablename__ = 'blocks'

    MAXIMUM_CHECKSUM_LENGTH = 64
    REPR_SQL_ATTR_SORT_FIRST = ['version_id', 'idx', 'uid_left', 'uid_right']

    # Sorted for best alignment to safe space (with PostgreSQL in mind)
    # idx and uid_right are first because they are most likely to go to BigInteger in the future
    idx = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)  # 4 bytes
    uid_right = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)  # 4 bytes
    uid_left = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)  # 4 bytes
    size = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)  # 4 bytes
    version_id = sqlalchemy.Column(sqlalchemy.Integer,
                                   sqlalchemy.ForeignKey('versions.id', ondelete='CASCADE'),
                                   nullable=False)  # 4 bytes
    valid = sqlalchemy.Column(sqlalchemy.Boolean(name='valid'), nullable=False)  # 1 byte
    checksum = sqlalchemy.Column(ChecksumType(MAXIMUM_CHECKSUM_LENGTH), nullable=True)  # 2 to 33 bytes

    uid = cast(BlockUid, sqlalchemy.orm.composite(BlockUid, uid_left, uid_right, comparator_factory=BlockUidComparator))
    __table_args__ = (
        sqlalchemy.PrimaryKeyConstraint('version_id', 'idx'),
        sqlalchemy.Index(None, 'uid_left', 'uid_right'),
        # Maybe using an hash index on PostgeSQL might be beneficial in the future
        # Index(None, 'checksum', postgresql_using='hash'),
        sqlalchemy.Index(None, 'checksum'),
    )

    def deref(self) -> DereferencedBlock:
        """ Dereference this to a namedtuple so that we can pass it around
        without any thread inconsistencies
        """
        return DereferencedBlock(
            uid=self.uid,
            version_id=self.version_id,
            idx=self.idx,
            checksum=self.checksum,
            size=self.size,
            valid=self.valid,
        )


class DeletedBlock(Base, ReprMixIn):
    __tablename__ = 'deleted_blocks'

    REPR_SQL_ATTR_SORT_FIRST = ['storage_id', 'uid']

    date = sqlalchemy.Column("date", BenjiDateTime, nullable=False)
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True, nullable=False)
    storage_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('storages.id'), nullable=False)
    # Force loading of storage so that the attribute can be accessed even when there is no associated session anymore.
    storage = sqlalchemy.orm.relationship('Storage', lazy='joined')

    uid_left = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    uid_right = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)

    uid = sqlalchemy.orm.composite(BlockUid, uid_left, uid_right, comparator_factory=BlockUidComparator)
    __table_args__ = (sqlalchemy.Index(None, 'uid_left', 'uid_right'),)

    @classmethod
    def get_unused_block_uids(cls, dt: int = 3600) -> Iterator[Dict[str, Set[BlockUid]]]:
        rounds = 0
        false_positives_count = 0
        hit_list_count = 0
        cut_off_date = datetime.datetime.utcnow() - datetime.timedelta(seconds=dt)
        while True:
            # http://stackoverflow.com/questions/7389759/memory-efficient-built-in-sqlalchemy-iterator-generator
            delete_candidates = Session.query(DeletedBlock) \
                .filter(DeletedBlock.date < cut_off_date) \
                .limit(250) \
                .all()
            if not delete_candidates:
                break

            false_positives = set()
            hit_list: Dict[str, Set[BlockUid]] = defaultdict(set)
            for candidate in delete_candidates:
                rounds += 1
                if rounds % 1000 == 0:
                    logger.info("Cleanup: {} false positives, {} data deletions.".format(
                        false_positives_count,
                        hit_list_count,
                    ))

                block = Session.query(Block) \
                    .filter(Block.uid == candidate.uid) \
                    .limit(1) \
                    .scalar()
                if block:
                    false_positives.add(candidate.uid)
                    false_positives_count += 1
                else:
                    hit_list[candidate.storage.name].add(candidate.uid)
                    hit_list_count += 1

            if false_positives:
                logger.debug("Cleanup: Removing {} false positive from delete candidates.".format(len(false_positives)))
                Session.query(DeletedBlock) \
                    .filter(DeletedBlock.uid.in_(false_positives)) \
                    .delete(synchronize_session=False)

            if hit_list:
                for uids in hit_list.values():
                    Session.query(DeletedBlock).filter(DeletedBlock.uid.in_(uids)).delete(synchronize_session=False)
                yield hit_list
                # We expect that the caller has handled all the blocks returned so far, so we can call commit after
                # the yield to keep the transaction small.
                Session.commit()

        Session.commit()
        logger.info("Cleanup finished: {} false positives, {} data deletions.".format(
            false_positives_count,
            hit_list_count,
        ))


class Lock(Base, ReprMixIn):
    __tablename__ = 'locks'

    REPR_SQL_ATTR_SORT_FIRST = ['lock_name', 'host', 'process_id', 'date']

    lock_name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False, primary_key=True)
    host = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    process_id = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    reason = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    date = sqlalchemy.Column(BenjiDateTime, nullable=False)


class Storage(Base, ReprMixIn):
    __tablename__ = 'storages'

    REPR_SQL_ATTR_SORT_FIRST = ['name']

    # # Use INTEGER to get AUTOINCREMENT on SQLite.
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True, nullable=False)
    name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False, unique=True)

    @classmethod
    def get_by_name(cls, storage_name: str) -> 'Storage':
        return Session.query(cls).filter(cls.name == storage_name).one_or_none()

    @classmethod
    def sync(cls, storage_name: str, storage_id: int = None) -> 'Storage':
        try:
            storage = Session.query(cls).filter(cls.name == storage_name).one_or_none()
            if storage:
                if storage_id is not None and storage.id != storage_id:
                    raise ConfigurationError(
                        'Storage ids of {} do not match between configuration and database ({} != {}).'.format(
                            storage_name, storage_id, storage.id))
                logger.debug('Found existing storage {} with id {}.'.format(storage.name, storage.id))
            else:
                storage = cls(name=storage_name, id=storage_id)
                Session.add(storage)
                Session.commit()
                logger.debug('Created new storage {} with id {}.'.format(storage.name, storage.id))

            return storage
        except:
            Session.rollback()
            raise


class _Database(ReprMixIn):
    _METADATA_VERSION_KEY = 'metadata_version'
    _METADATA_VERSION_REGEX = r'\d+\.\d+\.\d+'

    def __init__(self) -> None:
        self._config = None
        self._engine = None

    def configure(self, config: Config, in_memory: bool = False) -> None:
        if not in_memory:
            url = config.get('databaseEngine', types=str)
            connect_args = {}
            if url.startswith('sqlite:'):
                # This tries to work around a SQLite design limitation. It's best to use PostgreSQL if you're affected
                # by this as it doesn't have this limitation.
                # Also see https://github.com/elemental-lf/benji/issues/11.
                # Increase the timeout (5 seconds is the default). This will make "database is locked" errors
                # due to concurrent database access less likely.
                connect_args['timeout'] = 3 * Version.TIMED_COMMIT_INTERVAL
            self._engine = sqlalchemy.create_engine(url, connect_args=connect_args)
        else:
            logger.info('Running with ephemeral in-memory database.')
            self._engine = sqlalchemy.create_engine('sqlite://')

        self._config = config

    @staticmethod
    def _alembic_config():
        return alembic_config_Config(
            os.path.join(os.path.dirname(os.path.realpath(__file__)), "sql_migrations", "alembic.ini"))

    def _database_tables(self) -> List[str]:
        # Need to ignore internal SQLite table here
        return [table for table in self._engine.table_names() if table != 'sqlite_sequence']

    def _migration_needed(self, alembic_config: alembic_config_Config) -> Tuple[bool, str, str]:
        table_names = self._database_tables()
        if not table_names:
            raise RuntimeError('Database schema appears to be empty, it needs to be initialized.')

        with self._engine.begin() as connection:
            alembic_config.attributes['connection'] = connection
            script = ScriptDirectory.from_config(alembic_config)
            with EnvironmentContext(alembic_config, script) as env_context:
                env_context.configure(connection, version_table="alembic_version")
                head_revision = env_context.get_head_revision()
                migration_context = env_context.get_context()
                current_revision = migration_context.get_current_revision()

        logger.debug(
            'Current database schema revision: {}.'.format(current_revision if current_revision is not None else '<unknown>'))
        logger.debug('Expected database schema revision: {}.'.format(head_revision))

        return ((current_revision is not None and current_revision != head_revision), current_revision, head_revision)

    def migrate(self) -> None:
        alembic_config = self._alembic_config()
        migration_needed, current_revision, head_revision = self._migration_needed(alembic_config)
        if migration_needed:
            logger.info('Migrating from database schema revision {} to {}.'.format(current_revision, head_revision))
            with self._engine.begin() as connection:
                alembic_config.attributes['connection'] = connection
                alembic_config.attributes['benji_config'] = self._config
                alembic_command.upgrade(alembic_config, "head")
        else:
            logger.info('Current database schema revision: {}.'.format(current_revision))
            logger.info('The database schema is up-to-date.')

    def open(self):
        alembic_config = self._alembic_config()

        migration_needed, current_revision, head_revision = self._migration_needed(alembic_config)
        if migration_needed:
            logger.info('Current database schema revision: {}.'.format(current_revision))
            logger.info('Expected database schema revision: {}.'.format(head_revision))
            raise UsageError('The database schema requires migration.')

        Session.configure(bind=self._engine)

    def init(self, _destroy: bool = False) -> None:
        # This is dangerous and is only used by the test suite to get a clean slate
        if _destroy:
            Base.metadata.drop_all(self._engine)
            # Drop alembic_version table
            if self._engine.has_table('alembic_version'):
                with self._engine.begin() as connection:
                    connection.execute(
                        sqlalchemy.sql.ddl.DropTable(sqlalchemy.Table('alembic_version', sqlalchemy.MetaData())))

        table_names = self._database_tables()
        if not table_names:
            Base.metadata.create_all(self._engine, checkfirst=False)
        else:
            logger.debug('Existing tables: {}'.format(', '.join(sorted(table_names))))
            raise FileExistsError('Database schema already contains tables. Not touching anything.')

        alembic_config = self._alembic_config()
        with self._engine.begin() as connection:
            alembic_config.attributes['connection'] = connection
            alembic_config.attributes['benji_config'] = self._config
            alembic_command.stamp(alembic_config, "head")

    # Based on: https://stackoverflow.com/questions/5022066/how-to-serialize-sqlalchemy-result-to-json/7032311,
    # https://stackoverflow.com/questions/1958219/convert-sqlalchemy-row-object-to-python-dict
    @staticmethod
    def _new_benji_encoder(ignore_fields: Optional[List], ignore_relationships: Optional[List]):
        ignore_fields = list(ignore_fields) if ignore_fields is not None else []
        ignore_relationships = list(ignore_relationships) if ignore_relationships is not None else []

        # These are always ignored because they'd lead to a circle
        ignore_fields.append(((Label, Block), ('version_id',)))
        ignore_relationships.append(((Label, Block), ('version',)))
        # Ignore these as we favor the composite attribute
        ignore_fields.append(((Block,), ('uid_left', 'uid_right')))
        # Ignore storage_id as we export the storage attribute
        ignore_fields.append(((Version), ('storage_id')))

        # Source: https://stackoverflow.com/questions/21663800/python-make-a-list-generator-json-serializable/46841935#46841935
        # Alternative: simplejson with iterable_as_array=True
        class SerializableGenerator(list):
            """Generator that is serializable by JSON

            It is useful for serializing huge data by JSON
            >>> json.dumps(SerializableGenerator(iter([1, 2])))
            "[1, 2]"
            >>> json.dumps(SerializableGenerator(iter([])))
            "[]"

            It can be used in a generator of json chunks used e.g. for a stream
            >>> iter_json = ison.JSONEncoder().iterencode(SerializableGenerator(iter([])))
            >>> tuple(iter_json)
            ('[1', ']')
            # >>> for chunk in iter_json:
            # ...     stream.write(chunk)
            # >>> SerializableGenerator((x for x in range(3)))
            # [<generator object <genexpr> at 0x7f858b5180f8>]
            """

            def __init__(self, iterable):
                tmp_body = iter(iterable)
                try:
                    self._head = iter([next(tmp_body)])
                    self.append(tmp_body)
                except StopIteration:
                    self._head = iter([])

            def __iter__(self):
                return chain(self._head, *self[:1])

        class BenjiEncoder(json.JSONEncoder):

            def default(self, obj):
                if isinstance(obj, datetime.datetime):
                    return obj.isoformat(timespec='microseconds') + 'Z'

                if isinstance(obj, VersionUid):
                    return str(obj)

                if isinstance(obj, BlockUid):
                    return {'left': obj.left, 'right': obj.right}

                if isinstance(obj, VersionStatus):
                    return obj.name

                if isinstance(obj, Label):
                    return obj.value

                if isinstance(obj, Storage):
                    return obj.name

                if isinstance(obj.__class__, sqlalchemy.ext.declarative.DeclarativeMeta):
                    # Use ordered dictionary to make iterative JSON parsing possible. See below.
                    fields: OrderedDict[str, Any] = OrderedDict()

                    for field in sqlalchemy.inspect(obj).mapper.composites:
                        ignore = False
                        for types, names in ignore_fields:  # type: ignore
                            if isinstance(obj, types) and field.key in names:
                                ignore = True
                                break
                        if not ignore:
                            fields[field.key] = getattr(obj, field.key)

                    for field in sqlalchemy.inspect(obj).mapper.column_attrs:
                        ignore = False
                        for types, names in ignore_fields:  # type: ignore
                            if isinstance(obj, types) and field.key in names:
                                ignore = True
                                break
                        if not ignore:
                            fields[field.key] = getattr(obj, field.key)

                    for relationship in sqlalchemy.inspect(obj).mapper.relationships:
                        ignore = False
                        for types, names in ignore_relationships:  # type: ignore
                            if isinstance(obj, types) and relationship.key in names:
                                ignore = True
                                break
                        if not ignore:
                            fields[relationship.key] = getattr(obj, relationship.key)

                    # Force ordering for versions to make iterative JSON parsing possible.
                    if isinstance(obj, Version):
                        if 'labels' in fields:
                            fields.move_to_end('labels')

                        ignore = False
                        for types, names in ignore_relationships:  # type: ignore
                            if isinstance(obj, types) and 'blocks' in names:
                                ignore = True
                                break

                        if not ignore:
                            fields['blocks'] = obj.blocks

                    return fields

                if isinstance(obj, collections.abc.Iterable):
                    return SerializableGenerator(obj)

                return super().default(obj)

        return BenjiEncoder

    def export_any(self,
                   root_dict: Dict,
                   f: TextIO,
                   ignore_fields: List = None,
                   ignore_relationships: List = None,
                   compact: bool = False) -> None:
        # Metadata output is ordered in such a way as to make it possible to use an iterative JSON parser
        # on import for version metadata. This is also the reason for using an OrderedDict for database objects
        # and for placing the labels and blocks at the end.
        root_dict = OrderedDict(root_dict.copy())
        root_dict[self._METADATA_VERSION_KEY] = str(VERSIONS.database_metadata.current)
        root_dict.move_to_end(self._METADATA_VERSION_KEY, last=False)

        json.dump(
            root_dict,
            f,
            cls=self._new_benji_encoder(ignore_fields, ignore_relationships),
            check_circular=True,
            separators=(',', ':') if compact else (',', ': '),
            indent=None if compact else 2,
        )

    def export(self, version_uids: Sequence[VersionUid], f: TextIO):
        self.export_any({'versions': [Version.get_by_uid(version_uid) for version_uid in version_uids]},
                        f,
                        compact=True)

    def import_(self, f: TextIO) -> List[VersionUid]:
        try:
            json_input = json.load(f)
        except Exception as exception:
            raise InputDataError('Import file is invalid.') from exception
        if json_input is None:
            raise InputDataError('Import file is empty.')

        if self._METADATA_VERSION_KEY not in json_input:
            raise InputDataError('Import file is missing required key "{}".'.format(self._METADATA_VERSION_KEY))
        metadata_version = json_input[self._METADATA_VERSION_KEY]
        if not re.fullmatch(self._METADATA_VERSION_REGEX, metadata_version):
            raise InputDataError('Import file has an invalid vesion of "{}".'.format(metadata_version))

        metadata_version_obj = semantic_version.Version(metadata_version)
        if metadata_version_obj not in VERSIONS.database_metadata.supported:
            raise InputDataError('Unsupported metadata version (1): "{}".'.format(str(metadata_version_obj)))

        import_method_name = 'import_v{}'.format(metadata_version_obj.major)
        import_method = getattr(self, import_method_name, None)
        if import_method is None or not callable(import_method):
            raise InputDataError('Unsupported metadata version (2): "{}".'.format(metadata_version))

        try:
            version_uids = import_method(metadata_version_obj, json_input)
            Session.commit()
        except:
            Session.rollback()
            raise

        return version_uids

    def import_v1(self, metadata_version: semantic_version.Version, json_input: Dict) -> List[VersionUid]:
        for version_dict in json_input['versions']:
            # We only do enough input validation for the key we access here, the rest will be done by import_v2
            if not isinstance(version_dict, dict):
                raise InputDataError('Wrong data type for versions list element.')

            if 'uid' not in version_dict:
                raise InputDataError('Missing attribute uid in version.')

            # Will raise ValueError when invalid
            version_uid = VersionUid(f'V{version_dict["uid"]:010d}')
            version_dict['uid'] = str(version_uid)

            attributes_to_check = ['labels', 'blocks', 'date', 'storage_id', 'name']

            for attribute in attributes_to_check:
                if attribute not in version_dict:
                    raise InputDataError('Missing attribute {} in version {}.'.format(attribute, version_uid))

            version_dict['volume'] = version_dict['name']
            del version_dict['name']

            # Starting with 1.1.0 the statistics where folded into the versions table, fake them for 1.0.x
            if metadata_version.minor == 0:
                version_dict['bytes_read'] = None
                version_dict['bytes_written'] = None
                version_dict['bytes_deduplicated'] = None
                version_dict['bytes_sparse'] = None
                version_dict['duration'] = None
            else:
                version_dict['bytes_deduplicated'] = version_dict['bytes_dedup']
                del version_dict['bytes_dedup']

            if not isinstance(version_dict['labels'], list):
                raise InputDataError('Wrong data type for labels in version {}.'.format(version_uid))

            # Convert label list to dictionary
            labels_dict: Dict[str, str] = {}
            for name_value_dict in version_dict['labels']:
                if not isinstance(name_value_dict, dict):
                    raise InputDataError('Wrong data type for labels list element in version {}.'.format(version_uid))
                for attribute in ['name', 'value']:
                    if attribute not in name_value_dict:
                        raise InputDataError('Missing attribute {} in labels list in version {}.'.format(
                            attribute, version_uid))

                labels_dict[name_value_dict['name']] = name_value_dict['value']
            version_dict['labels'] = labels_dict

            if not isinstance(version_dict['blocks'], list):
                raise InputDataError('Wrong data type for blocks in version {}.'.format(version_uid))

            for block_dict in version_dict['blocks']:
                if 'id' not in block_dict:
                    raise InputDataError('Missing id attribute in block list of version {}.'.format(version_uid))
                block_dict['idx'] = block_dict['id']
                del block_dict['id']

            if not isinstance(version_dict['date'], str):
                raise InputDataError('Wrong data type for date in version {}.'.format(version_uid))

            version_dict['date'] = version_dict['date'] + 'Z'

            version_dict['storage'] = Session.query(Storage).get(version_dict['storage_id']).name
            del version_dict['storage_id']

            version_dict['snapshot'] = version_dict['snapshot_name']
            del version_dict['snapshot_name']

        return self.import_v3(metadata_version, json_input)

    def import_v2(self, metadata_version: semantic_version.Version, json_input: Dict) -> List[VersionUid]:
        # The v3 format doesn't list sparse blocks anymore.
        return self.import_v3(metadata_version, json_input)

    @staticmethod
    def import_v3(metadata_version: semantic_version.Version, json_input: Dict) -> List[VersionUid]:
        version_uids: List[VersionUid] = []
        for version_dict in json_input['versions']:
            if not isinstance(version_dict, dict):
                raise InputDataError('Wrong data type for versions list element.')

            if 'uid' not in version_dict:
                raise InputDataError('Missing attribute uid in version.')

            # Will raise ValueError when invalid
            version_uid = VersionUid(version_dict['uid'])

            attributes_to_check = [
                'date',
                'volume',
                'snapshot',
                'size',
                'storage',
                'block_size',
                'status',
                'protected',
                'blocks',
                'labels',
                'bytes_read',
                'bytes_written',
                'bytes_deduplicated',
                'bytes_sparse',
                'duration',
            ]

            for attribute in attributes_to_check:
                if attribute not in version_dict:
                    raise InputDataError('Missing attribute {} in version {}.'.format(attribute, version_uid))

            if not InputValidation.is_volume_name(version_dict['volume']):
                raise InputDataError('Volume name {} in version {} is invalid.'.format(
                    version_dict['name'], version_uid))

            if not InputValidation.is_snapshot_name(version_dict['snapshot']):
                raise InputDataError('Snapshot name {} in version {} is invalid.'.format(
                    version_dict['snapshot'], version_uid))

            if not isinstance(version_dict['labels'], dict):
                raise InputDataError('Wrong data type for labels in version {}.'.format(version_uid))

            if not isinstance(version_dict['blocks'], list):
                raise InputDataError('Wrong data type for blocks in version {}.'.format(version_uid))

            for name, value in version_dict['labels'].items():
                if not InputValidation.is_label_name(name):
                    raise InputDataError('Label name {} in version {} is invalid.'.format(name, version_uid))
                if not InputValidation.is_label_value(value):
                    raise InputDataError('Label value {} in version {} is invalid.'.format(value, version_uid))

            for block_dict in version_dict['blocks']:
                if not isinstance(block_dict, dict):
                    raise InputDataError('Wrong data type for block list element in version {}.'.format(version_uid))
                for attribute in ['uid', 'size', 'valid', 'checksum']:
                    if attribute not in block_dict:
                        raise InputDataError('Missing attribute {} in block list in version {}.'.format(
                            attribute, version_uid))

            storage = Session.query(Storage).filter(Storage.name == version_dict['storage']).one_or_none()
            if not storage:
                raise InputDataError('Storage {} is not defined in the configuration.'.format(version_dict['storage']))

            try:
                Version.get_by_uid(VersionUid(version_dict['uid']))
            except KeyError:
                pass  # does not exist
            else:
                raise FileExistsError('Version {} already exists and so cannot be imported.'.format(version_uid))

            version = Version(
                uid=version_uid,
                date=datetime.datetime.strptime(version_dict['date'], '%Y-%m-%dT%H:%M:%S.%fZ'),
                volume=version_dict['volume'],
                snapshot=version_dict['snapshot'],
                size=version_dict['size'],
                storage=storage,
                block_size=version_dict['block_size'],
                status=VersionStatus[version_dict['status']],
                protected=version_dict['protected'],
                bytes_read=version_dict['bytes_read'],
                bytes_written=version_dict['bytes_written'],
                bytes_deduplicated=version_dict['bytes_deduplicated'],
                bytes_sparse=version_dict['bytes_sparse'],
                duration=version_dict['duration'],
            )
            Session.add(version)
            Session.flush()

            assert isinstance(version_dict['blocks'], list)
            for block_dict in version_dict['blocks']:
                assert isinstance(block_dict, dict)
                for attribute in ('idx', 'uid', 'size', 'checksum'):
                    if attribute not in block_dict:
                        raise InputDataError('Missing attribute {} in block of version {}.'.format(
                            attribute, version_uid))

                assert isinstance(block_dict['uid'], dict)
                for attribute in ('left', 'right'):
                    if attribute not in block_dict['uid']:
                        raise InputDataError('Missing attribute {} in block uid of version {}.'.format(
                            attribute, version_uid))

                block_dict['version_id'] = version.id
                block_uid = BlockUid(block_dict['uid']['left'], block_dict['uid']['right'])
                block_dict['uid_left'] = block_uid.left
                block_dict['uid_right'] = block_uid.right
                del block_dict['uid']
            Session.bulk_insert_mappings(Block, version_dict['blocks'])

            labels: List[Dict[str, Any]] = []
            assert isinstance(version_dict['labels'], dict)
            for name, value in version_dict['labels'].items():
                labels.append({'version_id': version.id, 'name': name, 'value': value})
            Session.bulk_insert_mappings(Label, labels)

            version_uids.append(version_uid)

        return version_uids

    def close(self):
        Session.commit()
        Locking.unlock_all()
        Session.remove()
        self._engine.dispose()


Database = _Database()


class _Locking:

    def __init__(self) -> None:
        self._host = platform.node()
        self._uuid = uuid.uuid1().hex

    def _process_id(self):
        return '{}-{}'.format(self._uuid, threading.get_ident())

    def lock(self, *, lock_name: str, reason: str = None, locked_msg: str = None, override_lock: bool = False) -> None:
        try:
            lock = Session.query(Lock).filter(Lock.host == self._host, Lock.lock_name == lock_name,
                                              Lock.process_id == self._process_id()).one_or_none()
            if lock is not None:
                raise InternalError('Attempt to acquire lock {} twice.'.format(lock_name))
            lock = Lock(
                lock_name=lock_name,
                host=self._host,
                process_id=self._process_id(),
                reason=reason,
                date=datetime.datetime.utcnow(),
            )
            if override_lock:
                logger.warn('Will override any existing lock.')
                Session.merge(lock, load=True)
            else:
                Session.add(lock)
            Session.commit()
        except sqlalchemy.exc.IntegrityError:
            Session.rollback()
            if locked_msg is not None:
                raise AlreadyLocked(locked_msg) from None
            else:
                raise AlreadyLocked('Lock {} is already taken.'.format(lock_name)) from None
        except:
            Session.rollback()
            raise

    @staticmethod
    def is_locked(*, lock_name: str) -> bool:
        try:
            lock = Session.query(Lock).filter(Lock.lock_name == lock_name).one_or_none()
        except:
            Session.rollback()
            raise
        else:
            return lock is not None

    def update_lock(self, *, lock_name: str, reason: str = None) -> None:
        try:
            lock = Session.query(Lock).filter(Lock.host == self._host, Lock.lock_name == lock_name,
                                              Lock.process_id == self._process_id()).with_for_update().one_or_none()
            if not lock:
                raise InternalError('Lock {} isn\'t held by this instance or doesn\'t exist.'.format(lock_name))
            lock.reason = reason
            Session.commit()
        except:
            Session.rollback()
            raise

    def unlock(self, *, lock_name: str) -> None:
        try:
            lock = Session.query(Lock).filter(Lock.host == self._host, Lock.lock_name == lock_name,
                                              Lock.process_id == self._process_id()).one_or_none()
            if not lock:
                raise InternalError('Lock {} isn\'t held by this instance or doesn\'t exist.'.format(lock_name))
            Session.delete(lock)
            Session.commit()
        except:
            Session.rollback()
            raise

    def unlock_all(self) -> None:
        try:
            locks = Session.query(Lock).filter(Lock.host == self._host, Lock.process_id == self._process_id())
            for lock in locks:
                logger.error('Lock {} not released correctly, releasing it now.'.format(lock.lock_name))
                Session.delete(lock)
            Session.commit()
        except:
            pass

    def lock_version(self, version_uid: VersionUid, reason: str = None, override_lock: bool = False) -> None:
        self.lock(lock_name='Version {}'.format(version_uid),
                  reason=reason,
                  locked_msg='Version {} is already locked.'.format(version_uid),
                  override_lock=override_lock)

    def is_version_locked(self, version_uid: VersionUid) -> bool:
        return self.is_locked(lock_name='Version {}'.format(version_uid))

    def update_version_lock(self, version_uid: VersionUid, reason: str = None) -> None:
        self.update_lock(lock_name='Version {}'.format(version_uid), reason=reason)

    def unlock_version(self, version_uid: VersionUid) -> None:
        self.unlock(lock_name='Version {}'.format(version_uid))

    @contextmanager
    def with_lock(self,
                  *,
                  lock_name: str,
                  reason: str = None,
                  locked_msg: str = None,
                  unlock: bool = True,
                  override_lock: bool = False) -> Iterator[None]:
        self.lock(lock_name=lock_name, reason=reason, locked_msg=locked_msg, override_lock=override_lock)
        try:
            yield
        except:
            self.unlock(lock_name=lock_name)
            raise
        else:
            if unlock:
                self.unlock(lock_name=lock_name)

    @contextmanager
    def with_version_lock(self,
                          version_uid: VersionUid,
                          reason: str = None,
                          unlock: bool = True,
                          override_lock: bool = False) -> Iterator[None]:
        self.lock_version(version_uid, reason=reason, override_lock=override_lock)
        try:
            yield
        except:
            self.unlock_version(version_uid)
            raise
        else:
            if unlock:
                self.unlock_version(version_uid)


Locking = _Locking()


class _QueryBuilder:

    def __init__(self) -> None:
        self._parser = self._define_parser()

    @staticmethod
    def _define_parser() -> Any:

        pyparsing.ParserElement.enablePackrat()

        class Buildable:

            @abstractmethod
            def build(self) -> sqlalchemy.sql.ColumnElement:
                raise NotImplementedError()

        class Token(Buildable):
            pass

        class IdentifierToken(Token):

            def __init__(self, name: str) -> None:
                self.name = name

            def op(self, op: Callable[[Any, Any], sqlalchemy.sql.elements.BinaryExpression],
                   other: Any) -> sqlalchemy.sql.elements.BinaryExpression:
                if isinstance(other, IdentifierToken):
                    return op(getattr(Version, self.name), getattr(Version, other.name))
                elif isinstance(other, Token):
                    raise TypeError('Comparing identifiers to labels is not supported.')
                else:
                    return op(getattr(Version, self.name), other)

            def __eq__(self, other: Any) -> sqlalchemy.sql.elements.BinaryExpression:
                return self.op(operator.eq, other)

            def __ne__(self, other: Any) -> sqlalchemy.sql.elements.BinaryExpression:
                return self.op(operator.ne, other)

            def __lt__(self, other: Any) -> sqlalchemy.sql.elements.BinaryExpression:
                return self.op(operator.lt, other)

            def __le__(self, other: Any) -> sqlalchemy.sql.elements.BinaryExpression:
                return self.op(operator.le, other)

            def __gt__(self, other: Any) -> sqlalchemy.sql.elements.BinaryExpression:
                return self.op(operator.gt, other)

            def __ge__(self, other: Any) -> sqlalchemy.sql.elements.BinaryExpression:
                return self.op(operator.ge, other)

            # This is called when the token is not part of a comparison and tests for a non-empty identifier
            def build(self) -> sqlalchemy.sql.elements.BinaryExpression:
                return getattr(Version, self.name) != ''

        class LabelToken(Token):

            def __init__(self, name: str) -> None:
                self.name = name

            def op(self, op, other: Any) -> sqlalchemy.sql.elements.BinaryExpression:
                if isinstance(other, Token):
                    raise TypeError('Comparing labels to labels or labels to identifiers is not supported.')
                label_query = Session.query(Label.version_id).filter((Label.name == self.name) &
                                                                     op(Label.value, str(other)))
                return Version.id.in_(label_query)

            def __eq__(self, other: Any) -> sqlalchemy.sql.elements.BinaryExpression:
                return self.op(operator.eq, other)

            def __ne__(self, other: Any) -> sqlalchemy.sql.elements.BinaryExpression:
                return self.op(operator.ne, other)

            # This is called when the token is not part of a comparison and test for label existence
            def build(self) -> sqlalchemy.sql.elements.BinaryExpression:
                label_query = Session.query(Label.version_id).filter(Label.name == self.name)
                return Version.id.in_(label_query)

        attributes = []
        for attribute in sqlalchemy.inspect(Version).mapper.composites:
            attributes.append(attribute.key)

        for attribute in sqlalchemy.inspect(Version).mapper.column_attrs:
            attributes.append(attribute.key)

        identifier = pyparsing.Regex('|'.join(attributes)).setParseAction(lambda s, l, t: IdentifierToken(t[0]))
        integer = pyparsing.pyparsing_common.signed_integer
        string = pyparsing.quotedString().setParseAction(pyparsing.removeQuotes)
        bool_true = pyparsing.Keyword('True').setParseAction(pyparsing.replaceWith(True))
        bool_false = pyparsing.Keyword('False').setParseAction(pyparsing.replaceWith(False))
        label = (pyparsing.Literal('labels') + pyparsing.Literal('[') + string +
                 pyparsing.Literal(']')).setParseAction(lambda s, l, t: LabelToken(t[2]))
        atom = identifier | integer | string | bool_true | bool_false | label

        class BinaryOp(Buildable):

            op: Optional[Callable[[Any, Any], sqlalchemy.sql.elements.BooleanClauseList]] = None

            def __init__(self, t) -> None:
                assert len(t[0]) == 3
                self.args = t[0][0::2]

            def build(self) -> sqlalchemy.sql.elements.BooleanClauseList:
                assert self.op is not None
                return self.op(*self.args)

        class EqOp(BinaryOp):
            op = operator.eq

        class NeOp(BinaryOp):
            op = operator.ne

        class LeOp(BinaryOp):
            op = operator.le

        class GeOp(BinaryOp):
            op = operator.ge

        class LtOp(BinaryOp):
            op = operator.lt

        class GtOp(BinaryOp):
            op = operator.gt

        class MultiaryOp(Buildable):

            # Need to use Any here as mypy doesn't understand that Python thinks that op is a method and
            # so has a __func__ attribute
            op: Any = None

            def __init__(self, t) -> None:
                args = t[0][0::2]
                for token in args:
                    if not isinstance(token, Buildable):
                        raise pyparsing.ParseFatalException('Operands of boolean and must be expressions, identifier or label references.')
                self.args = args

            def build(self) -> sqlalchemy.sql.elements.BooleanClauseList:
                assert self.op is not None
                # __func__ is necessary to call op as a function instead of as a method
                return self.op.__func__(*map(lambda token: token.build(), self.args))

        class AndOp(MultiaryOp):
            op = sqlalchemy.and_

        class OrOp(MultiaryOp):
            op = sqlalchemy.or_

        class NotOp(Buildable):

            def __init__(self, t) -> None:
                self.args = [t[0][1]]

            def build(self) -> sqlalchemy.sql.elements.BooleanClauseList:
                return sqlalchemy.not_(self.args[0].build())

        return pyparsing.infixNotation(atom, [
            ("==", 2, pyparsing.opAssoc.LEFT, EqOp),
            ("!=", 2, pyparsing.opAssoc.LEFT, NeOp),
            ("<=", 2, pyparsing.opAssoc.LEFT, LeOp),
            (">=", 2, pyparsing.opAssoc.LEFT, GeOp),
            ("<", 2, pyparsing.opAssoc.LEFT, LtOp),
            (">", 2, pyparsing.opAssoc.LEFT, GtOp),
            ("not", 1, pyparsing.opAssoc.RIGHT, NotOp),
            ("and", 2, pyparsing.opAssoc.LEFT, AndOp),
            ("or", 2, pyparsing.opAssoc.LEFT, OrOp),
        ])

    def build(self, filter_expression: Optional[str], columns: Sequence[Any] = None) -> Any:
        if columns is not None:
            query = Session.query(*columns)
        else:
            query = Session.query(Version)
        if filter_expression:
            try:
                parsed_filter_expression = self._parser.parseString(filter_expression, parseAll=True)[0]
            except (pyparsing.ParseException, pyparsing.ParseFatalException) as exception:
                raise UsageError('Invalid filter expression {}.'.format(filter_expression)) from exception
            try:
                filter_result = parsed_filter_expression.build()
            except (AttributeError, TypeError) as exception:
                # User supplied only a constant or is trying to compare apples to oranges
                raise UsageError('Invalid filter expression {} (2).'.format(filter_expression)) from exception
            if not isinstance(filter_result, sqlalchemy.sql.ColumnElement):
                # Expression doesn't contain at least one expression with references to a SQL column
                raise UsageError('Invalid filter expression {} (3).'.format(filter_expression))
            query = query.filter(filter_result)
        return query
