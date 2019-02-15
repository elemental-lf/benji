#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import datetime
import enum
import json
import operator
import os
import platform
import re
import sqlite3
import time
import uuid
from abc import abstractmethod
from binascii import hexlify, unhexlify
from contextlib import contextmanager
from functools import total_ordering
from typing import Union, List, Tuple, TextIO, Dict, cast, Iterator, Set, Any, Optional, Sequence, Callable

import dateparser
import semantic_version
import sqlalchemy
from alembic import command as alembic_command
from alembic.config import Config as alembic_config_Config
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from pyparsing import pyparsing_common, quotedString, removeQuotes, replaceWith, Keyword, opAssoc, infixNotation, \
    Regex, ParseException, ParseFatalException, Literal, NoMatch, ParserElement
from sqlalchemy.sql.ddl import DropTable

ParserElement.enablePackrat()
from sqlalchemy import Column, String, Integer, BigInteger, ForeignKey, LargeBinary, Boolean, inspect, event, Index, \
    DateTime, UniqueConstraint, and_, or_, not_, MetaData, Table, CheckConstraint
from sqlalchemy import distinct
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.ext.mutable import MutableComposite
from sqlalchemy.orm import sessionmaker, composite, CompositeProperty
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql.elements import BooleanClauseList, BinaryExpression
from sqlalchemy.types import TypeDecorator

from benji.config import Config
from benji.exception import InputDataError, InternalError, AlreadyLocked, UsageError
from benji.logging import logger
from benji.repr import ReprMixIn
from benji.storage.key import StorageKeyMixIn
from benji.utils import InputValidation
from benji.versions import VERSIONS


class BenjiDateTime(TypeDecorator):

    impl = DateTime

    def process_bind_param(self, value: Optional[Union[datetime.datetime, str]], dialect) -> Optional[datetime.datetime]:
        if isinstance(value, datetime.datetime):
            if value.tzinfo is None:
                return value
            else:
                return value.astimezone(tz=datetime.timezone.utc).replace(tzinfo=None)
        elif isinstance(value, str):
            date = dateparser.parse(
                date_string=value,
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


class VersionStatusType(TypeDecorator):

    impl = Integer

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


@total_ordering
class VersionUid(StorageKeyMixIn['VersionUid']):

    def __init__(self, value: Union[str, int]) -> None:
        value_int: int
        if isinstance(value, int):
            value_int = value
        elif isinstance(value, str):
            try:
                value_int = int(value)
            except ValueError:
                if len(value) < 2:
                    raise ValueError('Version UID {} is too short.'.format(value)) from None
                if value[0].lower() != 'v':
                    raise ValueError(
                        'Version UID {} is invalid. A Version UID string has to start with the letter V.'.format(value)) from None
                try:
                    value_int = int(value[1:])
                except ValueError:
                    raise ValueError('Version UID {} is invalid.'.format(value)) from None
        else:
            raise ValueError('Version UID {} has unsupported type {}.'.format(str(value), type(value)))
        self._value = value_int

    @property
    def integer(self) -> int:
        return self._value

    @property
    def v_string(self) -> str:
        return 'V' + str(self._value).zfill(10)

    def __str__(self) -> str:
        return self.v_string

    def __repr__(self) -> str:
        return str(self.integer)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, VersionUid):
            return self.integer == other.integer
        elif isinstance(other, int):
            return self.integer == other
        else:
            return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, VersionUid):
            return self.integer < other.integer
        elif isinstance(other, int):
            return self.integer < other
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return self.integer

    # Start: Implements StorageKeyMixIn
    _STORAGE_PREFIX = 'versions/'

    @classmethod
    def storage_prefix(cls) -> str:
        return cls._STORAGE_PREFIX

    def _storage_object_to_key(self) -> str:
        return self.v_string

    @classmethod
    def _storage_key_to_object(cls, key: str) -> 'VersionUid':
        vl = len(VersionUid(1).v_string)
        if len(key) != vl:
            raise RuntimeError('Object key {} has an invalid length, expected exactly {} characters.'.format(key, vl))
        return VersionUid(key)

    # End: Implements StorageKeyMixIn


class VersionUidType(TypeDecorator):

    impl = Integer

    def process_bind_param(self, value: Optional[Union[int, str, VersionUid]], dialect) -> Optional[int]:
        if value is None:
            return None
        elif isinstance(value, int):
            return value
        elif isinstance(value, str):
            return VersionUid(value).integer
        elif isinstance(value, VersionUid):
            return value.integer
        else:
            raise InternalError('Unexpected type {} for value in VersionUidType.process_bind_param'.format(type(value)))

    def process_result_value(self, value: Optional[int], dialect) -> Optional[VersionUid]:
        if value is not None:
            return VersionUid(value)
        else:
            return None


class ChecksumType(TypeDecorator):

    impl = LargeBinary

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


class BlockUidComparator(CompositeProperty.Comparator):

    def in_(self, other):
        clauses = self.__clause_element__().clauses
        other_tuples = [element.__composite_values__() for element in other]
        return sqlalchemy.sql.or_(
            *[sqlalchemy.sql.and_(*[clauses[0] == element[0], clauses[1] == element[1]]) for element in other_tuples])


@total_ordering
class BlockUid(MutableComposite, StorageKeyMixIn['BlockUid']):

    def __init__(self, left: Optional[int], right: Optional[int]) -> None:
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
        return '{:016x}-{:016x}'.format(self.left, self.right)

    @classmethod
    def _storage_key_to_object(cls, key: str) -> 'BlockUid':
        if len(key) != (16 + 1 + 16):
            raise RuntimeError('Object key {} has an invalid length, expected exactly {} characters.'.format(
                key, (16 + 1 + 16)))
        return BlockUid(int(key[0:16], 16), int(key[17:17 + 16], 16))

    # End: Implements StorageKeyMixIn


# Explicit naming helps Alembic to auto-generate versions
metadata = MetaData(
    naming_convention={
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s"
    })
Base: Any = declarative_base(metadata=metadata)


# This mirrors Version with some extra fields
class VersionStatistic(Base):
    __tablename__ = 'version_statistics'
    # No foreign key references here, so that we can keep the stats around even when the version is deleted
    uid = Column(VersionUidType, primary_key=True, autoincrement=False)
    base_uid = Column(VersionUidType, nullable=True)
    hints_supplied = Column(Boolean(name='hints_supplied'), nullable=False)
    name = Column(String(255), nullable=False)
    date = Column(BenjiDateTime, nullable=False)
    snapshot_name = Column(String(255), nullable=False)
    size = Column(BigInteger, nullable=False)
    storage_id = Column(Integer, nullable=False)
    block_size = Column(BigInteger, nullable=False)
    bytes_read = Column(BigInteger, nullable=False)
    bytes_written = Column(BigInteger, nullable=False)
    bytes_dedup = Column(BigInteger, nullable=False)
    bytes_sparse = Column(BigInteger, nullable=False)
    duration = Column(BigInteger, nullable=False)


@total_ordering
class Version(Base):
    __tablename__ = 'versions'

    REPR_SQL_ATTR_SORT_FIRST = ['uid', 'name', 'snapshot_name']

    # This makes sure that SQLite won't reuse UIDs
    __table_args__ = {'sqlite_autoincrement': True}
    uid = Column(VersionUidType, primary_key=True, autoincrement=True, nullable=False)
    date = Column(BenjiDateTime, nullable=False)
    name = Column(String(255), nullable=False, index=True)
    snapshot_name = Column(String(255), nullable=False)
    size = Column(BigInteger, nullable=False)
    block_size = Column(Integer, nullable=False)
    storage_id = Column(Integer, nullable=False)
    status = Column(
        VersionStatusType,
        CheckConstraint(
            'status >= {} AND status <= {}'.format(VersionStatus.min.value, VersionStatus.max.value), name='status'),
        nullable=False)
    protected = Column(Boolean(name='protected'), nullable=False)

    labels = sqlalchemy.orm.relationship(
        'Label',
        backref='version',
        order_by='asc(Label.name)',
        passive_deletes=True,
    )

    blocks = sqlalchemy.orm.relationship(
        'Block',
        backref='version',
        order_by='asc(Block.id)',
        passive_deletes=True,
    )

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
        return self.uid.integer


class Label(Base):
    __tablename__ = 'labels'

    REPR_SQL_ATTR_SORT_FIRST = ['version_uid', 'name', 'value']

    version_uid = Column(
        VersionUidType, ForeignKey('versions.uid', ondelete='CASCADE'), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False, primary_key=True)
    value = Column(String(255), nullable=False, index=True)

    __table_args__ = (UniqueConstraint('version_uid', 'name'),)


class DereferencedBlock(ReprMixIn):

    def __init__(self, uid: Optional[BlockUid], version_uid: VersionUid, id: int, checksum: Optional[str], size: int,
                 valid: bool) -> None:
        self.uid = uid if uid is not None else BlockUid(None, None)
        self.version_uid = version_uid
        self.id = id
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


class Block(Base):
    __tablename__ = 'blocks'

    MAXIMUM_CHECKSUM_LENGTH = 64
    REPR_SQL_ATTR_SORT_FIRST = ['version_uid', 'id']

    # Sorted for best alignment to safe space (with PostgreSQL in mind)
    # id and uid_right are first because they are most likely to go to BigInteger in the future
    id = Column(Integer, primary_key=True, nullable=False)  # 4 bytes
    uid_right = Column(Integer, nullable=True)  # 4 bytes
    uid_left = Column(Integer, nullable=True)  # 4 bytes
    size = Column(Integer, nullable=True)  # 4 bytes
    version_uid = Column(
        VersionUidType, ForeignKey('versions.uid', ondelete='CASCADE'), primary_key=True, nullable=False)  # 4 bytes
    valid = Column(Boolean(name='valid'), nullable=False)  # 1 byte
    checksum = Column(ChecksumType(MAXIMUM_CHECKSUM_LENGTH), nullable=True)  # 2 to 33 bytes

    uid = cast(BlockUid, composite(BlockUid, uid_left, uid_right, comparator_factory=BlockUidComparator))
    __table_args__ = (
        Index(None, 'uid_left', 'uid_right'),
        # Maybe using an hash index on PostgeSQL might be beneficial in the future
        # Index(None, 'checksum', postgresql_using='hash'),
        Index(None, 'checksum'),
    )

    def deref(self) -> DereferencedBlock:
        """ Dereference this to a namedtuple so that we can pass it around
        without any thread inconsistencies
        """
        return DereferencedBlock(
            uid=self.uid,
            version_uid=self.version_uid,
            id=self.id,
            checksum=self.checksum,
            size=self.size,
            valid=self.valid,
        )


class DeletedBlock(Base):
    __tablename__ = 'deleted_blocks'

    REPR_SQL_ATTR_SORT_FIRST = ['id']

    date = Column("date", BenjiDateTime, nullable=False)
    # BigInteger as the id could get large over time
    # Use INTEGER with SQLLite to get AUTOINCREMENT and the INTEGER type of SQLLite can store huge values anyway.
    id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True, nullable=False)
    storage_id = Column(Integer, nullable=False)
    uid_left = Column(Integer, nullable=False)
    uid_right = Column(Integer, nullable=False)

    uid = composite(BlockUid, uid_left, uid_right, comparator_factory=BlockUidComparator)
    __table_args__ = (Index(None, 'uid_left', 'uid_right'), {'sqlite_autoincrement': True})


class Lock(Base):
    __tablename__ = 'locks'

    REPR_SQL_ATTR_SORT_FIRST = ['host', 'process_id', 'date']

    lock_name = Column(String(255), nullable=False, primary_key=True)
    host = Column(String(255), nullable=False)
    process_id = Column(String(255), nullable=False)
    reason = Column(String(255), nullable=False)
    date = Column(BenjiDateTime, nullable=False)


class DatabaseBackend(ReprMixIn):
    _METADATA_VERSION_KEY = 'metadata_version'
    _METADATA_VERSION_REGEX = r'\d+\.\d+\.\d+'
    _BLOCKS_COMMIT_INTERVAL = 20  # in seconds

    _locking = None

    def __init__(self, config: Config, in_memory: bool = False) -> None:
        if not in_memory:
            url = config.get('databaseEngine', types=str)
            connect_args = {}
            if url.startswith('sqlite:'):
                # This tries to work around a SQLite design limitation. It's best to use PostgreSQL if you're affected
                # by this as it doesn't have this limitation.
                # Also see https://github.com/elemental-lf/benji/issues/11.
                # Increase the timeout (5 seconds is the default). This will make "database is locked" errors
                # due to concurrent database access less likely.
                connect_args['timeout'] = 3 * self._BLOCKS_COMMIT_INTERVAL
            self._engine = sqlalchemy.create_engine(url, connect_args=connect_args)
        else:
            logger.info('Running with ephemeral in-memory database.')
            self._engine = sqlalchemy.create_engine('sqlite://')

    def _alembic_config(self):
        return alembic_config_Config(
            os.path.join(os.path.dirname(os.path.realpath(__file__)), "sql_migrations", "alembic.ini"))

    def _database_tables(self) -> List[str]:
        # Need to ignore internal SQLite table here
        return [table for table in self._engine.table_names() if table != 'sqlite_sequence']

    def _migration_needed(self, alembic_config: alembic_config_Config) -> Tuple[bool, str, str]:
        with self._engine.begin() as connection:
            alembic_config.attributes['connection'] = connection
            script = ScriptDirectory.from_config(alembic_config)
            with EnvironmentContext(alembic_config, script) as env_context:
                env_context.configure(connection, version_table="alembic_version")
                head_revision = env_context.get_head_revision()
                migration_context = env_context.get_context()
                current_revision = migration_context.get_current_revision()

        if current_revision is None:
            current_revision = '<unknown>'

        logger.debug('Current database schema revision: {}.'.format(current_revision))
        logger.debug('Expected database schema revision: {}.'.format(head_revision))

        return ((head_revision != current_revision), current_revision, head_revision)

    def migrate(self) -> None:
        table_names = self._database_tables()
        if not table_names:
            raise RuntimeError('Database schema appears to be empty. Not touching anything.')

        alembic_config = self._alembic_config()
        migration_needed, current_revision, head_revision = self._migration_needed(alembic_config)
        if migration_needed:
            logger.info('Migrating from database schema revision {} to {}.'.format(current_revision, head_revision))
            with self._engine.begin() as connection:
                alembic_config.attributes['connection'] = connection
                alembic_command.upgrade(alembic_config, "head")
        else:
            logger.info('Current database schema revision: {}.'.format(current_revision))
            logger.info('The database schema is up-to-date.')

    def open(self) -> 'DatabaseBackend':
        alembic_config = self._alembic_config()
        migration_needed, current_revision, head_revision = self._migration_needed(alembic_config)
        if migration_needed:
            logger.info('Current database schema revision: {}.'.format(current_revision))
            logger.info('Expected database schema revision: {}.'.format(head_revision))
            raise RuntimeError('The database schema requires migration.')

        # SQLite 3 supports checking of foreign keys but it needs to be enabled explicitly!
        # See: http://docs.sqlalchemy.org/en/latest/dialects/sqlite.html#foreign-key-support
        @event.listens_for(Engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            if isinstance(dbapi_connection, sqlite3.Connection):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

        Session = sessionmaker(bind=self._engine)
        self._session = Session()
        self._locking = DatabaseBackendLocking(self._session)
        self._last_blocks_commit = time.monotonic()
        return self

    def init(self, _destroy: bool = False) -> None:
        # This is dangerous and is only used by the test suite to get a clean slate
        if _destroy:
            Base.metadata.drop_all(self._engine)
            # Drop alembic_version table
            if self._engine.has_table('alembic_version'):
                with self._engine.begin() as connection:
                    connection.execute(DropTable(Table('alembic_version', MetaData())))  # type: ignore

        table_names = self._database_tables()
        if not table_names:
            Base.metadata.create_all(self._engine, checkfirst=False)
        else:
            logger.debug('Existing tables: {}'.format(', '.join(sorted(table_names))))
            raise FileExistsError('Database schema contains tables already. Not touching anything.')

        alembic_config = self._alembic_config()
        with self._engine.begin() as connection:
            alembic_config.attributes['connection'] = connection
            alembic_command.stamp(alembic_config, "head")

    def commit(self) -> None:
        self._session.commit()

    def create_version(self,
                       version_name: str,
                       snapshot_name: str,
                       size: int,
                       storage_id: int,
                       block_size: int,
                       status: VersionStatus = VersionStatus.incomplete,
                       protected: bool = False) -> Version:
        version = Version(
            name=version_name,
            snapshot_name=snapshot_name,
            size=size,
            storage_id=storage_id,
            block_size=block_size,
            status=status,
            protected=protected,
            date=datetime.datetime.utcnow(),
        )
        try:
            self._session.add(version)
            self._session.commit()
        except:
            self._session.rollback()
            raise

        return version

    def set_stats(self, *, uid: VersionUid, base_uid: Optional[VersionUid], hints_supplied: bool,
                  date: datetime.datetime, name: str, snapshot_name: str, size: int, storage_id: int, block_size: int,
                  bytes_read: int, bytes_written: int, bytes_dedup: int, bytes_sparse: int, duration: int) -> None:
        stats = VersionStatistic(
            uid=uid,
            base_uid=base_uid,
            hints_supplied=hints_supplied,
            date=date,
            name=name,
            snapshot_name=snapshot_name,
            size=size,
            storage_id=storage_id,
            block_size=block_size,
            bytes_read=bytes_read,
            bytes_written=bytes_written,
            bytes_dedup=bytes_dedup,
            bytes_sparse=bytes_sparse,
            duration=duration,
        )
        try:
            self._session.add(stats)
            self._session.commit()
        except:
            self._session.rollback()
            raise

    def get_stats_with_filter(self, filter_expression: str = None, limit: int = None) -> List[VersionStatistic]:
        builder = _QueryBuilder(self._session, VersionStatistic)
        try:
            stats = builder.build(filter_expression)
            if limit:
                stats = stats.limit(limit)
            stats_result = stats.all()
        except:
            self._session.rollback()
            raise

        return list(reversed(stats_result))

    def set_version(self, version_uid: VersionUid, *, status: VersionStatus = None, protected: bool = None):
        try:
            version = self.get_version(version_uid)
            if status is not None:
                version.status = status
            if protected is not None:
                version.protected = protected
            self._session.commit()
            if status is not None:
                logger_func = logger.info if version.status.is_valid() else logger.error
                logger_func('Set status of version {} to {}.'.format(version_uid.v_string, version.status.name))
            if protected is not None:
                logger.info('Marked version {} as {}.'.format(version_uid.v_string,
                                                              'protected' if protected else 'unprotected'))
        except:
            self._session.rollback()
            raise

    def get_version(self, version_uid: VersionUid) -> Version:
        version = None
        try:
            version = self._session.query(Version).filter_by(uid=version_uid).first()
        except:
            self._session.rollback()

        if version is None:
            raise KeyError('Version {} not found.'.format(version_uid))

        return version

    def get_versions(self,
                     version_uid: VersionUid = None,
                     version_name: str = None,
                     version_snapshot_name: str = None,
                     version_labels: List[Tuple[str, str]] = None) -> List[Version]:
        try:
            query = self._session.query(Version)
            if version_uid:
                query = query.filter_by(uid=version_uid)
            if version_name:
                query = query.filter_by(name=version_name)
            if version_snapshot_name:
                query = query.filter_by(snapshot_name=version_snapshot_name)
            if version_labels:
                for version_label in version_labels:
                    label_query = self._session.query(
                        Label.version_uid).filter((Label.name == version_label[0]) & (Label.value == version_label[1]))
                    query = query.filter(Version.uid.in_(label_query))
            versions = query.order_by(Version.name, Version.date).all()
        except:
            self._session.rollback()
            raise

        return versions

    def get_versions_with_filter(self, filter_expression: str = None):
        builder = _QueryBuilder(self._session, Version)
        try:
            versions = builder.build(filter_expression).order_by(Version.name, Version.date).all()
        except:
            self._session.rollback()
            raise

        return versions

    def add_label(self, version_uid: VersionUid, name: str, value: str) -> None:
        try:
            label = self._session.query(Label).filter_by(version_uid=version_uid, name=name).first()
            if label:
                label.value = value
            else:
                label = Label(version_uid=version_uid, name=name, value=value)
                self._session.add(label)

            self._session.commit()
        except:
            self._session.rollback()
            raise

    def rm_label(self, version_uid: VersionUid, name: str) -> None:
        try:
            deleted = self._session.query(Label).filter_by(version_uid=version_uid, name=name).delete()
            self._session.commit()
        except:
            self._session.rollback()
            raise

    def set_block(self,
                  *,
                  id: int,
                  version_uid: VersionUid,
                  block_uid: Optional[BlockUid],
                  checksum: Optional[str],
                  size: int,
                  valid: bool,
                  upsert: bool = True) -> None:
        try:
            block = None
            if upsert:
                block = self._session.query(Block).filter_by(id=id, version_uid=version_uid).first()

            if block:
                block.uid = block_uid
                block.checksum = checksum
                block.size = size
                block.valid = valid
            else:
                block = Block(
                    id=id,
                    version_uid=version_uid,
                    uid=block_uid,
                    checksum=checksum,
                    size=size,
                    valid=valid,
                )
                self._session.add(block)

            current_clock = time.monotonic()
            if current_clock - self._last_blocks_commit > self._BLOCKS_COMMIT_INTERVAL:
                t1 = time.time()
                self._session.commit()
                t2 = time.time()
                logger.debug('Commited database transaction in set_block in {:.2f}s'.format(t2 - t1))
                self._last_blocks_commit = current_clock
        except:
            self._session.rollback()
            raise

    def set_block_invalid(self, block_uid: BlockUid) -> List[VersionUid]:
        try:
            affected_version_uids = self._session.query(distinct(Block.version_uid)).filter_by(uid=block_uid).all()
            affected_version_uids = [version_uid[0] for version_uid in affected_version_uids]
            self._session.query(Block).filter_by(uid=block_uid).update({'valid': False}, synchronize_session='fetch')
            self._session.commit()

            logger.error('Marked block with UID {} as invalid. Affected versions: {}.'.format(
                block_uid, ', '.join([version_uid.v_string for version_uid in affected_version_uids])))

            for version_uid in affected_version_uids:
                self.set_version(version_uid, status=VersionStatus.invalid)
            self._session.commit()
        except:
            self._session.rollback()
            raise

        return affected_version_uids

    def get_block(self, block_uid: BlockUid) -> Block:
        try:
            block = self._session.query(Block).filter_by(uid=block_uid).first()
        except:
            self._session.rollback()
            raise

        return block

    def get_block_by_checksum(self, checksum, storage_id):
        try:
            block = self._session.query(Block).filter_by(
                checksum=checksum, valid=True).join(Version).filter_by(storage_id=storage_id).first()
        except:
            self._session.rollback()
            raise

        return block

    def get_blocks_by_version(self, version_uid: VersionUid) -> List[Block]:
        try:
            blocks = self._session.query(Block).filter_by(version_uid=version_uid).order_by(Block.id).all()
        except:
            self._session.rollback()
            raise

        return blocks

    def rm_version(self, version_uid: VersionUid) -> int:
        try:
            version = self._session.query(Version).filter_by(uid=version_uid).first()
            affected_blocks = self._session.query(Block).filter_by(version_uid=version.uid)
            num_blocks = affected_blocks.count()
            for affected_block in affected_blocks:
                if affected_block.uid:
                    deleted_block = DeletedBlock(
                        storage_id=version.storage_id,
                        uid=affected_block.uid,
                        date=datetime.datetime.utcnow(),
                    )
                    self._session.add(deleted_block)
            # The following delete statement will cascade this delete to the blocks table
            # and delete all blocks
            self._session.query(Version).filter_by(uid=version_uid).delete()
            self._session.commit()
        except:
            self._session.rollback()
            raise

        return num_blocks

    def get_delete_candidates(self, dt: int = 3600) -> Iterator[Dict[int, Set[BlockUid]]]:
        rounds = 0
        false_positives_count = 0
        hit_list_count = 0
        cut_off_date = datetime.datetime.utcnow() - datetime.timedelta(seconds=dt)
        while True:
            # http://stackoverflow.com/questions/7389759/memory-efficient-built-in-sqlalchemy-iterator-generator
            delete_candidates = self._session.query(DeletedBlock)\
                .filter(DeletedBlock.date < cut_off_date)\
                .limit(250)\
                .all()
            if not delete_candidates:
                break

            false_positives = set()
            hit_list: Dict[int, Set[BlockUid]] = {}
            for candidate in delete_candidates:
                rounds += 1
                if rounds % 1000 == 0:
                    logger.info("Cleanup: {} false positives, {} data deletions.".format(
                        false_positives_count,
                        hit_list_count,
                    ))

                block = self._session.query(Block)\
                    .filter(Block.uid == candidate.uid)\
                    .limit(1)\
                    .scalar()
                if block:
                    false_positives.add(candidate.uid)
                    false_positives_count += 1
                else:
                    if candidate.storage_id not in hit_list:
                        hit_list[candidate.storage_id] = set()
                    hit_list[candidate.storage_id].add(candidate.uid)
                    hit_list_count += 1

            if false_positives:
                logger.debug("Cleanup: Removing {} false positive from delete candidates.".format(len(false_positives)))
                self._session.query(DeletedBlock)\
                    .filter(DeletedBlock.uid.in_(false_positives))\
                    .delete(synchronize_session=False)

            if hit_list:
                for uids in hit_list.values():
                    self._session.query(DeletedBlock).filter(
                        DeletedBlock.uid.in_(uids)).delete(synchronize_session=False)
                yield hit_list
                # We expect that the caller has handled all the blocks returned so far, so we can call commit after
                # the yield to keep the transaction small.
                self._session.commit()

        self._session.commit()
        logger.info("Cleanup: Cleanup finished. {} false positives, {} data deletions.".format(
            false_positives_count,
            hit_list_count,
        ))

    # Based on: https://stackoverflow.com/questions/5022066/how-to-serialize-sqlalchemy-result-to-json/7032311,
    # https://stackoverflow.com/questions/1958219/convert-sqlalchemy-row-object-to-python-dict
    @staticmethod
    def new_benji_encoder(ignore_fields: List, ignore_relationships: List):

        class BenjiEncoder(json.JSONEncoder):

            def default(self, obj):
                if isinstance(obj.__class__, DeclarativeMeta):
                    fields = {}

                    for field in inspect(obj).mapper.composites:
                        ignore = False
                        for types, names in ignore_fields:
                            if isinstance(obj, types) and field.key in names:
                                ignore = True
                                break
                        if not ignore:
                            fields[field.key] = getattr(obj, field.key)

                    for field in inspect(obj).mapper.column_attrs:
                        ignore = False
                        for types, names in ignore_fields:
                            if isinstance(obj, types) and field.key in names:
                                ignore = True
                                break
                        if not ignore:
                            fields[field.key] = getattr(obj, field.key)

                    for relationship in inspect(obj).mapper.relationships:
                        ignore = False
                        for types, names in ignore_relationships:
                            if isinstance(obj, types) and relationship.key in names:
                                ignore = True
                                break
                        if not ignore:
                            fields[relationship.key] = getattr(obj, relationship.key)

                    return fields

                if isinstance(obj, datetime.datetime):
                    return obj.isoformat(timespec='microseconds')
                elif isinstance(obj, VersionUid):
                    return obj.integer
                elif isinstance(obj, BlockUid):
                    return {'left': obj.left, 'right': obj.right}
                elif isinstance(obj, VersionStatus):
                    return obj.name

                return super().default(obj)

        return BenjiEncoder

    def export_any(self, root_dict: Dict, f: TextIO, ignore_fields: List = None,
                   ignore_relationships: List = None) -> None:
        ignore_fields = list(ignore_fields) if ignore_fields is not None else []
        ignore_relationships = list(ignore_relationships) if ignore_relationships is not None else []

        # These are always ignored because they'd lead to a circle
        ignore_fields.append(((Label, Block), ('version_uid',)))
        ignore_relationships.append(((Label, Block), ('version',)))
        # Ignore these as we favor the composite attribute
        ignore_fields.append(((Block,), ('uid_left', 'uid_right')))

        root_dict = root_dict.copy()
        root_dict[self._METADATA_VERSION_KEY] = str(VERSIONS.database_metadata.current)

        json.dump(
            root_dict,
            f,
            cls=self.new_benji_encoder(ignore_fields, ignore_relationships),
            check_circular=True,
            indent=2,
        )

    def export(self, version_uids: Sequence[VersionUid], f: TextIO):
        self.export_any({'versions': [self.get_version(version_uid) for version_uid in version_uids]}, f)

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
            version_uids = import_method(json_input)
            self._session.commit()
        except:
            self._session.rollback()
            raise

        return version_uids

    def import_v1(self, json_input: Dict) -> List[VersionUid]:
        version_uids: List[VersionUid] = []
        for version_dict in json_input['versions']:
            if not isinstance(version_dict, dict):
                raise InputDataError('Wrong data type for versions list element.')

            if 'uid' not in version_dict:
                raise InputDataError('Missing attribute uid in version.')

            # Will raise ValueError when invalid
            version_uid = VersionUid(version_dict['uid'])

            for attribute in [
                    'date',
                    'name',
                    'snapshot_name',
                    'size',
                    'storage_id',
                    'block_size',
                    'status',
                    'protected',
                    'blocks',
                    'labels',
            ]:
                if attribute not in version_dict:
                    raise InputDataError('Missing attribute {} in version {}.'.format(attribute, version_uid.v_string))

            if not InputValidation.is_backup_name(version_dict['name']):
                raise InputDataError('Backup name {} in version {} is invalid.'.format(
                    version_dict['name'], version_uid.v_string))

            if not InputValidation.is_snapshot_name(version_dict['snapshot_name']):
                raise InputDataError('Snapshot name {} in version {} is invalid.'.format(
                    version_dict['snapshot_name'], version_uid.v_string))

            if not isinstance(version_dict['labels'], list):
                raise InputDataError('Wrong data type for labels in version {}.'.format(version_uid.v_string))

            if not isinstance(version_dict['blocks'], list):
                raise InputDataError('Wrong data type for blocks in version {}.'.format(version_uid.v_string))

            for label_dict in version_dict['labels']:
                if not isinstance(label_dict, dict):
                    raise InputDataError('Wrong data type for labels list element in version {}.'.format(
                        version_uid.v_string))
                for attribute in ['name', 'value']:
                    if attribute not in label_dict:
                        raise InputDataError('Missing attribute {} in labels list in version {}.'.format(
                            attribute, version_uid.v_string))
                if not InputValidation.is_label_name(label_dict['name']):
                    raise InputDataError('Label name {} in labels list in version {} is invalid.'.format(
                        label_dict['name'], version_uid.v_string))
                if not InputValidation.is_label_value(label_dict['value']):
                    raise InputDataError('Label value {} in labels list in version {} is invalid.'.format(
                        label_dict['value'], version_uid.v_string))

            for block_dict in version_dict['blocks']:
                if not isinstance(block_dict, dict):
                    raise InputDataError('Wrong data type for blocks list element in version {}.'.format(
                        version_uid.v_string))
                for attribute in ['id', 'uid', 'size', 'valid', 'checksum']:
                    if attribute not in block_dict:
                        raise InputDataError('Missing attribute {} in blocks list in version {}.'.format(
                            attribute, version_uid.v_string))

            try:
                self.get_version(version_dict['uid'])
            except KeyError:
                pass  # does not exist
            else:
                raise FileExistsError('Version {} already exists and so cannot be imported.'.format(
                    version_uid.v_string))

            version = Version(
                uid=version_uid,
                date=datetime.datetime.strptime(version_dict['date'], '%Y-%m-%dT%H:%M:%S.%f'),
                name=version_dict['name'],
                snapshot_name=version_dict['snapshot_name'],
                size=version_dict['size'],
                storage_id=version_dict['storage_id'],
                block_size=version_dict['block_size'],
                status=VersionStatus[version_dict['status']],
                protected=version_dict['protected'],
            )
            self._session.add(version)
            self._session.flush()

            for block_dict in version_dict['blocks']:
                block_dict['version_uid'] = version_uid
                block_uid = BlockUid(block_dict['uid']['left'], block_dict['uid']['right'])
                block_dict['uid_left'] = block_uid.left
                block_dict['uid_right'] = block_uid.right
                del block_dict['uid']

            self._session.bulk_insert_mappings(Block, version_dict['blocks'])

            for label_dict in version_dict['labels']:
                label_dict['version_uid'] = version_uid
            self._session.bulk_insert_mappings(Label, version_dict['labels'])

            version_uids.append(version_uid)

        return version_uids

    def locking(self):
        return self._locking

    def close(self):
        self._session.commit()
        self._locking.unlock_all()
        self._locking = None
        self._session.close()
        self._session = None
        self._engine.dispose()


class DatabaseBackendLocking:

    def __init__(self, session) -> None:
        self._session = session
        self._host = platform.node()
        self._uuid = uuid.uuid1().hex

    def lock(self, *, lock_name: str, reason: str = None, locked_msg: str = None, override_lock: bool = False):
        try:
            lock = self._session.query(Lock).filter_by(
                host=self._host, lock_name=lock_name, process_id=self._uuid).first()
            if lock is not None:
                raise InternalError('Attempt to acquire lock {} twice.'.format(lock_name))
            lock = Lock(
                lock_name=lock_name,
                host=self._host,
                process_id=self._uuid,
                reason=reason,
                date=datetime.datetime.utcnow(),
            )
            if override_lock:
                logger.warn('Will override any existing lock.')
                self._session.merge(lock, load=True)
            else:
                self._session.add(lock)
            self._session.commit()
        except IntegrityError:
            self._session.rollback()
            if locked_msg is not None:
                raise AlreadyLocked(locked_msg) from None
            else:
                raise AlreadyLocked('Lock {} is already taken.'.format(lock_name)) from None
        except:
            self._session.rollback()
            raise

    def is_locked(self, *, lock_name: str) -> bool:
        try:
            lock = self._session.query(Lock).filter_by(lock_name=lock_name).first()
        except:
            self._session.rollback()
            raise
        else:
            return lock is not None

    def update_lock(self, *, lock_name: str, reason: str = None) -> None:
        try:
            lock = self._session.query(Lock).filter_by(
                host=self._host, lock_name=lock_name, process_id=self._uuid).with_for_update().first()
            if not lock:
                raise InternalError('Lock {} isn\'t held by this instance or doesn\'t exist.'.format(lock_name))
            lock.reason = reason
            self._session.commit()
        except:
            self._session.rollback()
            raise

    def unlock(self, *, lock_name: str) -> None:
        try:
            lock = self._session.query(Lock).filter_by(
                host=self._host, lock_name=lock_name, process_id=self._uuid).first()
            if not lock:
                raise InternalError('Lock {} isn\'t held by this instance or doesn\'t exist.'.format(lock_name))
            self._session.delete(lock)
            self._session.commit()
        except:
            self._session.rollback()
            raise

    def unlock_all(self) -> None:
        try:
            locks = self._session.query(Lock).filter_by(host=self._host, process_id=self._uuid).all()
            for lock in locks:
                logger.error('Lock {} not released correctly, releasing it now.'.format(lock.lock_name))
                self._session.delete(lock)
            self._session.commit()
        except:
            pass

    def lock_version(self, version_uid: VersionUid, reason: str = None, override_lock: bool = False) -> None:
        self.lock(
            lock_name=version_uid.v_string,
            reason=reason,
            locked_msg='Version {} is already locked.'.format(version_uid.v_string),
            override_lock=override_lock)

    def is_version_locked(self, version_uid: VersionUid) -> bool:
        return self.is_locked(lock_name=version_uid.v_string)

    def update_version_lock(self, version_uid: VersionUid, reason: str = None) -> None:
        self.update_lock(lock_name=version_uid.v_string, reason=reason)

    def unlock_version(self, version_uid: VersionUid) -> None:
        self.unlock(lock_name=version_uid.v_string)

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


class _QueryBuilder:

    def __init__(self, session, orm_class: Base) -> None:
        self._session = session
        self._orm_class = orm_class
        self._parser = self._define_parser(session, orm_class)

    @staticmethod
    def _define_parser(session, orm_class: Base) -> Any:

        class Buildable:

            @abstractmethod
            def build(self) -> ColumnElement:
                raise NotImplementedError()

        class Token(Buildable):
            pass

        class IdentifierToken(Token):

            def __init__(self, name: str) -> None:
                self.name = name

            def op(self, op: Callable[[Any, Any], BinaryExpression], other: Any) -> BinaryExpression:
                if isinstance(other, IdentifierToken):
                    return op(getattr(orm_class, self.name), getattr(orm_class, other.name))
                elif isinstance(other, Token):
                    raise TypeError('Comparing identifiers to labels is not supported.')
                else:
                    return op(getattr(orm_class, self.name), other)

            # See https://github.com/python/mypy/issues/2783 for the reason of type: ignore
            def __eq__(self, other: Any) -> BinaryExpression:  # type: ignore
                return self.op(operator.eq, other)

            def __ne__(self, other: Any) -> BinaryExpression:  # type: ignore
                return self.op(operator.ne, other)

            def __lt__(self, other: Any) -> BinaryExpression:
                return self.op(operator.lt, other)

            def __le__(self, other: Any) -> BinaryExpression:
                return self.op(operator.le, other)

            def __gt__(self, other: Any) -> BinaryExpression:
                return self.op(operator.gt, other)

            def __ge__(self, other: Any) -> BinaryExpression:
                return self.op(operator.ge, other)

            # This is called when the token is not part of a comparison and tests for a non-empty identifier
            def build(self) -> BinaryExpression:
                return getattr(orm_class, self.name) != ''

        class LabelToken(Token):

            def __init__(self, name: str) -> None:
                self.name = name

            def op(self, op, other: Any) -> BinaryExpression:
                if isinstance(other, Token):
                    raise TypeError('Comparing labels to labels or labels to identifiers is not supported.')
                label_query = session.query(
                    Label.version_uid).filter((Label.name == self.name) & op(Label.value, str(other)))
                return getattr(orm_class, 'uid').in_(label_query)

            # See https://github.com/python/mypy/issues/2783 for the reason of type: ignore
            def __eq__(self, other: Any) -> BinaryExpression:  # type: ignore
                return self.op(operator.eq, other)

            def __ne__(self, other: Any) -> BinaryExpression:  # type: ignore
                return self.op(operator.ne, other)

            # This is called when the token is not part of a comparison and test for label existence
            def build(self) -> BinaryExpression:
                label_query = session.query(Label.version_uid).filter(Label.name == self.name)
                return getattr(orm_class, 'uid').in_(label_query)

        attributes = []
        for attribute in inspect(orm_class).mapper.composites:
            attributes.append(attribute.key)

        for attribute in inspect(orm_class).mapper.column_attrs:
            attributes.append(attribute.key)

        identifier = Regex('|'.join(attributes)).setParseAction(lambda s, l, t: IdentifierToken(t[0]))
        integer = pyparsing_common.signed_integer
        string = quotedString().setParseAction(removeQuotes)
        bool_true = Keyword('True').setParseAction(replaceWith(True))
        bool_false = Keyword('False').setParseAction(replaceWith(False))

        if 'labels' in inspect(orm_class).mapper.relationships:
            label = (Literal('labels') + Literal('[') + string + Literal(']')).setParseAction(lambda s, l, t: LabelToken(t[2]))
        else:
            label = NoMatch()

        atom = identifier | integer | string | bool_true | bool_false | label

        class BinaryOp(Buildable):

            op: Optional[Callable[[Any, Any], BooleanClauseList]] = None

            def __init__(self, t) -> None:
                assert len(t[0]) == 3
                self.args = t[0][0::2]

            def build(self) -> BooleanClauseList:
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
                        raise ParseFatalException('Operands of boolean and must be expressions, identifier or label references.')
                self.args = args

            def build(self) -> BooleanClauseList:
                assert self.op is not None
                # __func__ is necessary to call op as a function instead of as a method
                return self.op.__func__(*map(lambda token: token.build(), self.args))

        class AndOp(MultiaryOp):
            op = and_

        class OrOp(MultiaryOp):
            op = or_

        class NotOp(Buildable):

            def __init__(self, t) -> None:
                self.args = [t[0][1]]

            def build(self) -> BooleanClauseList:
                return not_(self.args[0].build())

        return infixNotation(atom, [
            ("==", 2, opAssoc.LEFT, EqOp),
            ("!=", 2, opAssoc.LEFT, NeOp),
            ("<=", 2, opAssoc.LEFT, LeOp),
            (">=", 2, opAssoc.LEFT, GeOp),
            ("<", 2, opAssoc.LEFT, LtOp),
            (">", 2, opAssoc.LEFT, GtOp),
            ("not", 1, opAssoc.RIGHT, NotOp),
            ("and", 2, opAssoc.LEFT, AndOp),
            ("or", 2, opAssoc.LEFT, OrOp),
        ])

    def build(self, filter_expression: Optional[str]):
        query = self._session.query(self._orm_class)
        if filter_expression:
            try:
                parsed_filter_expression = self._parser.parseString(filter_expression, parseAll=True)[0]
            except (ParseException, ParseFatalException) as exception:
                raise UsageError('Invalid filter expression {}.'.format(filter_expression)) from exception
            try:
                filter_result = parsed_filter_expression.build()
            except (AttributeError, TypeError) as exception:
                # User supplied only a constant or is trying to compare apples to oranges
                raise UsageError('Invalid filter expression {} (2).'.format(filter_expression)) from exception
            if not isinstance(filter_result, ColumnElement):
                # Expression doesn't contain at least one expression with references to a SQL column
                raise UsageError('Invalid filter expression {} (3).'.format(filter_expression))
            query = query.filter(filter_result)
        return query
