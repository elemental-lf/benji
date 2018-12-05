#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import datetime
import json
import os
import platform
import re
import sqlite3
import time
import uuid
from binascii import hexlify, unhexlify
from contextlib import contextmanager
from typing import Union, List, Tuple, TextIO, Dict, cast, Generator, Iterator, Set, Any, Optional, Sequence

import sqlalchemy
from sqlalchemy import Column, String, Integer, BigInteger, ForeignKey, LargeBinary, Boolean, inspect, event, Index, \
    DateTime
from sqlalchemy import distinct, desc
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.ext.mutable import MutableComposite
from sqlalchemy.orm import sessionmaker, composite, CompositeProperty
from sqlalchemy.types import TypeDecorator

from benji.config import Config
from benji.exception import InputDataError, InternalError, NoChange, AlreadyLocked
from benji.logging import logger
from benji.storage.key import StorageKeyMixIn


class VersionUid(StorageKeyMixIn['VersionUid']):

    def __init__(self, value) -> None:
        self._value = value

    @staticmethod
    def create_from_readables(
            readables: Union[None, List, Tuple, str, int]) -> Union[None, List['VersionUid'], 'VersionUid']:
        if readables is None:
            return None
        input_is_list = isinstance(readables, (list, tuple))
        if not input_is_list:
            readables_list = cast(Union[List, Tuple], [readables])
        else:
            readables_list = cast(Union[List, Tuple], readables)
        version_uids = []
        for readable in readables_list:
            if isinstance(readable, int):
                pass
            elif isinstance(readable, str):
                try:
                    readable = int(readable)
                except ValueError:
                    if len(readable) < 2:
                        raise ValueError('Version UID {} is too short.'.format(readable))
                    if readable[0].lower() != 'v':
                        raise ValueError('Version UID {} doesn\'t start with the letter V.'.format(readable))
                    try:
                        readable = int(readable[1:])
                    except ValueError:
                        raise ValueError('Version UID {} is invalid.'.format(readable)) from None
            else:
                raise ValueError('Version UID {} has unsupported type {}.'.format(str(readable), type(readable)))
            version_uids.append(VersionUid(readable))
        return version_uids if input_is_list else version_uids[0]

    @property
    def to_int(self) -> int:
        return self._value

    @property
    def readable(self) -> str:
        return 'V' + str(self._value).zfill(10)

    def __repr__(self) -> str:
        return self.readable

    def __eq__(self, other) -> bool:
        if isinstance(other, VersionUid):
            return self.to_int == other.to_int
        elif isinstance(other, int):
            return self.to_int == other
        else:
            return False

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return self.to_int

    # Start: Implements StorageKeyMixIn
    _STORAGE_PREFIX = 'versions/'

    @classmethod
    def storage_prefix(cls) -> str:
        return cls._STORAGE_PREFIX

    def _storage_object_to_key(self) -> str:
        return self.readable

    @classmethod
    def _storage_key_to_object(cls, key: str) -> 'VersionUid':
        vl = len(VersionUid(1).readable)
        if len(key) != vl:
            raise RuntimeError('Object key {} has an invalid length, expected exactly {} characters.'.format(key, vl))
        return cast(Version, VersionUid.create_from_readables(key))

    # End: Implements StorageKeyMixIn


class VersionUidType(TypeDecorator):

    impl = Integer

    def process_bind_param(self, value: Union[None, int, str, VersionUid], dialect) -> Union[None, int]:
        if value is None:
            return None
        elif isinstance(value, int):
            return value
        elif isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                raise InternalError(
                    'Supplied string value "{}" represents no integer VersionUidType.process_bind_param'.format(value)) from None
        elif isinstance(value, VersionUid):
            return value.to_int
        else:
            raise InternalError('Unexpected type {} for value in VersionUidType.process_bind_param'.format(type(value)))

    def process_result_value(self, value: int, dialect) -> Union[None, VersionUid]:
        if value is not None:
            return VersionUid(value)
        else:
            return None


class Checksum(TypeDecorator):

    impl = LargeBinary

    def process_bind_param(self, value: Union[None, str], dialect) -> Union[None, bytes]:
        if value is not None:
            return unhexlify(value)
        else:
            return None

    def process_result_value(self, value: bytes, dialect) -> Union[None, str]:
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


class BlockUid(MutableComposite, StorageKeyMixIn['BlockUid']):

    def __init__(self, left: int, right: int) -> None:
        self.left = left
        self.right = right

    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)
        self.changed()

    def __composite_values__(self) -> Tuple[int, int]:
        return self.left, self.right

    def __repr__(self) -> str:
        return "{:x}-{:x}".format(self.left if self.left is not None else 0, self.right if self.right is not None else 0)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, BlockUid) and \
               other.left == self.left and \
               other.right == self.right

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __bool__(self) -> bool:
        return self.left is not None and self.right is not None

    def __hash__(self) -> int:
        return hash((self.left, self.right))

    # For sorting
    def __lt__(self, other: 'BlockUid') -> bool:
        return self.left < other.left or self.left == other.left and self.right < other.right

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


Base: Any = declarative_base()


class Stats(Base):
    __tablename__ = 'stats'
    # No foreign key references here, so that we can keep the stats around even when the version is deleted
    version_uid = Column(VersionUidType, primary_key=True)
    base_version_uid = Column(VersionUidType, nullable=True)
    hints_supplied = Column(Boolean, nullable=False)
    version_name = Column(String, nullable=False)
    version_date = Column("date", DateTime, nullable=False)
    version_snapshot_name = Column(String, nullable=False)
    version_size = Column(BigInteger, nullable=False)
    version_storage_id = Column(Integer, nullable=False)
    version_block_size = Column(BigInteger, nullable=False)
    bytes_read = Column(BigInteger, nullable=False)
    bytes_written = Column(BigInteger, nullable=False)
    bytes_dedup = Column(BigInteger, nullable=False)
    bytes_sparse = Column(BigInteger, nullable=False)
    duration_seconds = Column(BigInteger, nullable=False)


class Version(Base):
    __tablename__ = 'versions'
    # This makes sure that SQLite won't reuse UIDs
    __table_args__ = {'sqlite_autoincrement': True}
    uid = Column(VersionUidType, primary_key=True, nullable=False)
    date = Column("date", DateTime, nullable=False)
    name = Column(String, nullable=False, default='', index=True)
    snapshot_name = Column(String, nullable=False, server_default='', default='')
    size = Column(BigInteger, nullable=False)
    block_size = Column(Integer, nullable=False)
    storage_id = Column(Integer, nullable=False)
    valid = Column(Boolean, nullable=False)
    protected = Column(Boolean, nullable=False)

    tags = sqlalchemy.orm.relationship(
        'Tag',
        backref='version',
        order_by='asc(Tag.name)',
        passive_deletes=True,
    )

    blocks = sqlalchemy.orm.relationship(
        'Block',
        backref='version',
        order_by='asc(Block.id)',
        passive_deletes=True,
    )

    def __repr__(self) -> str:
        return "<Version(uid='%s', name='%s', snapshot_name='%s', date='%s')>" % (self.uid, self.name, self.snapshot_name, self.date)


class Tag(Base):
    __tablename__ = 'tags'
    version_uid = Column(
        VersionUidType, ForeignKey('versions.uid', ondelete='CASCADE'), primary_key=True, nullable=False)
    name = Column(String, nullable=False, primary_key=True)

    def __repr__(self) -> str:
        return "<Tag(version_uid='%s', name='%s')>" % (self.version_uid, self.name)


class DereferencedBlock:

    def __init__(self, uid: BlockUid, version_uid: VersionUid, id: int, date: datetime.datetime,
                 checksum: Optional[str], size: int, valid: bool) -> None:
        self.uid = uid
        self.version_uid = version_uid
        self.id = id
        self.date = date
        self.checksum = checksum
        self.size = size
        self.valid = valid

    # Getter and setter need to directly follow each other
    # See https://github.com/python/mypy/issues/1465
    @property
    def uid(self) -> BlockUid:
        return self._uid

    @uid.setter
    def uid(self, uid: BlockUid) -> None:
        if isinstance(uid, BlockUid):
            self._uid = uid
        else:
            raise InternalError('Unexpected type {} for uid in BlockUid.uid.setter'.format(type(uid)))

    @property
    def uid_left(self) -> int:
        return self._uid.left

    @property
    def uid_right(self) -> int:
        return self._uid.right

    def __repr__(self) -> str:
        return "<BlockUid(id='%s', uid='%s', version_uid='%s')>" % (self.id, self.uid, self.version_uid.readable)


class Block(Base):
    __tablename__ = 'blocks'

    MAXIMUM_CHECKSUM_LENGTH = 64

    # Sorted for best alignment to safe space (with PostgreSQL in mind)
    # id and uid_right are first because they are most likely to go to BigInteger in the future
    date = Column("date", DateTime, nullable=False)  # 8 bytes
    id = Column(Integer, primary_key=True, nullable=False)  # 4 bytes
    uid_right = Column(Integer, nullable=True)  # 4 bytes
    uid_left = Column(Integer, nullable=True)  # 4 bytes
    size = Column(Integer, nullable=True)  # 4 bytes
    version_uid = Column(
        VersionUidType, ForeignKey('versions.uid', ondelete='CASCADE'), primary_key=True, nullable=False)  # 4 bytes
    valid = Column(Boolean, nullable=False)  # 1 byte
    checksum = Column(Checksum(MAXIMUM_CHECKSUM_LENGTH), nullable=True)  # 2 to 33 bytes

    uid: BlockUid = cast(BlockUid, composite(BlockUid, uid_left, uid_right, comparator_factory=BlockUidComparator))
    __table_args__ = (
        Index('ix_blocks_uid_left_uid_right', 'uid_left', 'uid_right'),
        # Maybe using an hash index on PostgeSQL might be beneficial in the future
        # Index('ix_blocks_checksum', 'checksum', postgresql_using='hash'),
        Index('ix_blocks_checksum', 'checksum'),
    )

    def deref(self) -> DereferencedBlock:
        """ Dereference this to a namedtuple so that we can pass it around
        without any thread inconsistencies
        """
        return DereferencedBlock(
            uid=self.uid,
            version_uid=self.version_uid,
            id=self.id,
            date=self.date,
            checksum=self.checksum,
            size=self.size,
            valid=self.valid,
        )

    def __repr__(self) -> str:
        return "<Block(id='%s', uid='%s', version_uid='%s')>" % (self.id, self.uid, self.version_uid.readable)


class DeletedBlock(Base):
    __tablename__ = 'deleted_blocks'
    date = Column("date", DateTime, nullable=False)
    # BigInteger as the id could get large over time
    # Use INTEGER with SQLLite to get AUTOINCREMENT and the INTEGER type of SQLLite can store huge values anyway.
    id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True, nullable=False)
    storage_id = Column(Integer, nullable=False)
    uid_left = Column(Integer, nullable=False)
    uid_right = Column(Integer, nullable=False)

    uid = composite(BlockUid, uid_left, uid_right, comparator_factory=BlockUidComparator)
    __table_args__ = (Index('ix_blocks_uid_left_uid_right_2', 'uid_left', 'uid_right'), {'sqlite_autoincrement': True})

    def __repr__(self) -> str:
        return "<DeletedBlock(id='%s', uid='%s')>" % (self.id, self.uid)


class Lock(Base):
    __tablename__ = 'locks'
    host = Column(String, nullable=False, primary_key=True)
    process_id = Column(String, nullable=False, primary_key=True)
    lock_name = Column(String, nullable=False, primary_key=True)
    reason = Column(String, nullable=False)
    date = Column("date", DateTime, nullable=False)

    def __repr__(self) -> str:
        return "<Lock(host='%s' process_id='%s' lock_name='%s')>" % (self.host, self.process_id, self.lock_name)


class MetadataBackend:
    _METADATA_VERSION = '1.0.0'
    _METADATA_VERSION_KEY = 'metadataVersion'
    _METADATA_VERSION_REGEX = '\d+\.\d+\.\d+'
    _COMMIT_EVERY_N_BLOCKS = 1000

    _locking = None

    def __init__(self, config: Config, in_memory: bool = False) -> None:
        if not in_memory:
            self._engine = sqlalchemy.create_engine(config.get('metadataEngine', types=str))
        else:
            logger.info('Running in metadata-backend-less mode.')
            self._engine = sqlalchemy.create_engine('sqlite://')

    def open(self, _migratedb: bool = True) -> 'MetadataBackend':
        if _migratedb:
            try:
                self.migrate_db()
            except Exception:
                raise RuntimeError('Invalid database ({}). Maybe you need to run initdb first?'.format(self._engine.url))

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
        self._locking = MetaBackendLocking(self._session)
        self._commit_block_counter = 0
        return self

    def migrate_db(self) -> None:
        # FIXME: fix to use supplied config
        # migrate the db to the lastest version
        from alembic.config import Config
        from alembic import command
        alembic_cfg = Config(os.path.join(os.path.dirname(os.path.realpath(__file__)), "sql_migrations", "alembic.ini"))
        with self._engine.begin() as connection:
            alembic_cfg.attributes['connection'] = connection
            #command.upgrade(alembic_cfg, "head", sql=True)
            command.upgrade(alembic_cfg, "head")

    def initdb(self, _destroydb: bool = False, _migratedb: bool = True) -> None:
        # This is dangerous and is only used by the test suite to get a clean slate
        if _destroydb:
            Base.metadata.drop_all(self._engine)

        # This will create all tables. It will NOT delete any tables or data.
        # Instead, it will raise when something can't be created.
        # TODO: explicitly check if the database is empty
        Base.metadata.create_all(
            self._engine, checkfirst=False)  # checkfirst False will raise when it finds an existing table

        # FIXME: fix to use supplied config
        if _migratedb:
            from alembic.config import Config
            from alembic import command
            alembic_cfg = Config(
                os.path.join(os.path.dirname(os.path.realpath(__file__)), "sql_migrations", "alembic.ini"))
            with self._engine.begin() as connection:
                alembic_cfg.attributes['connection'] = connection
                # mark the version table, "stamping" it with the most recent rev:
                command.stamp(alembic_cfg, "head")

    def commit(self) -> None:
        self._session.commit()

    def create_version(self,
                       version_name: str,
                       snapshot_name: str,
                       size: int,
                       storage_id: int,
                       block_size: int,
                       valid: bool = False,
                       protected: bool = False) -> Version:
        version = Version(
            name=version_name,
            snapshot_name=snapshot_name,
            size=size,
            storage_id=storage_id,
            block_size=block_size,
            valid=valid,
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

    def set_stats(self, *, version_uid: VersionUid, base_version_uid: Optional[VersionUid], hints_supplied: bool,
                  version_date: datetime.datetime, version_name: str, version_snapshot_name: str, version_size: int,
                  version_storage_id: int, version_block_size: int, bytes_read: int, bytes_written: int,
                  bytes_dedup: int, bytes_sparse: int, duration_seconds: int) -> None:
        stats = Stats(
            version_uid=version_uid,
            base_version_uid=base_version_uid,
            hints_supplied=hints_supplied,
            version_date=version_date,
            version_name=version_name,
            version_snapshot_name=version_snapshot_name,
            version_size=version_size,
            version_storage_id=version_storage_id,
            version_block_size=version_block_size,
            bytes_read=bytes_read,
            bytes_written=bytes_written,
            bytes_dedup=bytes_dedup,
            bytes_sparse=bytes_sparse,
            duration_seconds=duration_seconds,
        )
        try:
            self._session.add(stats)
            self._session.commit()
        except:
            self._session.rollback()
            raise

    def get_stats(self, version_uid: VersionUid = None, limit: int = None) -> Iterator[Stats]:
        """ gets the <limit> newest entries """
        if version_uid:
            try:
                stats = self._session.query(Stats).filter_by(version_uid=version_uid).all()
            except:
                self._session.rollback()
                raise

            if stats is None:
                raise KeyError('Statistics for version {} not found.'.format(version_uid.readable))

            return stats
        else:
            try:
                stats = self._session.query(Stats).order_by(desc(Stats.version_date))
                if limit:
                    stats = stats.limit(limit)
                stats = stats.all()
            except:
                self._session.rollback()
                raise

            return reversed(stats)

    def set_version(self, version_uid: VersionUid, *, valid: bool = None, protected: bool = None):
        try:
            version = self.get_version(version_uid)
            if valid is not None:
                version.valid = valid
            if protected is not None:
                version.protected = protected
            self._session.commit()
            if valid is not None:
                logger_func = logger.info if valid else logger.error
                logger_func('Marked version {} as {}.'.format(version_uid.readable, 'valid' if valid else 'invalid'))
            if protected is not None:
                logger.info('Marked version {} as {}.'.format(version_uid.readable,
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
                     version_tags: List[str] = None) -> List[Version]:
        try:
            query = self._session.query(Version)
            if version_uid:
                query = query.filter_by(uid=version_uid)
            if version_name:
                query = query.filter_by(name=version_name)
            if version_snapshot_name:
                query = query.filter_by(snapshot_name=version_snapshot_name)
            if version_tags:
                query = query.join(Version.tags).filter(Tag.name.in_(version_tags))
            versions = query.order_by(Version.name, Version.date).all()
        except:
            self._session.rollback()
            raise

        return versions

    def add_tag(self, version_uid: VersionUid, name: str) -> None:
        """ Add a tag to a version_uid, do nothing if the tag already exists.
        """
        tag = Tag(
            version_uid=version_uid,
            name=name,
        )
        try:
            self._session.add(tag)
            self._session.commit()
        except IntegrityError:
            self._session.rollback()
            raise NoChange('Version {} already has tag {}.'.format(version_uid.readable, name)) from None
        except:
            self._session.rollback()
            raise

    def rm_tag(self, version_uid: VersionUid, name: str) -> None:
        try:
            deleted = self._session.query(Tag).filter_by(version_uid=version_uid, name=name).delete()
            self._session.commit()
        except:
            self._session.rollback()
            raise

        if deleted != 1:
            raise NoChange('Version {} has not tag {}.'.format(version_uid.readable, name))

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
                block.date = datetime.datetime.utcnow()
            else:
                block = Block(
                    id=id,
                    version_uid=version_uid,
                    uid=block_uid,
                    checksum=checksum,
                    size=size,
                    valid=valid,
                    date=datetime.datetime.utcnow(),
                )
                self._session.add(block)

            self._commit_block_counter += 1
            if self._commit_block_counter % self._COMMIT_EVERY_N_BLOCKS == 0:
                t1 = time.time()
                self._session.commit()
                t2 = time.time()
                logger.debug('Commited metadata transaction in {:.2f}s'.format(t2 - t1))
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
                block_uid, ', '.join([version_uid.readable for version_uid in affected_version_uids])))

            for version_uid in affected_version_uids:
                self.set_version(version_uid, valid=False)
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
        one_hour_ago = datetime.datetime.utcnow() - datetime.timedelta(seconds=dt)
        while True:
            # http://stackoverflow.com/questions/7389759/memory-efficient-built-in-sqlalchemy-iterator-generator
            delete_candidates = self._session.query(DeletedBlock)\
                .filter(DeletedBlock.date < one_hour_ago)\
                .limit(250)\
                .all()
            if not delete_candidates:
                break

            false_positives = set()
            hit_list: Dict[int, Set[BlockUid]] = {}
            for candidate in delete_candidates:
                rounds += 1
                if rounds % 1000 == 0:
                    logger.info("Cleanup-fast: {} false positives, {} data deletions.".format(
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
                logger.debug("Cleanup-fast: Removing {} false positive from delete candidates.".format(
                    len(false_positives)))
                self._session.query(DeletedBlock)\
                    .filter(DeletedBlock.uid.in_(false_positives))\
                    .delete(synchronize_session=False)

            if hit_list:
                for uids in hit_list.values():
                    self._session.query(DeletedBlock).filter(
                        DeletedBlock.uid.in_(uids)).delete(synchronize_session=False)
                yield (hit_list)

        self._session.commit()
        logger.info("Cleanup-fast: Cleanup finished. {} false positives, {} data deletions.".format(
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
                    return obj.isoformat(timespec='seconds')
                elif isinstance(obj, VersionUid):
                    return obj.to_int
                elif isinstance(obj, BlockUid):
                    return {'left': obj.left, 'right': obj.right}

                return super().default(obj)

        return BenjiEncoder

    def export_any(self, root_dict: Dict, f: TextIO, ignore_fields: List = None,
                   ignore_relationships: List = None) -> None:
        ignore_fields = list(ignore_fields) if ignore_fields is not None else []
        ignore_relationships = list(ignore_relationships) if ignore_relationships is not None else []

        # These are always ignored because they'd lead to a circle
        ignore_fields.append(((Tag, Block), ('version_uid',)))
        ignore_relationships.append(((Tag, Block), ('version',)))
        # Ignore these as we favor the composite attribute
        ignore_fields.append(((Block,), ('uid_left', 'uid_right')))

        root_dict = root_dict.copy()
        root_dict[self._METADATA_VERSION_KEY] = self._METADATA_VERSION

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
            f.seek(0)
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
        import_method_name = 'import_{}'.format(metadata_version.replace('.', '_'))
        import_method = getattr(self, import_method_name, None)
        if import_method is None or not callable(import_method):
            raise InputDataError('Unsupported import format version "{}".'.format(metadata_version))

        try:
            version_uids = import_method(json_input)
            self._session.commit()
        except:
            self._session.rollback()
            raise

        return version_uids

    def import_1_0_0(self, json_input: Dict) -> List[VersionUid]:
        version_uids: List[VersionUid] = []
        for version_dict in json_input['versions']:
            if not isinstance(version_dict, dict):
                raise InputDataError('Import file is invalid.')

            if 'uid' not in version_dict:
                raise InputDataError('Import file is invalid (hint: uid).')

            if not isinstance(version_dict['tags'], list):
                raise InputDataError('Version {} contains invalid data (hint: tags).'.format(
                    VersionUid(version_dict['uid']).readable))

            if not isinstance(version_dict['blocks'], list):
                raise InputDataError('Version {} contains invalid data (hint blocks).'.format(
                    VersionUid(version_dict['uid']).readable))

            try:
                self.get_version(version_dict['uid'])
            except KeyError:
                pass  # does not exist
            else:
                raise FileExistsError('Version {} already exists and cannot be imported.'.format(version_dict['uid']))

            version = Version(
                uid=version_dict['uid'],
                date=datetime.datetime.strptime(version_dict['date'], '%Y-%m-%dT%H:%M:%S'),
                name=version_dict['name'],
                snapshot_name=version_dict['snapshot_name'],
                size=version_dict['size'],
                storage_id=version_dict['storage_id'],
                block_size=version_dict['block_size'],
                valid=version_dict['valid'],
                protected=version_dict['protected'],
            )
            self._session.add(version)
            self._session.flush()

            for block_dict in version_dict['blocks']:
                if not isinstance(block_dict, dict):
                    raise InputDataError('Version {} contains invalid data (hint blocks).'.format(
                        VersionUid(version_dict['uid']).readable))
                block_dict['version_uid'] = version.uid
                block_dict['date'] = datetime.datetime.strptime(block_dict['date'], '%Y-%m-%dT%H:%M:%S')
                block_dict['uid_left'] = int(block_dict['uid']['left']) if block_dict['uid']['left'] is not None else None
                block_dict['uid_right'] = int(block_dict['uid']['right']) if block_dict['uid']['right'] is not None else None
                del block_dict['uid']
            self._session.bulk_insert_mappings(Block, version_dict['blocks'])

            for tag_dict in version_dict['tags']:
                if not isinstance(tag_dict, dict):
                    raise InputDataError('Version {} contains invalid data (hint: tags).'.format(
                        VersionUid(version_dict['uid']).readable))
                tag_dict['version_uid'] = version.uid
            self._session.bulk_insert_mappings(Tag, version_dict['tags'])

            version_uids.append(cast(VersionUid, VersionUid.create_from_readables(version_dict['uid'])))

        return version_uids

    def locking(self):
        return self._locking

    def close(self):
        self._session.commit()
        self._locking.unlock_all()
        self._locking = None
        self._session.close()


class MetaBackendLocking:

    GLOBAL_LOCK = 'global'

    def __init__(self, session) -> None:
        self._session = session
        self._host = platform.node()
        self._uuid = uuid.uuid1().hex
        self._locks: Dict[str, Lock] = {}

    def lock(self, *, lock_name: str = GLOBAL_LOCK, reason: str = None, locked_msg: str = None):
        if lock_name in self._locks:
            raise InternalError('Attempt to acquire lock "{}" twice'.format(lock_name))

        lock = Lock(
            host=self._host,
            process_id=self._uuid,
            lock_name=lock_name,
            reason=reason,
            date=datetime.datetime.utcnow(),
        )
        try:
            self._session.add(lock)
            self._session.commit()
        except SQLAlchemyError:  # this is actually too broad and will also include other errors
            self._session.rollback()
            if locked_msg is not None:
                raise AlreadyLocked(locked_msg)
            else:
                raise AlreadyLocked('Lock {} is already taken.'.format(lock_name))
        except:
            self._session.rollback()
            raise
        else:
            self._locks[lock_name] = lock

    def is_locked(self, *, lock_name: str = GLOBAL_LOCK) -> bool:
        try:
            locks = self._session.query(Lock).filter_by(
                host=self._host, lock_name=lock_name, process_id=self._uuid).all()
        except:
            self._session.rollback()
            raise
        else:
            return len(locks) > 0

    def update_lock(self, *, lock_name: str = GLOBAL_LOCK, reason: str = None) -> None:
        try:
            lock = self._session.query(Lock).filter_by(
                host=self._host, lock_name=lock_name, process_id=self._uuid).first()
            if not lock:
                raise InternalError('Lock {} isn\'t held by this instance or doesn\'t exist.'.format(lock_name))
            lock.reason = reason
            self._session.commit()
        except:
            self._session.rollback()
            raise

    def unlock(self, *, lock_name: str = GLOBAL_LOCK) -> None:
        if lock_name not in self._locks:
            raise InternalError('Attempt to release lock "{}" even though it isn\'t held'.format(lock_name))

        lock = self._locks[lock_name]
        try:
            self._session.delete(lock)
            self._session.commit()
        except:
            self._session.rollback()
            raise
        else:
            del self._locks[lock_name]

    def unlock_all(self) -> None:
        for lock_name, lock in self._locks.items():
            try:
                logger.error('Lock {} not released correctly, trying to release it now.'.format(lock))
                self._session.delete(lock)
                self._session.commit()
            except:
                pass
        self._locks = {}

    def lock_version(self, version_uid: VersionUid, reason: str = None) -> None:
        self.lock(
            lock_name=version_uid.readable,
            reason=reason,
            locked_msg='Version {} is already locked.'.format(version_uid.readable))

    def is_version_locked(self, version_uid: VersionUid) -> bool:
        return self.is_locked(lock_name=version_uid.readable)

    def update_version_lock(self, version_uid: VersionUid, reason: str = None) -> None:
        self.update_lock(lock_name=version_uid.readable, reason=reason)

    def unlock_version(self, version_uid: VersionUid) -> None:
        self.unlock(lock_name=version_uid.readable)

    @contextmanager
    def with_lock(self,
                  *,
                  lock_name: str = GLOBAL_LOCK,
                  reason: str = None,
                  locked_msg: str = None,
                  unlock: bool = True) -> Iterator[None]:
        self.lock(lock_name=lock_name, reason=reason, locked_msg=locked_msg)
        try:
            yield
        except:
            self.unlock(lock_name=lock_name)
            raise
        else:
            if unlock:
                self.unlock(lock_name=lock_name)

    @contextmanager
    def with_version_lock(self, version_uid: VersionUid, reason: str = None, unlock: bool = True) -> Iterator[None]:
        self.lock_version(version_uid, reason=reason)
        try:
            yield
        except:
            self.unlock_version(version_uid)
            raise
        else:
            if unlock:
                self.unlock_version(version_uid)
