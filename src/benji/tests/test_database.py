import datetime
import math
import time
import timeit
import uuid
from typing import List, Dict, Any
from unittest import TestCase

from dateutil import tz

from benji.database import BlockUid, VersionUid, VersionStatus, Version, Storage, DeletedBlock, Locking
from benji.exception import InternalError, UsageError, AlreadyLocked
from benji.logging import logger
from benji.tests.testcase import DatabaseBackendTestCaseBase


class DatabaseBackendTestCase(DatabaseBackendTestCaseBase):

    def test_version(self):
        Storage.sync('s-1', storage_id=1)
        version = Version.create(version_uid=VersionUid('v1'),
                                 volume='backup-name',
                                 snapshot='snapshot-name',
                                 size=16 * 1024 * 4096,
                                 storage_id=1,
                                 block_size=4 * 1024 * 4096)

        version = Version.get_by_uid(version.uid)
        self.assertEqual('backup-name', version.volume)
        self.assertEqual('snapshot-name', version.snapshot)
        self.assertEqual(16 * 1024 * 4096, version.size)
        self.assertEqual(4 * 1024 * 4096, version.block_size)
        self.assertEqual(version.status, VersionStatus.incomplete)
        self.assertFalse(version.protected)

        version.set(status=VersionStatus.valid)
        version = Version.get_by_uid(version.uid)
        self.assertEqual(version.status, VersionStatus.valid)

        version.set(status=VersionStatus.invalid)
        version = Version.get_by_uid(version.uid)
        self.assertEqual(version.status, VersionStatus.invalid)

        version.set(protected=True)
        version = Version.get_by_uid(version.uid)
        self.assertTrue(version.protected)

        version.set(protected=False)
        version = Version.get_by_uid(version.uid)
        self.assertFalse(version.protected)

        version.add_label('label-1', 'bla')
        version.add_label('label-2', '')
        version = Version.get_by_uid(version.uid)
        self.assertEqual(2, len(version.labels))
        self.assertEqual(version.id, version.labels['label-1'].version_id)
        self.assertEqual('label-1', version.labels['label-1'].name)
        self.assertEqual('bla', version.labels['label-1'].value)
        self.assertEqual(version.id, version.labels['label-2'].version_id)
        self.assertEqual('label-2', version.labels['label-2'].name)
        self.assertEqual('', version.labels['label-2'].value)

        version.add_label('label-2', 'test123')
        version = Version.get_by_uid(version.uid)
        self.assertEqual(version.id, version.labels['label-2'].version_id)
        self.assertEqual('label-2', version.labels['label-2'].name)
        self.assertEqual('test123', version.labels['label-2'].value)

        version.rm_label('label-1')
        version = Version.get_by_uid(version.uid)
        self.assertEqual(1, len(version.labels))

        version.rm_label('label-2')
        version = Version.get_by_uid(version.uid)
        self.assertEqual(0, len(version.labels))

        version.rm_label('label-3')
        version = Version.get_by_uid(version.uid)
        self.assertEqual(0, len(version.labels))

    def test_block(self):
        Storage.sync('s-1', storage_id=1)
        version = Version.create(version_uid=VersionUid('v1'),
                                 volume='name-' + self.random_string(12),
                                 snapshot='snapshot-name-' + self.random_string(12),
                                 size=256 * 1024 * 4096,
                                 block_size=1024 * 4096,
                                 storage_id=1)

        checksums = []
        uids = []
        num_blocks = 256
        blocks: List[Dict[str, Any]] = []
        for idx in range(num_blocks):
            checksums.append(self.random_hex(64))
            uids.append(BlockUid(1, idx))
            blocks.append({
                'idx': idx,
                'uid_left': uids[idx].left,
                'uid_right': uids[idx].right,
                'checksum': checksums[idx],
                'size': 1024 * 4096,
                'valid': True
            })
        version.create_blocks(blocks=blocks)

        for idx, checksum in enumerate(checksums):
            block = version.get_block_by_checksum(checksum)
            self.assertEqual(idx, block.idx)
            self.assertEqual(version.id, block.version_id)
            self.assertEqual(uids[idx], block.uid)
            self.assertEqual(checksum, block.checksum)
            self.assertEqual(1024 * 4096, block.size)
            self.assertTrue(block.valid)

        for idx, uid in enumerate(uids):
            block = version.get_block_by_idx(idx)
            self.assertEqual(idx, block.idx)
            self.assertEqual(version.id, block.version_id)
            self.assertEqual(uid, block.uid)
            self.assertEqual(checksums[idx], block.checksum)
            self.assertEqual(1024 * 4096, block.size)
            self.assertTrue(block.valid)

        self.assertEqual(num_blocks, len(list(version.blocks)))
        self.assertEqual(num_blocks, version.blocks_count)
        self.assertEqual(0, version.sparse_blocks_count)

        for idx, block in enumerate(version.blocks):
            self.assertEqual(idx, block.idx)
            self.assertEqual(version.id, block.version_id)
            self.assertEqual(uids[idx], block.uid)
            self.assertEqual(checksums[idx], block.checksum)
            self.assertEqual(1024 * 4096, block.size)
            self.assertTrue(block.valid)

        for idx, block in enumerate(version.blocks):
            dereferenced_block = block.deref()
            self.assertEqual(idx, dereferenced_block.idx)
            self.assertEqual(version.id, dereferenced_block.version_id)
            self.assertEqual(uids[idx].left, dereferenced_block.uid.left)
            self.assertEqual(uids[idx].right, dereferenced_block.uid.right)
            self.assertEqual(checksums[idx], dereferenced_block.checksum)
            self.assertEqual(1024 * 4096, dereferenced_block.size)
            self.assertTrue(dereferenced_block.valid)

        version.remove()
        deleted_count = 0
        for uids_deleted in DeletedBlock.get_unused_block_uids(-1):
            for storage in uids_deleted.values():
                for uid in storage:
                    self.assertIn(uid, uids)
                    deleted_count += 1
        self.assertEqual(num_blocks, deleted_count)

    def test_lock_version(self):
        Locking.lock_version(VersionUid('v1'), reason='locking test')
        self.assertRaises(InternalError, lambda: Locking.lock_version(VersionUid('v1'), reason='locking test'))
        Locking.unlock_version(VersionUid('v1'))

    def test_is_locked(self):
        Locking.lock(lock_name='test', reason='locking test')
        self.assertTrue(Locking.is_locked(lock_name='test'))
        Locking.unlock(lock_name='test')
        self.assertFalse(Locking.is_locked(lock_name='test'))

    def test_is_version_locked(self):
        Locking.lock_version(VersionUid('v1'), reason='locking test')
        self.assertTrue(Locking.is_version_locked(VersionUid('v1')))
        Locking.unlock_version(VersionUid('v1'))
        self.assertFalse(Locking.is_version_locked(VersionUid('v1')))

    def test_lock_version_context_manager(self):
        with Locking.with_version_lock(VersionUid('v1'), reason='locking test'):
            with self.assertRaises(InternalError):
                Locking.lock_version(VersionUid('v1'), reason='locking test')
        Locking.lock_version(VersionUid('v1'), reason='locking test')
        Locking.unlock_version(VersionUid('v1'))

    def test_lock_context_manager(self):
        with Locking.with_lock(lock_name='test', reason='locking test'):
            with self.assertRaises(InternalError):
                Locking.lock(lock_name='test', reason='locking test')
        Locking.lock(lock_name='test', reason='locking test')
        Locking.unlock(lock_name='test')

    def test_lock_override(self):
        Locking.lock_version(VersionUid('v1'), reason='locking test')
        self.assertRaises(InternalError, lambda: Locking.lock_version(VersionUid('v1'), reason='locking test'))
        old_uuid = Locking._uuid
        new_uuid = uuid.uuid1().hex
        # This fakes the appearance of another instance
        Locking._uuid = new_uuid
        self.assertRaises(AlreadyLocked, lambda: Locking.lock_version(VersionUid('v1'), reason='locking test'))
        Locking.lock_version(VersionUid('v1'), reason='locking test', override_lock=True)
        self.assertRaises(InternalError, lambda: Locking.lock_version(VersionUid('v1'), reason='locking test'))
        Locking._uuid = old_uuid
        self.assertRaises(AlreadyLocked, lambda: Locking.lock_version(VersionUid('v1'), reason='locking test'))
        Locking._uuid = new_uuid
        Locking.unlock_version(VersionUid('v1'))

    def test_version_filter(self):
        Storage.sync('s-1', storage_id=1)
        for i in range(256):
            version = Version.create(version_uid=VersionUid(f'v{i + 1}'),
                                     volume='backup-name',
                                     snapshot='snapshot-name.{}'.format(i),
                                     size=16 * 1024 * 4096,
                                     storage_id=1,
                                     block_size=4 * 1024 * 4096,
                                     status=VersionStatus.valid)
            version = Version.get_by_uid(version.uid)
            self.assertEqual(0, len(version.labels))
            version.add_label('label-key', 'label-value')
            version.add_label('label-key-2', str(i))
            version.add_label('label-key-3', '')
            if i > 127:
                version.add_label('label-key-4', '')
                self.assertEqual(4, len(version.labels))
            else:
                self.assertEqual(3, len(version.labels))

        versions = Version.find_with_filter()
        self.assertEqual(256, len(versions))

        versions = Version.find_with_filter('labels["label-key"] == "label-value"')
        self.assertEqual(256, len(versions))

        versions = Version.find_with_filter('"label-value" == labels["label-key"]')
        self.assertEqual(256, len(versions))

        self.assertRaises(UsageError, lambda: Version.find_with_filter('labels["label-key"] and "label-value"'))
        self.assertRaises(UsageError, lambda: Version.find_with_filter('True'))
        self.assertRaises(UsageError, lambda: Version.find_with_filter('10'))
        self.assertRaises(UsageError, lambda: Version.find_with_filter('labels["label-key"] == "label-value" and True'))
        self.assertRaises(UsageError, lambda: Version.find_with_filter('labels["label-key"] == "label-value" and False'))
        self.assertRaises(UsageError, lambda: Version.find_with_filter('"hallo" == "hey"'))

        # volume is always true because it is never empty
        versions = Version.find_with_filter('volume')
        self.assertEqual(256, len(versions))

        versions = Version.find_with_filter('status == "valid"')
        self.assertEqual(256, len(versions))

        self.assertRaises(UsageError, lambda: Version.find_with_filter('status == wrong'))

        versions = Version.find_with_filter('labels["label-key-3"] == ""')
        self.assertEqual(256, len(versions))

        versions = Version.find_with_filter('labels["label-key"] != "label-value"')
        self.assertEqual(0, len(versions))

        versions = Version.find_with_filter('labels["label-key-2"] == 9')
        self.assertEqual(1, len(versions))

        versions = Version.find_with_filter('snapshot == "snapshot-name.1"')
        self.assertEqual(1, len(versions))
        self.assertEqual(VersionUid('v2'), versions[0].uid)

        versions = Version.find_with_filter('snapshot == "snapshot-name.1" and labels["label-key-2"] == 1')
        self.assertEqual(1, len(versions))
        self.assertEqual(VersionUid('v2'), versions[0].uid)

        versions = Version.find_with_filter('snapshot == "snapshot-name.1" and labels["label-key-2"] == "2"')
        self.assertEqual(0, len(versions))

        versions = Version.find_with_filter('snapshot == "snapshot-name.1" or labels["label-key-2"] == 2')
        self.assertEqual(2, len(versions))
        self.assertSetEqual({VersionUid('v2'), VersionUid('v3')}, {version.uid for version in versions})

        versions = Version.find_with_filter('volume == "backup-name" and snapshot == "snapshot-name.1"')
        self.assertEqual(1, len(versions))
        self.assertEqual(VersionUid('v2'), versions[0].uid)

        versions = Version.find_with_filter('volume == "backup-name" and (snapshot == "snapshot-name.1" or snapshot == "snapshot-name.2")')
        self.assertEqual(2, len(versions))
        self.assertSetEqual({VersionUid('v2'), VersionUid('v3')}, {version.uid for version in versions})

        versions = Version.find_with_filter('uid == "v1" or uid == "v12"')
        self.assertEqual(2, len(versions))
        self.assertSetEqual({VersionUid('v1'), VersionUid('v12')}, {version.uid for version in versions})

        versions = Version.find_with_filter('uid == "v1" and uid == "v12"')
        self.assertEqual(0, len(versions))

        versions = Version.find_with_filter('not labels["not-exists"]')
        self.assertEqual(256, len(versions))

        versions = Version.find_with_filter('labels["label-key-4"]')
        self.assertEqual(128, len(versions))

        versions = Version.find_with_filter('labels["label-key-4"] and volume')
        self.assertEqual(128, len(versions))

    # Issue https://github.com/elemental-lf/benji/issues/9
    def test_version_filter_issue_9(self):
        Storage.sync('s-1', storage_id=1)
        version_uids = set()
        for i in range(3):
            version = Version.create(version_uid=VersionUid(f'v{i + 1}'),
                                     volume='backup-name',
                                     snapshot='snapshot-name.{}'.format(i),
                                     size=16 * 1024 * 4096,
                                     storage_id=1,
                                     block_size=4 * 1024 * 4096,
                                     status=VersionStatus.valid)
            self.assertNotIn(version.uid, version_uids)
            version_uids.add(version.uid)

        versions = Version.find_with_filter('snapshot == "snapshot-name.2" and volume == "backup-name" and status == "valid"')
        self.assertEqual(1, len(versions))

        versions = Version.find_with_filter('snapshot == "snapshot-name.2" or volume == "backup-name" or status == "valid"')
        self.assertEqual(3, len(versions))

    # Issue https://github.com/elemental-lf/benji/issues/9 (slowness part)
    def test_version_filter_issue_9_slowness(self):
        Storage.sync('s-1', storage_id=1)
        version_uids = set()
        for i in range(3):
            version = Version.create(version_uid=VersionUid(f'v{i + 1}'),
                                     volume='backup-name',
                                     snapshot='snapshot-name.{}'.format(i),
                                     size=16 * 1024 * 4096,
                                     storage_id=1,
                                     block_size=4 * 1024 * 4096)
            self.assertNotIn(version.uid, version_uids)
            version_uids.add(version.uid)

        t1 = timeit.timeit(lambda: Version.find_with_filter('snapshot == "snapshot-name.2" and volume == "backup-name"'),
                           number=1)
        t2 = timeit.timeit(lambda: Version.find_with_filter('(snapshot == "snapshot-name.2" and volume == "backup-name")'),
                           number=1)
        logger.debug('test_version_filter_issue_9_slowness: t1 {}, t2 {}'.format(t1, t2))
        self.assertLess(t1 - t2, 5)

    def test_version_filter_dateparse(self):
        Storage.sync('s-1', storage_id=1)
        version_uids = set()
        for i in range(3):
            version = Version.create(version_uid=VersionUid(f'v{i + 1}'),
                                     volume='backup-name',
                                     snapshot='snapshot-name.{}'.format(i),
                                     size=16 * 1024 * 4096,
                                     storage_id=1,
                                     block_size=4 * 1024 * 4096)
            self.assertNotIn(version.uid, version_uids)
            version_uids.add(version.uid)

        # Wait at least one seconds
        time.sleep(1)

        versions = Version.find_with_filter('date <= "now"')
        self.assertEqual(3, len(versions))

        versions = Version.find_with_filter('date > "1 month ago"')
        self.assertEqual(3, len(versions))

        versions = Version.find_with_filter('date > "now"')
        self.assertEqual(0, len(versions))

        versions = Version.find_with_filter('date < "1 month ago"')
        self.assertEqual(0, len(versions))

        versions = Version.find_with_filter('"now" >= date')
        self.assertEqual(3, len(versions))

        versions = Version.find_with_filter('"1 month ago" < date')
        self.assertEqual(3, len(versions))

        versions = Version.find_with_filter('"now" < date')
        self.assertEqual(0, len(versions))

        versions = Version.find_with_filter('"1 month ago" > date')
        self.assertEqual(0, len(versions))

        versions = Version.find_with_filter('date <= "{}"'.format(
            datetime.datetime.now(tz=tz.tzlocal()).strftime("%Y-%m-%dT%H:%M:%S")))
        self.assertEqual(3, len(versions))

    def test_set_block_invalid(self):
        Storage.sync('s-1', storage_id=1)
        versions = []
        good_uid = BlockUid(1, 2)
        bad_uid = BlockUid(3, 4)
        for i in range(6):
            version = Version.create(version_uid=VersionUid(f'v{i + 1}'),
                                     volume='backup-name',
                                     snapshot='snapshot-name.{}'.format(i),
                                     size=16 * 1024 * 4096,
                                     storage_id=1,
                                     block_size=4 * 1024 * 4096)
            blocks = [{
                'idx': 0,
                'uid_left': bad_uid.left if i < 3 else good_uid.left,
                'uid_right': bad_uid.right if i < 3 else good_uid.right,
                'checksum': 'aabbcc',
                'size': 4 * 1024 * 4096,
                'valid': True,
            }]
            version.create_blocks(blocks=blocks)
            version.set(status=VersionStatus.valid)

            versions.append(version)

        Version.set_block_invalid(bad_uid)

        for i in range(3):
            self.assertEqual(VersionStatus.invalid, versions[i].status)
            self.assertFalse(list(versions[i].blocks)[0].valid)

        for i in range(3, 6):
            self.assertEqual(VersionStatus.valid, versions[i].status)
            self.assertTrue(list(versions[i].blocks)[0].valid)

    def test_version_blocks_count(self):
        Storage.sync('s-1', storage_id=1)
        for i in range(256):
            version = Version.create(version_uid=f'v{i + 1}',
                                     volume='backup-name',
                                     snapshot='snapshot-name.{}'.format(i),
                                     size=i * 1111 * 4096,
                                     storage_id=1,
                                     block_size=4 * 1024 * 4096)
            self.assertEqual(math.ceil(version.size / version.block_size), version.blocks_count)

    def test_storage_usage(self):
        Storage.sync('s-1', storage_id=1)
        for i in range(2):
            version = Version.create(version_uid=VersionUid(f'v{i + 1}'),
                                     volume='backup-name',
                                     snapshot='snapshot-name.{}'.format(i),
                                     size=32 * 1024 * 4096,
                                     storage_id=1,
                                     block_size=1024 * 4096)

            blocks: List[Dict[str, Any]] = []
            # shared
            for idx in range(0, 6):
                uid = BlockUid(1, idx)
                blocks.append({
                    'idx': idx,
                    'uid_left': uid.left,
                    'uid_right': uid.right,
                    'checksum': None,
                    'size': 1024 * 4096,
                    'valid': True
                })
            # sparse
            for idx in range(6, 13):
                uid = BlockUid(i + 1, idx)
                blocks.append({
                    'idx': idx,
                    'uid_left': None,
                    'uid_right': None,
                    'checksum': None,
                    'size': 1024 * 4096,
                    'valid': True
                })
            # exclusive
            for idx in range(13, 25):
                uid = BlockUid(i + 1, idx)
                blocks.append({
                    'idx': idx,
                    'uid_left': uid.left,
                    'uid_right': uid.right,
                    'checksum': None,
                    'size': 1024 * 4096,
                    'valid': True
                })
            # exclusive deduplicated
            for idx in range(25, 32):
                uid = BlockUid(i + 1, idx - 7)
                blocks.append({
                    'idx': idx,
                    'uid_left': uid.left,
                    'uid_right': uid.right,
                    'checksum': None,
                    'size': 1024 * 4096,
                    'valid': True
                })
            version.create_blocks(blocks=blocks)

        for uid in ('v1', 'v2'):
            usage = Version.storage_usage(f'uid == "{uid}"')
            self.assertIsInstance(usage.get('s-1', None), dict)
            usage_s_1 = usage['s-1']
            check_value_matrix = (
                ('virtual', 32 * 1024 * 4096),
                ('shared', 6 * 1024 * 4096),
                ('sparse', 7 * 1024 * 4096),
                ('exclusive', 19 * 1024 * 4096),
                ('deduplicated_exclusive', (19 - 7) * 1024 * 4096),
            )
            for field, amount in check_value_matrix:
                value = usage_s_1.get(field, None)
                self.assertIsInstance(value, int)
                self.assertEqual(amount, value)


class DatabaseBackendTestSQLLite(DatabaseBackendTestCase, TestCase):

    CONFIG = """
        configurationVersion: '1'
        logFile: /dev/stderr
        ios:
        - name: file
          module: file
        defaultStorage: s1
        storages:
        - name: s1
          storageId: 1
          module: file
          configuration:
            path: {testpath}/data
        databaseEngine: sqlite:///{testpath}/benji.sqlite
        """


class DatabaseBackendTestSQLLiteInMemory(DatabaseBackendTestCase, TestCase):

    CONFIG = """
        configurationVersion: '1'
        logFile: /dev/stderr
        ios:
        - name: file
          module: file
        defaultStorage: s1
        storages:
        - name: s1
          storageId: 1
          module: file
          configuration:
            path: {testpath}/data
        databaseEngine: sqlite://       
        """


class DatabaseBackendTestPostgreSQL(DatabaseBackendTestCase, TestCase):

    CONFIG = """
        configurationVersion: '1'
        logFile: /dev/stderr
        ios:
        - name: file
          module: file
        defaultStorage: s1
        storages:
        - name: s1
          storageId: 1
          module: file
          configuration:
            path: {testpath}/data
        databaseEngine: postgresql://benji:verysecret@localhost:15432/benji
        """
