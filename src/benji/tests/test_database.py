import datetime
from unittest import TestCase

from benji.database import BlockUid, VersionUid
from benji.exception import InternalError, UsageError
from benji.tests.testcase import DatabaseBackendTestCaseBase


class DatabaseBackendTestCase(DatabaseBackendTestCaseBase):

    def test_version(self):
        version = self.database_backend.create_version(
            version_name='backup-name',
            snapshot_name='snapshot-name',
            size=16 * 1024 * 4096,
            storage_id=1,
            block_size=4 * 1024 * 4096,
            valid=False)
        self.database_backend.commit()

        version = self.database_backend.get_version(version.uid)
        self.assertEqual('backup-name', version.name)
        self.assertEqual('snapshot-name', version.snapshot_name)
        self.assertEqual(16 * 1024 * 4096, version.size)
        self.assertEqual(4 * 1024 * 4096, version.block_size)
        self.assertFalse(version.valid)
        self.assertFalse(version.protected)

        self.database_backend.set_version(version.uid, valid=True)
        version = self.database_backend.get_version(version.uid)
        self.assertTrue(version.valid)

        self.database_backend.set_version(version.uid, valid=False)
        version = self.database_backend.get_version(version.uid)
        self.assertFalse(version.valid)

        self.database_backend.set_version(version.uid, protected=True)
        version = self.database_backend.get_version(version.uid)
        self.assertTrue(version.protected)

        self.database_backend.set_version(version.uid, protected=False)
        version = self.database_backend.get_version(version.uid)
        self.assertFalse(version.protected)

        self.database_backend.add_label(version.uid, 'label-1', 'bla')
        self.database_backend.add_label(version.uid, 'label-2', '')
        version = self.database_backend.get_version(version.uid)
        self.assertEqual(2, len(version.labels))
        self.assertEqual(version.uid, version.labels[0].version_uid)
        self.assertEqual('label-1', version.labels[0].name)
        self.assertEqual('bla', version.labels[0].value)
        self.assertEqual(version.uid, version.labels[1].version_uid)
        self.assertEqual('label-2', version.labels[1].name)
        self.assertEqual('', version.labels[1].value)

        self.database_backend.add_label(version.uid, 'label-2', 'test123')
        version = self.database_backend.get_version(version.uid)
        self.assertEqual(version.uid, version.labels[1].version_uid)
        self.assertEqual('label-2', version.labels[1].name)
        self.assertEqual('test123', version.labels[1].value)

        self.database_backend.rm_label(version.uid, 'label-1')
        version = self.database_backend.get_version(version.uid)
        self.assertEqual(1, len(version.labels))

        self.database_backend.rm_label(version.uid, 'label-2')
        version = self.database_backend.get_version(version.uid)
        self.assertEqual(0, len(version.labels))

        self.database_backend.rm_label(version.uid, 'label-3')
        version = self.database_backend.get_version(version.uid)
        self.assertEqual(0, len(version.labels))

        version_uids = set()
        for _ in range(256):
            version = self.database_backend.create_version(
                version_name='backup-name',
                snapshot_name='snapshot-name',
                size=16 * 1024 * 4096,
                storage_id=1,
                block_size=4 * 1024 * 4096,
                valid=False)
            version = self.database_backend.get_version(version.uid)
            self.assertNotIn(version.uid, version_uids)
            version_uids.add(version.uid)

    def test_block(self):
        version = self.database_backend.create_version(
            version_name='name-' + self.random_string(12),
            snapshot_name='snapshot-name-' + self.random_string(12),
            size=256 * 1024 * 4096,
            block_size=1024 * 4096,
            storage_id=1,
            valid=False)
        self.database_backend.commit()

        checksums = []
        uids = []
        num_blocks = 256
        for id in range(num_blocks):
            checksums.append(self.random_hex(64))
            uids.append(BlockUid(1, id))
            self.database_backend.set_block(
                id=id,
                version_uid=version.uid,
                block_uid=uids[id],
                checksum=checksums[id],
                size=1024 * 4096,
                valid=True)
        self.database_backend.commit()

        for id, checksum in enumerate(checksums):
            block = self.database_backend.get_block_by_checksum(checksum, 1)
            self.assertEqual(id, block.id)
            self.assertEqual(version.uid, block.version_uid)
            self.assertEqual(uids[id], block.uid)
            self.assertEqual(checksum, block.checksum)
            self.assertEqual(1024 * 4096, block.size)
            self.assertTrue(block.valid)

        for id, uid in enumerate(uids):
            block = self.database_backend.get_block(uid)
            self.assertEqual(id, block.id)
            self.assertEqual(version.uid, block.version_uid)
            self.assertEqual(uid, block.uid)
            self.assertEqual(checksums[id], block.checksum)
            self.assertEqual(1024 * 4096, block.size)
            self.assertTrue(block.valid)

        blocks = self.database_backend.get_blocks_by_version(version.uid)
        self.assertEqual(num_blocks, len(blocks))
        for id, block in enumerate(blocks):
            self.assertEqual(id, block.id)
            self.assertEqual(version.uid, block.version_uid)
            self.assertEqual(uids[id], block.uid)
            self.assertEqual(checksums[id], block.checksum)
            self.assertEqual(1024 * 4096, block.size)
            self.assertTrue(block.valid)

        for id, block in enumerate(blocks):
            dereferenced_block = block.deref()
            self.assertEqual(id, dereferenced_block.id)
            self.assertEqual(version.uid, dereferenced_block.version_uid)
            self.assertEqual(uids[id].left, dereferenced_block.uid.left)
            self.assertEqual(uids[id].right, dereferenced_block.uid.right)
            self.assertEqual(checksums[id], dereferenced_block.checksum)
            self.assertEqual(1024 * 4096, dereferenced_block.size)
            self.assertTrue(dereferenced_block.valid)

        self.database_backend.rm_version(version.uid)
        self.database_backend.commit()
        blocks = self.database_backend.get_blocks_by_version(version.uid)
        self.assertEqual(0, len(blocks))

        count = 0
        for uids_deleted in self.database_backend.get_delete_candidates(-1):
            for storage in uids_deleted.values():
                for uid in storage:
                    self.assertIn(uid, uids)
                    count += 1
        self.assertEqual(num_blocks, count)

    def test_lock_version(self):
        locking = self.database_backend.locking()
        locking.lock_version(VersionUid(1), reason='locking test')
        self.assertRaises(InternalError, lambda: locking.lock_version(VersionUid(1), reason='locking test'))
        locking.unlock_version(VersionUid(1))

    def test_lock_global(self):
        locking = self.database_backend.locking()
        locking.lock(reason='locking test')
        self.assertRaises(InternalError, lambda: locking.lock(reason='locking test'))
        locking.unlock()

    def test_lock_singleton(self):
        locking = self.database_backend.locking()
        locking2 = self.database_backend.locking()
        self.assertEqual(locking, locking2)

    def test_is_locked(self):
        locking = self.database_backend.locking()
        lock = locking.lock(reason='locking test')
        self.assertTrue(locking.is_locked())
        locking.unlock()
        self.assertFalse(locking.is_locked())

    def test_is_version_locked(self):
        locking = self.database_backend.locking()
        lock = locking.lock_version(VersionUid(1), reason='locking test')
        self.assertTrue(locking.is_version_locked(VersionUid(1)))
        locking.unlock_version(VersionUid(1))
        self.assertFalse(locking.is_version_locked(VersionUid(1)))

    def test_lock_version_context_manager(self):
        locking = self.database_backend.locking()
        with locking.with_version_lock(VersionUid(1), reason='locking test'):
            with self.assertRaises(InternalError):
                locking.lock_version(VersionUid(1), reason='locking test')
        locking.lock_version(VersionUid(1), reason='locking test')
        locking.unlock_version(VersionUid(1))

    def test_lock_context_manager(self):
        locking = self.database_backend.locking()
        with locking.with_lock(reason='locking test'):
            with self.assertRaises(InternalError):
                locking.lock(reason='locking test')
        locking.lock(reason='locking test')
        locking.unlock()

    def test_version_uid_string(self):
        self.assertEqual(VersionUid(1), VersionUid('V1'))

    def test_version_filter(self):
        version_uids = set()
        for i in range(256):
            version = self.database_backend.create_version(
                version_name='backup-name',
                snapshot_name='snapshot-name.{}'.format(i),
                size=16 * 1024 * 4096,
                storage_id=1,
                block_size=4 * 1024 * 4096,
                valid=False)
            version = self.database_backend.get_version(version.uid)
            self.assertEqual(0, len(version.labels))
            self.database_backend.add_label(version.uid, 'label-key', 'label-value')
            self.database_backend.add_label(version.uid, 'label-key-2', str(i))
            self.database_backend.add_label(version.uid, 'label-key-3', '')
            if i > 127:
                self.database_backend.add_label(version.uid, 'label-key-4', '')
                self.assertEqual(4, len(version.labels))
            else:
                self.assertEqual(3, len(version.labels))
            self.assertNotIn(version.uid, version_uids)
            version_uids.add(version.uid)

        versions = self.database_backend.get_versions_with_filter()
        self.assertEqual(256, len(versions))

        versions = self.database_backend.get_versions_with_filter('labels["label-key"] == "label-value"')
        self.assertEqual(256, len(versions))

        self.assertRaises(
            UsageError, lambda: self.database_backend.get_versions_with_filter('labels["label-key"] and "label-value"'))
        self.assertRaises(UsageError, lambda: self.database_backend.get_versions_with_filter('True'))
        self.assertRaises(UsageError, lambda: self.database_backend.get_versions_with_filter('10'))
        self.assertRaises(
            UsageError,
            lambda: self.database_backend.get_versions_with_filter('labels["label-key"] == "label-value" and True'))
        self.assertRaises(
            UsageError,
            lambda: self.database_backend.get_versions_with_filter('labels["label-key"] == "label-value" and False'))
        self.assertRaises(UsageError, lambda: self.database_backend.get_versions_with_filter('"hallo" == "hey"'))

        # name is always true because it is never empty
        versions = self.database_backend.get_versions_with_filter('name')
        self.assertEqual(256, len(versions))

        versions = self.database_backend.get_versions_with_filter('valid == False')
        self.assertEqual(256, len(versions))

        self.assertRaises(UsageError, lambda: self.database_backend.get_versions_with_filter('valid == wrong'))

        versions = self.database_backend.get_versions_with_filter('labels["label-key-3"] == ""')
        self.assertEqual(256, len(versions))

        versions = self.database_backend.get_versions_with_filter('labels["label-key"] != "label-value"')
        self.assertEqual(0, len(versions))

        versions = self.database_backend.get_versions_with_filter('labels["label-key-2"] == 9')
        self.assertEqual(1, len(versions))

        versions = self.database_backend.get_versions_with_filter('snapshot_name == "snapshot-name.1"')
        self.assertEqual(1, len(versions))
        self.assertEqual(VersionUid(2), versions[0].uid)

        versions = self.database_backend.get_versions_with_filter('snapshot_name == "snapshot-name.1" and labels["label-key-2"] == 1')
        self.assertEqual(1, len(versions))
        self.assertEqual(VersionUid(2), versions[0].uid)

        versions = self.database_backend.get_versions_with_filter('snapshot_name == "snapshot-name.1" and labels["label-key-2"] == "2"')
        self.assertEqual(0, len(versions))

        versions = self.database_backend.get_versions_with_filter('snapshot_name == "snapshot-name.1" or labels["label-key-2"] == 2')
        self.assertEqual(2, len(versions))
        self.assertSetEqual(set([VersionUid(2), VersionUid(3)]), set([version.uid for version in versions]))

        versions = self.database_backend.get_versions_with_filter('name == "backup-name" and snapshot_name == "snapshot-name.1"')
        self.assertEqual(1, len(versions))
        self.assertEqual(VersionUid(2), versions[0].uid)

        versions = self.database_backend.get_versions_with_filter('name == "backup-name" and (snapshot_name == "snapshot-name.1" or snapshot_name == "snapshot-name.2")')
        self.assertEqual(2, len(versions))
        self.assertSetEqual(set([VersionUid(2), VersionUid(3)]), set([version.uid for version in versions]))

        versions = self.database_backend.get_versions_with_filter('uid == "V1" or uid == "V12"')
        self.assertEqual(2, len(versions))
        self.assertSetEqual(set([VersionUid(1), VersionUid(12)]), set([version.uid for version in versions]))

        versions = self.database_backend.get_versions_with_filter('uid == 1 or uid == 2')
        self.assertEqual(2, len(versions))
        self.assertSetEqual(set([VersionUid(1), VersionUid(2)]), set([version.uid for version in versions]))

        versions = self.database_backend.get_versions_with_filter('uid == "V1" and uid == "V12"')
        self.assertEqual(0, len(versions))

        versions = self.database_backend.get_versions_with_filter('not labels["not-exists"]')
        self.assertEqual(256, len(versions))

        versions = self.database_backend.get_versions_with_filter('labels["label-key-4"]')
        self.assertEqual(128, len(versions))

        versions = self.database_backend.get_versions_with_filter('labels["label-key-4"] and name')
        self.assertEqual(128, len(versions))

    def test_version_statistic_filter(self):
        for i in range(16):
            self.database_backend.set_stats(
                uid=VersionUid(i),
                base_uid=None,
                hints_supplied=True,
                date=datetime.datetime.utcnow(),
                name='backup-name',
                snapshot_name='snapshot-name.{}'.format(i),
                size=16 * 1024 * 4096,
                storage_id=1,
                block_size=4 * 1024 * 4096,
                bytes_read=1,
                bytes_written=2,
                bytes_dedup=3,
                bytes_sparse=4,
                duration=5)

        stats = self.database_backend.get_stats_with_filter()
        self.assertEqual(16, len(stats))

        stats = self.database_backend.get_stats_with_filter('snapshot_name == "snapshot-name.1"')
        self.assertEqual(1, len(stats))

        self.assertRaises(UsageError,
                          lambda: self.database_backend.get_stats_with_filter('labels["label-key"] and "label-value"'))
        self.assertRaises(UsageError, lambda: self.database_backend.get_stats_with_filter('True'))
        self.assertRaises(UsageError, lambda: self.database_backend.get_stats_with_filter('10'))
        self.assertRaises(UsageError, lambda: self.database_backend.get_stats_with_filter('"hallo" == "hey"'))
        self.assertRaises(UsageError,
                          lambda: self.database_backend.get_stats_with_filter('labels["label-key"] == "label-value"'))


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
