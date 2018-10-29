import random
from unittest.mock import Mock

from benji.metadata import Block, BlockUid, VersionUid
from benji.tests.testcase import DataBackendTestCase


class DatabackendTestCase(DataBackendTestCase):

    def test_save_rm_sync(self):
        NUM_BLOBS = 15
        BLOB_SIZE = 4096

        saved_uids = self.storage.list_blocks()
        self.assertEqual(0, len(saved_uids))

        blocks = [
            Mock('Block', uid=BlockUid(i + 1, i + 100), size=BLOB_SIZE, checksum='CHECKSUM') for i in range(NUM_BLOBS)
        ]
        data_by_uid = {}
        for block in blocks:
            data = self.random_bytes(BLOB_SIZE)
            self.assertEqual(BLOB_SIZE, len(data))
            self.storage.save(block, data, sync=True)
            data_by_uid[block.uid] = data

        saved_uids = self.storage.list_blocks()
        self.assertEqual(NUM_BLOBS, len(saved_uids))

        uids_set = set([block.uid for block in blocks])
        saved_uids_set = set(saved_uids)
        self.assertEqual(NUM_BLOBS, len(uids_set))
        self.assertEqual(NUM_BLOBS, len(saved_uids_set))
        self.assertEqual(0, len(uids_set.symmetric_difference(saved_uids_set)))

        for block in blocks:
            data = self.storage.read(block, sync=True)
            self.assertEqual(data_by_uid[block.uid], data)

        for block in blocks:
            self.storage.rm(block.uid)
        saved_uids = self.storage.list_blocks()
        self.assertEqual(0, len(saved_uids))

    def test_save_rm_async(self):
        NUM_BLOBS = 15
        BLOB_SIZE = 4096

        saved_uids = self.storage.list_blocks()
        self.assertEqual(0, len(saved_uids))

        blocks = [
            Mock('Block', uid=BlockUid(i + 1, i + 100), size=BLOB_SIZE, checksum='CHECKSUM') for i in range(NUM_BLOBS)
        ]
        data_by_uid = {}
        for block in blocks:
            data = self.random_bytes(BLOB_SIZE)
            self.assertEqual(BLOB_SIZE, len(data))
            self.storage.save(block, data)
            data_by_uid[block.uid] = data

        self.storage.wait_saves_finished()

        for saved_block in self.storage.save_get_completed(timeout=1):
            pass

        saved_uids = self.storage.list_blocks()
        self.assertEqual(NUM_BLOBS, len(saved_uids))

        uids_set = set([block.uid for block in blocks])
        saved_uids_set = set(saved_uids)
        self.assertEqual(NUM_BLOBS, len(uids_set))
        self.assertEqual(NUM_BLOBS, len(saved_uids_set))
        self.assertEqual(0, len(uids_set.symmetric_difference(saved_uids_set)))

        for block in blocks:
            self.storage.read(block)

        self.storage.wait_reads_finished()

        for block, data, metadata in self.storage.read_get_completed(timeout=1):
            self.assertEqual(data_by_uid[block.uid], data)

        self.assertEqual([], [future for future in self.storage.read_get_completed(timeout=1)])

        for block in blocks:
            self.storage.rm(block.uid)
        saved_uids = self.storage.list_blocks()
        self.assertEqual(0, len(saved_uids))

    def _test_rm_many(self):
        NUM_BLOBS = 15

        blocks = [Mock('Block', uid=BlockUid(i + 1, i + 100), size=1, checksum='CHECKSUM') for i in range(NUM_BLOBS)]
        for block in blocks:
            self.storage.save(block, b'B', sync=True)

        self.assertEqual([], self.storage.rm_many([block.uid for block in blocks]))

        saved_uids = self.storage.list_blocks()
        self.assertEqual(0, len(saved_uids))

    def test_rm_many(self):
        self._test_rm_many()

    def test_rm_many_wo_multidelete(self):
        if hasattr(self.storage, '_multi_delete') and self.storage._multi_delete:
            self.storage.multi_delete = False
            self._test_rm_many()
        else:
            self.skipTest('not applicable to this backend')

    def test_not_exists(self):
        block = Mock(Block, uid=BlockUid(1, 2), size=15, checksum='CHECKSUM')
        self.storage.save(block, b'test_not_exists', sync=True)

        data = self.storage.read(block, sync=True)
        self.assertTrue(len(data) > 0)

        self.storage.rm(block.uid)

        self.assertRaises(FileNotFoundError, lambda: self.storage.rm(block.uid))
        self.assertRaises(FileNotFoundError, lambda: self.storage.read(block, sync=True))

    def test_block_uid_to_key(self):
        for i in range(100):
            block_uid = BlockUid(random.randint(1, pow(2, 32) - 1), random.randint(1, pow(2, 32) - 1))
            key = self.storage._block_uid_to_key(block_uid)
            block_uid_2 = self.storage._key_to_block_uid(key)
            self.assertEqual(block_uid, block_uid_2)
            self.assertEqual(block_uid.left, block_uid_2.left)
            self.assertEqual(block_uid.right, block_uid_2.right)

    def test_version_uid_to_key(self):
        for i in range(100):
            version_uid = VersionUid(random.randint(1, pow(2, 32) - 1))
            key = self.storage._version_uid_to_key(version_uid)
            version_uid_2 = self.storage._key_to_version_uid(key)
            self.assertEqual(version_uid, version_uid_2)

    def test_version(self):
        version_uid = VersionUid(1)
        self.storage.save_version(version_uid, 'Hallo')
        data = self.storage.read_version(version_uid)
        self.assertEqual('Hallo', data)
        version_uids = self.storage.list_versions()
        self.assertTrue(len(version_uids) == 1)
        self.assertEqual(version_uid, version_uids[0])
        self.storage.rm_version(version_uid)
        version_uids = self.storage.list_versions()
        self.assertTrue(len(version_uids) == 0)
