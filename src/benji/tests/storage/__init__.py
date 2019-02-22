import random

from benji.database import Block, BlockUid, VersionUid
from benji.storage.base import InvalidBlockException, BlockNotFoundError
from benji.tests.testcase import StorageTestCaseBase


class StorageTestCase(StorageTestCaseBase):

    def test_write_rm_sync(self):
        NUM_BLOBS = 15
        BLOB_SIZE = 4096

        saved_uids = self.storage.list_blocks()
        self.assertEqual(0, len(saved_uids))

        blocks = [
            Block(uid=BlockUid(i + 1, i + 100), size=BLOB_SIZE, checksum='0000000000000000') for i in range(NUM_BLOBS)
        ]
        data_by_uid = {}
        for block in blocks:
            data = self.random_bytes(BLOB_SIZE)
            self.assertEqual(BLOB_SIZE, len(data))
            self.storage.write_block(block, data)
            data_by_uid[block.uid] = data

        saved_uids = self.storage.list_blocks()
        self.assertEqual(NUM_BLOBS, len(saved_uids))

        uids_set = set([block.uid for block in blocks])
        saved_uids_set = set(saved_uids)
        self.assertEqual(NUM_BLOBS, len(uids_set))
        self.assertEqual(NUM_BLOBS, len(saved_uids_set))
        self.assertEqual(0, len(uids_set.symmetric_difference(saved_uids_set)))

        for block in blocks:
            data = self.storage.read_block(block)
            self.assertEqual(data_by_uid[block.uid], data)

        for block in blocks:
            self.storage.rm_block(block.uid)
        saved_uids = self.storage.list_blocks()
        self.assertEqual(0, len(saved_uids))

    def test_write_rm_async(self):
        NUM_BLOBS = 15
        BLOB_SIZE = 4096

        saved_uids = self.storage.list_blocks()
        self.assertEqual(0, len(saved_uids))

        blocks = [
            Block(uid=BlockUid(i + 1, i + 100), size=BLOB_SIZE, checksum='0000000000000000') for i in range(NUM_BLOBS)
        ]
        data_by_uid = {}
        for block in blocks:
            data = self.random_bytes(BLOB_SIZE)
            self.assertEqual(BLOB_SIZE, len(data))
            self.storage.write_block_async(block, data)
            data_by_uid[block.uid] = data

        self.storage.wait_writes_finished()

        for _ in self.storage.write_get_completed(timeout=1):
            pass

        saved_uids = self.storage.list_blocks()
        self.assertEqual(NUM_BLOBS, len(saved_uids))

        uids_set = set([block.uid for block in blocks])
        saved_uids_set = set(saved_uids)
        self.assertEqual(NUM_BLOBS, len(uids_set))
        self.assertEqual(NUM_BLOBS, len(saved_uids_set))
        self.assertEqual(0, len(uids_set.symmetric_difference(saved_uids_set)))

        for block in blocks:
            self.storage.read_block_async(block)

        for block, data, metadata in self.storage.read_get_completed(timeout=1):
            self.assertEqual(data_by_uid[block.uid], data)

        self.assertEqual([], [future for future in self.storage.read_get_completed(timeout=1)])

        for block in blocks:
            self.storage.rm_block_async(block.uid)

        self.storage.wait_rms_finished()

        saved_uids = self.storage.list_blocks()
        self.assertEqual(0, len(saved_uids))

    def test_not_exists(self):
        block = Block(uid=BlockUid(1, 2), size=15, checksum='00000000000000000000')
        self.storage.write_block(block, b'test_not_exists')

        data = self.storage.read_block(block)
        self.assertTrue(len(data) > 0)

        self.storage.rm_block(block.uid)

        self.assertRaises(BlockNotFoundError, lambda: self.storage.rm_block(block.uid))
        self.assertRaises(InvalidBlockException, lambda: self.storage.read_block(block))

    def test_block_uid_to_key(self):
        for i in range(100):
            block_uid = BlockUid(random.randint(1, pow(2, 32) - 1), random.randint(1, pow(2, 32) - 1))
            key = block_uid.storage_object_to_path()
            block_uid_2 = BlockUid.storage_path_to_object(key)
            self.assertEqual(block_uid, block_uid_2)
            self.assertEqual(block_uid.left, block_uid_2.left)
            self.assertEqual(block_uid.right, block_uid_2.right)

    def test_version_uid_to_key(self):
        for i in range(100):
            version_uid = VersionUid(random.randint(1, pow(2, 32) - 1))
            key = version_uid.storage_object_to_path()
            version_uid_2 = VersionUid.storage_path_to_object(key)
            self.assertEqual(version_uid, version_uid_2)

    def test_version(self):
        version_uid = VersionUid(1)
        self.storage.write_version(version_uid, 'Hallo')
        data = self.storage.read_version(version_uid)
        self.assertEqual('Hallo', data)
        version_uids = self.storage.list_versions()
        self.assertTrue(len(version_uids) == 1)
        self.assertEqual(version_uid, version_uids[0])
        self.storage.rm_version(version_uid)
        version_uids = self.storage.list_versions()
        self.assertTrue(len(version_uids) == 0)
