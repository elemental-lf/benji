import random
from unittest import TestCase

from benji.blockuidhistory import BlockUidHistory
from benji.database import BlockUid


class BlockUidHistoryTestCase(TestCase):

    def test_seen(self):
        history = BlockUidHistory()
        blocks_in_1 = set()
        blocks_out_1 = set()
        block_exists_1 = set()
        blocks_in_2 = set()
        blocks_out_2 = set()
        block_exists_2 = set()
        for i in range(0, 100000):
            block = BlockUid(random.randint(1, 2**8), random.randint(1, 2**64 - 1))
            if block in block_exists_1:
                continue
            block_exists_1.add(block)
            if random.randint(1, 100) > 20:
                blocks_in_1.add(block)
                history.add(1, block)
                self.assertTrue(history.seen(1, block))
            else:
                blocks_out_1.add(block)
                self.assertFalse(history.seen(1, block))
        for i in range(0, 100000):
            block = BlockUid(random.randint(1, 2**8), random.randint(1, 2**20))
            if block in block_exists_2:
                continue
            block_exists_2.add(block)
            if random.randint(1, 100) > 20:
                blocks_in_2.add(block)
                history.add(2, block)
                self.assertTrue(history.seen(2, block))
            else:
                blocks_out_2.add(block)
                self.assertFalse(history.seen(2, block))
        for block in blocks_in_1:
            self.assertTrue(history.seen(1, block))
        for block in blocks_out_1:
            self.assertFalse(history.seen(1, block))
        for block in blocks_in_2:
            self.assertTrue(history.seen(2, block))
        for block in blocks_out_2:
            self.assertFalse(history.seen(2, block))

    def test_oom(self):
        history = BlockUidHistory()
        for i in range(0, 1000000):
            storage_id = random.randint(1, 4)
            block = BlockUid(random.randint(1, 2**8), random.randint(1, 2**24))
            history.add(storage_id, block)
            self.assertTrue(history.seen(storage_id, block))