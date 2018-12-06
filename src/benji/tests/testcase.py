import logging
import os
import random
import shutil
import string
from binascii import hexlify

from benji.benji import Benji
from benji.config import Config
from benji.factory import StorageFactory
from benji.logging import init_logging
from benji.database import DatabaseBackend


class TestCaseBase:

    @staticmethod
    def random_string(length):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    @staticmethod
    def random_bytes(length):
        return bytes(random.getrandbits(8) for _ in range(length))

    @staticmethod
    def random_hex(length):
        return hexlify(bytes(random.getrandbits(8) for _ in range(length))).decode('ascii')

    class TestPath():

        def __init__(self):
            self.path = 'benji-test_' + TestCaseBase.random_string(16)
            for dir in [
                    self.path, self.path + '/data', self.path + '/data-2', self.path + '/lock',
                    self.path + '/nbd-cache', self.path + '/read-cache'
            ]:
                os.mkdir(dir)

        def close(self):
            pass
            shutil.rmtree(self.path)

    def setUp(self):
        self.testpath = self.TestPath()
        init_logging(None, logging.DEBUG)

        self.config = Config(ad_hoc_config=self.CONFIG.format(testpath=self.testpath.path))

    def tearDown(self):
        self.testpath.close()


class StorageTestCaseBase(TestCaseBase):

    def setUp(self):
        super().setUp()

        default_storage = self.config.get('defaultStorage', types=str)
        StorageFactory.initialize(self.config)

        self.storage = StorageFactory.get_by_name(default_storage)
        self.storage.rm_many(self.storage.list_blocks())
        for version_uid in self.storage.list_versions():
            self.storage.rm_version(version_uid)

    def tearDown(self):
        uids = self.storage.list_blocks()
        self.assertEqual(0, len(uids))
        StorageFactory.close()
        super().tearDown()


class DatabaseBackendTestCaseBase(TestCaseBase):

    def setUp(self):
        super().setUp()

        database_backend = DatabaseBackend(self.config)
        database_backend.init(_migrate=False, _destroy=True)
        self.database_backend = database_backend.open(_migrate=False)

    def tearDown(self):
        if hasattr(self, 'data_backend'):
            uids = self.data_backend.list_blocks()
            self.assertEqual(0, len(uids))
            self.data_backend.close()
        if hasattr(self, 'database_backend'):
            self.database_backend.close()
        super().tearDown()


class BenjiTestCaseBase(TestCaseBase):

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def benjiOpen(self, init_database=False, block_size=None, in_memory_database=False):
        self.benji = Benji(
            self.config,
            init_database=init_database,
            _destroy_database=init_database,
            _migrate_database=False,
            block_size=block_size,
            in_memory_database=in_memory_database)
        return self.benji
