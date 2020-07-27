import logging
import os
import random
import shutil
import string
from binascii import hexlify

from benji.benji import Benji
from benji.config import Config
from benji.database import Database
from benji.io.factory import IOFactory
from benji.logging import init_logging
from benji.storage.factory import StorageFactory


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
            self.path = '../../../tests-scratch/benji-test_' + TestCaseBase.random_string(16)
            for dir in [
                    self.path,
                    self.path + '/data',
                    self.path + '/data-2',
                    self.path + '/lock',
            ]:
                os.makedirs(dir, exist_ok=True)

        def close(self):
            pass
            shutil.rmtree(self.path)

    def setUp(self):
        self.testpath = self.TestPath()
        init_logging(console_level=logging.WARN if os.environ.get('UNITTEST_QUIET', False) else logging.DEBUG,
                     console_formatter='console-plain')
        self.config = Config(ad_hoc_config=self.CONFIG.format(testpath=self.testpath.path))

    def tearDown(self):
        self.testpath.close()


class StorageTestCaseBase(TestCaseBase):

    def setUp(self):
        super().setUp()

        StorageFactory.initialize(self.config)

        default_storage_name = self.config.get('defaultStorage', types=str)
        self.storage = StorageFactory.get_by_name(default_storage_name)
        for block_uid in self.storage.list_blocks():
            self.storage.rm_block(block_uid)
        for version_uid in self.storage.list_versions():
            self.storage.rm_version(version_uid)

    def tearDown(self):
        uids = list(self.storage.list_blocks())
        self.assertEqual(0, len(uids))
        StorageFactory.close()
        super().tearDown()


class DatabaseBackendTestCaseBase(TestCaseBase):

    def setUp(self):
        super().setUp()

        Database.configure(self.config)
        Database.init(_destroy=True)
        Database.open()

    def tearDown(self):
        Database.close()
        super().tearDown()


class BenjiTestCaseBase(TestCaseBase):

    def setUp(self):
        super().setUp()
        IOFactory.initialize(self.config)
        StorageFactory.initialize(self.config)

    def tearDown(self):
        StorageFactory.close()
        super().tearDown()

    def benji_open(self, init_database=False, in_memory_database=False):
        self.benji = Benji(self.config,
                           init_database=init_database,
                           in_memory_database=in_memory_database,
                           _destroy_database=init_database)
        return self.benji
