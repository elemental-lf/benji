import logging
import os
import random
import shutil
import string
from binascii import hexlify

from benji.benji import Benji
from benji.config import Config
from benji.factory import StorageFactory, IOFactory
from benji.logging import init_logging
from benji.metadata import MetadataBackend


class TestCase():

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
            self.path = 'benji-test_' + TestCase.random_string(16)
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

        self.config = Config(cfg=self.CONFIG.format(testpath=self.testpath.path))

    def tearDown(self):
        self.testpath.close()


class DataBackendTestCase(TestCase):

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


class SQLTestCase(TestCase):

    def setUp(self):
        super().setUp()

        metadata_backend = MetadataBackend(self.config)
        metadata_backend.initdb(_migratedb=False, _destroydb=True)
        self.metadata_backend = metadata_backend.open(_migratedb=False)

    def tearDown(self):
        if hasattr(self, 'data_backend'):
            uids = self.data_backend.list_blocks()
            self.assertEqual(0, len(uids))
            self.data_backend.close()
        if hasattr(self, 'metadata_backend'):
            self.metadata_backend.close()
        super().tearDown()


class BenjiTestCase(TestCase):

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def benjiOpen(self, initdb=False, block_size=None, in_memory=False):
        self.benji = Benji(
            self.config, initdb=initdb, _destroydb=initdb, _migratedb=False, block_size=block_size, in_memory=in_memory)
        return self.benji
