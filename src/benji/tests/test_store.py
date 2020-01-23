import os
import random
import unittest
import uuid
from unittest import TestCase

from parameterized import parameterized

from benji.benji import BenjiStore
from benji.tests.testcase import BenjiTestCaseBase

kB = 1024
MB = kB * 1024
GB = MB * 1024


class BenjiStoreTestCase(BenjiTestCaseBase):

    def generate_version(self, testpath):
        size = 512 * kB + 123
        image_filename = os.path.join(testpath, 'image')
        self.image = self.random_bytes(size - 2 * 128123) + b'\0' * 128123 + self.random_bytes(128123)
        with open(image_filename, 'wb') as f:
            f.write(self.image)
        benji_obj = self.benji_open(init_database=True)
        version = benji_obj.backup(version_uid=str(uuid.uuid4()),
                                   volume='data-backup',
                                   snapshot='snapshot-name',
                                   source='file:' + image_filename)
        version_uid = version.uid
        benji_obj.close()
        return version_uid, size, image_filename

    def setUp(self):
        super().setUp()
        version_uid, size, image_filename = self.generate_version(self.testpath.path)
        self.version_uid = version_uid
        self.size = size
        self.image_filename = image_filename

    def test_find_versions(self):
        benji_obj = self.benji_open()
        store = BenjiStore(benji_obj)
        versions = store.find_versions()
        self.assertEqual(1, len(versions))
        self.assertEqual(self.version_uid, versions[0].uid)
        self.assertEqual(self.size, versions[0].size)
        benji_obj.close()

    @parameterized.expand([(512,), (1024,), (4096,), (65536,), (1861,)])
    def test_read(self, block_size):
        benji_obj = self.benji_open()
        store = BenjiStore(benji_obj)
        version = store.find_versions(version_uid=self.version_uid)[0]
        store.open(version)
        image = bytearray()
        for pos in range(0, self.size, block_size):
            if pos + block_size > self.size:
                read_length = self.size - pos
            else:
                read_length = block_size
            image = image + store.read(version, None, pos, read_length)
        self.assertEqual(self.size, len(image))
        self.assertEqual(self.image, image)
        store.close(version)
        benji_obj.close()

    def test_create_cow_version(self):
        benji_obj = self.benji_open()
        store = BenjiStore(benji_obj)
        version = store.find_versions(version_uid=self.version_uid)[0]
        store.open(version)
        cow_version = store.create_cow_version(version)
        self.assertEqual(version.volume, cow_version.volume)
        self.assertEqual(version.size, cow_version.size)
        self.assertEqual(version.block_size, cow_version.block_size)
        self.assertEqual(version.storage_id, cow_version.storage_id)
        self.assertNotEqual(version.snapshot, cow_version.snapshot)
        store.fixate(cow_version)
        store.close(version)
        benji_obj.close()

    @parameterized.expand([['{:03}'.format(run)] for run in range(51)])
    def test_write_read(self, run):
        benji_obj = self.benji_open()
        store = BenjiStore(benji_obj)
        version = store.find_versions(version_uid=self.version_uid)[0]
        store.open(version)
        cow_version = store.create_cow_version(version)

        image_2_filename = os.path.join(self.testpath.path, 'image')
        image_2 = bytearray(self.image)

        block_size = random.randint(512, 2 * 65536)
        for pos in range(0, self.size, block_size):
            if pos + block_size > self.size:
                write_length = self.size - pos
            else:
                write_length = block_size
            if random.randint(1, 100) <= 25:
                if random.randint(0, 1):
                    image_2[pos:pos + write_length] = self.random_bytes(write_length)
                    store.write(cow_version, pos, image_2[pos:pos + write_length])
                else:
                    image_2[pos:pos + write_length] = b'\0' * write_length
                    store.write(cow_version, pos, b'\0' * write_length)

        with open(image_2_filename, 'wb') as f:
            f.write(image_2)

        for block_size in (512, 1024, 4096, 65536, 1861):
            image = bytearray()
            for pos in range(0, self.size, block_size):
                if pos + block_size > self.size:
                    read_length = self.size - pos
                else:
                    read_length = block_size
                image = image + store.read(version, cow_version, pos, read_length)
            self.assertEqual(self.size, len(image))
            for pos in range(0, self.size):
                if image_2[pos] != image[pos]:
                    self.fail('Written image different at offset {} (block size {}).'.format(pos, block_size))
                    break

        store.fixate(cow_version)

        benji_obj.deep_scrub(cow_version.uid, 'file:{}'.format(image_2_filename))

        store.close(version)
        benji_obj.close()


class BenjiStoreTestCaseSQLLite_File(BenjiStoreTestCase, TestCase):

    CONFIG = """
            configurationVersion: '1'
            processName: benji
            logFile: /dev/stderr
            hashFunction: BLAKE2b,digest_bits=256
            blockSize: 65536
            ios:
            - name: file
              module: file
              configuration:
                simultaneousReads: 2
            defaultStorage: s1
            storages:
            - name: s1
              storageId: 1
              module: file
              configuration:
                path: {testpath}/data
                consistencyCheckWrites: True
                simultaneousWrites: 5
                simultaneousReads: 5                    
                activeTransforms:
                  - zstd
                  - k1
                hmac:
                  kdfSalt: BBiZ+lIVSefMCdE4eOPX211n/04KY1M4c2SM/9XHUcA=
                  kdfIterations: 1000
                  password: Hallo123     
            transforms:
            - name: zstd
              module: zstd
              configuration:
                level: 1
            - name: k1
              module: aes_256_gcm
              configuration:
                kdfSalt: BBiZ+lIVSefMCdE4eOPX211n/04KY1M4c2SM/9XHUcA=
                kdfIterations: 20000
                password: "this is a very secret password"
            databaseEngine: sqlite:///{testpath}/benji.sqlite
            nbd:
                blockCache:
                    directory: {testpath}/nbd/block-cache
                    maximumSize: 134217728
                cowStore:
                    directory: {testpath}/nbd/cow-store
            """


@unittest.skipIf(os.environ.get('UNITTEST_SKIP_POSTGRESQL', False), 'No PostgreSQL setup available.')
class BenjiStoreTestCasePostgreSQL_S3(BenjiStoreTestCase, TestCase):

    CONFIG = """
            configurationVersion: '1'
            processName: benji
            logFile: /dev/stderr
            hashFunction: SHA512
            blockSize: 65536 
            ios:
            - name: file
              module: file
              configuration:
                simultaneousReads: 2
            defaultStorage: s1
            storages:
            - name: s1
              storageId: 1
              module: s3
              configuration:
                awsAccessKeyId: minio
                awsSecretAccessKey: minio123
                endpointUrl: http://127.0.0.1:9901/
                bucketName: benji
                addressingStyle: path
                disableEncodingType: false
                consistencyCheckWrites: True
                simultaneousWrites: 5
                simultaneousReads: 5                                   
                activeTransforms:
                  - zstd
                  - k1
                hmac:
                  kdfSalt: BBiZ+lIVSefMCdE4eOPX211n/04KY1M4c2SM/9XHUcA=
                  kdfIterations: 1000
                  password: Hallo123    
            transforms:
            - name: zstd
              module: zstd
              configuration:
                level: 1
            - name: k1
              module: aes_256_gcm
              configuration:
                kdfSalt: BBiZ+lIVSefMCdE4eOPX211n/04KY1M4c2SM/9XHUcA=
                kdfIterations: 20000
                password: "this is a very secret password"
            databaseEngine: postgresql://benji:verysecret@localhost:15432/benji
            nbd:
                blockCache:
                    directory: {testpath}/nbd/block-cache
                    maximumSize: 134217728
                cowStore:
                    directory: {testpath}/nbd/cow-store
            """
