import os
import random
import re
import subprocess
import threading
import unittest
from unittest import TestCase

from benji.benji import BenjiStore
from benji.database import VersionUid
from benji.logging import logger
from benji.nbdserver import NbdServer
from benji.tests.testcase import BenjiTestCaseBase

kB = 1024
MB = kB * 1024
GB = MB * 1024


@unittest.skipIf(os.environ.get('UNITTEST_SKIP_NBD', False), 'No NBD setup available.')
class NbdTestCase:

    @staticmethod
    def patch(filename, offset, data=None):
        """ write data into a file at offset """
        if not os.path.exists(filename):
            open(filename, 'wb').close()
        with open(filename, 'r+b') as f:
            f.seek(offset)
            f.write(data)

    @staticmethod
    def read_file(file1):
        with open(file1, 'rb') as f1:
            data = f1.read()
        return data

    def generate_version(self, testpath):
        size = 4 * MB
        image_filename = os.path.join(testpath, 'image')
        with open(image_filename, 'wb') as f:
            f.truncate(size)
        for j in range(random.randint(20, 30)):
            patch_size = random.randint(0, 128 * kB)
            data = self.random_bytes(patch_size)
            offset = random.randint(0, size - 1 - patch_size)
            self.patch(image_filename, offset, data)

        benji_obj = self.benjiOpen(init_database=True)
        version_uid = benji_obj.backup('data-backup', 'snapshot-name', 'file:' + image_filename, None, None)
        benji_obj.close()
        return version_uid, size

    def setUp(self):
        super().setUp()
        self.version_uid = self.generate_version(self.testpath.path)

    def tearDown(self):
        self.subprocess_run(args=['sudo', 'nbd-client', '-d', self.NBD_DEVICE], check=False)
        super().tearDown()

    def test(self):
        benji_obj = self.benjiOpen()
        store = BenjiStore(benji_obj)
        addr = ('127.0.0.1', self.SERVER_PORT)
        read_only = False
        self.nbd_server = NbdServer(addr, store, read_only)
        logger.info("Starting to serve NBD on %s:%s" % (addr[0], addr[1]))

        self.subprocess_run(args=['sudo', 'modprobe', 'nbd'])

        self.nbd_client_thread = threading.Thread(target=self.nbd_client, daemon=True, args=(self.version_uid,))
        self.nbd_client_thread.start()
        self.nbd_server.serve_forever()
        self.nbd_client_thread.join()

        self.assertEqual({self.version_uid[0], VersionUid(2)}, set([version.uid for version in benji_obj.ls()]))

        benji_obj.close()

    def subprocess_run(self, args, success_regexp=None, check=True):
        completed = subprocess.run(
            args=args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding='utf-8',
            errors='ignore')

        if check and completed.returncode != 0:
            self.fail('command {} failed: {}'.format(' '.join(args), completed.stdout.replace('\n', '|')))

        if success_regexp:
            if not re.match(success_regexp, completed.stdout, re.I | re.M | re.S):
                self.fail('command {} failed: {}'.format(' '.join(args), completed.stdout.replace('\n', '|')))

    def nbd_client(self, version_uid):
        self.subprocess_run(
            args=['sudo', 'nbd-client', '127.0.0.1', '-p',
                  str(self.SERVER_PORT), '-l'],
            success_regexp='^Negotiation: ..\n{}\n$'.format(version_uid[0].v_string))

        version_uid, size = version_uid
        self.subprocess_run(
            args=[
                'sudo', 'nbd-client', '-N', version_uid.v_string, '127.0.0.1', '-p',
                str(self.SERVER_PORT), self.NBD_DEVICE
            ],
            success_regexp='^Negotiation: ..size = \d+MB\nbs=1024, sz=\d+ bytes\n$|^Negotiation: ..size = \d+MB|Connected /dev/nbd\d+$')

        count = 0
        nbd_data = bytearray()
        with open(self.NBD_DEVICE, 'rb') as f:
            while True:
                data = f.read(64 * 1024 + random.randint(0, 8192))
                if not data:
                    break
                count += len(data)
                nbd_data += data
        self.assertEqual(size, count)

        image_data = self.read_file(self.testpath.path + '/image')
        logger.info('image_data size {}, nbd_data size {}'.format(len(image_data), len(nbd_data)))
        self.assertEqual(image_data, bytes(nbd_data))

        f = os.open(self.NBD_DEVICE, os.O_RDWR)
        for offset in range(0, size, 4096):
            os.lseek(f, offset, os.SEEK_SET)
            data = self.random_bytes(4096)
            written = os.write(f, data)
            os.fsync(f)
            self.assertEqual(len(data), written)
            # Discard cache so that the read request below really goes to the NBD server
            os.posix_fadvise(f, offset, len(data), os.POSIX_FADV_DONTNEED)

            os.lseek(f, offset, os.SEEK_SET)
            read_data = os.read(f, 4096)
            self.assertEqual(data, read_data)
        os.close(f)

        self.subprocess_run(
            args=['sudo', 'nbd-client', '-d', self.NBD_DEVICE], success_regexp='^disconnect, sock, done\n$')

        # Signal NBD server to stop
        self.nbd_server.stop()


class NbdTestCaseSQLLite_File(NbdTestCase, BenjiTestCaseBase, TestCase):

    SERVER_PORT = 1315

    NBD_DEVICE = '/dev/nbd15'

    CONFIG = """
            configurationVersion: '1'
            processName: benji
            logFile: /dev/stderr
            hashFunction: BLAKE2b,digest_bits=256
            blockSize: 4096
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
              cacheDirectory: {testpath}/nbd-cache
            """


class NbdTestCasePostgreSQL_S3(NbdTestCase, BenjiTestCaseBase, TestCase):

    SERVER_PORT = 1315

    NBD_DEVICE = '/dev/nbd15'

    CONFIG = """
            configurationVersion: '1'
            processName: benji
            logFile: /dev/stderr
            hashFunction: SHA512
            blockSize: 4096
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
              cacheDirectory: {testpath}/nbd-cache
            """
