# This is an port and update of the original smoketest.py
import datetime
import json
import os
import random
import uuid
from collections.abc import Iterable
from io import StringIO
from unittest import TestCase

from benji.database import VersionUid, VersionStatus, Block, Label
from benji.logging import logger
from benji.tests.testcase import BenjiTestCaseBase
from benji.utils import hints_from_rbd_diff
from benji.versions import VERSIONS

kB = 1024
MB = kB * 1024
GB = MB * 1024


class ImportExportTestCase():

    @staticmethod
    def patch(filename, offset, data=None):
        """ write data into a file at offset """
        if not os.path.exists(filename):
            open(filename, 'wb').close()
        with open(filename, 'r+b') as f:
            f.seek(offset)
            f.write(data)

    def generate_versions(self, testpath):
        version_uids = []
        old_size = 0
        init_database = True
        image_filename = os.path.join(testpath, 'image')
        for i in range(self.VERSIONS):
            logger.debug('Run {}'.format(i + 1))
            hints = []
            if old_size and random.randint(0, 10) == 0:  # every 10th time or so do not apply any changes.
                size = old_size
            else:
                size = 32 * 4 * kB + random.randint(-4 * kB, 4 * kB)
                old_size = size
                for j in range(random.randint(0, 10)):  # up to 10 changes
                    if random.randint(0, 1):
                        patch_size = random.randint(0, 4 * kB)
                        data = self.random_bytes(patch_size)
                        exists = "true"
                    else:
                        patch_size = random.randint(0, 4 * 4 * kB)  # we want full blocks sometimes
                        data = b'\0' * patch_size
                        exists = "false"
                    offset = random.randint(0, size - 1 - patch_size)
                    logger.debug('Applied change at {}:{}, exists {}'.format(offset, patch_size, exists))
                    self.patch(image_filename, offset, data)
                    hints.append({'offset': offset, 'length': patch_size, 'exists': exists})
            # truncate?
            if not os.path.exists(image_filename):
                open(image_filename, 'wb').close()
            with open(image_filename, 'r+b') as f:
                f.truncate(size)

            logger.debug('Applied {} changes, size is {}.'.format(len(hints), size))
            with open(os.path.join(testpath, 'hints'), 'w') as f:
                f.write(json.dumps(hints))

            benji_obj = self.benji_open(init_database=init_database)
            init_database = False
            with open(os.path.join(testpath, 'hints')) as hints:
                version = benji_obj.backup(version_uid=VersionUid(str(uuid.uuid4())),
                                           volume='data-backup',
                                           snapshot='snapshot-name',
                                           source='file:' + image_filename,
                                           hints=hints_from_rbd_diff(hints.read()))
            version_uids.append((version.uid, size))
            benji_obj.close()
        return version_uids

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_export(self):
        benji_obj = self.benji_open(init_database=True)
        benji_obj.close()
        self.version_uids = self.generate_versions(self.testpath.path)
        benji_obj = self.benji_open()
        with StringIO() as f:
            benji_obj.metadata_export([version_uid[0] for version_uid in self.version_uids], f)
            f.seek(0)
            export = json.load(f)
        benji_obj.close()
        self.assertEqual(str(VERSIONS.database_metadata.current), export['metadata_version'])
        self.assertIsInstance(export['versions'], list)
        self.assertTrue(len(export['versions']) == 3)
        version = export['versions'][0]
        expected_version_uid = self.version_uids[0][0]
        self.assertEqual(expected_version_uid, version['uid'])
        self.assertEqual('data-backup', version['volume'])
        self.assertEqual('snapshot-name', version['snapshot'])
        self.assertEqual(4096, version['block_size'])
        self.assertEqual(VersionStatus.valid.name, version['status'])
        self.assertFalse(version['protected'])
        self.assertEqual('file', version['storage'])

        self.assertIsInstance(version['blocks'], list)
        for block in version['blocks']:
            self.assertIsInstance(block['uid'], dict)
            self.assertIsInstance(block['uid']['left'], (int, type(None)))
            self.assertIsInstance(block['uid']['right'], (int, type(None)))
            self.assertIsInstance(block['idx'], int)
            self.assertIsInstance(block['size'], int)
            self.assertIsInstance(block['valid'], bool)
            self.assertIsInstance(block['checksum'], (str, type(None)))

    def test_import_1_0_0(self):
        benji_obj = self.benji_open(init_database=True)

        version_uid = VersionUid('V0000000001')
        benji_obj.metadata_import(StringIO(self.IMPORT_1_0_0))
        version = benji_obj.get_version_by_uid(version_uid=version_uid)
        self.assertTrue(isinstance(version.uid, VersionUid))
        self.assertEqual(version_uid, version.uid)
        self.assertEqual('data-backup', version.volume)
        self.assertEqual('snapshot-name', version.snapshot)
        self.assertEqual(4194304, version.block_size)
        self.assertEqual(version.status, VersionStatus.valid)
        self.assertFalse(version.protected)
        self.assertIsInstance(version.blocks, Iterable)
        self.assertIsInstance(version.labels, dict)
        self.assertEqual(datetime.datetime.strptime('2018-12-19T20:28:18.123456', '%Y-%m-%dT%H:%M:%S.%f'), version.date)

        self.assertIsNone(version.bytes_read)
        self.assertIsNone(version.bytes_written)
        self.assertIsNone(version.bytes_deduplicated)
        self.assertIsNone(version.bytes_sparse)
        self.assertIsNone(version.duration)

        self.assertTrue(len(list(version.blocks)) > 0)
        block = list(version.blocks)[0]
        self.assertEqual(version.id, block.version_id)
        self.assertEqual(0, block.idx)
        self.assertEqual(670293, block.size)
        self.assertTrue(block.valid)

        benji_obj.close()

    def test_import_1_1_0(self):
        benji_obj = self.benji_open(init_database=True)

        version_uid = VersionUid('V0000000001')
        benji_obj.metadata_import(StringIO(self.IMPORT_1_1_0))
        version = benji_obj.get_version_by_uid(version_uid=version_uid)
        self.assertTrue(isinstance(version.uid, VersionUid))
        self.assertEqual(version_uid, version.uid)
        self.assertEqual('data-backup', version.volume)
        self.assertEqual('snapshot-name', version.snapshot)
        self.assertEqual(4194304, version.block_size)
        self.assertEqual(version.status, VersionStatus.valid)
        self.assertFalse(version.protected)
        self.assertIsInstance(version.blocks, Iterable)
        self.assertIsInstance(version.labels, dict)
        self.assertEqual(datetime.datetime.strptime('2018-12-19T20:28:18.123456', '%Y-%m-%dT%H:%M:%S.%f'), version.date)

        self.assertEqual(1, version.bytes_read)
        self.assertEqual(2, version.bytes_written)
        self.assertEqual(3, version.bytes_deduplicated)
        self.assertEqual(4, version.bytes_sparse)
        self.assertEqual(5, version.duration)

        for label in version.labels.values():
            self.assertIsInstance(label, Label)

        self.assertSetEqual({'label-1', 'label-2'}, set(version.labels.keys()))
        self.assertEqual('label-1', version.labels['label-1'].name)
        self.assertEqual('label-2', version.labels['label-2'].name)
        self.assertEqual('bla', version.labels['label-1'].value)
        self.assertEqual('blub', version.labels['label-2'].value)
        self.assertEqual(version.id, version.labels['label-1'].version_id)
        self.assertEqual(version.id, version.labels['label-2'].version_id)

        self.assertTrue(len(list(version.blocks)) > 0)
        block = list(version.blocks)[0]
        self.assertIsInstance(block, Block)
        self.assertEqual(version.id, block.version_id)
        self.assertEqual(0, block.idx)
        self.assertEqual(670293, block.size)
        self.assertTrue(block.valid)

        benji_obj.close()

    def _test_import_2_and_3(self, import_source: str) -> None:
        benji_obj = self.benji_open(init_database=True)

        version_uid = VersionUid('V0000000001')
        benji_obj.metadata_import(StringIO(import_source))
        version = benji_obj.get_version_by_uid(version_uid=version_uid)
        self.assertTrue(isinstance(version.uid, VersionUid))
        self.assertEqual(version_uid, version.uid)
        self.assertEqual('data-backup', version.volume)
        self.assertEqual('snapshot-name', version.snapshot)
        self.assertEqual(4864597, version.size)
        self.assertEqual(4194304, version.block_size)
        self.assertEqual(version.status, VersionStatus.valid)
        self.assertFalse(version.protected)
        self.assertIsInstance(version.blocks, Iterable)
        self.assertIsInstance(version.labels, dict)
        self.assertEqual(datetime.datetime.strptime('2018-12-19T20:28:18.123456', '%Y-%m-%dT%H:%M:%S.%f'), version.date)

        self.assertEqual(1, version.bytes_read)
        self.assertEqual(2, version.bytes_written)
        self.assertEqual(3, version.bytes_deduplicated)
        self.assertEqual(4, version.bytes_sparse)
        self.assertEqual(5, version.duration)

        for label in version.labels.values():
            self.assertIsInstance(label, Label)

        self.assertSetEqual({'label-1', 'label-2'}, set(version.labels.keys()))
        self.assertEqual('label-1', version.labels['label-1'].name)
        self.assertEqual('label-2', version.labels['label-2'].name)
        self.assertEqual('bla', version.labels['label-1'].value)
        self.assertEqual('blub', version.labels['label-2'].value)
        self.assertEqual(version.id, version.labels['label-1'].version_id)
        self.assertEqual(version.id, version.labels['label-2'].version_id)

        self.assertEqual(2, len(list(version.blocks)))
        block = list(version.blocks)[0]
        self.assertIsInstance(block, Block)
        self.assertEqual(version.id, block.version_id)
        self.assertEqual(0, block.idx)
        self.assertEqual(4194304, block.size)
        self.assertTrue(block.valid)
        block = list(version.blocks)[1]
        self.assertIsInstance(block, Block)
        self.assertEqual(version.id, block.version_id)
        self.assertEqual(1, block.idx)
        self.assertEqual(670293, block.size)
        self.assertTrue(block.valid)
        self.assertEqual(None, block.checksum)

        benji_obj.close()

    def test_import_2_0_0(self) -> None:
        self._test_import_2_and_3(self.IMPORT_2_0_0)

    def test_import_3_0_0(self) -> None:
        self._test_import_2_and_3(self.IMPORT_3_0_0)

    IMPORT_1_0_0 = """
            {
              "versions": [
                {
                  "uid": 1,
                  "date": "2018-12-19T20:28:18.123456",
                  "name": "data-backup",
                  "snapshot_name": "snapshot-name",
                  "size": 670293,
                  "block_size": 4194304,
                  "storage_id": 1,
                  "status": "valid",
                  "protected": false,
                  "labels": [],
                  "blocks": [
                    {
                      "uid": {
                        "left": 1,
                        "right": 1
                      },
                      "id": 0,
                      "size": 670293,
                      "valid": true,
                      "checksum": "066dde4d22ebc3e72c485a6a38b9013ac8efa4e4951a9b1c301e3d6579e25564"
                    }
                  ]
                },
                {
                  "uid": 2,
                  "date": "2018-12-19T20:28:19.123456",
                  "name": "test",
                  "snapshot_name": "",
                  "size": 670293,
                  "block_size": 4194304,
                  "storage_id": 1,
                  "status": "valid",
                  "protected": false,
                  "labels": [],
                  "blocks": [
                    {
                      "uid": {
                        "left": 1,
                        "right": 1
                      },
                      "id": 0,
                      "size": 670293,
                      "valid": true,
                      "checksum": "066dde4d22ebc3e72c485a6a38b9013ac8efa4e4951a9b1c301e3d6579e25564"
                    }
                  ]
                },
                {
                  "uid": 3,
                  "date": "2018-12-19T20:28:21.123456",
                  "name": "test",
                  "snapshot_name": "",
                  "size": 670293,
                  "block_size": 4194304,
                  "storage_id": 1,
                  "status": "valid",
                  "protected": false,
                  "labels": [],
                  "blocks": [
                    {
                      "uid": {
                        "left": 1,
                        "right": 1
                      },
                      "id": 0,
                      "size": 670293,
                      "valid": true,
                      "checksum": "066dde4d22ebc3e72c485a6a38b9013ac8efa4e4951a9b1c301e3d6579e25564"
                    }
                  ]
                }
              ],
              "metadata_version": "1.0.0"
            }
            """

    IMPORT_1_1_0 = """
            {
              "versions": [
                {
                  "uid": 1,
                  "date": "2018-12-19T20:28:18.123456",
                  "name": "data-backup",
                  "snapshot_name": "snapshot-name",
                  "size": 670293,
                  "block_size": 4194304,
                  "storage_id": 1,
                  "status": "valid",
                  "protected": false,
                  "bytes_read": 1,
                  "bytes_written": 2,
                  "bytes_dedup": 3,
                  "bytes_sparse": 4,
                  "duration": 5,
                  "labels": [
                    {
                      "name": "label-1",
                      "value": "bla"
                    },
                    {
                      "name": "label-2",
                      "value": "blub"
                    }
                  ],
                  "blocks": [
                    {
                      "uid": {
                        "left": 1,
                        "right": 1
                      },
                      "id": 0,
                      "size": 670293,
                      "valid": true,
                      "checksum": "066dde4d22ebc3e72c485a6a38b9013ac8efa4e4951a9b1c301e3d6579e25564"
                    }
                  ]
                },
                {
                  "uid": 2,
                  "date": "2018-12-19T20:28:18.123456",
                  "name": "data-backup",
                  "snapshot_name": "snapshot-name",
                  "size": 670293,
                  "block_size": 4194304,
                  "storage_id": 1,
                  "status": "valid",
                  "protected": false,
                  "bytes_read": 1,
                  "bytes_written": 2,
                  "bytes_dedup": 3,
                  "bytes_sparse": 4,
                  "duration": 5,
                  "labels": [
                    {
                      "name": "label-1",
                      "value": "bla"
                    },
                    {
                      "name": "label-2",
                      "value": "blub"
                    }
                  ],
                  "blocks": [
                    {
                      "uid": {
                        "left": 1,
                        "right": 1
                      },
                      "id": 0,
                      "size": 670293,
                      "valid": true,
                      "checksum": "066dde4d22ebc3e72c485a6a38b9013ac8efa4e4951a9b1c301e3d6579e25564"
                    }
                  ]
                },
                {
                  "uid": 3,
                  "date": "2018-12-19T20:28:18.123456",
                  "name": "data-backup",
                  "snapshot_name": "snapshot-name",
                  "size": 670293,
                  "block_size": 4194304,
                  "storage_id": 1,
                  "status": "valid",
                  "protected": false,
                  "bytes_read": 1,
                  "bytes_written": 2,
                  "bytes_dedup": 3,
                  "bytes_sparse": 4,
                  "duration": 5,
                  "labels": [
                    {
                      "name": "label-1",
                      "value": "bla"
                    },
                    {
                      "name": "label-2",
                      "value": "blub"
                    }
                  ],
                  "blocks": [
                    {
                      "uid": {
                        "left": 1,
                        "right": 1
                      },
                      "id": 0,
                      "size": 670293,
                      "valid": true,
                      "checksum": "066dde4d22ebc3e72c485a6a38b9013ac8efa4e4951a9b1c301e3d6579e25564"
                    }
                  ]
                }
              ],
              "metadata_version": "1.1.0"
            }
            """

    IMPORT_2_0_0 = """
            {
              "versions": [
                {
                  "uid": "V0000000001",
                  "date": "2018-12-19T20:28:18.123456Z",
                  "volume": "data-backup",
                  "snapshot": "snapshot-name",
                  "size": 4864597,
                  "block_size": 4194304,
                  "storage": "file",
                  "status": "valid",
                  "protected": false,
                  "bytes_read": 1,
                  "bytes_written": 2,
                  "bytes_deduplicated": 3,
                  "bytes_sparse": 4,
                  "duration": 5,
                  "labels": {
                    "label-1": "bla",
                    "label-2": "blub"
                  },
                  "blocks": [
                    {
                      "uid": {
                        "left": 1,
                        "right": 1
                      },
                      "idx": 0,
                      "size": 4194304,
                      "valid": true,
                      "checksum": "066dde4d22ebc3e72c485a6a38b9013ac8efa4e4951a9b1c301e3d6579e25564"
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "idx": 1,
                      "size": 670293,
                      "valid": true,
                      "checksum": null
                    }
                  ]
                },
                {
                  "uid": "v000000002",
                  "date": "2018-12-19T20:28:18.123456Z",
                  "volume": "data-backup",
                  "snapshot": "snapshot-name",
                  "size": 670293,
                  "block_size": 4194304,
                  "storage": "file",
                  "status": "valid",
                  "protected": false,
                  "bytes_read": 1,
                  "bytes_written": 2,
                  "bytes_deduplicated": 3,
                  "bytes_sparse": 4,
                  "duration": 5,
                   "labels": {
                    "label-1": "bla",
                    "label-2": "blub"
                  },
                  "blocks": [
                    {
                      "uid": {
                        "left": 1,
                        "right": 1
                      },
                      "idx": 0,
                      "size": 670293,
                      "valid": true,
                      "checksum": "066dde4d22ebc3e72c485a6a38b9013ac8efa4e4951a9b1c301e3d6579e25564"
                    }
                  ]
                },
                {
                  "uid": "v000000003",
                  "date": "2018-12-19T20:28:18.123456Z",
                  "volume": "data-backup",
                  "snapshot": "snapshot-name",
                  "size": 670293,
                  "block_size": 4194304,
                  "storage": "file",
                  "status": "valid",
                  "protected": false,
                  "bytes_read": 1,
                  "bytes_written": 2,
                  "bytes_deduplicated": 3,
                  "bytes_sparse": 4,
                  "duration": 5,
                  "labels": {
                    "label-1": "bla",
                    "label-2": "blub"
                  },
                  "blocks": [
                    {
                      "uid": {
                        "left": 1,
                        "right": 1
                      },
                      "idx": 0,
                      "size": 670293,
                      "valid": true,
                      "checksum": "066dde4d22ebc3e72c485a6a38b9013ac8efa4e4951a9b1c301e3d6579e25564"
                    }
                  ]
                }
              ],
              "metadata_version": "2.0.0"
            }
            """

    IMPORT_3_0_0 = """
            {
              "versions": [
                {
                  "uid": "V0000000001",
                  "date": "2018-12-19T20:28:18.123456Z",
                  "volume": "data-backup",
                  "snapshot": "snapshot-name",
                  "size": 4864597,
                  "block_size": 4194304,
                  "storage": "file",
                  "status": "valid",
                  "protected": false,
                  "bytes_read": 1,
                  "bytes_written": 2,
                  "bytes_deduplicated": 3,
                  "bytes_sparse": 4,
                  "duration": 5,
                  "labels": {
                    "label-1": "bla",
                    "label-2": "blub"
                  },
                  "blocks": [
                    {
                      "uid": {
                        "left": 1,
                        "right": 1
                      },
                      "idx": 0,
                      "size": 4194304,
                      "valid": true,
                      "checksum": "066dde4d22ebc3e72c485a6a38b9013ac8efa4e4951a9b1c301e3d6579e25564"
                    },
                    {
                      "uid": {
                        "left": null,
                        "right": null
                      },
                      "idx": 1,
                      "size": 670293,
                      "valid": true,
                      "checksum": null
                    }
                  ]
                },
                {
                  "uid": "v000000002",
                  "date": "2018-12-19T20:28:18.123456Z",
                  "volume": "data-backup",
                  "snapshot": "snapshot-name",
                  "size": 670293,
                  "block_size": 4194304,
                  "storage": "file",
                  "status": "valid",
                  "protected": false,
                  "bytes_read": 1,
                  "bytes_written": 2,
                  "bytes_deduplicated": 3,
                  "bytes_sparse": 4,
                  "duration": 5,
                   "labels": {
                    "label-1": "bla",
                    "label-2": "blub"
                  },
                  "blocks": [
                    {
                      "uid": {
                        "left": 1,
                        "right": 1
                      },
                      "idx": 0,
                      "size": 670293,
                      "valid": true,
                      "checksum": "066dde4d22ebc3e72c485a6a38b9013ac8efa4e4951a9b1c301e3d6579e25564"
                    }
                  ]
                },
                {
                  "uid": "v000000003",
                  "date": "2018-12-19T20:28:18.123456Z",
                  "volume": "data-backup",
                  "snapshot": "snapshot-name",
                  "size": 670293,
                  "block_size": 4194304,
                  "storage": "file",
                  "status": "valid",
                  "protected": false,
                  "bytes_read": 1,
                  "bytes_written": 2,
                  "bytes_deduplicated": 3,
                  "bytes_sparse": 4,
                  "duration": 5,
                  "labels": {
                    "label-1": "bla",
                    "label-2": "blub"
                  },
                  "blocks": [
                    {
                      "uid": {
                        "left": 1,
                        "right": 1
                      },
                      "idx": 0,
                      "size": 670293,
                      "valid": true,
                      "checksum": "066dde4d22ebc3e72c485a6a38b9013ac8efa4e4951a9b1c301e3d6579e25564"
                    }
                  ]
                }
              ],
              "metadata_version": "3.0.0"
            }
            """


class ImportExportCaseSQLLite_File(ImportExportTestCase, BenjiTestCaseBase, TestCase):

    VERSIONS = 3

    CONFIG = """
            configurationVersion: '1'
            processName: benji
            logFile: /dev/stderr
            blockSize: 4096
            defaultStorage: file
            storages:
              - name: file
                module: file
                storageId: 1
                configuration:
                  path: {testpath}/data
            ios:
              - name: file
                module: file
            databaseEngine: sqlite:///{testpath}/benji.sqlite
            """


class ImportExportTestCasePostgreSQL_File(ImportExportTestCase, BenjiTestCaseBase, TestCase):

    VERSIONS = 3

    CONFIG = """
            configurationVersion: '1'
            processName: benji
            logFile: /dev/stderr
            blockSize: 4096
            defaultStorage: file
            storages:
              - name: file
                module: file
                storageId: 1
                configuration:
                  path: {testpath}/data
            ios:
              - name: file
                module: file                                 
            databaseEngine: postgresql://benji:verysecret@localhost:15432/benji
            """
