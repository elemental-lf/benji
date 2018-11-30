# This is an port and update of the original smoketest.py
import datetime
import json
import os
import random
from io import StringIO
from unittest import TestCase

from benji.metadata import MetadataBackend, VersionUid
from benji.scripts.benji import hints_from_rbd_diff
from benji.tests.testcase import BenjiTestCase

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
        base_version = None
        version_uids = []
        old_size = 0
        initdb = True
        image_filename = os.path.join(testpath, 'image')
        for i in range(self.VERSIONS):
            print('Run {}'.format(i + 1))
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
                    print('    Applied change at {}:{}, exists {}'.format(offset, patch_size, exists))
                    self.patch(image_filename, offset, data)
                    hints.append({'offset': offset, 'length': patch_size, 'exists': exists})
            # truncate?
            if not os.path.exists(image_filename):
                open(image_filename, 'wb').close()
            with open(image_filename, 'r+b') as f:
                f.truncate(size)

            print('  Applied {} changes, size is {}.'.format(len(hints), size))
            with open(os.path.join(testpath, 'hints'), 'w') as f:
                f.write(json.dumps(hints))

            benji_obj = self.benjiOpen(initdb=initdb)
            initdb = False
            with open(os.path.join(testpath, 'hints')) as hints:
                version_uid = benji_obj.backup('data-backup', 'snapshot-name', 'file://' + image_filename,
                                               hints_from_rbd_diff(hints.read()), base_version)
            benji_obj.close()
            version_uids.append((version_uid, size))
        return version_uids

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_export(self):
        benji_obj = self.benjiOpen(initdb=True)
        benji_obj.close()
        self.version_uids = self.generate_versions(self.testpath.path)
        benji_obj = self.benjiOpen()
        with StringIO() as f:
            benji_obj.metadata_export([version_uid[0] for version_uid in self.version_uids], f)
            f.seek(0)
            export = json.load(f)
            f.seek(0)
            print(f.getvalue())
            a = f.getvalue()
        benji_obj.close()
        self.assertEqual(MetadataBackend.METADATA_VERSION, export['metadataVersion'])
        self.assertIsInstance(export['versions'], list)
        self.assertTrue(len(export['versions']) == 3)
        version = export['versions'][0]
        self.assertEqual(1, version['uid'])
        self.assertEqual('data-backup', version['name'])
        self.assertEqual('snapshot-name', version['snapshot_name'])
        self.assertEqual(4096, version['block_size'])
        self.assertTrue(version['valid'])
        self.assertFalse(version['protected'])
        self.assertEqual(1, version['storage_id'])

    def test_import(self):
        benji_obj = self.benjiOpen(initdb=True)
        benji_obj.metadata_import(StringIO(self.IMPORT))
        version = benji_obj.ls(version_uid=VersionUid(1))[0]
        self.assertTrue(isinstance(version.uid, VersionUid))
        self.assertEqual(1, version.uid)
        self.assertEqual('data-backup', version.name)
        self.assertEqual('snapshot-name', version.snapshot_name)
        self.assertEqual(4096, version.block_size)
        self.assertTrue(version.valid)
        self.assertFalse(version.protected)
        self.assertIsInstance(version.blocks, list)
        self.assertIsInstance(version.tags, list)
        self.assertEqual({'b_daily', 'b_weekly', 'b_monthly'}, set([tag.name for tag in version.tags]))
        self.assertEqual(datetime.datetime.strptime('2018-10-29T21:19:15', '%Y-%m-%dT%H:%M:%S'), version.date)
        blocks = benji_obj.ls_version(VersionUid(1))
        self.assertTrue(len(blocks) > 0)
        max_i = len(blocks) - 1
        for i, block in enumerate(blocks):
            self.assertEqual(VersionUid(1), block.version_uid)
            self.assertEqual(i, block.id)
            if i != max_i:
                self.assertEqual(4096, block.size)
            self.assertEqual(datetime.datetime.strptime('2018-10-29T21:19:15', '%Y-%m-%dT%H:%M:%S'), block.date)
            self.assertTrue(block.valid)
        benji_obj.close()

    IMPORT = """
        {
          "versions": [
            {
              "uid": 1,
              "date": "2018-10-29T21:19:15",
              "name": "data-backup",
              "snapshot_name": "snapshot-name",
              "size": 128113,
              "block_size": 4096,
              "storage_id": 1,
              "valid": true,
              "protected": false,
              "tags": [{"name:": "b_daily"}, {"name": "b_weekly"}, {"name": "b_monthly"}],
              "blocks": [
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 0,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 1,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 2,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 3,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 4,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 5,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 6,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 7,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 8,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 9,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 10,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 11,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 12,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 13,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 14,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 15,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 16,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 17,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": 1,
                    "right": 19
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 18,
                  "size": 4096,
                  "valid": true,
                  "checksum": "422a0f5f214730d61b639ca48a0090305712c5ca0085881f1bae574c15793e37"
                },
                {
                  "uid": {
                    "left": 1,
                    "right": 20
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 19,
                  "size": 4096,
                  "valid": true,
                  "checksum": "185036be88d652cc244dead035093eadf45a1a8903657251d26b1aae289f0c38"
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 20,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 21,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 22,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 23,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 24,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 25,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 26,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 27,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 28,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 29,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": 1,
                    "right": 31
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 30,
                  "size": 4096,
                  "valid": true,
                  "checksum": "cf90d822e0a177fb0ab92165827259cf8f444a3db5137e6b13ebe91663eeaeb0"
                },
                {
                  "uid": {
                    "left": 1,
                    "right": 32
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 31,
                  "size": 1137,
                  "valid": true,
                  "checksum": "5beea9e690bc88afc1c2f922896b0e8ea00b14e4ad66bcc759d66acf72e23b05"
                }
              ]
            },
            {
              "uid": 2,
              "date": "2018-10-29T21:19:15",
              "name": "data-backup",
              "snapshot_name": "snapshot-name",
              "size": 128113,
              "block_size": 4096,
              "storage_id": 1,
              "valid": true,
              "protected": false,
              "tags": [{"name:": "b_daily"}, {"name": "b_weekly"}, {"name": "b_monthly"}],
              "blocks": [
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 0,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 1,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 2,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 3,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 4,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 5,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 6,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 7,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 8,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 9,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 10,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 11,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 12,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 13,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 14,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 15,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 16,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 17,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 18,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 19,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 20,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 21,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 22,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 23,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 24,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 25,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 26,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 27,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 28,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 29,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 30,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": 1,
                    "right": 32
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 31,
                  "size": 1137,
                  "valid": true,
                  "checksum": "5beea9e690bc88afc1c2f922896b0e8ea00b14e4ad66bcc759d66acf72e23b05"
                }
              ]
            },
            {
              "uid": 3,
              "date": "2018-10-29T21:19:15",
              "name": "data-backup",
              "snapshot_name": "snapshot-name",
              "size": 131042,
              "block_size": 4096,
              "storage_id": 1,
              "valid": true,
              "protected": false,
              "tags": [{"name:": "b_daily"}, {"name": "b_weekly"}, {"name": "b_monthly"}],
              "blocks": [
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 0,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 1,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 2,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": 3,
                    "right": 4
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 3,
                  "size": 4096,
                  "valid": true,
                  "checksum": "b4c2c5976ad608780fee1b60307a5570f8c05c02f69e2c911659afcf805378c5"
                },
                {
                  "uid": {
                    "left": 3,
                    "right": 5
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 4,
                  "size": 4096,
                  "valid": true,
                  "checksum": "ff5333cd65b9227eeb654a9a3405701d177fa48079c1ec602ddccdb3b4c475f2"
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 5,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 6,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 7,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 8,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 9,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 10,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 11,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 12,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 13,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 14,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": 3,
                    "right": 16
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 15,
                  "size": 4096,
                  "valid": true,
                  "checksum": "6ce4309bc44823426da6f998f1a243712dd59208951243a46d13f548cf5e091b"
                },
                {
                  "uid": {
                    "left": 3,
                    "right": 17
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 16,
                  "size": 4096,
                  "valid": true,
                  "checksum": "7f4b1dc3ebf8ddf461fc7d87374747c4034afbd9c141d4944ac18dac369fccd3"
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 17,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 18,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 19,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 20,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 21,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 22,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 23,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 24,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 25,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 26,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 27,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 28,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": null,
                    "right": null
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 29,
                  "size": 4096,
                  "valid": true,
                  "checksum": null
                },
                {
                  "uid": {
                    "left": 3,
                    "right": 31
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 30,
                  "size": 4096,
                  "valid": true,
                  "checksum": "ff2081852a3094a2bbb52c952609fe36b94673d1ff6864ef1871aaa8fdde2e88"
                },
                {
                  "uid": {
                    "left": 3,
                    "right": 32
                  },
                  "date": "2018-10-29T21:19:15",
                  "id": 31,
                  "size": 4066,
                  "valid": true,
                  "checksum": "47f7e16b2b3c6a00e4b1c148ce9b3c23f59e4db22f7229d359d869c2a737ce4c"
                }
              ]
            }
          ],
          "metadataVersion": "1.0.0"
        }
            """


class ImportExportCaseSQLLite_File(ImportExportTestCase, BenjiTestCase, TestCase):

    VERSIONS = 3

    CONFIG = """
            configurationVersion: '1.0.0'
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
            metadataEngine: sqlite:///{testpath}/benji.sqlite
            """


class ImportExportTestCasePostgreSQL_File(
        ImportExportTestCase,
        BenjiTestCase,
):

    VERSIONS = 3

    CONFIG = """
            configurationVersion: '1.0.0'
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
            metadataEngine: postgresql://benji:verysecret@localhost:15432/benji
            """
