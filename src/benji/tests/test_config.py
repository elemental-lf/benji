import os
from unittest import TestCase

from benji.config import Config, ConfigList
from benji.exception import ConfigurationError
from benji.tests.testcase import TestCaseBase


class ConfigTestCase(TestCaseBase, TestCase):

    CONFIG = """
        configurationVersion: '1'
        logFile: /var/log/benji.log
        blockSize: 4194304
        defaultStorage: s1
        databaseEngine: sqlite:////var/lib/benji/benji.sqlite
        storages:
          - name: file
            module: file
            configuration:
              path: /var/lib/benji/data
              simultaneousWrites: 5
              simultaneousReads: 5
        nbd:
        ios:
          - name: rbd
            module: rbd
            configuration:
              cephConfigFile: /etc/ceph/ceph.conf
              simultaneousReads: 10
              simultaneousWrites: 10
              newImageFeatures:
                - RBD_FEATURE_LAYERING
                - RBD_FEATURE_EXCLUSIVE_LOCK
        """

    CONFIG_INVALID_VERSION = """
        configurationVersion: '1-2-3'
        logFile: /var/log/benji.log
        blockSize: 4194304
        defaultStorage: s1
        databaseEngine: sqlite:////var/lib/benji/benji.sqlite
        storages:
          - name: file
            module: file
            configuration:
              path: /var/lib/benji/data
              simultaneousWrites: 5
              simultaneousReads: 5
        nbd:
        ios:
          - name: rbd
            module: rbd
            configuration:
              cephConfigFile: /etc/ceph/ceph.conf
              simultaneousReads: 10
              simultaneousWrites: 10
              newImageFeatures:
                - RBD_FEATURE_LAYERING
                - RBD_FEATURE_EXCLUSIVE_LOCK
        """

    CONFIG_INTEGER = """
        configurationVersion: 1
        logFile: /var/log/benji.log
        blockSize: 4194304
        defaultStorage: s1
        databaseEngine: sqlite:////var/lib/benji/benji.sqlite
        storages:
          - name: file
            module: file
            configuration:
              path: /var/lib/benji/data
              simultaneousWrites: 5
              simultaneousReads: 5
        nbd:
        ios:
          - name: rbd
            module: rbd
            configuration:
              cephConfigFile: /etc/ceph/ceph.conf
              simultaneousReads: 10
              simultaneousWrites: 10
              newImageFeatures:
                - RBD_FEATURE_LAYERING
                - RBD_FEATURE_EXCLUSIVE_LOCK
        """

    CONFIG_WITH_STORAGE_ID = """
        configurationVersion: '1'
        logFile: /var/log/benji.log
        blockSize: 4194304
        defaultStorage: s1
        databaseEngine: sqlite:////var/lib/benji/benji.sqlite
        storages:
          - name: file
            module: file
            storageId: 33
            configuration:
              path: /var/lib/benji/data
              simultaneousWrites: 5
              simultaneousReads: 5
        nbd:
        ios:
          - name: rbd
            module: rbd
            configuration:
              cephConfigFile: /etc/ceph/ceph.conf
              simultaneousReads: 10
              simultaneousWrites: 10
              newImageFeatures:
                - RBD_FEATURE_LAYERING
                - RBD_FEATURE_EXCLUSIVE_LOCK
        """

    def test_load_from_string(self):
        config = Config(ad_hoc_config=self.CONFIG)
        self.assertEqual('/var/log/benji.log', config.get('logFile', types=str))
        self.assertEqual(4194304, config.get('blockSize', types=int))

    def test_dict(self):
        config = Config(ad_hoc_config=self.CONFIG)
        nbd = config.get('nbd', types=dict)
        self.assertEqual('nbd', nbd.full_name)

    def test_lists(self):
        config = Config(ad_hoc_config=self.CONFIG)
        ios = config.get('ios', types=list)
        self.assertIsInstance(Config.get_from_dict(ios[0], 'configuration.newImageFeatures'), ConfigList)
        self.assertRaises(TypeError, lambda: Config.get_from_dict(ios[0], 'configuration.newImageFeatures', types=int))
        self.assertEqual('RBD_FEATURE_EXCLUSIVE_LOCK',
                         Config.get_from_dict(ios[0], 'configuration.newImageFeatures')[1])

    def test_correct_version(self):
        self.assertIsInstance(Config(ad_hoc_config=self.CONFIG), Config)

    def test_wrong_version(self):
        self.assertRaises(ConfigurationError, lambda: Config(ad_hoc_config=self.CONFIG_INVALID_VERSION))

    def test_missing_version(self):
        self.assertRaises(ConfigurationError, lambda: Config(ad_hoc_config='a: {b: 1, c: 2}'))

    def test_defaults(self):
        config = Config(ad_hoc_config=self.CONFIG)
        self.assertEqual('benji', config.get('processName'))
        self.assertEqual('BLAKE2b,digest_bits=256', config.get('hashFunction'))

    def test_missing(self):
        config = Config(ad_hoc_config=self.CONFIG)
        self.assertRaises(KeyError, lambda: config.get('missing.option'))

    def test_get_with_dict(self):
        self.assertEqual('Hi there!', Config.get_from_dict({'a': {'b': 'Hi there!'}}, 'a.b', types=str))

    def test_load_from_file(self):
        cfile = os.path.join(self.testpath.path, 'test-config.yaml')
        with open(cfile, 'w') as f:
            f.write(self.CONFIG)
        config = Config(sources=[cfile])
        self.assertEqual('/var/log/benji.log', config.get('logFile'))

    def test_validation(self):
        config = Config(ad_hoc_config=self.CONFIG)
        module_configuration = {'path': '/var/tmp'}
        self.assertEqual(
            {
                'bandwidthRead': 0,
                'bandwidthWrite': 0,
                'consistencyCheckWrites': False,
                'path': '/var/tmp',
                'simultaneousReads': 3,
                'simultaneousWrites': 3,
                'simultaneousRemovals': 5,
            }, config.validate(module='benji.storage.file', config=module_configuration))
        module_configuration = {'asdasdas': 'dasdasd'}
        self.assertRaises(ConfigurationError,
                          lambda: config.validate(module='benji.storage.file', config=module_configuration))
        module_configuration = {}
        self.assertRaises(ConfigurationError,
                          lambda: config.validate(module='benji.storage.file', config=module_configuration))
        module_configuration = {'path': '/var/tmp', 'bandwidthRead': -1}
        self.assertRaises(ConfigurationError,
                          lambda: config.validate(module='benji.storage.file', config=module_configuration))
        module_configuration = {'path': [1, 2, 3]}
        self.assertRaises(ConfigurationError,
                          lambda: config.validate(module='benji.storage.file', config=module_configuration))

    # Pull request https://github.com/elemental-lf/benji/pull/7
    def test_validation_io_rbd(self):
        config = Config(ad_hoc_config=self.CONFIG)
        module_configuration = config.get('ios')[0]['configuration']
        self.assertEqual(
            {
                'cephConfigFile': '/etc/ceph/ceph.conf',
                'clientIdentifier': 'admin',
                'newImageFeatures': ['RBD_FEATURE_LAYERING', 'RBD_FEATURE_EXCLUSIVE_LOCK'],
                'simultaneousReads': 10,
                'simultaneousWrites': 10,
            }, config.validate(module='benji.io.rbd', config=module_configuration))
        module_configuration['newImageFeatures'] = ['ASASA', 'DDASAD']
        self.assertRaises(ConfigurationError,
                          lambda: config.validate(module='benji.io.rbd', config=module_configuration))

    def test_integer_version(self):
        self.assertIsInstance(Config(ad_hoc_config=self.CONFIG_INTEGER), Config)

    def test_with_storage_id(self):
        config = Config(ad_hoc_config=self.CONFIG_WITH_STORAGE_ID)
        self.assertIsInstance(config, Config)
        storage_instance = config.get('storages')[0]
        self.assertIsInstance(storage_instance, dict)
        self.assertEqual(33, storage_instance['storageId'])
