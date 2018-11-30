import os
import unittest

from benji.config import Config, _ConfigList
from benji.exception import ConfigurationError
from benji.tests.testcase import TestCase


class ConfigTestCase(TestCase, unittest.TestCase):

    CONFIG = """
        configurationVersion: '1.0.0'
        logFile: /var/log/benji.log
        blockSize: 4194304
        defaultStorage: s1
        metadataEngine: sqlite:////var/lib/benji/benji.sqlite
        storages:
          - name: file
            module: file
            storageId: 1
            configuration:
              path: /var/lib/benji/data
              simultaneousWrites: 5
              simultaneousReads: 5
        nbd:
          cacheDirectory: /tmp
        ios:
          - name: rbd
            module: rbd
            configuration:
              ceph_conffile: /etc/ceph/ceph.conf
              simultaneousReads: 10
              newImageFeatures:
                - RBD_FEATURE_LAYERING
                - RBD_FEATURE_EXCLUSIVE_LOCK
        """

    CONFIG_INVALID_VERSION = """
        configurationVersion: '112345.0.0'
        logFile: /var/log/benji.log
        blockSize: 4194304
        defaultStorage: s1
        metadataEngine: sqlite:////var/lib/benji/benji.sqlite
        storages:
          - name: file
            module: file
            storageId: 1
            configuration:
              path: /var/lib/benji/data
              simultaneousWrites: 5
              simultaneousReads: 5
        nbd:
          cacheDirectory: /tmp
        ios:
          - name: rbd
            module: rbd
            configuration:
              ceph_conffile: /etc/ceph/ceph.conf
              simultaneousReads: 10
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
        self.assertTrue(isinstance(Config.get_from_dict(ios[0], 'configuration.newImageFeatures'), _ConfigList))
        self.assertRaises(TypeError, lambda: Config.get_from_dict(ios[0], 'configuration.newImageFeatures', types=int))
        self.assertEqual('RBD_FEATURE_EXCLUSIVE_LOCK',
                         Config.get_from_dict(ios[0], 'configuration.newImageFeatures')[1])

    def test_correct_version(self):
        self.assertTrue(isinstance(Config(ad_hoc_config=self.CONFIG), Config))

    def test_wrong_version(self):
        self.assertRaises(ConfigurationError, lambda: Config(ad_hoc_config=self.CONFIG_INVALID_VERSION))

    def test_missing_version(self):
        self.assertRaises(ConfigurationError, lambda: Config(ad_hoc_config='a: {b: 1, c: 2}'))

    def test_defaults(self):
        config = Config(ad_hoc_config=self.CONFIG)
        self.assertEqual('benji', config.get('processName'))
        self.assertEqual('blake2b,digest_size=32', config.get('hashFunction'))

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
        configuration = {'path': '/var/tmp'}
        self.assertEqual({
            'bandwidthRead': 0,
            'bandwidthWrite': 0,
            'consistencyCheckWrites': False,
            'path': '/var/tmp',
            'simultaneousReads': 1,
            'simultaneousWrites': 1
        }, Config.validate('benji.storage.file', configuration))
        configuration = {'asdasdas': 'dasdasd'}
        self.assertRaises(ConfigurationError, lambda: Config.validate('benji.storage.file', configuration))
        configuration = {}
        self.assertRaises(ConfigurationError, lambda: Config.validate('benji.storage.file', configuration))
        configuration = {'path': '/var/tmp', 'bandwidthRead': -1}
        self.assertRaises(ConfigurationError, lambda: Config.validate('benji.storage.file', configuration))
        configuration = {'path': [1, 2, 3]}
        self.assertRaises(ConfigurationError, lambda: Config.validate('benji.storage.file', configuration))
