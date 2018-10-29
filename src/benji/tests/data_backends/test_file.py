import unittest

from . import DatabackendTestCase


class test_file(DatabackendTestCase, unittest.TestCase):
    CONFIG = """
        configurationVersion: '1.0.0'
        logFile: /dev/stderr
        metadataBackend:
          engine: sqlite://
        dataBackends:
          defaultStorage: file-1
          storages:
            - identifier: file-1
              module: file
              configuration:
                path: {testpath}/data
                consistencyCheckWrites: True
                hmac:
                  key: !!binary CPJlYMjRjfbXWOcqsE309A==
        """


if __name__ == '__main__':
    unittest.main()
