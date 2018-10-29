import unittest

from . import DatabackendTestCase


class test_file(DatabackendTestCase, unittest.TestCase):
    CONFIG = """
        configurationVersion: '1.0.0'
        logFile: /dev/stderr
        metadataBackend:
          engine: sqlite://
        defaultStorage: file-1          
        storages:
        - name: file-1
          module: file
          storageId: 1
          configuration:
            path: {testpath}/data
            consistencyCheckWrites: True
            hmac:
              password: geheim12345
              kdfIterations: 1000
              kdfSalt: !!binary CPJlYMjRjfbXWOcqsE309A==
        """


if __name__ == '__main__':
    unittest.main()
