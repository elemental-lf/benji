import unittest

from . import StorageTestCase


class StorageTestFile(StorageTestCase, unittest.TestCase):
    CONFIG = """
        configurationVersion: '1.0.0'
        logFile: /dev/stderr
        databaseEngine: sqlite://
        defaultStorage: storage-1
        
        storages:
          - name: storage-1
            storageId: 1
            module: file
            configuration:
              path: {testpath}/data
              consistencyCheckWrites: True
              hmac:
                password: geheim12345
                kdfIterations: 1000
                kdfSalt: !!binary CPJlYMjRjfbXWOcqsE309A==
                
        ios:
            - name: file
              module: file              
        """


if __name__ == '__main__':
    unittest.main()
