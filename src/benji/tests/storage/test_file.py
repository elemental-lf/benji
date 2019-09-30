from unittest import TestCase

from . import StorageTestCase


class StorageTestFile(StorageTestCase, TestCase):
    CONFIG = """
        configurationVersion: '1'
        logFile: /dev/stderr
        databaseEngine: sqlite://
        defaultStorage: storage-1
        
        storages:
          - name: storage-1
            module: file
            configuration:
              path: {testpath}/data
              consistencyCheckWrites: True
              hmac:
                password: geheim12345
                kdfIterations: 1000
                kdfSalt: BBiZ+lIVSefMCdE4eOPX211n/04KY1M4c2SM/9XHUcA=
                
        ios:
            - name: file
              module: file              
        """
