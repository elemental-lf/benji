import os
import unittest
from unittest import TestCase

from . import StorageTestCase


@unittest.skipIf(os.environ.get('UNITTEST_SKIP_S3', False), 'No S3 setup available.')
class test_s3(StorageTestCase, TestCase):
    CONFIG = """
        configurationVersion: '1'
        logFile: /dev/stderr
        databaseEngine: sqlite://        
        defaultStorage: s1
        
        storages:
        - name: s1
          module: s3
          storageId: 1
          configuration:
            awsAccessKeyId: minio
            awsSecretAccessKey: minio123
            endpointUrl: http://127.0.0.1:9901/
            bucketName: benji
            addressingStyle: path
            disableEncodingType: true
            consistencyCheckWrites: True
            simultaneousWrites: 5
            simultaneousReads: 5
            activeTransforms:
              - zstd
              - k1
              - k2
        
        transforms:
        - name: zstd
          module: zstd
          configuration:
            level: 1
        - name: k1
          module: aes_256_gcm
          configuration:
            masterKey: VPSQYIyD+dfLIRBTYJlGziu1hsT2eNFXnEuvl6jM/m8=
        - name: k2
          module: aes_256_gcm
          configuration:
            kdfSalt: BBiZ+lIVSefMCdE4eOPX211n/04KY1M4c2SM/9XHUcA=
            kdfIterations: 20000
            password: "this is a very secret password"
            
        ios:
            - name: file
              module: file                  
        """
