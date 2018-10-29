import unittest

from . import DatabackendTestCase


class test_s3_boto3(DatabackendTestCase, unittest.TestCase):
    CONFIG = """
        configurationVersion: '1.0.0'
        logFile: /dev/stderr
        metadataBackend:
          engine: sqlite://        
        dataBackends:
          defaultStorage: s1
          
          storages:
            - identifier: s1
              module: s3
              configuration:
                awsAccessKeyId: minio
                awsSecretAccessKey: minio123
                endpointUrl: http://127.0.0.1:9901/
                bucketName: benji
                multiDelete: true
                addressingStyle: path
                disableEncodingType: true
                activeCompression: zstd
                activeEncryption: k1
                consistencyCheckWrites: True
                simultaneousWrites: 5
                simultaneousReads: 5
                activeTransforms:
                  - zstd
                  - k1
                  - k2
            
          transforms:
            - identifier: zstd
              module: zstd
              configuration:
                level: 1
            - identifier: k1
              module: aes_256_gcm
              configuration:
                masterKey: !!binary |
                  e/i1X4NsuT9k+FIVe2kd3vtHVkzZsbeYv35XQJeV8nA=
            - identifier: k2
              module: aes_256_gcm
              configuration:
                kdfSalt: !!binary CPJlYMjRjfbXWOcqsE309A==
                kdfIterations: 20000
                password: "this is a very secret password"
                
         
        """


if __name__ == '__main__':
    unittest.main()
