import unittest

from benji.exception import InternalError
from benji.storage.dicthmac import DictHMAC


class DictHMACHashTestCase(unittest.TestCase):

    def setUp(self):
        self.data = {'a': 10, 'b': 'test', 'c': True, 'e': {'a': 1, 'b': 'test'}}
        self.data_2 = {'a': 10, 'b': 'test', 'c': True, 'e': {'a': 1, 'b': 'different'}}
        self.dh = DictHMAC(hmac_key='hmac', secret_key=b'sadasdadsadasdadadadad')
        self.dh.add_hexdigest(self.data)
        self.dh.add_hexdigest(self.data_2)

    def test_result(self):
        self.assertDictEqual(self.data, {'a': 10, 'b': 'test', 'c': True, 'e': {'a': 1, 'b': 'test'}, 'hmac': {'algorithm': 'sha256', 'digest': '330cad929c84fc1d11b906216808688f140dd53eabff58fe900228cac53e24ce'}})

    def test_different_digest(self):
        self.assertNotEqual(self.data, self.data_2)

    def test_verify(self):
        self.dh.verify_hexdigest(self.data)

    def test_invalid_digest(self):
        self.data['hmac']['digest'] = 'test123'
        self.assertRaises(ValueError, lambda: self.dh.verify_hexdigest(self.data))

    def test_missing_hmac(self):
        del self.data['hmac']
        self.assertRaises(ValueError, lambda: self.dh.verify_hexdigest(self.data))

    def test_missing_hmac_algorithm(self):
        del self.data['hmac']['algorithm']
        self.assertRaises(ValueError, lambda: self.dh.verify_hexdigest(self.data))

    def test_unsupported_hmac_algorithm(self):
        self.data['hmac']['algorithm'] = 'SHA1024'
        self.assertRaises(ValueError, lambda: self.dh.verify_hexdigest(self.data))

    def test_missing_hmac_digest(self):
        del self.data['hmac']['digest']
        self.assertRaises(ValueError, lambda: self.dh.verify_hexdigest(self.data))

    def test_wrong_type(self):
        self.assertRaises(InternalError, lambda: self.dh.verify_hexdigest(1))

    def test_wrong_hmac_type(self):
        self.data['hmac'] = 1
        self.assertRaises(ValueError, lambda: self.dh.verify_hexdigest(self.data))
