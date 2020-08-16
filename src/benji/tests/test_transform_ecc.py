from unittest import TestCase

from Crypto.PublicKey import ECC

from benji.config import ConfigDict
from benji.transform.ecc import Transform


class TestEccTransform(TestCase):
    
    @staticmethod
    def _get_transform_args(key):
        conf = ConfigDict()
        conf['EccKey'] = Transform._pack_envelope_key(key)
        conf['EccCurve'] = key.curve
        return Transform(name='EccTest', config=None, module_configuration=conf)


    @classmethod
    def _get_transform(cls):
        curve = 'NIST P-384'
        return cls._get_transform_args(ECC.generate(curve=curve))
        
    
    def setUp(self):
        self.ecc_transform = self._get_transform()


    def test_decryption(self):
        data = b'THIS IS A TEST'
        enc_data, materials = self.ecc_transform.encapsulate(data=data)
        assert enc_data
        assert enc_data != data

        dec_data = self.ecc_transform.decapsulate(data=enc_data, materials=materials)
        assert dec_data == data


    def test_encryption_random(self):
        ecc_transform_ref = self._get_transform()

        data = b'THIS IS A TEST'
        
        enc_data, materials = self.ecc_transform.encapsulate(data=data)
        enc_data_ref, materials_ref = ecc_transform_ref.encapsulate(data=data)

        assert enc_data != enc_data_ref
        assert materials['ecc_envelope_key'] != materials_ref['ecc_envelope_key']


    def test_envelope_key(self):
        data = b'THIS IS A TEST'
        
        enc_data, materials = self.ecc_transform.encapsulate(data=data)

        envelope_key = self.ecc_transform._unpack_envelope_key(materials['ecc_envelope_key'])
        assert envelope_key.has_private() is False


    def test_pubkey_only_encryption(self):
        curve = 'NIST P-384'
        ecc_transform = self._get_transform_args(ECC.generate(curve=curve).public_key())
        data = b'THIS IS A TEST'
        enc_data, materials = ecc_transform.encapsulate(data=data)
        assert enc_data
        assert enc_data != data


    def test_pubkey_only_decryption(self):
        curve = 'NIST P-384'
        ecc_transform = self._get_transform_args(ECC.generate(curve=curve).public_key())
        data = b'THIS IS A TEST'
        enc_data, materials = ecc_transform.encapsulate(data=data)
        self.assertRaises(ValueError, ecc_transform.decapsulate, data=enc_data, materials=materials)
