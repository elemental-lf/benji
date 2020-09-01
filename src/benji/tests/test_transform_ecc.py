import base64
from unittest import TestCase

from Crypto.PublicKey import ECC

from benji.config import ConfigDict
from benji.transform.aes_256_gcm_ecc import Transform


class TestEccTransform(TestCase):

    @staticmethod
    def _get_transform_args(key):
        conf = ConfigDict()
        conf['eccKey'] = base64.b64encode(Transform._pack_envelope_key(key)).decode('ascii')
        conf['eccCurve'] = key.curve
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
        self.assertTrue(enc_data)
        self.assertNotEqual(enc_data, data)

        dec_data = self.ecc_transform.decapsulate(data=enc_data, materials=materials)
        self.assertEqual(dec_data, data)

    def test_encryption_random(self):
        ecc_transform_ref = self._get_transform()

        data = b'THIS IS A TEST'

        enc_data, materials = self.ecc_transform.encapsulate(data=data)
        enc_data_ref, materials_ref = ecc_transform_ref.encapsulate(data=data)

        self.assertNotEqual(enc_data, enc_data_ref)
        self.assertNotEqual(materials['envelope_key'], materials_ref['envelope_key'])

    def test_envelope_key(self):
        data = b'THIS IS A TEST'

        enc_data, materials = self.ecc_transform.encapsulate(data=data)

        envelope_key = self.ecc_transform._unpack_envelope_key(base64.b64decode(materials['envelope_key']))
        self.assertFalse(envelope_key.has_private())

    def test_pubkey_only_encryption(self):
        curve = 'NIST P-384'
        ecc_transform = self._get_transform_args(ECC.generate(curve=curve).public_key())
        data = b'THIS IS A TEST'
        enc_data, materials = ecc_transform.encapsulate(data=data)
        self.assertTrue(enc_data)
        self.assertNotEqual(enc_data, data)

    def test_pubkey_only_decryption(self):
        curve = 'NIST P-384'
        ecc_transform = self._get_transform_args(ECC.generate(curve=curve).public_key())
        data = b'THIS IS A TEST'
        enc_data, materials = ecc_transform.encapsulate(data=data)
        self.assertRaises(ValueError, ecc_transform.decapsulate, data=enc_data, materials=materials)
