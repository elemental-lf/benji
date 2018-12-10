import binascii
from unittest import TestCase

from benji.aes_keywrap import aes_unwrap_key, aes_wrap_key


class AesKeywrapTestCase(TestCase):

    def test_aes_unwrap_key(self):
        #test vector from RFC 3394
        KEK = binascii.unhexlify('000102030405060708090A0B0C0D0E0F')
        CIPHER = binascii.unhexlify('1FA68B0A8112B447AEF34BD8FB5A7B829D3E862371D2CFE5')
        PLAIN = binascii.unhexlify('00112233445566778899AABBCCDDEEFF')
        self.assertEqual(aes_unwrap_key(KEK, CIPHER), PLAIN)

    def test_aes_wrap_key(self):
        #test vector from RFC 3394
        KEK = binascii.unhexlify('000102030405060708090A0B0C0D0E0F')
        CIPHER = binascii.unhexlify('1FA68B0A8112B447AEF34BD8FB5A7B829D3E862371D2CFE5')
        PLAIN = binascii.unhexlify('00112233445566778899AABBCCDDEEFF')
        self.assertEqual(aes_wrap_key(KEK, PLAIN), CIPHER)
