import base64
from typing import Dict, Tuple, Optional
from hashlib import sha256

from Crypto.PublicKey import ECC

from benji.config import Config, ConfigDict
from benji.transform.base import TransformBase
from benji.transform.aes_256_gcm import Transform as TransformAES
from benji.logging import logger


AES_KEY_LEN = 32


class Transform(TransformAES):

    def __init__(self, *, config: Config, name: str, module_configuration: ConfigDict) -> None:
        ecc_key_der: str = Config.get_from_dict(module_configuration, 'EccKey', types=str)
        ecc_curve: Optionl[str] = Config.get_from_dict(module_configuration, 'EccCurve', 'NIST P-384', types=str)

        ecc_key = self._unpack_envelope_key(ecc_key_der)

        if ecc_key.curve != ecc_curve:
            raise ValueError('Key EccKey does not match the EccCurve setting. Found: {}, Expected: {}'.format(ecc_key.curve, ecc_curve))

        self._ecc_key = ecc_key
        self._ecc_curve = ecc_key.curve

        assert self._ecc_key.pointQ.x.size_in_bytes() >= AES_KEY_LEN

        # note: we don't actually have a "master" aes key, because the key is derived from the ECC key
        # and set before calling the parent's encapsulate/decapsulate method
        aes_config = module_configuration.copy()
        aes_config['masterKey'] = base64.b64encode(b'\x00' * AES_KEY_LEN).decode('ascii')
        super().__init__(config=config, name=name, module_configuration=aes_config)


    @staticmethod
    def _pack_envelope_key(key: ECC.EccKey) -> str:
        return base64.b64encode(key.export_key(format='DER', compress=True)).decode('ascii')


    @staticmethod
    def _unpack_envelope_key(key: str) -> ECC.EccKey:
        return ECC.import_key(base64.b64decode(key))


    @staticmethod
    def _ecc_point_to_key(point: ECC.EccPoint) -> bytes:
        sha = sha256(int.to_bytes(int(point.x), point.size_in_bytes(), 'big'))
        sha.update(int.to_bytes(int(point.y), point.size_in_bytes(), 'big'))
        return sha.digest()


    def _create_ecc_enc_key(self) -> Tuple[bytes, ECC.EccKey]:
        cipher_privkey = ECC.generate(curve=self._ecc_curve)
        shared_key = self._ecc_point_to_key(self._ecc_key.pointQ * cipher_privkey.d)
        return (shared_key, cipher_privkey.public_key())


    def _create_ecc_dec_key(self, cipher_pubkey: ECC.EccKey) -> bytes:
        return self._ecc_point_to_key(self._ecc_key.d * cipher_pubkey.pointQ)


    def encapsulate(self, *, data: bytes) -> Tuple[Optional[bytes], Optional[Dict]]:
        if self._ecc_key.has_private():
            logger.warning('EccKey from config includes private key data, which is not needed for encryption!')
        self._master_key, encrypted_key = self._create_ecc_enc_key()
        assert len(self._master_key) == AES_KEY_LEN

        enc_data, materials = super().encapsulate(data=data)
        materials['ecc_envelope_key'] = self._pack_envelope_key(encrypted_key)

        return enc_data, materials


    def decapsulate(self, *, data: bytes, materials: Dict) -> bytes:
        if not self._ecc_key.has_private():
            raise ValueError('EccKey from config does not include private key data.')
        if 'ecc_envelope_key' not in materials:
            raise KeyError('Encryption materials are missing required key ecc_envelope_key.')

        ecc_envelope_key = self._unpack_envelope_key(materials['ecc_envelope_key'])
        self._master_key = self._create_ecc_dec_key(ecc_envelope_key)

        if len(self._master_key) != AES_KEY_LEN:
            raise ValueError('Decrypted encryption materials master key has wrong length of {}. '
                             'It must be {} bytes long.'.format(len(self._master_key), AES_KEY_LEN))

        return super().decapsulate(data=data, materials=materials)

