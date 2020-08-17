import base64
from typing import Dict, Tuple, Optional
from hashlib import sha256

from Crypto.PublicKey import ECC

from benji.config import Config, ConfigDict
from benji.transform.aes_256_gcm import Transform as TransformAES
from benji.logging import logger


class Transform(TransformAES):

    def __init__(self, *, config: Config, name: str, module_configuration: ConfigDict) -> None:
        ecc_key_der: str = Config.get_from_dict(module_configuration, 'EccKey', types=str)
        ecc_curve: Optional[str] = Config.get_from_dict(module_configuration, 'EccCurve', 'NIST P-384', types=str)

        ecc_key = self._unpack_envelope_key(base64.b64decode(ecc_key_der))

        if ecc_key.curve != ecc_curve:
            raise ValueError('Key EccKey does not match the EccCurve setting. Found: {}, Expected: {}'.format(ecc_key.curve, ecc_curve))

        self._ecc_key = ecc_key
        self._ecc_curve = ecc_key.curve

        assert self._ecc_key.pointQ.size_in_bytes() >= self.AES_KEY_LEN

        # note: we don't actually have a "master" aes key, because the key is derived from the ECC key
        # and set before calling the parent's encapsulate/decapsulate method
        aes_config = module_configuration.copy()
        aes_config['masterKey'] = base64.b64encode(b'\x00' * self.AES_KEY_LEN).decode('ascii')
        super().__init__(config=config, name=name, module_configuration=aes_config)

    @staticmethod
    def _pack_envelope_key(key: ECC.EccKey) -> bytes:
        return key.export_key(format='DER', compress=True)

    @staticmethod
    def _unpack_envelope_key(key: bytes) -> ECC.EccKey:
        return ECC.import_key(key)

    @staticmethod
    def _ecc_point_to_key(point: ECC.EccPoint) -> bytes:
        sha = sha256(int.to_bytes(int(point.x), point.size_in_bytes(), 'big'))
        sha.update(int.to_bytes(int(point.y), point.size_in_bytes(), 'big'))
        return sha.digest()

    def _create_envelope_key(self) -> Tuple[bytes, bytes]:
        cipher_privkey = ECC.generate(curve=self._ecc_curve)
        shared_key = self._ecc_point_to_key(self._ecc_key.pointQ * cipher_privkey.d)
        return shared_key, self._pack_envelope_key(cipher_privkey.public_key())

    def _derive_envelope_key(self, cipher_pubkey: bytes) -> bytes:
        ecc_point = self._unpack_envelope_key(cipher_pubkey)
        return self._ecc_point_to_key(ecc_point.pointQ * self._ecc_key.d)

    def encapsulate(self, *, data: bytes) -> Tuple[Optional[bytes], Optional[Dict]]:
        if self._ecc_key.has_private():
            logger.warning('EccKey from config includes private key data, which is not needed for encryption!')
        return super().encapsulate(data=data)

    def decapsulate(self, *, data: bytes, materials: Dict) -> bytes:
        if not self._ecc_key.has_private():
            raise ValueError('EccKey from config does not include private key data.')
        return super().decapsulate(data=data, materials=materials)
