import base64
from typing import Dict, Tuple, Optional

from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

from benji.aes_keywrap import aes_wrap_key, aes_unwrap_key
from benji.config import Config, ConfigDict
from benji.transform.base import TransformBase
from benji.utils import derive_key


class Transform(TransformBase):
    AES_KEY_LEN = 32

    def __init__(self, *, config: Config, name: str, module_configuration: ConfigDict) -> None:
        super().__init__(config=config, name=name, module_configuration=module_configuration)

        master_key_encoded: Optional[str] = Config.get_from_dict(module_configuration, 'masterKey', None, types=str)
        if master_key_encoded is not None:
            master_key = base64.b64decode(master_key_encoded)

            if len(master_key) != self.AES_KEY_LEN:
                raise ValueError('Key masterKey has the wrong length. It must be 32 bytes long and encoded as BASE64.')

            self._master_key = master_key
        else:
            kdf_salt: bytes = base64.b64decode(Config.get_from_dict(module_configuration, 'kdfSalt', types=str))
            kdf_iterations: int = Config.get_from_dict(module_configuration, 'kdfIterations', types=int)
            password: str = Config.get_from_dict(module_configuration, 'password', types=str)

            self._master_key = derive_key(salt=kdf_salt, iterations=kdf_iterations, key_length=32, password=password)

    def _create_envelope_key(self) -> Tuple[bytes, bytes]:
        envelope_key = get_random_bytes(self.AES_KEY_LEN)
        encrypted_key = aes_wrap_key(self._master_key, envelope_key)
        return envelope_key, encrypted_key

    def _derive_envelope_key(self, encrypted_key: bytes) -> bytes:
        return aes_unwrap_key(self._master_key, encrypted_key)

    def encapsulate(self, *, data: bytes) -> Tuple[Optional[bytes], Optional[Dict]]:
        envelope_key, encrypted_key = self._create_envelope_key()
        envelope_iv = get_random_bytes(16)
        encryptor = AES.new(envelope_key, AES.MODE_GCM, nonce=envelope_iv)

        materials = {
            'envelope_key': base64.b64encode(encrypted_key).decode('ascii'),
            'iv': base64.b64encode(envelope_iv).decode('ascii'),
        }

        return encryptor.encrypt(data), materials

    def decapsulate(self, *, data: bytes, materials: Dict) -> bytes:
        for key in ['envelope_key', 'iv']:
            if key not in materials:
                raise KeyError('Encryption materials are missing required key {}.'.format(key))

        envelope_key = materials['envelope_key']
        iv = materials['iv']

        envelope_key = base64.b64decode(envelope_key)
        iv = base64.b64decode(iv)

        if len(iv) != 16:
            raise ValueError('Encryption materials IV iv has wrong length of {}. It must be 16 bytes long.'.format(
                len(iv)))

        envelope_key = self._derive_envelope_key(envelope_key)
        if len(envelope_key) != self.AES_KEY_LEN:
            raise ValueError(
                'Encryption materials key envelope_key has wrong length of {}. It must be 32 bytes long.'.format(
                    len(envelope_key)))

        decryptor = AES.new(envelope_key, AES.MODE_GCM, nonce=iv)
        return decryptor.decrypt(data)
