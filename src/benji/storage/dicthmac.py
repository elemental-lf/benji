#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import base64

from Crypto.Hash import HMAC, SHA256

from benji.exception import InternalError
from benji.repr import ReprMixIn


class DictHMAC(ReprMixIn):

    _CHARSET = 'utf-8'

    _HASH_NAME = 'sha256'
    _HASH_MODULE = SHA256

    _ALGORITHM_KEY = 'algorithm'
    _DIGEST_KEY = 'digest'

    def __init__(self, *, hmac_key, secret_key):
        self._hmac_key = hmac_key
        self._secret_key = secret_key

    def _calculate_digest(self, dict_data: dict) -> str:
        hmac = HMAC.new(self._secret_key, digestmod=self._HASH_MODULE)

        def traverse(cursor) -> None:
            if isinstance(cursor, dict):
                for key in sorted(cursor.keys()):
                    hmac.update(str(key).encode(self._CHARSET))
                    traverse(cursor[key])
            elif isinstance(cursor, list):
                for value in cursor:
                    traverse(value)
            else:
                hmac.update(str(cursor).encode(self._CHARSET))

        traverse(dict_data)

        return base64.b64encode(hmac.digest()).decode('ascii')

    def add_digest(self, dict_data: dict) -> None:
        if not isinstance(dict_data, dict):
            raise InternalError(f'dict_data must be of type dict, but is of type {type(dict_data)}')

        dict_data[self._hmac_key] = {
            self._ALGORITHM_KEY: self._HASH_NAME,
            self._DIGEST_KEY: self._calculate_digest(dict_data)
        }

    def verify_digest(self, dict_data) -> None:
        if not isinstance(dict_data, dict):
            raise InternalError(f'dict_data must be of type dict, but is of type {type(dict_data)}.')
        if self._hmac_key not in dict_data:
            raise ValueError(f'Dictionary is missing required HMAC key {self._hmac_key}.')

        hmac_dict = dict_data[self._hmac_key]

        if not isinstance(hmac_dict, dict):
            raise ValueError(f'HMAC key {self._hmac_key} has an invalid type of {type(hmac_dict)}.')

        for required_key in [self._ALGORITHM_KEY, self._DIGEST_KEY]:
            if required_key not in hmac_dict:
                raise ValueError(f'Required key {required_key} is missing in HMAC dictionary.')

        if hmac_dict[self._ALGORITHM_KEY] != self._HASH_NAME:
            raise ValueError(f'Unsupported hash algorithm {self._ALGORITHM_KEY}.')

        digest_expected = hmac_dict[self._DIGEST_KEY]
        del dict_data[self._hmac_key]
        digest = self._calculate_digest(dict_data)
        if digest != digest_expected:
            raise ValueError(f'Dictionary HMAC is invalid (expected {digest_expected}, actual {digest}).')
