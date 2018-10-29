#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import json

from Crypto.Hash import HMAC, SHA256
from benji.exception import InternalError


class DictHMAC:

    _CHARSET = 'utf-8'

    _HASH_NAME = 'sha256'
    _HASH_MODULE = SHA256

    _ALGORITHM_KEY = 'algorithm'
    _DIGEST_KEY = 'digest'

    def __init__(self, *, dict_key, key):
        self._dict_key = dict_key
        self._key = key

    def _calculate_hexdigest(self, dict_data):
        hmac = HMAC.new(self._key, digestmod=self._HASH_MODULE)

        dict_json = json.dumps(dict_data, separators=(',', ':'), sort_keys=True).encode(self._CHARSET)
        hmac.update(dict_json)

        return hmac.hexdigest()

    def add_hexdigest(self, dict_data):
        if not isinstance(dict_data, dict):
            raise InternalError('dict_data must be of type dict, but is of type {}.', type(dict_data))

        dict_data[self._dict_key] = {
            self._ALGORITHM_KEY: self._HASH_NAME,
            self._DIGEST_KEY: self._calculate_hexdigest(dict_data)
        }

    def verify_hexdigest(self, dict_data):
        if not isinstance(dict_data, dict):
            raise InternalError('dict_data must be of type dict, but is of type {}.', type(dict_data))
        if self._dict_key not in dict_data:
            raise ValueError('Dictionary is missing required HMAC key {}.'.format(self._dict_key))

        hmac_dict = dict_data[self._dict_key]

        if not isinstance(hmac_dict, dict):
            raise ValueError('HMAC key {} has an invalid type of {}.'.format(type(hmac_dict)))

        for required_key in [self._ALGORITHM_KEY, self._DIGEST_KEY]:
            if required_key not in hmac_dict:
                raise KeyError('Required key {} is missing in HMAC dictionary.'.format(required_key))

        if hmac_dict[self._ALGORITHM_KEY] != self._HASH_NAME:
            raise ValueError('Unsupported hash algorithm {}.'.format(hmac_dict[self._ALGORITHM_KEY]))

        hexdigest_expected = hmac_dict[self._DIGEST_KEY]
        del dict_data[self._dict_key]
        hexdigest = self._calculate_hexdigest(dict_data)
        if hexdigest != hexdigest_expected:
            raise ValueError('Dictionary HMAC is invalid (expected {}, actual {}).'.format(
                hexdigest_expected, hexdigest))
