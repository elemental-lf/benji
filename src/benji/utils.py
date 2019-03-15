#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import concurrent
import json
import re
import setproctitle
import sys
from ast import literal_eval
from concurrent.futures import Future
from datetime import datetime
from importlib import import_module
from threading import Lock
from time import time
from typing import List, Tuple, Union, Any, Optional, Dict, Iterator

from Crypto.Hash import SHA512
from Crypto.Protocol.KDF import PBKDF2
from dateutil import tz
from dateutil.relativedelta import relativedelta

from benji.exception import ConfigurationError
from benji.logging import logger


def hints_from_rbd_diff(rbd_diff: str) -> List[Tuple[int, int, bool]]:
    """ Return the required offset:length tuples from a rbd json diff
    """
    data = json.loads(rbd_diff)
    return [(l['offset'], l['length'], False if l['exists'] == 'false' or not l['exists'] else True) for l in data]


# old_msg is used as a stateful storage between calls
def notify(process_name: str, msg: str = '', old_msg: str = ''):
    """ This method can receive notifications and append them in '[]' to the
    process name seen in ps, top, ...
    """
    if msg:
        new_msg = '{} [{}]'.format(process_name, msg.replace('\n', ' '))
    else:
        new_msg = process_name

    if old_msg != new_msg:
        old_msg = new_msg
        setproctitle.setproctitle(new_msg)


# This is tricky to implement as we need to make sure that we don't hold a reference to the completed Future anymore.
# Indeed it's so tricky that older Python versions had the same problem. See https://bugs.python.org/issue27144.
def future_results_as_completed(futures: List[Future], semaphore=None, timeout: int = None) -> Iterator[Any]:
    if sys.version_info < (3, 6, 4):
        logger.warning('Large backup jobs are likely to fail because of excessive memory usage. ' + 'Upgrade your Python to at least 3.6.4.')

    for future in concurrent.futures.as_completed(futures, timeout=timeout):
        futures.remove(future)
        if semaphore and not future.cancelled():
            semaphore.release()
        try:
            result = future.result()
        except Exception as exception:
            result = exception
        del future
        yield result


def derive_key(*, password, salt, iterations, key_length):
    return PBKDF2(password=password, salt=salt, dkLen=key_length, count=iterations, hmac_hash_module=SHA512)


class BlockHash:

    _CRYPTO_PACKAGE = 'Crypto.Hash'

    _hash_module: Any
    _hash_kwargs: Dict[str, Any]

    def __init__(self, hash_function_config: str) -> None:
        hash_args: Optional[str] = None
        try:
            hash_name, hash_args = hash_function_config.split(',', 1)
        except ValueError:
            hash_name = hash_function_config

        try:
            hash_module: Any = import_module('{}.{}'.format(self._CRYPTO_PACKAGE, hash_name))
        except ImportError as exception:
            raise ConfigurationError('Unsupported block hash {}.'.format(hash_name)) from exception

        hash_kwargs: Dict[str, Any] = {}
        if hash_args is not None:
            hash_kwargs = dict((k, literal_eval(v)) for k, v in (pair.split('=') for pair in hash_args.split(',')))

        try:
            hash = hash_module.new(**hash_kwargs)
        except (TypeError, ValueError) as exception:
            raise ConfigurationError(
                'Unsupported or invalid block hash arguments: {}.'.format(hash_kwargs)) from exception

        from benji.database import Block
        if len(hash.digest()) > Block.MAXIMUM_CHECKSUM_LENGTH:
            raise ConfigurationError('Specified block hash {} exceeds maximum digest length of {} bytes.'.format(
                hash_name, Block.MAXIMUM_CHECKSUM_LENGTH))

        logger.debug('Using block hash {} with kwargs {}.'.format(hash_name, hash_kwargs))

        self._hash_module = hash_module
        self._hash_kwargs = hash_kwargs

    def data_hexdigest(self, data: bytes) -> str:
        return self._hash_module.new(data=data, **self._hash_kwargs).hexdigest()


class PrettyPrint:
    # Based on https://code.activestate.com/recipes/578113-human-readable-format-for-a-given-time-delta/
    @staticmethod
    def duration(duration: int) -> str:
        delta = relativedelta(seconds=duration)
        attrs = ['years', 'months', 'days', 'hours', 'minutes', 'seconds']
        readable = []
        for attr in attrs:
            if getattr(delta, attr) or attr == attrs[-1]:
                readable.append('{:02}{}'.format(getattr(delta, attr), attr[:1]))
        return ' '.join(readable)

    # Based on: https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    @staticmethod
    def bytes(num: Union[int, float], suffix: str = 'B') -> str:
        for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Yi', suffix)

    @staticmethod
    def local_time(date: datetime) -> str:
        if date.tzinfo is None:
            return date.replace(tzinfo=tz.tzutc()).astimezone(tz.tzlocal()).strftime("%Y-%m-%dT%H:%M:%S")
        else:
            return date.astimezone(tz.tzlocal()).strftime("%Y-%m-%dT%H:%M:%S")


class TokenBucket:
    """
    An implementation of the token bucket algorithm.
    """

    def __init__(self) -> None:
        self.tokens = 0.0
        self.rate = 0
        self.last = time()
        self.lock = Lock()

    def set_rate(self, rate: int) -> None:
        with self.lock:
            self.rate = rate
            self.tokens = self.rate

    def consume(self, tokens: int) -> float:
        with self.lock:
            if not self.rate:
                return 0

            now = time()
            lapse = now - self.last
            self.last = now
            self.tokens += lapse * self.rate

            if self.tokens > self.rate:
                self.tokens = self.rate

            self.tokens -= tokens

            if self.tokens >= 0:
                return 0
            else:
                return -self.tokens / self.rate


class InputValidation:

    QUALIFIED_NAME_REGEXP = '(?!-)[-a-zA-Z0-9_.]{1,63}(?<!-)'
    VALUE_REGEXP = '(?!-)[-a-zA-Z0-9_.:/@+]+(?<!-)'
    OPTIONAL_VALUE_REGEXP = '(' + VALUE_REGEXP + ')?'
    DNS1123_LABEL_REGEXP = '(?!-)[-a-z0-9]{1,63}(?<!-)'
    DNS1123_SUBDOMAIN_REGEXP = DNS1123_LABEL_REGEXP + '(\\.' + DNS1123_LABEL_REGEXP + ')*'
    DNS1123_SUBDOMAIN_MAX_LENGTH = 253

    @classmethod
    def is_backup_name(cls, label):
        return re.fullmatch(cls.VALUE_REGEXP, label) is not None

    @classmethod
    def is_snapshot_name(cls, label):
        return re.fullmatch(cls.OPTIONAL_VALUE_REGEXP, label) is not None

    @classmethod
    def is_label_value(cls, value: str) -> bool:
        return re.fullmatch(cls.OPTIONAL_VALUE_REGEXP, value) is not None

    @classmethod
    def is_dns1123_subdomain(cls, subdomain: str) -> bool:
        if len(subdomain) > cls.DNS1123_SUBDOMAIN_MAX_LENGTH:
            return False
        return re.fullmatch(cls.DNS1123_SUBDOMAIN_REGEXP, subdomain) is not None

    # This matches Kubernetes "qualified name"
    # See: https://github.com/errm/kubernetes/blob/master/pkg/util/validation/validation.go
    @classmethod
    def is_label_name(cls, name: str) -> bool:
        if name.find('/') > -1:
            prefix, name = name.split('/')
            if len(prefix) == 0 or len(name) == 0:
                return False
            if not cls.is_dns1123_subdomain(prefix):
                return False
            if not re.fullmatch(cls.QUALIFIED_NAME_REGEXP, name):
                return False
            return True
        else:
            return re.fullmatch(cls.QUALIFIED_NAME_REGEXP, name) is not None
