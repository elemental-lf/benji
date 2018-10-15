#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
import operator
import os
from functools import reduce
from os.path import expanduser
from pathlib import Path

from cerberus import Validator
from pkg_resources import resource_filename
from ruamel.yaml import YAML

from benji.exception import ConfigurationError, InternalError
from benji.logging import logger


class _ConfigDict(dict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.full_name = None


class _ConfigList(list):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.full_name = None


class Config:

    CONFIG_VERSION = '1.0.0'

    CONFIG_DIR = 'benji'
    CONFIG_FILE = 'benji.yaml'

    CONFIG_SCHEMA_TEMPLATE = 'benji-config-schema-{}.yaml'

    # Source: https://stackoverflow.com/questions/823196/yaml-merge-in-python
    @classmethod
    def _merge_dicts(cls, user, default):
        if isinstance(user, dict) and isinstance(default, dict):
            for k, v in default.items():
                if k not in user:
                    user[k] = v
                else:
                    user[k] = cls._merge_dicts(user[k], v)
        return user

    @staticmethod
    def _output_validation_errors(errors):

        def traverse(position, path=''):
            if isinstance(position, dict):
                for key, value in position.items():
                    traverse(value, path + ('.' if path else '') + str(key))
            elif isinstance(position, list):
                for value in position:
                    if isinstance(value, dict):
                        traverse(value, path)
                    else:
                        logger.error('  {}: {}'.format(path, value))

        traverse(errors)

    def __init__(self, cfg=None, sources=None):
        yaml = YAML(typ='safe', pure=True)

        if cfg is None:
            if not sources:
                sources = self._get_sources()

            config = None
            for source in sources:
                if os.path.isfile(source):
                    try:
                        config = yaml.load(Path(source))
                    except Exception as exception:
                        raise ConfigurationError('Configuration file {} is invalid.'.format(source)) from exception
                    if config is None:
                        raise ConfigurationError('Configuration file {} is empty.'.format(source))
                    break

            if not config:
                raise ConfigurationError('No configuration file found in the default places ({}).'.format(
                    ', '.join(sources)))
        else:
            config = yaml.load(cfg)
            if config is None:
                raise ConfigurationError('Configuration string is empty.')

        if 'configurationVersion' not in config or type(config['configurationVersion']) is not str:
            raise ConfigurationError('Configuration version is missing or not a string.')

        if config['configurationVersion'] != self.CONFIG_VERSION:
            raise ConfigurationError('Unknown configuration version {}.'.format(config['configurationVersion']))

        try:
            schema_file = resource_filename(__name__, self.CONFIG_SCHEMA_TEMPLATE.format(self.CONFIG_VERSION))
            schema = yaml.load(Path(schema_file))
        except Exception as exception:
            raise ConfigurationError('Schema file {} is invalid.'.format(schema_file)) from exception

        validator = Validator(schema)
        if not validator:
            raise ConfigurationError('Schema file {} is invalid.'.format(schema_file))

        if not validator.validate(config):
            logger.error('Configuration validation errors:')
            self._output_validation_errors(validator.errors)
            raise ConfigurationError('Configuration is invalid.')

        self.config = validator.document
        logger.debug('Loaded configuration (includes defaults): {}'.format(self.config))

    def _get_sources(self):
        sources = ['/etc/{file}'.format(file=self.CONFIG_FILE)]
        sources.append('/etc/{dir}/{file}'.format(dir=self.CONFIG_DIR, file=self.CONFIG_FILE))
        sources.append(expanduser('~/.{file}'.format(file=self.CONFIG_FILE)))
        sources.append(expanduser('~/{file}'.format(file=self.CONFIG_FILE)))
        return sources

    @staticmethod
    def _get(root, name, *args, types=None, check_func=None, check_message=None, full_name_override=None, index=None):
        if full_name_override is not None:
            full_name = full_name_override
        elif hasattr(root, 'full_name') and root.full_name:
            full_name = root.full_name
        else:
            full_name = ''

        if index is not None:
            full_name = '{}{}{}'.format(full_name, '.' if full_name else '', index)

        full_name = '{}{}{}'.format(full_name, '.' if full_name else '', name)

        if len(args) > 1:
            raise InternalError('Called with more than two arguments for key {}.'.format(full_name))

        try:
            value = reduce(operator.getitem, name.split('.'), root)
            if types is not None and not isinstance(value, types):
                raise TypeError('Config value {} has wrong type {}, expected {}.'.format(full_name, type(value), types))
            if check_func is not None and not check_func(value):
                if check_message is None:
                    raise ConfigurationError('Config option {} has the right type but the supplied value is invalid.'
                                             .format(full_name))
                else:
                    raise ConfigurationError('Config option {} is invalid: {}.'.format(full_name, check_message))
            if isinstance(value, dict):
                value = _ConfigDict(value)
                value.full_name = full_name
            elif isinstance(value, list):
                value = _ConfigList(value)
                value.full_name = full_name
            return value
        except KeyError:
            if len(args) == 1:
                return args[0]
            else:
                if types and isinstance({}, types):
                    raise KeyError('Config section {} is missing.'.format(full_name)) from None
                else:
                    raise KeyError('Config option {} is missing.'.format(full_name)) from None

    def get(self, name, *args, **kwargs):
        return Config._get(self.config, name, *args, **kwargs)

    @staticmethod
    def get_from_dict(dict_, name, *args, **kwargs):
        return Config._get(dict_, name, *args, **kwargs)
