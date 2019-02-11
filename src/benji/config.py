#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
import operator
import os
import re
from copy import deepcopy
from functools import reduce
from os.path import expanduser
from typing import List, Callable, Union, Dict, Any, Optional, Sequence

import ruamel.yaml
import semantic_version
from cerberus import Validator, SchemaError
from pkg_resources import resource_filename

from benji.exception import ConfigurationError, InternalError
from benji.logging import logger
from benji.versions import VERSIONS


class ConfigDict(dict):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.full_name: Optional[str] = None


class ConfigList(list):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.full_name: Optional[str] = None


class Config:
    _CONFIG_DIRS = ['/etc', '/etc/benji']
    _CONFIG_FILE = 'benji.yaml'
    _CONFIGURATION_VERSION_KEY = 'configurationVersion'
    _CONFIGURATION_VERSION_REGEX = r'\d+'
    _PARENTS_KEY = 'parents'
    _YAML_SUFFIX = '.yaml'

    _SCHEMA_VERSIONS = [semantic_version.Version('1', partial=True)]

    _schema_registry: Dict[str, Dict] = {}

    @staticmethod
    def _schema_name(module: str, version: semantic_version.Version) -> str:
        return '{}-v{}'.format(module, version.major)

    @classmethod
    def add_schema(cls, *, module: str, version: semantic_version.Version, file: str) -> None:
        name = cls._schema_name(module, version)
        try:
            with open(file, 'r') as f:
                schema = ruamel.yaml.load(f, Loader=ruamel.yaml.SafeLoader)
            cls._schema_registry[name] = schema
        except FileNotFoundError:
            raise InternalError('Schema {} not found or not accessible.'.format(file))
        except SchemaError as exception:
            raise InternalError('Schema {} is invalid.'.format(file)) from exception

    def _merge_dicts(self, result, parent):
        if isinstance(result, dict) and isinstance(parent, dict):
            for k, v in parent.items():
                if k not in result:
                    result[k] = deepcopy(v)
                else:
                    result[k] = self._merge_dicts(result[k], v)
        return result

    def _resolve_schema(self, *, name: str) -> Dict:
        try:
            child = self._schema_registry[name]
        except KeyError:
            raise InternalError('Schema for module {} is missing.'.format(name))
        result: Dict = {}
        if self._PARENTS_KEY in child:
            parent_names = child[self._PARENTS_KEY]
            for parent_name in parent_names:
                parent = self._resolve_schema(name=parent_name)
                self._merge_dicts(result, parent)
        result = self._merge_dicts(result, child)
        if self._PARENTS_KEY in result:
            del result[self._PARENTS_KEY]
        logger.debug('Resolved schema for {}: {}.'.format(name, result))
        return result

    class _Validator(Validator):

        def _normalize_coerce_to_string(self, value):
            return str(value)

    def _get_validator(self, *, module: str, version: semantic_version.Version) -> Validator:
        name = self._schema_name(module, version)
        schema = self._resolve_schema(name=name)
        try:
            validator = Config._Validator(schema)
        except SchemaError as exception:
            logger.error('Schema {} validation errors:'.format(name))
            self._output_validation_errors(exception.args[0])
            raise InternalError('Schema {} is invalid.'.format(name)) from exception
        return validator

    @staticmethod
    def _output_validation_errors(errors) -> None:

        def traverse(cursor, path=''):
            if isinstance(cursor, dict):
                for key, value in cursor.items():
                    traverse(value, path + ('.' if path else '') + str(key))
            elif isinstance(cursor, list):
                for value in cursor:
                    if isinstance(value, dict):
                        traverse(value, path)
                    else:
                        logger.error('  {}: {}'.format(path, value))

        traverse(errors)

    def validate(self, *, module: str, version: semantic_version.Version = None,
                 config: Union[Dict, ConfigDict]) -> Dict:
        validator = self._get_validator(module=module, version=self._config_version if version is None else version)
        if not validator.validate({'configuration': config if config is not None else {}}):
            logger.error('Configuration validation errors:')
            self._output_validation_errors(validator.errors)
            raise ConfigurationError('Configuration for module {} is invalid.'.format(module))

        config_validated = validator.document['configuration']
        # This output leaks sensitive information. Only reinstate when such infos are redacted somehow.
        # logger.debug('Configuration for module {}: {}.'.format(module, config_validated))
        return config_validated

    def __init__(self, ad_hoc_config: str = None, sources: Sequence[str] = None) -> None:
        if ad_hoc_config is None:
            if not sources:
                sources = self._get_sources()

            config = None
            for source in sources:
                if os.path.isfile(source):
                    try:
                        with open(source, 'r') as f:
                            config = ruamel.yaml.load(f, Loader=ruamel.yaml.SafeLoader)
                    except Exception as exception:
                        raise ConfigurationError('Configuration file {} is invalid.'.format(source)) from exception
                    if config is None:
                        raise ConfigurationError('Configuration file {} is empty.'.format(source))
                    break

            if not config:
                raise ConfigurationError('No configuration file found in the default places ({}).'.format(
                    ', '.join(sources)))
        else:
            config = ruamel.yaml.load(ad_hoc_config, Loader=ruamel.yaml.SafeLoader)
            if config is None:
                raise ConfigurationError('Configuration string is empty.')

        if self._CONFIGURATION_VERSION_KEY not in config:
            raise ConfigurationError('Configuration is missing required key "{}".'.format(
                self._CONFIGURATION_VERSION_KEY))

        version = str(config[self._CONFIGURATION_VERSION_KEY])
        if not re.fullmatch(self._CONFIGURATION_VERSION_REGEX, version):
            raise ConfigurationError('Configuration has invalid version of "{}".'.format(version))

        version_obj = semantic_version.Version(version, partial=True)
        if version_obj not in VERSIONS.configuration.supported:
            raise ConfigurationError('Configuration has unsupported version of "{}".'.format(version))

        self._config_version = version_obj
        self._config = ConfigDict(self.validate(module=__name__, config=config))
        logger.debug('Loaded configuration: {}'.format(self._config))

    def _get_sources(self) -> List[str]:
        sources = []
        for directory in self._CONFIG_DIRS:
            sources.append('{directory}/{file}'.format(directory=directory, file=self._CONFIG_FILE))
        sources.append(expanduser('~/.{file}'.format(file=self._CONFIG_FILE)))
        sources.append(expanduser('~/{file}'.format(file=self._CONFIG_FILE)))
        return sources

    @staticmethod
    def _get(root,
             name: str,
             *args,
             types: Any = None,
             check_func: Callable[[object], bool] = None,
             check_message: str = None,
             full_name_override: str = None,
             index: int = None) -> object:
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
                    raise ConfigurationError(
                        'Config option {} has the right type but the supplied value is invalid.'.format(full_name))
                else:
                    raise ConfigurationError('Config option {} is invalid: {}.'.format(full_name, check_message))
            if isinstance(value, dict):
                value = ConfigDict(value)
                value.full_name = full_name
            elif isinstance(value, list):
                value = ConfigList(value)
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

    def get(self, name: str, *args, **kwargs) -> Any:
        return Config._get(self._config, name, *args, **kwargs)

    @staticmethod
    def get_from_dict(dict_: ConfigDict, name: str, *args, **kwargs) -> Any:
        return Config._get(dict_, name, *args, **kwargs)


for version_obj in Config._SCHEMA_VERSIONS:
    schema_base_path = os.path.join(resource_filename(__name__, 'schemas'), 'v{}'.format(version_obj.major))
    for filename in os.listdir(schema_base_path):
        full_path = os.path.join(schema_base_path, filename)
        if not os.path.isfile(full_path) or not full_path.endswith(Config._YAML_SUFFIX):
            continue
        module = filename[0:len(filename) - len(Config._YAML_SUFFIX)]
        logger.debug('Loading  schema {} for module {}, version v{}.'.format(full_path, module, str(version_obj)))
        Config.add_schema(module=module, version=version_obj, file=full_path)
