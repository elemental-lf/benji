#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import importlib
from typing import Dict, NamedTuple, Any
from urllib import parse

from benji.config import Config, ConfigList
from benji.exception import ConfigurationError, InternalError, UsageError
from benji.io.base import IOBase
from benji.repr import ReprMixIn


class _StorageFactoryModule(NamedTuple):
    module: Any
    storage_id: int
    arguments: Dict[str, Any]


class _TransformFactoryModule(NamedTuple):
    module: Any
    arguments: Dict[str, Any]


class _IOFactoryModule(NamedTuple):
    module: Any
    arguments: Dict[str, Any]


class StorageFactory(ReprMixIn):

    _MODULE = 'storage'

    _modules: Dict[str, _StorageFactoryModule] = {}
    _instances: Dict[str, Any] = {}

    def __init__(self) -> None:
        raise InternalError('StorageFactory constructor called.')

    @classmethod
    def _import_modules(cls, config: Config, modules: ConfigList) -> None:
        for index, module_dict in enumerate(modules):
            module = Config.get_from_dict(module_dict,
                                          'module',
                                          types=str,
                                          full_name_override=modules.full_name,
                                          index=index)
            name = Config.get_from_dict(module_dict,
                                        'name',
                                        types=str,
                                        full_name_override=modules.full_name,
                                        index=index)
            storage_id = Config.get_from_dict(module_dict,
                                              'storageId',
                                              None,
                                              types=int,
                                              full_name_override=modules.full_name,
                                              index=index)
            configuration = Config.get_from_dict(module_dict,
                                                 'configuration',
                                                 None,
                                                 types=dict,
                                                 full_name_override=modules.full_name,
                                                 index=index)

            if name in cls._modules:
                raise ConfigurationError('Duplicate name "{}" in list {}.'.format(name, modules.full_name))

            module = importlib.import_module('{}.{}.{}'.format(__package__, cls._MODULE, module))
            try:
                configuration = config.validate(module=module.__name__, config=configuration)
            except ConfigurationError as exception:
                raise ConfigurationError('Configuration for storage {} is invalid.'.format(name)) from exception
            cls._modules[name] = _StorageFactoryModule(module=module,
                                                       storage_id=storage_id,
                                                       arguments={
                                                           'config': config,
                                                           'name': name,
                                                           'module_configuration': configuration
                                                       })

    @classmethod
    def initialize(cls, config: Config) -> None:
        TransformFactory.initialize(config)
        storages: ConfigList = config.get('storages', types=list)
        cls._import_modules(config, storages)

    @classmethod
    def close(cls) -> None:
        for storage in cls._instances.values():
            storage.close()

        cls._modules = {}
        cls._instances = {}

        TransformFactory.close()

    @classmethod
    def get_by_name(cls, name: str) -> Any:
        if name not in cls._instances:
            if name not in cls._modules:
                raise ConfigurationError('Storage {} is undefined.'.format(name))

            module = cls._modules[name].module
            module_arguments = cls._modules[name].arguments
            cls._instances[name] = module.Storage(**module_arguments)

        return cls._instances[name]

    @classmethod
    def get_modules(cls) -> Dict[str, _StorageFactoryModule]:
        return cls._modules


class TransformFactory(ReprMixIn):

    _MODULE = 'transform'

    _modules: Dict[str, _TransformFactoryModule] = {}
    _instances: Dict[str, Any] = {}

    def __init__(self) -> None:
        raise InternalError('TransformFactory constructor called.')

    @classmethod
    def _import_modules(cls, config: Config, modules: ConfigList) -> None:
        for index, module_dict in enumerate(modules):
            module = Config.get_from_dict(module_dict,
                                          'module',
                                          types=str,
                                          full_name_override=modules.full_name,
                                          index=index)
            name = Config.get_from_dict(module_dict,
                                        'name',
                                        types=str,
                                        full_name_override=modules.full_name,
                                        index=index)
            configuration = Config.get_from_dict(module_dict,
                                                 'configuration',
                                                 None,
                                                 types=dict,
                                                 full_name_override=modules.full_name,
                                                 index=index)

            if name in cls._modules:
                raise ConfigurationError('Duplicate name "{}" in list {}.'.format(name, modules.full_name))

            module = importlib.import_module('{}.{}.{}'.format(__package__, cls._MODULE, module))
            try:
                configuration = config.validate(module=module.__name__, config=configuration)
            except ConfigurationError as exception:
                raise ConfigurationError('Configuration for transform {} is invalid.'.format(name)) from exception
            cls._modules[name] = _TransformFactoryModule(module=module,
                                                         arguments={
                                                             'config': config,
                                                             'name': name,
                                                             'module_configuration': configuration
                                                         })

    @classmethod
    def initialize(cls, config: Config) -> None:
        transforms: ConfigList = config.get('transforms', None, types=list)
        if transforms is not None:
            cls._import_modules(config, transforms)

    @classmethod
    def close(cls) -> None:
        cls._modules = {}
        cls._instances = {}

    @classmethod
    def get_by_name(cls, name: str) -> Any:
        if name not in cls._instances:
            if name not in cls._modules:
                raise ConfigurationError('Transform name {} is undefined.'.format(name))

            module = cls._modules[name].module
            module_arguments = cls._modules[name].arguments
            cls._instances[name] = module.Transform(**module_arguments)

        return cls._instances[name]


class IOFactory(ReprMixIn):

    _MODULE = 'io'

    _modules: Dict[str, _IOFactoryModule] = {}

    def __init__(self) -> None:
        raise InternalError('IOFactory constructor called.')

    @classmethod
    def _import_modules(cls, config: Config, modules: ConfigList) -> None:
        for index, module_dict in enumerate(modules):
            module = Config.get_from_dict(module_dict,
                                          'module',
                                          types=str,
                                          full_name_override=modules.full_name,
                                          index=index)
            name = Config.get_from_dict(module_dict,
                                        'name',
                                        types=str,
                                        full_name_override=modules.full_name,
                                        index=index)
            configuration = Config.get_from_dict(module_dict,
                                                 'configuration',
                                                 None,
                                                 types=dict,
                                                 full_name_override=modules.full_name,
                                                 index=index)

            if name in cls._modules:
                raise ConfigurationError('Duplicate name "{}" in list {}.'.format(name, modules.full_name))

            module = importlib.import_module('{}.{}.{}'.format(__package__, cls._MODULE, module))
            try:
                configuration = config.validate(module=module.__name__, config=configuration)
            except ConfigurationError as exception:
                raise ConfigurationError('Configuration for IO {} is invalid.'.format(name)) from exception
            cls._modules[name] = _IOFactoryModule(module=module,
                                                  arguments={
                                                      'config': config,
                                                      'name': name,
                                                      'module_configuration': configuration
                                                  })

    @classmethod
    def initialize(cls, config: Config) -> None:
        ios: ConfigList = config.get('ios', None, types=list)
        cls._import_modules(config, ios)

    @classmethod
    def close(cls) -> None:
        cls._modules = {}

    @classmethod
    def get(cls, url: str, block_size: int) -> IOBase:
        parsed_url = parse.urlparse(url)

        name = parsed_url.scheme
        if not name:
            raise UsageError('The supplied URL {} is invalid. You must provide a scheme.'.format(url))
        if name not in cls._modules:
            raise ConfigurationError('IO scheme {} is undefined.'.format(name))

        module = cls._modules[name].module
        module_arguments = cls._modules[name].arguments.copy()
        module_arguments['url'] = url
        module_arguments['block_size'] = block_size
        return module.IO(**module_arguments)
