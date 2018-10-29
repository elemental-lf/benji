#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import importlib
from collections import namedtuple
from urllib import parse

from benji.config import Config

from benji.exception import ConfigurationError, InternalError, UsageError
from benji.logging import logger

_ModuleInstance = namedtuple('_ModuleInstance', ['module', 'arguments'])


class StorageFactory:

    _MODULE = 'storage'

    _modules = {}
    _name_to_storage_id = {}
    _storage_id_to_name = {}
    _instances = {}

    def __init__(self):
        raise InternalError('StorageFactory constructor called.')

    @classmethod
    def _import_modules(cls, config, modules):
        for index, module_dict in enumerate(modules):
            module = Config.get_from_dict(
                module_dict, 'module', types=str, full_name_override=modules.full_name, index=index)
            name = Config.get_from_dict(
                module_dict, 'name', types=str, full_name_override=modules.full_name, index=index)
            storage_id = Config.get_from_dict(
                module_dict, 'storageId', types=int, full_name_override=modules.full_name, index=index)
            configuration = Config.get_from_dict(
                module_dict, 'configuration', None, types=dict, full_name_override=modules.full_name, index=index)

            if name in cls._name_to_storage_id:
                raise ConfigurationError('Duplicate {} name {} in list {}.'.format(cls._MODULE, name, modules.full_name))

            if storage_id in cls._storage_id_to_name:
                raise ConfigurationError('Duplicate {} id {} in list {}.'.format(cls._MODULE, storage_id,
                                                                                 modules.full_name))

            try:
                module = importlib.import_module('{}.{}.{}'.format(__package__, cls._MODULE, module))
            except ImportError:
                raise ConfigurationError('Module file {}.{}.{} not found or related import error.'.format(
                    __package__, cls._MODULE, module))
            else:
                configuration = config.validate(module.__name__, config=configuration)
                cls._modules[storage_id] = _ModuleInstance(
                    module=module,
                    arguments={
                        'config': config,
                        'name': name,
                        'storage_id': storage_id,
                        'module_configuration': configuration
                    })
                cls._name_to_storage_id[name] = storage_id
                cls._storage_id_to_name[storage_id] = name

    @classmethod
    def initialize(cls, config):
        TransformFactory.initialize(config)
        storages = config.get('storages', types=list)
        cls._import_modules(config, storages)

    @classmethod
    def close(cls):
        for storage in cls._instances.values():
            storage.close()

        cls._modules = {}
        cls._name_to_storage_id = {}
        cls._storage_id_to_name = {}
        cls._instances = {}

        TransformFactory.close()

    @classmethod
    def get_by_storage_id(cls, storage_id):
        if storage_id not in cls._instances:
            if storage_id not in cls._modules:
                raise ConfigurationError('Storage id {} is undefined.'.format(storage_id))

            module = cls._modules[storage_id].module
            module_arguments = cls._modules[storage_id].arguments
            cls._instances[storage_id] = module.Storage(**module_arguments)

        return cls._instances[storage_id]

    @classmethod
    def get_by_name(cls, name):
        if not name in cls._name_to_storage_id:
            raise ConfigurationError('Storage name {} is undefined.'.format(name))

        return cls.get_by_storage_id(cls._name_to_storage_id[name])

    @classmethod
    def storage_id_to_name(cls, storage_id):
        if storage_id in cls._storage_id_to_name:
            return cls._storage_id_to_name[storage_id]
        else:
            raise ConfigurationError('Storage id {} is undefined.'.format(storage_id))

    @classmethod
    def name_to_storage_id(cls, name):
        if name in cls._name_to_storage_id:
            return cls._name_to_storage_id[name]
        else:
            raise ConfigurationError('Storage name {} is undefined.'.format(name))


class TransformFactory:

    _MODULE = 'transform'

    _modules = {}
    _instances = {}

    def __init__(self):
        raise InternalError('TransformFactory constructor called.')

    @classmethod
    def _import_modules(cls, config, modules):
        for index, module_dict in enumerate(modules):
            module = Config.get_from_dict(
                module_dict, 'module', types=str, full_name_override=modules.full_name, index=index)
            name = Config.get_from_dict(
                module_dict, 'name', types=str, full_name_override=modules.full_name, index=index)
            configuration = Config.get_from_dict(
                module_dict, 'configuration', None, types=dict, full_name_override=modules.full_name, index=index)

            if name in cls._modules:
                raise ConfigurationError('Duplicate {} name {} in list {}.'.format(cls._MODULE, name, modules.full_name))

            try:
                module = importlib.import_module('{}.{}.{}'.format(__package__, cls._MODULE, module))
            except ImportError:
                raise ConfigurationError('Module file {}.{}.{} not found or related import error.'.format(
                    __package__, cls._MODULE, module))
            else:
                configuration = config.validate(module.__name__, config=configuration)
                cls._modules[name] = _ModuleInstance(
                    module=module, arguments={
                        'config': config,
                        'name': name,
                        'module_configuration': configuration
                    })

    @classmethod
    def initialize(cls, config):
        transforms = config.get('transforms', None, types=list)
        if transforms is not None:
            cls._import_modules(config, transforms)

    @classmethod
    def close(cls):
        cls._modules = {}
        cls._instances = {}

    @classmethod
    def get_by_name(cls, name):
        if name not in cls._instances:
            if name not in cls._modules:
                raise ConfigurationError('Transform name {} is undefined.'.format(name))

            module = cls._modules[name].module
            module_arguments = cls._modules[name].arguments
            cls._instances[name] = module.Transform(**module_arguments)

        return cls._instances[name]


class IOFactory:

    _MODULE = 'io'
    _DEFAULT_IOS = ['file', 'rbd']

    _modules = {}

    def __init__(self):
        raise InternalError('IOFactory constructor called.')

    @classmethod
    def _import_modules(cls, config, modules):
        for index, module_dict in enumerate(modules):
            module = Config.get_from_dict(
                module_dict, 'module', types=str, full_name_override=modules.full_name, index=index)
            name = Config.get_from_dict(
                module_dict, 'name', types=str, full_name_override=modules.full_name, index=index)
            configuration = Config.get_from_dict(
                module_dict, 'configuration', None, types=dict, full_name_override=modules.full_name, index=index)

            logger.debug(cls._modules)
            if name in cls._modules:
                raise ConfigurationError('Duplicate {} name {} in list {}.'.format(cls._MODULE, name, modules.full_name))

            try:
                module = importlib.import_module('{}.{}.{}'.format(__package__, cls._MODULE, module))
            except ImportError:
                raise ConfigurationError('Module file {}.{}.{} not found or related import error.'.format(
                    __package__, cls._MODULE, module))
            else:
                print(configuration)
                configuration = config.validate(module.__name__, config=configuration)
                cls._modules[name] = _ModuleInstance(
                    module=module, arguments={
                        'config': config,
                        'name': name,
                        'module_configuration': configuration
                    })

    @classmethod
    def initialize(cls, config):
        ios = config.get('ios', None, types=list)
        io_names = [io['name'] for io in ios]
        for name in cls._DEFAULT_IOS:
            if name in io_names:
                logger.debug('Default IO {} overridden by user.'.format(name))
                continue
            ios.append({'module': name, 'name': name, 'configuration': {}})
        cls._import_modules(config, ios)

    @classmethod
    def close(cls):
        cls._modules = {}

    @classmethod
    def get(cls, url, block_size):
        res = parse.urlparse(url)

        if res.params or res.query or res.fragment:
            raise UsageError('The supplied URL {} is invalid.'.format(url))

        name = res.scheme
        if not name:
            raise UsageError('The supplied URL {} is invalid. You must provide a scheme.'.format(url))
        if name not in cls._modules:
            raise ConfigurationError('IO scheme {} is undefined.'.format(name))

        module = cls._modules[name].module
        module_arguments = cls._modules[name].arguments.copy()
        module_arguments['path'] = res.netloc + res.path
        module_arguments['block_size'] = block_size
        return module.IO(**module_arguments)
