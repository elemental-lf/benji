import importlib
from typing import NamedTuple, Any, Dict
from urllib import parse

from benji.config import Config, ConfigList
from benji.exception import InternalError, ConfigurationError, UsageError
from benji.io.base import IOBase
from benji.repr import ReprMixIn


class _IOFactoryModule(NamedTuple):
    module: Any
    arguments: Dict[str, Any]


class IOFactory(ReprMixIn):

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

            module = importlib.import_module('{}.{}'.format(__package__, module))
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
        cls._modules = {}
        cls._import_modules(config, ios)

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
