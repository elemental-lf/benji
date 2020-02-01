import importlib
from typing import NamedTuple, Any, Dict

from benji.config import Config, ConfigList
from benji.exception import InternalError, ConfigurationError
from benji.repr import ReprMixIn


class _TransformFactoryModule(NamedTuple):
    module: Any
    arguments: Dict[str, Any]


class TransformFactory(ReprMixIn):

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

            module = importlib.import_module('{}.{}'.format(__package__, module))
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
        cls._modules = {}
        transforms: ConfigList = config.get('transforms', None, types=list)
        if transforms is not None:
            cls._import_modules(config, transforms)

    @classmethod
    def close(cls) -> None:
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
