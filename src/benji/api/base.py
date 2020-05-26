from typing import Callable, NamedTuple

from benji.config import Config


def register_as_task(api_group: str, api_version: str) -> Callable:

    class _APIGroupVersion(NamedTuple):
        group: str
        version: str

    def decorator(func):
        func.api = _APIGroupVersion(api_group, api_version)
        return func

    return decorator


class TasksBase:

    def __init__(self, *, config: Config) -> None:
        self._config = config
