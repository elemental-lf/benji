from benji.config import Config


class APIBase:

    def __init__(self, *, config: Config) -> None:
        self._config = config
