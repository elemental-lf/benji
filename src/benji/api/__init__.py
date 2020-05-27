from benji.api.base import TasksBase
from benji.celery import RPCServer
from benji.config import Config
import benji.api.core
import benji.api.rbd


class APIServer:

    def __init__(self, *, config: Config, queue: str, threads: int) -> None:
        self._rpc_server = RPCServer(queue=queue, threads=threads)
        self._tasks_collection = []
        self._tasks_collection.append(benji.api.core.Tasks(config=config))
        self._tasks_collection.append(benji.api.rbd.Tasks(config=config))
        for tasks in self._tasks_collection:
            self._install_tasks(tasks)

    def _install_tasks(self, tasks_object: TasksBase) -> None:
        for kw in dir(tasks_object):
            attr = getattr(tasks_object, kw)
            if getattr(attr, 'api', False):
                self._rpc_server.register_as_task(attr.api.group, attr.api.version)(attr)

    def serve(self) -> None:
        self._rpc_server.serve()
