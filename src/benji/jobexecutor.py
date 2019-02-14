import concurrent
from concurrent.futures import ThreadPoolExecutor, Future
from threading import BoundedSemaphore
from typing import List, Callable, Iterator, Any

from benji.logging import logger


class JobExecutor:

    # The behaviour with blocking_submit == True is that the submit will block after queuing a number of jobs.
    # In the case of a storage write for example this ensures that we don't enqueue to many blocks at once and
    # so use up all available memory.
    # In the other case there is no limit on the number of submitted jobs. But the number of simultaneous
    # outstanding results is limited.
    # In the case of a storage read for example this ensures that we don't have to many outstanding read blocks
    # at once and so use up all available memory.
    def __init__(self, *, workers: int, blocking_submit: bool, name: str) -> None:
        self._name = name
        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix=name)
        self._futures: List[Future] = []
        self._blocking_submit = blocking_submit
        # Set the queue limit to two times the number of workers plus one to ensure that there are always
        # enough jobs available even when all futures finish at the same time.
        self._semaphore = BoundedSemaphore(2 * workers + 1)

    def submit(self, function: Callable) -> None:
        if self._blocking_submit:
            self._semaphore.acquire()

            def execute_with_release():
                try:
                    return function()
                except Exception:
                    raise
                finally:
                    self._semaphore.release()

            self._futures.append(self._executor.submit(execute_with_release))
        else:

            def execute_with_acquire():
                self._semaphore.acquire()
                return function()

            self._futures.append(self._executor.submit(execute_with_acquire))

    # This is tricky to implement as we need to make sure that we don't hold a reference to the completed Future anymore.
    # Indeed it's so tricky that older Python versions had the same problem. See https://bugs.python.org/issue27144.
    def get_completed(self, timeout: int = None) -> Iterator[Any]:
        for future in concurrent.futures.as_completed(self._futures, timeout=timeout):
            self._futures.remove(future)
            if not self._blocking_submit and not future.cancelled():
                self._semaphore.release()
            try:
                result = future.result()
            except Exception as exception:
                result = exception
            del future
            yield result

    def shutdown(self) -> None:
        if len(self._futures) > 0:
            logger.warning('Job executor "{}" is being shutdown with {} outstanding jobs, cancelling them.'.format(
                self._name, len(self._futures)))
            for future in self._futures:
                future.cancel()
            logger.debug('Job executor "{}" cancelled all outstanding jobs.'.format(self._name))
            if not self._blocking_submit:
                # Get all jobs so that the semaphore gets released and still waiting jobs can complete
                for _ in self.get_completed():
                    pass
                logger.debug('Job executor "{}" read results for all outstanding jobs.'.format(self._name))
        self._executor.shutdown()

    def wait_for_all(self) -> None:
        concurrent.futures.wait(self._futures)
