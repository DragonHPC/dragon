import asyncio
from functools import partial, wraps
import logging
from typing import Optional


LOGGER = logging.getLogger("dragon.transport.tcp.task")


def run_forever(func=None, /, *, ignore=(Exception,)):
    """Ignores exceptions and continues to yield.

    Any unignored exception will terminate the generator and log the
    exception. Note that asyncio.exceptions.CanncelledError exceptions are
    not logged.

    If no exception is raised, it is assumed the calling context intended
    to terminate.
    """

    if func is None:
        # We're being called as @run_forever() with parens
        return partial(run_forever, ignore=ignore)

    assert asyncio.iscoroutinefunction(func)

    @wraps(func)
    async def wrapper(*args, **kwds):
        while True:
            try:
                await func(*args, **kwds)
            except ignore:
                LOGGER.exception(f"Uncaught exception, continuing task: {asyncio.current_task()}")
                pass
            except asyncio.exceptions.CancelledError:
                LOGGER.debug(f"Task cancelled: {asyncio.current_task()}")
                return
            except:
                LOGGER.exception(f"Uncaught exception, terminating task: {asyncio.current_task()}")
                return
            else:
                LOGGER.debug(f"Successfully terminating: {asyncio.current_task()}")
                return

    return wrapper


async def cancel_all_tasks():
    """Cancels all running tasks, except the current one."""
    tasks = asyncio.all_tasks()
    tasks.remove(asyncio.current_task())
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


class TaskMixin:
    """Mixin class that provides a conventional interface for managing a task
    corresponding to the ``run()`` method.
    """

    _task = None

    def start(self, name: Optional[str] = None) -> None:
        assert self._task is None
        self._task = asyncio.create_task(self.run(), name=name)

    async def stop(self) -> None:
        assert self._task is not None
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            del self._task

    def is_running(self):
        return self._task is not None and not self._task.done()

    @run_forever
    async def run(self) -> None:
        raise NotImplementedError
