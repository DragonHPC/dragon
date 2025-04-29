import dragon
import multiprocessing as mp

import logging

# Uncomment this to see logging info
# logging.basicConfig(level=logging.DEBUG)

import typeguard
import concurrent.futures as cf

from typing import List, Optional

from parsl.data_provider.staging import Staging
from parsl.executors.base import ParslExecutor
from parsl.utils import RepresentationMixin
from parsl.executors.errors import UnsupportedFeatureError

logger = logging.getLogger(__name__)


class DragonPoolExecutor(ParslExecutor, RepresentationMixin):
    """A Dragon multiprocessing-based executor.

    Example usage:

    .. highlight:: python
    .. code-block:: python

        import dragon
        import multiprocessing as mp
        import parsl
        from parsl.config import Config
        from parsl import python_app

        @python_app
        def platform():
            import platform
            import time

            time.sleep(2)
            return platform.uname()

        if __name__ == "__main__":
            mp.set_start_method("dragon")

            config = Config(
                executors=[
                    DragonPoolExecutor(
                        max_processes=4,
                    ),
                ],
                strategy=None,
            )

            parsl.load(config)
            calls = [platform() for i in range(4)]
            print(calls)

            for c in calls:
                print("Got result: ", c.result())
    """

    @typeguard.typechecked
    def __init__(
        self,
        label: str = "dragon",
        max_processes: int = 2,
        storage_access: Optional[List[Staging]] = None,
        working_dir: Optional[str] = None,
    ):
        super().__init__()
        self.label = label
        self.max_processes = max_processes

        # we allow storage_access to be None now, which means something else to [] now
        # None now means that a default storage access list will be used, while
        # [] is a list with no storage access in it at all
        self.storage_access = storage_access
        self.working_dir = working_dir

    def start(self):
        context = mp.get_context("dragon")
        self.executor = cf.ProcessPoolExecutor(max_workers=self.max_processes, mp_context=context)

    def submit(self, func, resource_specification, *args, **kwargs):
        """Submits work to the process pool.

        This method is simply pass through and behaves like a submit call as described
        here `Python docs: <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor>`_

        """
        if resource_specification:
            logger.error(
                "Ignoring the resource specification. "
                "Parsl resource specification is not supported in ProcessPool Executor."
            )
            raise UnsupportedFeatureError("resource specification", "DragonPool Executor", None)

        return self.executor.submit(func, *args, **kwargs)

    def scale_out(self, workers=1):
        """Scales out the number of active workers by 1.

        This method is notImplemented and will raise the error if called.

        Raises:
             NotImplemented exception
        """

        raise NotImplementedError

    def scale_in(self, blocks):
        """Scale in the number of active blocks by specified amount.

        This method is not implemented and will raise the error if called.

        Raises:
             NotImplemented exception
        """

        raise NotImplementedError

    def shutdown(self, block=True):
        """Shutdown the DragonPool. The underlying concurrent.futures process pool
        implementation will not terminate tasks that are being executed, because it
        does not provide a mechanism to do that. With block set to false, this will
        return immediately and it will appear as if the DFK is shut down, but
        the python process will not be able to exit until the process pool has
        emptied out by task completions. In either case, this can be a very long wait.

        Kwargs:
            - block (Bool): To block for confirmations or not

        """
        logger.debug("Shutting down executor, which involves waiting for running tasks to complete")
        x = self.executor.shutdown(wait=block)
        logger.debug("Done with executor shutdown")
        return x

    def monitor_resources(self):
        """Resource monitoring sometimes deadlocks, so this function returns false to disable it."""
        return False


# =================================================
# The Code below is used to test the above executor
# =================================================

import parsl
from parsl.config import Config

from parsl import python_app


# Here we sleep for 2 seconds and return platform information
@python_app
def platform():
    import platform
    import time

    time.sleep(2)
    return platform.uname()


def main():
    mp.set_start_method("dragon")

    config = Config(
        executors=[
            DragonPoolExecutor(
                max_processes=4,
            ),
        ],
        strategy=None,
    )

    parsl.load(config)
    calls = [platform() for i in range(4)]
    print(calls)

    for c in calls:
        print("Got result: ", c.result())


if __name__ == "__main__":
    main()
