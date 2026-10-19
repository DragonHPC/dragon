"""An expiremental executor for parsl that uses Dragon's multiprocessing pool to submit batches of tasks to workers."""

import dragon
import multiprocessing as mp

import logging
import queue
import threading

from parsl.data_provider.staging import Staging
from parsl.executors.base import ParslExecutor
from parsl.utils import RepresentationMixin
from parsl.executors.errors import UnsupportedFeatureError

import typeguard
import concurrent.futures as cf

from typing import List, Optional

logger = logging.getLogger(__name__)


class DragonBatchPoolExecutor(ParslExecutor, RepresentationMixin):
    """A Parsl executor that can be used with python functions. This executor requires the user to be in an allocation. It will not allocate the resources for the user. This executor uses a multiprocessing pool with the specified number of workers placed in a round-robin fashion across the number of nodes in the allocation. Work submitted to the executor is batched to help reduce overhead. This executor is best for large, embarassingly parrallel python work.

    Example usage:

    .. highlight:: python
    .. code-block:: python

        import dragon
        import multiprocessing as mp
        import parsl
        from parsl.config import Config
        from parsl import python_app
        from dragon.workflows.parsl_batch_executor import DragonBatchPoolExecutor

        import os
        import math
        import argparse
        import numpy as np
        import time
        import itertools

        @python_app
        def f(arg):
            return 42

        if __name__ == "__main__":
            mp.set_start_method("dragon")

            nimages = 1024
            num_cpus = 256
            optimal_batch_size=math.floor(nimages/num_cpus)

            config = Config(
                executors=[
                    DragonBatchPoolExecutor(
                        max_processes=num_cpus,
                        batch_size=optimal_batch_size,
                    ),
                ],
                strategy=None,
            )

            parsl.load(config)

            results=[]
            for _ in range(nimages):
                res_future = f(None)
                results.append(res_future)

            for res_future in results:
                # this blocks till each result is available
                res_future.result()

            config.executors[0].shutdown()

    """

    @typeguard.typechecked
    def __init__(
        self,
        label: str = "dragon_batch_executor",
        max_processes: int = 2,
        storage_access: Optional[List[Staging]] = None,
        working_dir: Optional[str] = None,
        batch_size: int = 4,
        work_get_timeout: float = 0.1,
        max_num_timeouts: int = 4,
    ):
        """Init batched mp.Pool based executor

        :param label: unique label required by parsl, defaults to 'dragon_batch_executor'
        :type label: str, optional
        :param max_processes: number of workers in mp pool, defaults to 2
        :type max_processes: int, optional
        :param storage_access: parsl option for storage access, defaults to None
        :type storage_access: Optional[List[Staging]], optional
        :param working_dir: working directory that isn't used by mp.pool, defaults to None
        :type working_dir: Optional[str], optional
        :param batch_size: size of batches of work, defaults to 4
        :type batch_size: int, optional
        :param work_get_timeout: timeout for checking queues, defaults to 0.1
        :type work_get_timeout: float, optional
        :param max_num_timeouts: number of times queue.get should timeout before flushing an uncompleted batch, defaults to 4
        :type max_num_timeouts: int, optional
        """
        super().__init__()
        self.label = label
        self.max_processes = max_processes
        self.batch_size = batch_size
        self.work_get_timeout = work_get_timeout
        self.max_num_timeouts = max_num_timeouts

        self.storage_access = storage_access
        self.working_dir = working_dir
        self.already_closed = False

    def start(self):
        """start mp.Pool and threads that help with batching work"""
        self.executor = mp.Pool(self.max_processes)
        # queues to pass in results to be batched
        self.batching_queue = queue.Queue()
        # queue that takes batches and sets futures when completed
        self.results_queue = queue.Queue()
        # event to shutdown threads
        self.end_ev = threading.Event()
        # thread that handles batching of submitted work
        self.batching_thread = threading.Thread(
            target=self._batcher,
            args=(
                self.batching_queue,
                self.results_queue,
                self.executor,
                self.end_ev,
                self.batch_size,
                self.work_get_timeout,
                self.max_num_timeouts,
            ),
        )
        self.batching_thread.start()
        # thread that handles setting futures based on completed results in batch
        self.futures_thread = threading.Thread(
            target=self._futures_handler, args=(self.results_queue, self.end_ev, self.work_get_timeout)
        )
        self.futures_thread.start()

    def submit(self, func: callable, resource_specification: dict, *args, **kwargs) -> cf.Future:
        """Places work in batching queue to be submitted to mp.Pool

        :param func: work function with python_app decorator
        :type func: callable
        :param resource_specification: specifies resources to use. not utilized by the DragonBatchPool executor.
        :type resource_specification: dict
        :raises UnsupportedFeatureError: DragonBatchPool executor doesn't use resource specification
        :return: future for submitted work item
        :rtype: concurrent.futures.Future
        """
        if resource_specification:
            logger.error(
                "Ignoring the resource specification. "
                "Parsl resource specification is not supported in DragonBatchPool Executor."
            )
            raise UnsupportedFeatureError("resource specification", "DragonBatchPool Executor", None)
        # future for submitted work
        future = cf.Future()
        # send work to batching thread to be aggregated
        self.batching_queue.put((future, func, args, kwargs))
        return future

    @staticmethod
    def _identity(input_arg: tuple):
        """Computes fn(*args, **kwargs). This is necessary so that we can accept different functions as work items

        :param input_arg: packing of the function and args
        :type input_arg: tuple of (fn, args, kwargs)
        :return: return of the work function
        :rtype: any
        """
        fn, args, kwargs = input_arg
        return fn(*args, **kwargs)

    @staticmethod
    def _batcher(
        input_queue: queue.Queue,
        output_queue: queue.Queue,
        executor: mp.Pool,
        end_ev: threading.Event,
        batch_size: int,
        work_get_timeout: float,
        max_num_timeouts: int,
    ):
        """This is started in a thread and accepts work items, batches those work items, and submits these batches to the multiprocessing pool via map_async call. The returned MapResult and the list of corresponding futures are then sent to the other thread for parsing once the work is done.

        :param input_queue: queue containing work items and the futures corresponding to those work items
        :type input_queue: queue.Queue
        :param output_queue: queue containing the MapResult returned by the map_async call and the list of corresponding futures for that work
        :type output_queue: queue.Queue
        :param executor: the multiprocessing pool
        :type executor: mp.Pool
        :param end_ev: event used to signal shutdown to the thread.
        :type end_ev: threading.Event
        :param batch_size: size of each batch submitted to the mp.Pool
        :type batch_size: int
        :param work_get_timeout: timeout on the queue.get calls.
        :type work_get_timeout: float
        :param max_num_timeouts: number of times queue.get should timeout before the current batch of work items is flushed
        :type max_num_timeouts: int
        """

        work_items = []
        work_items_futures = []
        num_timeouts = 0
        while not end_ev.is_set():
            # get submitted work from queue
            try:
                future, fn, args, kwargs = input_queue.get(timeout=work_get_timeout)
                got_item = True
            except queue.Empty:
                num_timeouts += 1
                got_item = False
                if num_timeouts < max_num_timeouts:
                    continue
                else:
                    # if we've sat for sufficient iterations, send work out
                    pass

            # if we got a work item, add the item to the list
            if got_item:
                work_items.append((fn, args, kwargs))
                work_items_futures.append(future)
                num_timeouts = 0

            if len(work_items) >= batch_size or (num_timeouts >= max_num_timeouts and len(work_items) > 0):
                # send work to pool
                async_result = executor.map_async(DragonBatchPoolExecutor._identity, work_items, chunksize=batch_size)
                # send list of futures and async result to thread handling assignment of futures results
                output_queue.put((work_items_futures, async_result))
                # reset everything for next batch of work
                work_items = []
                work_items_futures = []
                num_timeouts = 0

    @staticmethod
    def _futures_handler(output_queue: queue.Queue, end_ev: threading.Event, work_get_timeout: float) -> None:
        """This is started in a thread and is responsible for handling the parsing of the MapResult back into the corresponding futures.

        :param output_queue: queue holding a list of futures and MapResult for those work items
        :type output_queue: queue.Queue
        :param end_ev: event used to signal shutdown
        :type end_ev: threading.Event
        :param work_get_timeout: timeout for queue.get call
        :type work_get_timeout: float
        """
        while not end_ev.is_set():
            # get batch job from queue
            try:
                futures_list, async_result = output_queue.get(timeout=work_get_timeout)
            except queue.Empty:
                continue

            # put job back in queue if it isn't ready
            try:
                async_result.wait(timeout=work_get_timeout)
            except TimeoutError:
                output_queue.put((futures_list, async_result))
                continue

            # get the results and check if any raise an exception
            try:
                results_iterable = async_result.get()
                exception_raised = False
            except Exception as e:
                results_iterable = [e] * len(futures_list)
                exception_raised = True

            # set results or exception for each future
            for future, result in zip(futures_list, results_iterable):
                if exception_raised:
                    future.set_exception(result)
                else:
                    future.set_result(result)

    def scale_out(self, workers: int = 1):
        """Scales pool out. Not implemented since multiprocessing pool maintains a pool of a specific size.

        :param workers: number of workers to scale out by, defaults to 1
        :type workers: int, optional
        :raises NotImplementedError: the scale out is not implemented for a multiprocessing pool
        """
        raise NotImplementedError

    def scale_in(self, workers: int):
        """Scales pool out. Not implemented since multiprocessing pool maintains a pool of a specific size.

        :param workers: number of workers to scale in by
        :type workers: int
        :raises NotImplementedError: the scale out is not implemented for a multiprocessing pool
        """
        raise NotImplementedError

    def shutdown(self, block: bool = True) -> bool:
        """Shuts down the worker pool. If block is set to False it will send the signal to shutdown even if all the work isn't done. If the expectation is for all work to be done, block=True is required.

        :param block: blocks shutdown till all submitted work is done, defaults to True
        :type block: bool, optional
        :return: if the pool is shutdown
        :rtype: bool
        """
        # if we've already called shutdown don't call it again
        if self.already_closed:
            return self.already_closed
        # different paths for shutting down the multiprocessing pool
        if block:
            self.executor.close()
            self.executor.join()
        else:
            self.executor.terminate()
            self.executor.join()
        # once pool is done, shutdown threads
        self.end_ev.set()
        self.futures_thread.join()
        self.batching_thread.join()
        self.already_closed = True
        return self.already_closed

    def monitor_resources(self) -> bool:
        """Used by parsl to monitor resources. Not implemented for multiprocessing pool.

        :return: returns whether we can monitor resources
        :rtype: bool
        """
        return False
