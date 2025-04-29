"""Dragon's experimental `@mpi_app` decorator and `DragonMPIExecutor` for  `parsl`. This work was done before parsl's official @mpi_app decorator was released."""
import dragon

from dragon.native.process import Process, ProcessTemplate, Popen
from dragon.native.process_group import ProcessGroup

import logging

import parsl
from parsl.data_provider.staging import Staging
from parsl.executors.base import ParslExecutor
from parsl.utils import RepresentationMixin
from parsl.executors.errors import UnsupportedFeatureError
from parsl.app.app import AppBase
from parsl.app.errors import wrap_error
from parsl.dataflow.dflow import DataFlowKernelLoader, DataFlowKernel

import typeguard

import concurrent.futures as cf

from typing import List, Optional, Union
from typing_extensions import Literal

logger = logging.getLogger(__name__)


class DragonMPIExecutor(ParslExecutor, RepresentationMixin):
    """Dragon based experimental @mpi_app executor. It executes an MPI application that is started using a ProcessGroup within an existing allocation of nodes.

    Example usage:

    .. highlight:: python
    .. code-block:: python

        import dragon

        from dragon.workflows.parsl_mpi_app import mpi_app, DragonMPIExecutor
        from dragon.infrastructure.connection import Connection
        from dragon.infrastructure.policy import Policy
        import parsl
        from parsl.config import Config
        from parsl.dataflow.dflow import DataFlowKernelLoader

        @mpi_app
        def mpi_factorial_app(num_ranks: int, bias: float, policy: Policy = None):
            import os

            # executable located in run_dir that we want to launch
            exe = "factorial"
            run_dir = os.getcwd()
            # list of the mpi args we want to pass to the app
            mpi_args = [str(bias)]
            # format that is expected by the DragonMPIExecutor
            return exe, run_dir, policy, num_ranks, mpi_args

        with DragonMPIExecutor() as dragon_mpi_exec:
            config = Config(
                executors=[dragon_mpi_exec],
                strategy=None,
            )

            parsl.load(config)

            bias = 10
            num_mpi_ranks = 10
            scale_factor = 1 / 10000
            connections = mpi_factorial_app(num_mpi_ranks, bias)
            output_string = get_results(connections.result()["out"])
            print(f"mpi computation: {output_string}", flush=True)

    """

    @typeguard.typechecked
    def __init__(
        self,
        label: str = "dragon_process_group",
        storage_access: Optional[List[Staging]] = None,
        working_dir: Optional[str] = None,
    ):
        super().__init__()
        self.label = label
        self.storage_access = storage_access
        self.working_dir = working_dir
        self.grps = []

    def start(self):
        """Since we cannot start a ProcessGroup without the executable start does not initialize anything"""
        pass

    def submit(self, func, resource_specification, *args):
        """Defines a dragon.native.ProcessGroup and launches the specified MPI executable with that process group. It returns stdin

        :param func: function with mpi_app decorator that defines ProcessGroup and mpi app parameters
        :type func: callable
        :param resource_specification: specifies resources to use. not utilized by the DragonMPI executor.
        :type resource_specification: dict
        :raises UnsupportedFeatureError: DragonMPI executor doesn't use resource specification
        :return: Future that is completed and holds connections to stdin and stdout to rank 0.
        :rtype: concurrent.futures.Future
        """
        import os

        if resource_specification:
            logger.error(
                "Ignoring the resource specification. "
                "Parsl resource specification is not supported in DragonMPI Executor."
            )
            raise UnsupportedFeatureError("resource specification", "DragonMPI Executor", None)

        exe, run_dir, policy, num_ranks, mpi_args = func(*args)
        target_exe = os.path.join(run_dir, exe)
        grp = ProcessGroup(restart=False, pmi_enabled=True, policy=policy)
        # Pipe stdin and stdout from the head process to Dragon connections
        grp.add_process(
            nproc=1,
            template=ProcessTemplate(
                target=target_exe, args=mpi_args, cwd=run_dir, stdout=Popen.PIPE, stdin=Popen.PIPE
            ),
        )

        # All other ranks should have their output go to DEVNULL
        grp.add_process(
            nproc=num_ranks - 1,
            template=ProcessTemplate(target=target_exe, args=mpi_args, cwd=run_dir, stdout=Popen.DEVNULL),
        )
        grp.init()
        grp.start()

        group_procs = [Process(None, ident=puid) for puid in grp.puids]

        # once process group has started, return futures holding connections to stdin and stdout of rank 0
        future_conn = cf.Future()
        results_dict = {}

        for proc in group_procs:
            if proc.stdin_conn:
                results_dict["in"] = proc.stdin_conn
            if proc.stdout_conn:
                results_dict["out"] = proc.stdout_conn

        future_conn.set_result(results_dict)

        # add grp to list of managed groups
        self.grps.append(grp)

        return future_conn

    def shutdown(self):
        if self.grps:
            for grp in self.grps:
                grp.join()
                grp.close()
            self.grps = []
        return True


class MPIApp(AppBase):
    """Extends AppBase to cover the MPI App."""

    def __init__(
        self,
        func,
        data_flow_kernel=None,
        cache=False,
        executors=["dragon_process_group"],
        ignore_for_cache=None,
        join=False,
    ):
        super().__init__(
            wrap_error(func),
            data_flow_kernel=data_flow_kernel,
            executors=executors,
            cache=cache,
            ignore_for_cache=ignore_for_cache,
        )
        self.join = join

    def __call__(self, *args, **kwargs):
        """This is where the call to a app is handled.

        Args:
             - Arbitrary
        Kwargs:
             - Arbitrary

        Returns:
                   App_fut

        """
        invocation_kwargs = {}
        invocation_kwargs.update(self.kwargs)
        invocation_kwargs.update(kwargs)

        if self.data_flow_kernel is None:
            dfk = DataFlowKernelLoader.dfk()
        else:
            dfk = self.data_flow_kernel

        func = self.func
        app_fut = dfk.submit(
            func,
            app_args=args,
            executors=self.executors,
            cache=self.cache,
            ignore_for_cache=self.ignore_for_cache,
            app_kwargs=invocation_kwargs,
            join=self.join,
        )

        return app_fut


@typeguard.typechecked
def mpi_app(
    function: callable = None,
    data_flow_kernel: Optional[DataFlowKernel] = None,
    cache: bool = False,
    executors: Union[List[str], Literal["all"]] = ["dragon_process_group"],
    ignore_for_cache: Optional[List[str]] = None,
):
    """wraps an mpi application

    :param function: function that returns arguments to start Process group and run the mpi app, defaults to None
    :type function: callable, optional
    :param data_flow_kernel: dataflow kernel to use, defaults to None
    :type data_flow_kernel: Optional[DataFlowKernel], optional
    :param cache: whether to cache. Passed to the dataflow kernel, defaults to False
    :type cache: bool, optional
    :param executors: executor used to launch mpi app. This has to be the dragon_process_group executor, defaults to ["dragon_process_group"]
    :type executors: Union[List[str], Literal[&quot;all&quot;]], optional
    :param ignore_for_cache: whether to ignore for cache. Passed to the dataflow kernel, defaults to None
    :type ignore_for_cache: Optional[List[str]], optional
    """

    def decorator(func: callable) -> cf.Future:
        """decorates a function that returns ProcessGroup parameters, the mpi app, and args for the mpi app.

        :param func: function that can do things but must return info for job
        :type func: callable
        :return: future returned by the executor's submit call. It holds connections to stdin and stdout of rank 0 of the MPI app
        :rtype: concurrent.futures.Future
        """

        def wrapper(f: callable):
            return MPIApp(
                f,
                data_flow_kernel=data_flow_kernel,
                cache=cache,
                executors=executors,
                ignore_for_cache=ignore_for_cache,
                join=False,
            )

        return wrapper(func)

    if function is not None:
        return decorator(function)
    return decorator
