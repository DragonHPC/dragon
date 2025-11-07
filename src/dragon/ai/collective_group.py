import os

from ..infrastructure.policy import Policy
from ..native.process_group import ProcessGroup
from ..native.process import ProcessTemplate, Popen
from ..native.machine import System, Node


class RankInfo:
    """Utility class, to be used by process part of a :py:meth:`~dragon.ai.collective_group.CollectiveGroup`, for
    accessing process-specific metadata about the process group."""

    def __init__(self):
        self._my_local_rank = int(os.getenv("_DRAGON_CG_LOCAL_RANK"))
        self._my_rank = int(os.getenv("_DRAGON_CG_RANK"))
        self._my_node_rank = int(os.getenv("_DRAGON_CG_GROUP_RANK"))
        self._my_local_world_size = int(os.getenv("_DRAGON_CG_LOCAL_WORLD_SIZE"))
        self._world_size = int(os.getenv("_DRAGON_CG_WORLD_SIZE"))
        self._master_addr = os.getenv("_DRAGON_CG_MASTER_ADDR")
        self._master_port = os.getenv("_DRAGON_CG_MASTER_PORT")

    @property
    def master_addr(self) -> str:
        """Address string of where a master bootsrap server is running, such as for NCCL"""
        return self._master_addr

    @property
    def master_port(self) -> str:
        """Port string that a master bootsrap server is listening to, such as for NCCL"""
        return self._master_port

    @property
    def my_local_rank(self) -> int:
        """The node-local rank identifier of the calling process"""
        return self._my_local_rank

    @property
    def my_rank(self) -> int:
        """The global rank identifier of the calling process"""
        return self._my_rank

    @property
    def my_node_rank(self):
        """The global rank identifier of the node the calling process is on"""
        return self._my_node_rank

    @property
    def my_local_world_size(self):
        """The number of process in the process group on the same node as the calling process"""
        return self._my_local_world_size

    @property
    def world_size(self):
        """The total number of process in the process group"""
        return self._world_size


def _make_coll_env(
    rank: int,
    local_rank: int,
    node_rank: int,
    master_addr: str,
    master_port: str,
    world_size: int,
    local_world_size: int,
) -> dict:
    env = dict(os.environ).copy()
    env["_DRAGON_CG_MASTER_ADDR"] = master_addr
    env["_DRAGON_CG_MASTER_PORT"] = master_port
    env["_DRAGON_CG_RANK"] = str(rank)
    env["_DRAGON_CG_LOCAL_RANK"] = str(local_rank)
    env["_DRAGON_CG_WORLD_SIZE"] = str(world_size)
    env["_DRAGON_CG_LOCAL_WORLD_SIZE"] = str(local_world_size)
    env["_DRAGON_CG_GROUP_RANK"] = str(node_rank)

    return env


def CollectiveGroup(
    training_fn,
    training_args: tuple = None,
    training_kwargs: dict = None,
    nprocs: int = None,
    hide_stderr: bool = False,
    port: int = 29500,
    policies: list = None,
) -> ProcessGroup:
    """Configure and return a :py:class:`~dragon.native.process_group.ProcessGroup` suitable for distributed training
    jobs. This helper sets up environment variables and process templates necessary for a training job (like PyTorch
    DDP) over multiple nodes using NCCL or similar backends. Users can specify the group in two ways by either
    specifying total processes or by providing a list of policies.

    .. highlight:: python
    .. code-block:: python

        import torch.distributed as dist
        from dragon.native.machine import System
        from dragon.ai.collective_group import CollectiveGroup, RankInfo


        def train():
            rank_info = RankInfo()
            rank = rank_info.my_rank
            master_addr = rank_info.master_addr
            master_port = rank_info.master_port
            world_size = rank_info.world_size

            dist.init_process_group(
                backend="nccl",
                init_method=f"tcp://{master_addr}:{master_port}",
                world_size=world_size,
                rank=rank,
            )

            device = torch.device("cuda")  # the provided Policy already sets which GPU id to use
            tensor = torch.ones(1, device=device) * rank
            dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

            print(f"Rank {rank}: Tensor after all_reduce = {tensor.item()}")

            dist.destroy_process_group()

        gpu_policies = System().gpu_policies()
        pg = CollectiveGroup(
                training_fn=train,
                training_args=None,
                training_kwargs=None,
                policies=gpu_policies,
                hide_stderr=False,
                port=29500,
            )
        pg.init()
        pg.start()
        pg.join()
        pg.close()


        :param training_fn: The target function to run on each distributed process.
        :type training_fn: callable
        :param training_args: Positional arguments to pass to training_fn, defaults to None
        :type training_args: tuple, optional
        :param training_kwargs: Keyword arguments to pass to training_fn, defaults to None
        :type training_kwargs: dict, optional
        :param nprocs: (Not yet implemented) Total number of processes. Required if policies is not provided. Ignored if policies is a list, defaults to None
        :type nprocs: int, optional
        :param hide_stderr: If True, suppress standard error from the launched processes, defaults to False
        :type hide_stderr: bool, optional
        :param port: Master port for NCCL backend communication, defaults to 29500
        :type port: int, optional
        :param policies: List of Policy objects, one per process.
        :type policies: list, optional
        :return: A configured and initialized process group for distributed training
        :rtype: ProcessGroup

    """
    # TODO
    if policies is None:
        raise NotImplementedError("Calling CollectiveGroup without a list of policies is not yet supported")

    if nprocs is None and policies is None:
        raise ValueError("Must provide either nprocs or a list of policies.")

    if policies is None and nprocs <= 0:
        raise ValueError("nprocs must be > 0 if policies is not provided.")

    if isinstance(policies, list):
        nprocs = len(policies)

    rank_info = []
    node_info = {}
    node_rank = 0
    for rank in range(nprocs):
        try:
            host = policies[rank].host_name
        except KeyError:
            raise ValueError("Training groups require that host_name be specified for each Policy")

        try:
            node_info[host]["ppn"] += 1
        except KeyError:
            node_info[host] = {"ppn": 1, "rank": node_rank}
            node_rank += 1

        rank_info.append({"local_rank": (node_info[host]["ppn"] - 1), "node": host})

    pg = ProcessGroup(restart=False, pmi=None)
    master_node = policies[0].host_name
    master_port = str(port)
    stderr = Popen.DEVNULL if hide_stderr else None

    try:
        for rank in range(nprocs):
            node_rank = node_info[rank_info[rank]["node"]]["rank"]
            ppn = node_info[rank_info[rank]["node"]]["ppn"]
            local_rank = rank_info[rank]["local_rank"]
            policy = policies[rank]

            env = _make_coll_env(
                rank=rank,
                local_rank=local_rank,
                node_rank=node_rank,
                master_addr=master_node,
                master_port=master_port,
                world_size=nprocs,
                local_world_size=ppn,
            )

            template = ProcessTemplate(
                target=training_fn,
                args=training_args,
                kwargs=training_kwargs,
                env=env,
                policy=policy,
                stderr=stderr,
            )

            pg.add_process(nproc=1, template=template)

    except Exception as e:
        raise RuntimeError("Exception during ProcessTemplate setup") from e

    return pg
