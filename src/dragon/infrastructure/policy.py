"""A class for controlling object or process affinity to physical resources."""
import os
import enum
from dataclasses import dataclass, field, fields
import threading
import json

from .facts import DRAGON_POLICY_CONTEXT_ENV

from typing import Dict


@dataclass
class Policy:
    """
    A Policy is used to define the placement or affinity of an object or process within the Dragon
    allocation to specific physical resources like nodes, CPUs, or GPUs. The use and evaluation of
    a Policy must be consistent across the multiple ways of starting a process
    (:py:class:`~dragon.native.process.Process`, :py:class:`multiprocessing.Process`,
    :py:class:`~dragon.native.process_group.ProcessGroup`, etc).

    Policy Hierarchy
    ----------------
    There are multiple ways and places that you can set a Policy. Dragon has defined the following hierarchy and will
    attempt to merge multiple policies in priority order (with 1 being the highest priority and higher numbers being a
    lower priority) to create a single combined policy for any given object:

    1. An explicit policy passed to the :py:class:`~dragon.native.process.Process`,
       :py:class:`~dragon.native.process_group.ProcessGroup`, or :ref:`Global Service API<DragonGSClient>`
    2. If creating a :py:class:`~dragon.native.process_group.ProcessGroup`, the process group's policy will be merged
       with the policy (if any) of each :py:class:`~dragon.native.process.ProcessTemplate`
    3. Use of the Python based Policy context manager.
    4. The Global Policy

    The Global Policy
    -----------------
    The `global_policy` object defines the default policy values to use when not otherwise set. The `global_policy`
    object will always be merged with any user supplied Policy before evaluating the policy to ensure that the resultant
    Policy object is complete and valid.

        .. highlight:: python
        .. code-block:: python

            GS_DEFAULT_POLICY = Policy(
                placement=Policy.Placement.ANYWHERE,
                host_name="",
                host_id=-1,
                distribution=Policy.Distribution.ROUNDROBIN,
                cpu_affinity=[],
                gpu_env_str="",
                gpu_affinity=[],
                wait_mode=Policy.WaitMode.IDLE,
                refcounted=True,
            )

    There is no way currently to modify or change the `global_policy` object.

    Examples
    ========

    No Policy
    ---------

    In the case that no policy is passed, the object being created will use the default `global_policy`.

        .. highlight:: python
        .. code-block:: python

            from dragon.native import Process

            process = Process(
                target=cmdline,
            )

    Single Explicit policy
    ----------------------

    Any user supplied policy passed to the object's constructor, or :ref:`Global Service API<DragonGSClient>`, will
    first be merged with the `global_policy` object. The resulting merged Policy is what will be used by the object
    being created.

        .. highlight:: python
        .. code-block:: python

            from dragon.infrastructure.policy import Policy
            from dragon.native import Process

            policy = Policy(Placement=Policy.Placement.LOCAL)
            process = Process(
                target=cmdline,
                policy=policy,
            )

    The resulting Policy for the above example will be

        .. highlight:: python
        .. code-block:: python

            Policy(
                   placement=Policy.Placement.LOCAL,              # from policy passed to object constructor.
                   distribution=Policy.Distribution.ROUNDROBIN,   # from global_policy.
                   wait_mode=Policy.WaitMode.IDLE,                # from global_policy. Not currently used.
                   refcounted=True,                               # from global_policy. Not currently used.
                   ...
                   )

    Using Policy Context Manager
    ----------------------------

    The Policy context manager that can be used to establish a thread- local stack of Policy objects.
    Any :py:class:`~dragon.native.process.Process` or :py:class:`~dragon.native.process_group.ProcessGroup` object
    that is created within this context will inherit the Policy defined by the nested stack of Policy objects.

        .. highlight:: python
        .. code-block:: python

            from dragon.infrastructure.policy import Policy

            with Policy(placement=Policy.Placement.HOST_NAME, host_name=socket.gethostname()):
                proc = mp.Process(target=cmdline)
                proc.start()

    In the above case, the Policy context manager is used to help place a Python :py:class:`multiprocessing.Process`
    object, which otherwise does not accept a policy parameter, within the Dragon allocation on a specific host.

        .. highlight:: python
        .. code-block:: python

            from dragon.infrastructure.policy import Policy
            from dragon.native import Process

            with Policy(placement=Policy.Placement.HOST_NAME, host_name=socket.gethostname()):
                policy = Policy(gpu_affinity=GPU_AFFINITY)
                process = Process(target=cmdline, policy=policy)


    In this example, the policy object passed to the :py:class:`~dragon.native.process.Process` object constructor and
    the policy created via the Policy context manager will be merged. The resultant policy will be:

        .. highlight:: python
        .. code-block:: python

            Policy(
                   placement = Policy.Placement.HOST_NAME,        # from Policy Context Manager.
                   hostname = socket.gethostname(),               # from Policy Context Manager.
                   distribution=Policy.Distribution.ROUNDROBIN,   # from global_policy.
                   gpu_affinity = GPU_AFFINITY                    # from explicit policy passed to the Process object.
                   wait_mode=Policy.WaitMode.IDLE,                # from global_policy. Not currently used.
                   refcounted=True,                               # from global_policy. Not currently used.
                   )

    In a case where there are multiple, nested Policy context managers, the policy of each context manager
    will be merged together (from inner-most to outer-most) before being merged with any policy passed to the
    object being created.

        .. highlight:: python
        .. code-block:: python

            from dragon.infrastructure.policy import Policy
            from dragon.native import Process

            with Policy(placement=Policy.Placement.ANYWHERE, distribution=Policy.Distribution.BLOCK):
                with Policy(placement=Policy.Placement.HOST_NAME, host_name=socket.gethostname()):
                    policy = Policy(gpu_affinity=GPU_AFFINITY)
                    process = Process(target=cmdline, policy=policy)

    In this case, the resultant policy will be:

        .. highlight:: python
        .. code-block:: python

            Policy(
                   distribution=Policy.Distribution.BLOCK,  # from outer-most context manager.
                   placement=Policy.Placement.HOST_NAME,    # from inner-most context manager.
                   host_name=socket.gethostname(),          # from inner-most context manager.
                   gpu_affinity=GPU_AFFINITY,               # from explicit policy passed to the constructor.
                   wait_mode=Policy.WaitMode.IDLE,          # from the global_policy. Not currently used.
                  )

    Starting a ProcessGroup
    =======================

    When creating a process group, a policy can be added on both the process group and process templates. The
    dragon Default policy will first be merged with the process group policy. The resultant policy will then be
    merged with each process template's policy.

        .. highlight:: python
        .. code-block:: python

            from dragon.native.process_group import ProcessGroup
            from dragon.native.process import MSG_PIPE, MSG_DEVNULL, Process, ProcessTemplate

            group_policy = Policy(distribution=Policy.Distribution.BLOCK)
            grp = ProcessGroup(restart=False, pmi_enabled=True, polic=group_policy)

            template_policy_1 = Policy(gpu_affinity=GPU_AFFINITY_1)

            # Pipe the stdout output from the head process to a Dragon connection
            grp.add_process(
                            nproc=1,
                            template=ProcessTemplate(target=exe, args=args, cwd=run_dir, stdout=MSG_PIPE,
                            policy=template_policy_1)
                           )

            template_policy_2 = Policy(gpu_affinity=GPU_AFFINITY_2)

            # All other ranks should have their output go to DEVNULL
            grp.add_process(
                            nproc=num_ranks-1,
                            template=ProcessTemplate(target=exe, args=args, cwd=run_dir, stdout=MSG_DEVNULL,
                            policy=template_policy_2)
                            )

    The first process template will have a policy matching

        .. highlight:: python
        .. code-block:: python

            Policy(
                   placement = Policy.Placement.ANYWHERE,         # from global_policy
                   distribution=Policy.Distribution.BLOCK,        # from Group policy
                   gpu_affinity = GPU_AFFINITY_1                  # from teplate_policy_1
                   wait_mode=Policy.WaitMode.IDLE,                # from global_policy. Not currently used.
                   refcounted=True,                               # from global_policy. Not currently used.
                  )

    The rest of the process templates will have a policy matching

        .. highlight:: python
        .. code-block:: python

            Policy(
                   placement = Policy.Placement.ANYWHERE,         # from global_policy
                   distribution=Policy.Distribution.BLOCK,        # from Group policy
                   gpu_affinity = GPU_AFFINITY_2                  # from teplate_policy_2
                   wait_mode=Policy.WaitMode.IDLE,                # from global_policy. Not currently used.
                   refcounted=True,                               # from global_policy. Not currently used.
            )
"""

    class Placement(enum.IntEnum):
        """
        Which node to assign a policy to.

        Local and Anywhere will be useful later for multi-system communication
        Right now Placement will have little effect unless HOST_NAME or HOST_ID
        are used, which will try to place a policy on the specified node

        LOCAL - Local to current system of nodes
        ANYWHERE - Place anywhere
        HOST_NAME - Place on node with specific name
        HOST_ID - Place on node with specific ID
        DEFAULT - Defaults to ANYWHERE
        """

        LOCAL = -5  # Local to the current system of nodes
        ANYWHERE = -4  # What it says on the tin
        HOST_NAME = -3  # Tells evaluator to check host_name in policy and apply to that node
        HOST_ID = -2  # Tells evaluator to check host_id in policy and apply to that node
        DEFAULT = -1  # For now, defaults to ANYWHERE

    class Distribution(enum.IntEnum):
        """
        Pattern to use to distribute policies across nodes

        ROUNDROBIN
        BLOCK
        DEFAULT - Defaults to roundrobin
        """

        ROUNDROBIN = enum.auto()
        BLOCK = enum.auto()
        DEFAULT = enum.auto()

    # TODO: Not implemented
    class WaitMode(enum.IntEnum):
        """Channel WaitMode type"""

        IDLE = enum.auto()
        SPIN = enum.auto()
        DEFAULT = enum.auto()

    placement: Placement = Placement.DEFAULT
    host_name: str = ""  # To be populated by caller in use with Placement.HOST_NAME
    host_id: int = -1  # To be populated by caller in use with Placement.HOST_ID
    distribution: Distribution = Distribution.DEFAULT
    cpu_affinity: list[int] = field(
        default_factory=list
    )  # To be populated by called in use with Affinity.SPECIFIC, specify exact device IDs or cores
    gpu_env_str: str = ""  # To be used with gpu_affinity for vendor specific environment vars
    gpu_affinity: list[int] = field(default_factory=list)
    wait_mode: WaitMode = WaitMode.DEFAULT  # TODO (For channels)
    refcounted: bool = True  # TODO (Apply refcounting for this resource)

    def __post_init__(self):
        if isinstance(self.placement, int):
            self.placement = Policy.Placement(self.placement)
        if isinstance(self.distribution, int):
            self.distribution = Policy.Distribution(self.distribution)
        if isinstance(self.WaitMode, int):
            self.wait_mode = Policy.WaitMode(self.wait_mode)

    @property
    def sdesc(self) -> Dict:
        return self.get_sdict()

    def get_sdict(self) -> Dict:
        rv = {
            "placement": self.placement,
            "host_name": self.host_name,
            "host_id": self.host_id,
            "distribution": self.distribution,
            "cpu_affinity": self.cpu_affinity,
            "gpu_env_str": self.gpu_env_str,
            "gpu_affinity": self.gpu_affinity,
            "wait_mode": self.wait_mode,
            "refcounted": self.refcounted,
        }

        return rv

    @classmethod
    def from_sdict(cls, sdict):
        return Policy(**sdict)

    @classmethod
    def _init_thread_policy(cls):
        # This is put in thread-local storage to insure that it behaves in a
        # multi-threaded environment.
        if not hasattr(_policy_local, "policy_stack"):
            # start with an empty list.
            _policy_local.policy_stack = []
            try:
                policy_env = os.environ[DRAGON_POLICY_CONTEXT_ENV]
                policy_sdesc = json.loads(policy_env)
                _policy_local.policy_stack.append(Policy.from_sdict(policy_sdesc))
            except (KeyError, json.JSONDecodeError):
                pass

    @classmethod
    def thread_policy(cls):
        return_val = cls()
        cls._init_thread_policy()

        # The following merges the policy stack, returning a policy that
        # reflects the aggregate policy defined by the stack.
        if _policy_local.policy_stack:

            # Iterate backwards through the policy stack, merging each
            # sucessive policy into our aggregate return value
            for i in range(-1, -len(_policy_local.policy_stack) - 1, -1):
                return_val = Policy.merge(_policy_local.policy_stack[i], return_val)

        return cls.from_sdict(return_val.get_sdict())

    def __enter__(self):
        self._init_thread_policy()
        _policy_local.policy_stack.append(self)

    def __exit__(self, *args):
        _policy_local.policy_stack.pop()

    @classmethod
    def merge(cls, low_policy, high_policy):
        """
        Merge two policies, using values from high_policy for values not assigned on init
        Returns a new policy

        :param low_policy: Default values will be replaced by `high_policy` values
        :type low_policy: Policy
        :param high_policy: Non-default values take precedence
        :type high_policy: Policy
        :return: Merged policy object
        :rtype: Policy
        """

        retain = {}

        for f in fields(high_policy):
            v = getattr(high_policy, f.name)

            # if the value is a Default value, do not retain
            if v == f.default:
                continue

            # Lists use default_factory to create a new instance, so checking f.default doesn't work.
            # If the list 'v' is empty (as in 'not v'), do not retain.
            if isinstance(v, list) and not v:
                continue

            retain[f.name] = v  # Value was assigned specifically, retain

        kwargs = {**low_policy.get_sdict(), **retain}  # Merge retained policy values with high priority values
        return cls(**kwargs)

    @classmethod
    def global_policy(cls):
        return cls.from_sdict(GS_GLOBAL_POLICY_SDICT)


# Global policy to merge incoming policies against
GS_GLOBAL_POLICY_SDICT = {
    "placement": Policy.Placement.ANYWHERE,
    "distribution": Policy.Distribution.ROUNDROBIN,
    "wait_mode": Policy.WaitMode.IDLE,
}

# _policy_local is private to the current thread
_policy_local = threading.local()
