import enum
from dataclasses import asdict, dataclass, field
from dragon.infrastructure.util import stack
import threading


@dataclass
class Policy:
    """
    The purpose of the Policy object is to enable fine-tuning of process distribution
    across nodes, devices, cores, and etc.
    For example spawning a process that should only run on certain CPU cores, or only have
    access to specific GPU devices on a node, or how to spread a collection of processes
    over nodes (e.g. Round-robin, Blocking, etc).

    There is a default global Policy in place that utilizes no core or accelerator affinity,
    and places processes across nodes in a round-robin fashion.  Using Policy is meant to fine-tune
    this default policy, and any non-specific attributes will use the default policy values.

    For a full example of setup for launching processes through Global Services, see `examples/dragon_gs_client/pi_demo.py`
    When launching a process simply create a policy with the desired fine-tuning, and pass it in as a paramter.

    .. code-block:: python
        import dragon.globalservices.process as dgprocess
        import dragon.infrastructure.policy as dgpol

        # Do setup
        # ...
        # ...
        # Create a custom process for core affinity and selecting a specific GPU
        policy = dgpol.Policy(core_affinity=[0,2], gpu_affinity=[0])
        # Launch the process with the fine-tuned policy
        p = dgprocess.create(cmd, wdir, cmd_params, None, options=options, policy=policy)
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

    class Device(enum.IntEnum):
        """
        Which type of device the affinity policy will apply to
        """

        CPU = enum.auto()
        GPU = enum.auto()
        DEFAULT = enum.auto()

    # TODO: The NodeDescriptor may need to hold its devices in a dictionary so we can count how many devices have been used to do some kind of rudamentary load-balancing
    class Affinity(enum.IntEnum):
        """
        Device Affinity distribution

        ANY - Place on any available core or GPU device
        SPECIFIC - Place to specific CPU core affinities or GPU device
        DEFAULT - Defaults to Any
        """

        ANY = -5
        # ROUNDROBIN = -4 # TODO: Not implemented
        # BLOCK = -3 # TODO: Not implemented
        SPECIFIC = -2
        DEFAULT = -1  # Same as ANY

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
    device: Device = Device.DEFAULT  # What device to apply to
    affinity: Affinity = Affinity.DEFAULT  # Affinity distribution
    cpu_affinity: list[int] = field(
        default_factory=list
    )  # To be populated by called in use with Affinity.SPECIFIC, specify exact device IDs or cores
    gpu_env_str: str = ""  # To be used with gpu_affinity for vendor specific environment vars
    gpu_affinity: list[int] = field(default_factory=list)
    wait_mode: WaitMode = WaitMode.DEFAULT  # TODO (For channels)
    refcounted: bool = True  # TODO (Apply refcounting for this resource)

    @property
    def sdesc(self):
        return self.get_sdict()

    def get_sdict(self):
        rv = {
            "placement": self.placement,
            "host_name": self.host_name,
            "host_id": self.host_id,
            "distribution": self.distribution,
            "device": self.device,
            "affinity": self.affinity,
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
    def _init_global_policy(cls):
        # This is put in thread-local storage to insure that it behaves in a
        # multi-threaded environment.
        if not hasattr(_policy_local, "policy_stack"):
            _policy_local.policy_stack = stack([GS_DEFAULT_POLICY])

    @classmethod
    def global_policy(cls):
        cls._init_global_policy()
        # The following clones the policy so that the original is not
        # mutated should the user mutate the returned policy.
        return cls.from_sdict(_policy_local.policy_stack.top().get_sdict())

    def __enter__(self):
        self._init_global_policy()
        _policy_local.policy_stack.push(self)

    def __exit__(self, *args):
        _policy_local.policy_stack.pop()


# Default policy to merge incoming policies against
GS_DEFAULT_POLICY = Policy(
    placement=Policy.Placement.ANYWHERE,
    host_name="",
    host_id=-1,
    distribution=Policy.Distribution.ROUNDROBIN,
    device=Policy.Device.CPU,
    affinity=Policy.Affinity.ANY,
    cpu_affinity=[],
    gpu_env_str="",
    gpu_affinity=[],
    wait_mode=Policy.WaitMode.IDLE,
    refcounted=True,
)

# _policy_local is private to the current thread
_policy_local = threading.local()
