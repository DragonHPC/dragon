import enum
from dataclasses import asdict, dataclass, field

@dataclass
class Policy:
    """
    Dataclass to describe what node, affinity, and pattern to distrbiute a Policy to
    """

    class Placement(enum.IntEnum):
        """
        Which node to assign a policy to.

        Local and Anywhere will be useful later for multi-system communication
        Right now Placement will have little effect unless HOST_NAME or HOST_ID
        are used, which will try to place a policy on the specified node
        """
        LOCAL = -5 # Local to the current system of nodes
        ANYWHERE = -4 # What it says on the tin
        HOST_NAME = -3 # Tells evaluator to check host_name in policy and apply to that node
        HOST_ID = -2 # Tells evaluator to check host_id in policy and apply to that node
        DEFAULT = -1 # For now, defaults to ANYWHERE

    class Distribution(enum.IntEnum):
        """
        Pattern to use to distribute policies across nodes
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
        """
        ANY = -5
        # ROUNDROBIN = -4 # TODO: Not implemented
        # BLOCK = -3 # TODO: Not implemented
        SPECIFIC = -2
        DEFAULT = -1 # Same as ANY

    # TODO: Not implemented
    class WaitMode(enum.IntEnum):
        """Channel WaitMode type"""
        IDLE = enum.auto()
        SPIN = enum.auto()
        DEFAULT = enum.auto()

    placement : Placement = Placement.DEFAULT
    host_name : str = "" # To be populated by caller in use with Placement.HOST_NAME
    host_id : int = -1 # To be populated by caller in use with Placement.HOST_ID
    distribution : Distribution = Distribution.DEFAULT
    device : Device = Device.DEFAULT
    affinity : Affinity = Affinity.DEFAULT
    specific_affinity : list[int] = field(default_factory=list) # To be populated by called in use with Affinity.SPECIFIC
    wait_mode : WaitMode = WaitMode.DEFAULT # TODO (For channels)
    refcounted : bool = True # TODO (Apply refcounting for this resource)

    @property
    def sdesc(self):
        return self.get_sdict()

    def get_sdict(self):
        rv = {
            "placement" : self.placement,
            "host_name" : self.host_name,
            "host_id" : self.host_id,
            "distribution" : self.distribution,
            "device" : self.device,
            "affinity" : self.affinity,
            "specific_affinity" : self.specific_affinity,
            "wait_mode" : self.wait_mode,
            "refcounted" : self.refcounted,
        }

        return rv

    @classmethod
    def from_sdict(cls, sdict):
        return Policy(**sdict)


# Default policy to merge incoming policies against
GS_DEFAULT_POLICY = Policy(
    placement=Policy.Placement.ANYWHERE,
    host_name="",
    host_id=-1,
    distribution=Policy.Distribution.ROUNDROBIN,
    device=Policy.Device.CPU,
    affinity=Policy.Affinity.ANY,
    specific_affinity=[],
    wait_mode=Policy.WaitMode.IDLE,
    refcounted=True
)