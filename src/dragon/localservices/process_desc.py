class ProcessOptions:
    """Open type, placeholder, for Shepherd options on how to launch a process."""

    def __init__(self, dummy=None):
        self.dummy = dummy  # until there are things to put in here

    def get_sdict(self):
        return {}

    @staticmethod
    def from_sdict(sdict):
        return ProcessOptions(**sdict)
