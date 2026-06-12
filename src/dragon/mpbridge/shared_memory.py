"""Dragon's replacement for Multiprocessing Shared Memory.

Not Implemented.
"""

import multiprocessing.shared_memory


class DragonSharedMemory(multiprocessing.shared_memory.SharedMemory):  # Dummy for now
    def __init__(self, *args, **kwargs):
        raise NotImplementedError(f"DragonSharedMemory has not been implemented with Dragon, yet")
        super().__init__(*args, **kwargs)


class DragonShareableList(multiprocessing.shared_memory.ShareableList):  # Dummy for now
    def __init__(self, *args, **kwargs):
        raise NotImplementedError(f"DragonShareableList has not been implemented with Dragon, yet")
        super().__init__(*args, **kwargs)
