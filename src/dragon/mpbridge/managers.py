"""Dragon's replacement of Multiprocessing Managers. Not implemented at the moment."""

import multiprocessing.managers


class DragonSyncManager(multiprocessing.managers.SyncManager):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError(f"DragonSyncManager is not implemented, yet")
        super().__init__(*args, **kwargs)


class BaseImplSyncManager(multiprocessing.managers.SyncManager):  # Dummy for now
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DragonBaseManager(multiprocessing.managers.BaseManager):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError(f"DragonBaseManager is not implemented, yet")
        super().__init__(*args, **kwargs)


class DragonBaseProxy(multiprocessing.managers.BaseProxy):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError(f"DragonBaseProxy is not implemented, yet")
        super().__init__(*args, **kwargs)


class DragonToken(multiprocessing.managers.Token):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError(f"DragonToken is not implemented, yet")
        super().__init__(*args, **kwargs)


class DragonSharedMemoryManager(multiprocessing.managers.SharedMemoryManager):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError(f"DragonSharedMemoryManager is not implemented, yet")
        super().__init__(*args, **kwargs)


def Manager(*, ctx=None, use_base_impl=True):
    if use_base_impl:
        return BaseImplSyncManager(ctx=ctx)
    else:
        return DragonSyncManager(ctx=ctx)
