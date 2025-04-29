"""A class to monkeypatch Dragon classes into Python Multiprocessing.

Monkeypatching fixes direct imports and is necessary, if the user asks for
Multiprocessing classes and functions without the context. In particular, people
may use direct imports before the `start_method` is actually set. Hence we have to
do this in dragons __init__.py, i.e. before `import Multiprocessing` is executed
by the user for the first time.

Consider:

.. code-block:: Python

    import dragon
    import multiprocessing as mp
    from multiprocessing.connection import wait

    if __name__ == "__main__":
        mp.set_start_method("dragon")
        ctx = mp.get_context()
        print(f"{wait.__module__=}")
        print(f"{ctx.wait.__module__=}")


without correct patching in __init__.py this would print:

.. code-block:: Python

    wait.__module__='multiprocessing.connection'
    ctx.wait.__module__='dragon.mpbridge.context'
"""

import dragon
import multiprocessing
import os


class Monkeypatcher:
    """Class managing the monkeypatching functionality.

    We replace the Multiprocessing API at the top level and at the submodule
    level to facilitate direct imports. The difference is that the top level import returns a
    function, while the submodule import returns the class.

    To understand this, consider:

    .. code-block:: Python

        import multiprocessing as mp
        from multiprocessing.pool import Pool

        p1 = mp.Pool # is a method
        p2 = Pool # is a class
        p3 = mp.Pool() # is the object
        p4 = Pool() # is also an object

    Note that multiprocessing is not designed in the same way everywhere.
    The standard pattern is that a function (`ctx.Queue()`) does `import multiprocessing.queues.Queue`
    and returns `Queue()`.
    However, some parts of the public API are not below the context, other parts have to be explicitly
    imported. A mechanism that works for the standard pattern, will leave other parts of the API
    unchanged.

    We store the original classes and functions in private attributes for the
    brave.

    If the switching is done before the user code is parsed, this replaces _all_
    mentions of Multiprocessing objects with Dragon ones, including the
    inheritance tree.
    """

    def __init__(self):
        """Store the original Multiprocessing classes"""
        from multiprocessing.managers import BaseManager, SyncManager, BaseProxy, Token
        from multiprocessing.util import get_logger, log_to_stderr
        from multiprocessing.heap import Arena

        # context functions not backed by classes
        self.cpu_count = multiprocessing.cpu_count
        self.active_children = multiprocessing.active_children
        self.parent_process = multiprocessing.parent_process
        self.current_process = multiprocessing.current_process
        self.log_to_stderr = multiprocessing.log_to_stderr
        self.get_logger = multiprocessing.get_logger
        self.freeze_support = multiprocessing.freeze_support

        # context functions backed by classes
        self.Process = multiprocessing.Process

        self.Queue = multiprocessing.Queue
        self.JoinableQueue = multiprocessing.JoinableQueue
        self.SimpleQueue = multiprocessing.SimpleQueue

        self.Event = multiprocessing.Event
        self.Lock = multiprocessing.Lock
        self.RLock = multiprocessing.RLock
        self.Barrier = multiprocessing.Barrier
        self.Condition = multiprocessing.Condition
        self.Semaphore = multiprocessing.Semaphore
        self.BoundedSemaphore = multiprocessing.BoundedSemaphore

        self.Manager = multiprocessing.Manager

        self.Pipe = multiprocessing.Pipe

        self.Pool = multiprocessing.Pool

        self.Array = multiprocessing.Array
        self.RawArray = multiprocessing.RawArray
        self.Value = multiprocessing.Value
        self.RawValue = multiprocessing.RawValue

        # submodule classes and functions
        self.process_current_process = multiprocessing.process.current_process
        self.process_parent_process = multiprocessing.process.parent_process
        self.process_active_children = multiprocessing.process.active_children

        self.queues_Queue = multiprocessing.queues.Queue
        self.queues_JoinableQueue = multiprocessing.queues.JoinableQueue
        self.queues_SimpleQueue = multiprocessing.queues.SimpleQueue

        self.synchronize_Event = multiprocessing.synchronize.Event
        self.synchronize_Barrier = multiprocessing.synchronize.Barrier
        self.synchronize_Lock = multiprocessing.synchronize.Lock
        self.synchronize_RLock = multiprocessing.synchronize.RLock
        self.synchronize_Condition = multiprocessing.synchronize.Condition
        self.synchronize_Semaphore = multiprocessing.synchronize.Semaphore
        self.synchronize_BoundedSemaphore = multiprocessing.synchronize.BoundedSemaphore

        self.managers_BaseManager = BaseManager  # only accessible via direct import :-S
        self.managers_SyncManager = SyncManager
        self.managers_BaseProxy = BaseProxy
        self.managers_Token = Token

        self.connection_wait = multiprocessing.connection.wait
        self.connection_Pipe = multiprocessing.connection.Pipe
        self.connection_Listener = multiprocessing.connection.Listener
        self.connection_Client = multiprocessing.connection.Client

        self.pool_Pool = multiprocessing.pool.Pool
        self.pool_wait = multiprocessing.pool.wait

        self.sharedctypes_Array = multiprocessing.sharedctypes.Array
        self.sharedctypes_RawArray = multiprocessing.sharedctypes.RawArray
        self.sharedctypes_Value = multiprocessing.sharedctypes.Value
        self.sharedctypes_RawValue = multiprocessing.sharedctypes.RawValue
        self.sharedctypes_copy = multiprocessing.sharedctypes.copy
        self.sharedctypes_synchronized = multiprocessing.sharedctypes.synchronized

        self.reduction_DupFd = multiprocessing.reduction.DupFd
        self.reduction_recvfds = multiprocessing.reduction.recvfds
        self.reduction_sendfds = multiprocessing.reduction.sendfds

        self.util_get_logger = get_logger
        self.util_log_to_stderr = log_to_stderr

        self.heap_Arena = Arena

    def switch_out(self, ctx) -> None:
        """Replace the standard Multiprocessing classes and functions outside the
        context with Dragons versions.

        To do so, we need the instantiated Dragon context class which is held next to
        the other context in `multiprocessing.context._concrete_contexts`.

        :param ctx: The actual Dragon context.
        :type ctx: dragon.mpbridge.context.DragonContext
        """

        from dragon.mpbridge.managers import (
            DragonBaseManager,
            DragonSyncManager,
            DragonBaseProxy,
            DragonToken,
        )
        from dragon.mpbridge.util import get_logger, log_to_stderr
        from dragon.mpbridge.heap import DragonArena

        # context functions not backed by classes
        multiprocessing.cpu_count = ctx.cpu_count
        multiprocessing.active_children = ctx.active_children
        multiprocessing.parent_process = ctx.parent_process
        multiprocessing.current_process = ctx.current_process
        multiprocessing.log_to_stderr = ctx.log_to_stderr
        multiprocessing.get_logger = ctx.get_logger
        multiprocessing.freeze_support = ctx.freeze_support

        # context functions backed by classes
        multiprocessing.Process = ctx.Process

        multiprocessing.Queue = ctx.Queue
        multiprocessing.JoinableQueue = ctx.JoinableQueue
        multiprocessing.SimpleQueue = ctx.SimpleQueue

        multiprocessing.Event = ctx.Event
        multiprocessing.Lock = ctx.Lock
        multiprocessing.RLock = ctx.RLock
        multiprocessing.Barrier = ctx.Barrier
        multiprocessing.Condition = ctx.Condition
        multiprocessing.Semaphore = ctx.Semaphore
        multiprocessing.BoundedSemaphore = ctx.BoundedSemaphore

        multiprocessing.Pipe = ctx.Pipe

        multiprocessing.Manager = ctx.Manager

        multiprocessing.Pool = ctx.Pool

        multiprocessing.Array = ctx.Array
        multiprocessing.RawArray = ctx.RawArray
        multiprocessing.Value = ctx.Value
        multiprocessing.RawValue = ctx.RawValue

        # submodule classes and functions are directly replaced to facilitate
        # direct imports of classes via e.g. `from multiprocessing.pool import Pool`
        # and subclassing, i.e. inheritance from Multiprocessing classes.

        # multiprocessing.process.current_process = dragon.mpbridge.process.current_process # we use the original
        # multiprocessing.process.parent_process = dragon.mpbridge.process.parent_process # we use the original
        # multprocessing.active_children = dragon.mpbridge.process.active_children # we use the original

        multiprocessing.queues.Queue = dragon.mpbridge.queues.DragonQueue  # WithProcessesTestQueue.test_fork
        multiprocessing.queues.JoinableQueue = (
            dragon.mpbridge.queues.DragonJoinableQueue
        )  # WithProcessesTestQueue.test_task_done
        multiprocessing.queues.SimpleQueue = dragon.mpbridge.queues.DragonSimpleQueue

        multiprocessing.synchronize.Event = dragon.mpbridge.synchronize.DragonEvent
        multiprocessing.synchronize.Lock = (
            dragon.mpbridge.synchronize.DragonLock
        )  # WithProcessesTestConnection.test_spawn_close
        multiprocessing.synchronize.RLock = dragon.mpbridge.synchronize.DragonRLock
        multiprocessing.synchronize.Barrier = dragon.mpbridge.synchronize.DragonBarrier
        multiprocessing.synchronize.Condition = dragon.mpbridge.synchronize.DragonCondition
        multiprocessing.synchronize.Semaphore = dragon.mpbridge.synchronize.DragonSemaphore
        multiprocessing.synchronize.BoundedSemaphore = dragon.mpbridge.synchronize.DragonBoundedSemaphore

        multiprocessing.connection.wait = dragon.mpbridge.context.DragonContext.wait  # TODO: PE-41642
        multiprocessing.connection.Pipe = ctx.Pipe  ## ??
        multiprocessing.connection.Listener = ctx.Listener
        multiprocessing.connection.Client = ctx.Client

        dragon_base_pool = os.getenv("DRAGON_BASEPOOL", "NATIVE")
        if dragon_base_pool == "PATCHED":
            multiprocessing.pool.Pool = dragon.mpbridge.pool.DragonPoolPatched
        else:
            multiprocessing.pool.Pool = dragon.mpbridge.pool.DragonPool
        multiprocessing.pool.wait = ctx.wait

        multiprocessing.sharedctypes.Array = dragon.mpbridge.sharedctypes.DragonArray
        multiprocessing.sharedctypes.RawArray = dragon.mpbridge.sharedctypes.DragonRawArray
        multiprocessing.sharedctypes.Value = dragon.mpbridge.sharedctypes.DragonValue
        multiprocessing.sharedctypes.RawValue = dragon.mpbridge.sharedctypes.DragonRawValue
        multiprocessing.sharedctypes.copy = dragon.mpbridge.sharedctypes.dragon_copy
        multiprocessing.sharedctypes.synchronized = dragon.mpbridge.sharedctypes.dragon_synchronized

        multiprocessing.reduction.DupFd = dragon.mpbridge.reduction.dragon_DupFd
        multiprocessing.reduction.recvfds = dragon.mpbridge.reduction.dragon_recvfds
        multiprocessing.reduction.sendfds = dragon.mpbridge.reduction.dragon_sendfds

        multiprocessing.util.get_logger = get_logger
        multiprocessing.util.log_to_stderr = log_to_stderr

        # Some submodules are declared public via __all__ but are not below the context
        # and are not accessible via `multiprocessing.module.Class`. Instead they must
        # be imported with `from multiprocessing.module import Class` or `import multiprocessing.module`
        # and `obj = multiprocessing.module.Class()`

        multiprocessing.managers.BaseManager = DragonBaseManager  # mimic insane multiprocessing structure here
        multiprocessing.managers.SyncManager = DragonSyncManager
        multiprocessing.managers.BaseProxy = DragonBaseProxy
        multiprocessing.managers.Token = DragonToken

        multiprocessing.heap.Arena = DragonArena

    def switch_in(self) -> None:
        """Restore standard Multiprocessing classes and functions"""

        # context functions not backed by classes
        multiprocessing.cpu_count = self.cpu_count
        multiprocessing.active_children = self.active_children
        multiprocessing.parent_process = self.parent_process
        multiprocessing.current_process = self.current_process
        multiprocessing.log_to_stderr = self.log_to_stderr
        multiprocessing.get_logger = self.get_logger
        multiprocessing.freeze_support = self.freeze_support

        # context functions
        multiprocessing.Process = self.Process

        multiprocessing.Queue = self.Queue
        multiprocessing.JoinableQueue = self.JoinableQueue
        multiprocessing.SimpleQueue = self.SimpleQueue

        multiprocessing.Event = self.Event
        multiprocessing.Lock = self.Lock
        multiprocessing.RLock = self.RLock
        multiprocessing.Barrier = self.Barrier
        multiprocessing.Condition = self.Condition
        multiprocessing.Semaphore = self.Semaphore
        multiprocessing.BoundedSemaphore = self.BoundedSemaphore

        multiprocessing.Manager = self.Manager

        multiprocessing.Pool = self.Pool

        multiprocessing.Pipe = self.Pipe

        multiprocessing.Array = self.Array
        multiprocessing.RawArray = self.RawArray
        multiprocessing.Value = self.Value
        multiprocessing.RawValue = self.RawValue

        # submodule classes and functions
        multiprocessing.queues.Queue = self.queues_Queue
        multiprocessing.queues.JoinableQueue = self.queues_JoinableQueue
        multiprocessing.queues.SimpleQueue = self.queues_SimpleQueue

        multiprocessing.connection.wait = self.connection_wait
        multiprocessing.connection.Pipe = self.connection_Pipe
        multiprocessing.connection.Listener = self.connection_Listener
        multiprocessing.connection.Client = self.connection_Client

        multiprocessing.synchronize.Event = self.synchronize_Event
        multiprocessing.synchronize.Lock = self.synchronize_Lock
        multiprocessing.synchronize.RLock = self.synchronize_RLock
        multiprocessing.synchronize.Barrier = self.synchronize_Barrier
        multiprocessing.synchronize.Condition = self.synchronize_Condition
        multiprocessing.synchronize.Semaphore = self.synchronize_Semaphore
        multiprocessing.synchronize.BoundedSemaphore = self.synchronize_BoundedSemaphore

        multiprocessing.pool.Pool = self.pool_Pool
        multiprocessing.pool.wait = self.pool_wait

        multiprocessing.managers.BaseManager = self.managers_BaseManager
        multiprocessing.managers.SyncManager = self.managers_SyncManager
        multiprocessing.managers.BaseProxy = self.managers_BaseProxy
        multiprocessing.managers.Token = self.managers_Token

        multiprocessing.sharedctypes.Array = self.sharedctypes_Array
        multiprocessing.sharedctypes.RawArray = self.sharedctypes_RawArray
        multiprocessing.sharedctypes.Value = self.sharedctypes_Value
        multiprocessing.sharedctypes.RawValue = self.sharedctypes_RawValue
        multiprocessing.sharedctypes.copy = self.sharedctypes_copy
        multiprocessing.sharedctypes.synchronized = self.sharedctypes_synchronized

        multiprocessing.reduction.DupFd = self.reduction_DupFd
        multiprocessing.reduction.recvfds = self.reduction_sendfds
        multiprocessing.reduction.sendfds = self.reduction_sendfds

        multiprocessing.util.get_logger = self.util_get_logger
        multiprocessing.util.log_to_stderr = self.util_log_to_stderr

        multiprocessing.heap.Arena = self.heap_Arena


# This object can be imported by other modules to obtain the original
# Multiprocessing classes.
original_multiprocessing = Monkeypatcher()


class AugmentedDefaultContext(multiprocessing.context.DefaultContext):
    """This class inserts the Dragon start method 'dragon' in Multiprocessing
    and enables context switching.
    """

    def __init__(self, context, _actual_context):
        super().__init__(context)
        self._actual_context = _actual_context

    def get_all_start_methods(self):
        return [dragon.mpbridge.context.DragonContext._name] + super().get_all_start_methods()

    def set_start_method(self, method: str, *args, **kwargs) -> None:
        """Dragons replacement monkeypatches the class as well.

        :param method: name of the start method
        :type method: str
        """
        if method == dragon.mpbridge.process.DragonPopen.method:
            original_multiprocessing.switch_out(multiprocessing.context._concrete_contexts[method])
        else:
            original_multiprocessing.switch_in()

        super().set_start_method(method, *args, **kwargs)


def patch_multiprocessing():
    """Add Dragon to the list of Multiprocessing start methods.

    This function is called when a Python program is started using the Launcher, i.e.
    Dragon is invoked with the `dragon` command.

    1. Insert Dragon's MPBridge context into the list of Multiprocessing contexts.
    2. Replace Multiprocessings default context with our own version to swap
       Dragon objects in and out when the start methods is changed.
    3. Replace the complete exported API (`multiprocessing.X`) with Dragon versions.
    4. Replace `multiprocessing.spawns` start method function with Dragon versions.
    5. Switch out the class hierarchy so the resultion of the inheritance tree can find
       Dragon's objects before the entrypoint is reached.
    """

    import multiprocessing.connection
    import multiprocessing.spawn

    # add Dragon context to multiprocessing concrete contexts
    multiprocessing.context._concrete_contexts[
        dragon.mpbridge.process.DragonPopen.method
    ] = dragon.mpbridge.context.DragonContext()

    # make our Augmented DefaultContext the new default context
    if not isinstance(multiprocessing.context._default_context, AugmentedDefaultContext):
        multiprocessing.context._default_context = AugmentedDefaultContext(
            multiprocessing.context._default_context._default_context,
            # Preserve whatever the normal system default context is.
            multiprocessing.context._default_context._actual_context,
            # If _actual_context already set, must preserve it here.
        )

    # as it dynamically built out its own module namespace to begin with.

    # this collects the Multiprocessing public API
    _all_ = (x for x in dir(multiprocessing.context._default_context) if not x.startswith("_"))

    # this inserts most of the AugmentedDefaultContext public API into the multiprocessing public API
    mp_context_api_dict = {(name, getattr(multiprocessing.context._default_context, name)) for name in _all_}
    multiprocessing.__dict__.update(mp_context_api_dict)

    # Necessary but not alone sufficient for child processes to get/set the start method correctly;
    # must also preserve any already set context in AugmentedDefaultContext instantiation above.
    multiprocessing.spawn.get_start_method = multiprocessing.context._default_context.get_start_method
    multiprocessing.spawn.set_start_method = multiprocessing.context._default_context.set_start_method

    # Switch out the original modules with the new ones
    # This replaces Multiprocessing classes even if the user has inherited from them !!
    # I.e. we are modifying the class hierarchy. This not for the faint hearted.
    original_multiprocessing.switch_out(multiprocessing.context._concrete_contexts["dragon"])
