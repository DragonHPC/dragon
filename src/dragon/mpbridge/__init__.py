"""The Dragon MPBridge component maps the Python Multiprocessing API onto Dragon
Native components.
"""

import io
import os
import pickle
import sys

import multiprocessing
import multiprocessing.spawn
from multiprocessing.util import debug
from multiprocessing.spawn import prepare
from multiprocessing.process import current_process

from dragon.infrastructure.parameters import this_process

from .connection import ConnGlue
from .process import DragonProcess, PUID, DragonPopen, DragonPopenStub
from .context import DragonContext
from .queues import DragonQueue, DragonSimpleQueue, DragonJoinableQueue
from .pool import DragonPool
from .synchronize import DragonSemaphore, DragonBoundedSemaphore, DragonLock, DragonRLock, DragonBarrier
from .reduction import dragon_DupFd, dragon_recvfds, dragon_sendfds
from .sharedctypes import DragonArray, DragonRawArray, DragonValue, DragonRawValue


def dragon_spawn_main():
    """Main entrypoint for running a Multiprocessing program backed by Dragon.
    I.e. this is the function first executed when a new Dragon process is
    started through a Multiprocessing call to `process.start()`.
    See `dragon.mpbridge.process.Popen._launch`.
    """
    assert multiprocessing.spawn.is_forking(sys.argv), "Not forking"
    if sys.platform == "win32":
        raise NotImplementedError("win32 unsupported by dragon")
    exitcode = _dragon_main()
    sys.exit(exitcode)


def _dragon_main():
    """This function gets the payload for the new process from the parent,
    constructs the new process object and runs it.
    """

    debug(f"_dragon_main starting os.getpid()={os.getpid()}")

    current_process()._inheriting = True

    try:
        import dragon.globalservices.api_setup as dga

        dga.connect_to_infrastructure()

        # Get argdata
        from_parent = pickle.loads(dga._ARG_PAYLOAD)
        assert isinstance(from_parent, io.BytesIO)
        from_parent.seek(0)

        preparation_data = multiprocessing.context.reduction.pickle.load(from_parent)
        prepare(preparation_data)

        # get process objects
        process_obj = multiprocessing.context.reduction.pickle.load(from_parent)

        # force it to be the right class & install fake _popen stub
        # object to fool some methods inside...
        process_obj._popen = DragonPopenStub(PUID(this_process.my_puid))

    except Exception as e:
        print(f"Starting Dragon process {this_process.my_puid} with pid {os.getpid()} failed: \n{e}", flush=True)
        raise e
    finally:
        del multiprocessing.process.current_process()._inheriting

    return process_obj._bootstrap(process_obj._p_p_uid)  # run process
