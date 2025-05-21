"""Dragon's replacement classes for Multiprocessing Process."""

import io
import logging
import os
import pickle
import multiprocessing
import multiprocessing.context
import multiprocessing.popen_spawn_posix
import multiprocessing.process
import multiprocessing.spawn
import multiprocessing.queues
import multiprocessing.connection
from multiprocessing.util import sub_debug

from ..globalservices.process import query, join, multi_join, create_with_argdata, kill, ProcessError
from ..infrastructure.parameters import this_process

LOG = logging.getLogger(__name__)


class PUID(int):
    """The purpose of this class is to use where such things like a fd (sentinel) and PID are found"""

    def thread_wait(self, timeout, done_ev, ready):
        """utility function for wait implementation

        :param timeout: _description_
        :type timeout: _type_
        :param done_ev: _description_
        :type done_ev: _type_
        :param ready: _description_
        :type ready: _type_
        """
        if not done_ev.is_set():
            done_res = join(int(self), timeout)
            if not done_ev.is_set():
                done_ev.set()
                if done_res is not None:
                    ready.append(self)


class DragonPopen(multiprocessing.popen_spawn_posix.Popen):
    """Dragon's class to spawn/open a new process."""

    method = "dragon"

    def _launch(self, process_obj):
        LOG.debug("launching")
        prep_data = multiprocessing.spawn.get_preparation_data(process_obj._name)
        fp = io.BytesIO()
        multiprocessing.context.set_spawning_popen(self)
        try:
            multiprocessing.context.reduction.dump(prep_data, fp)

            process_obj._p_p_uid = PUID(this_process.my_puid)

            multiprocessing.context.reduction.dump(process_obj, fp)
            sub_debug(f"{self} finished reduction.dump of process_obj self._fds={self._fds}")
        finally:
            multiprocessing.context.set_spawning_popen(None)

        # keep all this the same, and deliver fp as a BytesIO object for now
        cmd = multiprocessing.spawn.get_command_line()

        position = cmd.index("-c") + 1
        cmd[position] = "from dragon.mpbridge import dragon_spawn_main; dragon_spawn_main()"

        process_obj.proc_desc = create_with_argdata(
            exe=cmd[0], run_dir="", args=cmd[1:], env={}, argdata=pickle.dumps(fp, protocol=5)
        )

        self.p_uid = PUID(process_obj.proc_desc.p_uid)
        self.pid = self.p_uid
        self.sentinel = self.p_uid
        self.proc_desc = process_obj.proc_desc
        LOG.debug(f"launched {self.p_uid} as pid {self.pid}")

    def poll(self, flag=os.WNOHANG):
        if self.returncode is None:
            if flag & os.WNOHANG:
                current_desc = query(self.p_uid)
                if current_desc.state == current_desc.State.DEAD:
                    self.returncode = current_desc.ecode
            else:
                self.returncode = join(self.p_uid)

        return self.returncode

    def wait(self, timeout=None):
        if self.returncode is None:
            if timeout is None:
                return self.poll(flag=0)
            else:
                self.returncode = join(self.p_uid, timeout=timeout)

        return self.returncode

    def _send_signal(self, sig):
        if self.returncode is None:
            try:
                kill(self.p_uid, sig)
            except ProcessError:
                pass


class DragonProcess(multiprocessing.process.BaseProcess):
    """Create and manage a Python process, with a given target function, on a node available to the runtime"""

    _start_method = DragonPopen.method

    @staticmethod
    def _Popen(process_obj):
        return DragonPopen(process_obj)

    @property
    def ident(self) -> int:
        """Property method uniquely identifying the process.

        :return: The p_uid of this process, None if it has not been started.
        :rtype: int
        """
        if self._popen:
            return self._popen.p_uid
        else:
            return None

    pid = ident

    def _cleanup():
        if not multiprocessing.process._children:
            return
        # Check for processes which have finished
        p_uids = [int(p._popen.p_uid) for p in multiprocessing.process._children]
        dead, _ = multi_join(p_uids, timeout=0)
        if dead is None:  # All child processes are still alive
            return
        # Convert (p_uid, status) list of dead child processes to dict for convenience
        dead = dict(dead)
        # Remove dead processes from _children set
        for p in list(multiprocessing.process._children):
            if int(p._popen.p_uid) in dead:
                multiprocessing.process._children.discard(p)


class DragonPopenStub:
    """This Popen class replaces the actual DragonPopen in process objects returned
    to a child process by e.g. `mp.current_process()`. The parent will see the correct
    DragonPopen class, as they have actually started the child.  This class only contains
    what is necessary to make the process object work for the child with Multiprocessings
    internal methods that we have not modified.
    """

    def __init__(self, p_uid):
        self.p_uid = p_uid
        self.pid = p_uid
        self.returncode = None

    def poll(self, flag=os.WNOHANG):
        """So we can show the correct exitcode when printing a process
        object returned by mp.current_process()
        """
        if self.returncode is None:
            if flag & os.WNOHANG:
                current_desc = query(self.p_uid)
                if current_desc.state == current_desc.State.DEAD:
                    self.returncode = current_desc.ecode
            else:
                self.returncode = join(self.p_uid)

        return self.returncode
