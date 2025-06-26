"""The Dragon native process object provides process management across one or
multiple distributed systems. `ProcessTemplate` can hold a blueprint for a process
that can be used to generate many similar processes.
"""

import os
import sys
import logging
import signal
import cloudpickle
import threading
from shutil import which
import traceback

from ..globalservices.process import (
    ProcessError,
    create as process_create,
    query as process_query,
    join as process_join,
    kill as process_kill,
    create_with_argdata as process_create_with_argdata,
)

from ..infrastructure.parameters import this_process
from ..infrastructure.facts import ARG_IMMEDIATE_LIMIT
from ..infrastructure.process_desc import ProcessOptions
from ..infrastructure.messages import PIPE as MSG_PIPE, STDOUT as MSG_STDOUT, DEVNULL as MSG_DEVNULL
from ..infrastructure.policy import Policy
from ..globalservices.policy_eval import PolicyEvaluator
from ..utils import b64decode, b64encode

LOG = logging.getLogger(__file__)


class Popen:
    PIPE = MSG_PIPE
    STDOUT = MSG_STDOUT
    DEVNULL = MSG_DEVNULL

    def __init__(
        self,
        args,
        executable=None,
        stdin=None,
        stdout=None,
        stderr=None,
        preexec_fn=None,
        shell=False,
        cwd=None,
        env=None,
        group=None,
        user=None,
        proc_name=None,
        umask=-1,
        pipesize=-1,
    ):
        """
        Emulate the subprocess.Popen interface using Dragon infrastructure.

        The DragonPopen constructor mirrors the interface of the subprocess.Popen
        class. Not every field is used, but is provided for signature
        compatability. Unsupported options may be ignored. See subprocess.Popen
        description for a description of arguments. Here are notable differences.

        When PIPE is specified, stdout and stderr will be a Connection. When STDOUT
        is specified for stderr, they will both be set to the same Connection object.
        If DEVNULL is specified for stdout or stderr, then the output is thrown away.

        When bufpool is provided it will be used for any allocations of connections for
        this process.

        :param args: _description_
        :type args: _type_
        :param executable: _description_, defaults to None
        :type executable: _type_, optional
        :param stdin: _description_, defaults to None
        :type stdin: _type_, optional
        :param stdout: _description_, defaults to None
        :type stdout: _type_, optional
        :param stderr: _description_, defaults to None
        :type stderr: _type_, optional
        :param preexec_fn: _description_, defaults to None
        :type preexec_fn: _type_, optional
        :param shell: _description_, defaults to False
        :type shell: bool, optional
        :param cwd: _description_, defaults to None
        :type cwd: _type_, optional
        :param env: _description_, defaults to None
        :type env: _type_, optional
        :param group: _description_, defaults to None
        :type group: _type_, optional
        :param user: _description_, defaults to None
        :type user: _type_, optional
        :param proc_name: _description_, defaults to None
        :type proc_name: _type_, optional
        :param umask: _description_, defaults to -1
        :type umask: int, optional
        :param pipesize: _description_, defaults to -1
        :type pipesize: int, optional
        :raises ex: _description_
        """

        self._args = args
        self._executable = executable
        self._stdin_requested = stdin
        self._stdout_requested = stdout
        self._stderr_requested = stderr
        self._preexec_fn = preexec_fn
        self._shell = shell
        self._cwd = cwd
        self._env = env
        self._group = group
        self._user = user
        self._proc_name = proc_name
        self._umask = umask
        self._pipesize = pipesize
        self._puid = None
        self._pid = None
        self._theproc = None
        self._returncode = None

        if cwd is None or cwd == ".":
            cwd = os.getcwd()

        if not os.path.isfile(executable):
            executable = cwd + "/" + executable

        if executable.strip().endswith(".py"):
            args = [executable] + args
            executable = sys.executable

        options = ProcessOptions(make_inf_channels=True)

        # Create the manager owner process
        self._theproc = process_create(
            exe=executable,
            run_dir=cwd,
            args=args,
            env=env,
            options=options,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            group=group,
            user=user,
            umask=umask,
            pipesize=pipesize,
            user_name=proc_name,
        )

        self._stdin = self._theproc.stdin_conn
        self._stdout = self._theproc.stdout_conn
        self._stderr = self._theproc.stderr_conn

    def __del__(self):
        try:
            if self._stdin is not None:
                self._stdin.close()
        except:
            pass

        try:
            if self._stdout is not None:
                self._stdout.close()
        except:
            pass

        try:
            if self._stderr is not None:
                self._stderr.close()
        except:
            pass

    def poll(self):
        """Check if process has terminated"""
        raise NotImplementedError

    def wait(self, timeout=None):
        """wait for process to terminate"""
        self._returncode = process_join(self.puid, timeout=timeout)

    def communicate(self, input=None, timeout=None):
        """receive all output and transmit all input and wait for process to terminate."""
        raise NotImplementedError

    def send_signal(self, signal):
        """just like it sounds"""
        raise NotImplementedError

    def terminate(self):
        """Send SigTERM"""
        raise NotImplementedError

    def kill(self):
        """Send SigKILL"""
        raise NotImplementedError

    @property
    def args(self):
        """actual args passed to process"""
        raise NotImplementedError

    @property
    def stdin(self):
        """the stdin connection when PIPE was specified. None otherwise."""
        if self._theproc:
            return self._stdin

    @property
    def stdout(self):
        """The stdout connection when PIPE was specified. None otherwise."""
        if self._theproc:
            return self._stdout

    @property
    def stderr(self):
        """The stderr connection when PIPE was specified. None otherwise."""
        if self._theproc:
            return self._stderr

    @property
    def puid(self):
        """Dragon process puid. Globally unique"""
        if self._theproc:
            return self._theproc.p_uid

    @property
    def pid(self):
        """OS process pid. Node unique"""
        raise NotImplementedError

    @property
    def returncode(self):
        """When it has terminated, exit code. None otherwise."""
        return self._returncode


class ProcessTemplate:
    """This class provides a template for a Dragon process."""

    PIPE = MSG_PIPE
    STDOUT = MSG_STDOUT
    DEVNULL = MSG_DEVNULL

    def __init__(
        self,
        target: str or callable,
        args: tuple = None,
        kwargs: dict = None,
        cwd: str = ".",
        env: dict = None,
        stdin: int = None,
        stdout: int = None,
        stderr: int = None,
        policy: Policy = None,
        options: ProcessOptions = None,
    ):
        """Generic Dragon process template object defining a process based on a
        binary executable or a Python callable.

        If the target is a callable, define the Python process executing the
        callable.
        If the target is a string, assume it's the filename of a binary to be
        started.  If the target file is not found, the object tries to resolve
        the command via the PATH environment variable. If that fails, it assumes
        the file is in the current working directory.

        This template cannot be started, but it can be used to create many
        similar processes like this:

        .. code-block:: python

            t = ProcessTemplate(myfunc)
            p1 = Process.from_template(t, ident="NewProcess1")
            p2 = Process.from_template(t, ident="NewProcess2")

        The template stores the absolute path of the target if the target is an executable.
        If the target is a Python function, the template stores the Python executable in
        its `target` attribute, the command line parameters in its `args` attribute and
        the Python data in its `argdata` attribute. To obtain the original attributes in
        Python use

        .. code-block:: python

            target, args, kwargs = p.get_original_python_parameters()

        :param target: The binary or Python callable to execute.
        :type target: str or callable
        :param args: arguments to pass to the binary or callable, defaults to None
        :type args: tuple, optional
        :param kwargs: keyword arguments to pass to the Python callable, defaults to None
        :type kwargs: tuple, optional
        :param cwd: current working directory, defaults to "."
        :type cwd: str, optional
        :param env: environment variables to pass to the process environment, defaults to None
        :type env: dict, optional
        :param stdin: Stdin file handling. Valid values are PIPE and None.
        :type stdin: int, optional
        :param stdout: Stdout file handling. Valid values are PIPE, STDOUT and None.
        :type stdout: int, optional
        :param stderr: Stderr file handling. Valid values are PIPE, STDOUT and None.
        :type stderr: int, optional
        :param policy: determines the placement and resources of the process
        :type policy: dragon.infrastructure.policy.Policy, optional
        :param options: process options, such as allowing the process to connect to the infrastructure
        :type options: dragon.infrastructure.process_desc.ProcessOptions, optional
        """

        self.is_python = callable(target)

        # store only the modified targets. We want to be able to pickle the
        # ProcessTemplate. The user has to use a method to get the original arguments.
        if self.is_python:
            self.target, self.args, self.argdata = self._get_python_process_parameters(target, args, kwargs)
        else:  # binary
            self.args = args
            self.target = self._find_target(target, cwd)
            self.argdata = None

        if cwd is None or cwd == ".":
            cwd = os.getcwd()

        self.cwd = cwd
        self.env = env

        self.stdout = stdout
        self.stderr = stderr
        self.stdin = stdin
        self.options = options

        self.policy = policy
        # can't grab the global policy here because of the following.
        # default behavior will be that the global policy is grabbed when
        # the process is started. Thus, template processes hold a policy
        # that gets merged in later to the hierarchy of policies.
        # with Policy(placement=x)
        #     with Policy(placement=y)
        #       local_policy = Policy(cpu_affinity=)
        #       temp_proc = ProcessTemplate(..., policy=local_policy)
        #
        #    pg = ProcessGroup(policy = Policy(placement=z))
        #    pg.add_procs(10, temp_proc) <- If I merge the policy into template policy, I don't know if placement=y is from the local policy or if placement=y is from global policy so I can't inject the group policy into the hierarchy correctly.

    @property
    def sdesc(self):
        return self.get_sdict()

    def get_sdict(self):

        argdata = None
        if self.argdata is not None:
            argdata = b64encode(self.argdata)
        options = None
        if self.options is not None:
            options = self.options.get_sdict()
        policy = None
        if self.policy is not None:
            policy = b64encode(cloudpickle.dumps(self.policy))

        rv = {
            "is_python": self.is_python,
            "target": self.target,
            "args": self.args,
            "argdata": argdata,
            "env": self.env,
            "cwd": self.cwd,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "stdin": self.stdin,
            "options": options,
            "policy": policy,
        }

        return rv

    @classmethod
    def from_sdict(cls, sdict):

        argdata = None
        if sdict["argdata"] is not None:
            argdata = b64decode(sdict["argdata"])
        options = ProcessOptions()
        if sdict["options"] is not None:
            options = options.from_sdict(sdict["options"])
        policy = None
        if sdict["policy"] is not None:
            policy = cloudpickle.loads(b64decode(sdict["policy"]))

        tmpl = cls(target=sys.executable)
        tmpl.is_python = sdict["is_python"]
        tmpl.target = sdict["target"]
        tmpl.args = sdict["args"]
        tmpl.argdata = argdata
        tmpl.env = sdict["env"]
        tmpl.cwd = sdict["cwd"]
        tmpl.stdout = sdict["stdout"]
        tmpl.stderr = sdict["stderr"]
        tmpl.stdin = sdict["stdin"]
        tmpl.options = options
        tmpl.policy = policy
        return tmpl

    @staticmethod
    def _find_target(target, cwd) -> str:

        new_target = target

        if not os.path.isfile(target):
            if which(target):
                new_target = which(target)
            else:
                new_target = os.path.join(cwd, target)

                if not os.path.isfile(new_target):
                    raise AttributeError(
                        f"Cannot find executable {target} directly, in PATH or in working directory {cwd}"
                    )

        return new_target

    def _get_python_process_parameters(self, target, args, kwargs) -> tuple:

        new_target = sys.executable
        new_args = [
            "-c",
            "from dragon.native.process import _dragon_native_python_process_main; _dragon_native_python_process_main()",
        ]

        argdata = cloudpickle.dumps((target, args or (), kwargs or {}))

        return new_target, new_args, argdata

    def get_original_python_parameters(self) -> tuple[callable, tuple, dict]:
        """Return the original parameters of a Python process from the object.

        :return: function, args and kwargs of the templated process
        :rtype: tuple[callable, tuple, dict]
        """
        return cloudpickle.loads(self.argdata)

    def start(self) -> None:
        raise NotImplementedError(f"You need to create a Process object from this template to start it.")


class Process(ProcessTemplate):
    """This object describes a process managed by the Dragon runtime."""

    def __new__(cls, *args, **kwargs):
        """Prevent users from subclassing `Process`.
        :raises NotImplementedError: If a subclass of `Process` is being instantiated
        :return: A Process object
        :rtype: Process
        """

        if cls != Process:
            raise NotImplementedError("Subclassing process not supported by the Dragon runtime.")

        return super(Process, cls).__new__(cls)

    def __init__(
        self,
        target: str or callable,
        args: tuple = None,
        kwargs: dict = None,
        cwd: str = ".",
        env: dict = None,
        ident: str or int = None,
        _pmi_enabled: bool = False,
        stdin: int = None,
        stdout: int = None,
        stderr: int = None,
        policy: Policy = None,
        options: ProcessOptions = None,
    ):
        """Generic Dragon process object executing a binary executable or a
        Python callable.

        Processes are named and can be looked up by specifying the `ident`
        keyword: `Process(None, ident="Apple")`

        If the target is a callable, start a new Python process executing the
        callable.  The class uses cloudpickle to transfer the callable to the
        child process. See the cloudpickle documentation for possible
        limitations.

        If the target is a string, assume it's the filename of a binary to be
        started.  If the target file is not found, the object tries to resolve
        the command via the PATH environment variable. If that fails, it assumes
        the file is in the current working directory.

        The Process class should not be subclassed, because the Dragon runtime
        only holds on to attributes of the Process super class, breaking the name
        lookup.

        :param target: The binary or Python callable to execute.
        :type target: str or callable
        :param args: arguments to pass to the binary or callable, defaults to None
        :type args: tuple, optional
        :param kwargs: keyword arguments to pass to the Python callable, defaults to None
        :type kwargs: tuple, optional
        :param cwd: current working directory, defaults to "."
        :type cwd: str, optional
        :param env: environment variables to pass to the process environment, defaults to None
        :type env: dict, optional
        :param ident: unique id or name of this process
        :type ident: int or str
        :param stdin: Stdin file handling. Valid values are PIPE and None.
        :type stdin: int, optional
        :param stdout: Stdout file handling. Valid values are PIPE, STDOUT and None.
        :type stdout: int, optional
        :param stderr: Stderr file handling. Valid values are PIPE, STDOUT and None.
        :type stderr: int, optional
        :param policy: determines the placement and resources of the process
        :type policy: dragon.infrastructure.policy.Policy
        :param options: process options, such as allowing the process to connect to the infrastructure
        :type options: dragon.infrastructure.process_desc.ProcessOptions, optional
        """

        self.started = False
        self.ident = ident
        self._pmi_enabled = _pmi_enabled

        # strip the name/uid from the parameters, as ProcessTemplate cannot have it by definition.
        if ident:
            try:
                self._update_descriptor(ident=ident)
            except ProcessError:
                pass
            else:
                if target:
                    LOG.warning(f"Returning named process from infrastructure with valid target arg in call.")
                return  # we're done

        return super().__init__(target, args, kwargs, cwd, env, stdin, stdout, stderr, policy, options)

    @classmethod
    def from_template(cls, template: ProcessTemplate, ident: str = None, _pmi_enabled: bool = False) -> object:
        """A classmethod that creates a new process object from a template.

        :param template: the template to base the process on
        :type template: ProcessTemplate
        :param ident: intended name of the process, defaults to None
        :type ident: str, optional
        :return: The new process object
        :rtype: dragon.native.Process
        """
        # is there a way to write this less explicitly ?

        kwargs = None
        if template.is_python:
            target, args, kwargs = template.get_original_python_parameters()
        else:
            target = template.target
            args = template.args

        if ident:  # subtle detail, a process from a template _has_ to be new !
            try:
                _ = process_query(ident)
            except ProcessError:
                pass
            else:
                raise ProcessError(f"A process '{ident}' already exists within the Dragon runtime.")

        return cls(
            target,
            args,
            kwargs,
            template.cwd,
            template.env,
            ident=ident,
            _pmi_enabled=_pmi_enabled,
            stdin=template.stdin,
            stdout=template.stdout,
            stderr=template.stderr,
            policy=template.policy,
            options=template.options,
        )

    def start(self) -> None:
        """Start the process represented by the underlying process object."""

        if self.started:
            raise RuntimeError(f"This Process has already been started with puid {self.puid}")

        if self.policy is not None:
            # merge global policy and processes' policy
            self.policy = Policy.merge(Policy.global_policy(), self.policy)
        else:
            self.policy = Policy.global_policy()

        if self.is_python:

            # Python functions must be able to talk with the infrastructure
            options = self.options
            if options is None:
                options = ProcessOptions(make_inf_channels=True)
            else:
                options.make_inf_channels = True

            self._descr = process_create_with_argdata(
                exe=self.target,
                run_dir=self.cwd,
                args=self.args,
                argdata=self.argdata,
                env=self.env,
                options=options,
                user_name=self.ident,
                stdin=self.stdin,
                stdout=self.stdout,
                stderr=self.stderr,
                policy=self.policy,
            )
        else:  # binary

            self._descr = process_create(
                exe=self.target,
                run_dir=self.cwd,
                args=self.args,
                env=self.env,
                options=self.options,
                user_name=self.ident,
                stdin=self.stdin,
                stdout=self.stdout,
                stderr=self.stderr,
                policy=self.policy,
            )

        self.started = True

    @property
    def is_alive(self) -> bool:
        """Check if the process is still running

        :return: True if the process is running, False otherwise
        :rtype: bool
        """
        try:
            self._update_descriptor()
        except AttributeError:
            return False
        else:
            return self._descr.state == self._descr.State.ACTIVE

    def join(self, timeout: float = None) -> int:
        """Wait for the process to finish.

        :param timeout: timeout in seconds, defaults to None
        :type timeout: float, optional
        :return: exit code of the process, None if timeout occurs
        :rtype: int
        :raises: ProcessError
        """
        return process_join(self._descr.p_uid, timeout=timeout)

    def terminate(self) -> None:
        """Send SIGTERM signal to the process, terminating it.

        :return: None
        :rtype: NoneType
        """
        process_kill(self._descr.p_uid, sig=signal.SIGTERM)

    def kill(self) -> None:
        """Send SIGKILL signal to the process, killing it.

        :return: None
        :rtype: NoneType
        """
        process_kill(self._descr.p_uid, sig=signal.SIGKILL)

    def send_signal(self, sig) -> None:
        """Send sig to the process, killing it.

        :param sig: [description]
        :type sig: signal.Signals
        :return: None
        :rtype: NoneType
        """
        process_kill(self._descr.p_uid, sig=sig)

    @property
    def name(self) -> str:
        return self._descr.name

    @property
    def puid(self) -> int:
        """Process puid. Globally unique"""
        return self._descr.p_uid

    @property
    def node(self) -> int:
        """Return a number for the host node the process is running on assigned by Local Services.

        :return: nonnegative integer with values in [0, #nodes-1]
        :rtype: int
        """
        return self._descr.node

    @property
    def h_uid(self) -> int:
        """Return the unique host id of the node the process is running on.

        :return: h_uid of host node.
        :rtype: int
        """
        return self._descr.h_uid

    @property
    def returncode(self) -> int:
        """When the process has terminated, return exit code. None otherwise."""
        self._update_descriptor()
        return self._descr.ecode

    def children(self) -> list[object]:
        """Find all active children of the current process.

        :returns: a list of p_uids of active children of the current process
        :return type: list(int)
        """
        self._update_descriptor()
        return [Process(None, ident=puid) for puid in self._descr.live_children]

    def parent(self) -> object:
        """Get the process object of the parent process.
        :returns: The puid or the parent of the current process
        :return type: int
        """
        return Process(None, ident=self._descr.p_p_uid)

    def _update_descriptor(self, ident=None):
        self._descr = process_query(ident or self._descr.p_uid)

    @property
    def stdin_conn(self):
        return self._descr.stdin_conn

    @property
    def stdout_conn(self):
        return self._descr.stdout_conn

    @property
    def stderr_conn(self):
        return self._descr.stderr_conn


def current() -> Process:
    """Get the PythonProcess object of the current process.

    :returns: The current puid
    :return type: int
    """

    return Process(None, ident=this_process.my_puid)


def _dragon_native_python_process_main():
    """This function is called by the Dragon runtime using the Python executable
    when a new Python process is started.

    It returns an exit code of 0, if the Python function exited without raising
    an exception or called `sys.exit()` with an error code of `None` or `0`.  If
    `sys.exit()` is called with a non-zero error code, it returns that error
    code.  Otherwise it returns an exit code of 1.
    """

    from dragon.globalservices.api_setup import connect_to_infrastructure

    connect_to_infrastructure()

    myp = current()

    LOG.debug(f"New Dragon Python Process puid={myp.puid}, pid={os.getpid()}, node={myp.node}")

    try:
        from dragon.globalservices.api_setup import _ARG_PAYLOAD  # import as late as possible

        target, args, kwargs = cloudpickle.loads(_ARG_PAYLOAD)  # unpack argdata from GS
    except Exception as e:
        LOG.error(f"Could not unpickle payload data:\n{e}")
        sys.exit(1)

    try:
        if threading._HAVE_THREAD_NATIVE_ID:
            threading.main_thread()._set_native_id()

        target(*args, **kwargs)  # call the user-provided function
        ecode = 0
    except Exception as ex:
        print(
            f"*** Exception Occurred in User-Provided Code ***\n{traceback.format_exc()}", file=sys.stderr, flush=True
        )
        ecode = 1
    except SystemExit as e:  # catch sys.exit
        if e.code is None:
            ecode = 0
        elif isinstance(e.code, int):
            ecode = e.code
        else:
            ecode = 1
        if ecode != 0:
            print(
                f"*** System Exit({ecode}) in User-Provided Code ***\n{traceback.format_exc()}",
                file=sys.stderr,
                flush=True,
            )
    finally:
        threading._shutdown()
        try:
            sys.stdout.flush()
        except (AttributeError, ValueError):
            pass
        try:
            sys.stderr.flush()
        except (AttributeError, ValueError):
            pass
    # print("Exiting process main thing")
    sys.exit(ecode)
