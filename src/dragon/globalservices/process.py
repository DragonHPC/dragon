"""Global Services API for the Process primary resource component."""

import signal
import logging
import enum
import sys
import threading
import atexit
from distutils.util import strtobool
from typing import List, Tuple, Dict

from .. import channels as dch

from ..globalservices import api_setup as das
from ..globalservices import channel as dgchan

from ..infrastructure import messages as dmsg
from ..infrastructure import parameters as dparm
from ..infrastructure.parameters import this_process

from ..infrastructure import process_desc as pdesc
from ..infrastructure import facts as dfacts
from ..infrastructure import connection as dconn
from ..utils import B64
from ..infrastructure.policy import Policy
import os
import json

log = logging.getLogger("process_api")


@enum.unique
class StreamDestination(enum.Enum):
    STDIN = 0
    STDOUT = 1
    STDERR = 2


_capture_stdout_conn = None
_capture_stderr_conn = None
_capture_stdout_chan = None
_capture_stderr_chan = None

DRAGON_CAPTURE_MP_CHILD_OUTPUT = "DRAGON_CAPTURE_MP_CHILD_OUTPUT"
DRAGON_STOP_CAPTURING_MP_CHILD_OUTPUT = "DRAGON_STOP_CAPTURING_MP_CHILD_OUTPUT"

_capture_shutting_down = False


# This is for user processes to use when they wish to capture the
# output of child multiprocessing processes.
def start_capturing_child_mp_output():
    os.environ[DRAGON_CAPTURE_MP_CHILD_OUTPUT] = "True"

def cleanup():
    global _capture_shutting_down

    try:
        _capture_shutting_down = True
        stop_capturing_child_mp_output()
        destroy_capture_connections()
    except:
        pass

def mk_capture_threads():
    global _capture_stdout_conn, _capture_stderr_conn, _capture_shutting_down

    def forward(strm_conn: dconn.Connection, strm_dest: StreamDestination):

        file_dest = sys.stdout if strm_dest == StreamDestination.STDOUT else sys.stderr

        try:
            while not _capture_shutting_down:
                data = strm_conn.recv()
                if data == DRAGON_STOP_CAPTURING_MP_CHILD_OUTPUT:
                    return

                print(data, file=file_dest, flush=True, end="")
        except EOFError:
            pass
        except Exception as ex:
            log.debug(f"While forwarding output to {strm_dest} an unexpected error occurred:{repr(ex)}")


    stdout_monitor = threading.Thread(
        name="stdout monitor", target=forward, args=(_capture_stdout_conn, StreamDestination.STDOUT)
    )
    stdout_monitor.start()

    stderr_monitor = threading.Thread(
        name="stderr monitor", target=forward, args=(_capture_stderr_conn, StreamDestination.STDERR)
    )
    stderr_monitor.start()

    atexit.register(cleanup)


def stop_capturing_child_mp_output():
    global _capture_stdout_chan, _capture_stderr_chan

    stdout = dconn.Connection(
        outbound_initializer=_capture_stdout_chan,
        options=dconn.ConnectionOptions(min_block_size=512),
        policy=dparm.POLICY_INFRASTRUCTURE,
    )

    stderr = dconn.Connection(
        outbound_initializer=_capture_stderr_chan,
        options=dconn.ConnectionOptions(min_block_size=512),
        policy=dparm.POLICY_INFRASTRUCTURE,
    )

    stdout.send(DRAGON_STOP_CAPTURING_MP_CHILD_OUTPUT)
    stderr.send(DRAGON_STOP_CAPTURING_MP_CHILD_OUTPUT)


def destroy_capture_connections():
    global _capture_stdout_chan, _capture_stderr_chan

    if _capture_stdout_chan is not None:
        try:
            log.info("destroying the MP child capturing stdout channel")
            dgchan.destroy(_capture_stdout_chan.cuid)
            log.info("stdout channel destroy complete")
            _capture_stdout_chan = None
        except Exception:
            pass

    if _capture_stderr_chan is not None:
        try:
            log.info("destroying the MP child capturing stderr channel")
            dgchan.destroy(_capture_stderr_chan.cuid)
            log.info("stderr channel destroy complete")
            _capture_stderr_chan = None
        except Exception:
            pass


def _capture_child_output_setup():
    global _capture_stdout_conn, _capture_stderr_conn, _capture_stdout_chan, _capture_stderr_chan

    if _capture_stdout_conn is None:
        default_muid = dfacts.default_pool_muid_from_index(dparm.this_process.index)

        # ask GS to create a dragon channel
        stdout_descriptor = dgchan.create(default_muid)
        _capture_stdout_chan = dch.Channel.attach(stdout_descriptor.sdesc)
        os.environ[dfacts.STDOUT_DESC] = B64.bytes_to_str(stdout_descriptor.sdesc)
        _capture_stdout_conn = dconn.Connection(
            inbound_initializer=_capture_stdout_chan,
            options=dconn.ConnectionOptions(min_block_size=512),
            policy=dparm.POLICY_INFRASTRUCTURE,
        )

        stderr_descriptor = dgchan.create(default_muid)
        _capture_stderr_chan = dch.Channel.attach(stderr_descriptor.sdesc)
        os.environ[dfacts.STDERR_DESC] = B64.bytes_to_str(stderr_descriptor.sdesc)
        _capture_stderr_conn = dconn.Connection(
            inbound_initializer=_capture_stderr_chan,
            options=dconn.ConnectionOptions(min_block_size=512),
            policy=dparm.POLICY_INFRASTRUCTURE,
        )

PIPE = dmsg.PIPE
STDOUT = dmsg.STDOUT
DEVNULL = dmsg.DEVNULL


class ProcessError(Exception):
    pass


def _create_stdio_connections(the_desc):

    if the_desc.stdin_sdesc is not None:
        # If a stdin serialized descriptor was requested, then a channel
        # has been created and the initial refcnt was 2. This keeps it from
        # being destroyed until the user has a chance to write to it even
        # if the process exits. Then once we create a Connection, the refcnt
        # gets incremented to three, so we decrement the refcnt to 2 again,
        # one for when this connection is closed, and one for when the process
        # exits and local services closes its end of the connection.
        ch = dch.Channel.attach(B64.from_str(the_desc.stdin_sdesc).decode())
        conn = dconn.Connection(
            outbound_initializer=ch,
            options=dconn.ConnectionOptions(
                min_block_size=512, creation_policy=dconn.ConnectionOptions.CreationPolicy.PRE_CREATED
            ),
            policy=dparm.POLICY_INFRASTRUCTURE,
        )
        the_desc.stdin_conn = conn
        dgchan.release_refcnt(ch.cuid)

    if the_desc.stdout_sdesc is not None:
        # If a stdout serialized descriptor was requested, then a channel
        # has been created and the initial refcnt was 2 as was done for stdin
        # (please read the comment above). The analagous behaviour is coded
        # here.
        ch = dch.Channel.attach(B64.from_str(the_desc.stdout_sdesc).decode())
        conn = dconn.Connection(
            inbound_initializer=ch,
            options=dconn.ConnectionOptions(
                min_block_size=512, creation_policy=dconn.ConnectionOptions.CreationPolicy.PRE_CREATED
            ),
            policy=dparm.POLICY_INFRASTRUCTURE,
        )
        the_desc.stdout_conn = conn
        dgchan.release_refcnt(ch.cuid)

    if the_desc.stderr_sdesc is not None:
        # If a stderr serialized descriptor was requested, then a channel
        # has been created and the initial refcnt was 2 as was done for stdin
        # (please read the comment above). The analagous behaviour is coded
        # here.
        ch = dch.Channel.attach(B64.from_str(the_desc.stderr_sdesc).decode())
        conn = dconn.Connection(
            inbound_initializer=ch,
            options=dconn.ConnectionOptions(
                min_block_size=512, creation_policy=dconn.ConnectionOptions.CreationPolicy.PRE_CREATED
            ),
            policy=dparm.POLICY_INFRASTRUCTURE,
        )
        the_desc.stderr_conn = conn
        dgchan.release_refcnt(ch.cuid)

    return the_desc


def get_create_message(
    exe,
    run_dir,
    args,
    env,
    user_name="",
    options=None,
    stdin=None,
    stdout=None,
    stderr=None,
    group=None,
    user=None,
    umask=-1,
    pipesize=-1,
    pmi_required=False,
    policy=None,
):
    """Return a GSProcessCreate object.

    :param exe: executable to run
    :param run_dir: directory to run it in
    :param args: argv for the process
    :param env: environment for the new process, may be added to for infrastructure
    :param user_name: Requested user specified reference name
    :param options: ProcessOptions object, what options to apply to creation
    :param stdin: If `stdin=PIPE`, a Dragon Connection will be returned to enable sending data to the stdin of the child process.
    :param stdout: Capture stdout from the child process. If using PIPE, a Dragon Connection object will be returned which can be used to read from stdout. Using DEVNULL will cause the stdout data to be thrown away.
    :param stder: Capture stderr from the child process. If using PIPE, a Dragon Connection object will be returned which can be used to read from stderr. Using DEVNULL will cause the stdout data to be thrown away. Using STDOUT will cause the stderr data to be combined with the stdout data.
    :param group: Not used
    :param user: Not used
    :param umask: Not used
    :param pipesize: Set the channel capacity. Default = -1.
    :param pmi_required: This process is part of a Dragon managed MPI/PMI application group.
    :param policy: If a policy other than the global default is to be used for this process.
    :return: GSProcessCreate message object
    """

    # Like the OS, we should inherit the environment when creating a process.
    # Using dict below makes a copy.
    the_env = dict(os.environ)
    if env is not None:
        the_env.update(env)  # layer on the user supplied environment.

    thread_policy = Policy.thread_policy()
    if thread_policy:
        the_env[dfacts.DRAGON_POLICY_CONTEXT_ENV] = json.dumps(thread_policy.get_sdict())

    # Ensure that the executable is a string and not a bytes object
    try:
        exe = exe.decode()
    except (UnicodeDecodeError, AttributeError):
        pass

    log.debug("creating GSProcessCreate")
    return dmsg.GSProcessCreate(
        tag=das.next_tag(),
        p_uid=this_process.my_puid,
        r_c_uid=das.get_gs_ret_cuid(),
        exe=exe,
        args=args,
        env=the_env,
        rundir=run_dir,
        user_name=user_name,
        options=options,
        stdin=stdin,
        stdout=stdout,
        stderr=stderr,
        group=group,
        user=user,
        umask=umask,
        pipesize=pipesize,
        pmi_required=pmi_required,
        policy=policy,
    )


def get_create_message_with_argdata(
    exe,
    run_dir,
    args,
    env,
    argdata=None,
    user_name="",
    options=None,
    stdin=None,
    stdout=None,
    stderr=None,
    group=None,
    user=None,
    umask=-1,
    pipesize=-1,
    pmi_required=False,
    policy=None,
):
    """Return a GSProcessCreate object with starting args.

        This is an extension of the 'get_create_message' method that encapsulates our scheme for getting
        starting arguments to the new process.  This depends on how big the arguments
        turn out to be, and is tightly bound with logic in global services and in the api_setup module.

    :param exe: executable to run
    :param run_dir: directory to run it in
    :param args: argv for the process
    :param env: environment for the new process, may be added to for infrastructure
    :param argdata: bytes to deliver
    :param user_name: Requested user specified reference name
    :param options: ProcessOptions object, what options to apply to creation
    :param stdin: If `stdin=PIPE`, a Dragon Connection will be returned to enable sending data to the stdin of the child process.
    :param stdout: Capture stdout from the child process. If using PIPE, a Dragon Connection object will be returned which can be used to read from stdout. Using DEVNULL will cause the stdout data to be thrown away.
    :param stder: Capture stderr from the child process. If using PIPE, a Dragon Connection object will be returned which can be used to read from stderr. Using DEVNULL will cause the stdout data to be thrown away. Using STDOUT will cause the stderr data to be combined with the stdout data.
    :param group: Not used
    :param user: Not used
    :param umask: Not used
    :param pipesize: Set the channel capacity. Default = -1.
    :param pmi_required: This process is part of a Dragon managed MPI/PMI application group.
    :return: GSProcessCreate message object
    """

    # for the current argdata delivery mechanism (using the GS ret channel), we must make an infra channel
    # this will likely change
    if options is None:
        options = pdesc.ProcessOptions(make_inf_channels=True)
    else:
        options.make_inf_channels = True

    # Like the OS, we should inherit the environment when creating a process.
    # Using dict below makes a copy.
    the_env = dict(os.environ)
    if env is not None:
        the_env.update(env)  # layer on the user supplied environment.

    thread_policy = Policy.thread_policy()
    if thread_policy:
        the_env[dfacts.DRAGON_POLICY_CONTEXT_ENV] = str(thread_policy.get_sdict())

    # Ensure that the executable is a string and not a bytes object
    try:
        exe = exe.decode()
    except (UnicodeDecodeError, AttributeError):
        pass

    log.debug("creating GSProcessCreate")

    if argdata is None:
        # no arguments, so set the mode and start the normal way
        options.mode = pdesc.ArgMode.NONE
        options.argdata = None
        return dmsg.GSProcessCreate(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            exe=exe,
            args=args,
            env=the_env,
            rundir=run_dir,
            user_name=user_name,
            options=options,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            group=group,
            user=user,
            umask=umask,
            pipesize=pipesize,
            pmi_required=pmi_required,
            policy=policy,
        )

    if len(argdata) <= dfacts.ARG_IMMEDIATE_LIMIT:
        # deliver arguments in the argdata directly.
        options.mode = pdesc.ArgMode.PYTHON_IMMEDIATE

        # todo: make a setter for options.argdata?
        options.argdata = prepare_argdata_for_immediate(argdata)
        return dmsg.GSProcessCreate(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            exe=exe,
            args=args,
            env=the_env,
            rundir=run_dir,
            user_name=user_name,
            options=options,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            group=group,
            user=user,
            umask=umask,
            pipesize=pipesize,
            pmi_required=pmi_required,
            policy=policy,
        )
    else:
        raise NotImplementedError(
            f"Argument data larger than {dfacts.ARG_IMMEDIATE_LIMIT} bytes is not supported at the moment."
        )


def create(
    exe,
    run_dir,
    args,
    env,
    user_name="",
    options=None,
    soft=False,
    stdin=None,
    stdout=None,
    stderr=None,
    group=None,
    user=None,
    umask=-1,
    pipesize=-1,
    pmi_required=False,
    policy=None,
):
    """Asks Global Services to create a new process.

    :param exe: executable to run
    :param run_dir: directory to run it in
    :param args: argv for the process
    :param env: environment for the new process, may be added to for infrastructure
    :param user_name: Requested user specified reference name
    :param options: ProcessOptions object, what options to apply to creation
    :param soft: If process already exists with given name, do not create and return descriptor instead.
    :param stdin: If `stdin=PIPE`, a Dragon Connection will be returned to enable sending data to the stdin of the child process.
    :param stdout: Capture stdout from the child process. If using PIPE, a Dragon Connection object will be returned which can be used to read from stdout. Using DEVNULL will cause the stdout data to be thrown away.
    :param stder: Capture stderr from the child process. If using PIPE, a Dragon Connection object will be returned which can be used to read from stderr. Using DEVNULL will cause the stdout data to be thrown away. Using STDOUT will cause the stderr data to be combined with the stdout data.
    :param group: Not used
    :param user: Not used
    :param umask: Not used
    :param pipesize: Set the channel capacity. Default = -1.
    :param pmi_required: This process is part of a Dragon managed MPI/PMI application group.
    :param policy: If a policy other than the global default is to be used for this process.
    :return: ProcessDescriptor object
    """
    global _capture_stdout_conn, _capture_stderr_conn, _capture_stdout_chan, _capture_stderr_chan

    thread_policy = Policy.thread_policy()
    if all([policy, thread_policy]):
        # Since both policy and thread_policy are not None, we are likely within a
        # Policy context manager. In this case we need to merge the supplied policy
        # with the policy of the context manager.
        policy = Policy.merge(thread_policy, policy)
    elif policy is None:
        # If policy is None, then let's assign thread_policy to policy. thread_policy
        # may also be None, but that's OK.
        policy = thread_policy

    # When a new process is requested we want to check to see if we are supposed to
    # redirect output from new processes to the parent process (this process). If so, we
    # set that up here and then remove that indicator from the env vars. This
    # check and call is done here to delay the setup as long as possible. This is
    # especially needed for something like Jupyter notebooks which do an execve and
    # need this setup done after that occurs.
    if bool(strtobool(os.environ.get(DRAGON_CAPTURE_MP_CHILD_OUTPUT, "False"))):
        _capture_child_output_setup()
        mk_capture_threads()
        del os.environ[DRAGON_CAPTURE_MP_CHILD_OUTPUT]

    elif _capture_stdout_conn is not None:
        # This means we are capturing output already, but we are creating a new
        # dragon process. When we do this we shutdown the output capturing, but
        # immediately start it back up again. Why? This seems like it deserves an
        # explanation. Under a Jupyter notebook, the primary reason for capturing
        # output, shutting down the thread here and starting a new one makes the
        # output appear where it should in the notebook. It seems (a theory really,
        # but not completely verified) that a notebook evaluates each cell in its own
        # environment. So essentiallly we stop capturing output for the previous cell
        # and start capturing output again to get the output in the current cell. This
        # works except that this code is not called for every cell. It is only called when
        # a multiprocessing process is created. Which means that for things like mp.Pool,
        # the output appears in the cell where the pool was created, not where "map" was
        # called for instance. This is exactly what base multiprocessing does in notebooks
        # too, so this should be acceptable.
        stop_capturing_child_mp_output()
        mk_capture_threads()

    if options is None:
        options = {}

    if soft and not user_name:
        raise ProcessError("soft create requires a user supplied process name")

    log.debug("creating GSProcessCreate")
    req_msg = get_create_message(
        exe=exe,
        run_dir=run_dir,
        args=args,
        env=env,
        user_name=user_name,
        options=options,
        stdin=stdin,
        stdout=stdout,
        stderr=stderr,
        group=group,
        user=user,
        umask=umask,
        pipesize=pipesize,
        pmi_required=pmi_required,
        policy=policy,
    )

    reply_msg = das.gs_request(req_msg)
    log.debug("got GSProcessCreateResponse")

    assert isinstance(reply_msg, dmsg.GSProcessCreateResponse)

    ec = dmsg.GSProcessCreateResponse.Errors

    if ec.SUCCESS == reply_msg.err:
        the_desc = reply_msg.desc
    else:
        if soft and ec.ALREADY == reply_msg.err:
            the_desc = reply_msg.desc
        else:
            raise ProcessError(f"process create {req_msg} failed: {reply_msg.err_info}")

    return _create_stdio_connections(the_desc)


def get_argdata_from_immediate_handshake(argdata_field):
    return B64.str_to_bytes(argdata_field)


def prepare_argdata_for_immediate(argdata):
    return B64.bytes_to_str(argdata)


def create_with_argdata(
    exe,
    run_dir,
    args,
    env,
    argdata=None,
    user_name="",
    options=None,
    soft=False,
    pmi_required=False,
    stdin=None,
    stdout=None,
    stderr=None,
    policy=None,
):
    """Asks Global services to create a new process and deliver starting args to it thru messaging.

        This is an extension of the 'create' method that encapsulates our scheme for getting
        starting arguments to the new process.  This depends on how big the arguments
        turn out to be, and is tightly bound with logic in global services and in the api_setup module.

    :param exe: executable to run
    :param run_dir: directory to run it in
    :param args: argv for the process
    :param env: environment for the new process, may be added to for infrastructure
    :param argdata: bytes to deliver
    :param user_name: Requested user specified reference name
    :param options: ProcessOptions object, what options to apply to creation
    :param soft: If process already exists with given name, do not create and return descriptor instead.
    :param pmi_required: This process is part of a Dragon managed MPI/PMI application group.
    :param stdin: If `stdin=PIPE`, a Dragon Connection will be returned to enable sending data to the stdin of the child process.
    :param stdout: Capture stdout from the child process. If using PIPE, a Dragon Connection object will be returned which can be used to read from stdout. Using DEVNULL will cause the stdout data to be thrown away.
    :param stder: Capture stderr from the child process. If using PIPE, a Dragon Connection object will be returned which can be used to read from stderr. Using DEVNULL will cause the stdout data to be thrown away. Using STDOUT will cause the stderr data to be combined with the stdout data.
    :return: ProcessDescriptor object
    """

    if options is None:
        options = pdesc.ProcessOptions(make_inf_channels=True)
    else:
        options.make_inf_channels = True

    the_desc = None
    if argdata is None:
        # no arguments, so set the mode and start the normal way
        options.mode = pdesc.ArgMode.NONE
        options.argdata = None
        the_desc = create(
            exe=exe,
            run_dir=run_dir,
            args=args,
            env=env,
            user_name=user_name,
            options=options,
            soft=soft,
            pmi_required=pmi_required,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            policy=policy,
        )

    elif len(argdata) <= dfacts.ARG_IMMEDIATE_LIMIT:
        # deliver arguments in the argdata directly.
        options.mode = pdesc.ArgMode.PYTHON_IMMEDIATE

        # todo: make a setter for options.argdata?
        options.argdata = prepare_argdata_for_immediate(argdata)
        the_desc = create(
            exe=exe,
            run_dir=run_dir,
            args=args,
            env=env,
            user_name=user_name,
            options=options,
            soft=soft,
            pmi_required=pmi_required,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            policy=policy,
        )
    else:
        # TODO: capture these comments in documentation
        # Here we don't set argdata to anything at all.
        # GS will already be creating a channel local to the new process
        # for use in responding to that process's GS requests.
        # The ID of that channel is in the process descriptor that
        # is returned, so we will borrow that channel to send
        # the argument data.
        # There is no race, because the process will need to receive
        # its arguments before it can make any GS calls.
        # See globalservices.api_setup.py.

        options.mode = pdesc.ArgMode.PYTHON_CHANNEL
        options.argdata = None

        the_desc = create(
            exe=exe,
            run_dir=run_dir,
            args=args,
            env=env,
            user_name=user_name,
            options=options,
            soft=soft,
            pmi_required=pmi_required,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            policy=policy,
        )

        # Another transaction to gs but we are only here if
        # we are sending a lot of data.  Could be returned with create call
        # optionally.  Don't want to put the whole gs ret channel descriptor
        # in the process descriptor because this is the only legit
        # use for that.
        gs_ret_desc = dgchan.query(the_desc.gs_ret_cuid)
        gsret = dch.Channel.attach(gs_ret_desc.sdesc)
        # TODO PE-38745
        arg_conn = dconn.Connection(
            outbound_initializer=gsret,
            options=dconn.ConnectionOptions(min_block_size=2**16),
            policy=dparm.POLICY_INFRASTRUCTURE,
        )
        arg_conn.send_bytes(argdata)
        arg_conn.ghost_close()  # avoids sending an extra EOT message
        gsret.detach()

    return _create_stdio_connections(the_desc)


def get_list():
    """Asks Global Services for a list of the p_uids of all managed processes.

    This is the moral equivalent of a very simple unix 'ps' for managed processes.

    TODO: add some options to this, and to the message itself, to have a finer
    control over which processes you get back.

    :return: list of the p_uids of all processes, alive and dead
    """
    req_msg = dmsg.GSProcessList(tag=das.next_tag(), p_uid=this_process.my_puid, r_c_uid=das.get_gs_ret_cuid())

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSProcessListResponse)

    return reply_msg.plist


def query(identifier):
    """Asks Global Services for the ProcessDescriptor of a specified managed process

    Note you can query processes that have already exited; this is how one
    can find out the process's exit code.

    :param identifier: string indicating process name or integer indicating a p_uid
    :return: ProcessDescriptor object corresponding to specified managed process
    :raises: ProcessError if there is no such process
    """
    if isinstance(identifier, str):
        req_msg = dmsg.GSProcessQuery(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            user_name=identifier,
        )
    else:
        req_msg = dmsg.GSProcessQuery(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            t_p_uid=int(identifier),
        )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSProcessQueryResponse)

    if dmsg.GSProcessQueryResponse.Errors.SUCCESS == reply_msg.err:
        the_desc = reply_msg.desc
    else:
        raise ProcessError(f"process query {req_msg} failed: {reply_msg.err_info}")

    return _create_stdio_connections(the_desc)


def runtime_reboot(*, puids: List[int] = None, huids: List[int] = None, hostnames: List[str] = None):
    """For a list of puids, h_uids, or hostnames, find their host IDs and request a restart"""

    from ..globalservices import node as dgnode

    huids_to_restart = set()
    if puids is not None:
        for puid in puids:
            p_desc = query(puid)

            # Map the node id to a host id
            log.debug(f"process {p_desc.p_uid} was on node {p_desc.node}")
            huids_to_restart.add(p_desc.h_uid)

    if huids is not None:
        for huid in huids:
            huids_to_restart.add(huid)

    if hostnames is not None:
        for hostname in hostnames:
            n_desc = dgnode.query(hostname)
            # Map the hostname to a host id
            log.debug(f"hostname {hostname} is associated with node {n_desc.h_uid}")
            huids_to_restart.add(n_desc.h_uid)

    req_msg = dmsg.GSRebootRuntime(
        tag=das.next_tag(),
        h_uid=list(huids_to_restart),
        r_c_uid=das.get_gs_ret_cuid(),
    )
    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSRebootRuntimeResponse)


def kill(identifier, sig=signal.SIGKILL, hide_stderr=False):
    """Asks Global Services to kill a specified managed process with a specified signal.

    Note that this is like the unix 'kill' command - the signal given to the process
    might not necessarily be intended to cause it to terminate.

    :param identifier: string indicating process name or integer indicating a p_uid
    :param sig: signal to use to kill the process, default=signal.SIGKILL
    :type sig: int
    :param hide_stderr: whether or not to suppress stderr from the process with the delivery of this signal
    :type sig: bool
    :return: Nothing if successful
    :raises: ProcessError if there is no such process, or if the process has not yet started.
    """
    if isinstance(identifier, str):
        req_msg = dmsg.GSProcessKill(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            user_name=identifier,
            sig=int(sig),
            hide_stderr=hide_stderr,
        )
    else:
        req_msg = dmsg.GSProcessKill(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            t_p_uid=int(identifier),
            sig=int(sig),
            hide_stderr=hide_stderr,
        )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSProcessKillResponse)

    ec = dmsg.GSProcessKillResponse.Errors
    if ec.SUCCESS == reply_msg.err or ec.DEAD == reply_msg.err:
        return
    elif ec.UNKNOWN == reply_msg.err or ec.FAIL_KILL == reply_msg.err:
        raise ProcessError(f"process kill {req_msg} failed: {reply_msg.err_info}")
    elif ec.PENDING == reply_msg.err:
        raise ProcessError(f"process kill {req_msg} failed pending: {reply_msg.err_info}")
    else:
        raise NotImplementedError("close case")


def join(identifier, timeout=None):
    """Asks Global Services to join a specified managed process.

    Returns when the process has exited.


    :param identifier: string indicating process name or integer indicating a p_uid
    :param timeout: Timeout in seconds for max time to wait.  None = default, infinite wait
    :return: The unix exit code from the process, or None if there is a timeout.
    :raises: ProcessError if there is no such process or some other error has occurred.
    """

    if timeout is None:
        msg_timeout = -1
    elif timeout < 0:
        msg_timeout = 0
    else:
        msg_timeout = int(1000000 * timeout)

    if isinstance(identifier, str):
        req_msg = dmsg.GSProcessJoin(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            timeout=msg_timeout,
            user_name=identifier,
        )
    else:
        req_msg = dmsg.GSProcessJoin(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            timeout=msg_timeout,
            t_p_uid=int(identifier),
        )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSProcessJoinResponse)

    ec = dmsg.GSProcessJoinResponse.Errors

    if ec.SUCCESS == reply_msg.err:
        return reply_msg.exit_code
    elif ec.TIMEOUT == reply_msg.err:
        return None
    elif ec.SELF == reply_msg.err:
        raise ProcessError(f"join to {identifier} is a deadlocking self-join: {reply_msg.err_info}")
    else:
        raise ProcessError(f"process join {req_msg} failed: {reply_msg.err_info}")


def get_multi_join_success_puids(statuses: Dict[str, List[int]]) -> Tuple[List[Tuple[int, int]], bool]:
    """Go through list of processes that have been joined on to isolate successful zero exits

    :param statuses: Dict of puid keys pointing to error status and exit codes of multi_join function regardless of exit code
    :type statuses: Dict[str, List[int, int]]
    :returns: List comprised of puids and exit code tuples, if exit code was zero. And whether
              there was a timeout
    :rtype: Tuple[List[Tuple[int, int]], bool]
    """

    ec = dmsg.GSProcessJoinListResponse.Errors  # get the error codes
    success_list = []
    timeout_flag = False
    for t_p_uid, status_info in statuses.items():
        if status_info[0] == ec.SUCCESS.value:
            success_list.append((int(t_p_uid), status_info[1]))
        elif status_info[0] == ec.TIMEOUT.value:
            timeout_flag = True
    return success_list, timeout_flag


def get_multi_join_failure_puids(statuses: Dict[str, List[int]]) -> Tuple[List[Tuple[int, int]], bool]:
    """Go through list of processes that have been joined on to isolate non-zero exits

    :param statuses: Dict of puid keys pointing to error status and exit codes of multi_join function
    :type statuses: Dict[str, List[int, int]]
    :returns: Tuple made up of List comprised of puids and exit code tuples, if exit was no-zero.
              And whether there was a timeout
    :rtype: Tuple[List[Tuple[int, int]], bool]
    """
    ec = dmsg.GSProcessJoinListResponse.Errors  # get the error codes
    failure_list = []
    timeout_flag = False
    for t_p_uid, status_info in statuses.items():
        # Look for non-zero exit codes
        if status_info[0] == ec.SUCCESS.value and status_info[1] not in [0, None]:
            failure_list.append((int(t_p_uid), status_info[1]))
        elif status_info[0] == ec.TIMEOUT.value:
            timeout_flag = True
    return failure_list, timeout_flag


def multi_join(
    identifiers: List[int or str],
    timeout: bool = None,
    join_all: bool = False,
    return_on_bad_exit: bool = False,
) -> Tuple[List[Tuple[int, int]], Dict]:
    """Asks Global Services to join a list of specified managed processes.

    If join_all is False, it returns when 'any' process has exited or there is a timeout.
    If join_all is True, it returns when 'all' processes have exited or there is a timeout.

    :param identifiers: list of process identifiers indicating p_uid (int) or process name (string)
    :type identifiers: List[int, str]
    :param timeout: Timeout in seconds for max time to wait. defaults to None, infinite wait
    :type timeout: bool, optional
    :param join_all: indicates whether we need to wait on all processes in the list or not, defaults to False
    :type join_all: bool, optional
    :param return_on_bad_exit: If join_all is True, multi_join will still return if there was a
                               non-zero exit, defaults to False
    :type return_on_bad_exit: bool, optional
    :returns: If join_all is False, return a list of tuples (p_uid, unix_exit_code) for any processes exited,
              or None if none exited and there is a timeout, along with a dictionary with the status of each process.
              If join_all is True, return a list of tuples (p_uid, unix_exit_code) when all processes exited,
              or None if none exited and there is a timeout or some exited and some errored/timed out,
              along with a dictionary with the status of each process.
    """

    if len(identifiers) == 0:
        raise ProcessError("multi_join attempted on zero processes. Empty multi_join is disallowed.")

    if timeout is None:
        msg_timeout = -1
    elif timeout < 0:
        msg_timeout = 0
    else:
        msg_timeout = int(1000000 * timeout)

    puid_identifiers = []
    name_identifiers = []
    for item in identifiers:
        if isinstance(item, str):
            name_identifiers.append(item)
        else:
            puid_identifiers.append(item)

    req_msg = dmsg.GSProcessJoinList(
        tag=das.next_tag(),
        p_uid=this_process.my_puid,
        r_c_uid=das.get_gs_ret_cuid(),
        timeout=msg_timeout,
        t_p_uid_list=puid_identifiers,
        user_name_list=name_identifiers,
        join_all=join_all,
        return_on_bad_exit=return_on_bad_exit,
    )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSProcessJoinListResponse)
    success_list, timeout_flag = get_multi_join_success_puids(reply_msg.puid_status)

    if success_list:
        if join_all:  # 'all' option
            # we want all the processes finished, otherwise return None
            if len(success_list) == len(identifiers):
                return (
                    success_list,
                    reply_msg.puid_status,
                )  # also return the dict with the status of all processes
                # in the list for future use outside the
                # Connection.wait() context
            else:  # there is at least one process exited and at least one errored/timed out
                return None, reply_msg.puid_status
        else:  # 'any' option and at least one process exited
            return success_list, reply_msg.puid_status

    elif timeout_flag:  # none has exited and all timed out
        return None, reply_msg.puid_status
    else:
        log.debug(f"process join {req_msg} failed: {reply_msg.puid_status.items()}")
        raise ProcessError(f"process join {req_msg} failed")
