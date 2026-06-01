"""API for managing the life-cycle of objects, such as processes and channels, through Global Services"""

import logging
import sys
import threading
import gc
import os

import dragon.channels as dch
import dragon.globalservices.process as dproc
import dragon.infrastructure.process_desc as dpdesc
import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.connection as dconn
import dragon.infrastructure.parameters as dp
import dragon.infrastructure.debug_support as ddbg
import dragon.infrastructure.facts as dfacts
import dragon.utils as du

LOG = logging.getLogger("api_setup")

_GS_API_LOCK = threading.RLock()
_GS_SEND_LOCK = threading.RLock()
_GS_RECV_LOCK = threading.RLock()

_MSG_TAG_CTR = 0


def next_tag():
    global _MSG_TAG_CTR
    with _GS_API_LOCK:
        tmp = _MSG_TAG_CTR
        _MSG_TAG_CTR += 1
    return tmp


_ARG_PAYLOAD = None

_GS_INPUT_CHANNEL = None
_GS_RETURN_CHANNEL = None
_LOCAL_SHEP_INPUT_CHANNEL = None
_LOCAL_SHEP_RETURN_CHANNEL = None

_GS_INPUT = None
_GS_RETURN = None
_SHEP_INPUT = None
_SHEP_RETURN = None

_GS_RETURN_CUID = None
_SHEP_RETURN_CUID = None


def get_gs_ret_cuid():
    if not _INFRASTRUCTURE_CONNECTED:
        connect_to_infrastructure()

    return _GS_RETURN_CUID


def load_launch_parameter(name):
    param = getattr(dp.this_process, name.lower())
    assert param, f"Launch parameter not initialized: {name}"
    return du.B64.str_to_bytes(param)


def _connect_gs_input():
    global _GS_INPUT_CHANNEL

    channel_descriptor = load_launch_parameter(dfacts.GS_CD)
    _GS_INPUT_CHANNEL = dch.Channel.attach(channel_descriptor)
    # size chosen to be safely above ARG_IMMEDIATE_LIMIT
    # TODO PE-38745
    return dconn.Connection(
        outbound_initializer=_GS_INPUT_CHANNEL,
        options=dconn.ConnectionOptions(min_block_size=2**21, large_block_size=2**22, huge_block_size=2**23),
        policy=dp.POLICY_INFRASTRUCTURE,
    )


def _connect_gs_return():
    global _GS_RETURN_CHANNEL
    global _GS_RETURN_CUID

    channel_descriptor = load_launch_parameter(dfacts.GS_RET_CD)
    _GS_RETURN_CHANNEL = dch.Channel.attach(channel_descriptor)
    _GS_RETURN_CUID = _GS_RETURN_CHANNEL.cuid
    return dconn.Connection(inbound_initializer=_GS_RETURN_CHANNEL, policy=dp.POLICY_INFRASTRUCTURE)


def _connect_shep_input():
    global _LOCAL_SHEP_INPUT_CHANNEL

    channel_descriptor = load_launch_parameter(dfacts.LOCAL_SHEP_CD)
    _LOCAL_SHEP_INPUT_CHANNEL = dch.Channel.attach(channel_descriptor)
    return dconn.Connection(outbound_initializer=_LOCAL_SHEP_INPUT_CHANNEL, policy=dp.POLICY_INFRASTRUCTURE)


def _connect_shep_return():
    global _LOCAL_SHEP_RETURN_CHANNEL
    global _SHEP_RETURN_CUID

    # currently we have no use for the shep return cd (although we might in the future)
    # it isn't currently being constructed by GS so here we will see if it
    # is present and return None if it isn't.

    try:
        channel_descriptor = load_launch_parameter(dfacts.SHEP_RET_CD)
    except AssertionError:
        return None
    _LOCAL_SHEP_RETURN_CHANNEL = dch.Channel.attach(channel_descriptor)
    _SHEP_RETURN_CUID = _LOCAL_SHEP_RETURN_CHANNEL.cuid
    return dconn.Connection(inbound_initializer=_LOCAL_SHEP_RETURN_CHANNEL, policy=dp.POLICY_INFRASTRUCTURE)


def _close_connections():
    with _GS_API_LOCK:
        _GS_INPUT.close()
        _GS_RETURN.close()
        _SHEP_INPUT.close()
        _SHEP_RETURN.close()


def _detach_infrastructure():
    with _GS_API_LOCK:
        if _GS_INPUT_CHANNEL is not None:
            _GS_INPUT_CHANNEL.detach()

        if _GS_RETURN_CHANNEL is not None:
            _GS_RETURN_CHANNEL.detach()

        if _LOCAL_SHEP_INPUT_CHANNEL is not None:
            _LOCAL_SHEP_INPUT_CHANNEL.detach()

        if _LOCAL_SHEP_RETURN_CHANNEL is not None:
            _LOCAL_SHEP_RETURN_CHANNEL.detach()


# global because of threading.

# contains condition variables corresponding to threads awaiting
# a response from global services.  Key = tag, value = cv
_WAKEUPS = dict()

# contains messages gotten back from global services, to be picked up by
# waiting threads.
_RESULTS = dict()

_INFRASTRUCTURE_CONNECTED = False


def gs_request(req_msg, *, expecting_response=True):
    """Posts a message to GS and gets the response in a thread safe way

    This is an attempt to make global services transactions blocking
    and thread safe while avoiding needless head of queue blocking - we
    don't want there to be only one transaction outstanding at a time
    because it might take a long time to finish and block other threads'
    transactions in the meantime, but without starting a separate dedicated
    thread simply to deal and service gs transactions.  The implementation
    here tries to solve this without involving a new thread at all, but
    that might not be the best way to do it; a separate service thread
    would probably be simpler.

    The overhead of doing this stuff if only one thread is ever involved
    means acquiring a couple locks that have no contention on them
    and constructing an event variable which means another lock.


    Arguments:
        :param req_msg: request message to send.
        :param expecting_response: Bool default True, is a response message expected?

    Returns:
        the reply to the request message
    """

    global _WAKEUPS
    global _RESULTS

    with _GS_API_LOCK:
        if not _INFRASTRUCTURE_CONNECTED:
            connect_to_infrastructure()

    # PE-44998, do not allow the GC to run during the life of this function as it may result in a thread re-entering
    # this function, which can lead to a hang. GC cleanup of Dragon-native objects for multiprocessing
    # include communication with GS, and that is where the re-entering can happen.
    gc.disable()

    # Serialize request message
    req_msg_bytes = req_msg.serialize()

    # Ensure wake-up event is available prior to sending
    if expecting_response:
        ready = _WAKEUPS[req_msg.tag] = threading.Event()

    # Send request
    with _GS_SEND_LOCK:
        try:
            _GS_INPUT.send(req_msg_bytes)
        except Exception as e:
            gc.enable()
            raise e

    if not expecting_response:
        gc.enable()
        return

    # Wait for response
    while req_msg.tag not in _RESULTS:
        if _GS_RECV_LOCK.acquire(blocking=False):
            # Read responses until expected response is received
            try:
                while req_msg.tag not in _RESULTS:
                    # Receive and parse response
                    resp = dmsg.parse(_GS_RETURN.recv())

                    try:
                        wakeup = _WAKEUPS.pop(resp.ref)
                    except KeyError:
                        if resp.ref is not None:
                            logging.warning(f"Received unexpected message {resp.ref}")
                    else:
                        # Save response message
                        _RESULTS[resp.ref] = resp
                        # Alert owner response is available
                        wakeup.set()

                # Wake up last sender to ensure someone takes over receiving
                try:
                    # Avoid iteration for thread-safety using CPython
                    _k, _v = _WAKEUPS.popitem()
                except KeyError:
                    # No one else is waiting for a response
                    pass
                else:
                    # Re-add the event to _WAKEUPS and set it
                    _WAKEUPS[_k] = _v
                    _v.set()

                # I suppose we could return here too
                gc.enable()
                return _RESULTS.pop(resp.ref)
            except Exception as e:
                gc.enable()
                raise e
            finally:
                _GS_RECV_LOCK.release()

        # Wait for response or to be woken up to take over receiving.
        if ready.wait():
            # Reset event in case response is not ready, i.e., we were woken up
            # but not alerted
            ready.clear()

    gc.enable()
    return _RESULTS.pop(req_msg.tag)


def test_connection_override(
    test_gs_input=None,
    test_gs_return=None,
    test_gs_return_cuid=None,
    test_shep_input=None,
    test_shep_return=None,
    test_shep_return_cuid=None,
):
    global _GS_INPUT
    global _GS_RETURN
    global _SHEP_INPUT
    global _SHEP_RETURN
    global _INFRASTRUCTURE_CONNECTED
    global _GS_RETURN_CUID
    global _SHEP_RETURN_CUID

    LOG.debug("dragon connection override")

    with _GS_API_LOCK:

        _INFRASTRUCTURE_CONNECTED = True

        if test_gs_input is not None:
            _GS_INPUT = test_gs_input
        else:
            _GS_INPUT = _connect_gs_input()

        if test_gs_return is not None:
            _GS_RETURN = test_gs_return
            _GS_RETURN_CUID = test_gs_return_cuid
        else:
            _GS_RETURN = _connect_gs_return()

        if test_shep_input is not None:
            _SHEP_INPUT = test_shep_input
        else:
            _SHEP_INPUT = _connect_shep_input()

        if test_shep_return is not None:
            _SHEP_RETURN = test_shep_return
            _SHEP_RETURN_CUID = test_shep_return_cuid
        else:
            _SHEP_RETURN = _connect_shep_return()


def connect_to_infrastructure(force=False):
    global _GS_INPUT
    global _GS_RETURN
    global _SHEP_INPUT
    global _SHEP_RETURN
    global _INFRASTRUCTURE_CONNECTED

    # Here we register the gateway channels
    # provided to this process as environment
    # variables. This is done first to enable
    # off-node communication from this process
    # as soon as possible.
    if force:
        dp.reload_this_process()

    with _GS_API_LOCK:
        if not force and _INFRASTRUCTURE_CONNECTED:
            return

        LOG.info(f"connecting to infrastructure from {os.getpid()}")

        _GS_INPUT = _connect_gs_input()
        _GS_RETURN = _connect_gs_return()
        _SHEP_INPUT = _connect_shep_input()
        _SHEP_RETURN = _connect_shep_return()
        _INFRASTRUCTURE_CONNECTED = True

        if force:
            return

        LOG.debug("waiting for handshake")

        global _ARG_PAYLOAD

        # recv something from gs_return
        handshake = dmsg.parse(_GS_RETURN.recv())
        LOG.debug(f"Got response {handshake}")
        assert isinstance(handshake, dmsg.GSPingProc)

        if handshake.mode == dpdesc.ArgMode.NONE:
            _ARG_PAYLOAD = None
        elif handshake.mode == dpdesc.ArgMode.PYTHON_IMMEDIATE:
            _ARG_PAYLOAD = dproc.get_argdata_from_immediate_handshake(handshake.argdata)
        elif handshake.mode == dpdesc.ArgMode.PYTHON_CHANNEL:
            # receive as bytes through the gs ret connection.
            # This one lump of data comes from the managed process
            # that caused this process to be started and not GS.
            # There is no race with what GS might do with the channel
            # because this process can't send any responses
            # until this message is received
            _ARG_PAYLOAD = _GS_RETURN.recv_bytes()
        else:
            raise NotImplementedError("close case")

        LOG.debug("got handshake")

        sys.breakpointhook = ddbg.dragon_debug_hook
        LOG.info("debug entry hooked")

        # Would like to do this, but the bare except:
        # on line 327 of lib/python3.9/multiprocessing/process.py prevents
        # any variation in handling.      So the question arises:
        # if you want this, you have to do something about _bootstrap, and if
        # you are doing something about _bootstrap, why not everything?
        # sys.excepthook = ddbg.dragon_exception_hook
        # LOG.info('exception entry hooked')
