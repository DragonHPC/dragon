"""Global Services API for the Channels primary resource component."""

import logging
from math import inf
import time

import dragon.globalservices.api_setup as das

import dragon.infrastructure.messages as dmsg

from dragon.infrastructure.parameters import this_process
from dragon.infrastructure.channel_desc import ChannelDescriptor

log = logging.getLogger("channel_api")


class ChannelError(Exception):
    pass


def create(m_uid, user_name="", options=None, soft=False, puid_override=None):
    """Asks Global Services to create a new channel

    :param m_uid: m_uid of pool to create the channel in
    :param user_name: Requested user specified reference name
    :param options: ChannelOptions object, what options to apply to creation
    :param soft: Default False.
                 If channel already exists with given name, do not create and
                 return descriptor instead.
    :param puid_override: Only needed when the channel is being created on behalf
                 of some other process. Normally this is not needed.
    :return: ChannelDescriptor object
    """

    if options is None:
        options = {}

    if soft and not user_name:
        raise ChannelError("soft create requires a user supplied channel name")

    puid = puid_override

    if puid is None:
        puid = this_process.my_puid

    req_msg = dmsg.GSChannelCreate(
        tag=das.next_tag(), p_uid=puid, r_c_uid=das.get_gs_ret_cuid(), m_uid=m_uid, user_name=user_name, options=options
    )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSChannelCreateResponse)

    ec = dmsg.GSChannelCreateResponse.Errors

    if ec.SUCCESS == reply_msg.err:
        the_desc = reply_msg.desc
    else:
        if soft and ec.ALREADY == reply_msg.err:
            the_desc = reply_msg.desc
        else:
            raise ChannelError(f"channel create {req_msg} failed: {reply_msg!r}")

    return the_desc


def get_list():
    """Asks Global Services for a list of the c_uids of all currently active channels

    TODO: add some options to this, and to the message itself, to have a finer
    control over which channels you get back.  Deferring until we have a use case,
    but one that springs to mind is channels in a particular pool by m_uid.

    :return: list of the  c_uids of all currently existing channels
    """
    req_msg = dmsg.GSChannelList(tag=das.next_tag(), p_uid=this_process.my_puid, r_c_uid=das.get_gs_ret_cuid())

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSChannelListResponse)

    return reply_msg.clist


def query(identifier, *, inc_refcnt=False):
    """Asks Global Services for the ChannelDescriptor by c_uid or name

    Note you can only query currently existing channels

    :param identifier: string indicating channel name or integer indicating a c_uid
    :param inc_refcnt: bool indicating whether this query is also to inc the refcnt on the channel
    :return: ChannelDescriptor object corresponding to specified channel
    :raises: ChannelError if there is no such channel
    """
    if isinstance(identifier, str):
        req_msg = dmsg.GSChannelQuery(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            inc_refcnt=inc_refcnt,
            user_name=identifier,
        )
    else:
        req_msg = dmsg.GSChannelQuery(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            inc_refcnt=inc_refcnt,
            c_uid=int(identifier),
        )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSChannelQueryResponse)

    if dmsg.GSChannelQueryResponse.Errors.SUCCESS == reply_msg.err:
        the_desc = reply_msg.desc
    else:
        raise ChannelError(f"channel query {req_msg} failed: {reply_msg.err_info}")

    return the_desc


def wait_for(identifier, predicate, timeout=None, tries=1, interval=0.1):
    """Wait for predicate to return True. predicate must be a callable that
    takes a ChannelDescriptor instance. A timeout may be provided giving the
    maximum time to wait after a minimum number of tries for predicate to return
    True are completed.

    :param identifier: String indicating channel name or integer indicating a c_uid
    :param predicate: Callable that takes a ChannelDescriptor instance and returns a boolean
    :param timeout: Number of seconds before timing out (default: infinite)
    :param tries: Minimum number of query attempts before timing out (default: 1)
    :param interval: Number of seconds to sleep in between query attempts (default: 0.1)
    :return: ChannelDescriptor object corresponding to the specified channel
    :raises: TypeError if predicate is not callable
    :raises: ChannelError if there is no such channel
    :raises: TimeoutError if the number of tries is exhausted and the timeout expires
    """
    if not callable(predicate):
        raise TypeError("predicate is not callable")
    if timeout is None:
        timeout = inf
    end = time.monotonic() + timeout
    while time.monotonic() < end or tries > 0:
        desc = query(identifier)
        if predicate(desc):
            return desc
        if tries > 0:
            tries -= 1
        time.sleep(interval)
    raise TimeoutError(f"waiting for channel {identifier}")


def wait(identifier, state=ChannelDescriptor.State.DEAD, **kwds):
    """Wait for channel to be in a specified state. Other keyword arguments are
    passed through to wait_for().

    :param identifier: String indicating channel name or integer indicating a c_uid
    :param state: Desired ChannelDescriptor.State (default: DEAD)
    :return: ChannelDescriptor object corresponding to the specified channel
    :raises: ChannelError if there is no such channel
    :raises: TimeoutError if the number of tries is exhausted and the timeout expires
    """
    return wait_for(identifier, lambda desc: desc.state is state, **kwds)


def join(identifier, timeout=None):
    """Waits for a channel with a particular name to get made.

    :param identifier: string indicating channel name
    :param timeout: Time in seconds to timeout.  if absent, block indefinitely.
    :return: ChannelDescriptor for the channel specified
    :raises: TimeoutError if the timeout expires
    """

    assert isinstance(identifier, str)
    if timeout is None:
        timeout = -1
    elif timeout < 0:
        timeout = 0
    else:
        timeout = int(1000000 * timeout)

    req = dmsg.GSChannelJoin(
        tag=das.next_tag(), p_uid=this_process.my_puid, r_c_uid=das.get_gs_ret_cuid(), name=identifier, timeout=timeout
    )

    reply = das.gs_request(req)
    assert isinstance(reply, dmsg.GSChannelJoinResponse)

    if dmsg.GSChannelJoinResponse.Errors.TIMEOUT == reply.err:
        raise TimeoutError(f"join request to {identifier} timed out")

    return reply.desc


def destroy(identifier, *, just_decref=False):
    """Asks Global Services to destroy a specified channel.

    :param just_decref: bool default False, just decrement refcount, not hard destroy.
    :param identifier: string indicating channel name or integer indicating a c_uid
    :return: Nothing if successful
    :raises: ChannelError if there is no such channel
    """

    rreq = not just_decref

    if isinstance(identifier, str):
        req_msg = dmsg.GSChannelDestroy(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            dec_ref=just_decref,
            reply_req=rreq,
            user_name=identifier,
        )
    else:
        req_msg = dmsg.GSChannelDestroy(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            dec_ref=just_decref,
            reply_req=rreq,
            c_uid=int(identifier),
        )

    reply_msg = das.gs_request(req_msg, expecting_response=rreq)
    if not rreq:
        return

    assert isinstance(reply_msg, dmsg.GSChannelDestroyResponse)

    ec = dmsg.GSChannelDestroyResponse.Errors

    if ec.SUCCESS == reply_msg.err:
        return
    else:
        raise ChannelError(f"channel destroy {req_msg} failed: {reply_msg.err_info}")


def get_refcnt(identifier):
    return query(identifier, inc_refcnt=True)


def release_refcnt(identifier):
    return destroy(identifier, just_decref=True)
