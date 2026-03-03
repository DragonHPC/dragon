"""Global Services API for the Managed Memory Pools and Allocations primary resource component.

The current API only manages memory pools, future versions will also contain
methods to track allocations.
"""

import logging

import dragon.infrastructure.messages as dmsg
from dragon.infrastructure.parameters import this_process
import dragon.globalservices.api_setup as das

log = logging.getLogger("pool_api")


class PoolError(Exception):
    pass


def create(size, user_name="", options=None, soft=False):
    """Asks Global Services to create a new  memory pool

    :param size: size of memory pool to create
    :param user_name: Requested user specified reference name
    :param options: PoolOptions object, what options to apply to creation
    :param soft: If pool already exists with given name, do not create and return descriptor instead.
    :return: PoolDescriptor object
    """

    if options is None:
        options = {}

    if soft and not user_name:
        raise PoolError("soft create requires a user supplied pool name")

    req_msg = dmsg.GSPoolCreate(
        tag=das.next_tag(),
        p_uid=this_process.my_puid,
        r_c_uid=das.get_gs_ret_cuid(),
        size=size,
        user_name=user_name,
        options=options,
    )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSPoolCreateResponse)

    ec = dmsg.GSPoolCreateResponse.Errors

    if ec.SUCCESS == reply_msg.err:
        the_desc = reply_msg.desc
    else:
        if soft and ec.ALREADY == reply_msg.err:
            the_desc = reply_msg.desc
        else:
            if hasattr(reply_msg, "err_info"):
                raise PoolError(f"pool create {req_msg} failed: {reply_msg.err_info}")
            else:
                raise PoolError(f"pool create {req_msg} failed. Response message was {repr(reply_msg)}")

    return the_desc


def get_list():
    """Asks Global Services for a list of the m_uids of all memory pools.

    TODO: add some options to this, and to the message itself, to have a finer
    control over which pools you get back.  Deferring until we have a use case

    :return: list of the  m_uids of all currently existing memory pools
    """
    req_msg = dmsg.GSPoolList(tag=das.next_tag(), p_uid=this_process.my_puid, r_c_uid=das.get_gs_ret_cuid())

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSPoolListResponse)

    return reply_msg.mlist


def query(identifier):
    """Asks Global Services for the PoolDescriptor of a specified memory pool

    Note you can only query currently existing pools.

    :param identifier: string indicating pool name or integer indicating a m_uid
    :return: PoolDescriptor object corresponding to specified pool
    :raises: PoolError if there is no such pool
    """
    if isinstance(identifier, str):
        req_msg = dmsg.GSPoolQuery(
            tag=das.next_tag(), p_uid=this_process.my_puid, r_c_uid=das.get_gs_ret_cuid(), user_name=identifier
        )
    else:
        req_msg = dmsg.GSPoolQuery(
            tag=das.next_tag(), p_uid=this_process.my_puid, r_c_uid=das.get_gs_ret_cuid(), m_uid=int(identifier)
        )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSPoolQueryResponse)

    if dmsg.GSPoolQueryResponse.Errors.SUCCESS == reply_msg.err:
        the_desc = reply_msg.desc
    else:
        raise PoolError(f"pool query {req_msg} failed: {reply_msg.err_info}")

    return the_desc


def destroy(identifier):
    """Asks Global Services to destroy a specified pool.

    TODO: figure out the semantics on this.  Do we want to throw an error from
    GS if there are any channels in that pool currently?

    :param identifier: string indicating pool name or integer indicating a m_uid
    :return: Nothing if successful
    :raises: PoolError if there is no such pool
    """
    if isinstance(identifier, str):
        req_msg = dmsg.GSPoolDestroy(
            tag=das.next_tag(), p_uid=this_process.my_puid, r_c_uid=das.get_gs_ret_cuid(), user_name=identifier
        )
    else:
        req_msg = dmsg.GSPoolDestroy(
            tag=das.next_tag(), p_uid=this_process.my_puid, r_c_uid=das.get_gs_ret_cuid(), m_uid=int(identifier)
        )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSPoolDestroyResponse)

    ec = dmsg.GSPoolDestroyResponse.Errors

    if ec.SUCCESS == reply_msg.err or ec.GONE == reply_msg.err:
        return
    elif ec.UNKNOWN == reply_msg.err or ec.FAIL == reply_msg.err:
        raise PoolError(f"pool destroy {req_msg} failed: {reply_msg.err_info}")
    elif ec.PENDING == reply_msg.err:
        raise PoolError(f"pool destroy {req_msg} failed pending: {reply_msg.err_info}")
    else:
        raise NotImplementedError("close case")
