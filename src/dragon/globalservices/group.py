"""Global Services API for the Group of resources (processes, channels, pools, etc.)
primary resource component. Only process groups are supported at this time.
"""

import signal
import logging

from ..infrastructure.policy import Policy

from ..infrastructure import messages as dmsg
from ..infrastructure.parameters import this_process
from ..globalservices import api_setup as das

log = logging.getLogger("group_api")


class GroupError(Exception):
    pass


def _check_msg_types(items):
    for _, msg in items:
        if not isinstance(dmsg.parse(msg), (dmsg.GSChannelCreate, dmsg.GSProcessCreate, dmsg.GSPoolCreate)):
            raise GroupError("An invalid message type was provided.")


def create(items, policy, user_name="", soft=False):
    """Asks Global Services to create a new group of specified resources.

    :param items: list of tuples where each tuple contains a replication factor `n` and the Dragon create message. Dragon create messages do not have to be of the same primary resources, i.e. ChannelCreate and ProcessCreate messages can be mixed to into groups to represent composite resources.
    :type items: list[tuple[int, dragon.infrastructure.messages.Message]]
    :param policy: determines the placement of the group resources
    :type policy: dragon.infrastructure.policy.Policy
    :param user_name: Requested user specified reference name, defaults to ''
    :type user_name: str, optional
    :param soft: If group already exists with given name, do not create and return descriptor instead, defaults to False
    :type soft: bool, optional
    :raises GroupError: if soft is True and not given a group name
    :raises GroupError: if the action fails
    :return: the descriptor of the group object
    :rtype: dragon.infrastructure.group_desc.GroupDescriptor
    """

    if soft and not user_name:
        raise GroupError("soft create requires a user supplied group name")

    # check that we have valid message types on each tuple
    _check_msg_types(items)

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

    req_msg = dmsg.GSGroupCreate(
        tag=das.next_tag(),
        p_uid=this_process.my_puid,
        r_c_uid=das.get_gs_ret_cuid(),
        items=items,
        policy=policy,
        user_name=user_name,
    )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSGroupCreateResponse)

    if reply_msg.desc:
        the_desc = reply_msg.desc
        # TODO: add check that the items tuples correspond to the
        # correct tuples in the response message
    else:
        raise GroupError(f"group create {req_msg} failed, no descriptor returned")

    return the_desc


def kill(identifier, sig=signal.SIGKILL, hide_stderr=False):
    """Asks Global Services to send the processes belonging to a specified group
    a specified signal.

    Note that this is like the unix 'kill' command - the signal given to the process
    might not necessarily be intended to cause it to terminate.
    If the group contains members other than processes, such as channels, then
    this operation has no effect to these members.

    :param identifier: string indicating group name or integer indicating a g_uid
    :type identifier: str|int
    :param sig: signal to use to kill the process, default=signal.SIGKILL
    :type sig: int
    :param hide_stderr: whether or not to suppress stderr from the process with the delivery of this signal
    :type hide_stderr: bool
    :raises GroupError: if there is no such group
    :raises GroupError: if the group has not yet started
    :raises NotImplementedError: if any other case not implemented
    :return: GSGroupKillResponse.desc
    """
    if isinstance(identifier, str):
        req_msg = dmsg.GSGroupKill(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            sig=int(sig),
            user_name=identifier,
            hide_stderr=hide_stderr,
        )
    else:
        req_msg = dmsg.GSGroupKill(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            sig=int(sig),
            g_uid=int(identifier),
            hide_stderr=hide_stderr,
        )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSGroupKillResponse)
    ec = dmsg.GSGroupKillResponse.Errors

    if reply_msg.err in {ec.SUCCESS, ec.ALREADY}:
        # if a successful GSGroupKillResponse message was sent or all the
        # processes were already dead
        return reply_msg.desc
    elif ec.UNKNOWN == reply_msg.err:
        raise GroupError(f"group kill {req_msg} failed: {reply_msg.err_info}")
    elif ec.PENDING == reply_msg.err:
        raise GroupError(f"group kill {req_msg} failed pending: {reply_msg.err_info}")
    else:
        raise NotImplementedError("close case")


def destroy(identifier):
    """Asks Global Services to destroy a specified group of resources.
    It will destroy all the members of the group as well as the group itself.

    :param identifier: string indicating group name or integer indicating a g_uid
    :type identifier: str|int
    :raises GroupError: if there is no such group
    :raises GroupError: if the group has not yet started
    :raises NotImplementedError: if any other case not implemented
    :return: the group descriptor in case of a success
    :rtype: dragon.infrastructure.group_desc.GroupDescriptor
    """
    if isinstance(identifier, str):
        req_msg = dmsg.GSGroupDestroy(
            tag=das.next_tag(), p_uid=this_process.my_puid, r_c_uid=das.get_gs_ret_cuid(), user_name=identifier
        )
    else:
        req_msg = dmsg.GSGroupDestroy(
            tag=das.next_tag(), p_uid=this_process.my_puid, r_c_uid=das.get_gs_ret_cuid(), g_uid=int(identifier)
        )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSGroupDestroyResponse)

    ec = dmsg.GSGroupDestroyResponse.Errors

    if ec.SUCCESS == reply_msg.err:
        return reply_msg.desc
    elif ec.UNKNOWN == reply_msg.err:
        raise GroupError(f"group destroy {req_msg} failed: {reply_msg.err_info}")
    elif ec.PENDING == reply_msg.err:
        raise GroupError(f"group destroy {req_msg} failed pending: {reply_msg.err_info}")
    else:
        raise NotImplementedError("close case")


def add_to(identifier, items):
    """Add existing resources to an existing group.

    :param identifier: string indicating group name or integer indicating a g_uid
    :type identifier: str|int
    :param items: list of int or str corresponding to resource id or name respectively
    :type items: List[ids : int|str]
    :raises GroupError: if the addition of resources failed
    :raises GroupError: if the identifier corresponds to an unknown group
    """

    if isinstance(identifier, str):
        req_msg = dmsg.GSGroupAddTo(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            user_name=identifier,
            items=items,
        )
    else:
        req_msg = dmsg.GSGroupAddTo(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            g_uid=int(identifier),
            items=items,
        )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSGroupAddToResponse)

    if reply_msg.desc:
        the_desc = reply_msg.desc
    elif dmsg.GSGroupAddToResponse.Errors.UNKNOWN == reply_msg.err:
        raise GroupError(f"group addition {req_msg} failed due to unknown group: {reply_msg.err_info}")
    else:
        raise GroupError(f"group addition {req_msg} failed: {reply_msg.err_info}")

    return the_desc


def create_add_to(identifier, items, policy):
    """Create resources and then add them to an existing group.

    :param identifier: string indicating group name or integer indicating a g_uid
    :type identifier: str|int
    :param items: list of tuples where each tuple contains a replication factor `n` and the Dragon create message. Dragon create messages do not have to be of the same primary resources, i.e. ChannelCreate and ProcessCreate messages can be mixed to into groups to represent composite resources.
    :type items: list[tuple[int, dragon.infrastructure.messages.Message]]
    :param policy: determines the placement of the group resources
    :type policy: dragon.infrastructure.policy.Policy
    :raises GroupError: if the addition of resources failed
    :raises GroupError: if the identifier corresponds to an unknown group
    """

    # check that we have valid message types on each tuple
    _check_msg_types(items)

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

    if isinstance(identifier, str):
        req_msg = dmsg.GSGroupCreateAddTo(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            user_name=identifier,
            items=items,
            policy=policy,
        )
    else:
        req_msg = dmsg.GSGroupCreateAddTo(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            g_uid=int(identifier),
            items=items,
            policy=policy,
        )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSGroupCreateAddToResponse)

    if reply_msg.desc:
        return reply_msg.desc
    elif dmsg.GSGroupCreateAddToResponse.Errors.UNKNOWN == reply_msg.err:
        raise GroupError(f"group addition {req_msg} failed due to unknown group: {reply_msg.err_info}")
    else:
        raise GroupError(f"group addition {req_msg} failed: {reply_msg.err_info}")


def remove_from(identifier, items):
    """Remove resources from an existing group.

    :param identifier: string indicating group name or integer indicating a g_uid
    :type identifier: str|int
    :param items: list of int or str corresponding to resource id or name respectively
    :type items: List[ids : int|str]
    :raises GroupError: if the addition of resources failed
    :raises GroupError: if the identifier corresponds to an unknown group
    """

    if isinstance(identifier, str):
        req_msg = dmsg.GSGroupRemoveFrom(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            user_name=identifier,
            items=items,
        )
    else:
        req_msg = dmsg.GSGroupRemoveFrom(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            g_uid=int(identifier),
            items=items,
        )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSGroupRemoveFromResponse)

    if dmsg.GSGroupRemoveFromResponse.Errors.SUCCESS == reply_msg.err:
        return reply_msg.desc
    elif dmsg.GSGroupRemoveFromResponse.Errors.UNKNOWN == reply_msg.err:
        raise GroupError(f"removal of group resources {req_msg} failed due to unknown group: {reply_msg.err_info}")
    else:
        raise GroupError(f"removal of group resources {req_msg} failed: {reply_msg.err_info}")


def destroy_remove_from(identifier, items):
    """Remove resources from an existing group and call destroy on them.
    For processes we call process.kill (sends a SIGKILL signal) and for other
    types of resources we call destroy.

    :param identifier: string indicating group name or integer indicating a g_uid
    :type identifier: str|int
    :param items: list of int or str corresponding to resource id or name respectively
    :type items: List[ids : int|str]
    :raises GroupError: if the addition of resources failed
    :raises GroupError: if the identifier corresponds to an unknown group
    """

    if isinstance(identifier, str):
        req_msg = dmsg.GSGroupDestroyRemoveFrom(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            user_name=identifier,
            items=items,
        )
    else:
        req_msg = dmsg.GSGroupDestroyRemoveFrom(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=das.get_gs_ret_cuid(),
            g_uid=int(identifier),
            items=items,
        )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSGroupDestroyRemoveFromResponse)

    if dmsg.GSGroupDestroyRemoveFromResponse.Errors.SUCCESS == reply_msg.err:
        return reply_msg.desc
    elif dmsg.GSGroupDestroyRemoveFromResponse.Errors.UNKNOWN == reply_msg.err:
        raise GroupError(f"removal of group resources {req_msg} failed due to unknown group: {reply_msg.err_info}")
    else:
        raise GroupError(f"removal of group resources {req_msg} failed: {reply_msg.err_info}")


def get_list():
    """Asks Global Services for a list of the g_uids of all groups.

    :return: list of the g_uids of all groups, alive and dead
    :rtype: list[g_uids]
    """
    req_msg = dmsg.GSGroupList(tag=das.next_tag(), p_uid=this_process.my_puid, r_c_uid=das.get_gs_ret_cuid())

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSGroupListResponse)

    return reply_msg.glist


def query(identifier):
    """Asks Global Services for the GroupDescriptor of a specified group of resources.
    The group can be alive or dead.

    :param identifier: string indicating group name or integer indicating a g_uid
    :type identifier: str|int
    :raises GroupError: if there is no such group
    :return: GroupDescriptor object corresponding to specified group
    :rtype: GroupDescriptor
    """
    if isinstance(identifier, str):
        req_msg = dmsg.GSGroupQuery(
            tag=das.next_tag(), p_uid=this_process.my_puid, r_c_uid=das.get_gs_ret_cuid(), user_name=identifier
        )
    else:
        req_msg = dmsg.GSGroupQuery(
            tag=das.next_tag(), p_uid=this_process.my_puid, r_c_uid=das.get_gs_ret_cuid(), g_uid=int(identifier)
        )

    reply_msg = das.gs_request(req_msg)
    assert isinstance(reply_msg, dmsg.GSGroupQueryResponse)

    if dmsg.GSGroupQueryResponse.Errors.SUCCESS == reply_msg.err:
        return reply_msg.desc
    else:
        raise GroupError(f"group query {req_msg} failed: {reply_msg.err_info}")
