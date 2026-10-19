"""Global Services API for the Node primary resource component.

The 'Node' component abstracts the hardware on a node level, providing
information like the number of CPUs or the amount of memory available on the
node.
"""

import enum
import logging

from typing import List
from .api_setup import get_gs_ret_cuid, next_tag, gs_request

from ..infrastructure.messages import (
    GSNodeList,
    GSNodeListResponse,
    GSNodeQuery,
    GSNodeQueryResponse,
    GSNodeQueryAll,
    GSNodeQueryAllResponse,
    GSNodeQueryTotalCPUCount,
    GSNodeQueryTotalCPUCountResponse,
)

from ..infrastructure.parameters import this_process
from ..infrastructure.node_desc import NodeDescriptor

LOG = logging.getLogger(__name__)


class DragonNodeError(Exception):
    pass


def query(identifier) -> NodeDescriptor:
    """Asks GS for the node descriptor beloging to a node identified by name or h_uid .

    :param identifier: Node id to query. This is the name if of type str, h_uid if of type int
    :return: descriptor
    :raises:
    """

    if isinstance(identifier, int):  # by h_uid
        req_msg = GSNodeQuery(tag=next_tag(), p_uid=this_process.my_puid, r_c_uid=get_gs_ret_cuid(), h_uid=identifier)
    else:  # by name
        req_msg = GSNodeQuery(tag=next_tag(), p_uid=this_process.my_puid, r_c_uid=get_gs_ret_cuid(), name=identifier)

    reply_msg = gs_request(req_msg)
    assert isinstance(reply_msg, GSNodeQueryResponse)

    if GSNodeQueryResponse.Errors.SUCCESS == reply_msg.err:
        the_desc = reply_msg.desc

    return the_desc


def query_all() -> List[NodeDescriptor]:
    """
    Asks Global Services to return the NodeDescriptor for
    every node that is part of the system.

    :return: list of NodeDescriptors
    """

    req_msg = GSNodeQueryAll(tag=next_tag(), p_uid=this_process.my_puid, r_c_uid=get_gs_ret_cuid())

    reply_msg = gs_request(req_msg)
    assert isinstance(reply_msg, GSNodeQueryAllResponse)

    descriptors = []
    if GSNodeQueryAllResponse.Errors.SUCCESS == reply_msg.err:
        descriptors = reply_msg.descriptors
    return descriptors


def query_total_cpus() -> int:
    """Asks GS to return the total number of CPUS beloging to all of the registered nodes.

    :return: integer value of the number of cpus
    :raises:
    """
    total_cpus = 0

    req_msg = GSNodeQueryTotalCPUCount(tag=next_tag(), p_uid=this_process.my_puid, r_c_uid=get_gs_ret_cuid())

    reply_msg = gs_request(req_msg)
    assert isinstance(reply_msg, GSNodeQueryTotalCPUCountResponse)

    if GSNodeQueryTotalCPUCountResponse.Errors.SUCCESS == reply_msg.err:
        total_cpus = reply_msg.total_cpus

    return total_cpus


def get_list() -> list[int]:
    """Asks Global Services for a list of the h_uids of all nodes.

    :return: list of the  h_uids of all currently existing memory pools
    """
    req_msg = GSNodeList(tag=next_tag(), p_uid=this_process.my_puid, r_c_uid=get_gs_ret_cuid())

    reply_msg = gs_request(req_msg)
    assert isinstance(reply_msg, GSNodeListResponse)

    return reply_msg.hlist


# def discover():
# def add():
# def remove():
