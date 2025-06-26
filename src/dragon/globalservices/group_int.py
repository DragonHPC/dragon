"""Global services' internal Group context.."""

from collections import defaultdict
from typing import Dict, List

from .process_int import ProcessContext
from ..infrastructure import group_desc as group_desc
from ..infrastructure import process_desc as process_desc
from ..infrastructure import facts as dfacts
from ..infrastructure import messages as dmsg
from ..infrastructure.parameters import this_process
from ..infrastructure.policy import Policy
from ..infrastructure import util as dutil
from ..globalservices import api_setup as das
from .group import GroupError

import os
import logging
import copy
import signal

LOG = logging.getLogger("GS.group:")


class PMIJobHelper:
    """
    THe PMIJobHelper class calculates the per-process PMI information required
    to start each PMI rank.
    """

    _FIRST_PMI_JOB_ID = 1
    _NEXT_PMI_JOB_ID = _FIRST_PMI_JOB_ID

    _PMI_PID_DICT = {}  # key: hostname; value: list of tuples representing (base, size) allocations

    def __init__(self, items, layout_list):
        self._index = 0
        self.items = items
        self.layout_list = layout_list

        # calculated values
        self.job_id = self.get_next_pmi_job_id()
        self.nranks = self.get_nranks()

        (
            self.nid_list,
            self.host_list,
            self.ppn_map,
            self.lrank_list,
        ) = self.get_nid_and_host_data()
        self.pmi_h_uid_list = list(self.ppn_map.keys())

        self.pmi_nnodes = len(self.host_list)

        self.control_port = dfacts.DEFAULT_PMI_CONTROL_PORT + self.job_id
        self.pid_base_map = {}  # key h_uid, value base value
        for h_uid, lranks in self.ppn_map.items():
            self.pid_base_map[h_uid] = PMIJobHelper.allocate_pmi_pid_base(h_uid, lranks)

    @classmethod
    def is_pmi_required(cls, items: list[tuple]):
        for _, res_msg in items:
            resource_msg = dmsg.parse(res_msg)
            if isinstance(resource_msg, dmsg.GSProcessCreate) and resource_msg.pmi_required:
                return True
        return False

    @classmethod
    def get_next_pmi_job_id(cls):
        job_id = cls._NEXT_PMI_JOB_ID
        cls._NEXT_PMI_JOB_ID += 1
        return job_id

    @classmethod
    def allocate_pmi_pid_base(cls, hostname, lranks):
        if hostname in cls._PMI_PID_DICT:
            allocated_bases = cls._PMI_PID_DICT[hostname]
        else:
            allocated_bases = []
            cls._PMI_PID_DICT[hostname] = allocated_bases

        # reserve 0 for the single HSTA agent. this will not be needed
        # when we begin launching HSTA with PMOD
        min_pid = 1

        # handle the case where pid_bases is empty
        if not allocated_bases:
            allocated_bases.append((min_pid, lranks))
            return min_pid

        # loop over (base, size) pairs looking for a free segment of PIDS
        # with size at least lranks

        # (1) for a given (base, size) pair, check the segment starting at
        # the PID just past the previous segment

        prev_end = min_pid

        for base, size in allocated_bases:
            if base - prev_end >= lranks:
                allocated_bases.append((prev_end, lranks))
                allocated_bases.sort()
                return prev_end
            else:
                prev_end = base + size

        # (2) check the final segment from the end of the last (base, size)
        # pair extending to the maximum PID

        max_pid = 510

        if (max_pid + 1) - prev_end >= lranks:
            allocated_bases.append((prev_end, lranks))
            allocated_bases.sort()
            return prev_end

        return -1

    @classmethod
    def free_pmi_pid_base(cls, hostname, base, size):
        allocated_bases = cls._PMI_PID_DICT[hostname]
        allocated_bases.remove((base, size))

    def cleanup(self):
        LOG.debug("Before cleaning up pid base _PMI_PID_DICT=%s", str(PMIJobHelper._PMI_PID_DICT))
        for host_id, count in self.ppn_map.items():
            PMIJobHelper.free_pmi_pid_base(host_id, self.pid_base_map[host_id], count)
        LOG.debug("After cleaning up pid base _PMI_PID_DICT=%s", str(PMIJobHelper._PMI_PID_DICT))

    def get_nranks(self):
        total_ranks = 0
        for count, res_msg in self.items:
            resource_msg = dmsg.parse(res_msg)
            if isinstance(resource_msg, dmsg.GSProcessCreate) and resource_msg.pmi_required:
                total_ranks += count
        return total_ranks

    def get_nid_and_host_data(self):
        """
        Determine which of the resource items requires PMI. For those resources
        that do require PMI, determine number of PMI processes assigned to each h_uid.
        """
        layout_index = 0
        nid_list = []
        host_list = []
        ppn_map = {}
        lrank_list = []
        for count, res_msg in self.items:
            resource_msg = dmsg.parse(res_msg)
            if isinstance(resource_msg, dmsg.GSProcessCreate) and resource_msg.pmi_required:
                for _ in range(count):
                    h_uid = self.layout_list[layout_index].h_uid
                    lrank = ppn_map.get(h_uid, 0)
                    lrank_list.append(lrank)

                    if h_uid not in ppn_map:
                        # add this host to the host list
                        host_list.append(self.layout_list[layout_index].host_name)

                    ppn_map[h_uid] = lrank + 1
                    nid_list.append(list(ppn_map.keys()).index(h_uid))
                    layout_index += 1
            else:
                layout_index += count

        return nid_list, host_list, ppn_map, lrank_list

    def get_host_id_from_index(self, rank):
        layout_index = 0
        for count, res_msg in self.items:
            resource_msg = dmsg.parse(res_msg)
            if isinstance(resource_msg, dmsg.GSProcessCreate) and resource_msg.pmi_required:
                if rank >= count:
                    rank -= count
                    layout_index += count
                else:
                    return self.layout_list[layout_index + rank].h_uid
            else:
                layout_index += count

    def __iter__(self):
        return self

    def __next__(self):
        if self._index < self.nranks:
            lrank = self.lrank_list[self._index]
            h_uid = self.get_host_id_from_index(self._index)
            pmi_info = dmsg.PMIProcessInfo(
                lrank=lrank,
                ppn=self.ppn_map[h_uid],
                nid=self.pmi_h_uid_list.index(h_uid),
                pid_base=self.pid_base_map[h_uid],
            )
            self._index += 1
            return pmi_info
        else:
            raise StopIteration


class GroupContext:
    """Everything to do with a single group of resources in global services.

    This object manages all the transactions to a shepherd concerning
    the lifecycle of a group.
    """

    def __init__(self, server, request, reply_channel, g_uid, policy, pmi_job_helper):
        """_summary_

        :param server: _description_
        :type server: _type_
        :param request: _description_
        :type request: _type_
        :param reply_channel: _description_
        :type reply_channel: _type_
        :param g_uid: _description_
        :type g_uid: _type_
        :param policy: _description_
        :type policy: _type_
        :param pmi_job_id: unique job id for PMI processes
        :type pmi_job_id: int
        """
        self.server = server
        self.request = request
        self.reply_channel = reply_channel
        self.destroy_request = None
        self.pmi_job_helper = pmi_job_helper
        self.destroy_called = False
        self.destroy_remove_success_ids = None  # used when destroy_remove is called to keep the items to be
        # destroyed after having received all the SHProcessKillResponse messages
        self._descriptor = group_desc.GroupDescriptor(
            g_uid=g_uid, name=request.user_name, policy=policy, resilient=server.resilient_groups
        )

    def __str__(self):
        return f"[[{self.__class__.__name__}]] desc:{self.descriptor!r} req:{self.request!r}"

    @property
    def descriptor(self):
        return self._descriptor

    def _update_group_member(self, this_guid, member, lst_idx, item_idx, related_to_create=True):
        self.server.group_table[this_guid].descriptor.sets[lst_idx][item_idx] = (
            group_desc.GroupDescriptor.GroupMember.from_sdict(member)
        )
        # update the count that measures the number of responses we have received when we're creating the resources
        if related_to_create:
            if self.server.group_resource_count[this_guid][lst_idx] >= 1:
                self.server.group_resource_count[this_guid][lst_idx] -= 1

    def _generate_member(self, guid, proc_context, err_msg_type=None, channel_related=False):
        if err_msg_type:
            if channel_related:
                response_msg = f"Failed to create {err_msg_type} channel for {proc_context.descriptor.p_uid} that belongs to group {guid}."
            else:
                response_msg = f"Process {proc_context.descriptor.p_uid} that belongs to group {guid} failed with error: {err_msg_type}."
            error_code = dmsg.GSProcessCreateResponse.Errors.FAIL
        else:
            response_msg = f"Process {proc_context.descriptor.p_uid} that belongs to group {guid} successfully created."
            error_code = dmsg.GSProcessCreateResponse.Errors.SUCCESS

        LOG.info(response_msg)

        member = {
            "state": proc_context.descriptor.state,
            "uid": proc_context.descriptor.p_uid,
            "placement": proc_context.descriptor.node,
            "desc": proc_context.descriptor,
            "error_code": error_code,
            "error_info": response_msg,
        }

        return member

    def _remove_proc_from_group(self, puid, lst_idx, item_idx):
        # assign a None value to act as a placeholder temporarily
        self.descriptor.sets[lst_idx][item_idx] = None
        del self.server.resource_to_group_map[puid]

    def _update_ds_after_removals(self):
        g_uid = self.descriptor.g_uid
        # first, clean up descriptor.sets from None values
        for lst_idx, lst in enumerate(self.descriptor.sets):
            temp = [i for i in lst if i is not None]
            if temp:
                self.descriptor.sets[lst_idx] = temp
            else:
                # avoid keeping empty lists
                del self.descriptor.sets[lst_idx]

        # next, update the helper DS
        self.server.group_resource_count[g_uid] = []
        for lst_idx, lst in enumerate(self.descriptor.sets):
            self.server.group_resource_count[g_uid].append(0)
            for item_idx, item in enumerate(lst):
                self.server.resource_to_group_map[item.uid] = (g_uid, (lst_idx, item_idx))

    def _check_if_item_in_group(self, identifier, target_guid, failed_ids, success_ids):
        if isinstance(identifier, str):
            puid, found, _ = self.server.resolve_puid(name=identifier)
        else:
            puid, found, _ = self.server.resolve_puid(p_uid=identifier)

        if not found:
            # not a valid process
            failed_ids.append(identifier)
        else:
            if puid in self.server.resource_to_group_map:
                guid, (lst_idx, item_idx) = self.server.resource_to_group_map[puid]
                if guid != target_guid:
                    LOG.info(
                        f"Error: the target_guid {target_guid} was not the same as the guid {guid} that process {puid} belongs to."
                    )
                    failed_ids.append(identifier)
                else:
                    success_ids.append((puid, lst_idx, item_idx))
            else:
                # already dead or not a member of the group
                failed_ids.append(identifier)

    @staticmethod
    def _check_resource_msg_type(msg: dmsg.InfraMsg):
        if not isinstance(msg, (dmsg.GSChannelCreate, dmsg.GSProcessCreate, dmsg.GSPoolCreate)):
            raise GroupError("An invalid message type was provided.")

    @classmethod
    def construct(cls, server, msg, reply_channel):
        """Starts the resources that belong to the requested group.
        It also creates a GroupContext and registers it with the server.

        :param server: global server context object
        :type server: dragon.globalservices.server.GlobalContext
        :param msg: GSGroupCreate message
        :type msg: dragon.infrastructure.messages.GSGroupCreate
        :param reply_channel: the handle needed to reply to this message
        :type reply_channel: dragon.infrastructure.connection.Connection
        :raises NotImplementedError: when a channel is requested as a member resource
        :raises NotImplementedError: when a memory pool is requested as a member resource
        :raises GroupError: when the message for the requested resources is of unknown type
        :raises GroupError: if a replication factor `n` of any tuple is less than 1
        :return: True if there is a pending continuation, False otherwise
        :rtype: bool
        """
        if msg.user_name in server.group_names:
            LOG.info(f"group name {msg.user_name} in use")
            existing_ctx = server.group_table[server.group_names[msg.user_name]]
            rm = dmsg.GSGroupCreateResponse(tag=server.tag_inc(), ref=msg.tag, desc=existing_ctx.descriptor)
            reply_channel.send(rm.serialize())
            return False

        this_guid, auto_name = server.new_guid_and_default_name()

        if not msg.user_name:
            msg.user_name = auto_name

        if msg.policy is not None:
            assert isinstance(msg.policy, Policy)
            group_policy = Policy.merge(Policy.global_policy(), msg.policy)
        else:
            group_policy = Policy.global_policy()

        policies = []
        for replicas, serialized_msg in msg.items:
            resource_msg = dmsg.parse(serialized_msg)
            cls._check_resource_msg_type(resource_msg)

            policy = resource_msg.policy

            if policy is not None:
                assert isinstance(policy, Policy)
                policies.extend([Policy.merge(group_policy, policy)] * replicas)
            else:
                policies.extend([group_policy] * replicas)

        LOG.debug("group_policy=%s", group_policy)
        LOG.debug("policies=%s", policies)

        # Gather our node list and evaluate the policies against it
        layout_list = server.policy_eval.evaluate(policies=policies)
        if int(os.environ.get("PMI_DEBUG", 0)):
            LOG.info("layout_list=%s", layout_list)

        pmi_job_helper = None
        if PMIJobHelper.is_pmi_required(msg.items):
            # Setup PMI Helper
            pmi_job_helper = PMIJobHelper(msg.items, layout_list)
            pmi_job_iter = iter(pmi_job_helper)

        group_context = cls(
            server=server,
            request=msg,
            reply_channel=reply_channel,
            g_uid=this_guid,
            policy=group_policy,
            pmi_job_helper=pmi_job_helper,
        )

        server.group_names[msg.user_name] = this_guid
        server.group_table[this_guid] = group_context
        server.group_resource_count[this_guid] = (
            []
        )  # list of items corresponding to the multiplicity of each list in server.group_resource_list

        # Maps a given node (local services instance) to a list of
        # ProcessContexts that are to be created on that instance
        ls_proccontext_map: Dict[int, List[ProcessContext]] = defaultdict(list)

        # msg.items is a list of tuples of the form (count:int, create_msg: dragon.infrastructure.messages.Message)
        layout_index = 0
        for tuple_idx, item in enumerate(msg.items):
            count, res_msg = item
            resource_msg = dmsg.parse(res_msg)

            if count > 0:
                # initialize a new list
                group_context.descriptor.sets.append([])
                # add a new entry to server.group_resource_count[this_guid] list
                server.group_resource_count[this_guid].append(int(count))
                for item_idx in range(count):
                    item_layout = layout_list[layout_index]
                    item_policy = policies[layout_index]
                    layout_index += 1

                    # we need a unique copy of the msg for each member
                    resource_copy = copy.deepcopy(resource_msg)
                    resource_copy.tag = das.next_tag()

                    # create a unique name for this resource based on the tuple user_name
                    if resource_copy.user_name:
                        resource_copy.user_name = f"{resource_copy.user_name}.{this_guid}.{tuple_idx}.{item_idx}"

                    # update the layout to place the process correctly
                    resource_copy.layout = item_layout
                    resource_copy.policy = item_policy

                    # add a new resource into this list
                    group_context.descriptor.sets[tuple_idx] += [group_desc.GroupDescriptor.GroupMember()]

                    # this is where we're actually starting the creation of the group members
                    if isinstance(resource_msg, dmsg.GSProcessCreate):
                        if resource_msg.pmi_required:
                            # The PMIJobHelper generates the pmi_info structure
                            # from the layout_map and list of group items.
                            resource_copy.pmi_required = True
                            resource_copy._pmi_info = next(pmi_job_iter)
                            if int(os.environ.get("PMI_DEBUG", 0)):
                                LOG.info("%s", resource_copy._pmi_info)

                        success, outbound_tag, proc_context = ProcessContext.construct(
                            server, resource_copy, reply_channel, send_msg=False, belongs_to_group=True
                        )

                        server.resource_to_group_map[proc_context.descriptor.p_uid] = (this_guid, (tuple_idx, item_idx))

                        if success and outbound_tag and proc_context:
                            ls_proccontext_map[proc_context.node].append(proc_context)
                            server.pending[outbound_tag] = group_context.complete_construction
                            server.group_to_pending_resource_map[(outbound_tag, this_guid)] = proc_context

                            # Update the group with the corresponding GroupMember
                            member = group_context._generate_member(this_guid, proc_context)
                            group_context._update_group_member(
                                this_guid, member, tuple_idx, item_idx, related_to_create=False
                            )
                        else:
                            if proc_context and outbound_tag == "already":  # the process was already created
                                LOG.debug(f"The process {proc_context.descriptor.p_uid} was already created.")
                                member = {
                                    "state": proc_context.descriptor.state,
                                    "uid": proc_context.descriptor.p_uid,
                                    "placement": proc_context.descriptor.node,
                                    "error_code": dmsg.GSProcessCreateResponse.Errors.ALREADY,
                                    "desc": proc_context.descriptor,
                                }
                                # Update the group with the corresponding GroupMember
                                group_context._update_group_member(this_guid, member, tuple_idx, item_idx)
                            elif proc_context and not outbound_tag:
                                # this is the case where we needed to make inf channels and
                                # there is a pending request related to the process channels
                                # and we don't need to do anything for now. When the channel related pending will
                                # be cleared, a group pending construct_completion will be issued from process_int
                                continue

                            else:  # there was a failure
                                # in this case, outbound_tag contains the error message
                                LOG.debug(
                                    f"There was a failure in the process {proc_context.descriptor.p_uid} creation: {outbound_tag}"
                                )
                                member = group_context._generate_member(this_guid, proc_context, outbound_tag)
                                # Update the group with the corresponding GroupMember
                                group_context._update_group_member(this_guid, member, tuple_idx, item_idx)

                    elif isinstance(resource_msg, dmsg.GSChannelCreate):
                        raise NotImplementedError
                        # ChannelContext.construct(server, msg, reply_channel,
                        #                          belongs_to_group=True)
                    elif isinstance(resource_msg, dmsg.GSPoolCreate):
                        raise NotImplementedError
                        # PoolContext.construct(server, msg, reply_channel,
                        #                       belongs_to_group=True)
                    else:
                        raise GroupError(f"Unknown msg type {resource_msg} for a Group member.")
            else:
                raise GroupError("The Group should include at least one member in each subgroup.")

        # If PMI is required, we need to send these common PMI options.
        # By sending them as part of the SHMultiProcessCreate message,
        # we limit the duplication of these common vaules in each embedded
        # SHProcessCreate message, reducing the overall message size.
        pmi_group_info: dmsg.PMIGroupInfo = None
        if pmi_job_helper:
            pmi_group_info: dmsg.PMIGroupInfo = dmsg.PMIGroupInfo(
                job_id=pmi_job_helper.job_id,
                nnodes=pmi_job_helper.pmi_nnodes,
                nranks=pmi_job_helper.nranks,
                nidlist=pmi_job_helper.nid_list,
                hostlist=pmi_job_helper.host_list,
                control_port=pmi_job_helper.control_port,
            )

        for node, contexts in ls_proccontext_map.items():
            procs = [context.shprocesscreate_msg for context in contexts]
            shep_req = dmsg.SHMultiProcessCreate(
                tag=server.tag_inc(), r_c_uid=dfacts.GS_INPUT_CUID, pmi_group_info=pmi_group_info, procs=procs
            )
            shep_hdl = server.shep_inputs[node]
            server.pending_sends.put((shep_hdl, shep_req.serialize()))
            LOG.debug(f"request %s to shep %d", shep_req, node)

        return True

    def _construction_helper(self, msg):
        # get the resource context corresponding to this response message
        if (msg.ref, self.descriptor.g_uid) in self.server.group_to_pending_resource_map:
            proc_context = self.server.group_to_pending_resource_map.pop((msg.ref, self.descriptor.g_uid))
        else:
            raise GroupError("Error with group pending resource.")

        # this p_uid should exist in server.resource_to_group_map dict
        this_puid = proc_context.descriptor.p_uid
        guid, (lst_idx, item_idx) = self.server.resource_to_group_map[this_puid]

        if isinstance(msg, dmsg.GSProcessCreateResponse) and msg.err == dmsg.GSProcessCreateResponse.Errors.FAIL:
            # in this case, there was a failure related to the creation of the process channel
            # and _construction_helper() was called from process_int
            # So, we just update the group and we're done with this process
            LOG.debug(f"There was a failure in the process {proc_context.descriptor.p_uid} creation: {msg.err_info}")
            member = self._generate_member(guid, proc_context, msg.err_info)
            self._update_group_member(guid, member, lst_idx, item_idx)
            return

        # complete the construction of this resource without sending any response messages to the client
        succeeded = proc_context.complete_construction(msg, send_msg=False)

        if dmsg.SHProcessCreateResponse.Errors.SUCCESS == msg.err:
            # this process is successfully created
            member = self._generate_member(guid, proc_context, None)

            # in this case, something went wrong with the creation of stdin/stdout/stderr channels
            if not succeeded:
                member = self._generate_member(guid, proc_context, "stdin/stdout/stderr", channel_related=True)

        elif dmsg.SHProcessCreateResponse.Errors.FAIL == msg.err:
            member = self._generate_member(guid, proc_context, msg.err_info)

            del self.server.resource_to_group_map[this_puid]

        else:
            raise RuntimeError(f"got {msg!s} err {msg.err} unknown")

        # Before updating, make sure GS hasn't already marked this process as dead via out-of-order
        # exit of the proc
        cur_state = self.server.group_table[guid].descriptor.sets[lst_idx][item_idx].state
        if cur_state is process_desc.ProcessDescriptor.State.DEAD:
            LOG.debug(f'guid: {guid}, puid {member["uid"]} was already dead prior to create response')
            member["state"] = cur_state
            member["desc"].state = cur_state

        # Update the group with the corresponding GroupMember
        self._update_group_member(guid, member, lst_idx, item_idx)

        return succeeded, guid, lst_idx

    def complete_construction(self, msg):
        """Completes construction of a new managed group.

        :param msg: SHProcessCreateResponse message
        :type msg: dragon.infrastructure.messages.SHProcessCreateResponse
        :raises GroupError: when the context corresponding to the pending resource cannot be found
        :raises RuntimeError: when the message error is of unknown type
        :return: True or False according to success
        :rtype: bool
        """
        LOG.info("group_complete_construction")

        # TODO: implement for other types of resources

        succeeded, guid, lst_idx = self._construction_helper(msg)

        # if we have received responses for all the members of this list
        if self.server.group_resource_count[guid][lst_idx] == 0:
            # next, check if we have responses for the other lists as well
            if all(item == 0 for item in self.server.group_resource_count[guid]):
                # Update the group descriptor state to active
                self.descriptor.state = group_desc.GroupDescriptor.State.ACTIVE

                # now we can send the GSGroupCreateResponse msg back to the client
                response = dmsg.GSGroupCreateResponse(
                    tag=self.server.tag_inc(), ref=self.request.tag, desc=self.descriptor
                )
                self.reply_channel.send(response.serialize())
                LOG.debug(f"create response sent, tag {response.tag} ref {response.ref} pending cleared")

        # since this server.pending entry was related to a particular outbound tag
        # and a specific process creation, return the value returned from
        # proc_context.complete_construction
        return succeeded

    @staticmethod
    def add(server, msg, reply_channel):
        """Action to add existing resources to a group.

        :param server: global server context object
        :type server: dragon.globalservices.server.GlobalContext
        :param msg: addTo request message
        :type msg: dragon.infrastructure.messages.GSGroupAddTo
        :param reply_channel: the handle needed to reply to this message
        :type reply_channel: dragon.infrastructure.connection.Connection
        :raises GroupError: if the list of items to add is empty
        :raises NotImplementedError: when the group's state is unknown, i.e., other than ACTIVE, DEAD, PENDING
        :return: True if a successful addTo response message was issued and False otherwise
        :rtype: bool
        """

        if len(msg.items) == 0 or msg.items is None:
            raise GroupError("There should be at least one member to add to the group.")

        target_uid, found, errmsg = server.resolve_guid(msg.user_name, msg.g_uid)
        gsgar = dmsg.GSGroupAddToResponse

        succeeded = False

        if not found:
            rm = gsgar(tag=server.tag_inc(), ref=msg.tag, err=gsgar.Errors.UNKNOWN, err_info=errmsg)
            reply_channel.send(rm.serialize())
            LOG.debug(f"unknown group of resources: response to {msg}: {rm}")
            return succeeded
        else:
            groupctx = server.group_table[target_uid]
            groupdesc = groupctx.descriptor
            gds = group_desc.GroupDescriptor.State

            if gds.DEAD == groupdesc.state:
                rm = gsgar(tag=server.tag_inc(), ref=msg.tag, err=gsgar.Errors.DEAD)
                reply_channel.send(rm.serialize())
                LOG.debug(f"group dead, response to {msg}: {rm}")
                return succeeded

            # this is a highly unlikely case to happen
            # because the user must have received a create response back
            # before requesting adding to this group, which means that the
            # state of the group is not pending anymore
            # so, what we really want here is to log this is happening and move on
            elif gds.PENDING == groupdesc.state:
                rm = gsgar(
                    tag=server.tag_inc(),
                    ref=msg.tag,
                    err=gsgar.Errors.PENDING,
                    err_info=f"group {target_uid} is pending",
                )
                reply_channel.send(rm.serialize())
                LOG.debug(
                    f"group pending while add_to request -- this should not be happening, response to {msg}: {rm}"
                )
                return succeeded
            elif gds.ACTIVE == groupdesc.state:
                # update the group's state to PENDING as long as the addTo is pending
                groupdesc.state = gds.PENDING

                groupctx.request = msg
                groupctx.reply_channel = reply_channel
                existing_lists = len(groupdesc.sets)

                failed_ids = []  # keep potential resources that were not found or were dead
                for i, identifier in enumerate(msg.items):
                    if isinstance(identifier, str):
                        p_uid, found, errmsg = server.resolve_puid(name=identifier)
                    else:
                        p_uid, found, errmsg = server.resolve_puid(p_uid=int(identifier))

                    if not found:
                        failed_ids.append(identifier)
                    else:
                        # create a single list in groupdesc.sets for each resource we add
                        lst_idx = i + existing_lists
                        item_idx = 0  # each list will have a single item
                        groupdesc.sets.append([])

                        pctx = server.process_table[p_uid]
                        pdesc = pctx.descriptor
                        pds = process_desc.ProcessDescriptor.State
                        if pdesc.state == pds.DEAD:
                            failed_ids.append(identifier)
                        else:
                            # add each item to a single list in the groupdesc.sets list
                            groupdesc.sets[lst_idx] += [group_desc.GroupDescriptor.GroupMember()]
                            server.resource_to_group_map[p_uid] = (target_uid, (lst_idx, item_idx))
                            member = groupctx._generate_member(target_uid, pctx)
                            groupctx._update_group_member(
                                target_uid, member, lst_idx, item_idx, related_to_create=False
                            )

                # if any of the items to be added were not found or were dead
                # we return a FAIL response message to the client
                if failed_ids:
                    errmsg = f"The items {failed_ids} were not found or already dead."
                    response = gsgar(tag=server.tag_inc(), ref=msg.tag, err=gsgar.Errors.FAIL, err_info=errmsg)
                else:
                    response = gsgar(tag=server.tag_inc(), ref=msg.tag, err=gsgar.Errors.SUCCESS, desc=groupdesc)
                    succeeded = True

                groupdesc.state = gds.ACTIVE
                reply_channel.send(response.serialize())
                LOG.debug(f"addition response sent, tag {response.tag} ref {response.ref} pending cleared")

                return succeeded

            else:
                raise NotImplementedError("close case")

    @staticmethod
    def create_add(server, msg, reply_channel):
        """Action to first create resources and then add them to an existing group.

        :param server: global server context object
        :type server: dragon.globalservices.server.GlobalContext
        :param msg: addTo request message
        :type msg: dragon.infrastructure.messages.GSGroupCreateAddTo
        :param reply_channel: the handle needed to reply to this message
        :type reply_channel: dragon.infrastructure.connection.Connection
        :raises GroupError: when the message for the requested resources is of unknown type
        :raises GroupError: if a replication factor `n` of any tuple is less than 1
        :raises NotImplementedError: when the group's state is unknown, i.e., other than ACTIVE, DEAD, PENDING
        :return: True if a successful addTo message was issued and False otherwise
        :rtype: bool
        """

        target_uid, found, errmsg = server.resolve_guid(msg.user_name, msg.g_uid)
        gsgacr = dmsg.GSGroupCreateAddToResponse

        if not found:
            rm = gsgacr(tag=server.tag_inc(), ref=msg.tag, err=gsgacr.Errors.UNKNOWN, err_info=errmsg)
            reply_channel.send(rm.serialize())
            LOG.debug(f"unknown group of resources: response to {msg}: {rm}")
            return False
        else:
            groupctx = server.group_table[target_uid]
            groupdesc = groupctx.descriptor
            gds = group_desc.GroupDescriptor.State

            if gds.DEAD == groupdesc.state:
                rm = gsgacr(tag=server.tag_inc(), ref=msg.tag, err=gsgacr.Errors.DEAD)
                reply_channel.send(rm.serialize())
                LOG.debug(f"group dead, response to {msg}: {rm}")
                return False

            # this is a highly unlikely case to happen
            # because the user must have received a create response back
            # before requesting adding to this group, which means that the
            # state of the group is not pending anymore
            # so, what we really want here is to log this is happening and move on
            elif gds.PENDING == groupdesc.state:
                rm = gsgacr(
                    tag=server.tag_inc(),
                    ref=msg.tag,
                    err=gsgacr.Errors.PENDING,
                    err_info=f"group {target_uid} is pending",
                )
                reply_channel.send(rm.serialize())
                LOG.debug(
                    f"group pending while create_add request -- this should not be happening, response to {msg}: {rm}"
                )
                return False
            elif gds.ACTIVE == groupdesc.state:
                # update the group's state to PENDING as long as the addTo is pending
                groupdesc.state = gds.PENDING

                groupctx.request = msg
                groupctx.reply_channel = reply_channel

                if msg.policy:
                    assert isinstance(msg.policy, Policy)
                    policy = Policy.merge(Policy.global_policy(), msg.policy)
                else:
                    policy = groupdesc.policy

                # Policy Evaluator wants a list of policies, one for each member
                total_members = sum([n for n, _ in msg.items])
                policies = [policy] * total_members

                # Gather our node list and evaluate the policies against it
                layout_list = server.policy_eval.evaluate(policies=policies)
                if int(os.environ.get("PMI_DEBUG", 0)):
                    LOG.debug("layout_list=%s", layout_list)

                # we need the number of existing lists in the group
                existing_lists = len(groupdesc.sets)

                # Maps a given node (local services instance) to a list of
                # ProcessContexts that are to be created on that instance
                ls_proccontext_map: Dict[int, List[ProcessContext]] = defaultdict(list)

                # msg.items is a list of tuples of the form (count:int, create_msg: dragon.infrastructure.messages.Message)
                layout_index = 0
                for tuple_idx, item in enumerate(msg.items):
                    count, res_msg = item
                    resource_msg = dmsg.parse(res_msg)

                    LOG.debug(f"Creating msg {resource_msg}")
                    if count > 0:
                        # initialize a new list
                        groupdesc.sets.append([])
                        # add a new entry to server.group_resource_count[target_uid] list
                        server.group_resource_count[target_uid].append(int(count))
                        for item_idx in range(count):
                            item_layout = layout_list[layout_index]
                            layout_index += 1

                            # we need a unique copy of the msg for each member
                            resource_copy = copy.deepcopy(resource_msg)
                            resource_copy.tag = das.next_tag()

                            # create a unique name for this resource based on the tuple user_name
                            if resource_copy.user_name:
                                resource_copy.user_name = (
                                    f"{resource_copy.user_name}.{target_uid}.{tuple_idx+existing_lists}.{item_idx}"
                                )

                            # update the layout to place the process correctly
                            resource_copy.layout = item_layout

                            # add a new resource into this list
                            groupdesc.sets[tuple_idx + existing_lists] += [group_desc.GroupDescriptor.GroupMember()]

                            # this is where we're actually starting the creation of the group members
                            if isinstance(resource_msg, dmsg.GSProcessCreate):
                                success, outbound_tag, proc_context = ProcessContext.construct(
                                    server,
                                    resource_copy,
                                    reply_channel,
                                    send_msg=False,
                                    belongs_to_group=True,
                                    addition=True,
                                )

                                server.resource_to_group_map[proc_context.descriptor.p_uid] = (
                                    target_uid,
                                    (tuple_idx + existing_lists, item_idx),
                                )

                                if success and outbound_tag and proc_context:
                                    ls_proccontext_map[proc_context.node].append(proc_context)
                                    server.pending[outbound_tag] = groupctx.complete_addition
                                    server.group_to_pending_resource_map[(outbound_tag, target_uid)] = proc_context
                                else:
                                    if proc_context and outbound_tag == "already":  # the process was already created
                                        member = {
                                            "state": proc_context.descriptor.state,
                                            "uid": proc_context.descriptor.p_uid,
                                            "placement": proc_context.descriptor.node,
                                            "error_code": dmsg.GSProcessCreateResponse.Errors.ALREADY,
                                            "desc": proc_context.descriptor,
                                        }
                                        # Update the group with the corresponding GroupMember
                                        groupctx._update_group_member(
                                            target_uid, member, tuple_idx + existing_lists, item_idx
                                        )
                                    elif proc_context and not outbound_tag:
                                        # this is the case where we needed to make inf channels and
                                        # there is a pending request related to the process channels
                                        # and we don't need to do anything for now. When the channel related pending will
                                        # be cleared, a group pending construct_completion will be issued from process_int
                                        continue
                                    else:  # there was a failure
                                        LOG.debug(
                                            f"There was a failure in the process {proc_context.descriptor.p_uid} creation: {outbound_tag}"
                                        )
                                        member = groupctx._generate_member(target_uid, proc_context, outbound_tag)
                                        # Update the group with the corresponding GroupMember
                                        groupctx._update_group_member(
                                            target_uid, member, tuple_idx + existing_lists, item_idx
                                        )

                            elif isinstance(resource_msg, dmsg.GSChannelCreate):
                                raise NotImplementedError
                                # ChannelContext.construct(server, msg, reply_channel,
                                #                          belongs_to_group=True)
                            elif isinstance(resource_msg, dmsg.GSPoolCreate):
                                raise NotImplementedError
                                # PoolContext.construct(server, msg, reply_channel,
                                #                       belongs_to_group=True)
                            else:
                                raise GroupError(f"Unknown msg type {resource_msg} for a Group member.")
                    else:
                        raise GroupError("The Group should include at least one member in each subgroup.")

                for node, contexts in ls_proccontext_map.items():
                    procs = [context.shprocesscreate_msg for context in contexts]
                    shep_req = dmsg.SHMultiProcessCreate(
                        tag=server.tag_inc(), r_c_uid=dfacts.GS_INPUT_CUID, procs=procs
                    )
                    shep_hdl = server.shep_inputs[node]
                    server.pending_sends.put((shep_hdl, shep_req.serialize()))
                    LOG.debug(f"request %s to shep %d", shep_req, node)

            else:
                raise NotImplementedError("close case")

    def complete_addition(self, msg):
        """Completes the addition of new resources to a managed group.

        :param msg: SHProcessCreateResponse message
        :type msg: dragon.infrastructure.messages.SHProcessCreateResponse
        :raises GroupError: when the context corresponding to the pending resource cannot be found
        :raises RuntimeError: when the message error is of unknown type
        :return: True or False according to success
        :rtype: bool
        """

        LOG.info("group_complete_addition")

        # TODO: implement for other types of resources

        succeeded, guid, lst_idx = self._construction_helper(msg)

        # if we have received responses for all the members of this list
        if self.server.group_resource_count[guid][lst_idx] == 0:
            # next, check if we have responses for the other lists as well
            if all(item == 0 for item in self.server.group_resource_count[guid]):
                # Update the group descriptor state to active
                self.descriptor.state = group_desc.GroupDescriptor.State.ACTIVE

                # now we can send the GSGroupCreateAddToResponse msg back to the client
                response = dmsg.GSGroupCreateAddToResponse(
                    tag=self.server.tag_inc(),
                    ref=self.request.tag,
                    err=dmsg.GSGroupCreateAddToResponse.Errors.SUCCESS,
                    desc=self.descriptor,
                )
                self.reply_channel.send(response.serialize())
                LOG.debug(f"addition response sent, tag {response.tag} ref {response.ref} pending cleared")

        # since this server.pending entry was related to a particular outbound tag
        # and a specific process creation, return the value returned from
        # proc_context.complete_construction
        return succeeded

    @staticmethod
    def remove(server, msg, reply_channel):
        """Action to remove resources from a group.
        If there is at least one resource that is not found in the group, then we do not proceed with
        the removal request and we return a FAIL response message back to the client.

        :param server: global server context object
        :type server: dragon.globalservices.server.GlobalContext
        :param msg: removeFrom request message
        :type msg: dragon.infrastructure.messages.GSGroupRemoveFrom
        :param reply_channel: the handle needed to reply to this message
        :type reply_channel: dragon.infrastructure.connection.Connection
        :raises GroupError: if the list of items to remove is empty
        :raises NotImplementedError: when the group's state is unknown, i.e., other than ACTIVE, DEAD, PENDING
        :return: True if a successful removeFrom response message was issued and False otherwise
        :rtype: bool
        """

        if len(msg.items) == 0 or msg.items is None:
            raise GroupError("There should be at least one member to delete from the group.")

        target_uid, found, errmsg = server.resolve_guid(msg.user_name, msg.g_uid)
        gsgrr = dmsg.GSGroupRemoveFromResponse

        succeeded = False

        if not found:
            rm = gsgrr(tag=server.tag_inc(), ref=msg.tag, err=gsgrr.Errors.UNKNOWN, err_info=errmsg)
            reply_channel.send(rm.serialize())
            LOG.debug(f"unknown group of resources: response to {msg}: {rm}")
            return succeeded
        else:
            groupctx = server.group_table[target_uid]
            groupdesc = groupctx.descriptor
            gds = group_desc.GroupDescriptor.State

            if gds.DEAD == groupdesc.state:
                rm = gsgrr(tag=server.tag_inc(), ref=msg.tag, err=gsgrr.Errors.DEAD)
                reply_channel.send(rm.serialize())
                LOG.debug(f"group dead, response to {msg}: {rm}")
                return succeeded

            # this is a highly unlikely case to happen
            # because the user must have received a create response back
            # before requesting adding to this group, which means that the
            # state of the group is not pending anymore
            # so, what we really want here is to log this is happening and move on
            elif gds.PENDING == groupdesc.state:
                rm = gsgrr(
                    tag=server.tag_inc(),
                    ref=msg.tag,
                    err=gsgrr.Errors.PENDING,
                    err_info=f"group {target_uid} is pending",
                )
                reply_channel.send(rm.serialize())
                LOG.debug(
                    f"group pending while remove request -- this should not be happening, response to {msg}: {rm}"
                )
                return succeeded
            elif gds.ACTIVE == groupdesc.state:
                # update the group's state to PENDING as long as the removeFrom is pending
                groupdesc.state = gds.PENDING

                groupctx.request = msg
                groupctx.reply_channel = reply_channel

                failed_ids = []  # keep potential resources that were not found to be members of the group
                success_ids = []
                # TODO: take into account other types of resources except for processes
                for identifier in msg.items:
                    groupctx._check_if_item_in_group(identifier, target_uid, failed_ids, success_ids)

                # if any of the items to be removed were not found in the group
                # we return a FAIL response message to the client and do not remove any items from the group
                if failed_ids:
                    errmsg = f"The items with ids {failed_ids} are not members of the group. The removal of items from group failed."
                    response = gsgrr(tag=server.tag_inc(), ref=msg.tag, err=gsgrr.Errors.FAIL, err_info=errmsg)
                    del failed_ids
                else:
                    # proceed to removing the items only if all items were found in the group
                    for puid, lst_idx, item_idx in success_ids:
                        groupctx._remove_proc_from_group(puid, lst_idx, item_idx)
                    del success_ids
                    # Update the dict that holds the position of the members of the group
                    groupctx._update_ds_after_removals()

                    response = gsgrr(tag=server.tag_inc(), ref=msg.tag, err=gsgrr.Errors.SUCCESS, desc=groupdesc)
                    succeeded = True

                groupdesc.state = gds.ACTIVE
                reply_channel.send(response.serialize())
                LOG.debug(f"removal response sent, tag {response.tag} ref {response.ref}")

                return succeeded

            else:
                raise NotImplementedError("close case")

    @staticmethod
    def destroy_remove(server, msg, reply_channel):
        """Action to remove resources from a group and also destroy these resources.
        For processes we call process.kill (sends a SIGKILL signal) and for other
        types of resources we call destroy.
        If there is at least one resource that is not found in the group, then we do not proceed with
        the removal/destroy request and we return a FAIL response message back to the client.

        :param server: global server context object
        :type server: dragon.globalservices.server.GlobalContext
        :param msg: DestroyRemoveFrom request message
        :type msg: dragon.infrastructure.messages.GSGroupDestroyRemoveFrom
        :param reply_channel: the handle needed to reply to this message
        :type reply_channel: dragon.infrastructure.connection.Connection
        :raises GroupError: if the list of items to remove is empty
        :raises NotImplementedError: when the group's state is unknown, i.e., other than ACTIVE, DEAD, PENDING
        :return: True if a successful DestroyRemoveFrom response message was issued and False otherwise
        :rtype: bool
        """

        if len(msg.items) == 0 or msg.items is None:
            raise GroupError("There should be at least one member to delete from the group.")

        target_uid, found, errmsg = server.resolve_guid(msg.user_name, msg.g_uid)
        gsgdrr = dmsg.GSGroupDestroyRemoveFromResponse

        succeeded = False

        if not found:
            rm = gsgdrr(tag=server.tag_inc(), ref=msg.tag, err=gsgdrr.Errors.UNKNOWN, err_info=errmsg)
            reply_channel.send(rm.serialize())
            LOG.debug(f"unknown group of resources: response to {msg}: {rm}")
            return succeeded
        else:
            groupctx = server.group_table[target_uid]
            groupdesc = groupctx.descriptor
            gds = group_desc.GroupDescriptor.State

            if gds.DEAD == groupdesc.state:
                rm = gsgdrr(tag=server.tag_inc(), ref=msg.tag, err=gsgdrr.Errors.DEAD)
                reply_channel.send(rm.serialize())
                LOG.debug(f"group dead, response to {msg}: {rm}")
                return succeeded

            # this is a highly unlikely case to happen
            # because the user must have received a create response back
            # before requesting adding to this group, which means that the
            # state of the group is not pending anymore
            # so, what we really want here is to log this is happening and move on
            elif gds.PENDING == groupdesc.state:
                rm = gsgdrr(
                    tag=server.tag_inc(),
                    ref=msg.tag,
                    err=gsgdrr.Errors.PENDING,
                    err_info=f"group {target_uid} is pending",
                )
                reply_channel.send(rm.serialize())
                LOG.debug(
                    f"group pending while destroy_remove request -- this should not be happening, response to {msg}: {rm}"
                )
                return succeeded
            elif gds.ACTIVE == groupdesc.state:
                # update the group's state to PENDING as long as the DestroyRemoveFrom is pending
                groupdesc.state = gds.PENDING

                groupctx.destroy_request = msg
                groupctx.reply_channel = reply_channel

                server.group_destroy_resource_count[target_uid] = []

                failed_ids = []  # keep potential resources that were not found to be members of the group
                groupctx.destroy_remove_success_ids = []
                for identifier in msg.items:
                    groupctx._check_if_item_in_group(
                        identifier, target_uid, failed_ids, groupctx.destroy_remove_success_ids
                    )

                # if any of the items to be removed were not found in the group
                # we return a FAIL response message to the client and do not remove any items from the group
                if failed_ids:
                    errmsg = f"The items with ids {failed_ids} are not members of the group. The removal of items from group failed."
                    response = gsgdrr(tag=server.tag_inc(), ref=msg.tag, err=gsgdrr.Errors.FAIL, err_info=errmsg)
                    del failed_ids
                    groupdesc.state = gds.ACTIVE
                    reply_channel.send(response.serialize())
                    LOG.debug(f"DestroyRemoveFrom response sent, tag {response.tag} ref {response.ref}")
                else:
                    ls_kill_context_map: Dict[int, List[ProcessContext]] = defaultdict(list)

                    # proceed with destroying the resources
                    for puid, lst_idx, item_idx in groupctx.destroy_remove_success_ids:
                        item = groupdesc.sets[lst_idx][item_idx]
                        if item.state == process_desc.ProcessDescriptor.State.DEAD:
                            # the process is already dead, so we just need to remove it from the group
                            groupctx._remove_proc_from_group(puid, lst_idx, item_idx)
                        else:
                            # call kill on the process
                            # this process will be removed from the group after the completion of the pending
                            # SHKillProcess request and this happens in complete_kill()
                            resource_msg = groupctx._mk_gs_proc_kill(item.uid)
                            issued, outbound_tag = ProcessContext.kill(
                                server, resource_msg, dutil.AbsorbingChannel(), send_msg=False
                            )
                            pctx = server.process_table[item.uid]

                            if issued:
                                ls_kill_context_map[pctx.node].append(pctx)

                                # we need to issue a pending operation related to this process
                                groupdesc.sets[lst_idx][item_idx].state = process_desc.ProcessDescriptor.State.PENDING
                                groupdesc.sets[lst_idx][
                                    item_idx
                                ].desc.state = process_desc.ProcessDescriptor.State.PENDING

                                groupctx.destroy_called = True  # make sure we won't send a GSGroupKillResponse msg
                                server.pending[outbound_tag] = groupctx.complete_kill
                                server.group_to_pending_resource_map[(outbound_tag, target_uid)] = pctx
                                server.group_destroy_resource_count[target_uid].append(1)
                            else:
                                # the process is either unknown or dead or pending
                                # update the member's state accordingly
                                groupdesc.sets[lst_idx][item_idx].state = pctx.descriptor.state
                                groupdesc.sets[lst_idx][item_idx].desc.state = pctx.descriptor.state

                    for node, contexts in ls_kill_context_map.items():
                        procs = [context.shep_kill_msg for context in contexts]
                        shep_req = dmsg.SHMultiProcessKill(
                            tag=server.tag_inc(), r_c_uid=dfacts.GS_INPUT_CUID, procs=procs
                        )
                        shep_hdl = server.shep_inputs[node]
                        server.pending_sends.put((shep_hdl, shep_req.serialize()))
                        LOG.debug(f"request %s to shep %d", shep_req, node)

                # in this case, all the processes were already dead or no pending continuation
                # was issued and we need to send a response to the client
                if not server.group_destroy_resource_count[target_uid]:
                    # Update the dict that holds the position of the members of the group
                    groupctx._update_ds_after_removals()

                    response = gsgdrr(tag=server.tag_inc(), ref=msg.tag, err=gsgdrr.Errors.SUCCESS, desc=groupdesc)
                    succeeded = True
                    groupdesc.state = gds.ACTIVE
                    reply_channel.send(response.serialize())
                    LOG.debug(f"destroy_remove response sent, tag {response.tag} ref {response.ref}")
                return succeeded
            else:
                raise NotImplementedError("close case")

    def _mk_gs_proc_kill(self, puid, sig=signal.SIGKILL, hide_stderr=False):
        return dmsg.GSProcessKill(
            tag=das.next_tag(),
            p_uid=this_process.my_puid,
            r_c_uid=dfacts.GS_INPUT_CUID,
            t_p_uid=puid,
            sig=int(sig),
            hide_stderr=hide_stderr,
        )

    @staticmethod
    def kill(server, msg, reply_channel):
        """Action to to send the processes belonging to a specified group
        a specified signal.

        :param server: global server context object
        :type server: dragon.globalservices.server.GlobalContext
        :param msg: kill request message
        :type msg: dragon.infrastructure.messages.GSGroupKill
        :param reply_channel: the handle needed to reply to this message
        :type reply_channel: dragon.infrastructure.connection.Connection
        :raises NotImplementedError: when the group's state is unknown, i.e., other than ACTIVE, DEAD, PENDING
        :return: True if a successful kill message was issued and False otherwise
        :rtype: bool
        """
        target_uid, found, errmsg = server.resolve_guid(msg.user_name, msg.g_uid)
        gsgkr = dmsg.GSGroupKillResponse

        if not found:
            rm = gsgkr(tag=server.tag_inc(), ref=msg.tag, err=gsgkr.Errors.UNKNOWN, err_info=errmsg)
            reply_channel.send(rm.serialize())
            LOG.debug(f"unknown group of resources: response to {msg}: {rm}")
            return False
        else:
            groupctx = server.group_table[target_uid]
            groupdesc = groupctx.descriptor
            gds = group_desc.GroupDescriptor.State

            if gds.DEAD == groupdesc.state:
                rm = gsgkr(tag=server.tag_inc(), ref=msg.tag, err=gsgkr.Errors.DEAD)
                reply_channel.send(rm.serialize())
                LOG.debug(f"group dead, response to {msg}: {rm}")
                return False

            # this is a highly unlikely case to happen
            # because the user must have received a create response back
            # before requesting killing this group, which means that the
            # state of the group is not pending anymore
            # so, what we really want here is to log this is happening and move on
            elif gds.PENDING == groupdesc.state:
                rm = gsgkr(
                    tag=server.tag_inc(),
                    ref=msg.tag,
                    err=gsgkr.Errors.PENDING,
                    err_info=f"group {target_uid} is pending",
                )
                reply_channel.send(rm.serialize())
                LOG.debug(f"group pending while kill request -- this should not be happening, response to {msg}: {rm}")
                return False
            elif gds.ACTIVE == groupdesc.state:
                groupdesc.state = gds.PENDING
                groupctx.destroy_request = msg
                groupctx.reply_channel = reply_channel

                ls_kill_context_map: Dict[int, List[ProcessContext]] = defaultdict(list)

                # init sending the signal to any processes belonging to the group
                issued_pendings = False
                for lst_idx, lst in enumerate(groupdesc.sets):
                    for item_idx, item in enumerate(lst):
                        # if this is not a process, do nothing
                        if isinstance(item.desc, process_desc.ProcessDescriptor):
                            if item.state != process_desc.ProcessDescriptor.State.DEAD:
                                resource_msg = groupctx._mk_gs_proc_kill(
                                    item.uid, sig=msg.sig, hide_stderr=msg.hide_stderr
                                )
                                issued, outbound_tag = ProcessContext.kill(
                                    server, resource_msg, dutil.AbsorbingChannel(), send_msg=False
                                )
                                pctx = server.process_table[item.uid]

                                if issued:
                                    ls_kill_context_map[pctx.node].append(pctx)
                                    # we need to issue a pending operation related to this process
                                    groupdesc.sets[lst_idx][
                                        item_idx
                                    ].state = process_desc.ProcessDescriptor.State.PENDING
                                    groupdesc.sets[lst_idx][
                                        item_idx
                                    ].desc.state = process_desc.ProcessDescriptor.State.PENDING
                                    server.pending[outbound_tag] = groupctx.complete_kill
                                    server.group_to_pending_resource_map[(outbound_tag, target_uid)] = pctx
                                    server.group_resource_count[target_uid][lst_idx] += 1
                                    if not issued_pendings:
                                        issued_pendings = True
                                else:
                                    # the process is either unknown or dead or pending
                                    # update the member's state accordingly
                                    groupdesc.sets[lst_idx][item_idx].state = pctx.descriptor.state
                                    groupdesc.sets[lst_idx][item_idx].desc.state = pctx.descriptor.state
                if issued_pendings:
                    for node, contexts in ls_kill_context_map.items():
                        procs = [context.shep_kill_msg for context in contexts]
                        shep_req = dmsg.SHMultiProcessKill(
                            tag=server.tag_inc(), r_c_uid=dfacts.GS_INPUT_CUID, procs=procs
                        )
                        shep_hdl = server.shep_inputs[node]
                        server.pending_sends.put((shep_hdl, shep_req.serialize()))
                        LOG.debug(f"request %s to shep %d", shep_req, node)
                else:
                    # there were no pending requests issued and we need to send a response to the client
                    # We consider this case as a success; for example, all the processes were already dead
                    rm = gsgkr(tag=server.tag_inc(), ref=msg.tag, err=gsgkr.Errors.ALREADY, desc=groupdesc)
                    LOG.debug(f"sending kill response to request {msg}: {rm} -- all processes were already dead")
                    groupdesc.state = gds.ACTIVE
                    reply_channel.send(rm.serialize())
                return issued_pendings
            else:
                raise NotImplementedError("close case")

    def complete_kill(self, msg):
        """Completes the action of a group kill.

        :param msg: response message related to destroying a resource which is member to a group
        :type msg: dragon.infrastructure.messages.SHProcessKillResponse
        :raises GroupError: when the context corresponding to the pending resource cannot be found
        :raises NotImplementedError: when we receive an unknown response error related to a group's resource/member
        :return: True if killing the process of the group succeeded False otherwise.
        :rtype: bool
        """
        # TODO: implement for other types of resources apart from processes

        gsgkr = dmsg.GSGroupKillResponse
        shpkr = dmsg.SHProcessKillResponse

        if msg.err not in shpkr.Errors:
            raise NotImplementedError("close case")

        # get the resource context corresponding to this response message
        if (msg.ref, self.descriptor.g_uid) in self.server.group_to_pending_resource_map:
            proc_context = self.server.group_to_pending_resource_map.pop((msg.ref, self.descriptor.g_uid))
        else:
            raise GroupError("Error with pending process belonging to a group.")

        # this p_uid should exist in server.resource_to_group_map dict
        this_puid = proc_context.descriptor.p_uid

        guid, (lst_idx, item_idx) = self.server.resource_to_group_map[this_puid]

        # complete the kill of this process without sending any response messages to the client
        # as we've set the reply_channel of the process context to dutil.AbsorbingChannel() in kill() above
        succeeded = proc_context.complete_kill(msg)
        # TODO: should we do anything if succeeded is False, which means that the
        # SHProcessKillResponse was a FAIL?

        if not self.destroy_remove_success_ids:
            # update the member's state accordingly
            self.descriptor.sets[lst_idx][item_idx].state = proc_context.descriptor.state

        # if destroy_remove was called
        if self.destroy_remove_success_ids:
            if self.server.group_destroy_resource_count[guid]:
                # adjust the count since we received one more response msg
                del self.server.group_destroy_resource_count[guid][-1]

            # if we have received responses for all the members, we are ready to remove
            # the resources from the group and send the response back to the client
            if not self.server.group_destroy_resource_count[guid]:
                for puid, lst_idx, item_idx in self.destroy_remove_success_ids:
                    # if this is not already removed/dead
                    if puid in self.server.resource_to_group_map:
                        self._remove_proc_from_group(puid, lst_idx, item_idx)
                # Update the dict that holds the position of the members of the group
                self._update_ds_after_removals()

                self.destroy_remove_success_ids = None
                if self.descriptor.state == self.descriptor.State.PENDING:
                    self.descriptor.state = self.descriptor.State.ACTIVE
                rm = dmsg.GSGroupDestroyRemoveFromResponse(
                    tag=self.server.tag_inc(),
                    ref=self.destroy_request.tag,
                    err=dmsg.GSGroupDestroyRemoveFromResponse.Errors.SUCCESS,
                    desc=self.descriptor,
                )
                LOG.debug(f"sending remove_destroy response to request {self.destroy_request}: {rm}")
                self.reply_channel.send(rm.serialize())

                del self.server.group_destroy_resource_count[guid]

        # if this was called from destroy(), then we don't want to send a
        # kill response to the client or counting how many kill responses
        # we have received, as this is an intermediate step of destroy()
        if not self.destroy_called:
            if self.server.group_resource_count[guid][lst_idx] >= 1:
                self.server.group_resource_count[guid][lst_idx] -= 1

            # if we have received responses for all the members, we are ready
            # to send the group kill response back to the client
            # if we have received responses for all the processess of this list
            if self.server.group_resource_count[guid][lst_idx] == 0:
                # next, check if we have responses for the other lists as well
                if all(item == 0 for item in self.server.group_resource_count[guid]):
                    # we follow the same pattern with groups as we do with processes
                    if self.descriptor.state == self.descriptor.State.PENDING:
                        self.descriptor.state = self.descriptor.State.ACTIVE

                    rm = gsgkr(
                        tag=self.server.tag_inc(),
                        ref=self.destroy_request.tag,
                        err=gsgkr.Errors.SUCCESS,
                        desc=self.descriptor,
                    )
                    LOG.debug(f"sending kill response to request {self.destroy_request}: {rm}")
                    self.reply_channel.send(rm.serialize())

        return succeeded

    @staticmethod
    def destroy(server, msg, reply_channel):
        """Action to destroy this group of resources.
        We need to destroy all the members of the group as well as the container.

        :param server: global server context object
        :type server: dragon.globalservices.server.GlobalContext
        :param msg: destroy request message
        :type msg: dragon.infrastructure.messages.GSGroupDestroy
        :param reply_channel: the handle needed to reply to this message
        :type reply_channel: dragon.infrastructure.connection.Connection
        :raises NotImplementedError: when the group's state is unknown, i.e., other than ACTIVE, DEAD, PENDING
        :return: True if a successful destroy message was issued and False otherwise
        :rtype: bool
        """
        target_uid, found, errmsg = server.resolve_guid(msg.user_name, msg.g_uid)
        gsgdr = dmsg.GSGroupDestroyResponse

        if not found:
            rm = gsgdr(tag=server.tag_inc(), ref=msg.tag, err=gsgdr.Errors.UNKNOWN, err_info=errmsg)
            reply_channel.send(rm.serialize())
            LOG.debug(f"unknown group of resources: response to {msg}: {rm}")
            return False
        else:
            groupctx = server.group_table[target_uid]
            groupdesc = groupctx.descriptor
            gds = group_desc.GroupDescriptor.State

            if gds.DEAD == groupdesc.state:
                rm = gsgdr(tag=server.tag_inc(), ref=msg.tag, err=gsgdr.Errors.DEAD)
                reply_channel.send(rm.serialize())
                LOG.debug(f"group dead, response to {msg}: {rm}")
                return False

            # this is a highly unlikely case to happen
            # because the user must have received a create response back
            # before requesting destroying this group, which means that the
            # state of the group is not pending anymore
            # so, what we really want here is to log this is happening and move on
            elif gds.PENDING == groupdesc.state:
                rm = gsgdr(
                    tag=server.tag_inc(),
                    ref=msg.tag,
                    err=gsgdr.Errors.PENDING,
                    err_info=f"group {target_uid} is pending",
                )
                reply_channel.send(rm.serialize())
                LOG.debug(
                    f"group pending while destroy request -- this should not be happening, response to {msg}: {rm}"
                )
                return False
            elif gds.ACTIVE == groupdesc.state:
                # update the group's state to PENDING as long as the destroy is pending
                groupdesc.state = gds.PENDING

                groupctx.destroy_request = msg
                groupctx.reply_channel = reply_channel

                server.group_destroy_resource_count[target_uid] = []

                ls_kill_context_map: Dict[int, List[ProcessContext]] = defaultdict(list)

                # init the destruction of the group's members
                for lst_idx, lst in enumerate(groupdesc.sets):
                    server.group_destroy_resource_count[target_uid].append(0)

                    for item_idx, item in enumerate(lst):

                        # TODO: implement for other types of resources apart from processes
                        # for other types we don't need to call kill first

                        # if the process is not already dead, proceed
                        if item.state != process_desc.ProcessDescriptor.State.DEAD:
                            # first, call kill to send a kill request
                            resource_msg = groupctx._mk_gs_proc_kill(item.uid, sig=signal.SIGKILL)
                            issued, outbound_tag = ProcessContext.kill(
                                server, resource_msg, dutil.AbsorbingChannel(), send_msg=False
                            )
                            pctx = server.process_table[item.uid]

                            if issued:
                                ls_kill_context_map[pctx.node].append(pctx)

                                # there has been a pending operation related to this process
                                groupdesc.sets[lst_idx][item_idx].state = process_desc.ProcessDescriptor.State.PENDING
                                groupdesc.sets[lst_idx][
                                    item_idx
                                ].desc.state = process_desc.ProcessDescriptor.State.PENDING

                                # this is needed for identification after GS having received SHProcessExit msg
                                # because the SHProcessExit msg will have a different tag
                                server.pending_group_destroy[item.uid] = groupctx.complete_destruction
                                server.group_to_pending_resource_map[(outbound_tag, target_uid)] = server.process_table[
                                    item.uid
                                ]

                                # this applies only to the case where processes are members of the group
                                groupctx.destroy_called = True
                                server.pending[outbound_tag] = groupctx.complete_kill

                                server.group_destroy_resource_count[target_uid][lst_idx] += 1
                            else:
                                # the process is either unknown or dead or pending
                                groupdesc.sets[lst_idx][item_idx].state = process_desc.ProcessDescriptor.State.DEAD
                                groupdesc.sets[lst_idx][item_idx].desc.state = process_desc.ProcessDescriptor.State.DEAD

                for node, contexts in ls_kill_context_map.items():
                    procs = [context.shep_kill_msg for context in contexts]
                    shep_req = dmsg.SHMultiProcessKill(tag=server.tag_inc(), r_c_uid=dfacts.GS_INPUT_CUID, procs=procs)
                    shep_hdl = server.shep_inputs[node]
                    server.pending_sends.put((shep_hdl, shep_req.serialize()))
                    LOG.debug(f"request %s to shep %d", shep_req, node)

                # in this case, all the processes were already dead or no pending continuation
                # was issued and we need to send a response to the client
                # e.g., kill was called prior to destroy and the kill request completed successully (SHProcessExit was sent)
                if all(item == 0 for item in server.group_destroy_resource_count[target_uid]):
                    groupdesc.state = gds.DEAD
                    if groupctx.pmi_job_helper:
                        groupctx.pmi_job_helper.cleanup()
                    rm = gsgdr(tag=server.tag_inc(), ref=msg.tag, err=gsgdr.Errors.SUCCESS, desc=groupdesc)
                    reply_channel.send(rm.serialize())
                    LOG.debug(f"sending destroy response to request {msg}: {rm}")

                    # no need to keep this entry since the group is destroyed
                    del server.group_destroy_resource_count[target_uid]

                return True
            else:
                raise NotImplementedError("close case")

    def complete_destruction(self, msg):
        """Completes the destruction of a group of resources.
        When dealing with processes as members of the group, at this point, we assume that
        the kill request is being completed.

        :param msg: response message related to destroying a resource which is member to a group
        :type msg: dragon.infrastructure.messages.SHProcessExit
        :raises GroupError: when the context corresponding to the pending resource cannot be found
        :raises NotImplementedError: when we receive an unknown response error related to a group's resource/member
        """
        # TODO: implement for other types of resources apart from processes

        if not isinstance(msg, dmsg.SHProcessExit):
            raise GroupError(f"Received msg type {msg} - expected an SHProcessExit msg.")

        gsgdr = dmsg.GSGroupDestroyResponse

        # get the resource context corresponding to this response message
        if (msg.p_uid, self.descriptor.g_uid) in self.server.group_to_pending_resource_map:
            proc_context = self.server.group_to_pending_resource_map.pop((msg.p_uid, self.descriptor.g_uid))
        else:
            raise GroupError("Error with group pending resource.")

        # this p_uid should exist in server.resource_to_group_map dict
        this_puid = proc_context.descriptor.p_uid

        guid, (lst_idx, item_idx) = self.server.resource_to_group_map[this_puid]

        if self.server.group_destroy_resource_count[guid][lst_idx] >= 1:
            self.server.group_destroy_resource_count[guid][lst_idx] -= 1

        del self.server.resource_to_group_map[this_puid]

        # if we have received responses for all the members, we are ready
        # to send the group destroy response to the client
        # if we have received responses for all the members of this list
        if self.server.group_destroy_resource_count[guid][lst_idx] == 0:
            # next, check if we have responses for the other lists as well
            if all(item == 0 for item in self.server.group_destroy_resource_count[guid]):
                self.descriptor.state = self.descriptor.State.DEAD
                if self.pmi_job_helper:
                    self.pmi_job_helper.cleanup()
                rm = gsgdr(
                    tag=self.server.tag_inc(),
                    ref=self.destroy_request.tag,
                    err=gsgdr.Errors.SUCCESS,
                    desc=self.descriptor,
                )
                LOG.debug(f"sending destroy response to request {self.destroy_request}: {rm}")
                self.reply_channel.send(rm.serialize())

                # no need to keep this entry since the group is destroyed
                del self.server.group_destroy_resource_count[guid]

        return
