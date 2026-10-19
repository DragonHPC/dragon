"""Channel related objects internal to Global Services.

This file includes the ChannelContext object, and code that
talks to a local services to manipulate channel life cycle.
"""

import dragon.infrastructure.channel_desc as channel_desc
import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.messages as dmsg
import dragon.channels as dch

import logging

from dragon.dtypes import DragonError
from dragon.infrastructure.queue import InfraQueue
from dragon.utils import B64

LOG = logging.getLogger("GS.channel:")


class ChannelContext:
    """Everything to do with a single channel in global services

    This class includes the code that does the lifecycle of the
    channel to the local services hosting that channel - sending and receiving
    local services command messages.
    """

    def __init__(self, server, request, reply_queue, c_uid, node):
        self.server = server
        self.request = request
        self.destroy_request = None
        self.reply_queue = reply_queue  # TODO: really is a InfraQueue, should change var name
        self.attached_connection = None
        self.adopted_puid = None
        self.is_reading = False
        self.is_refcounted = self.request.options.ref_count
        self.refcnt = 0

        self._descriptor = channel_desc.ChannelDescriptor(
            node=node, m_uid=request.m_uid, c_uid=c_uid, name=request.user_name
        )

    def __str__(self):
        return f"[[{self.__class__.__name__}]] desc:{self.descriptor!r} req:{self.request!r}"

    def incref(self):
        self.refcnt += 1

    @property
    def descriptor(self):
        return self._descriptor

    def _mk_ls_channel_create(self, the_tag):
        self.lschannelcreate_msg = dmsg.LSChannelCreate(
            tag=the_tag,
            p_uid=dfacts.GS_PUID,
            r_c_uid=dfacts.GS_INPUT_CUID,
            m_uid=self.descriptor.m_uid,
            c_uid=self.descriptor.c_uid,
            options=self.request.options.local_opts,
        )
        return self.lschannelcreate_msg

    @classmethod
    def adopt_external_return(cls, server, msg):
        """Adopt an external GS-return channel that was created by LS."""
        if msg.node_index not in server.node_idx_to_huid:
            raise ValueError(f"unknown node index {msg.node_index}")

        expected_muid = dfacts.infrastructure_pool_muid_from_index(msg.node_index)
        if msg.gs_return_muid != expected_muid:
            raise ValueError(
                f"GS-return MUID {msg.gs_return_muid} does not match node {msg.node_index} infrastructure pool"
            )

        channel_sdesc = B64.str_to_bytes(msg.gs_return_cd)
        if dch.Channel.serialized_uid(channel_sdesc) != msg.gs_return_cuid:
            raise ValueError("GS-return channel descriptor CUID does not match prepared metadata")
        channel_muid, _ = dch.Channel.serialized_pool_uid_fname(channel_sdesc)
        if channel_muid != msg.gs_return_muid:
            raise ValueError("GS-return channel descriptor MUID does not match prepared metadata")

        existing = server.channel_table.get(msg.gs_return_cuid)
        if existing is not None:
            if existing.adopted_puid == msg.p_uid:
                return existing
            raise ValueError(f"GS-return CUID {msg.gs_return_cuid} is already registered")

        channel_name = f"external.{msg.p_uid}.gs_return"
        if channel_name in server.channel_names:
            raise ValueError(f"GS-return channel name {channel_name} is already registered")

        request = dmsg.GSChannelCreate(
            tag=msg.tag,
            p_uid=msg.p_uid,
            r_c_uid=msg.gs_return_cuid,
            m_uid=msg.gs_return_muid,
            user_name=channel_name,
        )
        context = cls(server, request, None, msg.gs_return_cuid, msg.node_index)
        context.descriptor.sdesc = msg.gs_return_cd
        context.descriptor.state = channel_desc.ChannelDescriptor.State.ACTIVE
        context.attached_connection = InfraQueue.attach(msg.gs_return_qd)
        context.adopted_puid = msg.p_uid
        context.is_refcounted = False

        server.channel_table[msg.gs_return_cuid] = context
        server.channel_names[channel_name] = msg.gs_return_cuid
        return context

    def mk_ls_channel_destroy(self, the_tag):
        return dmsg.LSChannelDestroy(
            tag=the_tag, p_uid=dfacts.GS_PUID, r_c_uid=dfacts.GS_INPUT_CUID, c_uid=self.descriptor.c_uid
        )

    @classmethod
    def construct(cls, server, msg, reply_queue, node_override=None, send_msg=True):
        """Constructs a new channel context and sends out a message
        to a local services to create the channel.

        :return: a tuple:
            First element True if construction message issued, False otherwise
            Second element the tag if message issued, None otherwise
            Third element The newly constructed context object if issued, None otherwise
        """
        try:

            if msg.user_name in server.channel_names:
                LOG.info(f"channel {msg.user_name} already created")
                if send_msg:
                    existing_ctx = server.channel_table[server.channel_names[msg.user_name]]
                    rm = dmsg.GSChannelCreateResponse(
                        tag=server.tag_inc(),
                        ref=msg.tag,
                        err=dmsg.GSChannelCreateResponse.Errors.ALREADY,
                        desc=existing_ctx.descriptor,
                    )
                    reply_queue.put(rm)
                return False, None, None

            try:
                if node_override is not None:
                    which_node = node_override
                else:
                    which_node = server.choose_channel_node(msg)
            except ValueError:
                # the requested pool to create the channel on does not exist
                # and we need to log this
                errmsg = (
                    f"Error creating channel. Either m_uid {msg.m_uid} does not exist (it is neither a predefined pool nor a user defined pool)\n"
                    + f"or the Policy did not specify a node on which to create the channel."
                )
                LOG.info(errmsg)
                if send_msg:
                    rm = dmsg.GSChannelCreateResponse(
                        tag=server.tag_inc(), ref=msg.tag, err=dmsg.GSChannelCreateResponse.Errors.FAIL, err_info=errmsg
                    )
                    reply_queue.put(rm)
                return False, None, None

            # check if the m_uid is ok
            if not (dfacts.is_pre_defined_pool(msg.m_uid) or msg.m_uid in server.pool_table):
                errmsg = f"m_uid {msg.m_uid} unknown"
                LOG.info(errmsg)
                if send_msg:
                    rm = dmsg.GSChannelCreateResponse(
                        tag=server.tag_inc(), ref=msg.tag, err=dmsg.GSChannelCreateResponse.Errors.FAIL, err_info=errmsg
                    )
                    reply_queue.put(rm)
                return False, None, None

            this_cuid, auto_name = server.new_cuid_and_default_name()

            if not msg.user_name:
                msg.user_name = auto_name

            outbound_tag = server.tag_inc()

            context = cls(server, msg, reply_queue, this_cuid, which_node)
            server.channel_names[msg.user_name] = this_cuid
            server.channel_table[this_cuid] = context

            # send request to local services, remember pending process.
            ls_create_msg = context._mk_ls_channel_create(outbound_tag)
            if send_msg:
                LOG.debug(f"ls_inputs: {server.ls_inputs}")
                ls_hdl = server.ls_inputs[which_node]
                LOG.debug(
                    f"sending {ls_create_msg} with muid {ls_create_msg.m_uid} to ls via {ls_hdl} on node {which_node}"
                )

                # In cases where we are sending a large amount of
                # messages, such as with the GSGroupCreate handler,
                # we can fill the GS Input Queue with responses and
                # basically cause the GS / TA / LS to be unable to
                # send/receive any messages. To prevent this, we'll
                # enqueue pending sends and interleave sending and
                # receiving messages to allow us to process responses
                # on the input queue.

                server.pending_sends.put((ls_hdl, ls_create_msg))
                LOG.debug(f"request {context} to ls")

            return True, outbound_tag, context

        except Exception as ex:
            response = dmsg.GSChannelCreateResponse(
                tag=server.tag_inc(),
                ref=msg.tag,
                err=dmsg.GSChannelCreateResponse.Errors.FAIL,
                desc=None,
            )

            reply_queue.put(response)

    def complete_construction(self, msg):
        """Completes channel construction, waiting on message from local services

        :return: True or False, according to if construction succeeded
        """

        if not isinstance(msg, dmsg.LSChannelCreateResponse):
            print(f"Failure: The message was: {msg}")
        assert isinstance(msg, dmsg.LSChannelCreateResponse)

        if dmsg.LSChannelCreateResponse.Errors.SUCCESS == msg.err:
            self.descriptor.state = channel_desc.ChannelDescriptor.State.ACTIVE
            self.descriptor.sdesc = msg.desc
            response = dmsg.GSChannelCreateResponse(
                tag=self.server.tag_inc(),
                ref=self.request.tag,
                err=dmsg.GSChannelCreateResponse.Errors.SUCCESS,
                desc=self.descriptor,
            )

            self.reply_queue.put(response)
            create_succeeded = True
            if self.is_refcounted:
                self.refcnt = 1  # original requester.
                LOG.debug(f"channel {self.descriptor.c_uid}: refcnt={self.refcnt}")

            # only check pending joins if the create succeeded.  The entity creating
            # the channel will get the error.  There could be a message back to
            # the entity trying to join explaining that the create failed, but
            # there isn't anything that entity can do about it, only the one making
            # the original create
            if self.descriptor.name in self.server.pending_channel_joins:
                for join_req in self.server.pending_channel_joins.get(self.descriptor.name):
                    reply_queue = self.server.get_reply_handle(join_req)
                    rm = dmsg.GSChannelJoinResponse(
                        tag=self.server.tag_inc(),
                        ref=join_req.tag,
                        err=dmsg.GSChannelJoinResponse.Errors.SUCCESS,
                        desc=self.descriptor,
                    )
                    reply_queue.put(rm)
                    LOG.debug(f"join response to {join_req}: {rm}")
        elif dmsg.LSChannelCreateResponse.Errors.FAIL == msg.err:
            self.descriptor.state = channel_desc.ChannelDescriptor.State.DEAD
            response = dmsg.GSChannelCreateResponse(
                tag=self.server.tag_inc(),
                ref=self.request.tag,
                err=dmsg.GSChannelCreateResponse.Errors.FAIL,
                err_info=msg.err_info,
            )

            # we don't keep descriptor for things that were never alive
            del self.server.channel_table[self.descriptor.c_uid]
            del self.server.channel_names[self.request.user_name]

            # log a warning if joins are orphaned.
            if self.descriptor.name in self.server.pending_channel_joins:
                LOG.warn(f"channel create name {self.descriptor.name} failed with joins pending")

            self.reply_queue.put(response)
            create_succeeded = False
        else:
            raise RuntimeError(f"got {str(msg)} err {msg.err} unknown")

        self.reply_queue = None  # deliberately drop reference to reply queue

        return create_succeeded

    @staticmethod
    def destroy(server, msg, reply_queue):
        """Action to destroy this channel

        :param server: global server context
        :param msg: destroy request message
        :param reply_queue: response channel
        :return: True if a destroy message was issued and False otherwise
        """
        target_uid, found, errmsg = server.resolve_cuid(msg.user_name, msg.c_uid)
        gscdr = dmsg.GSChannelDestroyResponse

        if not found:
            rm = gscdr(tag=server.tag_inc(), ref=msg.tag, err=gscdr.Errors.UNKNOWN, err_info=errmsg)
            reply_queue.put(rm)
            LOG.debug(f"unknown channel: response to {msg}: {rm}")
            return False
        else:
            channelctx = server.channel_table[target_uid]
            channeldesc = channelctx.descriptor
            cds = channel_desc.ChannelDescriptor.State
            if cds.DEAD == channeldesc.state:
                rm = gscdr(tag=server.tag_inc(), ref=msg.tag, err=gscdr.Errors.BUSY)
                reply_queue.put(rm)
                LOG.debug(f"gone; response to {msg}: {rm}")
                return False
            elif cds.PENDING == channeldesc.state:
                rm = gscdr(
                    tag=server.tag_inc(), ref=msg.tag, err=gscdr.Errors.BUSY, err_info=f"{target_uid} is pending"
                )
                reply_queue.put(rm)
                LOG.debug(f"pending; response to {msg}: {rm}")
                return False
            elif cds.ACTIVE == channeldesc.state:
                channelctx.destroy_request = msg
                channelctx.reply_queue = reply_queue

                if channelctx.is_refcounted and msg.dec_ref:
                    channelctx.refcnt -= 1
                    LOG.debug(f"channel {channelctx.descriptor.c_uid}: refcnt={channelctx.refcnt}")
                    if channelctx.refcnt == 0:
                        really_destroying = True
                    elif channelctx.refcnt < 0:
                        really_destroying = False
                        LOG.warning(f"refcnt overrun from msg={msg!r}")
                    else:
                        really_destroying = False
                else:
                    really_destroying = True

                if not really_destroying:  # just a refcount op that didn't cause destruction.
                    rm = gscdr(tag=server.tag_inc(), ref=msg.tag, err=gscdr.Errors.SUCCESS)
                    reply_queue.put(rm)
                    LOG.debug(f"channel {channelctx.descriptor.c_uid}: Not destroying, refcnt={channelctx.refcnt}")
                    return False
                else:
                    channelctx.descriptor.state = cds.PENDING  # do we want a pending destruction state?
                    the_tag = server.tag_inc()
                    ls_destroy_msg = channelctx.mk_ls_channel_destroy(the_tag)
                    target_node = channeldesc.node
                    server.pending[the_tag] = channelctx.complete_destruction
                    ls_hdl = server.ls_inputs[target_node]
                    server.pending_sends.put((ls_hdl, ls_destroy_msg))

                    LOG.debug(f"forwarded destroy {msg} as {ls_destroy_msg} to {target_node}")
                    return True
            else:
                raise NotImplementedError("close case")

    def complete_destruction(self, msg):
        gscdr = dmsg.GSChannelDestroyResponse
        lscdr = dmsg.LSChannelDestroyResponse

        if lscdr.Errors.FAIL == msg.err:
            self.descriptor.state = channel_desc.ChannelDescriptor.State.ACTIVE
            rm = gscdr(
                tag=self.server.tag_inc(), ref=self.destroy_request.tag, err=gscdr.Errors.UNKNOWN, err_info=msg.err_info
            )
            destroy_succeeded = False
        elif lscdr.Errors.SUCCESS == msg.err:
            self.descriptor.state = channel_desc.ChannelDescriptor.State.DEAD
            rm = gscdr(tag=self.server.tag_inc(), ref=self.destroy_request.tag, err=gscdr.Errors.SUCCESS)
            destroy_succeeded = True
            if self.adopted_puid is not None:
                if self.attached_connection is not None:
                    self.attached_connection.close()
                    self.attached_connection = None
                self.server.channel_table.pop(self.descriptor.c_uid, None)
                self.server.channel_names.pop(self.descriptor.name, None)
        else:
            raise NotImplementedError("no case")

        LOG.debug(f"channel destroy response to {self.destroy_request}: {rm}")
        self.reply_queue.put(rm)

        return destroy_succeeded
