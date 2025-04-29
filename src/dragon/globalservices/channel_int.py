"""Channel related objects internal to Global Services.

This file includes the ChannelContext object, and code that
talks to a shepherd to manipulate channel life cycle.
"""

import dragon.infrastructure.channel_desc as channel_desc
import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.messages as dmsg
import dragon.channels as dch

import logging

from dragon.dtypes import DragonError

LOG = logging.getLogger("GS.channel:")


class ChannelContext:
    """Everything to do with a single channel in global services

    This class includes the code that does the lifecycle of the
    channel to the shepherd hosting that channel - sending and receiving
    shepherd command messages.
    """

    def __init__(self, server, request, reply_channel, c_uid, node):
        self.server = server
        self.request = request
        self.destroy_request = None
        self.reply_channel = reply_channel  # TODO: really is a Connection, should change var name
        self.attached_connection = None
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

    def _mk_sh_channel_create(self, the_tag):
        self.shchannelcreate_msg = dmsg.SHChannelCreate(
            tag=the_tag,
            p_uid=dfacts.GS_PUID,
            r_c_uid=dfacts.GS_INPUT_CUID,
            m_uid=self.descriptor.m_uid,
            c_uid=self.descriptor.c_uid,
            options=self.request.options.local_opts,
        )
        return self.shchannelcreate_msg

    def mk_sh_channel_destroy(self, the_tag):
        return dmsg.SHChannelDestroy(
            tag=the_tag, p_uid=dfacts.GS_PUID, r_c_uid=dfacts.GS_INPUT_CUID, c_uid=self.descriptor.c_uid
        )

    @classmethod
    def construct(cls, server, msg, reply_channel, node_override=None, send_msg=True):
        """Constructs a new channel context and sends out a message
        to a shepherd to create the channel.

        :return: a tuple:
            First element True if construction message issued, False otherwise
            Second element the tag if message issued, None otherwise
            Third element The newly constructed context object if issued, None otherwise
        """

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
                reply_channel.send(rm.serialize())
            return False, None, None

        # check if the m_uid is ok
        if not (dfacts.is_pre_defined_pool(msg.m_uid) or msg.m_uid in server.pool_table):
            errmsg = f"m_uid {msg.m_uid} unknown"
            LOG.info(errmsg)
            if send_msg:
                rm = dmsg.GSChannelCreateResponse(
                    tag=server.tag_inc(), ref=msg.tag, err=dmsg.GSChannelCreateResponse.Errors.FAIL, err_info=errmsg
                )
                reply_channel.send(rm.serialize())
            return False, None, None

        this_cuid, auto_name = server.new_cuid_and_default_name()

        if not msg.user_name:
            msg.user_name = auto_name

        outbound_tag = server.tag_inc()

        try:
            which_node = node_override
            if which_node is None:
                which_node = server.choose_channel_node(msg)
        except ValueError:
            # the requested pool to create the channel on does not exist
            # and we need to log this
            errmsg = f"m_uid {msg.m_uid} does not exist - it is neither a predefined pool nor a user defined pool"
            LOG.info(errmsg)
            if send_msg:
                rm = dmsg.GSChannelCreateResponse(
                    tag=server.tag_inc(), ref=msg.tag, err=dmsg.GSChannelCreateResponse.Errors.FAIL, err_info=errmsg
                )
                reply_channel.send(rm.serialize())
            return False, None, None

        context = cls(server, msg, reply_channel, this_cuid, which_node)
        server.channel_names[msg.user_name] = this_cuid
        server.channel_table[this_cuid] = context

        # send request to local services, remember pending process.
        shep_create_msg = context._mk_sh_channel_create(outbound_tag)
        if send_msg:
            LOG.debug(f"shep_inputs: {server.shep_inputs}")
            shep_hdl = server.shep_inputs[which_node]
            LOG.debug(
                f"sending {shep_create_msg} with muid {shep_create_msg.m_uid} to shep via {shep_hdl} on node {which_node}"
            )

            # In cases where we are sending a large amount of
            # messages, such as with the GSGroupCreate handler,
            # we can fill the GS Input Queue with responses and
            # basically cause the GS / TA / LS to be unable to
            # send/receive any messages. To prevent this, we'll
            # enqueue pending sends and interleave sending and
            # receiving messages to allow us to process responses
            # on the input queue.

            server.pending_sends.put((shep_hdl, shep_create_msg.serialize()))
            LOG.debug(f"request {context} to shep")

        return True, outbound_tag, context

    def complete_construction(self, msg):
        """Completes channel construction, waiting on message from shep

        :return: True or False, according to if construction succeeded
        """

        if not isinstance(msg, dmsg.SHChannelCreateResponse):
            print(f"Failure: The message was: {msg}")
        assert isinstance(msg, dmsg.SHChannelCreateResponse)

        if dmsg.SHChannelCreateResponse.Errors.SUCCESS == msg.err:
            self.descriptor.state = channel_desc.ChannelDescriptor.State.ACTIVE
            self.descriptor.sdesc = msg.desc
            response = dmsg.GSChannelCreateResponse(
                tag=self.server.tag_inc(),
                ref=self.request.tag,
                err=dmsg.GSChannelCreateResponse.Errors.SUCCESS,
                desc=self.descriptor,
            )

            self.reply_channel.send(response.serialize())
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
                    reply_channel = self.server.get_reply_handle(join_req)
                    rm = dmsg.GSChannelJoinResponse(
                        tag=self.server.tag_inc(),
                        ref=join_req.tag,
                        err=dmsg.GSChannelJoinResponse.Errors.SUCCESS,
                        desc=self.descriptor,
                    )
                    reply_channel.send(rm.serialize())
                    LOG.debug(f"join response to {join_req}: {rm}")
        elif dmsg.SHChannelCreateResponse.Errors.FAIL == msg.err:
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

            self.reply_channel.send(response.serialize())
            create_succeeded = False
        else:
            raise RuntimeError(f"got {str(msg)} err {msg.err} unknown")

        self.reply_channel = None  # deliberately drop reference to reply channel

        return create_succeeded

    @staticmethod
    def destroy(server, msg, reply_channel):
        """Action to destroy this channel

        :param server: global server context
        :param msg: destroy request message
        :param reply_channel: response channel
        :return: True if a destroy message was issued and False otherwise
        """
        target_uid, found, errmsg = server.resolve_cuid(msg.user_name, msg.c_uid)
        gscdr = dmsg.GSChannelDestroyResponse

        if not found:
            rm = gscdr(tag=server.tag_inc(), ref=msg.tag, err=gscdr.Errors.UNKNOWN, err_info=errmsg)
            reply_channel.send(rm.serialize())
            LOG.debug(f"unknown channel: response to {msg}: {rm}")
            return False
        else:
            channelctx = server.channel_table[target_uid]
            channeldesc = channelctx.descriptor
            cds = channel_desc.ChannelDescriptor.State
            if cds.DEAD == channeldesc.state:
                rm = gscdr(tag=server.tag_inc(), ref=msg.tag, err=gscdr.Errors.BUSY)
                reply_channel.send(rm.serialize())
                LOG.debug(f"gone; response to {msg}: {rm}")
                return False
            elif cds.PENDING == channeldesc.state:
                rm = gscdr(
                    tag=server.tag_inc(), ref=msg.tag, err=gscdr.Errors.BUSY, err_info=f"{target_uid} is pending"
                )
                reply_channel.send(rm.serialize())
                LOG.debug(f"pending; response to {msg}: {rm}")
                return False
            elif cds.ACTIVE == channeldesc.state:
                channelctx.destroy_request = msg
                channelctx.reply_channel = reply_channel

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
                    reply_channel.send(rm.serialize())
                    LOG.debug(f"channel {channelctx.descriptor.c_uid}: Not destroying, refcnt={channelctx.refcnt}")
                    return False
                else:
                    channelctx.descriptor.state = cds.PENDING  # do we want a pending destruction state?
                    the_tag = server.tag_inc()
                    shep_destroy_msg = channelctx.mk_sh_channel_destroy(the_tag)
                    target_node = channeldesc.node
                    server.pending[the_tag] = channelctx.complete_destruction
                    server.pending_sends.put((server.shep_inputs[target_node], shep_destroy_msg.serialize()))

                    LOG.debug(f"forwarded destroy {msg} as {shep_destroy_msg} to {target_node}")
                    return True
            else:
                raise NotImplementedError("close case")

    def complete_destruction(self, msg):
        gscdr = dmsg.GSChannelDestroyResponse
        shcdr = dmsg.SHChannelDestroyResponse

        if shcdr.Errors.FAIL == msg.err:
            rm = gscdr(
                tag=self.server.tag_inc(), ref=self.destroy_request.tag, err=gscdr.Errors.UNKNOWN, err_info=msg.err_info
            )
            destroy_succeeded = False
        elif shcdr.Errors.SUCCESS == msg.err:
            self.descriptor.state = channel_desc.ChannelDescriptor.State.DEAD
            rm = gscdr(tag=self.server.tag_inc(), ref=self.destroy_request.tag, err=gscdr.Errors.SUCCESS)
            destroy_succeeded = True
        else:
            raise NotImplementedError("no case")

        LOG.debug(f"channel destroy response to {self.destroy_request}: {rm}")
        self.reply_channel.send(rm.serialize())

        return destroy_succeeded
