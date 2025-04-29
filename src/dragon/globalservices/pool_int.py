"""Global services' internal Pool context.."""

import dragon.infrastructure.pool_desc as pool_desc
import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.messages as dmsg

import logging

LOG = logging.getLogger("GS.pool:")


class PoolContext:
    """Everything to do with a single memory pool in global services.


    This object manages all the transactions to a shepherd concerning
    the lifecycle of a pool.
    """

    def __init__(self, server, request, reply_channel, m_uid, node):
        self.server = server
        self.request = request
        self.reply_channel = reply_channel
        self.destroy_request = None

        self._descriptor = pool_desc.PoolDescriptor(node=node, m_uid=m_uid, name=request.user_name)

    def __str__(self):
        return f"[[{self.__class__.__name__}]] desc:{self.descriptor!r} req:{self.request!r}"

    @property
    def descriptor(self):
        return self._descriptor

    def _mk_sh_pool_create(self, the_tag):
        return dmsg.SHPoolCreate(
            tag=the_tag,
            p_uid=dfacts.GS_PUID,
            r_c_uid=dfacts.GS_INPUT_CUID,
            m_uid=self.descriptor.m_uid,
            size=self.request.size,
            name=self.request.user_name,
            attr=self.request.options.sattr,
        )

    def mk_sh_pool_destroy(self, the_tag):
        return dmsg.SHPoolDestroy(
            tag=the_tag, p_uid=dfacts.GS_PUID, r_c_uid=dfacts.GS_INPUT_CUID, m_uid=self.descriptor.m_uid
        )

    @classmethod
    def construct(cls, server, msg, reply_channel):
        """Creates a PoolContext and registers it with the server.

        This routine will send a message to the Shepherd and will leave
        a continuation in the server's pending messages table.

        Getting the reply channel as a parameter makes it easier
        to make this an internal construction...

        :return: a tuple:
                First element True if construction message issued, False otherwise
                Second element the tag if message issued, None otherwise
                Third element The newly constructed context object if issued, None otherwise
        """

        if msg.user_name in server.pool_names:
            LOG.info(f"pool name {msg.user_name} in use")
            existing_ctx = server.pool_table[server.pool_names[msg.user_name]]
            rm = dmsg.GSPoolCreateResponse(
                tag=server.tag_inc(),
                ref=msg.tag,
                err=dmsg.GSPoolCreateResponse.Errors.ALREADY,
                desc=existing_ctx.descriptor,
            )
            reply_channel.send(rm.serialize())
            return False, None, None

        this_muid, auto_name = server.new_muid_and_default_name()

        if not msg.user_name:
            msg.user_name = auto_name

        which_node = server.choose_pool_node(msg)

        context = cls(server, msg, reply_channel, this_muid, which_node)

        server.pool_names[msg.user_name] = this_muid
        server.pool_table[this_muid] = context

        shep_hdl = server.shep_inputs[which_node]

        outbound_tag = server.tag_inc()
        shep_create_msg = context._mk_sh_pool_create(outbound_tag)

        # In cases where we are sending a large amount of
        # messages, such as with the GSGroupCreate handler,
        # we can fill the GS Input Queue with responses and
        # basically cause the GS / TA / LS to be unable to
        # send/receive any messages. To prevent this, we'll
        # enqueue pending sends and interleave sending and
        # receiving messages to allow us to process responses
        # on the input queue.

        server.pending_sends.put((shep_hdl, shep_create_msg.serialize()))
        return True, outbound_tag, context

    def complete_construction(self, msg):
        """Completes construction of a PoolContext

        :return: True if it succeeded, False otherwise
        """
        assert isinstance(msg, dmsg.SHPoolCreateResponse)

        if msg.Errors.SUCCESS == msg.err:
            self.descriptor.state = pool_desc.PoolDescriptor.State.ACTIVE
            self.descriptor.sdesc = msg.desc

            response = dmsg.GSPoolCreateResponse(
                tag=self.server.tag_inc(),
                ref=self.request.tag,
                err=dmsg.GSPoolCreateResponse.Errors.SUCCESS,
                desc=self.descriptor,
            )
            self.reply_channel.send(response.serialize())

            create_succeeded = True

        elif msg.Errors.FAIL == msg.err:
            self.descriptor.state = pool_desc.PoolDescriptor.State.DEAD
            response = dmsg.GSPoolCreateResponse(
                tag=self.server.tag_inc(),
                ref=self.request.tag,
                err=dmsg.GSPoolCreateResponse.Errors.FAIL,
                err_info=msg.err_info,
            )

            # we don't keep descriptor for things that were never alive
            del self.server.pool_table[self.descriptor.m_uid]
            del self.server.pool_names[self.request.user_name]

            self.reply_channel.send(response.serialize())
            create_succeeded = False
        else:
            raise RuntimeError(f"got {str(msg)} err {msg.err} unknown")

        LOG.debug(f"create response sent, tag {response.tag} ref {response.ref} pending cleared")

        self.reply_channel = None  # drop reference to reply channel obj

        return create_succeeded

    @staticmethod
    def destroy(server, msg, reply_channel):
        """Handle request to destroy a context.

        Feature we lack but that we might want:

        Since it's a pool, we might want to maintain a table of which channels
        belong to which pools and error if there are any live channels
        currently in the pool being requested to be destroyed.

        :param server:
        :param msg: a GSPoolDestroyResponse
        :param reply_channel: object one can send the response to
        :return: True if a destroy request got issued False otherwise.
        """
        target_uid, found, errmsg = server.resolve_muid(msg.user_name, msg.m_uid)
        gspdr = dmsg.GSPoolDestroyResponse

        if not found:
            rm = gspdr(tag=server.tag_inc(), ref=msg.tag, err=gspdr.Errors.UNKNOWN, err_info=errmsg)
            reply_channel.send(rm.serialize())
            LOG.debug(f"unknown: response to {msg}: {rm}")
            return False
        else:
            poolctx = server.pool_table[target_uid]
            pooldesc = poolctx.descriptor
            pds = pool_desc.PoolDescriptor.State
            if pds.DEAD == pooldesc.state:
                rm = gspdr(tag=server.tag_inc(), ref=msg.tag, err=gspdr.Errors.GONE)
                reply_channel.send(rm.serialize())
                LOG.debug(f"gone; response to {msg}: {rm}")
                return False
            elif pds.PENDING == pooldesc.state:
                rm = gspdr(
                    tag=server.tag_inc(), ref=msg.tag, err=gspdr.Errors.PENDING, err_info=f"{target_uid} is pending"
                )
                reply_channel.send(rm.serialize())
                LOG.debug(f"pending; response to {msg}: {rm}")
                return False
            elif pds.ACTIVE == pooldesc.state:
                the_tag = server.tag_inc()
                poolctx.reply_channel = reply_channel
                poolctx.destroy_request = msg
                poolctx.descriptor.state = pds.PENDING  # might want a PENDING_DESTRUCTION state?
                shep_destroy_msg = poolctx.mk_sh_pool_destroy(the_tag)
                target_node = pooldesc.node
                server.pending[the_tag] = poolctx.complete_destruction
                server.pending_sends.put((server.shep_inputs[target_node], shep_destroy_msg.serialize()))

                LOG.debug(f"forwarded destroy {msg} as {shep_destroy_msg} to {target_node}")
                return True
            else:
                raise NotImplementedError("bad case")

    def complete_destruction(self, msg):
        """

        :param msg:
        :return: True if the destroy succeeded False otherwise.
        """
        gspdr = dmsg.GSPoolDestroyResponse
        shpdr = dmsg.SHPoolDestroyResponse

        if shpdr.Errors.FAIL == msg.err:
            rm = gspdr(
                tag=self.server.tag_inc(), ref=self.destroy_request.tag, err=gspdr.Errors.FAIL, err_info=msg.err_info
            )
            destroy_succeeded = False
        elif shpdr.Errors.SUCCESS == msg.err:
            self.descriptor.state = pool_desc.PoolDescriptor.State.DEAD
            rm = gspdr(tag=self.server.tag_inc(), ref=self.destroy_request.tag, err=gspdr.Errors.SUCCESS)
            destroy_succeeded = True
        else:
            raise NotImplementedError("no case")

        LOG.debug(f"pool destroy response to {self.destroy_request}: {rm}")
        self.reply_channel.send(rm.serialize())
        return destroy_succeeded
