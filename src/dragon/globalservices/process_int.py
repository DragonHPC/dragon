"""Process related objects internal to Global Services.
"""

import copy
import logging
import signal

from .. import channels as dch
from ..globalservices import channel_int as channel_int
from ..infrastructure import process_desc as process_desc
from ..infrastructure import channel_desc as channel_desc
from ..infrastructure.group_desc import GroupDescriptor
from ..localservices import options as lsopt
from ..infrastructure import facts as dfacts
from ..infrastructure import messages as dmsg
from ..infrastructure import connection as dconn
from ..infrastructure import parameters as dp
from ..infrastructure import util as dutil
from ..utils import B64

LOG = logging.getLogger('process:')


class ProcessContext:
    """Everything to do with a single process in global services

        The methods in this object are used to communicate with
        the shepherd to which this processes is assigned to
        manage its lifecycle.
    """

    def __init__(self, *, server, request, reply_channel, p_uid, node):
        self.server = server
        self.request = request
        self.reply_channel = reply_channel
        self.exit_msg = None
        self.destroy_request = None
        self.gs_ret_channel_context = None
        self._descriptor = process_desc.ProcessDescriptor(p_uid=p_uid,
                                                          name=request.user_name,
                                                          node=node,
                                                          p_p_uid=request.p_uid)

        self.process_parms = copy.copy(dp.this_process)
        self.process_parms.my_puid = p_uid
        self.process_parms.index = node
        self.stdin_context = None
        self.stdout_context = None
        self.stderr_context = None

        self.belongs_to_group = False
        self.group_addition = False

    def __str__(self):
        return f"[[{self.__class__.__name__}]] desc:{self.descriptor!r} req:{self.request!r} exit:{self.exit_msg!r}"

    @property
    def descriptor(self):
        return self._descriptor

    def _mk_sh_proc_create(self, the_tag, which_node):

        the_env = self.request.env
        for k, v in self.process_parms.env().items():
            the_env[k] = v

        # if the parent process wants output or input redirected,
        # we encapsulate necessary messages here to be sent with the
        # SHProcessCreate. All requests are then handled at once by
        # Local Services.


        fake_reply_channel = dutil.AbsorbingChannel()
        if self.request.pipesize is not None and self.request.pipesize <= 0:
            capacity = None
        else:
            capacity = self.request.pipesize

        if self.request.stdin == dmsg.PIPE:
            puid = self.process_parms.my_puid

            channel_options = lsopt.ChannelOptions(capacity=capacity)
            options = channel_desc.ChannelOptions(ref_count=True, local_opts=channel_options)
            req_msg = dmsg.GSChannelCreate(tag=self.server.tag_inc(), p_uid=puid,
                                r_c_uid=dfacts.GS_INPUT_CUID, m_uid=dfacts.infrastructure_pool_muid_from_index(which_node),
                                user_name="stdin_"+str(puid), options=options)
            _, _, self.stdin_context = channel_int.ChannelContext.construct(self.server, req_msg, fake_reply_channel,
                                node_override=self.process_parms.index, send_msg=False)
            self.stdin_context.incref()
            stdin_msg = self.stdin_context.shchannelcreate_msg
        else:
            stdin_msg = None


        if self.request.stdout == dmsg.PIPE:
            puid = self.process_parms.my_puid

            channel_options = lsopt.ChannelOptions(capacity=capacity)
            options = channel_desc.ChannelOptions(ref_count=True, local_opts=channel_options)
            req_msg = dmsg.GSChannelCreate(tag=self.server.tag_inc(), p_uid=puid,
                                r_c_uid=dfacts.GS_INPUT_CUID, m_uid=dfacts.infrastructure_pool_muid_from_index(which_node),
                                user_name="stdout_"+str(puid), options=options)
            _, _, self.stdout_context = channel_int.ChannelContext.construct(self.server, req_msg, fake_reply_channel,
                                node_override=self.process_parms.index, send_msg=False)
            self.stdout_context.incref()
            stdout_msg = self.stdout_context.shchannelcreate_msg
        else:
            stdout_msg = None

        if self.request.stderr == dmsg.PIPE:
            puid = self.process_parms.my_puid

            channel_options = lsopt.ChannelOptions(capacity=capacity)
            options = channel_desc.ChannelOptions(ref_count=True, local_opts=channel_options)
            req_msg = dmsg.GSChannelCreate(tag=self.server.tag_inc(), p_uid=puid,
                                r_c_uid=dfacts.GS_INPUT_CUID, m_uid=dfacts.infrastructure_pool_muid_from_index(which_node),
                                user_name="stderr_"+str(puid), options=options)
            _, _, self.stderr_context = channel_int.ChannelContext.construct(self.server, req_msg, fake_reply_channel,
                                node_override=self.process_parms.index, send_msg=False)
            self.stderr_context.incref()
            stderr_msg = self.stderr_context.shchannelcreate_msg
        else:
            stderr_msg = None

        return dmsg.SHProcessCreate(tag=the_tag,
                                    p_uid=dfacts.GS_PUID,
                                    r_c_uid=dfacts.GS_INPUT_CUID,
                                    t_p_uid=self.descriptor.p_uid,
                                    exe=self.request.exe,
                                    args=self.request.args,
                                    rundir=self.request.rundir,
                                    env=the_env,
                                    stdin = self.request.stdin,
                                    stdout = self.request.stdout,
                                    stderr=self.request.stderr,
                                    stdin_msg=stdin_msg,
                                    stdout_msg=stdout_msg,
                                    stderr_msg=stderr_msg,
                                    pmi_info=self.request._pmi_info)  #pylint: disable=protected-access

    def mk_sh_proc_kill(self, the_tag, the_sig=signal.SIGKILL):
        return dmsg.SHProcessKill(tag=the_tag,
                                  p_uid=dfacts.GS_PUID,
                                  r_c_uid=dfacts.GS_INPUT_CUID,
                                  t_p_uid=self.descriptor.p_uid,
                                  sig=the_sig)

    @classmethod
    def construct(cls, server, msg, reply_channel, head, send_msg=True, belongs_to_group=False, addition=False):
        """Makes a new context, registers it with the server, and sends a request start message

        :param server: global server context object
        :type server: dragon.globalservices.server.GlobalContext
        :param msg: GSProcessCreate message
        :type msg: dragon.infrastructure.messages.GSProcessCreate
        :param reply_channel: the handle needed to reply to this message
        :type reply_channel: dragon.infrastructure.connection.Connection
        :param head: whether this is the head process or not
        :type head: bool
        :param send_msg: whether to issue and send the response message or not, defaults to True
        :type send_msg: bool, optional
        :param belongs_to_group: whether this process is a member of a group or not, defaults to False
        :type belongs_to_group: bool, optional
        :param addition: whether this was called by gs.group_int.create_add() or gs.group_int.construct(), defaults to False
        :type addition: bool, optional
        :return: a tuple:
                First element: True if there is a pending continuation, False otherwise (see the reply channel)
                Second element: The tag if message issued, or the error message if there was a failure, None otherwise
                Third element: The newly constructed context object if issued, None otherwise
        :rtype: tuple
        """
        if msg.user_name in server.process_names:
            LOG.info(f'process name {msg.user_name} in use')
            existing_ctx = server.process_table[server.process_names[msg.user_name]]
            if send_msg:
                rm = dmsg.GSProcessCreateResponse(tag=server.tag_inc(),
                                                  ref=msg.tag,
                                                  err=dmsg.GSProcessCreateResponse.Errors.ALREADY,
                                                  desc=existing_ctx.descriptor)
                reply_channel.send(rm.serialize())
            return False, 'already', existing_ctx

        this_puid, auto_name = server.new_puid_and_default_name()

        if head:
            LOG.info(f'head puid is {this_puid}')
            server.head_puid = this_puid
            server._state = server.RunState.HAS_HEAD

        if not msg.user_name:
            msg.user_name = auto_name

        which_node = server.choose_shepherd(msg)

        context = cls(server=server, request=msg, reply_channel=reply_channel,
                      p_uid=this_puid, node=which_node)
        server.process_names[msg.user_name] = this_puid
        server.process_table[this_puid] = context

        if belongs_to_group:
            context.belongs_to_group = belongs_to_group
            if addition:
                context.group_addition = addition

        outbound_tag = None

        # start making the infrastructure channels if they are needed,
        # otherwise just start the process directly.
        if msg.options.make_inf_channels:
            LOG.debug(f'making inf channels for {which_node}')
            fake_reply_channel = dutil.AbsorbingChannel()
            gsr_msg = dmsg.GSChannelCreate(tag=server.tag_inc(), p_uid=0, r_c_uid=0,
                                           m_uid=dfacts.infrastructure_pool_muid_from_index(which_node))

            issued, tag, chan_context = channel_int.ChannelContext.construct(server, gsr_msg,
                                                                             fake_reply_channel)

            context.gs_ret_channel_context = chan_context

            if not issued:
                LOG.info(f'failed creating gs ret channel for puid {this_puid}')
                context.descriptor.state = process_desc.ProcessDescriptor.State.DEAD
                err_msg = 'gs channel create fail'
                if send_msg:
                    rm = dmsg.GSProcessCreateResponse(tag=server.tag_inc(),
                                                      ref=msg.tag,
                                                      err=dmsg.GSProcessCreateResponse.Errors.FAIL,
                                                      err_info=err_msg)
                    context.reply_channel.send(rm.serialize())
                return False, err_msg, context
            else:
                server.pending[tag] = context.check_channel_const
                return False, None, context
        else:
            context.gs_ret_channel_context = None
            outbound_tag = context.send_start()

            # if it does not belong to a group issue a pending completion now
            if not belongs_to_group:
                server.pending[outbound_tag] = context.complete_construction

            return True, outbound_tag, context

    def send_start(self):
        # send request to shep, remember pending process.
        shep_hdl = self.server.shep_inputs[self.descriptor.node]

        outbound_tag = self.server.tag_inc()
        shep_req = self._mk_sh_proc_create(outbound_tag, self._descriptor.node)

        # In cases where we are sending a large amount of
        # messages, such as with the GSGroupCreate handler,
        # we can fill the GS Input Queue with responses and
        # basically cause the GS / TA / LS to be unable to
        # send/receive any messages. To prevent this, we'll
        # enqueue pending sends and interleave sending and
        # receiving messages to allow us to process responses
        # on the input queue.

        self.server.pending_sends.put((shep_hdl, shep_req.serialize()))
        LOG.debug(f'request {shep_req} to shep')
        return outbound_tag

    def check_channel_const(self, msg):
        """This is the channel construction message - see if that worked and if so
        continue on maybe to finishing up the construction
        """
        LOG.debug('checking inf channel construction')
        channel_constructed = self.gs_ret_channel_context.complete_construction(msg)

        if self.belongs_to_group:
            guid, (_, _) = self.server.resource_to_group_map[self.descriptor.p_uid]
            group_context = self.server.group_table[guid]

        if not channel_constructed:
            LOG.info(f'failed post issue creating gs ret channel for puid {self.descriptor.p_uid}')
            self.descriptor.state = process_desc.ProcessDescriptor.State.DEAD
            rm = dmsg.GSProcessCreateResponse(tag=self.server.tag_inc(),
                                                ref=self.request.tag,
                                                err=dmsg.GSProcessCreateResponse.Errors.FAIL,
                                                err_info='gs channel create fail, post issue')
            if self.belongs_to_group:
                self.server.group_to_pending_resource_map[(self.request.tag, guid)] = self
                group_context._construction_helper(rm)
            else:
                self.reply_channel.send(rm.serialize())
            return

        sdesc = self.gs_ret_channel_context.descriptor.sdesc

        self.process_parms.gs_ret_cd = B64.bytes_to_str(sdesc)

        outbound_tag = self.send_start()
        if self.belongs_to_group:
            self.server.group_to_pending_resource_map[(outbound_tag, guid)] = self
            if self.group_addition:
                self.server.pending[outbound_tag] = group_context.complete_addition
            else:
                self.server.pending[outbound_tag] = group_context.complete_construction
        else:
            self.server.pending[outbound_tag] = self.complete_construction

    def _send_gspingproc(self):
        LOG.debug(f'sending ping to new process {self.descriptor.p_uid}')

        self.descriptor.gs_ret_cuid = self.gs_ret_channel_context.descriptor.c_uid

        pingmsg = dmsg.GSPingProc(tag=self.server.tag_inc(),
                                  mode=self.request.options.mode,
                                  argdata=self.request.options.argdata)

        # Perf issue: how long it takes to
        # attach to a remote channel and send this stuff.
        # The head of queue blocking could get troublesome if
        # there is a long wait here.  We could
        # attach to the channel and load the ping in it
        # ahead of actually starting the process if we wanted to.

        dsd = self.gs_ret_channel_context.descriptor.sdesc
        ping_chan = dch.Channel.attach(dsd)
        ping_con = dconn.Connection(outbound_initializer=ping_chan, policy=dp.POLICY_INFRASTRUCTURE)
        self.gs_ret_channel_context.attached_connection = ping_con
        ping_con.send(pingmsg.serialize())
        LOG.debug('ping sent, channel and connection kept for later')
        # note that if the argdata mode is ArgMode.PYTHON_CHANNEL the
        # starting process will be using this channel to deliver the argdata
        # directly.  There is no race because this must be complete before
        # any GS transactions get done from the new process

    def complete_construction(self, msg, send_msg=True):
        """Completes construction of a new managed process

        :param msg: SHProcessCreateResponse message
        :type msg: SHProcessCreateResponse
        :param send_msg: whether to send the response message or not, defaults to True
        :type send_msg: bool, optional
        :raises RuntimeError: when the message error is of unknown type
        :return: True or False according to success
        :rtype: bool
        """
        if dmsg.SHProcessCreateResponse.Errors.SUCCESS == msg.err:
            self.descriptor.state = process_desc.ProcessDescriptor.State.ACTIVE

            if self.stdin_context is not None:
                channel_constructed = self.stdin_context.complete_construction(msg.stdin_resp)

                if not channel_constructed:
                    err_msg = f'Failed to create stdin channel for {self.descriptor.p_uid}.'
                    LOG.info(err_msg)
                    self.descriptor.state = process_desc.ProcessDescriptor.State.DEAD
                    if send_msg:
                        rm = dmsg.GSProcessCreateResponse(tag=self.server.tag_inc(),
                                                          ref=self.request.tag,
                                                          err=dmsg.GSProcessCreateResponse.Errors.FAIL,
                                                          err_info=err_msg)
                        self.reply_channel.send(rm.serialize())
                    return False

                # After line below, refcnt is 2. This is necessary to keep the channel from
                # being deleted should the process exit before the parent has had a chance to
                # read its output. The refcnt is decremented again once the connection is handed
                # off to the parent process.
                self.stdin_context.refcnt += 1
                stdin_sdesc = B64.bytes_to_str(self.stdin_context.descriptor.sdesc)
            else:
                stdin_sdesc = None

            if self.stdout_context is not None:
                channel_constructed = self.stdout_context.complete_construction(msg.stdout_resp)

                if not channel_constructed:
                    err_msg = f'Failed to create stdout channel for {self.descriptor.p_uid}.'
                    LOG.info(err_msg)
                    self.descriptor.state = process_desc.ProcessDescriptor.State.DEAD
                    if send_msg:
                        rm = dmsg.GSProcessCreateResponse(tag=self.server.tag_inc(),
                                                          ref=self.request.tag,
                                                          err=dmsg.GSProcessCreateResponse.Errors.FAIL,
                                                          err_info=err_msg)
                        self.reply_channel.send(rm.serialize())
                    return False

                # After line below, refcnt is 2. This is necessary to keep the channel from
                # being deleted should the process exit before the parent has had a chance to
                # read its output. The refcnt is decremented again once the connection is handed
                # off to the parent process.
                self.stdout_context.refcnt += 1
                stdout_sdesc = B64.bytes_to_str(self.stdout_context.descriptor.sdesc)
            else:
                stdout_sdesc = None

            if self.stderr_context is not None:
                channel_constructed = self.stderr_context.complete_construction(msg.stderr_resp)

                if not channel_constructed:
                    err_msg = f'Failed to create stderr channel for {self.descriptor.p_uid}.'
                    LOG.info(err_msg)
                    self.descriptor.state = process_desc.ProcessDescriptor.State.DEAD
                    if send_msg:
                        rm = dmsg.GSProcessCreateResponse(tag=self.server.tag_inc(),
                                                          ref=self.request.tag,
                                                          err=dmsg.GSProcessCreateResponse.Errors.FAIL,
                                                          err_info=err_msg)
                        self.reply_channel.send(rm.serialize())
                    return False

                # After line below, refcnt is 2. This is necessary to keep the channel from
                # being deleted should the process exit before the parent has had a chance to
                # read its output. The refcnt is decremented again once the connection is handed
                # off to the parent process.
                self.stderr_context.refcnt += 1
                stderr_sdesc = B64.bytes_to_str(self.stderr_context.descriptor.sdesc)
            else:
                stderr_sdesc = None

            self.descriptor.stdin_sdesc = stdin_sdesc
            self.descriptor.stdout_sdesc = stdout_sdesc
            self.descriptor.stderr_sdesc = stderr_sdesc

            if self.descriptor.p_p_uid in self.server.process_table:
                parent_ctx = self.server.process_table[self.descriptor.p_p_uid]
                parent_ctx.descriptor.live_children.add(self.descriptor.p_uid)

            # if we made a gs return channel then we must send a GSPingProc to it.
            if self.request.options.make_inf_channels:
                self._send_gspingproc()

            if send_msg:
                response = dmsg.GSProcessCreateResponse(tag=self.server.tag_inc(),
                                                        ref=self.request.tag,
                                                        err=dmsg.GSProcessCreateResponse.Errors.SUCCESS,
                                                        desc=self.descriptor)
                self.reply_channel.send(response.serialize())
            succeeded = True
        elif dmsg.SHProcessCreateResponse.Errors.FAIL == msg.err:
            self.descriptor.state = process_desc.ProcessDescriptor.State.DEAD

            # clean up tables - don't keep ProcessDescriptor stuff around
            # for things that never were alive.
            if self.descriptor.p_uid == self.server.head_puid:
                LOG.info('head process creation failed')
                # TODO: whatever cleanup actions desired when the head process leaves.
                self.server._state = self.server.RunState.WAITING_FOR_HEAD
                self.server.head_puid = None

            del self.server.process_table[self.descriptor.p_uid]
            del self.server.process_names[self.request.user_name]

            if self.request.options.make_inf_channels:
                # TODO: can we bind this in a cleaner way?
                clean_msg = dmsg.GSChannelDestroy(tag=self.server.tag_inc(), r_c_uid=0,
                                                  p_uid=dfacts.GS_PUID,
                                                  c_uid=self.gs_ret_channel_context.descriptor.c_uid)
                channel_int.ChannelContext.destroy(self.server, clean_msg, dutil.AbsorbingChannel())

            if send_msg:
                response = dmsg.GSProcessCreateResponse(tag=self.server.tag_inc(),
                                                        ref=self.request.tag,
                                                        err=dmsg.GSProcessCreateResponse.Errors.FAIL,
                                                        err_info=msg.err_info)
                self.reply_channel.send(response.serialize())
            succeeded = False
        else:
            raise RuntimeError(f'got {msg!s} err {msg.err} unknown')

        if send_msg:
            LOG.debug(f'create response sent, tag {response.tag} ref {response.ref} pending cleared')

        return succeeded

    @staticmethod
    def kill(server, msg, reply_channel, belongs_to_group=False):
        target_uid, found, errmsg = server.resolve_puid(msg.user_name, msg.t_p_uid)
        gspkr = dmsg.GSProcessKillResponse

        if not found:
            rm = gspkr(tag=server.tag_inc(),
                       ref=msg.tag,
                       err=gspkr.Errors.UNKNOWN,
                       err_info=errmsg)
            reply_channel.send(rm.serialize())
            LOG.debug(f'process absent; response to {msg}: {rm}')
            return False, None
        else:
            pctx = server.process_table[target_uid]
            pdesc = pctx.descriptor
            pds = process_desc.ProcessDescriptor.State
            if pds.DEAD == pdesc.state:
                rm = gspkr(tag=server.tag_inc(),
                           ref=msg.tag,
                           err=gspkr.Errors.DEAD,
                           exit_code=pdesc.ecode)
                reply_channel.send(rm.serialize())
                LOG.debug(f'process dead, response to {msg}: {rm}')
                return False, None

            # this is a highly unlikely case to happen
            # because the user must have received a create response back
            # before requesting killing this process, which means that the
            # state of the process is not pending anymore
            # so, what we really want here is to log the case and move on
            elif pds.PENDING == pdesc.state:
                rm = gspkr(tag=server.tag_inc(),
                           ref=msg.tag,
                           err=gspkr.Errors.PENDING,
                           err_info=f'process {target_uid} is pending')
                reply_channel.send(rm.serialize())
                LOG.debug(f'process pending while kill request -- this should not be happening, response to {msg}: {rm}')
                return False, None
            elif pds.ACTIVE == pdesc.state:
                pctx.descriptor.state = pds.PENDING
                pctx.destroy_request = msg
                pctx.reply_channel = reply_channel
                the_tag = server.tag_inc()
                shep_kill_msg = pctx.mk_sh_proc_kill(the_tag, msg.sig)
                target_node = pdesc.node
                if not belongs_to_group:
                    server.pending[the_tag] = pctx.complete_kill

                server.pending_sends.put((server.shep_inputs[target_node], shep_kill_msg.serialize()))
                LOG.debug(f'kill {msg} sent to shep as {shep_kill_msg} on node {target_node}')
                return True, the_tag
            else:
                raise NotImplementedError('close case')

    def complete_kill(self, msg):
        gspkr = dmsg.GSProcessKillResponse
        shpkr = dmsg.SHProcessKillResponse

        if shpkr.Errors.FAIL == msg.err:
            rm = gspkr(tag=self.server.tag_inc(), ref=self.destroy_request.tag,
                       err=gspkr.Errors.FAIL_KILL, err_info=msg.err_info)
            kill_succeeded = False
        elif shpkr.Errors.SUCCESS == msg.err:

            rm = gspkr(tag=self.server.tag_inc(),
                       ref=self.destroy_request.tag,
                       err=gspkr.Errors.SUCCESS)

            kill_succeeded = True
        else:
            raise NotImplementedError('close case')

        if self.descriptor.state == self.descriptor.State.PENDING:
            self.descriptor.state = self.descriptor.State.ACTIVE

        LOG.debug(f'sending kill response to request {self.destroy_request}: {rm}')
        self.reply_channel.send(rm.serialize())
        return kill_succeeded
