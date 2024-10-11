#include "agent.hpp"
#include "channel.hpp"
#include "dragon/return_codes.h"
#include "globals.hpp"
#include "magic_numbers.hpp"
#include "mem_descr.hpp"
#include "request.hpp"
#include "utils.hpp"
#include <cstdio>
#include <string>

// global vars

uint64_t dragon_hsta_max_getmsg_bytes = HSTA_DEFAULT_MAX_GETMSG_MB * HSTA_NBYTES_IN_MB;
uint64_t TargetChan::uid_counter = 0UL;

std::unordered_map<port_t, ObjectRing<CqEvent *>> hsta_cqes_awaiting_resp;

void TargetChan::init(
    Agent *agent,
    dragonULInt c_uid,
    dragonGatewayMessage_t *gw_msg,
    uint8_t *conn_data,
    size_t conn_data_size,
    bool is_local)
{
    this->uid                = this->uid_counter++;
    this->agent              = agent;
    this->c_uid              = c_uid;
    // TODO: think carefully about this to make sure it's always correct
    this->is_local           = is_local;
    this->num_pending_ch_ops = 0UL;

    auto dragon_err = dragon_channel_message_attr_init(&this->mattrs);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "initializing message attributes");

    if (is_local) {
        // extract serialized channel data and sendhid from conn_data
        dragonChannelSerial_t sdesc;
        Network::unpack_conn_data(
            conn_data,
            conn_data_size,
            sdesc,
            this->mattrs.sendhid
        );

        // get dragon channel from serialized descriptor
        dragon_err =
            dragon_channel_attach((const dragonChannelSerial_t *) &sdesc,
                                  &this->dragon_ch);
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "attaching to target channel");

        free(sdesc.data);

        // get dragon memory pool associated with channel
        dragon_err =
            dragon_channel_get_pool((const dragonChannelDescr_t *) &this->dragon_ch,
                                    &this->dragon_mem_pool);
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "getting target channel's memory pool");

        dragon_err = dragon_channel_recvh(&this->dragon_ch,
                                            &this->dragon_recvh,
                                            nullptr);
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "creating receive handle");

        dragon_err = dragon_chrecv_open(&this->dragon_recvh);
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "opening receive handle");
    }
}

void TargetChan::fini()
{
    for (auto& port_sendh: this->dragon_send_handles) {
        auto& sendh = port_sendh.second;
        auto dragon_err = dragon_chsend_close(&sendh);
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "closing send handle");
    }

    dragonMemoryPoolDescr_t dragon_mem_pool;

    auto dragon_err = dragon_channel_get_pool(&this->dragon_ch, &dragon_mem_pool);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "getting target channel's memory pool");

    dragon_err = dragon_memory_pool_detach(&dragon_mem_pool);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "detaching from target channel's memory pool");

    dragon_err = dragon_channel_detach(&this->dragon_ch);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "detaching from target channel");
}

void TargetChan::init_flow(port_t port, dragonChannelSendReturnWhen_t send_return_mode)
{
    if (this->dragon_send_handles.find(port) == this->dragon_send_handles.end()) {
        hsta_dbg_assert(send_return_mode != DRAGON_CHANNEL_SEND_RETURN_WHEN_NONE);

        dragonChannelSendAttr_t sattrs;

        auto dragon_err = dragon_channel_send_attr_init(&sattrs);
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "initializing send attributes");

        auto sendh = &this->dragon_send_handles[port];
        sattrs.return_mode = send_return_mode;

        dragon_err = dragon_channel_sendh(&this->dragon_ch, sendh, &sattrs);
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "creating send handle");

        dragon_err = dragon_chsend_open(sendh);
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "opening send handle");
    }
}

static bool finish_try_send(CqEvent *cqe)
{
    if (   cqe->header->get_send_return_mode() == DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED
        || cqe->header->get_send_return_mode() == DRAGON_CHANNEL_SEND_RETURN_WHEN_RECEIVED)
    {
        // TODO: use send_ack() for getmsg and poll acks too
        cqe->send_ack(nullptr, sizeof(EventInfo));
    }

    cqe->eager_buf->dec_refcount(cqe->eager_buf_ref);

    // free dragon msg buf allocated for local sendmsg
    auto dragon_err = dragon_channel_message_destroy(&cqe->dragon_msg, false);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, cqe->target_ch, "destroying sendmsg message");

    return true;
}

// TODO: move these try_<op> functions into CqEvent and make them
// more generic "completer" functions

bool TargetChan::try_send(CqEvent *cqe)
{
    // make sure the recv buffer has been allocated and the channel
    // message initialized before we continue
    if (!cqe->recv_buf_is_allocated && !cqe->dragon_msg_buf_alloc()) {
        if (cqe->check_timeout()) {
            hsta_log_history(
                cqe, "timed out while trying to allocate buffer for payload",
                false, false
            );
            return finish_try_send(cqe);
        }
        return false;
    }

    // make sure the payload has arrived before we continue
    if (!cqe->payload_has_arrived) {
        if (!cqe->recv_is_queued) {
            hsta_my_agent->network.queue_recv(cqe);
            cqe->recv_is_queued = true;
        }
        return false;
    }

    // verify the integrity of the payload, that it's ordered correctly,
    // and that it's going to the right channel
    // TODO: this should be improved/expanded upon, but it works for now
    if (dragon_hsta_debug && cqe->verify_payload) {
        cqe->verify_payload = false;

        auto seqnum = ++this->seqnum[cqe->header->port];
        auto checksum = hsta_utils.hash_array((uint8_t *)cqe->dst_addr, cqe->size);

        hsta_dbg_assert(this->c_uid == cqe->header->c_uid);
        hsta_dbg_assert(seqnum      == cqe->header->seqnum);
        hsta_dbg_assert(checksum    == cqe->header->checksum);
    }

    if (!cqe->ch_msg_is_initialized) {
        auto header = cqe->header;

        this->mattrs.clientid = header->get_clientid();
        this->mattrs.hints    = header->get_hints();

        auto dragon_err =
            dragon_channel_message_init(&cqe->dragon_msg, &cqe->dragon_msg_buf, &this->mattrs);
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this,
                          "initializing message to target channel");

        cqe->ch_msg_is_initialized = true;
    }

    const timespec_t timeout = {0, 0};

    cqe->dragon_err =
        dragon_chsend_send_msg(&this->dragon_send_handles[cqe->header->port],
                               &cqe->dragon_msg,
                               cqe->dst_mem_descr,
                               &timeout);

    if (cqe->dragon_err == DRAGON_CHANNEL_FULL) {
        if (!cqe->check_timeout()) {
            return false;
        }
    } else {
        hsta_dbg_errcheck(cqe->dragon_err, DRAGON_SUCCESS, this,
                          "sending message to target channel");
    }

    if (dragon_hsta_debug) {
        char tmp[100];
        sprintf(tmp, "completed send_msg to target channel: err = %d, %s", cqe->dragon_err, dragon_get_rc_string(cqe->dragon_err));
        hsta_log_history(cqe, tmp, false, true);
    }

    return finish_try_send(cqe);
}

void TargetChan::send_to_channel(CqEvent **cqe_inout)
{
    auto *cqe = *cqe_inout;

    cqe->check_timeout();

    if (cqe->header->get_type() == HEADER_TYPE_DATA_EAGER) {
        cqe->dst_mem_descr = nullptr;
    } else {
        cqe->dst_mem_descr = (dragonMemoryDescr_t *) DRAGON_CHANNEL_SEND_TRANSFER_OWNERSHIP;
    }

    // update bookkeeping for pending channel ops

    auto *pending_ch_ops = &this->pending_ch_ops[PROTOCOL_TYPE_SENDMSG];
    hsta_push_back(pending_ch_ops, cqe);
    hsta_cqe_inc_refcount(cqe, pending_ch_ops, "pending_ch_ops");

    ++this->num_pending_ch_ops;
    ++this->agent->num_pending_ch_ops;
    ++this->agent->total_ch_ops;

    this->agent->active_chs.push_back_unique(this);

    // increment refcount to indicate that this eager_buf is busy
    cqe->eager_buf->inc_refcount(cqe->eager_buf_ref);

    hsta_log_history(cqe, "send_to_channel operation queued", false, true);
}

bool TargetChan::try_recv(CqEvent *cqe)
{
    auto getmsg_credits_available = (this->agent->pending_getmsg_bytes <= dragon_hsta_max_getmsg_bytes);

    if (!getmsg_credits_available) {
        return false;
    }

    bool timed_out = false;

    auto dragon_err =
        dragon_chrecv_get_msg((const dragonChannelRecvh_t *) &this->dragon_recvh,
                              &cqe->dragon_msg);

    if (dragon_err == DRAGON_CHANNEL_EMPTY) {
        if (cqe->check_timeout()) {
            timed_out = true;
        } else {
            return false;
        }
    } else {
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this,
                          "receiving message from target channel");
    }

    if (dragon_hsta_debug) {
        char tmp[100];
        sprintf(tmp, "completed get_msg from target channel: err = %d, %s", dragon_err, dragon_get_rc_string(dragon_err));
        hsta_log_history(cqe, tmp, false, true);
    }

    dragonError_t save_err = DRAGON_SUCCESS;
    ReqType req_type;
    RmaIov *rma_iov = nullptr;

    if (timed_out) {
        rma_iov = this->agent->network.rma_iov_objq.pull_front(
            this->agent,
            nullptr, // pool_base
            HSTA_INVALID_UINT64, // puid
            0UL, // payload_offset
            sizeof(EventInfo), // payload_size
            false // requires_rkey_setup
        );

        save_err = dragon_err;
        req_type = WORK_REQ_TYPE_ERR;

        dragon_err = dragon_channel_message_destroy(&cqe->dragon_msg, false);
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this,
                          "destroying getmsg message");
    } else {
        // extract the address and size from the dragon msg

        dragon_err =
            dragon_channel_message_get_mem((const dragonMessage_t *) &cqe->dragon_msg,
                                           &cqe->dragon_msg_buf);
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this,
                          "getting memory descriptor from message");

        rma_iov = this->agent->network.get_rma_iov_tx(&cqe->dragon_msg_buf, cqe->nid);
        this->agent->pending_getmsg_bytes += rma_iov->core.payload_size;
        req_type = WORK_REQ_TYPE_SENDMSG;

        // set the clientid and hints

        dragonMessageAttr_t mattrs;

        auto dragon_err = dragon_channel_message_getattr(&cqe->dragon_msg, &mattrs);
        hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS,
                                 "getting response message attributes");

        cqe->header->set_clientid(mattrs.clientid);
        cqe->header->set_hints(mattrs.hints);
    }

    // create work request for the reply message

    auto *work_req =
        this->agent->wreq_objq.pull_front(
            this->agent,
            PROTOCOL_TYPE_GETMSG,
            req_type,
            rma_iov,
            cqe->nid,
            cqe->header,
            nullptr, // gw_msg
            nullptr, // gw_ch
            false    // is_complete
        );

    // TODO: wrap this init logic up in a function
    if (timed_out) {
        work_req->event_info.err = save_err;
        work_req->rma_iov->core.payload_offset = (uint64_t)&work_req->event_info;
        work_req->rma_iov->core.payload_size = sizeof(work_req->event_info);
    }
    work_req->port = cqe->header->port;
    work_req->c_uid = cqe->header->c_uid;
    work_req->target_ch = this;
    work_req->is_response = true;
    work_req->free_dragon_msg_buf = req_type == WORK_REQ_TYPE_SENDMSG;
    work_req->dragon_msg = cqe->dragon_msg;

    this->agent->handle_work_req(work_req);

    return true;
}

void TargetChan::recv_from_channel(CqEvent **cqe_inout)
{
    auto *cqe = *cqe_inout;

    cqe->check_timeout();

    // init message to recv from channel

    auto dragon_err =
        dragon_channel_message_init(&cqe->dragon_msg,
                                    nullptr,
                                    nullptr);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this,
                      "initializing message from target channel");

    // update bookkeeping for pending channel ops

    auto *pending_ch_ops = &this->pending_ch_ops[PROTOCOL_TYPE_GETMSG];
    hsta_push_back(pending_ch_ops, cqe);
    hsta_cqe_inc_refcount(cqe, pending_ch_ops, "pending_ch_ops");

    ++this->num_pending_ch_ops;
    ++this->agent->num_pending_ch_ops;
    ++this->agent->total_ch_ops;

    this->agent->active_chs.push_back_unique(this);

    *cqe_inout = nullptr;

    hsta_log_history(cqe, "recv_from_channel operation queued", false, true);
}

bool is_barrier_return_code(dragonError_t dragon_err)
{
    switch (dragon_err) {
        case DRAGON_BARRIER_BROKEN:
        case DRAGON_BARRIER_WAIT_TRY_AGAIN:
        case DRAGON_BARRIER_READY_TO_RELEASE: {
            return true;
        }
        default: {
            return false;
        }
    }
}

bool TargetChan::try_poll(CqEvent *cqe)
{
    const timespec_t timeout = { 0, 0 };
    dragonULInt result;

    auto dragon_err =
        dragon_channel_poll((const dragonChannelDescr_t *) &this->dragon_ch,
                            DRAGON_SPIN_WAIT, cqe->event_mask, &timeout, &result);
    if (dragon_err == DRAGON_TIMEOUT) {
        if (!cqe->check_timeout()) {
            return false;
        }
    } else {
        if (!is_barrier_return_code(dragon_err)) {
            hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, cqe,
                              "polling target channel");
        }
    }

    if (dragon_hsta_debug) {
        char tmp[100];
        sprintf(tmp, "completed poll of target channel: err = %d, %s", dragon_err, dragon_get_rc_string(dragon_err));
        hsta_log_history(cqe, tmp, false, true);
    }

    // create work request for the reply message

    auto rma_iov = this->agent->network.rma_iov_objq.pull_front(
        this->agent,
        nullptr, // pool_base
        HSTA_INVALID_UINT64, // puid
        0UL, // payload_offset
        sizeof(EventInfo), // payload_size
        false // requires_rkey_setup
    );

    // don't send clientid or hints with the poll response
    cqe->header->clientid = HSTA_INVALID_CLIENTID_HINTS;
    cqe->header->hints    = HSTA_INVALID_CLIENTID_HINTS;

    auto *work_req =
        this->agent->wreq_objq.pull_front(
            this->agent,
            PROTOCOL_TYPE_POLL,
            WORK_REQ_TYPE_ERR,
            rma_iov,
            cqe->nid,
            cqe->header,
            nullptr, // gw_msg
            nullptr, // gw_ch
            false    // is_complete
        );

    // TODO: wrap this init logic up in a function
    work_req->event_info.mask   = cqe->event_mask;
    work_req->event_info.err    = dragon_err;
    work_req->event_info.result = result;

    work_req->rma_iov->core.payload_offset = (uint64_t)&work_req->event_info;
    work_req->port = cqe->header->port;
    work_req->target_ch = this;
    work_req->is_response = true;

    this->agent->handle_work_req(work_req);

    return true;
}

void TargetChan::poll_channel(CqEvent **cqe_inout)
{
    auto *cqe = *cqe_inout;

    cqe->check_timeout();

    auto *event_info = (EventInfo *) cqe->dst_addr;
    cqe->event_mask = event_info->mask;

    // update bookkeeping for pending channel ops

    auto *pending_ch_ops = &this->pending_ch_ops[PROTOCOL_TYPE_POLL];
    hsta_push_back(pending_ch_ops, cqe);
    hsta_cqe_inc_refcount(cqe, pending_ch_ops, "pending_ch_ops");

    ++this->num_pending_ch_ops;
    ++this->agent->num_pending_ch_ops;
    ++this->agent->total_ch_ops;

    this->agent->active_chs.push_back_unique(this);

    *cqe_inout = nullptr;

    hsta_log_history(cqe, "poll operation queued", false, true);
}

bool TargetChan::try_op(CqEvent *cqe)
{
    switch (cqe->header->get_protocol_type()) {
        case PROTOCOL_TYPE_SENDMSG: return try_send(cqe);
        case PROTOCOL_TYPE_GETMSG:  return try_recv(cqe);
        case PROTOCOL_TYPE_POLL:    return try_poll(cqe);
        default:                    hsta_utils.graceful_exit(SIGINT);
    }

    return false;
}

uint64_t TargetChan::process_pending_ch_ops(ProtocolType protocol_type)
{
    // make progress on pending target channel operations

    auto *pending_ch_ops = &this->pending_ch_ops[protocol_type];

    while (auto *cqe = pending_ch_ops->peek_front()) {
        if (this->try_op(cqe)) {
            hsta_pop_front(pending_ch_ops);
            hsta_cqe_dec_refcount(cqe, pending_ch_ops);

            --this->num_pending_ch_ops;
            --this->agent->num_pending_ch_ops;

            if (this->num_pending_ch_ops == 0ul) {
                return 0ul;
            }
        } else {
            break;
        }
    }

    return this->num_pending_ch_ops;
}

bool TargetChan::is_active()
{
    auto num_active_chs = this->agent->active_chs.size();

    for (auto i = 0ul; i < num_active_chs; ++i) {
        if (this == this->agent->active_chs[i]) {
            return true;
        }
    }

    return false;
}

