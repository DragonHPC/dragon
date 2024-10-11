#include "agent.hpp"
#include "gateway.hpp"
#include "request.hpp"
#include "extras.hpp"
#include "globals.hpp"

#define HSTA_USE_PEEK_POP 1

// GatewayChan function definitions

void GatewayChan::init(Agent *agent, char *gw_descr_str)
{
    this->agent = agent;
    this->max_batch_size = 32;

    dragonChannelSerial_t dragon_ch_ser;

    dragon_ch_ser.data = dragon_base64_decode(gw_descr_str,
                                              &dragon_ch_ser.len);

    auto dragon_err =
        dragon_channel_attach((const dragonChannelSerial_t *) &dragon_ch_ser,
                              &this->dragon_gw_ch);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "attaching to gateway channel");

    free(dragon_ch_ser.data);

    dragon_err = dragon_channel_recvh(&this->dragon_gw_ch,
                                      &this->dragon_gw_recvh,
                                      nullptr);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "creating gateway receive handle");

    dragon_err = dragon_chrecv_open(&this->dragon_gw_recvh);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "opening gateway receive handle");
}

void GatewayChan::fini()
{
    auto dragon_err = dragon_chrecv_close(&this->dragon_gw_recvh);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "closing receive handle");

    dragon_err = dragon_channel_detach(&this->dragon_gw_ch);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "detaching from gateway channel");
}

WorkRequest *GatewayChan::create_work_req(dragonGatewayMessage_t *gw_msg)
{
    // queue up work request
    RmaIov *rma_iov = nullptr;
    ProtocolType protocol_type;
    ReqType req_type;
    bool is_complete = false;

    auto nid = hsta_hostid_to_nid[gw_msg->target_hostid];

    switch (gw_msg->msg_kind) {
        case DRAGON_GATEWAY_MESSAGE_SEND: {
            dragonMemoryDescr_t payload;

            auto dragon_err =
                dragon_channel_message_get_mem((const dragonMessage_t *) &gw_msg->send_payload_message,
                                               &payload);
            hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "getting memory descriptor from send payload message");

            rma_iov = this->agent->network.get_rma_iov_tx(&payload, nid);

            if (gw_msg->send_return_mode == DRAGON_CHANNEL_SEND_RETURN_IMMEDIATELY) {
                is_complete = true;
            }

            protocol_type = PROTOCOL_TYPE_SENDMSG;
            req_type = WORK_REQ_TYPE_SENDMSG;
            break;
        }
        case DRAGON_GATEWAY_MESSAGE_GET: {
            rma_iov = this->agent->network.rma_iov_objq.pull_front(
                this->agent,
                nullptr, // pool_base
                HSTA_INVALID_UINT64, // puid
                0UL, // payload_offset
                0UL, // payload_size
                false // requires_rkey_setup
            );
            protocol_type = PROTOCOL_TYPE_GETMSG;
            req_type = WORK_REQ_TYPE_GETMSG;
            break;
        }
        case DRAGON_GATEWAY_MESSAGE_EVENT: {
            rma_iov = this->agent->network.rma_iov_objq.pull_front(
                this->agent,
                nullptr, // pool_base
                HSTA_INVALID_UINT64, // puid
                0UL, // payload_offset
                sizeof(EventInfo), // payload_size
                false // requires_rkey_setup
            );
            protocol_type = PROTOCOL_TYPE_POLL;
            req_type = WORK_REQ_TYPE_POLL;
            break;
        }
        default: {
            // set these to remove warning
            protocol_type = PROTOCOL_TYPE_LAST;
            req_type = WORK_REQ_TYPE_LAST;

            hsta_default_case(gw_msg->msg_kind);
        }
    }

    auto *work_req =
        this->agent->wreq_objq.pull_front(
            this->agent,
            protocol_type,
            req_type,
            rma_iov,
            nid,
            nullptr, // no header
            gw_msg,
            this,
            is_complete
        );

    if (gw_msg->msg_kind == DRAGON_GATEWAY_MESSAGE_EVENT) {
        work_req->event_info.mask = gw_msg->event_mask;
        work_req->rma_iov->core.payload_offset = (uint64_t)&work_req->event_info;
    }

    return work_req;
}

bool GatewayChan::check_gateway()
{
    dragonMessage_t msg_from_dragon;
    dragonMemoryDescr_t payload;
    dragonGatewayMessage_t *gw_msg = nullptr;
    void *payload_ptr = nullptr;
    size_t payload_size;

    // check for a new gateway message

    auto dragon_err =
        dragon_channel_message_init(&msg_from_dragon, nullptr, nullptr);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "initializing gateway message");

#if HSTA_USE_PEEK_POP
    dragon_err = dragon_chrecv_peek_msg(
        (const dragonChannelRecvh_t *) &this->dragon_gw_recvh,
        &msg_from_dragon
    );
#else
    dragon_err = dragon_chrecv_get_msg(
        (const dragonChannelRecvh_t *) &this->dragon_gw_recvh,
        &msg_from_dragon
    );
#endif

    if (   dragon_err == DRAGON_CHANNEL_EMPTY
        || dragon_err == DRAGON_DYNHEAP_REQUESTED_SIZE_NOT_AVAILABLE)
    {
        dragon_err =
            dragon_channel_message_destroy(&msg_from_dragon, false);
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "destroying gateway message");

        return false;
    } else {
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "receiving gateway message");

#if HSTA_USE_PEEK_POP
        auto dragon_err =
            dragon_chrecv_pop_msg((const dragonChannelRecvh_t *) &this->dragon_gw_recvh);
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "popping gateway message");
#endif
    }

    // get the address and size of the serialized message

    dragon_err =
        dragon_channel_message_get_mem((const dragonMessage_t *) &msg_from_dragon,
                                        &payload);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "getting gateway message memory descriptor");

    dragon_err = dragon_memory_get_pointer(&payload, &payload_ptr);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "getting pointer for memory descriptor");

    dragon_err = dragon_memory_get_size(&payload, &payload_size);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "getting size for memory descriptor");

    // deserialize to get the actual gateway message

    dragonGatewayMessageSerial_t gmsg_ser;
    gmsg_ser.len = payload_size;
    gmsg_ser.data = (uint8_t *) payload_ptr;

    // TODO: allocate this from an objq
    gw_msg = (dragonGatewayMessage_t *) malloc(sizeof(dragonGatewayMessage_t));
    hsta_dbg_assert(gw_msg != nullptr);

    dragon_err = dragon_channel_gatewaymessage_attach(&gmsg_ser, gw_msg);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "attaching to gateway");

    dragon_err = dragon_channel_message_destroy(&msg_from_dragon, true);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this, "destroying gateway message");

    auto *work_req = this->create_work_req(gw_msg);
    hsta_my_agent->handle_work_req(work_req);

    return true;
}

void GatewayChan::check_gw_msgs()
{
    // check for new gateway messages
    int batch_size = 0;
    while (this->check_gateway()) {
        // TODO: take payload size into account when determining batch size
        if (++batch_size == this->max_batch_size) {
            break;
        }
    }
}
