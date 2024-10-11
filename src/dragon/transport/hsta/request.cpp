#include "agent.hpp"
#include "channel.hpp"
#include "dragon/channels.h"
#include "dragon/global_types.h"
#include "dragon/return_codes.h"
#include "globals.hpp"
#include "magic_numbers.hpp"
#include "request.hpp"
#include "utils.hpp"

static std::unordered_set<dragonGatewayMessage_t *> active_gw_msgs;
static std::unordered_map<dragonGatewayMessage_t *, std::string> gw_msg_backtraces;

static void
check_gw_msg_is_not_active(dragonGatewayMessage_t *gw_msg)
{
    auto is_active = (active_gw_msgs.find(gw_msg) != active_gw_msgs.end());
    if (is_active) {
        fprintf(
            hsta_dbg_file,
            "error: double completion of gateway message\n"
            "%s" // current backtrace
            "backtrace from previous completion:\n%s",
            hsta_utils.get_backtrace().c_str(),
            gw_msg_backtraces[gw_msg].c_str()
        );
        hsta_utils.graceful_exit(SIGINT);
    }
}

static void
check_gw_msg_is_active(dragonGatewayMessage_t *gw_msg)
{
    auto is_inactive = (active_gw_msgs.find(gw_msg) == active_gw_msgs.end());
    if (is_inactive) {
        fprintf(
            hsta_dbg_file,
            "error: freeing gateway message before completing it\n"
            "%s" // current backtrace
            "backtrace from previous completion:\n%s",
            hsta_utils.get_backtrace().c_str(),
            gw_msg_backtraces[gw_msg].c_str()
        );
        hsta_utils.graceful_exit(SIGINT);
    }
}

void WorkRequest::init(Agent *agent,
                       ProtocolType protocol_type,
                       ReqType type,
                       RmaIov *rma_iov,
                       int remote_nid,
                       Header *header,
                       dragonGatewayMessage_t *gw_msg,
                       GatewayChan *gw_ch,
                       bool is_complete)
{
    this->agent         = agent;
    this->type          = type;
    this->protocol_type = protocol_type;
    this->rma_iov       = rma_iov;
    this->nid           = remote_nid;
    this->target_ch     = nullptr;
    this->gw_msg        = gw_msg;
    this->gw_ch         = gw_ch;
    this->is_complete   = is_complete;

    if (gw_msg != nullptr)
    {
        agent->get_cuid_and_port(gw_msg, remote_nid, protocol_type, this->c_uid, this->port);

        // set timeout

        auto nsec_per_sec = (uint64_t) 1e9;
        timespec_t timeout_ts;

        if (gw_msg->deadline.tv_sec == 0l && gw_msg->deadline.tv_nsec == 0l) {
            this->timeout = 0ul;
        } else {
            auto dragon_err = dragon_timespec_remaining(&gw_msg->deadline, &timeout_ts);
            if (dragon_err != DRAGON_TIMEOUT) {
                hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this,
                                "getting timeout from deadline");
            }

            this->timeout = ((uint64_t) timeout_ts.tv_sec * nsec_per_sec) + timeout_ts.tv_nsec;
        }

        // set clientid and hints if necessary

        if (protocol_type == PROTOCOL_TYPE_SENDMSG) {
            dragonMessageAttr_t mattrs;

            auto dragon_err =
                dragon_channel_message_getattr(&gw_msg->send_payload_message, &mattrs);
            hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS,
                                     "getting send payload message attributes");

            this->clientid = mattrs.clientid;
            this->hints    = mattrs.hints;
        } else {
            this->clientid = HSTA_INVALID_CLIENTID_HINTS;
            this->hints    = HSTA_INVALID_CLIENTID_HINTS;
        }

        ++hsta_my_agent->num_pending_work_reqs;
        ++hsta_my_agent->total_work_reqs;
    } else {
        hsta_dbg_assert(header != nullptr);
        this->clientid = header->get_clientid();
        this->hints    = header->get_hints();
    }

    // these are overwritten in [recv_from,poll]_channel
    this->is_response         = false;
    this->free_dragon_msg_buf = false;

#ifndef HSTA_NDEBUG
    if (dragon_hsta_debug) {
        if (gw_msg == nullptr) {
            // this is an ack, so use the seqnum and checksum found in the header
            this->seqnum   = header->seqnum;
            this->checksum = header->checksum;
            this->cuid     = header->c_uid;
        }

        this->already_completed = false;

        switch (type) {
            case WORK_REQ_TYPE_SENDMSG: ++agent->tx_stats.total_reqs; break;
            case WORK_REQ_TYPE_GETMSG:  ++agent->getmsg_stats.total_reqs; break;
            case WORK_REQ_TYPE_POLL:    ++agent->poll_stats.total_reqs; break;
            case WORK_REQ_TYPE_ERR:     ++agent->err_stats.total_reqs; break;
            default:                    hsta_default_case(this->type);
        }
    }
#endif // !HSTA_NDEBUG

    if (is_complete)
    {
        this->complete(nullptr);
    }
}

// TODO: does cqe_resp make sense in this case? it might be a response, but not always.
void WorkRequest::send_complete(CqEvent *cqe_resp)
{
    dragonError_t gw_dragon_err;

    if (cqe_resp != nullptr) {
        gw_dragon_err = cqe_resp->dragon_err;
    } else {
        gw_dragon_err = DRAGON_SUCCESS;
    }

    if (this->gw_msg != nullptr) {
        if (dragon_hsta_debug) {
            check_gw_msg_is_not_active(gw_msg);
            active_gw_msgs.insert(gw_msg);
            gw_msg_backtraces[gw_msg] = hsta_utils.get_backtrace();
        }

        auto derr = DRAGON_SUCCESS;
        derr = dragon_channel_gatewaymessage_transport_start_send_cmplt(
            this->gw_msg,
            gw_dragon_err,
            &this->completion_deadline
        );
        hsta_dbg_errcheck(derr, DRAGON_SUCCESS, cqe_resp, "completing gateway send request");

        hsta_my_agent->pending_work_req_completions.push_back(this);
    } else if (this->free_dragon_msg_buf) {
        // free dragon msg buf allocated for getmsg response
        auto derr = DRAGON_SUCCESS;
        derr = dragon_channel_message_destroy(&this->dragon_msg, true);
        hsta_dbg_errcheck(derr, DRAGON_SUCCESS, this, "destroying getmsg message");

        hsta_my_agent->pending_getmsg_bytes -= this->rma_iov->core.payload_size;
        hsta_my_agent->wreq_objq.push_back(this);
    }

    if (dragon_hsta_debug) {
        ++this->agent->tx_stats.num_complete;
    }
}

void WorkRequest::get_msg_complete(CqEvent *cqe_resp)
{
    if (dragon_hsta_debug) {
        hsta_dbg_assert(this->gw_msg != nullptr);
        check_gw_msg_is_not_active(gw_msg);
        active_gw_msgs.insert(gw_msg);
        gw_msg_backtraces[gw_msg] = hsta_utils.get_backtrace();
    }

    dragonMessage_t msg;
    dragonMemoryDescr_t *mem_descr = nullptr;

    // complete get_msg

    if (cqe_resp->header->get_type() != HEADER_TYPE_CTRL_ERR) {
        hsta_dbg_assert(cqe_resp->dragon_err == DRAGON_SUCCESS);
        mem_descr = &cqe_resp->dragon_msg_buf;
    } else {
        hsta_dbg_assert(cqe_resp->dragon_err != DRAGON_SUCCESS);
    }

    auto header = cqe_resp->header;
    dragonMessageAttr_t mattr{ .hints = header->get_hints(), .clientid = header->get_clientid() };

    auto derr = DRAGON_SUCCESS;

    derr = dragon_channel_message_init(&msg, mem_descr, &mattr);
    hsta_dbg_errcheck(derr, DRAGON_SUCCESS, this, "initializing message to target channel");

    derr = dragon_channel_gatewaymessage_transport_start_get_cmplt(
        this->gw_msg,
        &msg,
        cqe_resp->dragon_err,
        &this->completion_deadline
    );
    hsta_dbg_errcheck(derr, DRAGON_SUCCESS, cqe_resp, "transport agent completing get_msg request");

    hsta_my_agent->pending_work_req_completions.push_back(this);

    derr = dragon_channel_message_destroy(&msg, false);
    hsta_dbg_errcheck(derr, DRAGON_SUCCESS, this, "destroying gateway message");

    // send ack for WHEN_DEPOSITED and WHEN_RECEIVED responses
    auto must_send_ack =
           cqe_resp->header->get_send_return_mode() == DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED
        || cqe_resp->header->get_send_return_mode() == DRAGON_CHANNEL_SEND_RETURN_WHEN_RECEIVED;

    if (must_send_ack) {
        cqe_resp->send_ack(nullptr, sizeof(EventInfo));
    }

#ifndef HSTA_NDEBUG
    if (dragon_hsta_debug) {
        ++this->agent->getmsg_stats.num_complete;
    }
#endif // !HSTA_NDEBUG
}

void WorkRequest::poll_complete(CqEvent *cqe_resp)
{
    if (dragon_hsta_debug) {
        hsta_dbg_assert(this->gw_msg != nullptr);
        check_gw_msg_is_not_active(gw_msg);
        active_gw_msgs.insert(gw_msg);
        gw_msg_backtraces[gw_msg] = hsta_utils.get_backtrace();
    }

    auto derr = DRAGON_SUCCESS;

    derr = dragon_channel_gatewaymessage_transport_start_event_cmplt(
        this->gw_msg,
        this->event_info.result,
        this->event_info.err,
        &this->completion_deadline);
    hsta_dbg_errcheck(derr, DRAGON_SUCCESS, cqe_resp, "transport agent completing event request");

    hsta_my_agent->pending_work_req_completions.push_back(this);

    if (dragon_hsta_debug) {
        ++this->agent->poll_stats.num_complete;
    }
}

void WorkRequest::err_complete(CqEvent *cqe_req)
{
    hsta_my_agent->wreq_objq.push_back(this);

    if (dragon_hsta_debug) {
        ++this->agent->err_stats.num_complete;
    }
}

void WorkRequest::complete(CqEvent *cqe)
{
    if (dragon_hsta_debug) {
        if (this->already_completed) {
            fprintf(
                hsta_dbg_file,
                "error: double completion of work request\n> port=%p, c_uid=%py, seqnum=%lu\n"
                "%s" // current backtrace
                "backtrace from previous completion:\n%s",
                (void *)this->port, (void *)this->cuid, this->seqnum,
                hsta_utils.get_backtrace().c_str(),
                this->completion_backtrace.c_str()
            );
        }
        this->already_completed = true;
        this->completion_backtrace = hsta_utils.get_backtrace();
    }

    switch (this->type)
    {
        case WORK_REQ_TYPE_SENDMSG: {
            this->send_complete(cqe);
            if (cqe != nullptr) {
                hsta_log_history(cqe, "pending SENDMSG work request complete", false, true);
            }
            break;
        }
        case WORK_REQ_TYPE_GETMSG: {
            this->get_msg_complete(cqe);
            if (cqe != nullptr) {
                hsta_log_history(cqe, "pending GETMSG work request complete", false, false);
            }
            break;
        }
        case WORK_REQ_TYPE_POLL: {
            this->poll_complete(cqe);
            if (cqe != nullptr) {
                hsta_log_history(cqe, "pending POLL work request complete", false, false);
            }
            break;
        }
        case WORK_REQ_TYPE_ERR: {
            this->err_complete(cqe);
            if (cqe != nullptr) {
                hsta_log_history(cqe, "pending ERR work request complete", false, true);
            }
            break;
        }
        default: {
            hsta_default_case(this->type);
        }
    }

    // clean up gateway msg if one exists
    if (this->gw_msg != nullptr) {
        --hsta_my_agent->num_pending_work_reqs;
    }
    this->is_complete = true;
}

bool WorkRequest::check_gw_complete()
{
    auto derr = DRAGON_SUCCESS;
    auto gw_msg = this->gw_msg;

    switch (this->type) {
        case WORK_REQ_TYPE_SENDMSG: {
            derr = dragon_channel_gatewaymessage_transport_check_send_cmplt(
                gw_msg,
                &this->completion_deadline
            );
            break;
        }
        case WORK_REQ_TYPE_GETMSG: {
            derr = dragon_channel_gatewaymessage_transport_check_get_cmplt(
                gw_msg,
                &this->completion_deadline
            );
            break;
        }
        case WORK_REQ_TYPE_POLL: {
            derr = dragon_channel_gatewaymessage_transport_check_event_cmplt(
                gw_msg,
                &this->completion_deadline
            );
            break;
        }
        default: {
            // WORK_REQ_TYPE_ERR is handled here, since it should never happen
            hsta_default_case(this->type);
        }
    }

    switch (derr) {
        case DRAGON_SUCCESS:
        case DRAGON_TIMEOUT: {
            if (dragon_hsta_debug) {
                hsta_dbg_assert(gw_msg != nullptr);
                check_gw_msg_is_active(gw_msg);
                active_gw_msgs.erase(gw_msg);
                gw_msg_backtraces.erase(gw_msg);
            }
            dragon_channel_gatewaymessage_destroy(gw_msg);
            free(gw_msg);
            hsta_my_agent->wreq_objq.push_back(this);
            return true;
        }
        case DRAGON_EAGAIN: {
            return false;
        }
        default: {
            hsta_default_case(derr);
        }
    }

    return false;
}
