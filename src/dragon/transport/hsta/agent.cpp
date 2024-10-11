#include "agent.hpp"
#include "channel.hpp"
#include "dragon/global_types.h"
#include "dragon_fabric.hpp"
#include "eager.hpp"
#include "globals.hpp"
#include "header.hpp"
#include "magic_numbers.hpp"
#include "mem_descr.hpp"
#include "network.hpp"
#include "obj_ring.hpp"
#include "request.hpp"
#include "utils.hpp"

// globals

Lock hsta_mr_lock("hsta_mr_lock");

// member function implementations

/**
 * @brief Initialize fabric backend and set up transmit and
 *        receive queues.
 */

void Agent::init()
{
    this->num_pending_work_reqs  = 0ul;
    this->total_work_reqs        = 0ul;
    this->num_pending_ch_ops     = 0ul;
    this->total_ch_ops           = 0ul;
    this->sendh_ctr              = 0u;
    this->cuid_ctr               = 0u;
    this->pending_getmsg_bytes   = 0ul;
#ifndef HSTA_NDEBUG
    this->progress_loop_count    = 0ul;
#endif // !HSTA_NDEBUG

    // set up dragon gateway channels

    if (!hsta_dump_net_config) {
        auto dragon_err = dragon_channel_register_gateways_from_env();
        hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS, "failed to register dragon channels");

        this->get_gateway_chans();
    }

    // initialize network

    this->network.init(this);

    // the rest of the initalization isn't needed if we're
    // just dumping fabric config
    if (hsta_dump_net_config) {
        return;
    }

    // allocate transmit and receive queues

    this->num_tx_queues = this->network.num_nodes;
    this->num_rx_queues = HSTA_DEFAULT_NUM_RX_QUEUES;

    // no idea if these sizes make sense, just a guess for now
    auto tx_queue_size = HSTA_TX_QUEUE_SIZE;
    auto rx_queue_size = this->init_rx_queue_size();

    hsta_dbg_assert(Utils::is_power_of_two((uint32_t) tx_queue_size));
    hsta_dbg_assert(Utils::is_power_of_two((uint32_t) rx_queue_size));
    hsta_dbg_assert(Utils::is_power_of_two((uint32_t) this->num_rx_queues));

    this->tx_queues.resize(this->num_tx_queues);
    this->rx_queues.resize(this->num_rx_queues);

    // init tx queues
    for (auto i = 0; i < this->num_tx_queues; ++i) {
        auto *tx_queue = &this->tx_queues[i];

        tx_queue->agent = this;
        tx_queue->nid = i;

        // set the traffic type for each eager buffer

        for (auto j = 0ul; j < tx_queue_size; ++j) {
            tx_queue->new_eager_buf();
        }
    }

    // init rx queues
    for (auto i = 0; i < this->num_rx_queues; ++i) {
        auto *rx_queue = &this->rx_queues[i];

        rx_queue->agent = this;

        // init eager buffers and post initial batch of recvs
        for (auto j = 0ul; j < rx_queue_size; ++j) {
            rx_queue->new_eager_buf();
        }
    }

    // init stats collections

#ifndef HSTA_NDEBUG
    this->tx_stats.total_reqs       = 0ul;
    this->tx_stats.num_complete     = 0ul;
    this->getmsg_stats.total_reqs   = 0ul;
    this->getmsg_stats.num_complete = 0ul;
    this->poll_stats.total_reqs     = 0ul;
    this->poll_stats.num_complete   = 0ul;
    this->err_stats.total_reqs      = 0ul;
    this->err_stats.num_complete    = 0ul;
#endif
}

size_t Agent::init_rx_queue_size()
{
    size_t rx_queue_size = HSTA_RX_QUEUE_FACTOR * Utils::next_power_of_two(this->network.num_nodes);

    if (rx_queue_size > HSTA_RX_QUEUE_SIZE_MAX) {
        rx_queue_size = HSTA_RX_QUEUE_SIZE_MAX;
    }

    return rx_queue_size;
}

/**
 * @brief Allocates a cqe and header, and queues the cqe to be sent
 *
 * @param[in] addr Source address for the send operation
 *
 * @param[in] size Size of the data to be sent
 *
 * @param[in] nid Node id of the destination agent
 *
 * @param[in] port Port associated with the target channel
 *
 * @param[in] header_type Type of the header to be sent
 */

CqEvent *Agent::queue_to_send(const void *addr,
                              size_t size,
                              int nid,
                              dragonULInt c_uid,
                              port_t port,
                              HeaderType header_type,
                              ReqType work_req_type,
                              ProtocolType protocol_type,
                              WorkRequest *work_req)
{
    // TODO: sanity tests for params

    // create header

    auto send_return_mode = DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED;

    dragonGatewayMessage_t *gw_msg = nullptr;
    if (work_req != nullptr) {
        gw_msg = work_req->gw_msg;
    }

    if (gw_msg != nullptr) {
        send_return_mode = gw_msg->send_return_mode;
    }

    dragonULInt timeout;
    dragonULInt clientid;
    dragonULInt hints;

    if (work_req != nullptr) {
        timeout = work_req->timeout;
        clientid = work_req->clientid;
        hints = work_req->hints;
    } else {
        timeout = 0UL;
        clientid = 0UL;
        hints = 0UL;
    }

    // this is to make sure we always get an ack for rndv sends
    // when using the ofi_rma backend, since the rndv send is a
    // no-op and we can't add the cqe to the relevant flow

    auto transport_type = this->network.dfabric->transport_type;

    auto is_buffered_send =
           transport_type   == DFABRIC_TRANSPORT_TYPE_RMA
        && work_req_type    == WORK_REQ_TYPE_SENDMSG
        && header_type      == HEADER_TYPE_CTRL_RTS
        && send_return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED;

    if (is_buffered_send) {
        send_return_mode = DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED;
    }

    auto *header =
        this->header_objq.pull_front(
            header_type,
            protocol_type,
            send_return_mode,
            timeout,
            size,
            c_uid,
            port,
            clientid,
            hints
        );

    // create and queue up cqe

    auto *cqe =
        this->cqe_objq.pull_front(
            header,
            nid,
            (void *) addr,  // src_addr
            nullptr,        // dst_addr
            size,
            false,          // is_aggregated
            nullptr,        // eager_buf
            work_req,
            this,
            TRAFFIC_TYPE_TX
        );

    return cqe;
}

// TODO: create a single cqe at init for each eager buf and update as necessary
CqEvent *Agent::create_cqe_aggregated(EagerBuf *eager_buf)
{
    void *src_addr = nullptr;
    void *dst_addr = nullptr;
    size_t size;
    int nid;
    dragonULInt c_uid = HSTA_INVALID_UINT64;
    port_t port = HSTA_INVALID_UINT64;

    if (eager_buf->traffic_type == TRAFFIC_TYPE_TX) {
        src_addr = eager_buf->payload;
        size     = eager_buf->ag_header->size;
        nid      = eager_buf->target_nid;

        this->get_cuid_and_port(nullptr, eager_buf->target_nid, PROTOCOL_TYPE_AGGREGATED, c_uid, port);
    }
    else {
        hsta_dbg_assert(eager_buf->traffic_type == TRAFFIC_TYPE_RX);

        dst_addr = eager_buf->payload;
        size     = HSTA_INVALID_SIZE;
        nid      = HSTA_INVALID_NID;

        this->get_cuid_and_port(nullptr, this->network.this_nid, PROTOCOL_TYPE_AGGREGATED, c_uid, port);
    }

    // TODO: should we use INVALID_UINT64 for the invalid values?
    auto *header =
        this->header_objq.pull_front(
            HEADER_TYPE_DATA_EAGER,
            PROTOCOL_TYPE_AGGREGATED,
            DRAGON_CHANNEL_SEND_RETURN_WHEN_NONE, // aggregated msgs don't have a specific send_return_mode
            0UL,                                  // ... or timeout
            0UL,                                  // ... or eager data (not in the disaggregated sense)
            c_uid,
            port,
            0UL,                                  // ... or clientid
            0UL                                   // ... or hints
        );

    auto *cqe =
        this->cqe_objq.pull_front(
            header,
            nid,
            src_addr,
            dst_addr,
            size,
            true,    // is_aggregated
            eager_buf,
            nullptr, // work_req
            this,
            eager_buf->traffic_type
        );

    eager_buf->cqe_ag = cqe;

    return cqe;
}

//
// progress functions
//

std::string get_gw_name(int i)
{
    auto gw_name_base = std::string(HSTA_DEFAULT_GW_ENV_PREFIX);

    auto gw_name =
          gw_name_base
        + std::to_string(i)
        + std::string("_TIDX")
        + std::to_string(hsta_thread_idx);

    return gw_name;
}

/**
 * @brief Get all gateway channels that the transport agent
 *        needs to poll
 */

void Agent::get_gateway_chans()
{
    auto i = 1;
    auto num_gws_found = 0;

    auto gw_name = get_gw_name(i++);

    auto start_time = this->utils.get_time();
    auto timeout    = (double) 30.0;

    while (getenv(gw_name.c_str()) == nullptr)
    {
        if (this->utils.get_time() - start_time > timeout)
        {
            hsta_dbg_noclass_printf(std::string("timeout while waiting for gateway descriptor"));
            hsta_utils.graceful_exit(SIGINT);
        }
    }

    auto gw_descr_str = getenv(gw_name.c_str());

    while (gw_descr_str)
    {
        auto *gw_chan = this->gw_chan_objq.pull_front(this, gw_descr_str);
        this->gw_chans.push_back(gw_chan);

        hsta_dbg_printf(gw_chan, std::string("found Dragon gateway: ") + gw_name);

        gw_name = get_gw_name(i++);
        gw_descr_str = getenv(gw_name.c_str());

        ++num_gws_found;
    }

    hsta_dbg_assert(num_gws_found >= HSTA_NUM_GW_TYPES);
}

/**
 * @brief Check gateways for new work requests and push them
 *        onto a gateway channel's work queue
 *
 * @return A gateway channel with new work is returned
 */

void Agent::check_gateway_chans()
{
    for (auto i = 0ul; i < this->gw_chans.size(); ++i) {
        this->gw_chans[i]->check_gw_msgs();
    }
}

/**
 * @brief Check for incoming work requests and initiate work
 */

void Agent::handle_work_req(WorkRequest *work_req)
{
    // TODO: sanity tests for wreq

    // if we don't yet have a connection to the remote
    // channel, set that up now

    TargetChan *target_ch = nullptr;

    if (work_req->gw_msg != nullptr) {
        // this is a work request received over a gateway channel, so
        // we need to look up the target_ch based on the protocol type
        // and the port (or create a new target_ch if necessary)
        target_ch = this->chan_from_cuid[work_req->c_uid];

        // if target_ch is null, then we haven't set up the TargetChan object
        // at the target node, forcing this work request to become a multi-part
        // request (CTRL_CONN to handle setup at the target node, followed by
        // the actual request received over the gateway channel)
        if (target_ch == nullptr) {
            // pack the target_ch_ser size and data, as well as the sendhid,
            // into the conn_data vector

            auto gw_msg  = work_req->gw_msg;
            auto sendhid = gw_msg->send_payload_message._attr.sendhid;

            Network::pack_conn_data(
                gw_msg->target_ch_ser,
                sendhid,
                work_req->conn_data
            );

            // create a new hsta channel object to track the port/dragon channel
            target_ch = this->chan_objq.pull_front(
                this,
                work_req->c_uid,
                gw_msg,
                nullptr,
                0UL,
                false
            );

            this->chan_from_cuid[work_req->c_uid] = target_ch;

            // send the port, serialized descriptor and sendhid (if necessary)
            // to the remote agent
            auto *cqe =
                this->queue_to_send(
                    (const void *) &work_req->conn_data[0],
                    work_req->conn_data.size(),
                    work_req->nid,
                    work_req->c_uid,
                    work_req->port,
                    HEADER_TYPE_CTRL_CONN,
                    work_req->type,
                    work_req->protocol_type,
                    nullptr
                );

            auto& tx_queue = this->tx_queues[cqe->nid];
            tx_queue.try_tx(cqe);
        }
    } else {
        // this is a work request generated at the target node to handle
        // an ack or send response data, so the target channel was set in
        // the work request
        target_ch = work_req->target_ch;
    }

    // queue up the first stage of the message protocol

    CqEvent *cqe = nullptr;

    switch (work_req->type)
    {
        case WORK_REQ_TYPE_SENDMSG:
        {
            // queue up the send operation to the receiving agent

            auto header_type = this->get_send_header_type(work_req->rma_iov->core.payload_size);

            void *addr = nullptr;
            size_t size;

            if (header_type == HEADER_TYPE_CTRL_RTS) {
                // the payload for an RTS message specifies the pool_base address, size,
                // and rkey(s) for the user's payload
                addr = (void *) &work_req->rma_iov->core;
                size = sizeof(work_req->rma_iov->core);
            } else {
                // the payload for an EAGER message is the eager data
                hsta_dbg_assert(header_type == HEADER_TYPE_DATA_EAGER);
                addr = work_req->rma_iov->get_payload_base();
                size = work_req->rma_iov->core.payload_size;
            }

            cqe = this->queue_to_send(
                addr,
                size,
                work_req->nid,
                work_req->c_uid,
                work_req->port,
                header_type,
                work_req->type,
                work_req->protocol_type,
                work_req
            );

            hsta_log_history(cqe, "new pending SENDMSG work request", false, true);

            auto send_return_mode = cqe->header->get_send_return_mode();

            if (   send_return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_RECEIVED
                || send_return_mode == DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED)
            {
                auto& cqes_awaiting_resp = hsta_cqes_awaiting_resp[work_req->port];

                cqes_awaiting_resp.push_back(cqe);
                hsta_cqe_inc_refcount(cqe, &cqes_awaiting_resp, "cqes_awaiting_resp");
                cqe->needs_ack_for_completion = true;
            }

            break;
        }
        case WORK_REQ_TYPE_GETMSG:
        {
            cqe = this->queue_to_send(
                (void *)work_req->rma_iov->get_payload_base(),
                work_req->rma_iov->core.payload_size,
                work_req->nid,
                work_req->c_uid,
                work_req->port,
                HEADER_TYPE_CTRL_GETMSG,
                work_req->type,
                work_req->protocol_type,
                work_req
            );

            hsta_log_history(cqe, "new pending GETMSG work request", false, true);

            auto& cqes_awaiting_resp = hsta_cqes_awaiting_resp[work_req->port];

            cqes_awaiting_resp.push_back(cqe);
            hsta_cqe_inc_refcount(cqe, &cqes_awaiting_resp, "cqes_awaiting_resp");
            cqe->needs_ack_for_completion = true;

            break;
        }
        case WORK_REQ_TYPE_POLL:
        {
            cqe = this->queue_to_send(
                (void *)work_req->rma_iov->get_payload_base(),
                work_req->rma_iov->core.payload_size,
                work_req->nid,
                work_req->c_uid,
                work_req->port,
                HEADER_TYPE_CTRL_POLL,
                work_req->type,
                work_req->protocol_type,
                work_req
            );

            hsta_log_history(cqe, "new pending POLL work request", false, true);

            cqe->event_mask = work_req->gw_msg->event_mask;
            auto& cqes_awaiting_resp = hsta_cqes_awaiting_resp[work_req->port];

            cqes_awaiting_resp.push_back(cqe);
            hsta_cqe_inc_refcount(cqe, &cqes_awaiting_resp, "cqes_awaiting_resp");
            cqe->needs_ack_for_completion = true;

            break;
        }
        case WORK_REQ_TYPE_ERR:
        {
            cqe = this->queue_to_send(
                (void *)work_req->rma_iov->get_payload_base(),
                work_req->rma_iov->core.payload_size,
                work_req->nid,
                work_req->c_uid,
                work_req->port,
                HEADER_TYPE_CTRL_ERR,
                work_req->type,
                work_req->protocol_type,
                work_req
            );

            hsta_log_history(cqe, "new pending ERR work request", false, true);
            break;
        }
        default:
        {
            hsta_default_case(work_req->type);
        }
    }

    hsta_dbg_assert(cqe != nullptr);

    auto& tx_queue = this->tx_queues[cqe->nid];
    tx_queue.try_tx(cqe);
}

/**
 * @brief Process aggregation queues with headers queued up (corresponding
 *        to Send requests), and aggregate those headers into a single header
 *        for all Send requests
 */

void Agent::process_active_tx_queues()
{
    auto txq_count = 0ul;
    auto num_txqs  = this->active_tx_queues.size();

    while (auto tx_queue = this->active_tx_queues.pull_front_unique()) {
        // process eager bufs

        while (auto eager_buf = tx_queue->active_eager_bufs.peek_front()) {
            if (!this->network.target_ep_addr_is_ready(eager_buf->target_nid)) {
                break;
            }

            if (eager_buf->ready_to_send()) {
                auto *cqe_ag = this->create_cqe_aggregated(eager_buf);
                this->network.send_eager(cqe_ag);
                tx_queue->active_eager_bufs.pop_front();
            } else {
                break;
            }
        }

        // process overflow cqes

        while (tx_queue->try_tx(nullptr)) {}

        // check if tx_queue is still active

        auto still_active = (tx_queue->active_eager_bufs.size() > 0ul) || (tx_queue->overflow_cqes.size() > 0ul);

        if (still_active) {
            this->active_tx_queues.push_back_unique(tx_queue);
        }

        if (++txq_count == num_txqs) {
            break;
        }
    }
}

/**
 * @brief Main progress function for the HSTA transport agent
 */

void Agent::progress(bool check_gateways)
{
    if (check_gateways) {
        this->check_gateway_chans();
    }
    this->process_active_tx_queues();
    this->network.progress();
    this->process_active_chs();
    this->process_pending_ch_op_bufs();
    this->process_pending_work_req_completions();

    ++this->progress_loop_count;
}

void Agent::quiesce()
{
    if (dragon_hsta_debug) {
        hsta_utils.log("BEGIN NETWORK QUIESCE\n\n");
    }

    auto start_time = this->utils.get_time();

    while ((this->num_pending_ch_ops > 0) || (this->num_pending_work_reqs > 0)) {
        this->progress(false);

        if (this->utils.get_time() - start_time > HSTA_QUIESCE_TIMEOUT) {
            if (dragon_hsta_debug) {
                hsta_utils.log("TIMEOUT REACHED DURING NETWORK QUIESCE\n\n");
            }
            break;
        }
    }
}
