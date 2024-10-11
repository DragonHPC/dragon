#ifndef AGENT_HPP
#define AGENT_HPP

#include "data.hpp"
#include "extras.hpp"
#include "cq.hpp"
#include "eager.hpp"
#include "channel.hpp"
#include "gateway.hpp"
#include "globals.hpp"
#include "header.hpp"
#include "network.hpp"
#include "obj_ring.hpp"
#include "request.hpp"
#include "tx_queue.hpp"
#include "utils.hpp"
#include <unordered_set>

// forward declarations

class Agent;
class MemDescr;

// class definitions

class LAChannelsInfo
{
public:

    int nid;
    int node_idx;
    dragonULInt host_id;
};

class Agent
{
public:

    // data

    int idx;                                    // thread idx for this agent
    Network network;                            // network transport
    int num_tx_queues;                          // number of transmit queues
    int num_rx_queues;                          // number of recv queues
    uint64_t num_pending_work_reqs;             // number of work requests still pending
    uint64_t total_work_reqs;                   // total number of work requests
    uint64_t num_pending_ch_ops;                // number of channel operations still pending
    uint64_t total_ch_ops;                      // total number of channel operations
    uint64_t pending_getmsg_bytes;              // number of bytes currently being received over a local channel
    std::vector<TxQ> tx_queues;                 // transmit queues
    std::vector<RxQ> rx_queues;                 // receive queues
    // TODO: chan_from_cuid doesn't need to be an array any more
    std::unordered_map<dragonULInt, TargetChan *>
        chan_from_cuid;                         // get a channel object for target ops
    ObjectRing<int> target_nids;                // tracks nids currently being targeted by a send operation
    ObjectRing<TargetChan *> active_chs;        // ring of target channels with pending operations
    ObjectRing<TxQ *> active_tx_queues;         // ring of transmit queues with pending operations
    ObjectRing<GatewayChan *> gw_chans;         // ring of gateway channels to monitor for work requests
    ObjectRing<WorkRequest *> work;             // ring of new work requests
    ObjectRing<WorkRequest *>
        pending_work_req_completions;           // ring of pending work request completions
    std::unordered_map<uint64_t, uint32_t>
        pid_and_obj_idx_to_sendh_idx;           // maps (pid, obj idx) to a 32-bit send handle index
    std::unordered_map<uint64_t, uint32_t>
        cuid_to_cuid_idx;                       // maps a channel c_uid to a corresponding 32-bit index
    uint32_t sendh_ctr;                         // counter for new send handle indexes
    uint32_t cuid_ctr;                          // counter for new c_uid indexes
    Utils utils;                                // access to utility functions
    // object queues
    Header_ObjQ header_objq;                    // object queue for headers
    CqEvent_ObjQ cqe_objq;                      // object queue for completion queue events
    TargetChan_ObjQ chan_objq;                  // object queue for channels
    GatewayChan_ObjQ gw_chan_objq;              // object queue for gateway channels
    WorkRequest_ObjQ wreq_objq;                 // object queue for work requests
    EagerBuf_ObjQ eager_buf_objq;               // object queue for eager buffers
    // debugging stuff
#ifndef HSTA_NDEBUG
    uint64_t progress_loop_count;               // number of times through the progress loop
    Stats tx_stats;                             // transmit statistics
    Stats getmsg_stats;                         // getmsg statistics
    Stats poll_stats;                           // poll/event statistics
    Stats err_stats;                            // err/ack statistics
    std::set<CqEvent *> active_cqes;            // set of cqes currently in use (used to dump cqe histories upon failure)
#endif // !HSTA_NDEBUG

    // function

    Agent(int idx)
    {
        this->idx = idx;
    }

    ~Agent()
    {
        // clean up tx queues
        for (auto i = 0; i < this->num_tx_queues; ++i) {
            auto *tx_queue = &this->tx_queues[i];
            tx_queue->cleanup();
        }

        // clean up rx queues
        for (auto i = 0; i < this->num_rx_queues; ++i) {
            auto *rx_queue = &this->rx_queues[i];
            rx_queue->cleanup();
        }
    }

    void init();
    size_t init_rx_queue_size();
    CqEvent *queue_to_send(const void *addr,
                           size_t size,
                           int nid,
                           dragonULInt c_uid,
                           port_t port,
                           HeaderType header_type,
                           ReqType work_req_type,
                           ProtocolType protocol_type,
                           WorkRequest *work_req);
    CqEvent *create_cqe_aggregated(EagerBuf *eager_buf);
    void handle_work_req(WorkRequest *work_req);
    void get_gateway_chans();
    void check_gateway_chans();
    void process_active_tx_queues();
    void process_cq_events();
    void progress(bool check_gateways);
    void quiesce();

    uint32_t
    get_sendh_idx(pid_t pid, uint32_t obj_idx)
    {
        auto pid_and_obj_idx = (((uint64_t) pid) << 32) | (uint64_t) obj_idx;
        auto sendh_idx = this->pid_and_obj_idx_to_sendh_idx[pid_and_obj_idx];

        if (sendh_idx == 0U) {
            sendh_idx = ++this->sendh_ctr;
            // check for overflow in sendh_ctr
            hsta_dbg_assert(sendh_idx > 0U);
            this->pid_and_obj_idx_to_sendh_idx[pid_and_obj_idx] = sendh_idx;
        }

        return sendh_idx;
    }

    uint32_t
    get_cuid_idx(uint64_t c_uid)
    {
        auto cuid_idx = this->cuid_to_cuid_idx[c_uid];

        if (cuid_idx == 0U) {
            cuid_idx = ++this->cuid_ctr;
            // check for overflow in cuid_ctr
            hsta_dbg_assert(cuid_idx > 0U);
            this->cuid_to_cuid_idx[c_uid] = cuid_idx;
        }

        return cuid_idx;
    }

    void
    get_cuid_and_port(dragonGatewayMessage_t *gw_msg, int nid, ProtocolType protocol_type, dragonULInt& c_uid, port_t& port)
    {
        uint64_t src_rank;
        uint64_t dst_rank;
        uint64_t order_obj_idx;

        if (gw_msg != nullptr) {
            auto dragon_err =
                dragon_channel_get_uid_type(&gw_msg->target_ch_ser, &c_uid, (dragonULInt *) nullptr);
            hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS, "getting channel uid");

            /*
             * set port using:
             * (1) the source and destination ranks (15 bits each)
             * (2) the 32 bit index of an "order object", i.e., the send handle
             *     or c_uid used to order operations
             * (3) the protocol type: sendmsg, getmsg or poll
             *
             *  src rank     dst rank      obj idx     protocol
             *    ____         ____         ____         ____
             *   /    \       /    \       /    \       /    \
             *  b63   b49    b48   b34    b33    b2    b1    b0
             */
            // TODO: we should use src_nid/dst_nid rather than src_rank/dst_rank,
            // since 15 bits won't be enough for ranks at scale

            src_rank = (uint64_t) this->network.rank;
            dst_rank = (uint64_t) this->network.get_rank_from_nid(nid);

            switch (protocol_type) {
                case PROTOCOL_TYPE_SENDMSG: {
                    dragonMessageAttr_t mattr;

                    auto dragon_err =
                        dragon_channel_message_getattr(&gw_msg->send_payload_message,
                                                       &mattr);
                    hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS,
                                             "getting send payload message attributes");

                    auto pid = dragon_get_pid_from_uuid(mattr.sendhid);
                    auto obj_idx = dragon_get_ctr_from_uuid(mattr.sendhid);
                    order_obj_idx = (uint64_t) this->get_sendh_idx(pid, obj_idx);

                    break;
                }
                case PROTOCOL_TYPE_GETMSG:
                case PROTOCOL_TYPE_POLL: {
                    order_obj_idx = (uint64_t) this->get_cuid_idx(c_uid);
                    break;
                }
                default: {
                    // setting this to silence warning
                    order_obj_idx = 0UL;
                    hsta_default_case(protocol_type);
                }
            }
        } else {
            src_rank = (uint64_t) HSTA_INVALID_RANK;
            dst_rank = (uint64_t) this->network.get_rank_from_nid(nid);
            order_obj_idx = 0UL;
        }

        port = (src_rank << 49) | (dst_rank << 34) | (order_obj_idx << 2) | (uint64_t) protocol_type;
    }

    int
    get_src_rank_from_port(port_t port)
    {
        auto lowest_fifteen = 0x0000000000007fffUL;
        return (int) ((port >> 49) & lowest_fifteen);
    }

    int
    get_dst_rank_from_port(port_t port)
    {
        auto lowest_fifteen = 0x0000000000007fffUL;
        return (int)((port >> 34) & lowest_fifteen);
    }

    HeaderType
    get_send_header_type(size_t size)
    {
        return (size >= HSTA_RNDV_THRESHOLD) ? HEADER_TYPE_CTRL_RTS : HEADER_TYPE_DATA_EAGER;
    }

    void
    cleanup_cqe(CqEvent *cqe)
    {
        if (dragon_hsta_debug) {
            // dump the history if there was an error with this cqe
            auto force_print = (cqe->dragon_err != DRAGON_SUCCESS);
            hsta_log_history(cqe, "dtor", force_print, false);
        }

        this->header_objq.push_back(cqe->header);

        if (cqe->rma_iov != nullptr) {
            this->network.rma_iov_objq.push_back(cqe->rma_iov);
        }

        this->cqe_objq.push_back(cqe);
    }

    void
    process_active_chs()
    {
        auto i = 0ul;
        auto num_active_chs = this->active_chs.size();

        // TODO: create a "remove_if" type function to replace this sort of loop
        // (unordered and requiring removal of elements in the middle)
        while (auto *target_ch = this->active_chs.pull_front_unique())
        {
            ++i;

            if (0ul == target_ch->process_pending_ch_ops(PROTOCOL_TYPE_SENDMSG)) continue;
            if (0ul == target_ch->process_pending_ch_ops(PROTOCOL_TYPE_GETMSG))  continue;
            if (0ul == target_ch->process_pending_ch_ops(PROTOCOL_TYPE_POLL))    continue;

            this->active_chs.push_back_unique(target_ch);

            if (i >= num_active_chs) break;
        }
    }

    void
    process_pending_ch_op_bufs()
    {
        static auto max_failed_attempts = 4ul;

        for (auto& rx_queue : this->rx_queues)
        {
            while (auto *eager_buf = rx_queue.pending_ch_op_bufs.peek_front())
            {
                if (eager_buf->refcount_is_zero())
                {
                    // all channels ops are complete for this buf, so post the next
                    // recv and reset failed_attempts (to complete channel ops)
                    rx_queue.pending_ch_op_bufs.pop_front();
                    rx_queue.post_next_recv(eager_buf);
                    rx_queue.failed_attempts = 0ul;
                }
                else
                {
                    // all eager bufs are in use for channel ops, so allocate a
                    // new one and post a recv with it

                    if ((this->network.num_pending_eager_rx == 0ul) && (++rx_queue.failed_attempts == max_failed_attempts)) {
                        rx_queue.new_eager_buf();
                        rx_queue.failed_attempts = 0ul;
                    }
                    break;
                }
            }
        }
    }

    void
    process_pending_work_req_completions()
    {
        auto num_processed = 0UL;
        auto num_work_reqs = this->pending_work_req_completions.size();

        while (auto work_req = this->pending_work_req_completions.pull_front()) {
            auto complete = work_req->check_gw_complete();
            if (!complete) {
                this->pending_work_req_completions.push_back(work_req);
            }

            if (++num_processed == num_work_reqs) {
                break;
            }
        }
    }

    void
    update_tx_credits(EagerBuf *eager_buf)
    {
        auto *tx_queue = &this->tx_queues[eager_buf->target_nid];
        ++tx_queue->read_counter;

        // indicate that this tx queue is no longer blocked, and
        // (re)add nid to list of target nid's with queued work

        if (tx_queue->progress_blocked_no_credits)
        {
#ifndef HSTA_NDEBUG
            hsta_dbg_printf(tx_queue, std::string("tx credits replenished"));
#endif
            tx_queue->progress_blocked_no_credits = false;
            this->target_nids.push_back_unique_idx(tx_queue->nid);
        }
    }

    bool
    nothing_to_do()
    {
        return (this->num_pending_work_reqs == 0UL) && (this->num_pending_ch_ops == 0UL);
    }

    bool
    made_progress()
    {
        static auto first_time = true;
        static auto prev_completed_work_reqs = 0UL;
        static auto prev_completed_ch_ops = 0UL;

        if (first_time) {
            first_time = false;
            prev_completed_work_reqs = this->total_work_reqs - this->num_pending_work_reqs;
            prev_completed_ch_ops = this->total_ch_ops - this->num_pending_ch_ops;
            return true;
        }

        auto cur_completed_work_reqs = this->total_work_reqs - this->num_pending_work_reqs;
        auto cur_completed_ch_ops = this->total_ch_ops - this->num_pending_ch_ops;

        auto progress_completed_work_reqs = cur_completed_work_reqs > prev_completed_work_reqs;
        auto progress_completed_ch_ops = cur_completed_ch_ops > prev_completed_ch_ops;

        prev_completed_work_reqs = cur_completed_work_reqs;
        prev_completed_ch_ops = cur_completed_ch_ops;

        return progress_completed_work_reqs || progress_completed_ch_ops;
    }
};

extern thread_local Agent *hsta_my_agent;
extern std::vector<Agent *> hsta_agent;

#endif // AGENT_HPP
