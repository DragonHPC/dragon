#ifndef NETWORK_HPP
#define NETWORK_HPP

#include "cq.hpp"
#include "dragon/global_types.h"
#include "dragon_fabric.hpp"
#include "extras.hpp"
#include "agent.hpp"
#include "globals.hpp"
#include "magic_numbers.hpp"
#include "request.hpp"
#include "utils.hpp"

// forward declarations

class Agent;
class CqEvent;
class Network;

// function declarations

void *ep_addr_setup_progress(void *network);

// class definitions

enum EpAddrSetupState {
    EP_ADDR_SETUP_NOT_STARTED = 0,
    EP_ADDR_SETUP_IN_PROGRESS,
    EP_ADDR_SETUP_READY,
    EP_ADDR_SETUP_INVALID
};

class NodeInfo {
public:

    // always set during get_startup_info
    dragonULInt host_id;
    int num_nics;
    std::string ip_addr;
    int ip_addr_len;
    bool have_sent_ep_addrs;
    EpAddrSetupState ep_addr_setup_state;
    bool ep_addrs_available;
    // only set when available (ofi: yes, ucx: no, mpi: no)
    std::vector<uint8_t> ep_addrs;
    std::vector<int> ep_addr_lens;
};

class StartupInfo {
public:

    // always set during get_startup_info
    int num_nodes;
    int node_idx;
    std::vector<NodeInfo> nodes;
    // only set when available (ofi: yes, ucx: no, mpi: no)
    std::vector<uint8_t> addr_table;
};

extern StartupInfo hsta_startup_info;

struct PackedRkeyVec {
    dragonULInt puid;
    int num_rkeys;
    size_t packed_rkey_sizes[HSTA_MAX_NUM_NICS];
    uint8_t packed_rkeys[HSTA_MAX_NUM_NICS * HSTA_MAX_PACKED_RKEY_SIZE];
};

struct RmaIovCore {
    dragonULInt puid;
    size_t payload_offset;
    size_t payload_size;
};

class RmaIov {
public:

    Agent *agent;
    std::unordered_set<int> rkey_setup_table;
    PackedRkeyVec *packed_rkey_vec;
    Rkey *rkeys; // only set at the target
    bool free_packed_rkeys;
    bool free_rkeys;
    void *pool_base;
    RmaIovCore core;
    bool requires_rkey_setup;

    void
    init(Agent *agent, void *pool_base, dragonULInt puid, size_t payload_offset, size_t payload_size, bool requires_rkey_setup);

    void
    fini();

    void
    setup_rkeys_at(int target_nid);

    void *
    get_payload_base()
    {
        return (void *) ((uint8_t *)this->pool_base + this->core.payload_offset);
    }
};

DECL_OBJQ_RING_6(
    RmaIov,
    Agent *,
    void *,
    dragonULInt,
    size_t,
    size_t,
    bool
)

class FlowQ
{
public:

    static uint64_t uid_counter;
    uint64_t uid;
    ObjectRing<CqEvent *> q;

    void init()
    {
        this->uid = this->uid_counter++;
    }

    void fini() {}
};

DECL_OBJQ_RING_0(FlowQ)

class Network
{
private:

    // private data fields

    std::unordered_map<port_t, FlowQ *>
        flowq_map;                            // maps a port to its corresponding flowq
    ObjectRing<FlowQ *> active_flowqs;        // flowqs with pending cqes
    ObjectRing<CqEvent *> work;               // rndv recv work items to be immediately handled
    ObjectRing<CqEvent *> pending_responses;  // pending getmsg responses to be returned to user
    Agent *agent;                             // agent associated with this fabric object

public:

    // public data fields

    DragonFabric *dfabric;                    // Dragon fabric to handle network operations
    int lrank;                                // agent's local rank on a node
    int ppn;                                  // number of MPI processes per node
    int rank;                                 // the global index of this agent
    int this_nid;                             // node id for this agent
    int num_nodes;                            // number of nodes in this job
    int num_ranks;                            // number of agents in this job
    uint64_t num_pending_eager_rx;            // number of pending eager receive operations
    uint64_t total_rndv_bytes;                // total number of bytes received via the rndv gets or recvs
    std::atomic<uint64_t>
        pending_ejection_bytes;               // number of bytes currently being received over the network
    dragonMemoryPoolDescr_t dragon_mem_pool;  // dragon memory pool
    ObjectRing<CqEvent *> rndv_recv_workq;    // cqes waiting for resources to post a recv
    RmaIov_ObjQ rma_iov_objq;                 // objq for managing RmaIov objects
    FlowQ_ObjQ flow_objq;                     // objq for managing FlowQ objects
    Lock state_machine_lock =
        Lock("ep_addr_setup_state_machine");  // protects the ep addr setup state machine
    Lock rndv_recv_workq_lock =
        Lock("rndv_recv_workq_lock");         // protects the rendezvous receive queue

    // class functions

    static void
    get_startup_info();

    // member functions

    void init(Agent *agent);
    void cancel_pending_cqes();
    void finalize();

    void send_ep_addrs(std::string ip_addr);
    void ep_addr_setup_progress();
    bool target_ep_addr_is_ready(int target_nid);

    void send_eager(CqEvent *cqe);
    void recv_eager(CqEvent *cqe);
    void send_rndv(CqEvent *cqe);
    void recv_rndv(CqEvent *cqe);
    void mem_register(void *base, size_t size, RmaIov *rma_iov);
    void mem_unregister(void *base);
    RmaIov *get_rma_iov_rndv(void *pool_base, size_t pool_size, dragonULInt puid, size_t payload_offset, size_t payload_size, int target_nid, PackedRkeyVec *packed_rkey_vec);
    RmaIov *get_rma_iov_tx(const dragonMemoryDescr_t *mem_descr, int target_nid);

    void process_cq_events();
    ObjectRing<CqEvent *> *get_work_from_agent(Agent *agent);
    bool try_recv_rndv(CqEvent *cqe);
    void process_rndv_recv_workq();
    void process_pending_responses();
    void process_tx_eager_buf(CqEvent *cqe);
    void process_rx_eager_buf(CqEvent *cqe);
    void process_eager_buf(CqEvent *cqe);

    void handle_rx_ctrl_conn_event(CqEvent *cqe);
    void handle_rx_ctrl_rkey(CqEvent *cqe);
    void handle_rx_ctrl_rts_event(CqEvent *cqe);
    void handle_rx_ctrl_getmsg_event(CqEvent *cqe);
    void handle_rx_ctrl_poll_event(CqEvent *cqe);
    void handle_rx_ctrl_err_event(CqEvent *cqe);
    void handle_rx_data_eager_event(CqEvent *cqe);
    void handle_rx_data_rndv_event(CqEvent *cqe);
    void handle_disag_cq_event(CqEvent *cqe);
    void handle_disag_tx_cq_event(CqEvent *cqe);
    void handle_disag_rx_cq_event(CqEvent *cqe);
    void handle_cq_event(CqEvent *cqe);

    void dragon_msg_buf_set_from_eager(CqEvent *cqe);
    void post_next_recv(EagerBuf *eager_buf);

    void
    progress()
    {
        this->process_cq_events();
        this->process_pending_responses();
        if (this->ejection_credits_available(nullptr)) {
            this->process_rndv_recv_workq();
        }
    }

    bool
    ejection_credits_available(CqEvent *cqe)
    {
        auto size = cqe ? cqe->size : HSTA_RNDV_THRESHOLD;

        // TODO: the first part of this condition (ec0) can be improved by
        // strip-mining larger messages (needed for many-to-many anyways)

        // verify that worker agent has enough ejection credits
        auto eca0 = (size > dragon_hsta_max_ejection_bytes);
        auto eca1 = (size + this->pending_ejection_bytes <= dragon_hsta_max_ejection_bytes);

        return eca0 || eca1;
    }

    dfBackendType
    get_backend()
    {
        return this->dfabric->get_backend();
    }

    DragonFabric *get_dfabric()
    {
        return this->dfabric;
    }

    int
    get_rank_from_nid(int nid)
    {
        return nid * this->ppn + this->lrank;
    }

    int
    get_base_rank_from_nid(int nid)
    {
        return nid * this->ppn;
    }

    int
    get_nid_from_rank(int rank)
    {
        // TODO: try to avoid using division here
        return rank / this->ppn;
    }

    ObjectRing<CqEvent *>&
    get_rndv_recv_workq()
    {
        return this->rndv_recv_workq;
    }

    void *
    get_ep_addr()
    {
        return this->dfabric->ep_addr;
    }

    size_t
    get_ep_addr_len()
    {
        return this->dfabric->ep_addr_len;
    }

    bool
    get_ep_addrs_available()
    {
        switch (this->dfabric->backend) {
            case DFABRIC_BACKEND_OFI: return true;
            case DFABRIC_BACKEND_UCX:
            case DFABRIC_BACKEND_MPI: return false;
            default:                  hsta_default_case(this->dfabric->backend);
        }

        return false;
    }

    RmaIov *
    clone_rma_iov(RmaIov *rma_iov, size_t payload_offset, size_t payload_size, int target_nid)
    {
        if (payload_offset == HSTA_INVALID_UINT64) {
            payload_offset = rma_iov->core.payload_offset;
            payload_size = rma_iov->core.payload_size;
        }

        auto cloned_riov = this->rma_iov_objq.pull_front(
            rma_iov->agent,
            rma_iov->pool_base,
            rma_iov->core.puid,
            payload_offset,
            payload_size,
            rma_iov->requires_rkey_setup
        );

        cloned_riov->packed_rkey_vec = rma_iov->packed_rkey_vec;
        cloned_riov->rkeys = rma_iov->rkeys;

        if (rma_iov->requires_rkey_setup) {
            // the rma_iov here always needs to be the primary one here, since that's
            // the one with the populated rkey_setup_table for this puid
            rma_iov->setup_rkeys_at(target_nid);
        }

        return cloned_riov;
    }

    RmaIov *
    clone_rma_iov(RmaIov *rma_iov, int target_nid)
    {
        return clone_rma_iov(
            rma_iov,
            HSTA_INVALID_UINT64,
            HSTA_INVALID_UINT64,
            target_nid
        );
    }

    void
    log_nic_info()
    {
        this->dfabric->log_nic_info();
    }

    // static member functions

    static void
    pack_conn_data(dragonChannelSerial_t& ch_sdesc, dragonUUID sendhid, std::vector<uint8_t>& conn_data)
    {
        auto off = 0ul;

        auto size = sizeof(ch_sdesc.len);
        conn_data.resize(off + size);
        memcpy(&conn_data[off], &ch_sdesc.len, size);
        off += size;

        size = ch_sdesc.len;
        conn_data.resize(off + size);
        memcpy(&conn_data[off], ch_sdesc.data, size);
        off += size;

        size = DRAGON_UUID_SIZE;
        conn_data.resize(off + size);
        memcpy(&conn_data[off], sendhid, size);
        // off += size;
    }

    static void
    unpack_conn_data(uint8_t *conn_data, size_t conn_data_size, dragonChannelSerial_t& ch_sdesc, dragonUUID sendhid)
    {
        if (dragon_hsta_debug) {
            // TODO: add c_uid here
            fprintf(
                hsta_dbg_file, "unpacking %lu bytes of connection data\n",
                conn_data_size
            );
        }

        auto off = 0ul;

        memcpy(&ch_sdesc.len, &conn_data[off], sizeof(ch_sdesc.len));
        off = sizeof(ch_sdesc.len);

        ch_sdesc.data = (uint8_t *)malloc(ch_sdesc.len);
        hsta_dbg_assert(ch_sdesc.data != nullptr);

        memcpy(ch_sdesc.data, &conn_data[off], ch_sdesc.len);
        off += ch_sdesc.len;

        memcpy(sendhid, &conn_data[off], DRAGON_UUID_SIZE);
        off += DRAGON_UUID_SIZE;

        hsta_dbg_assert(off == conn_data_size);
    }

    void
    unpack_rkey_vec(int src_nid, PackedRkeyVec *packed_rkey_vec, RmaIov *rma_iov)
    {
        if (dragon_hsta_debug) {
            fprintf(
                hsta_dbg_file, "unpacking %d rkeys from %d for the memory segment with base=%p and size=%lu\n",
                packed_rkey_vec->num_rkeys, src_nid, rma_iov->get_payload_base(), rma_iov->core.payload_size
            );
        }

        auto num_rkeys = packed_rkey_vec->num_rkeys;
        auto offset = 0UL;
        auto src_rank = this->get_base_rank_from_nid(src_nid);

        rma_iov->rkeys = (Rkey *)malloc(num_rkeys * sizeof(Rkey));
        hsta_dbg_assert(rma_iov->rkeys != nullptr);
        // only the primary rma_iov needs to free the rkeys
        rma_iov->free_packed_rkeys = true;

        for (auto i = 0; i < num_rkeys; ++i) {
            rma_iov->rkeys[i] =
                (Rkey)this->dfabric->unpack_rkey(src_rank, &packed_rkey_vec->packed_rkeys[offset]);
            ++src_rank;
            offset += packed_rkey_vec->packed_rkey_sizes[i];
        }
    }

    FlowQ *get_flowq(port_t port)
    {
        auto flowq = this->flowq_map[port];
        if (flowq == nullptr) {
            flowq = this->flow_objq.pull_front();
            this->flowq_map[port] = flowq;
        }
        return flowq;
    }

    void return_response(CqEvent *cqe_resp)
    {
        auto& cqes_awaiting_resp = hsta_cqes_awaiting_resp[cqe_resp->header->port];
        auto cqe_req = cqes_awaiting_resp.pull_front();
        hsta_dbg_assert(cqe_req != nullptr);
        // we assume that get_msg responses don't have to be strictly ordered
        //hsta_dbg_assert(cqe_req->header->seqnum == cqe->header->seqnum);

        cqe_resp->work_req = cqe_req->work_req;
        cqe_req->work_req = nullptr;
        hsta_cqe_dec_refcount(cqe_req, &cqes_awaiting_resp);

        this->pending_responses.push_back(cqe_resp);
        hsta_cqe_inc_refcount(cqe_resp, &this->pending_responses, "pending_responses");

        // increment refcount to indicate that this eager_buf is busy
        cqe_resp->eager_buf->inc_refcount(cqe_resp->eager_buf_ref);

        hsta_log_history(cqe_resp, "return_resp operation queued", false, true);
    }

    void queue_recv(CqEvent *cqe)
    {
        hsta_lock_acquire(this->rndv_recv_workq_lock);
        this->rndv_recv_workq.push_back(cqe);
        hsta_cqe_inc_refcount(cqe, &this->rndv_recv_workq, "rndv_recv_workq");
        hsta_lock_release(this->rndv_recv_workq_lock);
    }
};

#endif // NETWORK_HPP
