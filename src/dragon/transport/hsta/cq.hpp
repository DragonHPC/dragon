#ifndef CQ_HPP
#define CQ_HPP

#include "extras.hpp"
#include "header.hpp"
#include "request.hpp"
#include "utils.hpp"

#define hsta_cqe_inc_refcount(cqe, queue, queue_name)                                \
do {                                                                                 \
    if (dragon_hsta_debug) {                                                         \
        if (cqe->refcount_queues[queue] == nullptr) {                                \
            char *qname = (char *)malloc(512);                                       \
            hsta_dbg_assert(qname != nullptr);                                       \
            sprintf(qname, "queue=%s in file=%s at line %d", queue_name, __FILE__, __LINE__); \
            cqe->refcount_queues[queue] = qname;                                     \
        }                                                                            \
        auto event = std::string("inc refcount, ") + std::string(cqe->refcount_queues[queue]); \
        hsta_log_history(cqe, event.c_str(), false, false);                          \
        cqe->verify_is_active();                                                     \
    }                                                                                \
    cqe->inc_refcount();                                                             \
} while (0)

#define hsta_cqe_dec_refcount(cqe, queue)          \
do {                                               \
    if (dragon_hsta_debug && cqe->refcount_queues[queue] != nullptr) {  \
        auto qname = cqe->refcount_queues[queue];  \
        hsta_dbg_assert(qname != nullptr);         \
        auto event = std::string("dec refcount, ") + std::string(qname); \
        hsta_log_history(cqe, event.c_str(), false, false); \
        if (cqe->get_refcount() - 1 == 0) {        \
            free(qname);                           \
            cqe->refcount_queues.erase(queue);     \
        }                                          \
        cqe->verify_is_active();                   \
    }                                              \
    cqe->dec_refcount();                           \
} while (0)

extern uint64_t hsta_cqe_uid;

class EagerBuf;

class CqEvent
{
private:

    uint64_t refcount;
    bool is_deallocated;
    int queue_count;

public:

    // data fields

    Header *header;
    int nid;
    void *src_addr;
    void *dst_addr;
    size_t size;
    bool is_aggregated;
    bool needs_ack_for_completion;
    bool payload_has_arrived;
    std::atomic<uint64_t> tx_is_complete;
    void *fabric_request;
    RmaIov *rma_iov;
    EagerBuf *eager_buf;
    int eager_buf_ref;
    WorkRequest *work_req;
    // TODO: this agent field should be superfluous with the
    // addition of order/work agent indexes
    Agent *agent;
    int order_agent_idx;
    int work_agent_idx;
    TargetChan *target_ch;
    short event_mask;
    TrafficType traffic_type;
    dragonMemoryDescr_t *dst_mem_descr;
    dragonMemoryDescr_t dragon_msg_buf;
    dragonMessage_t dragon_msg;
    dragonError_t dragon_err;
    bool recv_buf_is_allocated;
    bool recv_is_queued;
    bool ch_msg_is_initialized;
    uint64_t begin_timeout;
#ifndef HSTA_NDEBUG
    uint64_t uid;          // unique id for this cqe
    std::string history;   // history of cqe along flow, currently only local
    int checkpoint_count;  // counter tracking checkpoint number along a flow
    bool verify_payload;   // indicates that we need to verify the received payload
    double init_time;      // timestamp for cqe intialization
    std::unordered_map<void *, char *> refcount_queues;  // used to track active cqe references
#endif

    // functions

    void init(Header *header,
              int nid,
              void *src_addr,
              void *dst_addr,
              size_t size,
              bool is_aggregated,
              EagerBuf *eager_buf,
              WorkRequest *work_req,
              Agent *agent,
              TrafficType traffic_type);
    void fini();
    bool dragon_msg_buf_alloc(dragonMemorySerial_t *dest_mem_descr_ser, dragonMemoryPoolDescr_t *mem_pool);
    bool dragon_msg_buf_alloc();
    uint64_t get_refcount();
    void inc_refcount();
    void dec_refcount();
    void log_active_references();
    void send_ack(void *addr, size_t size);
    bool finish_try_return();
    bool try_return();
    void log_history(const char *event, bool force_print, const char *file, int line);
    uint64_t generate_checksum(bool for_verification);
    void verify_checksum();
    void set_seqnum_and_checksum(WorkRequest *work_req);
    void check_ordering_violation(std::string event);
    void verify_is_active();

    // NOTE: this is slightly conservative, since we don't always
    // pack the whole header into the eager buf
    size_t
    space_needed()
    {
        return sizeof(Header) + this->header->eager_size;
    }

    bool
    check_timeout()
    {
        if (this->begin_timeout == HSTA_INVALID_TIME)
        {
            this->begin_timeout = Utils::get_nanoseconds();
        }
        return (Utils::get_nanoseconds() - this->begin_timeout) >= this->header->get_timeout();
    }

    void
    immediate_completion()
    {
        this->tx_is_complete.store(1UL, std::memory_order_relaxed);
    }

#ifndef HSTA_NDEBUG
    void reset_queue_count()
    {
        this->queue_count = 0;
    }

    void inc_queue_count()
    {
        ++this->queue_count;
    }

    void dec_queue_count()
    {
        --this->queue_count;
    }

    void verify_queue_count()
    {
        if (this->queue_count > 0)
        {
            hsta_log_history(this, "non-zero queue count in dtor", false, false);
            hsta_utils.graceful_exit(SIGINT);
        }
    }
#endif // !HSTA_NDEBUG
};

DECL_OBJQ_RING_10(CqEvent,
                  Header *,
                  int,
                  void *,
                  void *,
                  size_t,
                  bool,
                  EagerBuf *,
                  WorkRequest *,
                  Agent *,
                  TrafficType)

#endif // CQ_HPP
