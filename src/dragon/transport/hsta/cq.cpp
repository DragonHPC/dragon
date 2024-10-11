#include <cstdio>
#include <cstdlib>
#include <set>
#include <string>
#include "agent.hpp"
#include "cq.hpp"
#include "globals.hpp"
#include "header.hpp"
#include "magic_numbers.hpp"
#include "obj_ring.hpp"
#include "request.hpp"
#include "tx_queue.hpp"
#include "utils.hpp"

#ifndef HSTA_NDEBUG
// maps to help with ordering violations
thread_local SeqnumMap         hsta_cqe_seqnum_ctr;
thread_local SeqnumAtEventMap  hsta_cqe_seqnum_at_event;
thread_local HistoryAtEventMap hsta_cqe_history_at_event;

// counter for cqe unique ids
uint64_t hsta_cqe_uid = 0ul;
#endif //!HSTA_NDEBUG

void CqEvent::init(Header *header,
                   int nid,
                   void *src_addr,
                   void *dst_addr,
                   size_t size,
                   bool is_aggregated,
                   EagerBuf *eager_buf,
                   WorkRequest *work_req,
                   Agent *agent,
                   TrafficType traffic_type)
{
    this->header          = header;
    this->nid             = nid;
    this->src_addr        = src_addr;
    this->dst_addr        = dst_addr;
    this->size            = size;
    this->is_aggregated   = is_aggregated;
    this->eager_buf       = eager_buf;
    this->traffic_type    = traffic_type;
    this->work_req        = work_req;
    this->rma_iov         = work_req ? work_req->rma_iov : nullptr;
    this->agent           = agent;
    this->order_agent_idx = agent->idx;
    this->work_agent_idx  = agent->idx;

    // independent of init params

    this->refcount                 = 0ul;
    this->target_ch                = nullptr;
    this->dragon_err               = DRAGON_SUCCESS;
    this->begin_timeout            = HSTA_INVALID_TIME;
    this->fabric_request           = nullptr;
    this->needs_ack_for_completion = false;
    this->eager_buf_ref            = 0;
    this->recv_buf_is_allocated    = false;
    this->recv_is_queued           = false;
    this->ch_msg_is_initialized    = false;
    this->tx_is_complete           = false;
    this->payload_has_arrived      = false;

#ifndef HSTA_NDEBUG
    if (dragon_hsta_debug)
    {
        // don't mark eager recv cqes as active yet to avoid cluttering the log
        if (!this->is_aggregated || this->traffic_type != TRAFFIC_TYPE_RX) {
            this->agent->active_cqes.insert(this);
        }

        auto must_set_seqnum_and_checksum =
               this->header->get_type()          != HEADER_TYPE_CTRL_CONN
            && this->header->get_type()          != HEADER_TYPE_CTRL_RKEY
            && this->header->get_protocol_type() != PROTOCOL_TYPE_AGGREGATED;

        if (must_set_seqnum_and_checksum) {
            this->set_seqnum_and_checksum(work_req);
        }

        this->uid              = hsta_cqe_uid++;
        this->init_time        = Utils::get_time();
        this->checkpoint_count = 0;
        this->is_deallocated   = false;
        this->verify_payload   = true;

        this->reset_queue_count();

        hsta_utils.reset_access_count(this);

        hsta_log_history(this, "ctor", false, false);

        // check for duplicate cqes on the objq

        for (auto i = 0ul; i < agent->cqe_objq.q.size(); ++i)
        {
            if (this == agent->cqe_objq.q[i])
            {
                hsta_log_history(this, "duplicate cqe on objq after allocation", false, false);
                hsta_utils.graceful_exit(SIGINT);
            }
        }
    }
#endif // !HSTA_NDEBUG
}

void CqEvent::fini()
{
    if (dragon_hsta_debug)
    {
        this->is_deallocated = true;

        // verify this cqe is no longer on any queues
        this->verify_queue_count();

        // reset history
        this->history.resize(0);
        if (dragon_hsta_debug_prehistory) {
            this->checkpoint_count = -1;
            hsta_log_history(this, "pre-history", false, false);
        } else {
            this->checkpoint_count = 0;
        }

        // remove from active_cqes
        this->agent->active_cqes.erase(this);
    }
}

bool CqEvent::dragon_msg_buf_alloc(dragonMemorySerial_t *dest_mem_descr_ser, dragonMemoryPoolDescr_t *mem_pool)
{
    dragonError_t dragon_err;
    dragonMemoryDescr_t mem_descr;
    dragonMemoryPoolDescr_t pool_descr;
    size_t landing_size = 0;
    const timespec_t timeout = { 0, 0 };

    hsta_dbg_assert(this->size > 0);

    if (dest_mem_descr_ser != nullptr) {
        dragon_err = dragon_memory_attach(&mem_descr, dest_mem_descr_ser);
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this,
                          "There was an error attaching to the get message "
                          "memory descriptor.");

        dragon_err = dragon_memory_get_size(&mem_descr, &landing_size);
        hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this,
                          "There was an error getting the size of the get message "
                          "destination memory.");

        if (landing_size == 0) {
            // This indicates we are to make a memory allocation for the
            // payload in the pool of this zero-byte memory allocation.
            dragon_err = dragon_memory_get_pool(&mem_descr, &pool_descr);
            hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this,
                              "There was an error getting the pool from the get message "
                              "destination memory.");

            this->dragon_err = dragon_memory_alloc_blocking(&this->dragon_msg_buf, &pool_descr, this->size, &timeout);
            if (Utils::check_alloc_failure(this->dragon_err)) {
                hsta_log_history(this, "managed memory allocation failed", false, false);
                return false;
            }
            hsta_dbg_errcheck(this->dragon_err, DRAGON_SUCCESS, this,
                              "The landing pad could not be allocated in the indicated "
                              "destination pool. Possible timeout?");

            this->recv_buf_is_allocated = true;
        } else {
            // The landing area was provided. So we'll make it the landing area.
            dragon_err = dragon_memory_descr_clone(&this->dragon_msg_buf, &mem_descr, 0, nullptr);
            hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this,
                              "Could not clone the landing pad for the get message "
                              "destination memory.");

            this->recv_buf_is_allocated = true;
        }
    }

    if (!this->recv_buf_is_allocated) {
        // there was no destination memory descriptor provided, so allocate space
        // from the target channel's pool (we don't need to free this, ownership
        // is transferred to the application)
        this->dragon_err = dragon_memory_alloc_blocking(&this->dragon_msg_buf, mem_pool, this->size, &timeout);
        if (Utils::check_alloc_failure(this->dragon_err)) {
            hsta_log_history(this, "managed memory allocation failed", false, false);
            return false;
        }
        hsta_dbg_errcheck(this->dragon_err, DRAGON_SUCCESS, this,
                          "allocating from target channel's memory pool");

        this->recv_buf_is_allocated = true;
    }

    // get the virtual address for the memory segment
    dragon_err = dragon_memory_get_pointer(&this->dragon_msg_buf, &this->dst_addr);
    hsta_dbg_errcheck(dragon_err, DRAGON_SUCCESS, this,
                      "getting pointer from memory descriptor");

    return true;
}

bool CqEvent::dragon_msg_buf_alloc()
{
    auto dest_mem_descr_ser = (dragonMemorySerial_t *)nullptr;
    if (this->work_req != nullptr && this->work_req->gw_msg != nullptr) {
        dest_mem_descr_ser = this->work_req->gw_msg->get_dest_mem_descr_ser;
    }

    auto mem_pool =
          (this->header->get_protocol_type() == PROTOCOL_TYPE_SENDMSG)
        ? &this->target_ch->dragon_mem_pool
        : &this->agent->network.dragon_mem_pool;

    return this->dragon_msg_buf_alloc(dest_mem_descr_ser, mem_pool);
}

uint64_t CqEvent::get_refcount()
{
    return this->refcount;
}

void CqEvent::inc_refcount()
{
    ++this->refcount;

#ifndef HSTA_NDEBUG
    if (dragon_hsta_debug) {
        if (this->is_deallocated) {
            hsta_log_history(this, "refcount incremented after deallocation", true, false);
            hsta_utils.graceful_exit(SIGINT);
        }
    }
#endif // !HSTA_NDEBUG
}

void CqEvent::dec_refcount()
{
    --this->refcount;

#ifndef HSTA_NDEBUG
    if (dragon_hsta_debug) {
        if (this->is_deallocated) {
            hsta_log_history(this, "refcount decremented after deallocation", true, false);
            hsta_utils.graceful_exit(SIGINT);
        }
    }
#endif // !HSTA_NDEBUG

    if (this->refcount == 0ul) {
        this->agent->cleanup_cqe(this);
    }
}

void CqEvent::log_active_references()
{
    hsta_lock_acquire(hsta_utils.lock);

    fprintf(hsta_dbg_file, "CQE ACTIVE REFERENCES\n");

    for (auto& qaddr_qname: this->refcount_queues) {
        auto qaddr = qaddr_qname.first;
        auto qname = qaddr_qname.second;
        fprintf(hsta_dbg_file, "> queue: addr = %p, name = %s\n", qaddr, qname);
    }

    fprintf(hsta_dbg_file, "\n");
    fflush(hsta_dbg_file);

    hsta_lock_release(hsta_utils.lock);
}

void CqEvent::send_ack(void *addr, size_t size)
{
    auto *agent = this->target_ch->agent;

    auto rma_iov =
        this->agent->network.rma_iov_objq.pull_front(
            this->agent,
            nullptr, // pool_base
            HSTA_INVALID_UINT64, // puid
            (uint64_t)addr, // payload_offset
            size,  // payload_size
            false // requires_rkey_setup
        );

    // don't send clientid or hints with an ack
    this->header->clientid = HSTA_INVALID_CLIENTID_HINTS;
    this->header->hints    = HSTA_INVALID_CLIENTID_HINTS;

    auto *work_req =
        agent->wreq_objq.pull_front(
            agent,
            this->header->get_protocol_type(),
            WORK_REQ_TYPE_ERR,
            rma_iov,
            this->nid,
            this->header,
            nullptr, // gw_msg
            nullptr, // gw_ch
            false    // is_complete
        );

    // TODO: wrap this init logic up in a function
    if (addr == nullptr) {
        work_req->event_info.err = this->dragon_err;
        work_req->rma_iov->core.payload_offset = (uint64_t)&work_req->event_info;
        work_req->rma_iov->core.payload_size = sizeof(work_req->event_info);
    }
    work_req->port = this->header->port;
    work_req->target_ch = this->target_ch;
    work_req->is_response = true;

    hsta_log_history(this, "sending ack", false, false);

    this->agent->handle_work_req(work_req);
}

bool CqEvent::finish_try_return()
{
    this->eager_buf->dec_refcount(this->eager_buf_ref);
    this->work_req->complete(this);
    return true;
}

bool CqEvent::try_return()
{
    auto save_dst_addr = this->dst_addr;

    if (!this->recv_buf_is_allocated && !this->dragon_msg_buf_alloc()) {
        if (this->check_timeout()) {
            hsta_log_history(
                this, "timed out while trying to allocate buffer for payload",
                false, false
            );
            // since the user data has already been removed from the remote
            // channel, it's now permanently lost--we can't return it to the
            // user after this timeout
            fprintf(hsta_dbg_file, "ERROR: LOST DATA -- unable to return user data after getting it from remote channel\n");
            return this->finish_try_return();
        }
        return false;
    }

    if (this->payload_has_arrived) {
        // if this is the eager protocol, we need to copy the data from
        // the eager buffer into the newly allocated buffer that we're
        // returning to the user
        if (this->header->get_type() == HEADER_TYPE_DATA_EAGER) {
            memcpy(this->dst_addr, save_dst_addr, this->size);
        }
        return this->finish_try_return();
    } else {
        // this else branch is only taken by the rndv protocol, so queue up
        // the recv of the user payload
        if (!this->recv_is_queued) {
            auto& network = hsta_my_agent->network;
            network.queue_recv(this);
            this->recv_is_queued = true;
        }
        return false;
    }
}

#define BEGIN_PRE_HISTORY " /| ==================================== BEGIN PRE-HISTORY ==================================== |\\\n//\n"
#define BEGIN_HISTORY     " /| ====================================== BEGIN HISTORY ====================================== |\\\n//\n"
#define END_HISTORY      " \\| ======================================  END HISTORY  ====================================== |/\n\n"

void CqEvent::log_history(const char *event, bool force_print, const char *file, int line)
{
    char tmp_cstr[2048];
    auto queue_index = -1;
    auto queue_busy = -1ul;
    auto queue_cap = -1ul;

    hsta_lock_acquire(hsta_utils.lock);

    if (this->traffic_type == TRAFFIC_TYPE_TX)
    {
        if (this->eager_buf != nullptr)
        {
            auto *tx_queue = &this->agent->tx_queues[this->eager_buf->queue_index];
            queue_index = this->eager_buf->queue_index;
            queue_busy  = tx_queue->write_counter - tx_queue->read_counter;
            queue_cap   = tx_queue->eager_bufs.size();
        }
    }
    else
    {
        if (this->eager_buf != nullptr)
        {
            auto *rx_queue = &this->agent->rx_queues[this->eager_buf->queue_index];
            queue_index = this->eager_buf->queue_index;
            queue_busy  = rx_queue->pending_ch_op_bufs.size();
            queue_cap   = this->agent->network.num_pending_eager_rx + rx_queue->pending_ch_op_bufs.size();
        }
    }

    auto num_pending_ch_ops_protocol = 0ul;
    auto num_pending_ch_ops_total    = 0ul;

    char cuid_str[32];

    if (this->target_ch != nullptr) {
        // set the number of pending channel ops for this specific
        // protocol, as well as the total number for this channel
        auto *pending_ch_ops        = &this->target_ch->pending_ch_ops[this->header->get_protocol_type()];
        num_pending_ch_ops_protocol = pending_ch_ops->size();
        num_pending_ch_ops_total    = this->target_ch->num_pending_ch_ops;
        sprintf(cuid_str, "%p", (void *) this->target_ch->c_uid);
    }

    if (this->checkpoint_count == 0) {
        sprintf(tmp_cstr,
                "%s"
                ">> [ckpt %3d]\n"
                "|| > uid                 %18lu,    src_addr            %18p\n"
                "|| > seqnum              %18lu,    dst_addr            %18p\n"
                "|| > port                %18p,    size                %18s\n"
                "|| >                     %18s     eager buf refcount  %18d\n"
                "|| > checksum            %18p,    q idx/busy/cap      (%4d, %4lu, %4lu)\n"
                "|| > protocol type       %18s,    is_aggregated       %18s\n"
                "|| > header type         %18s,    work_req            %18p\n"
                "|| > traffic type        %18s,    work req count      %18lu\n"
                "|| > remote_nid          %18d,    tg chan c_uid        %18s\n"
                "|| > gw_msg              %18p,    active ch count     %18lu\n"
                "|| > send_return_mode    %18s,    ch op count: prtcl  %18lu\n"
                "|| > timeout (sec)       %18lf,    ch op count: total  %18lu\n"
                "|| > time (sec)          %18lf,    access count        %18lu\n"
                "|| > refcount            %18lu,    ch is active        %18s\n"
                "|| > tg available_msgs   %18lu,    gw available_msgs   %18lu\n"
                "|| > tg available_blocks %18lu,    gw available_blocks %18lu\n"
                "||\n"
                "|| >>> event: %s => in file %s, at line %d\n"
                "\\/\n",
                BEGIN_HISTORY,
                this->checkpoint_count,
                this->uid,
                this->src_addr,
                this->header->seqnum,
                this->dst_addr,
                (void *) this->header->port,
                (this->size != (size_t) HSTA_INVALID_SIZE) ? std::to_string(this->size).c_str() : "unknown size",
                "",  // filling blank space
                this->eager_buf ? this->eager_buf->get_refcount() : 0,
                (void *) this->header->checksum,
                queue_index,
                queue_busy,
                queue_cap,
                this->header->protocol_type_to_string(),
                this->is_aggregated ? "true" : "false",
                this->header->type_to_string(),
                (void *) this->work_req,
                hsta_utils.traffic_type_to_string(this->traffic_type),
                this->agent->num_pending_work_reqs,
                this->nid,
                this->target_ch ? cuid_str : "no channel",
                (void *) (this->work_req ? this->work_req->gw_msg : nullptr),
                this->agent->active_chs.size(),
                this->header->send_return_mode_to_string(),
                num_pending_ch_ops_protocol,
                ((double) this->header->get_timeout()) / 1e9,
                num_pending_ch_ops_total,
                hsta_utils.get_time() - this->init_time,
                hsta_utils.get_access_count(this),
                this->refcount,
                this->target_ch ? (this->target_ch->is_active() ? "true" : "false") : "no channel",
                this->target_ch  && this->target_ch->is_local ? hsta_utils.get_available_msgs(&this->target_ch->dragon_ch) : 0ul,
                this->work_req && this->work_req->gw_ch ? hsta_utils.get_available_msgs(&this->work_req->gw_ch->dragon_gw_ch) : 0ul,
                this->target_ch && this->target_ch->is_local ? hsta_utils.get_available_blocks(&this->target_ch->dragon_ch) : 0ul,
                this->work_req && this->work_req->gw_ch ? hsta_utils.get_available_blocks(&this->work_req->gw_ch->dragon_gw_ch) : 0ul,
                event, file, line);
    } else {
        sprintf(tmp_cstr,
                "%s"
                ">> [ckpt %3d]\n"
                "|| > timeout (sec)       %18lf,    refcount            %18lu\n"
                "|| > time (sec)          %18lf,    access count        %18lu\n"
                "||\n"
                "|| >>> event: %s => in file %s, at line %d\n"
                "\\/\n",
                (this->checkpoint_count == -1) ? BEGIN_PRE_HISTORY : "",
                this->checkpoint_count,
                ((double) this->header->get_timeout()) / 1e9,
                this->refcount,
                hsta_utils.get_time() - this->init_time,
                hsta_utils.get_access_count(this),
                event, file, line);
    }

    std::string new_history(tmp_cstr);

    this->history += new_history;
    ++this->checkpoint_count;

    if (force_print) {
        fprintf(hsta_dbg_file, "%s%s", this->history.c_str(), END_HISTORY);
        fflush(hsta_dbg_file);
    }

    hsta_lock_release(hsta_utils.lock);
}

uint64_t CqEvent::generate_checksum(bool for_verification)
{
#if 0
    // TODO: this needs to be updated for a number of reasons, one of which
    // is that TargetChan no longer contains ser_chan_desc
    void *payload_addr = nullptr;
    size_t payload_size;
    std::string ser_ch_descr;

    if (for_verification) {
        payload_addr = this->dst_addr;
        payload_size = this->size;
        ser_ch_descr = this->target_ch->ser_chan_desc;
    } else {
        payload_addr = (void *)this->work_req->rma_iov->get_payload_base();
        payload_size = this->work_req->rma_iov->core.payload_size;
        ser_ch_descr = this->work_req->ser_chan_desc;
    }

    auto hash = hsta_utils.hash_array((uint8_t *) payload_addr, payload_size);

    auto ser_ch_descr_hash =
        hsta_utils.hash_array((uint8_t *) ser_ch_descr.c_str(), ser_ch_descr.size());

    hash ^= ser_ch_descr_hash;
    hash = hsta_utils.hash_array((uint8_t *) &hash, sizeof(hash));

    hash ^= this->header->port;
    hash = hsta_utils.hash_array((uint8_t *) &hash, sizeof(hash));

    hash ^= header->seqnum;
    hash = hsta_utils.hash_array((uint8_t *) &hash, sizeof(hash));

    return hash;
#endif

    return HSTA_INVALID_UINT64;
}

void CqEvent::verify_checksum()
{
    auto test_checksum = this->generate_checksum(true);

    if (this->header->checksum != test_checksum) {
        hsta_utils.log("BAD CHECKSUM\n\n");

        char tmp[64];
        sprintf(tmp, "bad checksum: %p", (void *) test_checksum);
        hsta_log_history(this, tmp, false, false);
        hsta_utils.graceful_exit(SIGINT);
    }
}

void CqEvent::set_seqnum_and_checksum(WorkRequest *work_req)
{
    if (work_req != nullptr) {
        if (work_req->gw_msg != nullptr) {
            // generate new sequence number and checksum only at the beginning
            // of a flow, i.e., when we have a non-null gw message
            this->header->seqnum = ++hsta_cqe_seqnum_ctr[this->header->port];

            // only generate checksums for sendmsg work requests, since getmsg
            // and poll requests don't have any payload
            if (work_req->type == WORK_REQ_TYPE_SENDMSG) {
                auto buf = work_req->rma_iov->get_payload_base();
                auto size = work_req->rma_iov->core.payload_size;
                this->header->checksum = hsta_utils.hash_array((uint8_t *)buf, size);
            } else {
                this->header->checksum = HSTA_DEFAULT_CHECKSUM;
            }

            auto target_ch = hsta_my_agent->chan_from_cuid[work_req->c_uid];
            this->header->c_uid = target_ch->c_uid;
        } else {
            // this is the cqe for an ack, the work request should have the
            // sequence number and checksum set based on the received header
            this->header->seqnum   = work_req->seqnum;
            this->header->checksum = work_req->checksum;
            this->header->c_uid     = work_req->cuid;
        }
    }
    // else... this is a cqe generated while processing the rx eager buf,
    // so the sequence number and checksum will be available in the header
}

void CqEvent::check_ordering_violation(std::string event)
{
    auto return_early =
           !dragon_hsta_debug
        || this->header->get_type() == HEADER_TYPE_CTRL_CONN
        || this->header->get_type() == HEADER_TYPE_CTRL_RKEY;

    if (return_early) {
        return;
    }

    if (!this->is_aggregated)
    {
        auto event_and_fid = event + std::to_string(this->header->port);

        auto last_history = hsta_cqe_history_at_event[event_and_fid];
        auto last_seqnum = hsta_cqe_seqnum_at_event[event_and_fid];

        hsta_cqe_history_at_event[event_and_fid] = this->history;
        hsta_cqe_seqnum_at_event[event_and_fid]  = this->header->seqnum;

        if (last_seqnum >= this->header->seqnum)
        {
            hsta_lock_acquire(hsta_utils.lock);
            fprintf(hsta_dbg_file, "\nerror: ordering violation at event \"%s\": "
                                   "current fid = %p, current seqnum = %lu, current uid = %lu, last seqnum = %lu\n"
                                   "> previous history below:\n"
                                   "%s\n\n",
                                   event.c_str(), (void *) this->header->port, this->header->seqnum, this->uid, last_seqnum, last_history.c_str());
            fflush(hsta_dbg_file);
            hsta_lock_release(hsta_utils.lock);

            this->log_history("ordering violation: seqnum lower than expected", false, __FILE__, __LINE__);
            hsta_utils.graceful_exit(SIGINT);
        }
    }
}

void CqEvent::verify_is_active()
{
    if (hsta_my_agent->cqe_objq.is_free(this)) {
        fprintf(
            hsta_dbg_file, "error: refcount updated on free cqe\n"
            "%s" // current backtrace
            "backtrace from completion event\n%s",
            hsta_utils.get_backtrace().c_str(),
            hsta_my_agent->cqe_objq.get_free_backtrace(this).c_str()
        );
        hsta_utils.graceful_exit(SIGINT);
    }
}
