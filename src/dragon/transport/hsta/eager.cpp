#include "eager.hpp"
#include "agent.hpp"
#include "cq.hpp"
#include "globals.hpp"
#include "mem_descr.hpp"
#include "tx_queue.hpp"
#include "utils.hpp"

uint64_t EagerBuf::uid_counter = 0ul;

void EagerBuf::init(Agent *agent, TrafficType traffic_type, int queue_index)
{
    // allocate payload
    if (traffic_type == TRAFFIC_TYPE_TX) {
        this->payload = (char *) malloc(HSTA_EAGER_BUF_MAX_BYTES);
        this->mem_descr = nullptr;
    } else {
        this->mem_descr = new MemDescr(agent, HSTA_EAGER_BUF_MAX_BYTES);
        this->payload = (char *) this->mem_descr->get_buf_at_offset(0ul);
    }

    hsta_dbg_assert(this->payload != nullptr);

    // set header for aggregation buffer
    auto ag_header = (AgHeader *)this->payload;
    ag_header->nid  = agent->network.this_nid;
    ag_header->size = sizeof(AgHeader);

    this->ag_header    = ag_header;
    this->uid          = this->uid_counter++;
    this->agent        = agent;
    this->traffic_type = traffic_type;
    this->queue_index  = queue_index;
    this->target_nid   = queue_index;
    this->set_refcount(0);

    // (re)set some default values, must happen after payload is set
    this->reset();
}

void EagerBuf::fini()
{
    // free the payload
    if (this->traffic_type == TRAFFIC_TYPE_TX) {
        free(this->payload);
    } else {
        delete this->mem_descr;
    }
}

Header *EagerBuf::get_next_header(void **eager_data, eager_size_t *eager_size)
{
    auto current_offset =
        (size_t) this->header_buf - (size_t) this->payload;

    if (current_offset == this->ag_header->size) {
        return nullptr;
    }

    auto header_buf_size = this->tmp_header.copy_in(this->header_buf);

    if (this->tmp_header.eager_size > 0) {
        *eager_data = (void *) ((uint8_t *) this->header_buf + header_buf_size);
        *eager_size = tmp_header.eager_size;
    } else {
        *eager_data = nullptr;
        *eager_size = 0u;
    }

    this->header_buf =
        (void *) ((uint8_t *) this->header_buf + (header_buf_size + *eager_size));

    return &this->tmp_header;
}

void EagerBuf::add_to_payload(Header *header, void *eager_data, size_t eager_size)
{
    ++this->fill_count;
    this->spin_count = 0;

    // copy the header and any payload into the eager buf

    auto payload_buf = &this->payload[this->ag_header->size];
    this->ag_header->size += header->copy_out(payload_buf);

    if (eager_size > 0ul) {
        hsta_dbg_assert(eager_data != nullptr);
        payload_buf = &this->payload[this->ag_header->size];
        memcpy(payload_buf, eager_data, eager_size);
        this->ag_header->size += eager_size;
    }

    // there should be space for a header and any eager data in the
    // eager buffer before calling this function
    hsta_dbg_assert(HSTA_EAGER_BUF_MAX_BYTES >= this->ag_header->size);
}

void EagerBuf::insert(CqEvent *cqe)
{
    if (dragon_hsta_debug) {
        hsta_log_history(cqe, "inserting into eager buf", false, true);
    }

    this->stashed_cqes.push_back(cqe);
    hsta_cqe_inc_refcount(cqe, &this->stashed_cqes, "stashed_cqes");

    this->add_to_payload(cqe->header,
                         cqe->src_addr,
                         cqe->size);

    auto header_type = cqe->header->get_type();

    // TODO: this could be handled more cleanly with refcounts

    // post the send operation if needed

    auto delay_completion = false;

    if (header_type == HEADER_TYPE_CTRL_RTS) {
        auto *work_req = cqe->work_req;

        cqe->header->set_type(HEADER_TYPE_DATA_RNDV);

        cqe->src_addr = (void *)work_req->rma_iov->get_payload_base();
        cqe->size = work_req->rma_iov->core.payload_size;

        hsta_my_agent->network.send_rndv(cqe);

        if (cqe->header->get_send_return_mode() == DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED) {
            delay_completion = true;
        }
    }

    // TODO: simplify this
    if (   !delay_completion
        && cqe->work_req != nullptr
        && !cqe->work_req->is_complete
        && !cqe->needs_ack_for_completion)
    {
        cqe->work_req->complete(cqe);
    }
}

void EagerBuf::inc_refcount(int ref)
{
    // ref == 1 means the caller is taking a reference to the buffer
    this->refcount += ref;
}

void EagerBuf::dec_refcount(int ref)
{
    this->refcount -= ref;
}

void EagerBuf::set_refcount(int updated_refcount)
{
    this->refcount = updated_refcount;
}

int EagerBuf::get_refcount()
{
    return this->refcount;
}

bool EagerBuf::refcount_is_zero()
{
    return this->refcount == 0;
}
